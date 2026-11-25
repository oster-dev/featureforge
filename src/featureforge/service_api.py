"""HTTP layer of the feature serving service (ADR-016)."""

import argparse
import asyncio
from collections.abc import AsyncIterator, Sequence
from contextlib import asynccontextmanager, suppress
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST
from pydantic import BaseModel, ConfigDict, Field
from redis import Redis
from redis.asyncio import BlockingConnectionPool as AsyncPool
from redis.asyncio import Redis as AsyncRedis
from starlette.concurrency import run_in_threadpool

from featureforge.feature_service import (
    FailPolicyError,
    FeatureRef,
    FeatureServer,
    RequestError,
)
from featureforge.service_metrics import ServiceMetrics, watch_event_loop_lag
from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FeatureRefBody(_Body):
    name: str = Field(min_length=1)
    version: int | None = Field(default=None, ge=1)


class LookupBody(_Body):
    features: list[FeatureRefBody]
    entities: list[dict[str, str]]
    reference_time: datetime | None = None


def create_app(server: FeatureServer, metrics: ServiceMetrics | None = None) -> FastAPI:
    service_metrics = metrics if metrics is not None else ServiceMetrics()

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        watcher = asyncio.create_task(watch_event_loop_lag(service_metrics))
        try:
            yield
        finally:
            watcher.cancel()
            with suppress(asyncio.CancelledError):
                await watcher

    app = FastAPI(title="FeatureForge feature serving", version="1", lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "alive"}

    @app.get("/readyz")
    def readyz() -> JSONResponse:
        try:
            ready = server.ping()
        except Exception:
            ready = False
        return JSONResponse(
            {"status": "ready" if ready else "not_ready"},
            status_code=200 if ready else 503,
        )

    @app.get("/metrics", include_in_schema=False)
    def metrics_endpoint() -> Response:
        return Response(service_metrics.render(), media_type=CONTENT_TYPE_LATEST)

    @app.post("/v1/features/lookup")
    async def lookup(body: LookupBody) -> JSONResponse:
        refs = [FeatureRef(ref.name, ref.version) for ref in body.features]
        try:
            if server.supports_async:
                result = await server.lookup_async(
                    refs, body.entities, reference_time=body.reference_time
                )
            else:
                result = await run_in_threadpool(
                    server.lookup, refs, body.entities, reference_time=body.reference_time
                )
        except RequestError as error:
            service_metrics.record_rejected("request_error")
            return JSONResponse({"detail": str(error)}, status_code=422)
        except FailPolicyError as error:
            service_metrics.record_rejected("fail_policy")
            return JSONResponse(
                {"detail": str(error), "features": list(error.features)},
                status_code=503,
            )
        service_metrics.record_lookup(result.entries, result.elapsed_ms)
        return JSONResponse(result.to_dict())

    return app


def build_server(specs_dir: Path, redis_url: str, budget_ms: float) -> FeatureServer:
    from featureforge.redis_serving_reader import RedisServingReader

    timeout = budget_ms / 1000
    client = Redis.from_url(redis_url, socket_timeout=timeout, socket_connect_timeout=timeout)
    specs = [load_spec(path) for path in sorted(specs_dir.glob("*.yaml"))]
    # Same limit as the 40 worker threads of the synchronous path. A request waits for a
    # free connection like it waited for a thread, and the socket timeout is the budget.
    pool = AsyncPool.from_url(
        redis_url,
        max_connections=40,
        timeout=None,
        socket_timeout=timeout,
        socket_connect_timeout=timeout,
    )
    reader = RedisServingReader(client, specs, async_client=AsyncRedis(connection_pool=pool))
    return FeatureServer(reader, specs)


def build_cassandra_server(
    specs: Sequence[FeatureSpec], session: object, budget_ms: float, *, keyspace: str
) -> FeatureServer:
    """Build a server that reads from Cassandra. The caller owns the session."""
    from featureforge.cassandra_serving_reader import CassandraServingReader

    reader = CassandraServingReader(session, specs, keyspace=keyspace, timeout=budget_ms / 1000)
    return FeatureServer(reader, specs)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="featureforge-serve", description="Serve spec features.")
    parser.add_argument("--specs-dir", type=Path, default=Path("feature_repo/specs"))
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--cassandra-host", default="127.0.0.1")
    parser.add_argument("--cassandra-port", type=int, default=9042)
    parser.add_argument("--cassandra-keyspace", default="featureforge")
    parser.add_argument("--cassandra-local-dc", default="datacenter1")
    parser.add_argument("--budget-ms", type=float, default=50.0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--quiet", action="store_true", help="No access log.")
    args = parser.parse_args(argv)

    import uvicorn

    specs = [load_spec(path) for path in sorted(args.specs_dir.glob("*.yaml"))]
    backends = sorted({str(spec.online.store) for spec in specs})
    if len(backends) != 1:
        parser.error(f"the specs must name one online store, found: {', '.join(backends)}")

    def serve(server: FeatureServer) -> None:
        uvicorn.run(
            create_app(server, ServiceMetrics(backends[0])),
            host=args.host,
            port=args.port,
            log_level="warning" if args.quiet else "info",
            access_log=not args.quiet,
        )

    if backends[0] == "cassandra":
        from cassandra.query import tuple_factory

        from featureforge.streaming.cassandra_connection import open_session

        with open_session(
            args.cassandra_host,
            args.cassandra_port,
            local_dc=args.cassandra_local_dc,
            row_factory=tuple_factory,
        ) as session:
            serve(
                build_cassandra_server(
                    specs, session, args.budget_ms, keyspace=args.cassandra_keyspace
                )
            )
    elif backends[0] == "redis":
        serve(build_server(args.specs_dir, args.redis_url, args.budget_ms))
    else:
        parser.error(f"unsupported online store: {backends[0]}")


if __name__ == "__main__":
    main()
