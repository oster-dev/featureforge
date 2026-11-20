"""HTTP layer of the feature serving service (ADR-016)."""

import argparse
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from redis import Redis

from featureforge.feature_service import (
    FailPolicyError,
    FeatureRef,
    FeatureServer,
    RequestError,
)
from featureforge.spec.loader import load_spec


class _Body(BaseModel):
    model_config = ConfigDict(extra="forbid")


class FeatureRefBody(_Body):
    name: str = Field(min_length=1)
    version: int | None = Field(default=None, ge=1)


class LookupBody(_Body):
    features: list[FeatureRefBody]
    entities: list[dict[str, str]]
    reference_time: datetime | None = None


def create_app(server: FeatureServer) -> FastAPI:
    app = FastAPI(title="FeatureForge feature serving", version="1")

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

    @app.post("/v1/features/lookup")
    def lookup(body: LookupBody) -> JSONResponse:
        try:
            result = server.lookup(
                [FeatureRef(ref.name, ref.version) for ref in body.features],
                body.entities,
                reference_time=body.reference_time,
            )
        except RequestError as error:
            return JSONResponse({"detail": str(error)}, status_code=422)
        except FailPolicyError as error:
            return JSONResponse(
                {"detail": str(error), "features": list(error.features)},
                status_code=503,
            )
        return JSONResponse(result.to_dict())

    return app


def build_server(specs_dir: Path, redis_url: str, budget_ms: float) -> FeatureServer:
    timeout = budget_ms / 1000
    client = Redis.from_url(redis_url, socket_timeout=timeout, socket_connect_timeout=timeout)
    specs = [load_spec(path) for path in sorted(specs_dir.glob("*.yaml"))]
    return FeatureServer(client, specs)


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="featureforge-serve", description="Serve spec features.")
    parser.add_argument("--specs-dir", type=Path, default=Path("feature_repo/specs"))
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--budget-ms", type=float, default=50.0)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args(argv)

    import uvicorn

    app = create_app(build_server(args.specs_dir, args.redis_url, args.budget_ms))
    uvicorn.run(app, host=args.host, port=args.port)


if __name__ == "__main__":
    main()
