"""Benchmark: feature serving over HTTP at fixed request rates (ADR-016).

The script fills one Redis database with the user features of N users, starts
`featureforge-serve` as one process and sends requests at a fixed rate from several
client processes. The load is open: a request is due at a fixed time whether or not the
previous ones have returned, and its latency counts from that due time. A slow service
therefore shows its queue in the numbers and does not slow the clients down.

    python scripts/benchmark_feature_serving.py --entities 1000000 --rates 100,500,1000,2000

Each request asks for two features of five users, ten entries in all. The users follow a
Zipf-like distribution with an exponent of about one. Client and service share one machine
and one Redis container, so the numbers describe that setup and not a network.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib.metadata
import json
import math
import multiprocessing
import platform
import resource
import subprocess
import sys
import time
import urllib.request
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import numpy as np

TOKEN = "__E__"
PATH = "/v1/features/lookup"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)
FEATURES = ("user_play_count_1h", "user_skip_rate_24h")
PERCENTILES = (50.0, 90.0, 95.0, 99.0, 99.9)
SPECS_DIR = Path(__file__).resolve().parents[1] / "feature_repo" / "specs"


def zipf_ranks(rng: np.random.Generator, entities: int, size: int) -> np.ndarray:
    """Ranks 1..entities with a probability roughly proportional to 1 / rank."""
    uniform = rng.random(size)
    ranks = np.floor(np.exp(uniform * math.log(entities + 1))).astype(np.int64)
    return np.clip(ranks, 1, entities)


def make_payloads(
    entities: int, per_request: int, count: int, seed: int, reference_time: str
) -> list[bytes]:
    if per_request > entities:
        raise ValueError("entities per request must not exceed the number of entities")
    rng = np.random.default_rng(seed)
    base: dict[str, Any] = {
        "features": [{"name": name} for name in FEATURES],
        "reference_time": reference_time,
    }
    payloads = []
    for _ in range(count):
        chosen: list[int] = []
        while len(chosen) < per_request:
            for rank in zipf_ranks(rng, entities, per_request * 2).tolist():
                if rank not in chosen:
                    chosen.append(rank)
                if len(chosen) == per_request:
                    break
        body = {**base, "entities": [{"user_id": f"u{rank}"} for rank in chosen]}
        payloads.append(json.dumps(body, separators=(",", ":")).encode())
    return payloads


def http_request(host: str, port: int, body: bytes) -> bytes:
    head = (
        f"POST {PATH} HTTP/1.1\r\nHost: {host}:{port}\r\n"
        f"Content-Type: application/json\r\nContent-Length: {len(body)}\r\n\r\n"
    )
    return head.encode() + body


class Connection:
    """One keep-alive HTTP/1.1 connection that reads a response with a Content-Length."""

    def __init__(self, host: str, port: int) -> None:
        self._host = host
        self._port = port
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
        self._reader = None
        self._writer = None

    async def exchange(self, request: bytes) -> tuple[int, bytes]:
        if self._reader is None or self._writer is None:
            self._reader, self._writer = await asyncio.open_connection(self._host, self._port)
        self._writer.write(request)
        head = await self._reader.readuntil(b"\r\n\r\n")
        lines = head.split(b"\r\n")
        status = int(lines[0].split()[1])
        length = 0
        closing = False
        for line in lines[1:]:
            name, _, value = line.partition(b":")
            key = name.strip().lower()
            if key == b"content-length":
                length = int(value)
            elif key == b"connection" and value.strip().lower() == b"close":
                closing = True
        body = await self._reader.readexactly(length) if length else b""
        if closing:
            self.close()
        return status, body


@dataclass
class LoadResult:
    scheduled: int = 0
    latencies_ms: list[float] = field(default_factory=list)
    service_ms: list[float] = field(default_factory=list)
    http_errors: int = 0
    transport_errors: int = 0
    reconnects: int = 0
    entries_total: int = 0
    entry_statuses: dict[str, int] = field(default_factory=dict)
    entry_error_types: dict[str, int] = field(default_factory=dict)
    max_in_flight: int = 0
    duration_s: float = 0.0


async def run_load(
    host: str, port: int, rate: float, payloads: list[bytes], connections: int, start: float
) -> LoadResult:
    """Send the payloads at a fixed rate from `start` on and time each from its due time."""
    result = LoadResult(scheduled=len(payloads))
    pool: asyncio.Queue[Connection] = asyncio.Queue()
    for _ in range(connections):
        pool.put_nowait(Connection(host, port))
    requests = [http_request(host, port, payload) for payload in payloads]
    in_flight = 0

    async def one(due: float, request: bytes) -> None:
        nonlocal in_flight
        in_flight += 1
        result.max_in_flight = max(result.max_in_flight, in_flight)
        connection = await pool.get()
        sent = time.perf_counter()
        try:
            try:
                status, body = await asyncio.wait_for(connection.exchange(request), 10)
            except (OSError, EOFError):
                connection.close()
                result.reconnects += 1
                status, body = await asyncio.wait_for(connection.exchange(request), 10)
            finished = time.perf_counter()
        except (OSError, EOFError, TimeoutError):
            connection.close()
            result.transport_errors += 1
            return
        finally:
            in_flight -= 1
            pool.put_nowait(connection)
        if status != 200:
            result.http_errors += 1
            return
        try:
            entries = json.loads(body)["entries"]
        except (ValueError, KeyError, TypeError):
            result.http_errors += 1
            return
        result.latencies_ms.append((finished - due) * 1000)
        result.service_ms.append((finished - sent) * 1000)
        for entry in entries:
            name = str(entry.get("status"))
            result.entry_statuses[name] = result.entry_statuses.get(name, 0) + 1
            error_type = entry.get("error_type")
            if error_type:
                kind = str(error_type)
                result.entry_error_types[kind] = result.entry_error_types.get(kind, 0) + 1
            result.entries_total += 1

    tasks = []
    for index, request in enumerate(requests):
        due = start + index / rate
        delay = due - time.perf_counter()
        if delay > 0:
            await asyncio.sleep(delay)
        tasks.append(asyncio.create_task(one(due, request)))
    await asyncio.gather(*tasks)
    result.duration_s = time.perf_counter() - start
    return result


def worker_main(arguments: tuple[Any, ...]) -> LoadResult:
    host, port, rate, count, connections, entities, per_request, seed, reference, start_at = (
        arguments
    )
    payloads = make_payloads(entities, per_request, count, seed, reference)
    start = time.perf_counter() + max(0.0, start_at - time.time())
    return asyncio.run(run_load(host, port, rate, payloads, connections, start))


def percentile_dict(values: np.ndarray) -> dict[str, float]:
    if values.size == 0:
        return {**{f"p{p:g}": math.nan for p in PERCENTILES}, "max": math.nan}
    points = np.percentile(values, PERCENTILES)
    result = {f"p{p:g}": float(v) for p, v in zip(PERCENTILES, points, strict=True)}
    result["max"] = float(values.max())
    return result


def summarize(results: list[LoadResult], target_rate: int) -> dict[str, Any]:
    latencies = np.concatenate([np.asarray(r.latencies_ms, dtype=float) for r in results])
    scheduled = sum(r.scheduled for r in results)
    errors = sum(r.http_errors + r.transport_errors for r in results)
    statuses: dict[str, int] = {}
    for result in results:
        for name, count in result.entry_statuses.items():
            statuses[name] = statuses.get(name, 0) + count
    error_types: dict[str, int] = {}
    for result in results:
        for name, count in result.entry_error_types.items():
            error_types[name] = error_types.get(name, 0) + count
    entries_total = sum(r.entries_total for r in results)
    duration = max(r.duration_s for r in results)
    latency = percentile_dict(latencies)
    service = percentile_dict(
        np.concatenate([np.asarray(r.service_ms, dtype=float) for r in results])
    )
    return {
        "target_rps": target_rate,
        "scheduled": scheduled,
        "completed": int(latencies.size),
        "achieved_rps": latencies.size / duration if duration else 0.0,
        "errors": errors,
        "error_rate": errors / scheduled if scheduled else 0.0,
        "entries_total": entries_total,
        "entries_not_ok": entries_total - statuses.get("ok", 0),
        "entry_statuses": statuses,
        "entry_error_types": error_types,
        "latency_ms": latency,
        "service_ms": service,
        "max_in_flight": max(r.max_in_flight for r in results),
        "reconnects": sum(r.reconnects for r in results),
    }


def render_table(rows: list[dict[str, Any]]) -> str:
    lines = [
        "| Target req/s | Run | Achieved req/s | p50 ms | p95 ms | p99 ms | p99.9 ms | Max ms "
        "| Service p50 ms | Service p99 ms | Errors | Not-ok entries "
        "| Server CPU cores | Client CPU cores |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]
    for row in rows:
        latency = row["latency_ms"]
        service = row["service_ms"]
        lines.append(
            f"| {row['target_rps']} | {row['repeat']} | {row['achieved_rps']:.0f} "
            f"| {latency['p50']:.2f} | {latency['p95']:.2f} | {latency['p99']:.2f} "
            f"| {latency['p99.9']:.2f} | {latency['max']:.2f} | {service['p50']:.2f} "
            f"| {service['p99']:.2f} | {row['errors']} | {row['entries_not_ok']} "
            f"| {row['server_cpu_cores']:.2f} | {row['client_cpu_cores']:.2f} |"
        )
    return "\n".join(lines)


def children_cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    return usage.ru_utime + usage.ru_stime


def run_rate(
    args: argparse.Namespace, server_pid: int, reference: str, rate: int, duration: float
) -> dict[str, Any]:
    import psutil

    workers = args.workers
    per_worker_rate = rate / workers
    count = int(per_worker_rate * duration)
    connections = max(2, min(args.connections, math.ceil(per_worker_rate / 10)))
    start_at = time.time() + 3.0
    jobs = [
        (
            "127.0.0.1",
            args.port,
            per_worker_rate,
            count,
            connections,
            args.entities,
            args.entities_per_request,
            args.seed + index + rate,
            reference,
            start_at,
        )
        for index in range(workers)
    ]
    server = psutil.Process(server_pid)
    client_before = children_cpu_seconds()
    context = multiprocessing.get_context("spawn")
    with context.Pool(workers) as pool:
        time.sleep(max(0.0, start_at - time.time()))
        server_before = sum(server.cpu_times()[:2])
        wall_start = time.time()
        results = pool.map(worker_main, jobs)
        wall = time.time() - wall_start
        server_after = sum(server.cpu_times()[:2])
    client_after = children_cpu_seconds()
    row = summarize(results, rate)
    row["duration_s"] = duration
    row["server_cpu_cores"] = (server_after - server_before) / wall
    row["client_cpu_cores"] = (client_after - client_before) / wall
    row["workers"] = workers
    row["connections_per_worker"] = connections
    return row


def load_specs() -> list[Any]:
    from featureforge.spec.loader import load_spec

    return [load_spec(SPECS_DIR / f"{name}.yaml") for name in FEATURES]


def reference_time() -> str:
    specs = load_specs()
    return (OBSERVATION + min(spec.freshness_slo for spec in specs) / 2).isoformat()


def seed(client: Any, entities: int, batch: int = 10_000) -> str:
    """Write one valid record per user and feature, and return the reference time."""
    from featureforge.spec.reference import evaluate_expression
    from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
    from featureforge.streaming.spec_store import RedisSpecStore

    templates = []
    for spec in load_specs():
        counts = {a.name: 3 for a in spec.aggregations}
        data: dict[str, Any] = {
            "feature_schema_version": "v1",
            "feature_name": spec.name,
            spec.keys[0]: TOKEN,
            "observation_time": stamp(OBSERVATION),
            "window_start_exclusive": stamp(OBSERVATION - spec.window.size),
            **counts,
        }
        for name, expression in spec.derived.items():
            data[name] = evaluate_expression(expression, counts)
        payload = json.dumps(data, sort_keys=True, separators=(",", ":"))
        parse_feature_record(payload.replace(TOKEN, "u1"), spec)
        key = RedisSpecStore(client, spec).key_for(TOKEN)
        templates.append((key, payload, int(spec.online.ttl.total_seconds())))
    for start in range(1, entities + 1, batch):
        pipeline = client.pipeline(transaction=False)
        for rank in range(start, min(start + batch, entities + 1)):
            entity = f"u{rank}"
            for key, payload, ttl in templates:
                pipeline.set(key.replace(TOKEN, entity), payload.replace(TOKEN, entity), ex=ttl)
        pipeline.execute()
    marker_ttl = min(ttl for _, _, ttl in templates) - 300
    client.set("loadtest:seed", json.dumps({"entities": entities}), ex=marker_ttl)
    return reference_time()


def payload_template(spec: Any) -> str:
    """One valid record of a spec, with the entity value left as TOKEN."""
    from featureforge.spec.reference import evaluate_expression
    from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp

    counts = {a.name: 3 for a in spec.aggregations}
    data: dict[str, Any] = {
        "feature_schema_version": "v1",
        "feature_name": spec.name,
        spec.keys[0]: TOKEN,
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - spec.window.size),
        **counts,
    }
    for name, expression in spec.derived.items():
        data[name] = evaluate_expression(expression, counts)
    payload = json.dumps(data, sort_keys=True, separators=(",", ":"))
    parse_feature_record(payload.replace(TOKEN, "u1"), spec)
    return payload


def cassandra_rows(
    entities: int, templates: list[tuple[str, str, str, int]]
) -> Iterator[tuple[str, str, str, str, int]]:
    """Yield (entity key, entity value, feature, payload, ttl) for every user and feature."""
    for rank in range(1, entities + 1):
        entity = f"u{rank}"
        for entity_key, feature, payload, ttl in templates:
            yield (entity_key, entity, feature, payload.replace(TOKEN, entity), ttl)


def write_cassandra_specs(directory: Path) -> Path:
    """Copy the benchmarked specs with store: cassandra, as the end-to-end test does."""
    directory.mkdir(parents=True, exist_ok=True)
    for name in FEATURES:
        text = (SPECS_DIR / f"{name}.yaml").read_text()
        if text.count("store: redis") != 1:
            raise ValueError(f"{name}.yaml must name store: redis exactly once")
        (directory / f"{name}.yaml").write_text(text.replace("store: redis", "store: cassandra"))
    return directory


def cassandra_seeded_entities(session: Any, keyspace: str) -> int | None:
    from featureforge.streaming.cassandra_schema import TABLE

    row = session.execute(
        f"SELECT payload FROM {keyspace}.{TABLE} "
        "WHERE entity_key = %s AND entity_value = %s AND feature = %s",
        ("loadtest", "seed", "marker"),
    ).one()
    return None if row is None else int(json.loads(row.payload)["entities"])


def seed_cassandra(session: Any, keyspace: str, entities: int, concurrency: int = 64) -> str:
    """Write one valid row per user and feature, and return the reference time."""
    from cassandra.concurrent import execute_concurrent_with_args

    from featureforge.streaming.cassandra_schema import TABLE

    templates = [
        (
            spec.keys[0],
            f"v{spec.version}:{spec.name}",
            payload_template(spec),
            int(spec.online.ttl.total_seconds()),
        )
        for spec in load_specs()
    ]
    statement = session.prepare(
        f"INSERT INTO {keyspace}.{TABLE} (entity_key, entity_value, feature, payload) "
        "VALUES (?, ?, ?, ?) USING TTL ?"
    )
    for success, result in execute_concurrent_with_args(
        session,
        statement,
        cassandra_rows(entities, templates),
        concurrency=concurrency,
        results_generator=True,
    ):
        if not success:
            raise result
    marker_ttl = min(template[3] for template in templates) - 300
    session.execute(
        f"INSERT INTO {keyspace}.{TABLE} (entity_key, entity_value, feature, payload) "
        f"VALUES (%s, %s, %s, %s) USING TTL {marker_ttl}",
        ("loadtest", "seed", "marker", json.dumps({"entities": entities})),
    )
    return reference_time()


def start_server(
    redis_url: str,
    port: int,
    budget_ms: float,
    extra_arguments: list[str] | None = None,
) -> subprocess.Popen[bytes]:
    command = [
        sys.executable,
        "-m",
        "featureforge.service_api",
        "--redis-url",
        redis_url,
        "--port",
        str(port),
        "--budget-ms",
        str(budget_ms),
        "--quiet",
        *(extra_arguments or []),
    ]
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL)
    for _ in range(60):
        if process.poll() is not None:
            raise RuntimeError("the service exited before it was ready")
        try:
            with urllib.request.urlopen(f"http://127.0.0.1:{port}/readyz", timeout=1) as reply:
                if reply.status == 200:
                    return process
        except OSError:
            time.sleep(0.5)
    process.terminate()
    raise RuntimeError("the service did not become ready")


def git_state() -> dict[str, Any]:
    def run(*command: str) -> str:
        return subprocess.run(command, capture_output=True, text=True, check=False).stdout.strip()

    return {
        "commit": run("git", "rev-parse", "HEAD") or None,
        "dirty": bool(run("git", "status", "--porcelain")),
    }


def environment(client: Any) -> dict[str, Any]:
    import psutil

    versions: dict[str, str | None] = {}
    for package in ("fastapi", "uvicorn", "starlette", "redis", "pydantic", "numpy", "psutil"):
        try:
            versions[package] = importlib.metadata.version(package)
        except importlib.metadata.PackageNotFoundError:
            versions[package] = None
    cpu = platform.processor()
    if sys.platform == "darwin":
        brand = subprocess.run(
            ["sysctl", "-n", "machdep.cpu.brand_string"],
            capture_output=True,
            text=True,
            check=False,
        ).stdout.strip()
        cpu = brand or cpu
    info = client.info() if client is not None else {}
    return {
        "platform": platform.platform(),
        "cpu": cpu,
        "logical_cpus": psutil.cpu_count(),
        "memory_bytes": psutil.virtual_memory().total,
        "python": platform.python_version(),
        "packages": versions,
        "redis_server": info.get("redis_version"),
        "redis_aof_enabled": bool(info.get("aof_enabled")),
        "git": git_state(),
    }


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Benchmark the feature serving service.")
    parser.add_argument("--entities", type=int, default=1_000_000)
    parser.add_argument("--entities-per-request", type=int, default=5)
    parser.add_argument("--rates", default="100,500,1000,2000,3000,4000,6000")
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--warmup", type=float, default=10.0)
    parser.add_argument("--workers", type=int, default=4)
    parser.add_argument("--connections", type=int, default=32)
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/1")
    parser.add_argument("--backend", choices=("redis", "cassandra"), default="redis")
    parser.add_argument("--cassandra-host", default="127.0.0.1")
    parser.add_argument("--cassandra-keyspace", default="ff_loadtest")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--budget-ms", type=float, default=50.0)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--reseed", action="store_true")
    parser.add_argument("--output", type=Path, default=None)
    return parser.parse_args(argv)


def main_cassandra(args: argparse.Namespace) -> int:
    """Run the benchmark against a service that reads from Cassandra (ADR-016)."""
    import tempfile

    from featureforge.streaming.cassandra_connection import open_session
    from featureforge.streaming.cassandra_schema import TABLE, ensure_schema

    rates = [int(value) for value in args.rates.split(",")]
    keyspace = args.cassandra_keyspace
    with tempfile.TemporaryDirectory() as directory, open_session(args.cassandra_host) as session:
        specs_dir = write_cassandra_specs(Path(directory))
        ensure_schema(session, keyspace)
        seeded = cassandra_seeded_entities(session, keyspace)
        if args.reseed or seeded != args.entities:
            print(f"Seeding {args.entities} users in keyspace {keyspace} ...", file=sys.stderr)
            session.execute(f"TRUNCATE {keyspace}.{TABLE}", timeout=300)
            started = time.time()
            reference = seed_cassandra(session, keyspace, args.entities)
            print(f"Seeded in {time.time() - started:.0f} s.", file=sys.stderr)
        else:
            reference = reference_time()
        server = start_server(
            args.redis_url,
            args.port,
            args.budget_ms,
            extra_arguments=[
                "--specs-dir",
                str(specs_dir),
                "--cassandra-host",
                args.cassandra_host,
                "--cassandra-keyspace",
                keyspace,
            ],
        )
        rows: list[dict[str, Any]] = []
        try:
            print(f"Warm-up at {rates[0]} req/s for {args.warmup:.0f} s ...", file=sys.stderr)
            run_rate(args, server.pid, reference, rates[0], args.warmup)
            for rate in rates:
                for repeat in range(1, args.repeats + 1):
                    print(
                        f"Measuring {rate} req/s, run {repeat} of {args.repeats}, "
                        f"for {args.duration:.0f} s ...",
                        file=sys.stderr,
                    )
                    row = run_rate(args, server.pid, reference, rate, args.duration)
                    row["repeat"] = repeat
                    rows.append(row)
        finally:
            server.terminate()
            server.wait(timeout=10)
        details = environment(None)
        details["cassandra_server"] = (
            session.execute("SELECT release_version FROM system.local").one().release_version
        )
        details["packages"]["cassandra-driver"] = importlib.metadata.version("cassandra-driver")
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "setup": {
            "backend": "cassandra",
            "entities": args.entities,
            "cassandra_keyspace": keyspace,
            "cassandra_rows_expected": args.entities * len(FEATURES),
            "read_consistency": "LOCAL_QUORUM",
            "entities_per_request": args.entities_per_request,
            "features": list(FEATURES),
            "access_pattern": "Zipf-like, exponent about 1",
            "budget_ms": args.budget_ms,
            "warmup_s": args.warmup,
            "repeats": args.repeats,
            "service": "one uvicorn process, access log off",
        },
        "environment": details,
        "results": rows,
    }
    output = args.output or Path(
        f"output/benchmarks/feature-serving-cassandra-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2))
    print(render_table(rows))
    print(f"\nWrote {output}", file=sys.stderr)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    if args.backend == "cassandra":
        return main_cassandra(args)
    from redis import Redis

    args = parse_args(argv)
    if (urlparse(args.redis_url).path.strip("/") or "0") == "0":
        print("Use a dedicated Redis database such as redis://127.0.0.1:6379/1.", file=sys.stderr)
        return 2
    rates = [int(value) for value in args.rates.split(",")]
    client = Redis.from_url(args.redis_url)
    marker = client.get("loadtest:seed")
    if args.reseed or marker is None or json.loads(marker)["entities"] != args.entities:
        print(f"Seeding {args.entities} users in {args.redis_url} ...", file=sys.stderr)
        client.flushdb()
        started = time.time()
        reference = seed(client, args.entities)
        print(f"Seeded in {time.time() - started:.0f} s.", file=sys.stderr)
    else:
        reference = reference_time()
    keys = client.dbsize()
    server = start_server(args.redis_url, args.port, args.budget_ms)
    rows = []
    try:
        print(f"Warm-up at {rates[0]} req/s for {args.warmup:.0f} s ...", file=sys.stderr)
        run_rate(args, server.pid, reference, rates[0], args.warmup)
        for rate in rates:
            for repeat in range(1, args.repeats + 1):
                print(
                    f"Measuring {rate} req/s, run {repeat} of {args.repeats}, "
                    f"for {args.duration:.0f} s ...",
                    file=sys.stderr,
                )
                row = run_rate(args, server.pid, reference, rate, args.duration)
                row["repeat"] = repeat
                rows.append(row)
    finally:
        server.terminate()
        server.wait(timeout=10)
    report = {
        "generated_at": datetime.now(UTC).isoformat(),
        "setup": {
            "entities": args.entities,
            "backend": "redis",
            "redis_keys": keys,
            "entities_per_request": args.entities_per_request,
            "features": list(FEATURES),
            "access_pattern": "Zipf-like, exponent about 1",
            "budget_ms": args.budget_ms,
            "warmup_s": args.warmup,
            "repeats": args.repeats,
            "service": "one uvicorn process, access log off",
        },
        "environment": environment(client),
        "results": rows,
    }
    output = args.output or Path(
        f"output/benchmarks/feature-serving-{datetime.now(UTC):%Y%m%dT%H%M%SZ}.json"
    )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=2))
    print(render_table(rows))
    print(f"\nWrote {output}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
