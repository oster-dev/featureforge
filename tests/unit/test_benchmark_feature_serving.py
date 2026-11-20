"""Tests for the feature serving load generator (ADR-016)."""

import asyncio
import importlib.util
import json
import sys
import time
from pathlib import Path
from typing import Any

import numpy as np
import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "benchmark_feature_serving.py"
_spec = importlib.util.spec_from_file_location("benchmark_feature_serving", SCRIPT)
assert _spec is not None and _spec.loader is not None
bench = importlib.util.module_from_spec(_spec)
sys.modules["benchmark_feature_serving"] = bench
_spec.loader.exec_module(bench)

REFERENCE = "2026-01-02T12:02:30+00:00"


async def stub_server(
    delay: float = 0.0, status: int = 200, close_after_each: bool = False
) -> tuple[asyncio.AbstractServer, int]:
    body = json.dumps({"entries": [{"status": "ok"}, {"status": "missing"}]}).encode()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                head = await reader.readuntil(b"\r\n\r\n")
                length = 0
                for line in head.split(b"\r\n"):
                    name, _, value = line.partition(b":")
                    if name.strip().lower() == b"content-length":
                        length = int(value)
                await reader.readexactly(length)
                if delay:
                    await asyncio.sleep(delay)
                reply = (
                    f"HTTP/1.1 {status} X\r\nContent-Type: application/json\r\n"
                    f"Content-Length: {len(body)}\r\n\r\n"
                ).encode() + body
                writer.write(reply)
                await writer.drain()
                if close_after_each:
                    break
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            writer.close()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    return server, server.sockets[0].getsockname()[1]


def run(rate: float, count: int, connections: int, **stub: Any) -> Any:
    async def scenario() -> Any:
        server, port = await stub_server(**stub)
        payloads = bench.make_payloads(1000, 5, count, 1, REFERENCE)
        start = time.perf_counter() + 0.05
        try:
            return await bench.run_load("127.0.0.1", port, rate, payloads, connections, start)
        finally:
            server.close()
            await server.wait_closed()

    return asyncio.run(scenario())


def test_zipf_ranks_stay_in_range_and_favour_the_head() -> None:
    ranks = bench.zipf_ranks(np.random.default_rng(1), 1000, 200_000)
    assert ranks.min() >= 1
    assert ranks.max() <= 1000
    counts = np.bincount(ranks, minlength=1001)
    assert counts[1] > 10 * counts[100]


def test_each_payload_names_distinct_users_and_both_features() -> None:
    payloads = bench.make_payloads(50, 5, 200, 3, REFERENCE)
    for payload in payloads:
        body = json.loads(payload)
        users = [entity["user_id"] for entity in body["entities"]]
        assert len(users) == len(set(users)) == 5
        assert [f["name"] for f in body["features"]] == list(bench.FEATURES)
        assert body["reference_time"] == REFERENCE


def test_more_entities_per_request_than_entities_is_rejected() -> None:
    with pytest.raises(ValueError):
        bench.make_payloads(3, 5, 1, 1, REFERENCE)


def test_a_fast_service_completes_every_request_at_the_target_rate() -> None:
    result = run(rate=100, count=100, connections=4)
    assert len(result.latencies_ms) == 100
    assert result.http_errors == result.transport_errors == 0
    assert result.entries_total == 200
    assert result.entry_statuses == {"ok": 100, "missing": 100}
    assert 0.9 <= result.duration_s <= 3.0
    assert max(result.latencies_ms) < 500


def test_a_slow_service_shows_its_queue_in_the_latency() -> None:
    result = run(rate=100, count=100, connections=1, delay=0.02)
    latencies = np.asarray(result.latencies_ms)
    assert latencies.size == 100
    assert latencies.max() > 500
    assert np.percentile(latencies, 50) > 100
    assert len(result.service_ms) == 100
    assert np.percentile(result.service_ms, 50) < 100


def test_a_connection_closed_by_the_server_is_reopened() -> None:
    result = run(rate=50, count=25, connections=1, close_after_each=True)
    assert len(result.latencies_ms) == 25
    assert result.transport_errors == 0
    assert result.reconnects >= 20


def test_an_error_status_is_counted_and_has_no_latency() -> None:
    result = run(rate=100, count=20, connections=2, status=503)
    assert result.http_errors == 20
    assert result.latencies_ms == []


def test_summarize_reports_percentiles_and_error_rate() -> None:
    first = bench.LoadResult(
        scheduled=100,
        latencies_ms=[float(v) for v in range(1, 91)],
        http_errors=10,
        entries_total=180,
        entry_statuses={"ok": 150, "stale": 30},
        duration_s=1.0,
    )
    second = bench.LoadResult(scheduled=100, latencies_ms=[100.0] * 100, duration_s=2.0)
    row = bench.summarize([first, second], 200)
    assert row["scheduled"] == 200
    assert row["completed"] == 190
    assert row["errors"] == 10
    assert row["error_rate"] == pytest.approx(0.05)
    assert row["entries_not_ok"] == 30
    assert row["achieved_rps"] == pytest.approx(95.0)
    assert row["latency_ms"]["p50"] == pytest.approx(100.0)
    assert row["latency_ms"]["max"] == 100.0


def test_the_table_has_one_row_per_rate() -> None:
    row = {
        "target_rps": 100,
        "repeat": 1,
        "achieved_rps": 99.6,
        "errors": 0,
        "entries_not_ok": 0,
        "latency_ms": {"p50": 1.0, "p95": 2.0, "p99": 3.0, "p99.9": 4.0, "max": 5.0},
        "service_ms": {"p50": 0.5, "p99": 2.5},
        "server_cpu_cores": 0.5,
        "client_cpu_cores": 0.7,
    }
    table = bench.render_table([row, row])
    assert len(table.splitlines()) == 4
    assert (
        "| 100 | 1 | 100 | 1.00 | 2.00 | 3.00 | 4.00 | 5.00 | 0.50 | 2.50 | 0 | 0 | 0.50 | 0.70 |"
        in table
    )
