"""Drills for the feature serving core with a stopped and a slow Redis (ADR-016).

Redis is reached through Toxiproxy. A latency toxic makes it slow and a disabled proxy
makes it unreachable. Start it with: docker compose --profile fault up -d toxiproxy
"""

import json
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from redis import Redis

from featureforge.feature_service import FeatureEntry, FeatureRef, FeatureServer
from featureforge.service_api import create_app
from featureforge.spec.loader import load_spec
from featureforge.spec.model import Fallback, FallbackPolicy
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")
REFERENCE = OBSERVATION + SKIP.freshness_slo / 2

PROXY = "featureforge-serving-redis"
BUDGET = 0.1
SLOW_MS = 600
MAX_ELAPSED = 0.4


class Toxiproxy:
    """The few calls of the Toxiproxy HTTP API that the drills need."""

    def __init__(self, url: str) -> None:
        self._http = httpx.Client(base_url=url, timeout=5.0)

    def wait_ready(self) -> None:
        for _ in range(20):
            try:
                if self._http.get("/version").status_code == 200:
                    return
            except httpx.TransportError:
                pass
            time.sleep(0.25)
        raise RuntimeError("Toxiproxy is not reachable. Start it with the fault profile.")

    def reset_proxy(self, name: str, listen: str, upstream: str) -> None:
        self._http.delete(f"/proxies/{name}")
        response = self._http.post(
            "/proxies",
            json={"name": name, "listen": listen, "upstream": upstream, "enabled": True},
        )
        response.raise_for_status()

    def delete_proxy(self, name: str) -> None:
        self._http.delete(f"/proxies/{name}")

    def set_enabled(self, name: str, enabled: bool) -> None:
        self._http.post(f"/proxies/{name}", json={"enabled": enabled}).raise_for_status()

    def add_latency(self, name: str, milliseconds: int) -> None:
        self._http.post(
            f"/proxies/{name}/toxics",
            json={
                "name": "slow",
                "type": "latency",
                "stream": "downstream",
                "toxicity": 1.0,
                "attributes": {"latency": milliseconds, "jitter": 0},
            },
        ).raise_for_status()

    def remove_latency(self, name: str) -> None:
        self._http.delete(f"/proxies/{name}/toxics/slow").raise_for_status()


@pytest.fixture
def toxiproxy() -> Iterator[Toxiproxy]:
    api = Toxiproxy(os.environ.get("FEATUREFORGE_TOXIPROXY_URL", "http://127.0.0.1:8474"))
    api.wait_ready()
    upstream = os.environ.get("FEATUREFORGE_TOXIPROXY_UPSTREAM", "redis:6379")
    api.reset_proxy(PROXY, "0.0.0.0:16379", upstream)
    yield api
    api.delete_proxy(PROXY)


@pytest.fixture
def direct() -> Iterator[Redis]:
    url = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
    with Redis.from_url(url, socket_connect_timeout=3, socket_timeout=3) as client:
        assert client.ping() is True
        yield client


@pytest.fixture
def proxied(toxiproxy: Toxiproxy) -> Iterator[Redis]:
    url = os.environ.get("FEATUREFORGE_PROXIED_REDIS_URL", "redis://127.0.0.1:16379/0")
    with Redis.from_url(url, socket_connect_timeout=BUDGET, socket_timeout=BUDGET) as client:
        yield client


@pytest.fixture
def namespace(direct: Redis) -> Iterator[str]:
    value = f"featureforge-stream:integration:{uuid4().hex}"
    yield value
    keys = list(direct.scan_iter(match=f"{value}:*"))
    if keys:
        direct.delete(*keys)


def write(client: Redis, namespace: str, entity: str) -> None:
    counts = {a.name: 3 for a in SKIP.aggregations}
    data: dict[str, Any] = {
        "feature_schema_version": "v1",
        "feature_name": SKIP.name,
        SKIP.keys[0]: entity,
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - SKIP.window.size),
        **counts,
    }
    for name, expression in SKIP.derived.items():
        data[name] = evaluate_expression(expression, counts)
    store = RedisSpecStore(client, SKIP, key_prefix=f"{namespace}:v{SKIP.version}:{SKIP.name}")
    assert store.write(parse_feature_record(json.dumps(data), SKIP)) == "written"


def lookup(server: FeatureServer) -> tuple[FeatureEntry, float]:
    started = time.perf_counter()
    result = server.lookup([FeatureRef(SKIP.name)], [{"user_id": "u1"}], reference_time=REFERENCE)
    return result.entries[0], time.perf_counter() - started


def test_a_slow_redis_gives_a_timeout_within_the_budget_and_recovers(
    toxiproxy: Toxiproxy, direct: Redis, proxied: Redis, namespace: str
) -> None:
    write(direct, namespace, "u1")
    server = FeatureServer.for_redis(proxied, [SKIP], namespace=namespace)

    entry, _ = lookup(server)
    assert entry.status == "ok"

    toxiproxy.add_latency(PROXY, SLOW_MS)
    entry, elapsed = lookup(server)
    assert entry.status == "backend_error"
    assert entry.error_type == "TimeoutError"
    assert elapsed < MAX_ELAPSED < SLOW_MS / 1000

    toxiproxy.remove_latency(PROXY)
    entry, _ = lookup(server)
    assert entry.status == "ok"


def test_a_stopped_redis_gives_a_backend_error_fast_and_recovers(
    toxiproxy: Toxiproxy, direct: Redis, proxied: Redis, namespace: str
) -> None:
    write(direct, namespace, "u1")
    server = FeatureServer.for_redis(proxied, [SKIP], namespace=namespace)

    entry, _ = lookup(server)
    assert entry.status == "ok"

    toxiproxy.set_enabled(PROXY, False)
    entry, elapsed = lookup(server)
    assert entry.status == "backend_error"
    assert entry.error_type in {"ConnectionError", "TimeoutError"}
    assert elapsed < MAX_ELAPSED

    toxiproxy.set_enabled(PROXY, True)
    entry, _ = lookup(server)
    assert entry.status == "ok"


def test_over_http_a_slow_redis_is_a_200_or_a_503_with_the_fail_policy(
    toxiproxy: Toxiproxy, direct: Redis, proxied: Redis, namespace: str
) -> None:
    write(direct, namespace, "u1")
    failing = SKIP.model_copy(
        update={
            "online": SKIP.online.model_copy(
                update={"fallback": Fallback(policy=FallbackPolicy.FAIL)}
            )
        }
    )
    body = {
        "features": [{"name": SKIP.name}],
        "entities": [{"user_id": "u1"}],
        "reference_time": REFERENCE.isoformat(),
    }
    omitting = TestClient(create_app(FeatureServer.for_redis(proxied, [SKIP], namespace=namespace)))
    strict = TestClient(
        create_app(FeatureServer.for_redis(proxied, [failing], namespace=namespace))
    )

    toxiproxy.add_latency(PROXY, SLOW_MS)

    started = time.perf_counter()
    response = omitting.post("/v1/features/lookup", json=body)
    assert time.perf_counter() - started < MAX_ELAPSED
    assert response.status_code == 200
    assert response.json()["entries"][0]["status"] == "backend_error"

    started = time.perf_counter()
    response = strict.post("/v1/features/lookup", json=body)
    assert time.perf_counter() - started < MAX_ELAPSED
    assert response.status_code == 503
    assert response.json()["features"] == [SKIP.name]
