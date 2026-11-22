"""Verify the feature serving core against a local Redis (ADR-016)."""

import json
import os
import time
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

import pytest
from redis import Redis

from featureforge.feature_service import FeatureRef, FeatureServer
from featureforge.service_api import build_server
from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import (
    FeatureRecord,
    parse_feature_record,
    stamp,
)
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)

PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")


@pytest.fixture
def redis_url() -> str:
    return os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")


@pytest.fixture
def client(redis_url: str) -> Iterator[Redis]:
    with Redis.from_url(redis_url, socket_connect_timeout=3, socket_timeout=3) as redis_client:
        assert redis_client.ping() is True
        yield redis_client


@pytest.fixture
def namespace(client: Redis) -> Iterator[str]:
    value = f"featureforge-stream:integration:{uuid4().hex}"
    yield value
    keys = list(client.scan_iter(match=f"{value}:*"))
    if keys:
        client.delete(*keys)


def key_prefix(namespace: str, spec: FeatureSpec) -> str:
    return f"{namespace}:v{spec.version}:{spec.name}"


def make_record(spec: FeatureSpec, entity: str, when: datetime, base: int = 3) -> FeatureRecord:
    counts = {a.name: base for a in spec.aggregations}
    data: dict[str, Any] = {
        "feature_schema_version": "v1",
        "feature_name": spec.name,
        spec.keys[0]: entity,
        "observation_time": stamp(when),
        "window_start_exclusive": stamp(when - spec.window.size),
        **counts,
    }
    for name, expression in spec.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return parse_feature_record(json.dumps(data), spec)


def write(client: Redis, namespace: str, spec: FeatureSpec, entity: str, when: datetime) -> None:
    store = RedisSpecStore(client, spec, key_prefix=key_prefix(namespace, spec))
    assert store.write(make_record(spec, entity, when)) == "written"


def command_calls(client: Redis, name: str) -> int:
    stats = client.info("commandstats")
    entry = stats.get(f"cmdstat_{name}")
    return int(entry["calls"]) if entry else 0


def fresh_reference(spec: FeatureSpec) -> datetime:
    return OBSERVATION + spec.freshness_slo / 2


def test_a_written_record_is_served_as_ok(client: Redis, namespace: str) -> None:
    write(client, namespace, SKIP, "u1", OBSERVATION)
    server = FeatureServer.for_redis(client, [SKIP], namespace=namespace)
    result = server.lookup(
        [FeatureRef(SKIP.name)], [{"user_id": "u1"}], reference_time=fresh_reference(SKIP)
    )
    (entry,) = result.entries
    assert entry.status == "ok"
    assert entry.freshness == "fresh"
    assert entry.values == {"play_count": 3, "skip_count": 3, "skip_rate": 0.5}


def test_an_unknown_entity_is_missing(client: Redis, namespace: str) -> None:
    server = FeatureServer.for_redis(client, [SKIP], namespace=namespace)
    result = server.lookup(
        [FeatureRef(SKIP.name)], [{"user_id": "nobody"}], reference_time=fresh_reference(SKIP)
    )
    assert result.entries[0].status == "missing"


def test_an_old_record_is_stale_and_keeps_its_value(client: Redis, namespace: str) -> None:
    write(client, namespace, SKIP, "u1", OBSERVATION)
    server = FeatureServer.for_redis(client, [SKIP], namespace=namespace)
    late = OBSERVATION + SKIP.freshness_slo + timedelta(minutes=1)
    (entry,) = server.lookup(
        [FeatureRef(SKIP.name)], [{"user_id": "u1"}], reference_time=late
    ).entries
    assert entry.status == "stale"
    assert entry.values is not None


def test_an_expired_key_is_missing(client: Redis, namespace: str) -> None:
    write(client, namespace, SKIP, "u1", OBSERVATION)
    client.pexpire(f"{key_prefix(namespace, SKIP)}:u1", 1)
    time.sleep(0.05)
    server = FeatureServer.for_redis(client, [SKIP], namespace=namespace)
    result = server.lookup(
        [FeatureRef(SKIP.name)], [{"user_id": "u1"}], reference_time=fresh_reference(SKIP)
    )
    assert result.entries[0].status == "missing"


def test_a_corrupt_value_fails_only_its_own_entry(client: Redis, namespace: str) -> None:
    write(client, namespace, SKIP, "good", OBSERVATION)
    client.set(f"{key_prefix(namespace, SKIP)}:bad", "{not json")
    server = FeatureServer.for_redis(client, [SKIP], namespace=namespace)
    bad, good = server.lookup(
        [FeatureRef(SKIP.name)],
        [{"user_id": "bad"}, {"user_id": "good"}],
        reference_time=fresh_reference(SKIP),
    ).entries
    assert bad.status == "backend_error"
    assert good.status == "ok"


def test_a_request_makes_one_mget_and_no_get(client: Redis, namespace: str) -> None:
    for entity in ("u1", "u2"):
        write(client, namespace, PLAY, entity, OBSERVATION)
        write(client, namespace, SKIP, entity, OBSERVATION)
    server = FeatureServer.for_redis(client, [PLAY, SKIP], namespace=namespace)
    mget_before = command_calls(client, "mget")
    get_before = command_calls(client, "get")
    result = server.lookup(
        [FeatureRef(PLAY.name), FeatureRef(SKIP.name)],
        [{"user_id": "u1"}, {"user_id": "u2"}],
        reference_time=fresh_reference(SKIP),
    )
    assert len(result.entries) == 4
    assert command_calls(client, "mget") - mget_before == 1
    assert command_calls(client, "get") - get_before == 0


def test_the_default_namespace_matches_the_key_the_store_writes(client: Redis) -> None:
    entity = f"it-{uuid4().hex}"
    store = RedisSpecStore(client, SKIP)
    key = store.key_for(entity)
    try:
        assert store.write(make_record(SKIP, entity, OBSERVATION)) == "written"
        result = FeatureServer.for_redis(client, [SKIP]).lookup(
            [FeatureRef(SKIP.name)], [{"user_id": entity}], reference_time=fresh_reference(SKIP)
        )
        assert result.entries[0].status == "ok"
    finally:
        client.delete(key)


def test_the_builder_loads_every_real_spec_and_reads_a_real_redis(redis_url: str) -> None:
    server = build_server(SPECS, redis_url, 1000.0)
    assert server.ping() is True
    entity = f"nobody-{uuid4().hex}"
    names = [path.stem for path in sorted(SPECS.glob("*.yaml"))]
    result = server.lookup(
        [FeatureRef(name) for name in names],
        [{"user_id": entity, "content_id": entity}],
    )
    assert [entry.feature for entry in result.entries] == names
    assert {entry.status for entry in result.entries} == {"missing"}


def test_an_unreachable_redis_gives_backend_errors_and_hides_the_address() -> None:
    dead = Redis.from_url("redis://127.0.0.1:1/0", socket_connect_timeout=0.2, socket_timeout=0.2)
    server = FeatureServer.for_redis(dead, [SKIP])
    started = time.perf_counter()
    result = server.lookup(
        [FeatureRef(SKIP.name)], [{"user_id": "u1"}], reference_time=fresh_reference(SKIP)
    )
    elapsed = time.perf_counter() - started
    (entry,) = result.entries
    assert entry.status == "backend_error"
    assert entry.error_type in {"ConnectionError", "TimeoutError"}
    assert "127.0.0.1" not in json.dumps(result.to_dict())
    assert elapsed < 2
