"""Verify the spec-driven Redis store against a local Redis (ADR-009)."""

import json
import os
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.count_vector import UpdateConflictError
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)

PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")
CONTENT = load_spec(SPECS / "content_play_count_1h.yaml")


def record(spec, entity, counts, *, minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    data = {
        "feature_schema_version": "v1",
        "feature_name": spec.name,
        spec.keys[0]: entity,
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - spec.window.size),
        **counts,
    }
    for name, expression in spec.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return parse_feature_record(json.dumps(data), spec)


@pytest.fixture
def redis_client() -> Iterator[Redis]:
    url = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
    with Redis.from_url(
        url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        assert client.ping() is True
        yield client


@pytest.fixture
def make_store(redis_client: Redis) -> Iterator[Callable[..., RedisSpecStore]]:
    prefixes: list[str] = []

    def factory(spec):
        prefix = f"featureforge-stream:integration:{uuid4().hex}"
        prefixes.append(prefix)
        return RedisSpecStore(redis_client, spec, key_prefix=prefix)

    yield factory

    for prefix in prefixes:
        keys = list(redis_client.scan_iter(match=f"{prefix}:*"))
        if keys:
            redis_client.delete(*keys)


@pytest.mark.parametrize(
    ("spec", "entity", "counts"),
    [
        (PLAY, "user_000001", {"play_count": 3}),
        (SKIP, "user_000001", {"play_count": 2, "skip_count": 1}),
        (CONTENT, "content_000001", {"play_count": 4}),
    ],
)
def test_round_trip(make_store, spec, entity, counts):
    store = make_store(spec)
    incoming = record(spec, entity, counts)

    assert store.read(entity) is None
    assert store.write(incoming) == "written"
    assert store.read(entity) == incoming


def test_newer_window_replaces_the_value_with_zero(make_store):
    store = make_store(PLAY)
    newer = record(PLAY, "user_000001", {"play_count": 0}, minutes=1)

    assert store.write(record(PLAY, "user_000001", {"play_count": 3})) == "written"
    assert store.write(newer) == "written"
    assert store.read("user_000001") == newer


@pytest.mark.parametrize(
    ("incoming_count", "outcome", "stored_count"),
    [(2, "stale", 3), (3, "unchanged", 3), (4, "written", 4)],
)
def test_same_window_corrections_follow_the_policy(
    make_store, incoming_count, outcome, stored_count
):
    store = make_store(PLAY)
    assert store.write(record(PLAY, "user_000001", {"play_count": 3})) == "written"

    result = store.write(record(PLAY, "user_000001", {"play_count": incoming_count}))

    assert result == outcome
    assert store.read("user_000001").counts["play_count"] == stored_count


def test_incomparable_skip_rate_counts_raise_and_keep_the_value(make_store):
    store = make_store(SKIP)
    stored = record(SKIP, "user_000001", {"play_count": 1, "skip_count": 1})
    assert store.write(stored) == "written"

    with pytest.raises(UpdateConflictError, match="Incomparable"):
        store.write(record(SKIP, "user_000001", {"play_count": 2, "skip_count": 0}))
    assert store.read("user_000001") == stored


def test_ttl_is_set_and_only_written_updates_refresh_it(make_store, redis_client):
    store = make_store(PLAY)
    key = store.key_for("user_000001")
    ttl = int(PLAY.online.ttl.total_seconds())

    assert store.write(record(PLAY, "user_000001", {"play_count": 3})) == "written"
    assert 0 < redis_client.ttl(key) <= ttl

    redis_client.expire(key, 100)
    assert store.write(record(PLAY, "user_000001", {"play_count": 3})) == "unchanged"
    assert redis_client.ttl(key) <= 100
    assert store.write(record(PLAY, "user_000001", {"play_count": 2})) == "stale"
    assert redis_client.ttl(key) <= 100

    assert store.write(record(PLAY, "user_000001", {"play_count": 4})) == "written"
    assert redis_client.ttl(key) > 100
