"""Real-Redis checks of the spec store with a minimum and a maximum component (ADR-017)."""

import json
import os
import threading
from collections.abc import Callable, Iterator
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import pytest
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.count_vector import UpdateConflictError
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
SPEC = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_extremes_1h.yaml")
OBSERVATION = datetime(2026, 1, 1, 11, tzinfo=UTC)
ENTITY = "user000001"


def record(play_count, smallest, largest):
    data = {
        "feature_schema_version": "v1",
        "feature_name": SPEC.name,
        "user_id": ENTITY,
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - SPEC.window.size),
        "play_count": play_count,
        "min_play_seconds": smallest,
        "max_play_seconds": largest,
    }
    return parse_feature_record(json.dumps(data), SPEC)


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
def make_store(redis_client: Redis) -> Iterator[Callable[[], RedisSpecStore]]:
    prefixes: list[str] = []

    def factory() -> RedisSpecStore:
        prefix = f"featureforge-stream:integration:{uuid4().hex}"
        prefixes.append(prefix)
        return RedisSpecStore(redis_client, SPEC, key_prefix=prefix)

    yield factory

    for prefix in prefixes:
        keys = list(redis_client.scan_iter(match=f"{prefix}:*"))
        if keys:
            redis_client.delete(*keys)


def test_ttl_is_set_and_only_written_updates_refresh_it(make_store, redis_client):
    store = make_store()
    key = store.key_for(ENTITY)
    ttl = int(SPEC.online.ttl.total_seconds())

    assert store.write(record(3, 60, 120)) == "written"
    assert 0 < redis_client.ttl(key) <= ttl

    redis_client.expire(key, 100)
    assert store.write(record(3, 60, 120)) == "unchanged"
    assert store.write(record(2, 100, 120)) == "stale"
    assert redis_client.ttl(key) <= 100

    assert store.write(record(4, 50, 120)) == "written"
    assert redis_client.ttl(key) > 100


def test_missing_extremes_are_stored_as_null_and_read_back_as_none(make_store, redis_client):
    store = make_store()

    assert store.write(record(0, None, None)) == "written"

    raw = json.loads(redis_client.get(store.key_for(ENTITY)))
    assert raw["min_play_seconds"] is None
    assert raw["max_play_seconds"] is None
    stored = store.read(ENTITY)
    assert stored is not None
    assert stored.counts["min_play_seconds"] is None
    assert stored.counts["max_play_seconds"] is None


def test_a_correction_lowers_the_minimum_and_a_replay_cannot_undo_it(make_store):
    store = make_store()

    assert store.write(record(0, None, None)) == "written"
    assert store.write(record(2, 100, 120)) == "written"
    assert store.write(record(3, 60, 120)) == "written"

    stored = store.read(ENTITY)
    assert stored is not None
    assert dict(stored.counts) == {
        "play_count": 3,
        "min_play_seconds": 60,
        "max_play_seconds": 120,
    }

    assert store.write(record(2, 100, 120)) == "stale"
    assert store.write(record(0, None, None)) == "stale"
    again = store.read(ENTITY)
    assert again is not None
    assert dict(again.counts) == dict(stored.counts)


def test_a_rising_minimum_with_a_rising_count_raises_and_keeps_the_value(make_store):
    store = make_store()
    stored = record(2, 50, 100)
    assert store.write(stored) == "written"

    with pytest.raises(UpdateConflictError, match="Incomparable"):
        store.write(record(3, 60, 100))
    assert store.read(ENTITY) == stored


def test_concurrent_writers_end_with_the_most_advanced_vector(make_store):
    store = make_store()
    vectors = [(count, 100 - count, 100 + count) for count in range(1, 21)]
    errors: list[Exception] = []

    def worker(chunk: list[tuple[int, int, int]]) -> None:
        try:
            for play_count, smallest, largest in chunk:
                store.write(record(play_count, smallest, largest))
        except Exception as error:  # noqa: BLE001
            errors.append(error)

    threads = [threading.Thread(target=worker, args=(vectors[i::4],)) for i in range(4)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)

    assert not errors, errors
    stored = store.read(ENTITY)
    assert stored is not None
    assert dict(stored.counts) == {
        "play_count": 20,
        "min_play_seconds": 80,
        "max_play_seconds": 120,
    }
