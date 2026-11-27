"""Real-Redis checks of the spec store with a sum component and a derived average (ADR-017)."""

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
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
SPEC = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_derived_1h.yaml")
OBSERVATION = datetime(2026, 1, 1, 11, tzinfo=UTC)
ENTITY = "user000001"


def record(play_count: int, play_seconds_sum: int):
    counts = {"play_count": play_count, "play_seconds_sum": play_seconds_sum}
    data = {
        "feature_schema_version": "v1",
        "feature_name": SPEC.name,
        "user_id": ENTITY,
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - SPEC.window.size),
        **counts,
        "avg_play_seconds": evaluate_expression(SPEC.derived["avg_play_seconds"], counts),
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

    assert store.write(record(3, 270)) == "written"
    assert 0 < redis_client.ttl(key) <= ttl

    redis_client.expire(key, 100)
    assert store.write(record(3, 270)) == "unchanged"
    assert store.write(record(2, 200)) == "stale"
    assert redis_client.ttl(key) <= 100

    assert store.write(record(4, 350)) == "written"
    assert redis_client.ttl(key) > 100


def test_a_missing_average_is_stored_as_null_and_read_back_as_none(make_store, redis_client):
    store = make_store()

    assert store.write(record(0, 0)) == "written"

    raw = json.loads(redis_client.get(store.key_for(ENTITY)))
    assert raw["avg_play_seconds"] is None
    stored = store.read(ENTITY)
    assert stored is not None
    assert stored.derived["avg_play_seconds"] is None


def test_a_correction_may_lower_the_average_and_a_replay_cannot_undo_it(make_store):
    store = make_store()

    assert store.write(record(2, 300)) == "written"
    assert store.write(record(3, 330)) == "written"

    stored = store.read(ENTITY)
    assert stored is not None
    assert stored.derived["avg_play_seconds"] == pytest.approx(110.0)

    assert store.write(record(2, 300)) == "stale"
    stored = store.read(ENTITY)
    assert stored is not None
    assert dict(stored.counts) == {"play_count": 3, "play_seconds_sum": 330}


def test_concurrent_writers_end_with_the_highest_vector(make_store):
    store = make_store()
    vectors = [(count, 10 * count) for count in range(1, 21)]
    errors: list[Exception] = []

    def worker(chunk: list[tuple[int, int]]) -> None:
        try:
            for count, total in chunk:
                store.write(record(count, total))
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
    assert dict(stored.counts) == {"play_count": 20, "play_seconds_sum": 200}
