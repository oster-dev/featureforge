"""Contract tests that every online store backend must pass (ADR-013).

Each test runs once per entry in BACKENDS. A new backend adds one entry and one branch
in the make_store fixture.
"""

import json
import os
import threading
import time
from collections.abc import Callable, Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.count_vector import UpdateConflictError
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.store_protocol import OnlineStore, create_online_store

pytestmark = pytest.mark.integration

BACKENDS = ["redis"]

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


def with_ttl(spec: FeatureSpec, seconds: int) -> FeatureSpec:
    online = spec.online.model_copy(update={"ttl": timedelta(seconds=seconds)})
    return spec.model_copy(update={"online": online})


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


@pytest.fixture(params=BACKENDS)
def make_store(
    request: pytest.FixtureRequest, redis_client: Redis
) -> Iterator[Callable[[FeatureSpec], OnlineStore]]:
    prefixes: list[str] = []

    def factory(spec: FeatureSpec) -> OnlineStore:
        if request.param == "redis":
            prefix = f"featureforge-stream:contract:{uuid4().hex}"
            prefixes.append(prefix)
            return create_online_store(spec, redis_client=redis_client, key_prefix=prefix)
        raise AssertionError(f"no store for backend {request.param}")

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


def test_a_record_for_another_feature_raises_and_leaves_the_store_empty(make_store):
    store = make_store(SKIP)

    with pytest.raises(ValueError):
        store.write(record(PLAY, "user_000001", {"play_count": 1}))
    assert store.read("user_000001") is None


def test_replaying_the_same_records_changes_nothing(make_store):
    store = make_store(PLAY)
    sequence = [
        record(PLAY, "user_000001", {"play_count": 1}),
        record(PLAY, "user_000001", {"play_count": 3}),
        record(PLAY, "user_000001", {"play_count": 2}),
    ]

    assert [store.write(item) for item in sequence] == ["written", "written", "stale"]
    after_first_run = store.read("user_000001")

    assert [store.write(item) for item in sequence] == ["stale", "unchanged", "stale"]
    assert store.read("user_000001") == after_first_run


def test_concurrent_writers_never_lose_the_highest_count(make_store):
    store = make_store(PLAY)
    counts = list(range(1, 9))
    outcomes: list[str] = []
    errors: list[BaseException] = []
    lock = threading.Lock()
    barrier = threading.Barrier(len(counts))

    def write(count: int) -> None:
        try:
            barrier.wait(timeout=5)
            outcome = store.write(record(PLAY, "user_000001", {"play_count": count}))
            with lock:
                outcomes.append(outcome)
        except BaseException as error:  # noqa: BLE001
            with lock:
                errors.append(error)

    threads = [threading.Thread(target=write, args=(count,)) for count in counts]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=30)

    assert not errors
    assert len(outcomes) == len(counts)
    assert set(outcomes) <= {"written", "stale"}
    assert "written" in outcomes
    assert store.read("user_000001").counts["play_count"] == max(counts)


def test_an_expired_value_reads_as_none(make_store):
    store = make_store(with_ttl(PLAY, 1))

    assert store.write(record(PLAY, "user_000001", {"play_count": 3})) == "written"
    assert store.read("user_000001") is not None
    time.sleep(1.5)
    assert store.read("user_000001") is None


def test_unchanged_and_stale_deliveries_do_not_extend_the_expiry(make_store):
    store = make_store(with_ttl(PLAY, 3))
    first = record(PLAY, "user_000001", {"play_count": 3})

    assert store.write(first) == "written"
    time.sleep(2)
    assert store.write(first) == "unchanged"
    assert store.write(record(PLAY, "user_000001", {"play_count": 2})) == "stale"
    time.sleep(1.5)
    assert store.read("user_000001") is None


def test_a_written_update_restarts_the_expiry(make_store):
    store = make_store(with_ttl(PLAY, 3))

    assert store.write(record(PLAY, "user_000001", {"play_count": 1})) == "written"
    time.sleep(2)
    assert store.write(record(PLAY, "user_000001", {"play_count": 2})) == "written"
    time.sleep(2)
    assert store.read("user_000001") is not None
