"""Unit tests for the spec-driven Redis store without a Redis server (ADR-009)."""

import dataclasses
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest
from redis.exceptions import WatchError

from featureforge.spec.loader import load_spec
from featureforge.spec.model import UpdatePolicy
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.count_vector import UpdateConflictError
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.online_store import RedisUserPlayCountStore
from featureforge.streaming.skip_rate_store import RedisUserSkipRateStore
from featureforge.streaming.spec_store import RedisSpecStore, UnsupportedPolicyError

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)

PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")
CONTENT = load_spec(SPECS / "content_play_count_1h.yaml")

KEY = "p:user_000001"


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


def fake_client(stored_payload=None):
    client = MagicMock()
    pipeline = client.pipeline.return_value.__enter__.return_value
    pipeline.get.return_value = stored_payload
    return client, pipeline


def test_default_keys_equal_the_hand_written_stores():
    play = RedisSpecStore(Mock(), PLAY).key_for("user_000001")
    skip = RedisSpecStore(Mock(), SKIP).key_for("user_000001")
    assert play == RedisUserPlayCountStore(Mock()).key_for("user_000001")
    assert skip == RedisUserSkipRateStore(Mock()).key_for("user_000001")


def test_content_key_follows_the_schema():
    key = RedisSpecStore(Mock(), CONTENT).key_for("content_000001")
    assert key == "featureforge-stream:online:v1:content_play_count_1h:content_000001"


def test_custom_prefix_is_used():
    assert RedisSpecStore(Mock(), PLAY, key_prefix="p").key_for("u") == "p:u"


@pytest.mark.parametrize("value", ["", "user 1", "user_1\n"])
def test_key_for_rejects_invalid_entity_values(value):
    with pytest.raises(ValueError):
        RedisSpecStore(Mock(), PLAY).key_for(value)


def test_key_for_rejects_non_strings():
    with pytest.raises(TypeError):
        RedisSpecStore(Mock(), PLAY).key_for(123)


@pytest.mark.parametrize("prefix", ["", "has space"])
def test_constructor_rejects_invalid_prefixes(prefix):
    with pytest.raises(ValueError):
        RedisSpecStore(Mock(), PLAY, key_prefix=prefix)


def test_constructor_rejects_non_string_prefix():
    with pytest.raises(TypeError):
        RedisSpecStore(Mock(), PLAY, key_prefix=5)


def test_constructor_rejects_window_replace():
    online = PLAY.online.model_copy(update={"update_policy": UpdatePolicy.WINDOW_REPLACE})
    spec = PLAY.model_copy(update={"online": online})
    with pytest.raises(UnsupportedPolicyError):
        RedisSpecStore(Mock(), spec)


def test_constructor_rejects_other_online_stores():
    online = PLAY.online.model_copy(update={"store": "cassandra"})
    spec = PLAY.model_copy(update={"online": online})
    with pytest.raises(ValueError, match="not redis"):
        RedisSpecStore(Mock(), spec)


def test_constructor_rejects_composite_entity_keys():
    spec = PLAY.model_copy(update={"entity": ["user_id", "content_id"]})
    with pytest.raises(ValueError, match="composite"):
        RedisSpecStore(Mock(), spec)


@pytest.mark.parametrize(
    ("spec", "counts", "ttl"),
    [
        (PLAY, {"play_count": 3}, 7200),
        (SKIP, {"play_count": 2, "skip_count": 1}, 90000),
    ],
)
def test_first_write_sets_the_value_with_the_spec_ttl(spec, counts, ttl):
    client, pipeline = fake_client()
    incoming = record(spec, "user_000001", counts)
    store = RedisSpecStore(client, spec, key_prefix="p")

    assert store.write(incoming) == "written"

    pipeline.watch.assert_called_once_with(KEY)
    pipeline.multi.assert_called_once_with()
    pipeline.set.assert_called_once_with(KEY, incoming.to_json(), ex=ttl)
    pipeline.execute.assert_called_once_with()


@pytest.mark.parametrize(
    ("counts", "minutes", "outcome"),
    [
        ({"play_count": 2}, 0, "stale"),
        ({"play_count": 3}, 0, "unchanged"),
        ({"play_count": 99}, -1, "stale"),
    ],
)
def test_unchanged_and_stale_writes_do_not_touch_the_value_or_the_ttl(counts, minutes, outcome):
    stored = record(PLAY, "user_000001", {"play_count": 3})
    client, pipeline = fake_client(stored.to_json())
    store = RedisSpecStore(client, PLAY, key_prefix="p")

    assert store.write(record(PLAY, "user_000001", counts, minutes=minutes)) == outcome

    pipeline.unwatch.assert_called_once_with()
    pipeline.multi.assert_not_called()
    pipeline.set.assert_not_called()


def test_newer_window_replaces_the_value_with_zero():
    stored = record(PLAY, "user_000001", {"play_count": 5})
    client, pipeline = fake_client(stored.to_json())
    incoming = record(PLAY, "user_000001", {"play_count": 0}, minutes=1)

    assert RedisSpecStore(client, PLAY, key_prefix="p").write(incoming) == "written"
    pipeline.set.assert_called_once_with(KEY, incoming.to_json(), ex=7200)


def test_incomparable_counts_raise_and_write_nothing():
    stored = record(SKIP, "user_000001", {"play_count": 1, "skip_count": 1})
    client, pipeline = fake_client(stored.to_json())
    store = RedisSpecStore(client, SKIP, key_prefix="p")

    with pytest.raises(UpdateConflictError, match="Incomparable"):
        store.write(record(SKIP, "user_000001", {"play_count": 2, "skip_count": 0}))
    pipeline.set.assert_not_called()


def test_watch_conflict_is_retried():
    client, pipeline = fake_client()
    pipeline.execute.side_effect = [WatchError(), None]
    store = RedisSpecStore(client, PLAY, key_prefix="p")

    assert store.write(record(PLAY, "user_000001", {"play_count": 1})) == "written"
    assert pipeline.execute.call_count == 2


def test_exhausted_retries_raise_after_sixteen_attempts():
    client, pipeline = fake_client()
    pipeline.execute.side_effect = WatchError()
    store = RedisSpecStore(client, PLAY, key_prefix="p")

    with pytest.raises(WatchError, match="16"):
        store.write(record(PLAY, "user_000001", {"play_count": 1}))
    assert pipeline.execute.call_count == 16


def test_invalid_record_is_rejected_before_redis_is_touched():
    client, _ = fake_client()
    incoming = dataclasses.replace(
        record(PLAY, "user_000001", {"play_count": 1}),
        feature_name="user_skip_rate_24h",
    )

    with pytest.raises(ValueError, match="feature_name"):
        RedisSpecStore(client, PLAY, key_prefix="p").write(incoming)
    client.pipeline.assert_not_called()


def test_read_returns_the_stored_record():
    stored = record(PLAY, "user_000001", {"play_count": 3})
    client = MagicMock()
    client.get.return_value = stored.to_json()

    assert RedisSpecStore(client, PLAY, key_prefix="p").read("user_000001") == stored
    client.get.assert_called_once_with(KEY)


def test_read_of_an_unknown_entity_is_none():
    client = MagicMock()
    client.get.return_value = None
    assert RedisSpecStore(client, PLAY, key_prefix="p").read("user_000001") is None


def test_read_rejects_a_record_of_another_entity():
    client = MagicMock()
    client.get.return_value = record(PLAY, "other_user", {"play_count": 1}).to_json()
    with pytest.raises(ValueError, match="does not match"):
        RedisSpecStore(client, PLAY, key_prefix="p").read("user_000001")
