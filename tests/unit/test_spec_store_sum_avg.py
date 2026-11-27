"""Unit tests for the spec-driven Redis store with a sum component and a derived average."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.count_vector import UpdateConflictError
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.spec_store import RedisSpecStore

ROOT = Path(__file__).resolve().parents[2]
SPEC = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_derived_1h.yaml")
OBSERVATION = datetime(2026, 1, 1, 11, tzinfo=UTC)
KEY = "p:user000001"


def record(play_count: int, play_seconds_sum: int):
    counts = {"play_count": play_count, "play_seconds_sum": play_seconds_sum}
    data = {
        "feature_schema_version": "v1",
        "feature_name": SPEC.name,
        "user_id": "user000001",
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - SPEC.window.size),
        **counts,
        "avg_play_seconds": evaluate_expression(SPEC.derived["avg_play_seconds"], counts),
    }
    return parse_feature_record(json.dumps(data), SPEC)


def fake_client(stored_payload=None):
    client = MagicMock()
    pipeline = client.pipeline.return_value.__enter__.return_value
    pipeline.get.return_value = stored_payload
    return client, pipeline


def test_the_store_accepts_a_spec_with_a_sum_component() -> None:
    store = RedisSpecStore(Mock(), SPEC, key_prefix="p")

    assert store.key_for("user000001") == KEY


def test_first_write_keeps_a_missing_average_as_null_with_the_spec_ttl() -> None:
    client, pipeline = fake_client()
    incoming = record(0, 0)
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    assert store.write(incoming) == "written"

    pipeline.set.assert_called_once_with(KEY, incoming.to_json(), ex=7200)
    assert json.loads(incoming.to_json())["avg_play_seconds"] is None


@pytest.mark.parametrize(
    ("stored", "incoming", "outcome"),
    [
        ((3, 270), (4, 350), "written"),
        ((3, 270), (3, 270), "unchanged"),
        ((4, 350), (3, 270), "stale"),
        ((0, 0), (1, 100), "written"),
    ],
)
def test_count_and_sum_order_same_window_updates(
    stored: tuple[int, int], incoming: tuple[int, int], outcome: str
) -> None:
    client, pipeline = fake_client(record(*stored).to_json())
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    assert store.write(record(*incoming)) == outcome

    assert pipeline.set.called is (outcome == "written")


def test_a_count_that_rises_while_the_sum_falls_is_a_conflict() -> None:
    client, _ = fake_client(record(3, 270).to_json())
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    with pytest.raises(UpdateConflictError):
        store.write(record(4, 250))


def test_read_returns_the_missing_average_as_none() -> None:
    client = MagicMock()
    client.get.return_value = record(0, 0).to_json()
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    stored = store.read("user000001")

    assert stored is not None
    assert stored.derived["avg_play_seconds"] is None
    assert dict(stored.counts) == {"play_count": 0, "play_seconds_sum": 0}
