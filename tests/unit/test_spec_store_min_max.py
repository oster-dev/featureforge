"""Unit tests for the spec-driven Redis store with a minimum and a maximum component."""

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import MagicMock, Mock

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.count_vector import UpdateConflictError
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.spec_store import RedisSpecStore

ROOT = Path(__file__).resolve().parents[2]
SPEC = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_extremes_1h.yaml")
OBSERVATION = datetime(2026, 1, 1, 11, tzinfo=UTC)
KEY = "p:user000001"


def record(play_count, smallest, largest):
    data = {
        "feature_schema_version": "v1",
        "feature_name": SPEC.name,
        "user_id": "user000001",
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - SPEC.window.size),
        "play_count": play_count,
        "min_play_seconds": smallest,
        "max_play_seconds": largest,
    }
    return parse_feature_record(json.dumps(data), SPEC)


def fake_client(stored_payload=None):
    client = MagicMock()
    pipeline = client.pipeline.return_value.__enter__.return_value
    pipeline.get.return_value = stored_payload
    return client, pipeline


def test_the_store_accepts_a_spec_with_min_and_max_components() -> None:
    store = RedisSpecStore(Mock(), SPEC, key_prefix="p")

    assert store.key_for("user000001") == KEY


def test_first_write_keeps_missing_extremes_as_null_with_the_spec_ttl() -> None:
    client, pipeline = fake_client()
    incoming = record(0, None, None)
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    assert store.write(incoming) == "written"

    pipeline.set.assert_called_once_with(KEY, incoming.to_json(), ex=7200)
    payload = json.loads(incoming.to_json())
    assert payload["min_play_seconds"] is None
    assert payload["max_play_seconds"] is None


@pytest.mark.parametrize(
    ("stored", "incoming", "outcome"),
    [
        ((2, 50, 100), (3, 40, 120), "written"),
        ((2, 50, 100), (2, 50, 100), "unchanged"),
        ((3, 40, 120), (2, 50, 100), "stale"),
        ((0, None, None), (1, 100, 100), "written"),
        ((1, 100, 100), (0, None, None), "stale"),
    ],
)
def test_the_minimum_falls_and_the_maximum_rises_in_a_correction(
    stored: tuple, incoming: tuple, outcome: str
) -> None:
    client, pipeline = fake_client(record(*stored).to_json())
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    assert store.write(record(*incoming)) == outcome

    assert pipeline.set.called is (outcome == "written")


def test_a_count_that_rises_while_the_minimum_rises_is_a_conflict() -> None:
    client, _ = fake_client(record(2, 50, 100).to_json())
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    with pytest.raises(UpdateConflictError):
        store.write(record(3, 60, 100))


def test_read_returns_missing_extremes_as_none() -> None:
    client = MagicMock()
    client.get.return_value = record(0, None, None).to_json()
    store = RedisSpecStore(client, SPEC, key_prefix="p")

    stored = store.read("user000001")

    assert stored is not None
    assert dict(stored.counts) == {
        "play_count": 0,
        "min_play_seconds": None,
        "max_play_seconds": None,
    }
