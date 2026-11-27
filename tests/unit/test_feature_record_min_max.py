"""Feature records with a minimum and a maximum as components (ADR-017)."""

import json
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import parse_feature_record

SPEC_YAML = """\
name: play_seconds_extremes_record_test
version: 1
owner: test
description: Record parser test.
entity: user_id
source: interaction-events.validated.v1
aggregations:
  - {name: play_count, op: count, filter: "event_type == 'play'"}
  - {name: min_play_seconds, op: min, column: play_seconds, filter: "event_type == 'play'"}
  - {name: max_play_seconds, op: max, column: play_seconds, filter: "event_type == 'play'"}
derived:
  spread: "max_play_seconds - min_play_seconds"
window: {size: 1h, slide: 1m, boundary: right_closed}
lateness: {watermark_delay: 10m, allowed: 30m}
dedupe: {key: event_id, ttl: 24h}
online: {store: redis, ttl: 2h, update_policy: componentwise_monotonic}
offline: {store: iceberg, table: features.play_seconds_extremes_record_test}
freshness_slo: 5m
"""


@pytest.fixture
def spec(tmp_path: Path):
    path = tmp_path / "spec.yaml"
    path.write_text(SPEC_YAML, encoding="utf-8")
    return load_spec(path)


def payload(**overrides: object) -> str:
    record: dict[str, object] = {
        "feature_schema_version": "v1",
        "feature_name": "play_seconds_extremes_record_test",
        "user_id": "user000001",
        "observation_time": "2026-01-01T11:00:00Z",
        "window_start_exclusive": "2026-01-01T10:00:00Z",
        "play_count": 2,
        "min_play_seconds": 100,
        "max_play_seconds": 120,
        "spread": 20,
    }
    record.update(overrides)
    return json.dumps(record)


def test_record_holds_min_and_max_as_components_with_their_directions(spec) -> None:
    record = parse_feature_record(payload(), spec)

    assert dict(record.counts) == {
        "play_count": 2,
        "min_play_seconds": 100,
        "max_play_seconds": 120,
    }
    assert dict(record.derived) == {"spread": 20}
    assert dict(record.directions) == {
        "play_count": "up",
        "min_play_seconds": "down",
        "max_play_seconds": "up",
    }
    assert dict(record.vector().directions) == dict(record.directions)


def test_an_empty_window_has_missing_extremes_that_survive_a_round_trip(spec) -> None:
    empty = payload(play_count=0, min_play_seconds=None, max_play_seconds=None, spread=None)

    record = parse_feature_record(empty, spec)

    assert record.counts["min_play_seconds"] is None
    assert record.counts["max_play_seconds"] is None
    assert record.derived["spread"] is None
    assert parse_feature_record(record.to_json(), spec) == record


def test_an_extreme_may_be_negative_because_the_sign_is_not_restricted(spec) -> None:
    record = parse_feature_record(payload(min_play_seconds=-5, spread=125), spec)

    assert record.counts["min_play_seconds"] == -5


def test_a_derived_value_over_a_missing_extreme_must_be_missing(spec) -> None:
    zero = payload(play_count=0, min_play_seconds=None, max_play_seconds=None, spread=0)

    with pytest.raises(ValueError, match="spread must match its spec expression"):
        parse_feature_record(zero, spec)


def test_a_missing_derived_value_is_rejected_when_the_inputs_are_present(spec) -> None:
    with pytest.raises(ValueError, match="spread must be a number"):
        parse_feature_record(payload(spread=None), spec)


@pytest.mark.parametrize("bad", [100.5, True, "100"])
def test_an_extreme_must_be_an_integer_or_null(spec, bad: object) -> None:
    with pytest.raises(ValueError, match="min_play_seconds must be an integer or null"):
        parse_feature_record(payload(min_play_seconds=bad), spec)


def test_a_count_still_cannot_be_null_or_negative(spec) -> None:
    with pytest.raises(ValueError, match="play_count must be a non-negative integer"):
        parse_feature_record(payload(play_count=None), spec)
