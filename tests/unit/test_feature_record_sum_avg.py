"""Feature records with a sum component and a derived average (ADR-017)."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.count_vector import (
    CountVector,
    UpdateConflictError,
    decide_count_vector_update,
)
from featureforge.streaming.contracts.feature_record import parse_feature_record

SPEC_YAML = """\
name: play_seconds_record_test
version: 1
owner: test
description: Record parser test.
entity: user_id
source: interaction-events.validated.v1
aggregations:
  - {name: play_count, op: count, filter: "event_type == 'play'"}
  - {name: play_seconds_sum, op: sum, column: play_seconds, filter: "event_type == 'play'"}
derived:
  avg_play_seconds: "play_seconds_sum / play_count if play_count > 0 else None"
window: {size: 1h, slide: 1m, boundary: right_closed}
lateness: {watermark_delay: 10m, allowed: 30m}
dedupe: {key: event_id, ttl: 24h}
online: {store: redis, ttl: 2h, update_policy: componentwise_monotonic}
offline: {store: iceberg, table: features.play_seconds_record_test}
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
        "feature_name": "play_seconds_record_test",
        "user_id": "user000001",
        "observation_time": "2026-01-01T11:00:00Z",
        "window_start_exclusive": "2026-01-01T10:00:00Z",
        "play_count": 3,
        "play_seconds_sum": 270,
        "avg_play_seconds": 90.0,
    }
    record.update(overrides)
    return json.dumps(record)


def vector(play_count: int, play_seconds_sum: int) -> CountVector:
    return CountVector(
        entity="user000001",
        observation_time=datetime(2026, 1, 1, 11, tzinfo=UTC),
        counts={"play_count": play_count, "play_seconds_sum": play_seconds_sum},
    )


def test_record_holds_the_sum_as_a_component_and_the_average_as_derived(spec) -> None:
    record = parse_feature_record(payload(), spec)

    assert dict(record.counts) == {"play_count": 3, "play_seconds_sum": 270}
    assert dict(record.derived) == {"avg_play_seconds": 90.0}
    assert dict(record.vector().counts) == {"play_count": 3, "play_seconds_sum": 270}


def test_empty_window_has_a_missing_average_that_survives_a_round_trip(spec) -> None:
    empty = payload(play_count=0, play_seconds_sum=0, avg_play_seconds=None)

    record = parse_feature_record(empty, spec)

    assert record.derived["avg_play_seconds"] is None
    assert parse_feature_record(record.to_json(), spec) == record


def test_missing_average_is_rejected_when_the_window_has_events(spec) -> None:
    with pytest.raises(ValueError, match="avg_play_seconds must be a number"):
        parse_feature_record(payload(avg_play_seconds=None), spec)


def test_average_of_an_empty_window_is_never_zero(spec) -> None:
    zero = payload(play_count=0, play_seconds_sum=0, avg_play_seconds=0.0)

    with pytest.raises(ValueError, match="must match its spec expression"):
        parse_feature_record(zero, spec)


def test_wrong_average_is_rejected(spec) -> None:
    with pytest.raises(ValueError, match="must match its spec expression"):
        parse_feature_record(payload(avg_play_seconds=91.0), spec)


@pytest.mark.parametrize("bad", [-1, 270.5, True, None, "270"])
def test_sum_must_be_a_non_negative_integer(spec, bad: object) -> None:
    with pytest.raises(ValueError, match="play_seconds_sum must be a non-negative integer"):
        parse_feature_record(payload(play_seconds_sum=bad), spec)


def test_a_late_event_raises_count_and_sum_and_counts_as_a_correction() -> None:
    assert decide_count_vector_update(vector(3, 270), vector(4, 350)) == "written"
    assert decide_count_vector_update(vector(3, 270), vector(3, 270)) == "unchanged"
    assert decide_count_vector_update(vector(4, 350), vector(3, 270)) == "stale"


def test_a_count_that_rises_with_a_falling_sum_cannot_be_ordered() -> None:
    with pytest.raises(UpdateConflictError):
        decide_count_vector_update(vector(3, 270), vector(4, 250))
