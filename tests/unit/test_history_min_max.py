"""History columns, update identity and version ranking with min and max (ADR-017)."""

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import (
    component_directions,
    parse_feature_record,
)
from featureforge.streaming.history import history_arrow_schema, history_columns, update_id
from featureforge.streaming.published_as_of import _rank

SPEC_YAML = """\
name: play_seconds_extremes_history_test
version: 1
owner: test
description: History test.
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
offline: {store: iceberg, table: features.play_seconds_extremes_history_test}
freshness_slo: 5m
"""

COUNTERS = ("play_count", "min_play_seconds", "max_play_seconds")
MOMENT = datetime(2026, 1, 1, 11, tzinfo=UTC)


@pytest.fixture
def spec(tmp_path: Path):
    path = tmp_path / "spec.yaml"
    path.write_text(SPEC_YAML, encoding="utf-8")
    return load_spec(path)


def payload(**overrides: object) -> str:
    record: dict[str, object] = {
        "feature_schema_version": "v1",
        "feature_name": "play_seconds_extremes_history_test",
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


def row(count: int, smallest: int | None, largest: int | None) -> dict:
    return {
        "published_at": MOMENT,
        "observation_time": MOMENT,
        "play_count": count,
        "min_play_seconds": smallest,
        "max_play_seconds": largest,
        "update_id": "update",
    }


def rank(candidate: dict, spec) -> tuple:
    return _rank(candidate, COUNTERS, component_directions(spec))


def test_the_extreme_columns_can_be_null_and_the_count_cannot(spec) -> None:
    required = {column.name: column.required for column in history_columns(spec)}

    assert required["play_count"] is True
    assert required["min_play_seconds"] is False
    assert required["max_play_seconds"] is False
    assert required["spread"] is False
    schema = history_arrow_schema(spec)
    assert schema.field("min_play_seconds").nullable is True
    assert schema.field("play_count").nullable is False


def test_the_update_identity_of_a_missing_extreme_is_stable_and_distinct(spec) -> None:
    empty = payload(play_count=0, min_play_seconds=None, max_play_seconds=None, spread=None)

    first = update_id(spec, parse_feature_record(empty, spec))
    again = update_id(spec, parse_feature_record(empty, spec))
    full = update_id(spec, parse_feature_record(payload(), spec))

    assert first == again
    assert first != full


def test_a_missing_extreme_ranks_below_a_value(spec) -> None:
    assert rank(row(0, None, None), spec) < rank(row(1, 100, 100), spec)


def test_a_smaller_minimum_and_a_larger_maximum_rank_higher(spec) -> None:
    assert rank(row(2, 40, 100), spec) > rank(row(2, 50, 100), spec)
    assert rank(row(2, 50, 120), spec) > rank(row(2, 50, 100), spec)
