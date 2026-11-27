import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.spec.flink_plan import (
    EPOCH,
    UnsupportedSpecError,
    accumulate,
    build_flink_plan,
    feature_record,
    late_event_record,
    merge,
    utc_timestamp,
)
from featureforge.spec.loader import load_spec
from featureforge.spec.model import Aggregation, Op
from featureforge.streaming.contracts.feature_record import record_fields

ROOT = Path(__file__).resolve().parents[2]
SPECS = ROOT / "feature_repo" / "specs"
FIXTURE = ROOT / "fixtures" / "streaming" / "user_skip_rate_24h_v1.jsonl"
HOUR_MS = 3_600_000
PLAY_SECONDS = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_1h.yaml")
PLAY_SECONDS_EVENTS = ROOT / "fixtures" / "streaming" / "user_play_seconds_1h_v1.jsonl"


def at(day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, minute, second, tzinfo=UTC)


def to_ms(moment: datetime) -> int:
    return (moment - EPOCH) // timedelta(milliseconds=1)


@pytest.fixture(scope="module")
def skip_spec():
    return load_spec(SPECS / "user_skip_rate_24h.yaml")


@pytest.fixture(scope="module")
def skip_plan(skip_spec):
    return build_flink_plan(skip_spec)


@pytest.mark.parametrize(
    ("spec_file", "window_hours", "dedupe_hours", "counters", "derived"),
    [
        ("user_play_count_1h.yaml", 1, 24, ("play_count",), ()),
        ("user_skip_rate_24h.yaml", 24, 48, ("play_count", "skip_count"), ("skip_rate",)),
    ],
)
def test_plan_for_shipped_specs(spec_file, window_hours, dedupe_hours, counters, derived):
    plan = build_flink_plan(load_spec(SPECS / spec_file))
    assert plan.entity_key == "user_id"
    assert plan.dedupe_key == "event_id"
    assert plan.window_size_ms == window_hours * HOUR_MS
    assert plan.window_slide_ms == 60_000
    assert plan.watermark_delay_ms == 600_000
    assert plan.allowed_lateness_ms == 1_800_000
    assert plan.dedupe_ttl_ms == dedupe_hours * HOUR_MS
    assert tuple(a.name for a in plan.aggregations) == counters
    assert tuple(name for name, _ in plan.derived) == derived
    assert plan.zero == (0,) * len(counters)


def test_composite_key_is_rejected(skip_spec):
    spec = skip_spec.model_copy(update={"entity": ["user_id", "content_id"]})
    with pytest.raises(UnsupportedSpecError, match="composite"):
        build_flink_plan(spec)


def test_an_operator_beyond_sum_and_avg_is_rejected(skip_spec):
    smallest = Aggregation(name="min_play_seconds", op=Op.MIN, column="play_seconds")
    spec = skip_spec.model_copy(update={"aggregations": [smallest]})
    with pytest.raises(UnsupportedSpecError, match="min_play_seconds"):
        build_flink_plan(spec)


def golden_record(plan, user_id, observation, fixture=FIXTURE):
    end = to_ms(observation)
    start = end - plan.window_size_ms
    seen: set[str] = set()
    accumulator = plan.zero
    for line in fixture.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        identity = str(payload[plan.dedupe_key])
        if identity in seen:
            continue
        seen.add(identity)
        timestamp = to_ms(datetime.fromisoformat(payload["event_time"]))
        if payload[plan.entity_key] == user_id and start < timestamp <= end:
            accumulator = accumulate(plan, payload, accumulator)
    return json.loads(feature_record(plan, user_id, start, end, accumulator))


@pytest.mark.parametrize(
    ("user_id", "observation", "plays", "skips", "rate"),
    [
        ("user000001", at(2, 12), 2, 2, 0.5),
        ("user000001", at(2, 12, 0, 1), 1, 3, 0.75),
        ("user000001", at(2, 18), 1, 2, 2 / 3),
        ("user000001", at(3, 12, 0, 1), 0, 0, 0.0),
        ("user000002", at(2, 12), 0, 1, 1.0),
    ],
)
def test_plan_reproduces_golden_values(skip_plan, user_id, observation, plays, skips, rate):
    record = golden_record(skip_plan, user_id, observation)
    assert record["user_id"] == user_id
    assert record["play_count"] == plays
    assert record["skip_count"] == skips
    assert record["skip_rate"] == pytest.approx(rate)
    assert record["observation_time"] == observation.isoformat().replace("+00:00", "Z")
    start = observation - timedelta(hours=24)
    assert record["window_start_exclusive"] == start.isoformat().replace("+00:00", "Z")


def test_record_has_the_hand_written_job_fields(skip_plan):
    record = golden_record(skip_plan, "user000001", at(2, 12))
    assert set(record) == {
        "feature_schema_version",
        "feature_name",
        "user_id",
        "observation_time",
        "window_start_exclusive",
        "play_count",
        "skip_count",
        "skip_rate",
    }
    assert record["feature_schema_version"] == "v1"
    assert record["feature_name"] == "user_skip_rate_24h"


def test_accumulate_counts_only_matching_events_and_merge_adds(skip_plan):
    accumulator = skip_plan.zero
    for event_type in ("play", "skip", "impression", "complete", "play"):
        accumulator = accumulate(skip_plan, {"event_type": event_type}, accumulator)
    assert accumulator == (2, 1)
    assert merge((2, 1), (1, 3)) == (3, 4)


def test_utc_timestamp_matches_the_hand_written_format():
    assert utc_timestamp(0) == "1970-01-01T00:00:00Z"
    assert utc_timestamp(1_767_268_800_000) == "2026-01-01T12:00:00Z"


def test_late_event_record_names_feature_and_keeps_payload(skip_plan):
    record = json.loads(late_event_record(skip_plan, '{"event_id":"x"}'))
    assert record == {
        "error_schema_version": "v1",
        "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
        "feature_name": "user_skip_rate_24h",
        "raw_payload": '{"event_id":"x"}',
    }


@pytest.fixture(scope="module")
def play_seconds_plan():
    return build_flink_plan(PLAY_SECONDS)


def test_sum_and_avg_take_one_and_two_slots(play_seconds_plan):
    assert [(a.name, a.op, a.slot) for a in play_seconds_plan.aggregations] == [
        ("play_count", Op.COUNT, 0),
        ("play_seconds_sum", Op.SUM, 1),
        ("avg_play_seconds", Op.AVG, 2),
    ]
    assert play_seconds_plan.slots == 4
    assert play_seconds_plan.zero == (0, 0, 0, 0)


@pytest.mark.parametrize(
    ("user_id", "observation", "plays", "seconds", "average"),
    [
        ("user000001", at(1, 11), 3, 270, 90),
        ("user000002", at(1, 11), 1, 200, 200),
        ("user000003", at(1, 11), 0, 0, None),
        ("user000001", at(1, 11, 30), 2, 590, 295),
        ("user000002", at(1, 11, 30), 0, 0, None),
    ],
)
def test_plan_reproduces_the_sum_and_avg_golden_values(
    play_seconds_plan, user_id, observation, plays, seconds, average
):
    record = golden_record(play_seconds_plan, user_id, observation, PLAY_SECONDS_EVENTS)
    assert record["play_count"] == plays
    assert record["play_seconds_sum"] == seconds
    assert record["avg_play_seconds"] == average


def test_accumulate_sums_the_column_and_merge_adds_every_slot(play_seconds_plan):
    accumulator = play_seconds_plan.zero
    for event_type, seconds in (("play", 100), ("skip", 30), ("play", 50)):
        event = {"event_type": event_type, "play_seconds": seconds}
        accumulator = accumulate(play_seconds_plan, event, accumulator)
    assert accumulator == (2, 150, 150, 2)
    assert merge((1, 10, 10, 1), (2, 20, 30, 2)) == (3, 30, 40, 3)


def test_a_derived_value_with_a_missing_input_is_missing():
    spec = PLAY_SECONDS.model_copy(update={"derived": {"double_avg": "avg_play_seconds * 2"}})
    plan = build_flink_plan(spec)
    empty = json.loads(feature_record(plan, "user000003", 0, HOUR_MS, plan.zero))
    full = json.loads(feature_record(plan, "user000001", 0, HOUR_MS, (3, 270, 270, 3)))
    assert empty["avg_play_seconds"] is None
    assert empty["double_avg"] is None
    assert full["double_avg"] == 180


def test_the_online_path_still_refuses_sum_and_avg_records():
    with pytest.raises(ValueError, match="only count aggregations are supported"):
        record_fields(PLAY_SECONDS)
