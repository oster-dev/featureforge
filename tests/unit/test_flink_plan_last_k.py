"""The Flink plan computes last_k with a buffer slot and counts the events seen (ADR-017)."""

import json
import pickle
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.spec.flink_plan import (
    UnsupportedSpecError,
    accumulate,
    aggregate_values,
    build_flink_plan,
    feature_record,
    merge,
)
from featureforge.spec.loader import load_spec
from featureforge.spec.model import Aggregation, Op

ROOT = Path(__file__).resolve().parents[2]
SPEC = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_last2_1h.yaml")
PLAN = build_flink_plan(SPEC)
EVENTS = [
    json.loads(line)
    for line in (ROOT / "fixtures" / "streaming" / "user_play_seconds_1h_v1.jsonl")
    .read_text(encoding="utf-8")
    .splitlines()
    if line.strip()
]
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
HOUR = timedelta(hours=1)


def at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, second, tzinfo=UTC)


def millis(moment: datetime) -> int:
    return (moment - EPOCH) // timedelta(milliseconds=1)


def window_events(user_id: str, observation: datetime) -> list[dict]:
    start = observation - HOUR
    seen: set[str] = set()
    chosen = []
    for event in EVENTS:
        if event["user_id"] != user_id or event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        if start < datetime.fromisoformat(event["event_time"]) <= observation:
            chosen.append(event)
    return chosen


def fold(events: list[dict]) -> tuple:
    accumulator = PLAN.zero
    for event in events:
        accumulator = accumulate(PLAN, event, accumulator)
    return accumulator


def window_record(user_id: str, observation: datetime) -> dict:
    accumulator = fold(window_events(user_id, observation))
    start = observation - HOUR
    return json.loads(
        feature_record(PLAN, user_id, millis(start), millis(observation), accumulator)
    )


def test_last_k_takes_one_buffer_slot_and_the_events_seen_one_more():
    assert [(a.name, a.op, a.slot, a.width, a.k) for a in PLAN.aggregations] == [
        ("play_count", Op.COUNT, 0, 1, None),
        ("last_play_seconds", Op.LAST_K, 1, 1, 2),
    ]
    assert PLAN.events_seen_slot == 2
    assert PLAN.slots == 3
    assert PLAN.zero == (0, (), 0)
    assert PLAN.buffered


@pytest.mark.parametrize(
    ("user_id", "observation", "plays", "last", "seen"),
    [
        ("user000001", at(10), 1, [100], 1),
        ("user000001", at(10, 1), 2, [100, 120], 2),
        ("user000001", at(10, 30), 3, [120, 60], 3),
        ("user000001", at(10, 45), 3, [120, 60], 4),
        ("user000001", at(11), 3, [60, 90], 4),
        ("user000001", at(11, 0, 1), 3, [90, 500], 4),
        ("user000001", at(11, 30), 2, [90, 500], 3),
        ("user000001", at(12), 1, [500], 1),
        ("user000001", at(12, 0, 1), 0, [], 0),
        ("user000002", at(11), 1, [200], 1),
        ("user000002", at(11, 15), 0, [], 0),
        ("user000003", at(11), 0, [], 1),
    ],
)
def test_plan_matches_hand_calculated_windows(user_id, observation, plays, last, seen):
    record = window_record(user_id, observation)

    assert record["play_count"] == plays
    assert record["last_play_seconds"] == last
    assert record["events_seen"] == seen
    assert set(record) == {
        "feature_schema_version",
        "feature_name",
        "user_id",
        "observation_time",
        "window_start_exclusive",
        "play_count",
        "last_play_seconds",
        "events_seen",
    }


def test_a_filtered_event_counts_as_seen_but_adds_no_value():
    skip = next(e for e in EVENTS if e["event_type"] == "skip" and e["user_id"] == "user000001")

    assert accumulate(PLAN, skip, PLAN.zero) == (0, (), 1)


def test_equal_event_times_are_ordered_by_event_id():
    state = PLAN.zero
    for suffix, seconds in (("a3", 30), ("a1", 20), ("a2", 10)):
        event = {
            **EVENTS[0],
            "event_id": f"00000000-0000-4000-8000-0000000000{suffix}",
            "event_time": "2026-01-01T10:00:00Z",
            "play_seconds": seconds,
        }
        state = accumulate(PLAN, event, state)

    # by event id the values are 20, 10, 30; by arrival 30, 20, 10; by value 10, 20, 30
    assert aggregate_values(PLAN, state)["last_play_seconds"] == [10, 30]


def test_merge_keeps_the_last_k_of_both_sides():
    events = window_events("user000001", at(10, 30))
    assert len(events) == 3
    left = fold([events[0], events[2]])
    right = fold(events[1:2])

    merged = merge(PLAN, left, right)

    assert merged == fold(events)
    assert aggregate_values(PLAN, merged)["last_play_seconds"] == [120, 60]
    assert merged[2] == 3
    assert merge(PLAN, PLAN.zero, left) == left


def test_the_buffered_state_survives_pickling():
    state = fold(window_events("user000001", at(11)))

    assert pickle.loads(pickle.dumps(state)) == state


def test_events_seen_is_a_reserved_name():
    spec = SPEC.model_copy(update={"aggregations": [Aggregation(name="events_seen", op=Op.COUNT)]})

    with pytest.raises(UnsupportedSpecError, match="'events_seen' is reserved"):
        build_flink_plan(spec)


def test_other_policies_have_no_events_seen():
    plan = build_flink_plan(
        load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_extremes_1h.yaml")
    )

    assert plan.events_seen_slot is None
    assert plan.slots == 5
    assert not plan.buffered


def test_approx_distinct_is_still_rejected():
    spec = SPEC.model_copy(
        update={
            "aggregations": [
                Aggregation(name="distinct_content", op=Op.APPROX_DISTINCT, column="content_id")
            ]
        }
    )

    with pytest.raises(UnsupportedSpecError, match="approx_distinct"):
        build_flink_plan(spec)
