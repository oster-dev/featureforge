"""The Flink plan computes min and max with a value slot and a count slot (ADR-017)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.spec.flink_plan import (
    accumulate,
    aggregate_values,
    build_flink_plan,
    feature_record,
    merge,
)
from featureforge.spec.loader import load_spec
from featureforge.spec.model import Op

ROOT = Path(__file__).resolve().parents[2]
SPEC = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_extremes_1h.yaml")
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


def window_record(user_id: str, observation: datetime) -> dict:
    start = observation - HOUR
    seen: set[str] = set()
    accumulator = PLAN.zero
    for event in EVENTS:
        if event["user_id"] != user_id or event["event_id"] in seen:
            continue
        seen.add(event["event_id"])
        if start < datetime.fromisoformat(event["event_time"]) <= observation:
            accumulator = accumulate(PLAN, event, accumulator)
    return json.loads(
        feature_record(PLAN, user_id, millis(start), millis(observation), accumulator)
    )


def test_min_and_max_take_two_slots_each():
    assert [(a.name, a.op, a.slot, a.width) for a in PLAN.aggregations] == [
        ("play_count", Op.COUNT, 0, 1),
        ("min_play_seconds", Op.MIN, 1, 2),
        ("max_play_seconds", Op.MAX, 3, 2),
    ]
    assert PLAN.slots == 5
    assert PLAN.zero == (0, 0, 0, 0, 0)


@pytest.mark.parametrize(
    ("user_id", "observation", "plays", "smallest", "largest"),
    [
        ("user000001", at(10), 1, 100, 100),
        ("user000001", at(10, 1), 2, 100, 120),
        ("user000001", at(10, 30), 3, 60, 120),
        ("user000001", at(10, 45), 3, 60, 120),
        ("user000001", at(11), 3, 60, 120),
        ("user000001", at(11, 0, 1), 3, 60, 500),
        ("user000001", at(11, 30), 2, 90, 500),
        ("user000001", at(12), 1, 500, 500),
        ("user000001", at(12, 0, 1), 0, None, None),
        ("user000002", at(11), 1, 200, 200),
        ("user000002", at(11, 15), 0, None, None),
        ("user000003", at(11), 0, None, None),
    ],
)
def test_plan_reproduces_the_min_and_max_golden_values(
    user_id, observation, plays, smallest, largest
):
    record = window_record(user_id, observation)

    assert record["play_count"] == plays
    assert record["min_play_seconds"] == smallest
    assert record["max_play_seconds"] == largest


def test_accumulate_keeps_the_extremes_of_matching_events_only():
    accumulator = PLAN.zero
    for event_type, seconds in (("play", 100), ("skip", 30), ("play", 50)):
        event = {"event_type": event_type, "play_seconds": seconds}
        accumulator = accumulate(PLAN, event, accumulator)

    assert accumulator == (2, 50, 2, 100, 2)


def test_an_empty_window_has_missing_extremes_and_a_zero_extreme_is_not_missing():
    assert aggregate_values(PLAN, PLAN.zero) == {
        "play_count": 0,
        "min_play_seconds": None,
        "max_play_seconds": None,
    }
    zero = accumulate(PLAN, {"event_type": "play", "play_seconds": 0}, PLAN.zero)

    assert aggregate_values(PLAN, zero) == {
        "play_count": 1,
        "min_play_seconds": 0,
        "max_play_seconds": 0,
    }


def test_merge_takes_the_extreme_and_ignores_a_side_without_events():
    left = (2, 50, 2, 100, 2)
    right = (1, 40, 1, 70, 1)

    assert merge(PLAN, left, right) == (3, 40, 3, 100, 3)
    assert merge(PLAN, PLAN.zero, left) == left
    assert merge(PLAN, left, PLAN.zero) == left
    assert merge(PLAN, PLAN.zero, PLAN.zero) == PLAN.zero


def test_merge_checks_the_slot_count():
    with pytest.raises(ValueError, match="5 slots"):
        merge(PLAN, (0, 0), PLAN.zero)
