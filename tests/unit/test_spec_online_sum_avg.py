"""The online sum and avg spec: policy, derived average and the fixture values (ADR-017)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.model import Op, UpdatePolicy
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1

ROOT = Path(__file__).resolve().parents[2]
SPEC = load_spec(ROOT / "fixtures" / "specs" / "user_play_seconds_derived_1h.yaml")
EVENTS = [
    InteractionEventV1.model_validate_json(line)
    for line in (ROOT / "fixtures" / "streaming" / "user_play_seconds_1h_v1.jsonl")
    .read_text(encoding="utf-8")
    .splitlines()
    if line.strip()
]


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=UTC)


def test_the_spec_keeps_the_sum_as_a_component_and_the_average_as_derived() -> None:
    assert SPEC.online.update_policy is UpdatePolicy.COMPONENTWISE_MONOTONIC
    assert [aggregation.op for aggregation in SPEC.aggregations] == [Op.COUNT, Op.SUM]
    assert set(SPEC.derived) == {"avg_play_seconds"}


@pytest.mark.parametrize(
    ("user", "when", "count", "total", "average"),
    [
        ("user000001", at(10), 1, 100, 100.0),
        ("user000001", at(10, 1), 2, 220, 110.0),
        ("user000001", at(11), 3, 270, 90.0),
        ("user000001", at(11, 30), 2, 590, 295.0),
        ("user000001", at(12), 1, 500, 500.0),
        ("user000002", at(11), 1, 200, 200.0),
        ("user000003", at(10, 30), 0, 0, None),
        ("user000003", at(11), 0, 0, None),
    ],
)
def test_the_reference_gives_the_hand_calculated_windows(
    user: str, when: datetime, count: int, total: int, average: float | None
) -> None:
    result = evaluate_spec(SPEC, EVENTS, entity={"user_id": user}, observation_time=when)

    assert result.values == {
        "play_count": count,
        "play_seconds_sum": total,
        "avg_play_seconds": average,
    }
