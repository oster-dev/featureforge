from datetime import UTC, datetime
from pathlib import Path

import pytest

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_skip_rate import calculate_user_skip_rate_24h

FIXTURE_PATH = (
    Path(__file__).resolve().parents[2] / "fixtures" / "streaming" / "user_skip_rate_24h_v1.jsonl"
)


@pytest.fixture
def golden_events() -> list[InteractionEventV1]:
    return [
        InteractionEventV1.model_validate_json(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def at(day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, minute, second, tzinfo=UTC)


@pytest.mark.parametrize(
    ("user_id", "observation", "expected"),
    [
        ("user000001", at(2, 12), 0.5),
        ("user000001", at(2, 12, 0, 1), 0.75),
        ("user000001", at(2, 18), 2 / 3),
        ("user000001", at(3, 12, 0, 1), 0.0),
        ("user000002", at(2, 12), 1.0),
    ],
)
def test_golden_skip_rate_values(golden_events, user_id, observation, expected):
    actual = calculate_user_skip_rate_24h(
        golden_events, user_id=user_id, observation_time=observation
    )
    assert actual == pytest.approx(expected)


def test_golden_fixture_shape(golden_events):
    assert len(golden_events) == 10
    ids = [str(event.event_id) for event in golden_events]
    assert len(set(ids)) == 9
    assert {event.event_type for event in golden_events} == {
        "play",
        "skip",
        "impression",
        "complete",
    }
