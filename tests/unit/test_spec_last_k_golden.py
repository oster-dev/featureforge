"""The spec reference reproduces hand-calculated values for last_k."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1

ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = ROOT / "fixtures" / "specs" / "user_play_seconds_last2_1h.yaml"
FIXTURE_PATH = ROOT / "fixtures" / "streaming" / "user_play_seconds_1h_v1.jsonl"


def at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, second, tzinfo=UTC)


def play(event_id: str, event_time: datetime, seconds: int) -> InteractionEventV1:
    return InteractionEventV1.model_validate_json(
        json.dumps(
            {
                "event_id": event_id,
                "event_time": event_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "ingested_at": (event_time + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "user_id": "user000001",
                "content_id": "content000001",
                "event_type": "play",
                "play_seconds": seconds,
                "source": "web",
                "schema_version": "v1",
            }
        )
    )


@pytest.fixture(scope="module")
def spec():
    return load_spec(SPEC_PATH)


@pytest.fixture(scope="module")
def events() -> list[InteractionEventV1]:
    return [
        InteractionEventV1.model_validate_json(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@pytest.mark.parametrize(
    ("user_id", "observation", "plays", "last"),
    [
        ("user000001", at(10), 1, [100]),
        ("user000001", at(10, 1), 2, [100, 120]),
        ("user000001", at(10, 30), 3, [120, 60]),
        ("user000001", at(10, 45), 3, [120, 60]),
        ("user000001", at(11), 3, [60, 90]),
        ("user000001", at(11, 0, 1), 3, [90, 500]),
        ("user000001", at(11, 30), 2, [90, 500]),
        ("user000001", at(12), 1, [500]),
        ("user000001", at(12, 0, 1), 0, []),
        ("user000002", at(11), 1, [200]),
        ("user000002", at(11, 15), 0, []),
        ("user000003", at(11), 0, []),
    ],
)
def test_reference_matches_hand_calculated_values(spec, events, user_id, observation, plays, last):
    result = evaluate_spec(spec, events, entity={"user_id": user_id}, observation_time=observation)

    assert result.values == {"play_count": plays, "last_play_seconds": last}


def test_equal_event_times_are_ordered_by_event_id(spec):
    arrival = [
        play("00000000-0000-4000-8000-0000000000a3", at(10), 30),
        play("00000000-0000-4000-8000-0000000000a1", at(10), 20),
        play("00000000-0000-4000-8000-0000000000a2", at(10), 10),
    ]

    result = evaluate_spec(spec, arrival, entity={"user_id": "user000001"}, observation_time=at(10))

    # by event id the values are 20, 10, 30; by arrival 30, 20, 10; by value 10, 20, 30
    assert result.values["last_play_seconds"] == [10, 30]
