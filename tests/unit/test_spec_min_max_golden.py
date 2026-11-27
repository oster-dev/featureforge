"""The spec reference reproduces hand-calculated values for min and max."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1

ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = ROOT / "fixtures" / "specs" / "user_play_seconds_extremes_1h.yaml"
FIXTURE_PATH = ROOT / "fixtures" / "streaming" / "user_play_seconds_1h_v1.jsonl"


def at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, second, tzinfo=UTC)


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
def test_reference_matches_hand_calculated_values(
    spec, events, user_id, observation, plays, smallest, largest
):
    result = evaluate_spec(spec, events, entity={"user_id": user_id}, observation_time=observation)

    assert result.values == {
        "play_count": plays,
        "min_play_seconds": smallest,
        "max_play_seconds": largest,
    }
