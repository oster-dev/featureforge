from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_skip_rate import calculate_user_skip_rate_24h

ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = ROOT / "feature_repo" / "specs" / "user_skip_rate_24h.yaml"
FIXTURE_PATH = ROOT / "fixtures" / "streaming" / "user_skip_rate_24h_v1.jsonl"


def at(day: int, hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime(2026, 1, day, hour, minute, second, tzinfo=UTC)


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
    ("user_id", "observation", "plays", "skips", "rate"),
    [
        ("user000001", at(2, 12), 2, 2, 0.5),
        ("user000001", at(2, 12, 0, 1), 1, 3, 0.75),
        ("user000001", at(2, 18), 1, 2, 2 / 3),
        ("user000001", at(3, 12, 0, 1), 0, 0, 0.0),
        ("user000002", at(2, 12), 0, 1, 1.0),
    ],
)
def test_spec_reference_matches_golden_values(
    spec, events, user_id, observation, plays, skips, rate
):
    result = evaluate_spec(spec, events, entity={"user_id": user_id}, observation_time=observation)
    assert result.values["play_count"] == plays
    assert result.values["skip_count"] == skips
    assert result.values["skip_rate"] == pytest.approx(rate)


def test_spec_reference_reports_the_24h_window(spec, events):
    result = evaluate_spec(
        spec, events, entity={"user_id": "user000001"}, observation_time=at(2, 12)
    )
    assert result.observation_time == at(2, 12)
    assert result.window_start_exclusive == at(1, 12)


def boundary_times(events: list[InteractionEventV1]) -> list[datetime]:
    day = timedelta(hours=24)
    offsets = (timedelta(0), timedelta(seconds=1), timedelta(seconds=-1))
    times = {
        base + offset
        for event in events
        for base in (event.event_time, event.event_time + day)
        for offset in offsets
    }
    return sorted(times)


@pytest.mark.parametrize("user_id", ["user000001", "user000002"])
def test_spec_reference_matches_hand_written_reference_at_every_boundary(spec, events, user_id):
    times = boundary_times(events)
    assert len(times) > 20
    for observation in times:
        generic = evaluate_spec(
            spec, events, entity={"user_id": user_id}, observation_time=observation
        )
        hand_written = calculate_user_skip_rate_24h(
            events, user_id=user_id, observation_time=observation
        )
        assert generic.values["skip_rate"] == pytest.approx(hand_written), observation
