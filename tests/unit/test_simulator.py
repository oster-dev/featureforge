from datetime import UTC, datetime

import pytest

from featureforge.config import SyntheticDataConfig
from featureforge.streaming.simulator import SimulatorConfig, simulate_interactions


@pytest.fixture
def data_config() -> SyntheticDataConfig:
    return SyntheticDataConfig(
        seed=7,
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 3, 1, tzinfo=UTC),
        num_users=50,
        num_content_items=20,
        num_base_events=10,
        num_observations=10,
        duplicate_rate=0.0,
        late_event_rate=0.0,
        label_horizon_days=7,
        max_late_arrival_hours=0,
        event_generation_mode="behavioral",
    )


@pytest.fixture
def sim() -> SimulatorConfig:
    return SimulatorConfig(seed=3, num_impressions=2_000)


def test_same_seed_is_deterministic(data_config, sim):
    first = simulate_interactions(data_config, sim)
    assert first == simulate_interactions(data_config, sim)


def test_all_contract_event_types_occur(data_config, sim):
    types = {e.event_type for e in simulate_interactions(data_config, sim)}
    assert types == {"impression", "play", "skip", "complete"}


def test_event_ids_are_unique(data_config, sim):
    events = simulate_interactions(data_config, sim)
    assert len({e.event_id for e in events}) == len(events)


def test_every_terminal_event_has_a_prior_play(data_config, sim):
    events = simulate_interactions(data_config, sim)
    plays: dict[tuple[str, str], list[datetime]] = {}
    for e in events:
        key = (e.user_id, e.content_id)
        if e.event_type == "play":
            plays.setdefault(key, []).append(e.event_time)
        elif e.event_type in {"skip", "complete"}:
            assert any(t < e.event_time for t in plays.get(key, []))


def test_late_events_exist_when_rate_is_high(data_config):
    sim = SimulatorConfig(seed=3, num_impressions=500, late_rate=0.5)
    events = simulate_interactions(data_config, sim)
    assert any((e.ingested_at - e.event_time).total_seconds() >= 3_600 for e in events)


def test_invalid_shares_are_rejected():
    with pytest.raises(ValueError, match="must not exceed 1"):
        SimulatorConfig(seed=1, num_impressions=1, skip_share=0.7, complete_share=0.5)
