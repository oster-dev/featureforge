from pathlib import Path

import pytest
from pydantic import ValidationError

from featureforge.config import SyntheticDataConfig, load_synthetic_data_config


def test_loads_valid_synthetic_data_config() -> None:
    config_path = Path("configs/synthetic_data.yaml")

    config = load_synthetic_data_config(config_path)

    assert config.seed == 20260914
    assert config.num_users == 500
    assert config.num_content_items == 250
    assert config.num_base_events == 10000
    assert config.num_observations == 1000
    assert config.start_time < config.end_time


def test_rejects_invalid_duplicate_rate() -> None:
    with pytest.raises(ValidationError):
        SyntheticDataConfig(
            seed=1,
            start_time="2026-01-01T00:00:00Z",
            end_time="2026-01-02T00:00:00Z",
            num_users=1,
            num_content_items=1,
            num_base_events=1,
            num_observations=1,
            duplicate_rate=1.1,
            late_event_rate=0.0,
            label_horizon_days=7,
            max_late_arrival_hours=72,
        )


def test_rejects_invalid_time_range() -> None:
    with pytest.raises(ValidationError, match="end_time must be after start_time"):
        SyntheticDataConfig(
            seed=1,
            start_time="2026-01-02T00:00:00Z",
            end_time="2026-01-01T00:00:00Z",
            num_users=1,
            num_content_items=1,
            num_base_events=1,
            num_observations=1,
            duplicate_rate=0.0,
            late_event_rate=0.0,
            label_horizon_days=7,
            max_late_arrival_hours=72,
        )


def test_rejects_late_events_without_allowed_lateness() -> None:
    with pytest.raises(
        ValidationError,
        match=(
            "max_late_arrival_hours must be greater than zero "
            "when late_event_rate is positive"
        ),
    ):
        SyntheticDataConfig(
            seed=1,
            start_time="2026-01-01T00:00:00Z",
            end_time="2026-01-02T00:00:00Z",
            num_users=1,
            num_content_items=1,
            num_base_events=1,
            num_observations=1,
            duplicate_rate=0.0,
            late_event_rate=0.1,
            label_horizon_days=7,
            max_late_arrival_hours=0,
        )