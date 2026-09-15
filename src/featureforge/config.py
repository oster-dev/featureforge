from __future__ import annotations

from datetime import datetime
from pathlib import Path

import yaml
from pydantic import BaseModel, Field, model_validator


class SyntheticDataConfig(BaseModel):
    seed: int = Field(ge=0)

    start_time: datetime
    end_time: datetime

    num_users: int = Field(gt=0)
    num_content_items: int = Field(gt=0)
    num_base_events: int = Field(gt=0)
    num_observations: int = Field(gt=0)

    duplicate_rate: float = Field(ge=0.0, le=1.0)
    late_event_rate: float = Field(ge=0.0, le=1.0)

    label_horizon_days: int = Field(gt=0)
    max_late_arrival_hours: int = Field(ge=0)

    @model_validator(mode="after")
    def validate_config(self) -> SyntheticDataConfig:
        if self.end_time <= self.start_time:
            raise ValueError("end_time must be after start_time")

        if self.late_event_rate > 0.0 and self.max_late_arrival_hours == 0:
            raise ValueError(
                "max_late_arrival_hours must be greater than zero when late_event_rate is positive"
            )

        return self


def load_synthetic_data_config(path: str | Path) -> SyntheticDataConfig:
    config_path = Path(path)

    with config_path.open(encoding="utf-8") as file:
        raw_config = yaml.safe_load(file)

    if raw_config is None:
        raise ValueError(f"Configuration file is empty: {config_path}")

    return SyntheticDataConfig.model_validate(raw_config)
