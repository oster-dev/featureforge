"""Deterministic behavioral simulator that emits interaction-events.v1."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import NAMESPACE_URL, uuid5

import numpy as np
from pydantic import BaseModel, ConfigDict, Field, model_validator

from featureforge.config import SyntheticDataConfig
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.synthetic_data import (
    generate_content,
    generate_user_activity_weights,
    generate_users,
)

SOURCES = ("web", "mobile", "tv")


class SimulatorConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    seed: int = Field(ge=0)
    num_impressions: int = Field(gt=0)
    play_probability: float = Field(default=0.35, ge=0.0, le=1.0)
    skip_share: float = Field(default=0.35, ge=0.0, le=1.0)
    complete_share: float = Field(default=0.25, ge=0.0, le=1.0)
    skip_threshold_fraction: float = Field(default=0.1, gt=0.0, lt=1.0)
    late_rate: float = Field(default=0.02, ge=0.0, le=1.0)
    max_late_hours: int = Field(default=6, gt=0)

    @model_validator(mode="after")
    def validate_shares(self) -> SimulatorConfig:
        if self.skip_share + self.complete_share > 1.0:
            raise ValueError("skip_share + complete_share must not exceed 1")
        return self


def _utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def simulate_interactions(
    data_config: SyntheticDataConfig,
    sim: SimulatorConfig,
) -> list[InteractionEventV1]:
    """Emit impression/play/skip/complete events; unfinished plays emit no end event."""
    users = generate_users(data_config)
    content = generate_content(data_config)
    weights = generate_user_activity_weights(data_config, users)

    probabilities = np.array([weights[u.user_id] for u in users], dtype=float)
    probabilities /= probabilities.sum()

    rng = np.random.default_rng(sim.seed)
    start = _utc(data_config.start_time)
    end = _utc(data_config.end_time)
    counter = 0
    events: list[InteractionEventV1] = []

    def emit(
        user_id: str,
        content_id: str,
        event_type: str,
        event_time: datetime,
        play_seconds: int,
        source: str,
    ) -> None:
        nonlocal counter
        counter += 1
        if rng.random() < sim.late_rate:
            delay = int(rng.integers(3_600, sim.max_late_hours * 3_600 + 1))
        else:
            delay = int(rng.integers(0, 6))
        events.append(
            InteractionEventV1.model_validate(
                {
                    "event_id": uuid5(NAMESPACE_URL, f"featureforge-sim:{sim.seed}:{counter}"),
                    "event_time": event_time,
                    "ingested_at": event_time + timedelta(seconds=delay),
                    "user_id": user_id,
                    "content_id": content_id,
                    "event_type": event_type,
                    "play_seconds": play_seconds,
                    "source": source,
                    "schema_version": "v1",
                }
            )
        )

    for _ in range(sim.num_impressions):
        user = users[int(rng.choice(len(users), p=probabilities))]
        item = content[int(rng.integers(0, len(content)))]
        source = SOURCES[int(rng.integers(0, len(SOURCES)))]

        earliest = max(start, _utc(user.signup_at))
        span = int((end - earliest).total_seconds())
        shown_at = earliest + timedelta(seconds=int(rng.integers(0, span + 1)))
        emit(user.user_id, item.content_id, "impression", shown_at, 0, source)

        if rng.random() >= sim.play_probability:
            continue

        play_at = shown_at + timedelta(seconds=int(rng.integers(1, 31)))
        emit(user.user_id, item.content_id, "play", play_at, 0, source)

        outcome = rng.random()
        duration = item.duration_seconds
        if outcome < sim.skip_share:
            limit = max(1, int(duration * sim.skip_threshold_fraction))
            played = int(rng.integers(1, limit + 1))
            kind = "skip"
        elif outcome < sim.skip_share + sim.complete_share:
            played = duration
            kind = "complete"
        else:
            continue

        end_at = play_at + timedelta(seconds=played)
        emit(user.user_id, item.content_id, kind, end_at, played, source)

    events.sort(key=lambda e: (e.event_time, str(e.event_id)))
    return events
