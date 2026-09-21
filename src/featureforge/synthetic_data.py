from __future__ import annotations

from datetime import timedelta

import numpy as np

from featureforge.config import SyntheticDataConfig
from featureforge.models import (
    Content,
    Event,
    ObservationLabel,
    SyntheticDataset,
    User,
)

COUNTRIES = ("DE", "RO", "US", "GB", "FR", "ES")
PLAN_TIERS = ("free", "basic", "premium")
ACQUISITION_CHANNELS = ("organic", "search", "social", "referral")
GENRES = ("drama", "comedy", "documentary", "action", "sci_fi", "thriller")
EVENT_TYPES = ("impression", "click", "play", "watch", "like", "search")
DEVICE_TYPES = ("web", "ios", "android", "tv")

ACTIVITY_SEGMENTS = ("dormant", "casual", "engaged", "power")

ACTIVITY_SEGMENT_PROBABILITIES = np.array(
    [0.20, 0.45, 0.25, 0.10],
    dtype=float,
)

ACTIVITY_SEGMENT_WEIGHTS = {
    "dormant": 0.15,
    "casual": 1.0,
    "engaged": 3.5,
    "power": 8.0,
}


def generate_users(config: SyntheticDataConfig) -> list[User]:
    """Generate deterministic synthetic users."""
    rng = np.random.default_rng(config.seed)
    total_seconds = int((config.end_time - config.start_time).total_seconds())

    users: list[User] = []

    for index in range(1, config.num_users + 1):
        signup_offset = int(rng.integers(0, total_seconds + 1))
        signup_at = config.start_time + timedelta(seconds=signup_offset)

        users.append(
            User(
                user_id=f"user_{index:06d}",
                signup_at=signup_at,
                country=str(rng.choice(COUNTRIES)),
                plan_tier=str(rng.choice(PLAN_TIERS)),
                acquisition_channel=str(rng.choice(ACQUISITION_CHANNELS)),
            )
        )

    return users


def generate_content(config: SyntheticDataConfig) -> list[Content]:
    """Generate deterministic synthetic content items."""
    rng = np.random.default_rng(config.seed + 1)
    total_seconds = int((config.end_time - config.start_time).total_seconds())

    content_items: list[Content] = []

    for index in range(1, config.num_content_items + 1):
        release_offset = int(rng.integers(0, total_seconds + 1))
        released_at = config.start_time + timedelta(seconds=release_offset)

        content_items.append(
            Content(
                content_id=f"content_{index:06d}",
                title=f"Content {index:06d}",
                genre=str(rng.choice(GENRES)),
                released_at=released_at,
                duration_seconds=int(rng.integers(300, 7_201)),
            )
        )

    return content_items


def generate_user_activity_weights(
    config: SyntheticDataConfig,
    users: list[User],
) -> dict[str, float]:
    """Assign deterministic latent activity weights for behavioral generation."""
    rng = np.random.default_rng(config.seed + 20)

    segments = rng.choice(
        ACTIVITY_SEGMENTS,
        size=len(users),
        p=ACTIVITY_SEGMENT_PROBABILITIES,
    )

    return {
        user.user_id: ACTIVITY_SEGMENT_WEIGHTS[str(segment)]
        for user, segment in zip(users, segments, strict=True)
    }


def generate_base_events(
    config: SyntheticDataConfig,
    users: list[User],
    content_items: list[Content],
) -> list[Event]:
    """Generate deterministic base events using the configured generation mode."""
    rng = np.random.default_rng(config.seed + 2)
    total_seconds = int((config.end_time - config.start_time).total_seconds())

    user_ids = [user.user_id for user in users]
    content_ids = [item.content_id for item in content_items]

    if config.event_generation_mode == "behavioral":
        activity_weights = generate_user_activity_weights(config, users)
        user_probabilities = np.array(
            [activity_weights[user_id] for user_id in user_ids],
            dtype=float,
        )
        user_probabilities /= user_probabilities.sum()
    else:
        user_probabilities = None

    events: list[Event] = []

    for index in range(1, config.num_base_events + 1):
        event_type = str(rng.choice(EVENT_TYPES))
        event_offset = int(rng.integers(0, total_seconds + 1))
        event_time = config.start_time + timedelta(seconds=event_offset)

        content_id = None if event_type == "search" else str(rng.choice(content_ids))
        watch_seconds = int(rng.integers(30, 3_601)) if event_type in {"play", "watch"} else 0

        user_id = str(rng.choice(user_ids, p=user_probabilities))

        events.append(
            Event(
                event_id=f"event_{index:08d}",
                user_id=user_id,
                content_id=content_id,
                event_type=event_type,
                event_time=event_time,
                ingested_at=event_time,
                session_id=f"session_{index:08d}",
                device_type=str(rng.choice(DEVICE_TYPES)),
                watch_seconds=watch_seconds,
            )
        )

    return events


def inject_duplicates(
    config: SyntheticDataConfig,
    events: list[Event],
) -> list[Event]:
    """Inject deterministic duplicate events without mutating input events."""
    rng = np.random.default_rng(config.seed + 3)
    duplicate_count = int(len(events) * config.duplicate_rate)

    duplicate_indexes = rng.choice(
        len(events),
        size=duplicate_count,
        replace=False,
    )

    duplicates: list[Event] = []

    for duplicate_number, event_index in enumerate(duplicate_indexes, start=1):
        original_event = events[int(event_index)]

        duplicates.append(
            original_event.model_copy(
                update={
                    "event_id": (f"{original_event.event_id}_duplicate_{duplicate_number:02d}"),
                    "is_duplicate": True,
                }
            )
        )

    return [*events, *duplicates]


def inject_late_events(
    config: SyntheticDataConfig,
    events: list[Event],
) -> list[Event]:
    """Mark a deterministic subset of events as late-arriving."""
    rng = np.random.default_rng(config.seed + 4)
    late_event_count = int(len(events) * config.late_event_rate)

    if late_event_count == 0:
        return list(events)

    late_indexes = set(
        rng.choice(
            len(events),
            size=late_event_count,
            replace=False,
        )
    )

    late_events: list[Event] = []

    for index, event in enumerate(events):
        if index not in late_indexes:
            late_events.append(event)
            continue

        delay_seconds = int(
            rng.integers(
                1,
                config.max_late_arrival_hours * 3_600 + 1,
            )
        )

        late_events.append(
            event.model_copy(
                update={
                    "ingested_at": event.ingested_at + timedelta(seconds=delay_seconds),
                    "is_late": True,
                }
            )
        )

    return late_events


def generate_observation_labels(
    config: SyntheticDataConfig,
    users: list[User],
    events: list[Event],
) -> list[ObservationLabel]:
    """Generate labels from real event activity in the future horizon."""
    rng = np.random.default_rng(config.seed + 5)
    label_horizon = timedelta(days=config.label_horizon_days)
    latest_observation_time = config.end_time - label_horizon
    available_seconds = int((latest_observation_time - config.start_time).total_seconds())

    labels: list[ObservationLabel] = []

    for index in range(1, config.num_observations + 1):
        observation_offset = int(rng.integers(0, available_seconds + 1))
        observation_time = config.start_time + timedelta(seconds=observation_offset)
        label_window_end = observation_time + label_horizon
        user = users[int(rng.integers(0, len(users)))]

        is_active_next_7d = any(
            event.user_id == user.user_id
            and observation_time < event.event_time <= label_window_end
            for event in events
        )

        labels.append(
            ObservationLabel(
                label_id=f"label_{index:08d}",
                user_id=user.user_id,
                observation_time=observation_time,
                label_window_end=label_window_end,
                is_active_next_7d=is_active_next_7d,
            )
        )

    return labels


def generate_synthetic_dataset(
    config: SyntheticDataConfig,
) -> SyntheticDataset:
    """Generate the complete deterministic synthetic dataset."""
    users = generate_users(config)
    content_items = generate_content(config)
    base_events = generate_base_events(config, users, content_items)
    events_with_duplicates = inject_duplicates(config, base_events)
    events = inject_late_events(config, events_with_duplicates)
    labels = generate_observation_labels(config, users, events)

    return SyntheticDataset(
        users=users,
        content_items=content_items,
        events=events,
        labels=labels,
    )
