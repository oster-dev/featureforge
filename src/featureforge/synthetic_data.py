from __future__ import annotations

from datetime import timedelta

import numpy as np

from featureforge.config import SyntheticDataConfig
from featureforge.models import Content, Event, User

COUNTRIES = ("DE", "RO", "US", "GB", "FR", "ES")
PLAN_TIERS = ("free", "basic", "premium")
ACQUISITION_CHANNELS = ("organic", "search", "social", "referral")
GENRES = ("drama", "comedy", "documentary", "action", "sci_fi", "thriller")
EVENT_TYPES = ("impression", "click", "play", "watch", "like", "search")
DEVICE_TYPES = ("web", "ios", "android", "tv")


def generate_users(config: SyntheticDataConfig) -> list[User]:
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


def generate_base_events(
    config: SyntheticDataConfig,
    users: list[User],
    content_items: list[Content],
) -> list[Event]:
    rng = np.random.default_rng(config.seed + 2)
    total_seconds = int((config.end_time - config.start_time).total_seconds())

    user_ids = [user.user_id for user in users]
    content_ids = [item.content_id for item in content_items]

    events: list[Event] = []

    for index in range(1, config.num_base_events + 1):
        event_type = str(rng.choice(EVENT_TYPES))
        event_offset = int(rng.integers(0, total_seconds + 1))
        event_time = config.start_time + timedelta(seconds=event_offset)

        content_id = None if event_type == "search" else str(rng.choice(content_ids))
        watch_seconds = int(rng.integers(30, 3_601)) if event_type in {"play", "watch"} else 0

        events.append(
            Event(
                event_id=f"event_{index:08d}",
                user_id=str(rng.choice(user_ids)),
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
