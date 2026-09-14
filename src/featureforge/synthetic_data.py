from __future__ import annotations

from datetime import timedelta

import numpy as np

from featureforge.config import SyntheticDataConfig
from featureforge.models import Content, User

COUNTRIES = ("DE", "RO", "US", "GB", "FR", "ES")
PLAN_TIERS = ("free", "basic", "premium")
ACQUISITION_CHANNELS = ("organic", "search", "social", "referral")
GENRES = ("drama", "comedy", "documentary", "action", "sci_fi", "thriller")


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