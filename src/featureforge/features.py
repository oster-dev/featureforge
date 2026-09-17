"""User-level feature aggregation for FeatureForge."""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from .feature_schema import (
    ContentFeatureBatch,
    ContentPopularityFeatures,
    UserEngagementFeatures,
    UserFeatureBatch,
)
from .models import SyntheticDataset


def _events_to_dataframe(dataset: SyntheticDataset) -> pd.DataFrame:
    """Convert dataset events into a flat DataFrame for aggregation."""
    records = [
        {
            "event_id": event.event_id,
            "user_id": event.user_id,
            "content_id": event.content_id,
            "event_type": event.event_type,
            "event_time": event.event_time,
            "watch_seconds": event.watch_seconds,
        }
        for event in dataset.events
    ]
    return pd.DataFrame.from_records(records)


def compute_user_engagement_features(
    dataset: SyntheticDataset,
    observation_time: datetime,
    window_days: int,
) -> UserFeatureBatch:
    """Compute point-in-time-correct user engagement features.

    Only events with event_time in (observation_time - window_days, observation_time]
    are included. This strictly excludes any event at or after observation_time,
    preventing future-data leakage.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be positive, got {window_days}")

    window_start = observation_time - timedelta(days=window_days)
    events_df = _events_to_dataframe(dataset)

    # Handle empty events DataFrame
    if events_df.empty:
        in_window = events_df
    else:
        in_window = events_df[
            (events_df["event_time"] > window_start) & (events_df["event_time"] <= observation_time)
        ]

    feature_records: list[UserEngagementFeatures] = []

    for user in dataset.users:
        # Handle empty in_window DataFrame
        if in_window.empty:
            user_events = in_window
        else:
            user_events = in_window[in_window["user_id"] == user.user_id]

        if user_events.empty:
            feature_records.append(
                UserEngagementFeatures(
                    user_id=user.user_id,
                    observation_time=observation_time,
                    window_days=window_days,
                    event_count=0,
                    unique_content_count=0,
                    total_watch_seconds=0,
                    search_count=0,
                    play_count=0,
                    watch_count=0,
                    days_since_last_activity=None,
                )
            )
            continue

        last_event_time = user_events["event_time"].max()
        days_since_last_activity = (observation_time - last_event_time).total_seconds() / 86400

        feature_records.append(
            UserEngagementFeatures(
                user_id=user.user_id,
                observation_time=observation_time,
                window_days=window_days,
                event_count=len(user_events),
                unique_content_count=user_events["content_id"].nunique(),
                total_watch_seconds=int(user_events["watch_seconds"].sum()),
                search_count=int((user_events["event_type"] == "search").sum()),
                play_count=int((user_events["event_type"] == "play").sum()),
                watch_count=int((user_events["event_type"] == "watch").sum()),
                days_since_last_activity=days_since_last_activity,
            )
        )

    return UserFeatureBatch(
        observation_time=observation_time,
        window_days=window_days,
        features=feature_records,
    )


def compute_content_popularity_features(
    dataset: SyntheticDataset,
    observation_time: datetime,
    window_days: int,
) -> ContentFeatureBatch:
    """Compute point-in-time-correct content popularity features.

    Only events with event_time in (observation_time - window_days, observation_time]
    are included. This strictly excludes any event at or after observation_time,
    preventing future-data leakage.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be positive, got {window_days}")

    window_start = observation_time - timedelta(days=window_days)
    events_df = _events_to_dataframe(dataset)

    if events_df.empty:
        in_window = events_df
    else:
        in_window = events_df[
            (events_df["event_time"] > window_start) & (events_df["event_time"] <= observation_time)
        ]

    feature_records: list[ContentPopularityFeatures] = []

    for content in dataset.content_items:
        if in_window.empty:
            content_events = in_window
        else:
            content_events = in_window[in_window["content_id"] == content.content_id]

        if content_events.empty:
            feature_records.append(
                ContentPopularityFeatures(
                    content_id=content.content_id,
                    observation_time=observation_time,
                    window_days=window_days,
                    view_count=0,
                    unique_viewer_count=0,
                    total_watch_seconds=0,
                    average_watch_seconds=0.0,
                    search_count=0,
                    play_count=0,
                    watch_count=0,
                    days_since_last_view=None,
                )
            )
            continue

        last_event_time = content_events["event_time"].max()
        days_since_last_view = (observation_time - last_event_time).total_seconds() / 86400

        total_watch = int(content_events["watch_seconds"].sum())
        view_count = len(content_events)

        feature_records.append(
            ContentPopularityFeatures(
                content_id=content.content_id,
                observation_time=observation_time,
                window_days=window_days,
                view_count=view_count,
                unique_viewer_count=content_events["user_id"].nunique(),
                total_watch_seconds=total_watch,
                average_watch_seconds=total_watch / view_count,
                search_count=int((content_events["event_type"] == "search").sum()),
                play_count=int((content_events["event_type"] == "play").sum()),
                watch_count=int((content_events["event_type"] == "watch").sum()),
                days_since_last_view=days_since_last_view,
            )
        )

    return ContentFeatureBatch(
        observation_time=observation_time,
        window_days=window_days,
        features=feature_records,
    )
