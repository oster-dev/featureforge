"""Feast feature-view definitions for FeatureForge."""

from datetime import timedelta

from feast import FeatureView, Field
from feast.types import Float64, Int64

from entities import content, user
from sources import content_features_source, user_features_source


user_engagement_features = FeatureView(
    name="user_engagement_features",
    entities=[user],
    ttl=timedelta(days=30),
    schema=[
        Field(name="window_days", dtype=Int64),
        Field(name="event_count", dtype=Int64),
        Field(name="unique_content_count", dtype=Int64),
        Field(name="total_watch_seconds", dtype=Int64),
        Field(name="search_count", dtype=Int64),
        Field(name="play_count", dtype=Int64),
        Field(name="watch_count", dtype=Int64),
        Field(name="days_since_last_activity", dtype=Float64),
    ],
    source=user_features_source,
    online=True,
    description=(
        "Point-in-time user engagement features computed from behavioral "
        "events over a configurable lookback window."
    ),
)


content_popularity_features = FeatureView(
    name="content_popularity_features",
    entities=[content],
    ttl=timedelta(days=30),
    schema=[
        Field(name="window_days", dtype=Int64),
        Field(name="view_count", dtype=Int64),
        Field(name="unique_viewer_count", dtype=Int64),
        Field(name="total_watch_seconds", dtype=Int64),
        Field(name="average_watch_seconds", dtype=Float64),
        Field(name="search_count", dtype=Int64),
        Field(name="play_count", dtype=Int64),
        Field(name="watch_count", dtype=Int64),
        Field(name="days_since_last_view", dtype=Float64),
    ],
    source=content_features_source,
    online=True,
    description=(
        "Point-in-time content popularity features computed from behavioral "
        "events over a configurable lookback window."
    ),
)