"""PySpark implementation of point-in-time feature aggregation.

This module mirrors ``featureforge.features`` exactly. Every feature
definition, time-window boundary, and edge case here must match the
pandas reference implementation. Parity between the two engines is
enforced by ``tests/unit/test_spark_features.py``.

Engine choice (pandas vs. Spark) is an execution detail, not a change
in feature semantics: the window is always
``(observation_time - window_days, observation_time]`` -- strictly
excluding events at or after ``observation_time`` to prevent
future-data leakage.

Timezone handling
------------------
Spark's TimestampType round-trips through the JVM's default timezone
during Python <-> JVM conversion (via py4j/Arrow), independent of the
``spark.sql.session.timeZone`` SQL setting. On a machine whose local
timezone is not UTC, naive round-tripping of ``datetime`` objects can
silently shift timestamps by the local UTC offset (including DST).

To make this deterministic regardless of the machine's local timezone,
all event timestamps are converted to UTC epoch microseconds (plain
integers) before being handed to Spark, and converted back to
timezone-aware UTC ``datetime`` objects after ``collect()``. Integers
have no timezone to misinterpret, which removes the entire class of
bug at the source instead of patching it after the fact.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
)

from .feature_schema import (
    ContentFeatureBatch,
    ContentPopularityFeatures,
    UserEngagementFeatures,
    UserFeatureBatch,
)
from .models import SyntheticDataset

_EVENTS_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("content_id", StringType(), nullable=True),
        StructField("event_type", StringType(), nullable=False),
        StructField("event_time_micros", LongType(), nullable=False),
        StructField("watch_seconds", LongType(), nullable=False),
    ]
)


def _to_epoch_micros(value: datetime) -> int:
    """Convert a timezone-aware datetime to UTC epoch microseconds.

    Using integers instead of Spark TimestampType sidesteps any
    ambiguity in how Spark/py4j interpret naive vs. aware datetimes
    or the JVM's local timezone during Python <-> JVM conversion.
    """
    if value.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")
    return int(value.astimezone(UTC).timestamp() * 1_000_000)


def _from_epoch_micros(value: int) -> datetime:
    """Convert UTC epoch microseconds back to a timezone-aware UTC datetime."""
    return datetime.fromtimestamp(value / 1_000_000, tz=UTC)


def _events_to_spark_dataframe(spark: SparkSession, dataset: SyntheticDataset) -> DataFrame:
    """Convert dataset events into a Spark DataFrame for aggregation."""
    records = [
        (
            event.event_id,
            event.user_id,
            event.content_id,
            event.event_type,
            _to_epoch_micros(event.event_time),
            int(event.watch_seconds),
        )
        for event in dataset.events
    ]
    return spark.createDataFrame(records, schema=_EVENTS_SCHEMA)


def _filter_window(
    events_df: DataFrame, window_start: datetime, observation_time: datetime
) -> DataFrame:
    """Apply the strict point-in-time window: (window_start, observation_time]."""
    window_start_micros = _to_epoch_micros(window_start)
    observation_time_micros = _to_epoch_micros(observation_time)
    return events_df.filter(
        (F.col("event_time_micros") > F.lit(window_start_micros))
        & (F.col("event_time_micros") <= F.lit(observation_time_micros))
    )


def compute_user_engagement_features_spark(
    spark: SparkSession,
    dataset: SyntheticDataset,
    observation_time: datetime,
    window_days: int,
) -> UserFeatureBatch:
    """Compute point-in-time-correct user engagement features using Spark.

    Mirrors ``featureforge.features.compute_user_engagement_features``
    exactly. Only events with event_time in
    (observation_time - window_days, observation_time] are included.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be positive, got {window_days}")

    window_start = observation_time - timedelta(days=window_days)
    events_df = _events_to_spark_dataframe(spark, dataset)
    in_window = _filter_window(events_df, window_start, observation_time)

    aggregated = (
        in_window.groupBy("user_id")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.countDistinct("content_id").alias("unique_content_count"),
            F.sum("watch_seconds").alias("total_watch_seconds"),
            F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
            F.sum(F.when(F.col("event_type") == "play", 1).otherwise(0)).alias("play_count"),
            F.sum(F.when(F.col("event_type") == "watch", 1).otherwise(0)).alias("watch_count"),
            F.max("event_time_micros").alias("last_event_time_micros"),
        )
        .collect()
    )

    stats_by_user = {row["user_id"]: row for row in aggregated}

    feature_records: list[UserEngagementFeatures] = []
    for user in dataset.users:
        stats = stats_by_user.get(user.user_id)

        if stats is None or stats["event_count"] == 0:
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

        last_event_time = _from_epoch_micros(stats["last_event_time_micros"])
        days_since_last_activity = (observation_time - last_event_time).total_seconds() / 86400

        feature_records.append(
            UserEngagementFeatures(
                user_id=user.user_id,
                observation_time=observation_time,
                window_days=window_days,
                event_count=int(stats["event_count"]),
                unique_content_count=int(stats["unique_content_count"]),
                total_watch_seconds=int(stats["total_watch_seconds"]),
                search_count=int(stats["search_count"]),
                play_count=int(stats["play_count"]),
                watch_count=int(stats["watch_count"]),
                days_since_last_activity=days_since_last_activity,
            )
        )

    return UserFeatureBatch(
        observation_time=observation_time,
        window_days=window_days,
        features=feature_records,
    )


def compute_content_popularity_features_spark(
    spark: SparkSession,
    dataset: SyntheticDataset,
    observation_time: datetime,
    window_days: int,
) -> ContentFeatureBatch:
    """Compute point-in-time-correct content popularity features using Spark.

    Mirrors ``featureforge.features.compute_content_popularity_features``
    exactly. Only events with event_time in
    (observation_time - window_days, observation_time] are included.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be positive, got {window_days}")

    window_start = observation_time - timedelta(days=window_days)
    events_df = _events_to_spark_dataframe(spark, dataset)
    in_window = _filter_window(events_df, window_start, observation_time)

    aggregated = (
        in_window.groupBy("content_id")
        .agg(
            F.count(F.lit(1)).alias("view_count"),
            F.countDistinct("user_id").alias("unique_viewer_count"),
            F.sum("watch_seconds").alias("total_watch_seconds"),
            F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
            F.sum(F.when(F.col("event_type") == "play", 1).otherwise(0)).alias("play_count"),
            F.sum(F.when(F.col("event_type") == "watch", 1).otherwise(0)).alias("watch_count"),
            F.max("event_time_micros").alias("last_event_time_micros"),
        )
        .collect()
    )

    stats_by_content = {row["content_id"]: row for row in aggregated}

    feature_records: list[ContentPopularityFeatures] = []
    for content in dataset.content_items:
        stats = stats_by_content.get(content.content_id)

        if stats is None or stats["view_count"] == 0:
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

        last_event_time = _from_epoch_micros(stats["last_event_time_micros"])
        days_since_last_view = (observation_time - last_event_time).total_seconds() / 86400

        total_watch = int(stats["total_watch_seconds"])
        view_count = int(stats["view_count"])

        feature_records.append(
            ContentPopularityFeatures(
                content_id=content.content_id,
                observation_time=observation_time,
                window_days=window_days,
                view_count=view_count,
                unique_viewer_count=int(stats["unique_viewer_count"]),
                total_watch_seconds=total_watch,
                average_watch_seconds=total_watch / view_count,
                search_count=int(stats["search_count"]),
                play_count=int(stats["play_count"]),
                watch_count=int(stats["watch_count"]),
                days_since_last_view=days_since_last_view,
            )
        )

    return ContentFeatureBatch(
        observation_time=observation_time,
        window_days=window_days,
        features=feature_records,
    )
