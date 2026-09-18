"""Parity tests: PySpark feature computation must match the pandas reference exactly.

These tests do not re-verify feature *semantics* (that is the job of
tests/unit/test_features.py). They verify that the Spark implementation
in featureforge.spark_features produces byte-for-byte identical
UserFeatureBatch / ContentFeatureBatch objects to the pandas
implementation in featureforge.features, across a range of scenarios:
empty datasets, boundary events, typed event counts, duplicate content,
and multi-entity batches.
"""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta

import pytest
from pyspark.sql import SparkSession

from featureforge.features import (
    compute_content_popularity_features,
    compute_user_engagement_features,
)
from featureforge.models import Content, Event, SyntheticDataset, User
from featureforge.spark_features import (
    compute_content_popularity_features_spark,
    compute_user_engagement_features_spark,
)

OBSERVATION_TIME = datetime(2026, 3, 15, tzinfo=UTC)
EVENT_TYPES = ["search", "play", "watch", "browse"]


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("featureforge-parity-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        # Pin Spark's session timezone to UTC. Without this, Spark
        # interprets/serializes TimestampType values using the JVM's
        # local timezone, which silently shifts timestamps on round-trip
        # through collect() on any machine not already running in UTC.
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def _make_user(user_id: str) -> User:
    return User(
        user_id=user_id,
        signup_at=datetime(2026, 1, 1, tzinfo=UTC),
        country="DE",
        plan_tier="premium",
        acquisition_channel="organic",
    )


def _make_content(content_id: str) -> Content:
    return Content(
        content_id=content_id,
        title=f"Title for {content_id}",
        genre="drama",
        released_at=datetime(2020, 1, 1, tzinfo=UTC),
        duration_seconds=3600,
    )


def _make_event(
    event_id: str,
    user_id: str,
    event_time: datetime,
    event_type: str = "watch",
    content_id: str | None = "content_0001",
    watch_seconds: int = 120,
) -> Event:
    return Event(
        event_id=event_id,
        user_id=user_id,
        content_id=content_id,
        event_type=event_type,
        event_time=event_time,
        ingested_at=event_time,
        session_id="session_0001",
        device_type="web",
        watch_seconds=watch_seconds,
        is_duplicate=False,
        is_late=False,
    )


def _build_random_dataset(
    num_users: int, num_content: int, num_events: int, seed: int
) -> SyntheticDataset:
    rng = random.Random(seed)
    users = [_make_user(f"user_{i:04d}") for i in range(num_users)]
    content_items = [_make_content(f"content_{i:04d}") for i in range(num_content)]

    events = []
    for i in range(num_events):
        offset_hours = rng.randint(0, 24 * 14)
        event_time = OBSERVATION_TIME - timedelta(hours=offset_hours)
        events.append(
            _make_event(
                event_id=f"event_{i:05d}",
                user_id=rng.choice(users).user_id,
                event_time=event_time,
                event_type=rng.choice(EVENT_TYPES),
                content_id=rng.choice(content_items).content_id,
                watch_seconds=rng.randint(0, 3600),
            )
        )

    return SyntheticDataset(users=users, content_items=content_items, events=events, labels=[])


def test_user_features_parity_empty_dataset(spark):
    """Users with zero events must match between pandas and Spark."""
    dataset = SyntheticDataset(
        users=[_make_user("user_0001")], content_items=[], events=[], labels=[]
    )

    pandas_batch = compute_user_engagement_features(dataset, OBSERVATION_TIME, window_days=7)
    spark_batch = compute_user_engagement_features_spark(
        spark, dataset, OBSERVATION_TIME, window_days=7
    )

    assert pandas_batch == spark_batch


def test_user_features_parity_boundary_events(spark):
    """Boundary/off-by-one event times must match between engines."""
    user = _make_user("user_0001")
    events = [
        _make_event("event_0001", "user_0001", OBSERVATION_TIME - timedelta(days=1)),
        _make_event("event_0002", "user_0001", OBSERVATION_TIME),
        _make_event("event_0003", "user_0001", OBSERVATION_TIME + timedelta(seconds=1)),
        _make_event("event_0004", "user_0001", OBSERVATION_TIME - timedelta(days=7)),
        _make_event("event_0005", "user_0001", OBSERVATION_TIME - timedelta(days=6, hours=23)),
    ]
    dataset = SyntheticDataset(users=[user], content_items=[], events=events, labels=[])

    pandas_batch = compute_user_engagement_features(dataset, OBSERVATION_TIME, window_days=7)
    spark_batch = compute_user_engagement_features_spark(
        spark, dataset, OBSERVATION_TIME, window_days=7
    )

    assert pandas_batch == spark_batch


def test_user_features_parity_typed_events_and_multiple_users(spark):
    """Typed event counts across multiple users must match exactly."""
    dataset = _build_random_dataset(num_users=8, num_content=5, num_events=200, seed=42)

    pandas_batch = compute_user_engagement_features(dataset, OBSERVATION_TIME, window_days=14)
    spark_batch = compute_user_engagement_features_spark(
        spark, dataset, OBSERVATION_TIME, window_days=14
    )

    pandas_by_user = {f.user_id: f for f in pandas_batch.features}
    spark_by_user = {f.user_id: f for f in spark_batch.features}

    assert pandas_by_user.keys() == spark_by_user.keys()
    for user_id in pandas_by_user:
        assert pandas_by_user[user_id] == spark_by_user[user_id]


def test_user_features_parity_rejects_non_positive_window_days(spark):
    """Both engines must reject the same invalid window_days input identically."""
    dataset = SyntheticDataset(
        users=[_make_user("user_0001")], content_items=[], events=[], labels=[]
    )

    with pytest.raises(ValueError, match="window_days must be positive"):
        compute_user_engagement_features(dataset, OBSERVATION_TIME, window_days=0)

    with pytest.raises(ValueError, match="window_days must be positive"):
        compute_user_engagement_features_spark(spark, dataset, OBSERVATION_TIME, window_days=0)


def test_content_features_parity_empty_dataset(spark):
    """Content with zero events must match between pandas and Spark."""
    dataset = SyntheticDataset(
        users=[], content_items=[_make_content("content_0001")], events=[], labels=[]
    )

    pandas_batch = compute_content_popularity_features(dataset, OBSERVATION_TIME, window_days=7)
    spark_batch = compute_content_popularity_features_spark(
        spark, dataset, OBSERVATION_TIME, window_days=7
    )

    assert pandas_batch == spark_batch


def test_content_features_parity_boundary_events(spark):
    """Boundary/off-by-one event times must match between engines for content features."""
    content = _make_content("content_0001")
    events = [
        _make_event("event_0001", "user_0001", OBSERVATION_TIME - timedelta(days=1)),
        _make_event("event_0002", "user_0002", OBSERVATION_TIME),
        _make_event("event_0003", "user_0003", OBSERVATION_TIME + timedelta(seconds=1)),
        _make_event("event_0004", "user_0004", OBSERVATION_TIME - timedelta(days=7)),
    ]
    dataset = SyntheticDataset(users=[], content_items=[content], events=events, labels=[])

    pandas_batch = compute_content_popularity_features(dataset, OBSERVATION_TIME, window_days=7)
    spark_batch = compute_content_popularity_features_spark(
        spark, dataset, OBSERVATION_TIME, window_days=7
    )

    assert pandas_batch == spark_batch


def test_content_features_parity_average_watch_seconds(spark):
    """average_watch_seconds must match exactly, including its float division."""
    content = _make_content("content_0001")
    events = [
        _make_event(
            "event_0001", "user_0001", OBSERVATION_TIME - timedelta(hours=1), watch_seconds=333
        ),
        _make_event(
            "event_0002", "user_0002", OBSERVATION_TIME - timedelta(hours=2), watch_seconds=777
        ),
        _make_event(
            "event_0003", "user_0003", OBSERVATION_TIME - timedelta(hours=3), watch_seconds=101
        ),
    ]
    dataset = SyntheticDataset(users=[], content_items=[content], events=events, labels=[])

    pandas_batch = compute_content_popularity_features(dataset, OBSERVATION_TIME, window_days=7)
    spark_batch = compute_content_popularity_features_spark(
        spark, dataset, OBSERVATION_TIME, window_days=7
    )

    assert pandas_batch == spark_batch
    assert pandas_batch.features[0].average_watch_seconds == pytest.approx(
        spark_batch.features[0].average_watch_seconds
    )


def test_content_features_parity_multiple_content_and_random_data(spark):
    """Full random dataset across many content items must match exactly."""
    dataset = _build_random_dataset(num_users=10, num_content=12, num_events=300, seed=7)

    pandas_batch = compute_content_popularity_features(dataset, OBSERVATION_TIME, window_days=10)
    spark_batch = compute_content_popularity_features_spark(
        spark, dataset, OBSERVATION_TIME, window_days=10
    )

    pandas_by_content = {f.content_id: f for f in pandas_batch.features}
    spark_by_content = {f.content_id: f for f in spark_batch.features}

    assert pandas_by_content.keys() == spark_by_content.keys()
    for content_id in pandas_by_content:
        assert pandas_by_content[content_id] == spark_by_content[content_id]


def test_content_features_parity_rejects_non_positive_window_days(spark):
    """Both engines must reject the same invalid window_days input identically."""
    dataset = SyntheticDataset(
        users=[], content_items=[_make_content("content_0001")], events=[], labels=[]
    )

    with pytest.raises(ValueError, match="window_days must be positive"):
        compute_content_popularity_features(dataset, OBSERVATION_TIME, window_days=-1)

    with pytest.raises(ValueError, match="window_days must be positive"):
        compute_content_popularity_features_spark(spark, dataset, OBSERVATION_TIME, window_days=-1)
