from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

from featureforge.features import compute_user_engagement_features
from featureforge.models import Event, SyntheticDataset, User


def _make_user(user_id: str) -> User:
    return User(
        user_id=user_id,
        signup_at=datetime(2026, 1, 1, tzinfo=UTC),
        country="DE",
        plan_tier="premium",
        acquisition_channel="organic",
    )


def _make_event(
    event_id: str,
    user_id: str,
    event_time: datetime,
    event_type: str = "watch",
    content_id: str | None = "content_0001",
    watch_seconds: int = 120,
    session_id: str = "session_0001",
    device_type: str = "web",
) -> Event:
    return Event(
        event_id=event_id,
        user_id=user_id,
        content_id=content_id,
        event_type=event_type,
        event_time=event_time,
        ingested_at=event_time,
        session_id=session_id,
        device_type=device_type,
        watch_seconds=watch_seconds,
        is_duplicate=False,
        is_late=False,
    )


OBSERVATION_TIME = datetime(2026, 3, 15, tzinfo=UTC)


def test_excludes_events_at_or_after_observation_time():
    """Events at or after observation_time must never appear in the feature window."""
    user = _make_user("user_0001")
    in_window_event = _make_event("event_0001", "user_0001", OBSERVATION_TIME - timedelta(days=1))
    future_event = _make_event("event_0002", "user_0001", OBSERVATION_TIME + timedelta(seconds=1))
    boundary_event = _make_event("event_0003", "user_0001", OBSERVATION_TIME)

    dataset = SyntheticDataset(
        users=[user],
        content_items=[],
        events=[in_window_event, future_event, boundary_event],
        labels=[],
    )

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    feature = batch.features[0]
    assert feature.event_count == 2


def test_excludes_events_before_window_start():
    """Events older than the lookback window must not contribute to counts."""
    user = _make_user("user_0001")
    inside_event = _make_event("event_0001", "user_0001", OBSERVATION_TIME - timedelta(days=6))
    outside_event = _make_event("event_0002", "user_0001", OBSERVATION_TIME - timedelta(days=8))

    dataset = SyntheticDataset(
        users=[user],
        content_items=[],
        events=[inside_event, outside_event],
        labels=[],
    )

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    assert batch.features[0].event_count == 1


def test_user_with_no_events_gets_zeroed_features():
    """A user with no activity in the window must still produce a valid record."""
    user = _make_user("user_0001")
    dataset = SyntheticDataset(users=[user], content_items=[], events=[], labels=[])

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    feature = batch.features[0]
    assert feature.event_count == 0
    assert feature.unique_content_count == 0
    assert feature.total_watch_seconds == 0
    assert feature.days_since_last_activity is None


def test_counts_typed_events_correctly():
    """Search, play, and watch events must be classified into separate counters."""
    user = _make_user("user_0001")
    events = [
        _make_event(
            "event_0001",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=1),
            event_type="search",
            content_id=None,
            watch_seconds=0,
        ),
        _make_event(
            "event_0002",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=2),
            event_type="play",
        ),
        _make_event(
            "event_0003",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=3),
            event_type="watch",
        ),
    ]
    dataset = SyntheticDataset(users=[user], content_items=[], events=events, labels=[])

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    feature = batch.features[0]
    assert feature.search_count == 1
    assert feature.play_count == 1
    assert feature.watch_count == 1
    assert feature.event_count == 3


def test_computes_unique_content_count():
    """Repeated interactions with the same content must count once for uniqueness."""
    user = _make_user("user_0001")
    events = [
        _make_event(
            "event_0001",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=1),
            content_id="content_0001",
        ),
        _make_event(
            "event_0002",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=2),
            content_id="content_0001",
        ),
        _make_event(
            "event_0003",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=3),
            content_id="content_0002",
        ),
    ]
    dataset = SyntheticDataset(users=[user], content_items=[], events=events, labels=[])

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    assert batch.features[0].unique_content_count == 2


def test_sums_watch_seconds_within_window():
    """Total watch time must sum only in-window watch_seconds values."""
    user = _make_user("user_0001")
    events = [
        _make_event(
            "event_0001",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=1),
            watch_seconds=100,
        ),
        _make_event(
            "event_0002",
            "user_0001",
            OBSERVATION_TIME - timedelta(hours=2),
            watch_seconds=50,
        ),
    ]
    dataset = SyntheticDataset(users=[user], content_items=[], events=events, labels=[])

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    assert batch.features[0].total_watch_seconds == 150


def test_days_since_last_activity_is_computed_from_most_recent_event():
    """days_since_last_activity must reflect the most recent in-window event."""
    user = _make_user("user_0001")
    events = [
        _make_event("event_0001", "user_0001", OBSERVATION_TIME - timedelta(days=2)),
        _make_event("event_0002", "user_0001", OBSERVATION_TIME - timedelta(days=5)),
    ]
    dataset = SyntheticDataset(users=[user], content_items=[], events=events, labels=[])

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    assert batch.features[0].days_since_last_activity == pytest.approx(2.0)


def test_rejects_non_positive_window_days():
    """window_days must be strictly positive."""
    user = _make_user("user_0001")
    dataset = SyntheticDataset(users=[user], content_items=[], events=[], labels=[])

    with pytest.raises(ValueError, match="window_days must be positive"):
        compute_user_engagement_features(dataset, observation_time=OBSERVATION_TIME, window_days=0)


def test_produces_one_feature_record_per_user():
    """The output batch must contain exactly one record per user in the dataset."""
    users = [_make_user(f"user_{i:04d}") for i in range(5)]
    dataset = SyntheticDataset(users=users, content_items=[], events=[], labels=[])

    batch = compute_user_engagement_features(
        dataset, observation_time=OBSERVATION_TIME, window_days=7
    )

    assert len(batch.features) == 5
    assert {f.user_id for f in batch.features} == {u.user_id for u in users}
