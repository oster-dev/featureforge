"""Unit tests for persisted offline feature-store quality validation."""

from __future__ import annotations

from datetime import UTC, date, datetime
from pathlib import Path

import pandas as pd

from featureforge.backfill import run_backfill
from featureforge.feature_quality import validate_offline_feature_store
from featureforge.models import (
    Content,
    Event,
    SyntheticDataset,
    User,
)


def _dataset() -> SyntheticDataset:
    """Build a minimal valid dataset that produces both feature views."""
    return SyntheticDataset(
        users=[
            User(
                user_id="user_000001",
                signup_at=datetime(2026, 1, 1, tzinfo=UTC),
                country="DE",
                plan_tier="standard",
                acquisition_channel="organic",
            )
        ],
        content_items=[
            Content(
                content_id="content_000001",
                title="Example Content",
                genre="drama",
                released_at=datetime(2026, 1, 1, tzinfo=UTC),
                duration_seconds=1_200,
            )
        ],
        events=[
            Event(
                event_id="event_000001",
                user_id="user_000001",
                content_id="content_000001",
                event_type="watch",
                event_time=datetime(2026, 3, 19, 12, tzinfo=UTC),
                ingested_at=datetime(2026, 3, 19, 12, tzinfo=UTC),
                session_id="session_000001",
                device_type="web",
                watch_seconds=120,
            )
        ],
        labels=[],
    )


def _write_valid_feature_store(offline_store_dir: Path) -> None:
    """Write one valid user and content feature partition."""
    run_backfill(
        dataset=_dataset(),
        start_date=date(2026, 3, 20),
        end_date=date(2026, 3, 20),
        window_days=7,
        output_dir=offline_store_dir,
    )


def _user_partition_path(offline_store_dir: Path) -> Path:
    """Return the deterministic user feature partition path."""
    return (
        offline_store_dir
        / "user_engagement_features"
        / "observation_date=2026-03-20"
        / "features.parquet"
    )


def test_valid_offline_feature_store_passes_quality_validation(
    tmp_path: Path,
) -> None:
    """A backfill-produced feature store must pass all persisted-data checks."""
    offline_store_dir = tmp_path / "offline_store"
    _write_valid_feature_store(offline_store_dir)

    report = validate_offline_feature_store(offline_store_dir)

    assert report.passed is True
    assert report.failed_checks == []
    assert len(report.checked_partition_paths) == 2


def test_negative_user_metric_fails_quality_validation(
    tmp_path: Path,
) -> None:
    """Negative persisted metrics must fail before online materialization."""
    offline_store_dir = tmp_path / "offline_store"
    _write_valid_feature_store(offline_store_dir)

    user_partition_path = _user_partition_path(offline_store_dir)
    user_features = pd.read_parquet(user_partition_path)
    user_features.loc[0, "event_count"] = -1
    user_features.to_parquet(user_partition_path, index=False)

    report = validate_offline_feature_store(offline_store_dir)

    assert report.passed is False
    assert (
        "user_engagement_features.observation_date=2026-03-20.non_negative_metrics"
        in report.failed_checks
    )


def test_non_numeric_user_metric_fails_quality_validation(
    tmp_path: Path,
) -> None:
    """Non-numeric persisted metrics must fail with a quality report."""
    offline_store_dir = tmp_path / "offline_store"
    _write_valid_feature_store(offline_store_dir)

    user_partition_path = _user_partition_path(offline_store_dir)
    user_features = pd.read_parquet(user_partition_path)
    user_features["event_count"] = user_features["event_count"].astype(str)
    user_features.to_parquet(user_partition_path, index=False)

    report = validate_offline_feature_store(offline_store_dir)

    assert report.passed is False
    assert (
        "user_engagement_features.observation_date=2026-03-20.numeric_feature_types"
        in report.failed_checks
    )


def test_non_datetime_observation_time_fails_quality_validation(
    tmp_path: Path,
) -> None:
    """Non-datetime persisted observation_time values must fail cleanly."""
    offline_store_dir = tmp_path / "offline_store"
    _write_valid_feature_store(offline_store_dir)

    user_partition_path = _user_partition_path(offline_store_dir)
    user_features = pd.read_parquet(user_partition_path)
    user_features["observation_time"] = user_features["observation_time"].astype(str)
    user_features.to_parquet(user_partition_path, index=False)

    report = validate_offline_feature_store(offline_store_dir)

    assert report.passed is False
    assert (
        "user_engagement_features.observation_date=2026-03-20.observation_time_type"
        in report.failed_checks
    )
