"""Unit tests for Feast materialization contracts and manifests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from featureforge.cli import parse_utc_datetime
from featureforge.manifest import MaterializationRunManifest
from featureforge.materialization import (
    MaterializationBlockedError,
    _require_utc_timestamp,
    materialize,
    materialize_incremental,
)


def utc_datetime(
    year: int,
    month: int,
    day: int,
    hour: int = 0,
    minute: int = 0,
) -> datetime:
    """Build a timezone-aware UTC datetime for one test."""
    return datetime(year, month, day, hour, minute, tzinfo=UTC)


def test_require_utc_timestamp_rejects_naive_datetime() -> None:
    """Materialization timestamps must declare a timezone."""
    with pytest.raises(ValueError, match="start_time must be timezone-aware"):
        _require_utc_timestamp(datetime(2026, 3, 24), "start_time")


def test_require_utc_timestamp_normalizes_to_utc() -> None:
    """Timezone-aware values must normalize to the equivalent UTC time."""
    berlin_time = datetime.fromisoformat("2026-03-24T01:00:00+01:00")

    assert _require_utc_timestamp(berlin_time, "end_time") == utc_datetime(
        2026,
        3,
        24,
    )


def test_parse_utc_datetime_rejects_timestamp_without_timezone() -> None:
    """CLI timestamps must include an explicit timezone offset."""
    with pytest.raises(ValueError, match="must include an explicit timezone"):
        parse_utc_datetime("2026-03-24T00:00:00")


def test_parse_utc_datetime_normalizes_to_utc() -> None:
    """CLI timestamps must convert valid ISO 8601 offsets to UTC."""
    assert parse_utc_datetime("2026-03-24T01:00:00+01:00") == utc_datetime(
        2026,
        3,
        24,
    )


def test_full_materialization_manifest_writes_deterministic_path(
    tmp_path: Path,
) -> None:
    """A full run manifest must include its explicit UTC time interval."""
    manifest = MaterializationRunManifest.completed(
        mode="full",
        repo_path=Path("feature_repo"),
        offline_store_dir=Path("output/offline_store"),
        start_time=utc_datetime(2026, 1, 10),
        end_time=utc_datetime(2026, 3, 25),
        manifest_output_dir=tmp_path,
        started_at=utc_datetime(2026, 3, 25, 10),
        completed_at=utc_datetime(2026, 3, 25, 10, 1),
        quality_report={"passed": True, "failed_checks": []},
    )

    manifest_path = manifest.write(tmp_path)

    assert manifest_path == (
        tmp_path
        / "materialization_manifests"
        / "materialize-2026-01-10T000000+0000-to-2026-03-25T000000+0000.json"
    )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["run_type"] == "feast_materialization"
    assert payload["status"] == "completed"
    assert payload["mode"] == "full"
    assert payload["repo_path"] == "feature_repo"
    assert payload["offline_store_dir"] == "output/offline_store"
    assert payload["start_time"] == "2026-01-10T00:00:00+00:00"
    assert payload["end_time"] == "2026-03-25T00:00:00+00:00"
    assert payload["quality_report"] == {"passed": True, "failed_checks": []}


def test_incremental_materialization_manifest_writes_deterministic_path(
    tmp_path: Path,
) -> None:
    """An incremental manifest must omit start time and use its own path."""
    manifest = MaterializationRunManifest.completed(
        mode="incremental",
        repo_path=Path("feature_repo"),
        offline_store_dir=Path("output/offline_store"),
        start_time=None,
        end_time=utc_datetime(2026, 3, 25),
        manifest_output_dir=tmp_path,
        started_at=utc_datetime(2026, 3, 25, 10),
        completed_at=utc_datetime(2026, 3, 25, 10, 1),
        quality_report={"passed": True, "failed_checks": []},
    )

    manifest_path = manifest.write(tmp_path)

    assert manifest_path == (
        tmp_path
        / "materialization_manifests"
        / "materialize-incremental-to-2026-03-25T000000+0000.json"
    )

    payload = json.loads(manifest_path.read_text(encoding="utf-8"))

    assert payload["mode"] == "incremental"
    assert payload["start_time"] is None
    assert payload["end_time"] == "2026-03-25T00:00:00+00:00"
    assert payload["quality_report"] == {"passed": True, "failed_checks": []}


def _create_minimal_valid_offline_store(
    base_dir: Path,
    *,
    negative_metric: bool = False,
    non_numeric_metric: bool = False,
) -> None:
    """Create a minimal valid offline store with Hive-partitioned directories."""
    user_partition_dir = base_dir / "user_engagement_features" / "observation_date=2026-01-10"
    user_partition_dir.mkdir(parents=True)

    user_df = pd.DataFrame(
        {
            "user_id": [1, 2, 3],
            "observation_time": pd.to_datetime(
                ["2026-01-10T00:00:00+00:00"] * 3,
                utc=True,
            ),
            "window_days": [7, 7, 7],
            "event_count": [10, 5, 15],
            "unique_content_count": [2, 1, 4],
            "total_watch_seconds": [100, 50, 200],
            "search_count": [2, 1, 3],
            "play_count": [3, 2, 5],
            "watch_count": [2, 1, 4],
            "days_since_last_activity": [1, 2, 0],
        }
    )

    if negative_metric:
        user_df.loc[1, "event_count"] = -3
    elif non_numeric_metric:
        user_df["event_count"] = ["high", "medium", "low"]

    user_df.to_parquet(user_partition_dir / "features.parquet")

    content_partition_dir = base_dir / "content_popularity_features" / "observation_date=2026-01-10"
    content_partition_dir.mkdir(parents=True)

    content_df = pd.DataFrame(
        {
            "content_id": [100, 101, 102],
            "observation_time": pd.to_datetime(
                ["2026-01-10T00:00:00+00:00"] * 3,
                utc=True,
            ),
            "window_days": [7, 7, 7],
            "view_count": [20, 15, 30],
            "unique_viewer_count": [10, 8, 20],
            "total_watch_seconds": [500, 300, 1000],
            "average_watch_seconds": [10.0, 12.5, 15.0],
            "search_count": [5, 3, 8],
            "play_count": [5, 4, 10],
            "watch_count": [5, 3, 7],
            "days_since_last_view": [0, 1, 2],
        }
    )
    content_df.to_parquet(content_partition_dir / "features.parquet")


def test_blocked_full_materialization_writes_blocked_manifest_and_raises(
    tmp_path: Path,
) -> None:
    """A failed offline feature validation must block materialization and write a blocked manifest."""
    start_time = utc_datetime(2026, 1, 10)
    end_time = utc_datetime(2026, 3, 25)
    offline_store_dir = tmp_path / "offline_store"

    _create_minimal_valid_offline_store(
        offline_store_dir,
        negative_metric=True,
    )

    with pytest.raises(MaterializationBlockedError) as exc_info:
        materialize(
            repo_path=Path("feature_repo"),
            start_time=start_time,
            end_time=end_time,
            manifest_output_dir=tmp_path,
            offline_store_dir=offline_store_dir,
        )

    exc = exc_info.value
    assert any("non_negative_metrics" in check for check in exc.failed_checks)
    assert exc.manifest_path.exists()

    payload = json.loads(exc.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "blocked"
    assert payload["mode"] == "full"
    assert any("non_negative_metrics" in check for check in payload["failed_checks"])


def test_completed_full_materialization_calls_feast_once_and_writes_manifest(
    tmp_path: Path,
) -> None:
    """A valid offline store must call Feast.materialize once and write a completed manifest."""
    start_time = utc_datetime(2026, 1, 10)
    end_time = utc_datetime(2026, 3, 25)
    offline_store_dir = tmp_path / "offline_store"

    _create_minimal_valid_offline_store(offline_store_dir)

    with patch("featureforge.materialization.FeatureStore") as MockFeatureStore:
        mock_store = MockFeatureStore.return_value
        result = materialize(
            repo_path=Path("feature_repo"),
            start_time=start_time,
            end_time=end_time,
            manifest_output_dir=tmp_path,
            offline_store_dir=offline_store_dir,
        )

        mock_store.materialize.assert_called_once_with(
            start_date=start_time,
            end_date=end_time,
        )

    assert result.manifest_path.exists()
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert payload["mode"] == "full"


def test_blocked_incremental_materialization_writes_blocked_manifest_and_raises(
    tmp_path: Path,
) -> None:
    """A failed offline feature validation must block incremental materialization."""
    end_time = utc_datetime(2026, 3, 25)
    offline_store_dir = tmp_path / "offline_store"

    _create_minimal_valid_offline_store(
        offline_store_dir,
        non_numeric_metric=True,
    )

    with pytest.raises(MaterializationBlockedError) as exc_info:
        materialize_incremental(
            repo_path=Path("feature_repo"),
            end_time=end_time,
            manifest_output_dir=tmp_path,
            offline_store_dir=offline_store_dir,
        )

    exc = exc_info.value
    assert any("numeric_feature_types" in check for check in exc.failed_checks)
    assert exc.manifest_path.exists()

    payload = json.loads(exc.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "blocked"
    assert payload["mode"] == "incremental"
    assert any("numeric_feature_types" in check for check in payload["failed_checks"])


def test_completed_incremental_materialization_calls_feast_once_and_writes_manifest(
    tmp_path: Path,
) -> None:
    """A valid offline store must call Feast.materialize_incremental once."""
    end_time = utc_datetime(2026, 3, 25)
    offline_store_dir = tmp_path / "offline_store"

    _create_minimal_valid_offline_store(offline_store_dir)

    with patch("featureforge.materialization.FeatureStore") as MockFeatureStore:
        mock_store = MockFeatureStore.return_value
        result = materialize_incremental(
            repo_path=Path("feature_repo"),
            end_time=end_time,
            manifest_output_dir=tmp_path,
            offline_store_dir=offline_store_dir,
        )

        mock_store.materialize_incremental.assert_called_once_with(
            end_date=end_time,
        )

    assert result.manifest_path.exists()
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert payload["mode"] == "incremental"
