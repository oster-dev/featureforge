"""Unit tests for Feast materialization contracts and manifests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from featureforge.cli import parse_utc_datetime
from featureforge.feature_quality import FeatureQualityCheck, OfflineFeatureQualityReport
from featureforge.manifest import MaterializationRunManifest
from featureforge.materialization import (
    _CANONICAL_OFFLINE_STORE_DIR,
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


def test_blocked_full_materialization_writes_blocked_manifest_and_raises(
    tmp_path: Path,
) -> None:
    """A failed offline feature validation must block materialization and write a blocked manifest."""
    start_time = utc_datetime(2026, 1, 10)
    end_time = utc_datetime(2026, 3, 25)

    failed_report = OfflineFeatureQualityReport(
        offline_store_dir=str(_CANONICAL_OFFLINE_STORE_DIR),
        checked_partition_paths=[
            "output/offline_store/user_engagement_features/observation_date=2026-01-10"
        ],
        checks=[
            FeatureQualityCheck(
                name="non_negative_metrics",
                passed=False,
                message="Negative event_count detected.",
                affected_row_count=1,
            )
        ],
    )

    with patch(
        "featureforge.materialization.validate_offline_feature_store",
        return_value=failed_report,
    ):
        with pytest.raises(MaterializationBlockedError) as exc_info:
            materialize(
                repo_path=Path("feature_repo"),
                start_time=start_time,
                end_time=end_time,
                manifest_output_dir=tmp_path,
            )

    exc = exc_info.value
    assert exc.failed_checks == ["non_negative_metrics"]
    assert exc.manifest_path.exists()

    payload = json.loads(exc.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "blocked"
    assert payload["mode"] == "full"
    assert payload["failed_checks"] == ["non_negative_metrics"]


def test_completed_full_materialization_calls_feast_once_and_writes_manifest(
    tmp_path: Path,
) -> None:
    """A valid offline store must call Feast.materialize once and write a completed manifest."""
    start_time = utc_datetime(2026, 1, 10)
    end_time = utc_datetime(2026, 3, 25)

    valid_report = OfflineFeatureQualityReport(
        offline_store_dir=str(_CANONICAL_OFFLINE_STORE_DIR),
        checked_partition_paths=[
            "output/offline_store/user_engagement_features/observation_date=2026-01-10"
        ],
        checks=[
            FeatureQualityCheck(
                name="non_negative_metrics",
                passed=True,
                message="All metrics are non-negative.",
            )
        ],
    )

    with patch(
        "featureforge.materialization.validate_offline_feature_store",
        return_value=valid_report,
    ):
        with patch("featureforge.materialization.FeatureStore") as MockFeatureStore:
            mock_store = MockFeatureStore.return_value
            result = materialize(
                repo_path=Path("feature_repo"),
                start_time=start_time,
                end_time=end_time,
                manifest_output_dir=tmp_path,
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

    failed_report = OfflineFeatureQualityReport(
        offline_store_dir=str(_CANONICAL_OFFLINE_STORE_DIR),
        checked_partition_paths=[
            "output/offline_store/user_engagement_features/observation_date=2026-01-10"
        ],
        checks=[
            FeatureQualityCheck(
                name="numeric_feature_types",
                passed=False,
                message="Non-numeric feature type detected.",
                affected_row_count=3,
            )
        ],
    )

    with patch(
        "featureforge.materialization.validate_offline_feature_store",
        return_value=failed_report,
    ):
        with pytest.raises(MaterializationBlockedError) as exc_info:
            materialize_incremental(
                repo_path=Path("feature_repo"),
                end_time=end_time,
                manifest_output_dir=tmp_path,
            )

    exc = exc_info.value
    assert exc.failed_checks == ["numeric_feature_types"]
    assert exc.manifest_path.exists()

    payload = json.loads(exc.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "blocked"
    assert payload["mode"] == "incremental"
    assert payload["failed_checks"] == ["numeric_feature_types"]


def test_completed_incremental_materialization_calls_feast_once_and_writes_manifest(
    tmp_path: Path,
) -> None:
    """A valid offline store must call Feast.materialize_incremental once."""
    end_time = utc_datetime(2026, 3, 25)

    valid_report = OfflineFeatureQualityReport(
        offline_store_dir=str(_CANONICAL_OFFLINE_STORE_DIR),
        checked_partition_paths=[
            "output/offline_store/user_engagement_features/observation_date=2026-01-10"
        ],
        checks=[
            FeatureQualityCheck(
                name="non_negative_metrics",
                passed=True,
                message="All metrics are non-negative.",
            )
        ],
    )

    with patch(
        "featureforge.materialization.validate_offline_feature_store",
        return_value=valid_report,
    ):
        with patch("featureforge.materialization.FeatureStore") as MockFeatureStore:
            mock_store = MockFeatureStore.return_value
            result = materialize_incremental(
                repo_path=Path("feature_repo"),
                end_time=end_time,
                manifest_output_dir=tmp_path,
            )

            mock_store.materialize_incremental.assert_called_once_with(
                end_date=end_time,
            )

    assert result.manifest_path.exists()
    payload = json.loads(result.manifest_path.read_text(encoding="utf-8"))
    assert payload["status"] == "completed"
    assert payload["mode"] == "incremental"