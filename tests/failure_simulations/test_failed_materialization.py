"""Failure simulation: failed Feast materialization."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from featureforge.feature_quality import OfflineFeatureQualityReport
from featureforge.materialization import materialize, materialize_incremental


def _passing_quality_report() -> OfflineFeatureQualityReport:
    """Return a minimal passing canonical offline-store quality report."""
    return OfflineFeatureQualityReport(
        offline_store_dir="output/offline_store",
        checked_partition_paths=[],
        checks=[],
    )


def test_materialization_with_missing_repo_raises_error(tmp_path: Path) -> None:
    """A missing Feast repository must fail after canonical validation passes."""
    missing_repo = tmp_path / "nonexistent_repo"
    start_time = datetime.now(UTC) - timedelta(hours=1)
    end_time = datetime.now(UTC)

    with (
        patch(
            "featureforge.materialization.validate_offline_feature_store",
            return_value=_passing_quality_report(),
        ),
        pytest.raises(FileNotFoundError),
    ):
        materialize(
            repo_path=missing_repo,
            start_time=start_time,
            end_time=end_time,
            manifest_output_dir=tmp_path / "manifests",
        )


def test_incremental_materialization_with_missing_repo_raises_error(
    tmp_path: Path,
) -> None:
    """A missing Feast repository must fail after canonical validation passes."""
    missing_repo = tmp_path / "nonexistent_repo"
    end_time = datetime.now(UTC)

    with (
        patch(
            "featureforge.materialization.validate_offline_feature_store",
            return_value=_passing_quality_report(),
        ),
        pytest.raises(FileNotFoundError),
    ):
        materialize_incremental(
            repo_path=missing_repo,
            end_time=end_time,
            manifest_output_dir=tmp_path / "manifests",
        )
