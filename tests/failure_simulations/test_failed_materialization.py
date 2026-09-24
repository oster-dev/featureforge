"""Failure simulation: failed Feast materialization."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.materialization import (
    materialize,
    materialize_incremental,
)


def test_materialization_with_missing_repo_raises_error(tmp_path: Path) -> None:
    """Materialization with missing Feast repo should raise a clear error."""
    missing_repo = tmp_path / "nonexistent_repo"

    start_time = datetime.now(UTC) - timedelta(hours=1)
    end_time = datetime.now(UTC)

    with pytest.raises(FileNotFoundError):
        materialize(
            repo_path=missing_repo,
            start_time=start_time,
            end_time=end_time,
            manifest_output_dir=tmp_path / "manifests",
        )


def test_incremental_materialization_with_missing_repo_raises_error(tmp_path: Path) -> None:
    """Incremental materialization with missing Feast repo should raise a clear error."""
    missing_repo = tmp_path / "nonexistent_repo"

    end_time = datetime.now(UTC)

    with pytest.raises(FileNotFoundError):
        materialize_incremental(
            repo_path=missing_repo,
            end_time=end_time,
            manifest_output_dir=tmp_path / "manifests",
        )
