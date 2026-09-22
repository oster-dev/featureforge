"""Unit tests for Feast materialization contracts and manifests."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest

from featureforge.cli import parse_utc_datetime
from featureforge.manifest import MaterializationRunManifest
from featureforge.materialization import _require_utc_timestamp


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
    manifest = MaterializationRunManifest.from_run(
        mode="full",
        repo_path=Path("feature_repo"),
        start_time=utc_datetime(2026, 1, 10),
        end_time=utc_datetime(2026, 3, 25),
        manifest_output_dir=tmp_path,
        started_at=utc_datetime(2026, 3, 25, 10),
        completed_at=utc_datetime(2026, 3, 25, 10, 1),
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
    assert payload["start_time"] == "2026-01-10T00:00:00+00:00"
    assert payload["end_time"] == "2026-03-25T00:00:00+00:00"


def test_incremental_materialization_manifest_writes_deterministic_path(
    tmp_path: Path,
) -> None:
    """An incremental manifest must omit start time and use its own path."""
    manifest = MaterializationRunManifest.from_run(
        mode="incremental",
        repo_path=Path("feature_repo"),
        start_time=None,
        end_time=utc_datetime(2026, 3, 25),
        manifest_output_dir=tmp_path,
        started_at=utc_datetime(2026, 3, 25, 10),
        completed_at=utc_datetime(2026, 3, 25, 10, 1),
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
