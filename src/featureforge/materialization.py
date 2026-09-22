"""Feast materialization workflows with explicit UTC contracts and manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from feast import FeatureStore

from .manifest import MaterializationRunManifest


@dataclass(frozen=True)
class MaterializationResult:
    """Outcome of a completed Feast materialization invocation."""

    mode: str
    repo_path: Path
    start_time: datetime | None
    end_time: datetime
    manifest_path: Path


def _require_utc_timestamp(value: datetime, field_name: str) -> datetime:
    """Validate and normalize a timezone-aware timestamp to UTC."""
    if value.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware.")

    return value.astimezone(UTC)


def materialize(
    *,
    repo_path: Path,
    start_time: datetime,
    end_time: datetime,
    manifest_output_dir: Path,
) -> MaterializationResult:
    """Materialize Feast feature views for the explicit half-open UTC interval."""
    normalized_start = _require_utc_timestamp(start_time, "start_time")
    normalized_end = _require_utc_timestamp(end_time, "end_time")

    if normalized_start >= normalized_end:
        raise ValueError("start_time must be earlier than end_time.")

    started_at = datetime.now(UTC)

    store = FeatureStore(repo_path=str(repo_path))
    store.materialize(
        start_date=normalized_start,
        end_date=normalized_end,
    )

    completed_at = datetime.now(UTC)

    manifest = MaterializationRunManifest.from_run(
        mode="full",
        repo_path=repo_path,
        start_time=normalized_start,
        end_time=normalized_end,
        manifest_output_dir=manifest_output_dir,
        started_at=started_at,
        completed_at=completed_at,
    )
    manifest_path = manifest.write(manifest_output_dir)

    return MaterializationResult(
        mode="full",
        repo_path=repo_path,
        start_time=normalized_start,
        end_time=normalized_end,
        manifest_path=manifest_path,
    )


def materialize_incremental(
    *,
    repo_path: Path,
    end_time: datetime,
    manifest_output_dir: Path,
) -> MaterializationResult:
    """Materialize only data newer than Feast's registered materialization watermark."""
    normalized_end = _require_utc_timestamp(end_time, "end_time")
    started_at = datetime.now(UTC)

    store = FeatureStore(repo_path=str(repo_path))
    store.materialize_incremental(end_date=normalized_end)

    completed_at = datetime.now(UTC)

    manifest = MaterializationRunManifest.from_run(
        mode="incremental",
        repo_path=repo_path,
        start_time=None,
        end_time=normalized_end,
        manifest_output_dir=manifest_output_dir,
        started_at=started_at,
        completed_at=completed_at,
    )
    manifest_path = manifest.write(manifest_output_dir)

    return MaterializationResult(
        mode="incremental",
        repo_path=repo_path,
        start_time=None,
        end_time=normalized_end,
        manifest_path=manifest_path,
    )