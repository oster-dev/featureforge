"""Feast materialization workflows with explicit UTC contracts and manifests."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from feast import FeatureStore

from .feature_quality import OfflineFeatureQualityReport, validate_offline_feature_store
from .manifest import MaterializationRunManifest


class MaterializationBlockedError(RuntimeError):
    """Raised when offline feature validation blocks a materialization run."""

    def __init__(
        self,
        message: str,
        failed_checks: list[str],
        quality_report: OfflineFeatureQualityReport,
        manifest_path: Path,
    ) -> None:
        super().__init__(message)
        self.failed_checks = failed_checks
        self.quality_report = quality_report
        self.manifest_path = manifest_path


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


_CANONICAL_OFFLINE_STORE_DIR = Path("output/offline_store")


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

    quality_report = validate_offline_feature_store(_CANONICAL_OFFLINE_STORE_DIR)

    if not quality_report.passed:
        blocked_at = datetime.now(UTC)

        manifest = MaterializationRunManifest.blocked(
            mode="full",
            repo_path=repo_path,
            offline_store_dir=_CANONICAL_OFFLINE_STORE_DIR,
            start_time=normalized_start,
            end_time=normalized_end,
            manifest_output_dir=manifest_output_dir,
            started_at=started_at,
            blocked_at=blocked_at,
            failed_checks=quality_report.failed_checks,
            quality_report=quality_report.model_dump(),
        )
        manifest_path = manifest.write(manifest_output_dir)

        raise MaterializationBlockedError(
            (
                "Offline feature validation failed. "
                f"Failed checks: {', '.join(quality_report.failed_checks)}. "
                "Materialization blocked to prevent invalid Feast write."
            ),
            failed_checks=quality_report.failed_checks,
            quality_report=quality_report,
            manifest_path=manifest_path,
        )

    store = FeatureStore(repo_path=str(repo_path))
    store.materialize(
        start_date=normalized_start,
        end_date=normalized_end,
    )

    completed_at = datetime.now(UTC)

    manifest = MaterializationRunManifest.completed(
        mode="full",
        repo_path=repo_path,
        offline_store_dir=_CANONICAL_OFFLINE_STORE_DIR,
        start_time=normalized_start,
        end_time=normalized_end,
        manifest_output_dir=manifest_output_dir,
        started_at=started_at,
        completed_at=completed_at,
        quality_report=quality_report.model_dump(),
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

    quality_report = validate_offline_feature_store(_CANONICAL_OFFLINE_STORE_DIR)

    if not quality_report.passed:
        blocked_at = datetime.now(UTC)

        manifest = MaterializationRunManifest.blocked(
            mode="incremental",
            repo_path=repo_path,
            offline_store_dir=_CANONICAL_OFFLINE_STORE_DIR,
            start_time=None,
            end_time=normalized_end,
            manifest_output_dir=manifest_output_dir,
            started_at=started_at,
            blocked_at=blocked_at,
            failed_checks=quality_report.failed_checks,
            quality_report=quality_report.model_dump(),
        )
        manifest_path = manifest.write(manifest_output_dir)

        raise MaterializationBlockedError(
            (
                "Offline feature validation failed. "
                f"Failed checks: {', '.join(quality_report.failed_checks)}. "
                "Materialization blocked to prevent invalid Feast write."
            ),
            failed_checks=quality_report.failed_checks,
            quality_report=quality_report,
            manifest_path=manifest_path,
        )

    store = FeatureStore(repo_path=str(repo_path))
    store.materialize_incremental(end_date=normalized_end)

    completed_at = datetime.now(UTC)

    manifest = MaterializationRunManifest.completed(
        mode="incremental",
        repo_path=repo_path,
        offline_store_dir=_CANONICAL_OFFLINE_STORE_DIR,
        start_time=None,
        end_time=normalized_end,
        manifest_output_dir=manifest_output_dir,
        started_at=started_at,
        completed_at=completed_at,
        quality_report=quality_report.model_dump(),
    )
    manifest_path = manifest.write(manifest_output_dir)

    return MaterializationResult(
        mode="incremental",
        repo_path=repo_path,
        start_time=None,
        end_time=normalized_end,
        manifest_path=manifest_path,
    )
