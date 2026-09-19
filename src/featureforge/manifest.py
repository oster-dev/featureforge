"""Run manifest generation for synthetic dataset generation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

from .config import SyntheticDataConfig
from .models import DatasetQualityReport, SyntheticDataset


@dataclass(frozen=True)
class GenerationRunManifest:
    """Metadata for a single synthetic dataset generation run."""

    generated_at: str
    config: dict[str, Any]
    row_counts: dict[str, int]
    quality_report: dict[str, Any]
    output_paths: dict[str, str]

    @classmethod
    def from_run(
        cls,
        config: SyntheticDataConfig,
        dataset: SyntheticDataset,
        quality_report: DatasetQualityReport,
        output_paths: dict[str, Path],
    ) -> GenerationRunManifest:
        """Create a manifest from a completed generation run."""
        return cls(
            generated_at=datetime.now(UTC).isoformat(),
            config=config.model_dump(),
            row_counts={
                "users": len(dataset.users),
                "content": len(dataset.content_items),
                "events": len(dataset.events),
                "labels": len(dataset.labels),
            },
            quality_report=quality_report.model_dump(),
            output_paths={name: str(path) for name, path in output_paths.items()},
        )

    def write(self, output_dir: Path) -> Path:
        """Write the manifest to a JSON file in the given directory."""
        output_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = output_dir / "run_manifest.json"

        manifest_dict = asdict(self)

        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_dict, f, indent=2, default=str)

        return manifest_path


@dataclass(frozen=True)
class BackfillRunManifest:
    """Metadata for a completed feature backfill run."""

    run_type: str
    started_at: str
    completed_at: str
    status: str
    start_date: str
    end_date: str
    window_days: int
    output_dir: str
    partitions: list[dict[str, Any]]
    engine: str = "pandas"

    @classmethod
    def from_run(
        cls,
        start_date: date,
        end_date: date,
        window_days: int,
        output_dir: Path,
        started_at: datetime,
        completed_at: datetime,
        partitions: list[dict[str, Any]],
        engine: str = "pandas",
    ) -> BackfillRunManifest:
        """Create a manifest from a successful completed backfill run."""
        return cls(
            run_type="feature_backfill",
            started_at=started_at.astimezone(UTC).isoformat(),
            completed_at=completed_at.astimezone(UTC).isoformat(),
            status="completed",
            start_date=start_date.isoformat(),
            end_date=end_date.isoformat(),
            window_days=window_days,
            output_dir=str(output_dir),
            partitions=partitions,
            engine=engine,
        )

    def write(self, output_dir: Path) -> Path:
        """Write the backfill manifest using a deterministic file name."""
        manifests_dir = output_dir / "manifests"
        manifests_dir.mkdir(parents=True, exist_ok=True)

        manifest_path = manifests_dir / f"backfill-{self.start_date}-to-{self.end_date}.json"

        with open(manifest_path, "w", encoding="utf-8") as file:
            json.dump(asdict(self), file, indent=2, default=str)

        return manifest_path
