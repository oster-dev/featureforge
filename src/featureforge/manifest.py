"""Run manifest generation for synthetic dataset generation."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
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
