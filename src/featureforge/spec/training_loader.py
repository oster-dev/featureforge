"""Read a training dataset back through its manifest (ADR-014).

The loader needs pandas and pyarrow, not Spark. It repeats three constants of
training_dataset.py because that module imports PySpark at import time.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

MANIFEST_NAME = "_manifest.json"
SUPPORTED_MANIFEST_VERSION = 1
PARTITION_COLUMN = "observation_date"


class TrainingDatasetError(ValueError):
    """The directory is not a complete and consistent training dataset."""


@dataclass(frozen=True)
class LoadedDataset:
    """The rows of a dataset, in manifest column order, and its manifest."""

    frame: pd.DataFrame
    manifest: dict[str, Any]


def read_manifest(path: Path) -> dict[str, Any]:
    """Return the manifest of a dataset directory after the checks every reader needs."""
    if not path.is_dir():
        raise FileNotFoundError(f"dataset directory does not exist: {path}")
    manifest_path = path / MANIFEST_NAME
    if not manifest_path.is_file():
        raise TrainingDatasetError(
            f"{MANIFEST_NAME} is missing in {path}; the dataset is not complete"
        )
    manifest: dict[str, Any] = json.loads(manifest_path.read_text(encoding="utf-8"))
    version = manifest.get("manifest_version")
    if version != SUPPORTED_MANIFEST_VERSION:
        raise TrainingDatasetError(
            f"unsupported manifest_version {version!r}, "
            f"this loader reads {SUPPORTED_MANIFEST_VERSION}"
        )
    return manifest


def load_training_dataset(path: Path) -> LoadedDataset:
    """Load a dataset and check the files against its manifest."""
    manifest = read_manifest(path)
    columns: list[str] = manifest["columns"]
    frame = pd.read_parquet(path)
    missing = [name for name in [*columns, PARTITION_COLUMN] if name not in frame.columns]
    if missing:
        raise TrainingDatasetError(f"dataset lacks columns from the manifest: {missing}")

    if len(frame) != manifest["rows"]:
        raise TrainingDatasetError(
            f"manifest says {manifest['rows']} rows, the files hold {len(frame)}"
        )
    actual = {
        str(day): int(count)
        for day, count in frame[PARTITION_COLUMN].astype(str).value_counts().items()
    }
    expected = {str(part[PARTITION_COLUMN]): int(part["rows"]) for part in manifest["partitions"]}
    if actual != expected:
        raise TrainingDatasetError(
            f"partition counts differ from the manifest: files {actual}, manifest {expected}"
        )

    sort_columns = columns[: columns.index("observation_time") + 1]
    ordered = frame[columns].sort_values(sort_columns).reset_index(drop=True)
    return LoadedDataset(frame=ordered, manifest=manifest)
