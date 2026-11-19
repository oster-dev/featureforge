"""Read a training dataset in Arrow batches through its manifest (ADR-014).

The checks run when the dataset is opened and use the Parquet metadata, so no row
is read before the first batch. Batches come in a fixed order, by file path and so
by partition day. Unlike load_training_dataset the rows are not sorted by key,
because that needs the whole dataset in memory.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.dataset as ds

from featureforge.spec.training_loader import (
    PARTITION_COLUMN,
    TrainingDatasetError,
    read_manifest,
)

DEFAULT_BATCH_SIZE = 65_536


@dataclass(frozen=True)
class BatchedDataset:
    """A checked dataset that yields Arrow record batches in manifest column order."""

    manifest: dict[str, Any]
    dataset: Any

    @property
    def columns(self) -> list[str]:
        return list(self.manifest["columns"])

    def batches(self, batch_size: int = DEFAULT_BATCH_SIZE) -> Iterator[pa.RecordBatch]:
        fragments = sorted(self.dataset.get_fragments(), key=lambda fragment: fragment.path)
        for fragment in fragments:
            yield from fragment.to_batches(columns=self.columns, batch_size=batch_size)


def open_training_batches(path: Path) -> BatchedDataset:
    """Open a dataset and check the files against its manifest without reading rows."""
    manifest = read_manifest(path)
    partitioning = ds.partitioning(pa.schema([(PARTITION_COLUMN, pa.string())]), flavor="hive")
    dataset = ds.dataset(str(path), format="parquet", partitioning=partitioning)

    names = set(dataset.schema.names)
    missing = [name for name in [*manifest["columns"], PARTITION_COLUMN] if name not in names]
    if missing:
        raise TrainingDatasetError(f"dataset lacks columns from the manifest: {missing}")

    actual: dict[str, int] = {}
    for fragment in dataset.get_fragments():
        day = str(ds.get_partition_keys(fragment.partition_expression)[PARTITION_COLUMN])
        actual[day] = actual.get(day, 0) + int(fragment.count_rows())
    total = sum(actual.values())
    if total != manifest["rows"]:
        raise TrainingDatasetError(f"manifest says {manifest['rows']} rows, the files hold {total}")
    expected = {str(part[PARTITION_COLUMN]): int(part["rows"]) for part in manifest["partitions"]}
    if actual != expected:
        raise TrainingDatasetError(
            f"partition counts differ from the manifest: files {actual}, manifest {expected}"
        )
    return BatchedDataset(manifest=manifest, dataset=dataset)
