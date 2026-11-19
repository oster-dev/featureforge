from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from featureforge.spec.training_batches import open_training_batches
from featureforge.spec.training_loader import SUPPORTED_MANIFEST_VERSION, TrainingDatasetError

COLUMNS = ["user_id", "observation_time", "f__skip_rate", "label__x"]


def write_dataset(path: Path, **manifest_changes: Any) -> None:
    frame = pd.DataFrame(
        {
            "user_id": ["u3", "u1", "u2"],
            "observation_time": pd.to_datetime(
                ["2026-01-01T13:00:00", "2026-01-02T12:00:00", "2026-01-01T12:00:00"], utc=True
            ),
            "f__skip_rate": [1.0, 0.0, 0.5],
            "label__x": [True, True, False],
        }
    )
    dated = frame.assign(observation_date=frame["observation_time"].dt.strftime("%Y-%m-%d"))
    dated.to_parquet(path, partition_cols=["observation_date"], index=False)
    manifest: dict[str, Any] = {
        "manifest_version": SUPPORTED_MANIFEST_VERSION,
        "mode": "event_time",
        "columns": COLUMNS,
        "rows": 3,
        "partitions": [
            {"observation_date": "2026-01-01", "rows": 2},
            {"observation_date": "2026-01-02", "rows": 1},
        ],
    }
    manifest.update(manifest_changes)
    (path / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def test_batches_keep_manifest_columns_and_follow_the_partition_order(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds")
    batches = list(open_training_batches(tmp_path / "ds").batches(batch_size=2))
    assert [batch.num_rows for batch in batches] == [2, 1]
    assert all(batch.schema.names == COLUMNS for batch in batches)
    users = [user for batch in batches for user in batch.column("user_id").to_pylist()]
    assert sorted(users) == ["u1", "u2", "u3"]
    assert users[-1] == "u1"


def test_the_checks_run_when_the_dataset_is_opened(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds")
    (tmp_path / "ds" / "_manifest.json").unlink()
    with pytest.raises(TrainingDatasetError, match="is missing"):
        open_training_batches(tmp_path / "ds")


def test_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        open_training_batches(tmp_path / "nope")


def test_column_missing_in_files(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds", columns=[*COLUMNS, "ghost"])
    with pytest.raises(TrainingDatasetError, match="lacks columns"):
        open_training_batches(tmp_path / "ds")


def test_row_count_differs_when_a_partition_is_gone(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds")
    shutil.rmtree(tmp_path / "ds" / "observation_date=2026-01-02")
    with pytest.raises(TrainingDatasetError, match="manifest says 3 rows"):
        open_training_batches(tmp_path / "ds")


def test_partition_counts_differ_with_equal_total(tmp_path: Path) -> None:
    swapped = [
        {"observation_date": "2026-01-01", "rows": 1},
        {"observation_date": "2026-01-02", "rows": 2},
    ]
    write_dataset(tmp_path / "ds", partitions=swapped)
    with pytest.raises(TrainingDatasetError, match="partition counts"):
        open_training_batches(tmp_path / "ds")
