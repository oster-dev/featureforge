from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

from featureforge.spec.training_loader import (
    SUPPORTED_MANIFEST_VERSION,
    TrainingDatasetError,
    load_training_dataset,
)

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


def test_round_trip_keeps_manifest_columns_and_order(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds")
    loaded = load_training_dataset(tmp_path / "ds")
    assert list(loaded.frame.columns) == COLUMNS
    assert loaded.frame["user_id"].tolist() == ["u1", "u2", "u3"]
    assert loaded.manifest["rows"] == 3


def test_missing_directory(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        load_training_dataset(tmp_path / "nope")


def test_missing_manifest(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds")
    (tmp_path / "ds" / "_manifest.json").unlink()
    with pytest.raises(TrainingDatasetError, match="is missing"):
        load_training_dataset(tmp_path / "ds")


def test_unsupported_version(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds", manifest_version=2)
    with pytest.raises(TrainingDatasetError, match="manifest_version"):
        load_training_dataset(tmp_path / "ds")


def test_column_missing_in_files(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds", columns=[*COLUMNS, "ghost"])
    with pytest.raises(TrainingDatasetError, match="lacks columns"):
        load_training_dataset(tmp_path / "ds")


def test_row_count_differs_when_a_partition_is_gone(tmp_path: Path) -> None:
    write_dataset(tmp_path / "ds")
    shutil.rmtree(tmp_path / "ds" / "observation_date=2026-01-02")
    with pytest.raises(TrainingDatasetError, match="manifest says 3 rows"):
        load_training_dataset(tmp_path / "ds")


def test_partition_counts_differ_with_equal_total(tmp_path: Path) -> None:
    swapped = [
        {"observation_date": "2026-01-01", "rows": 1},
        {"observation_date": "2026-01-02", "rows": 2},
    ]
    write_dataset(tmp_path / "ds", partitions=swapped)
    with pytest.raises(TrainingDatasetError, match="partition counts"):
        load_training_dataset(tmp_path / "ds")
