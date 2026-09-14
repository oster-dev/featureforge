from pathlib import Path

import pandas as pd

from featureforge.config import SyntheticDataConfig
from featureforge.storage import write_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset


def build_test_config() -> SyntheticDataConfig:
    return SyntheticDataConfig(
        seed=42,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-31T23:59:59Z",
        num_users=3,
        num_content_items=2,
        num_base_events=10,
        num_observations=5,
        duplicate_rate=0.1,
        late_event_rate=0.1,
        label_horizon_days=7,
        max_late_arrival_hours=24,
    )


def test_writes_expected_parquet_files(tmp_path: Path) -> None:
    dataset = generate_synthetic_dataset(build_test_config())

    output_paths = write_synthetic_dataset(dataset, tmp_path)

    assert set(output_paths) == {"users", "content", "events", "labels"}
    assert all(path.exists() for path in output_paths.values())
    assert all(path.suffix == ".parquet" for path in output_paths.values())


def test_written_parquet_files_preserve_row_counts(tmp_path: Path) -> None:
    dataset = generate_synthetic_dataset(build_test_config())

    output_paths = write_synthetic_dataset(dataset, tmp_path)

    assert len(pd.read_parquet(output_paths["users"])) == len(dataset.users)
    assert len(pd.read_parquet(output_paths["content"])) == len(dataset.content_items)
    assert len(pd.read_parquet(output_paths["events"])) == len(dataset.events)
    assert len(pd.read_parquet(output_paths["labels"])) == len(dataset.labels)


def test_events_parquet_preserves_time_and_quality_columns(tmp_path: Path) -> None:
    dataset = generate_synthetic_dataset(build_test_config())

    output_paths = write_synthetic_dataset(dataset, tmp_path)
    events = pd.read_parquet(output_paths["events"])

    assert {
        "event_id",
        "user_id",
        "content_id",
        "event_type",
        "event_time",
        "ingested_at",
        "session_id",
        "device_type",
        "watch_seconds",
        "is_duplicate",
        "is_late",
    }.issubset(events.columns)

    assert pd.api.types.is_datetime64_any_dtype(events["event_time"])
    assert pd.api.types.is_datetime64_any_dtype(events["ingested_at"])
    assert events["is_duplicate"].sum() == 1
    assert events["is_late"].sum() == 1
