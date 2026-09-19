"""Parquet persistence for FeatureForge synthetic datasets."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from .models import Content, Event, ObservationLabel, SyntheticDataset, User


def _write_parquet_compatible(dataframe: pd.DataFrame, path: Path) -> None:
    """Write Parquet using Spark-compatible UTC timestamp precision.

    Pandas/PyArrow can otherwise write timezone-aware timestamps as
    TIMESTAMP(NANOS, true), which Spark 4.x cannot read. Coercing to
    microseconds creates a portable Parquet contract consumable by Pandas,
    PyArrow, and PySpark.
    """
    table = pa.Table.from_pandas(dataframe, preserve_index=False)
    pq.write_table(
        table,
        path,
        coerce_timestamps="us",
        allow_truncated_timestamps=False,
    )


def write_synthetic_dataset(
    dataset: SyntheticDataset,
    output_dir: Path,
) -> dict[str, Path]:
    """Write a synthetic dataset to Spark-compatible Parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    users_df = pd.DataFrame([user.model_dump() for user in dataset.users])
    content_df = pd.DataFrame([content.model_dump() for content in dataset.content_items])
    events_df = pd.DataFrame([event.model_dump() for event in dataset.events])
    labels_df = pd.DataFrame([label.model_dump() for label in dataset.labels])

    users_path = output_dir / "users.parquet"
    content_path = output_dir / "content.parquet"
    events_path = output_dir / "events.parquet"
    labels_path = output_dir / "labels.parquet"

    _write_parquet_compatible(users_df, users_path)
    _write_parquet_compatible(content_df, content_path)
    _write_parquet_compatible(events_df, events_path)
    _write_parquet_compatible(labels_df, labels_path)

    return {
        "users": users_path,
        "content": content_path,
        "events": events_path,
        "labels": labels_path,
    }


def read_synthetic_dataset(input_dir: Path) -> SyntheticDataset:
    """Read a synthetic dataset from Parquet files."""
    users_df = pd.read_parquet(input_dir / "users.parquet")
    content_df = pd.read_parquet(input_dir / "content.parquet")
    events_df = pd.read_parquet(input_dir / "events.parquet")
    labels_df = pd.read_parquet(input_dir / "labels.parquet")

    users = [User(**row) for row in users_df.to_dict("records")]
    content_items = [Content(**row) for row in content_df.to_dict("records")]
    events = [Event(**row) for row in events_df.to_dict("records")]
    labels = [ObservationLabel(**row) for row in labels_df.to_dict("records")]

    return SyntheticDataset(
        users=users,
        content_items=content_items,
        events=events,
        labels=labels,
    )


def write_feature_batch(
    records: list[dict],
    feature_view: str,
    observation_date: str,
    output_dir: Path,
) -> Path:
    """Write a feature batch as a deterministic partitioned Parquet file."""
    partition_dir = output_dir / feature_view / f"observation_date={observation_date}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    features_path = partition_dir / "features.parquet"
    features_df = pd.DataFrame(records)
    _write_parquet_compatible(features_df, features_path)

    return features_path
