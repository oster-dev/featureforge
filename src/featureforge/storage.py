"""Parquet persistence for FeatureForge synthetic datasets."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from .models import Content, Event, ObservationLabel, SyntheticDataset, User


def write_synthetic_dataset(
    dataset: SyntheticDataset,
    output_dir: Path,
) -> dict[str, Path]:
    """Write a synthetic dataset to Parquet files."""
    output_dir.mkdir(parents=True, exist_ok=True)

    users_df = pd.DataFrame([u.model_dump() for u in dataset.users])
    content_df = pd.DataFrame([c.model_dump() for c in dataset.content_items])
    events_df = pd.DataFrame([e.model_dump() for e in dataset.events])
    labels_df = pd.DataFrame([label.model_dump() for label in dataset.labels])

    users_path = output_dir / "users.parquet"
    content_path = output_dir / "content.parquet"
    events_path = output_dir / "events.parquet"
    labels_path = output_dir / "labels.parquet"

    users_df.to_parquet(users_path, index=False)
    content_df.to_parquet(content_path, index=False)
    events_df.to_parquet(events_path, index=False)
    labels_df.to_parquet(labels_path, index=False)

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
