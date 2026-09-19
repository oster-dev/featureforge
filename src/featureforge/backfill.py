"""Idempotent, date-parameterized feature backfills."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Literal

from .features import compute_content_popularity_features, compute_user_engagement_features
from .manifest import BackfillRunManifest
from .models import SyntheticDataset
from .storage import write_feature_batch

Engine = Literal["pandas", "spark"]


@dataclass(frozen=True)
class BackfillDayResult:
    """Outcome of computing and persisting features for a single observation date."""

    observation_date: str
    user_feature_count: int
    content_feature_count: int
    user_features_path: str
    content_features_path: str


def _daterange(start: date, end: date) -> list[date]:
    """Return the inclusive list of dates from start to end."""
    if start > end:
        raise ValueError(f"start_date ({start}) must not be after end_date ({end})")

    days = (end - start).days
    return [start + timedelta(days=offset) for offset in range(days + 1)]


def _observation_time_for_day(day: date) -> datetime:
    """Return the UTC midnight timestamp representing the start of the given day.

    A backfill partition for `day` represents the feature state as of this
    timestamp: events at or before this point are eligible for inclusion
    according to the feature window semantics.
    """
    return datetime(day.year, day.month, day.day, tzinfo=UTC)


def _compute_feature_batches_pandas(
    dataset: SyntheticDataset,
    observation_time: datetime,
    window_days: int,
):
    """Compute both feature batches using the Pandas reference engine."""
    user_batch = compute_user_engagement_features(
        dataset,
        observation_time=observation_time,
        window_days=window_days,
    )
    content_batch = compute_content_popularity_features(
        dataset,
        observation_time=observation_time,
        window_days=window_days,
    )
    return user_batch, content_batch


def _compute_feature_batches_spark(
    spark,
    input_dir: Path,
    observation_time: datetime,
    window_days: int,
):
    """Compute both feature batches using an already-running SparkSession."""
    from .spark_features import (
        compute_content_popularity_features_from_parquet,
        compute_user_engagement_features_from_parquet,
    )

    user_batch = compute_user_engagement_features_from_parquet(
        spark,
        input_dir,
        observation_time,
        window_days,
    )
    content_batch = compute_content_popularity_features_from_parquet(
        spark,
        input_dir,
        observation_time,
        window_days,
    )

    return user_batch, content_batch


def run_backfill(
    dataset: SyntheticDataset,
    start_date: date,
    end_date: date,
    window_days: int,
    output_dir: Path,
    engine: Engine = "pandas",
    input_dir: Path | None = None,
) -> tuple[list[BackfillDayResult], Path]:
    """Compute and persist feature batches for every date in [start_date, end_date].

    The backfill uses deterministic partition paths and overwrite semantics.
    Therefore an identical run produces the same partition layout and values.
    A machine-readable run manifest records the completed run, including
    which engine produced it.

    ``engine="pandas"`` (the default) computes features from the in-memory
    ``dataset`` using the Pandas reference implementation. ``engine="spark"``
    computes features by reading source Parquet directly from ``input_dir``
    using PySpark; ``input_dir`` is required in that case and must point to
    the directory containing ``users.parquet``, ``content.parquet``, and
    ``events.parquet``.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be positive, got {window_days}")

    if engine not in {"pandas", "spark"}:
        raise ValueError(f"engine must be 'pandas' or 'spark', got {engine!r}")

    if engine == "spark" and input_dir is None:
        raise ValueError("input_dir is required when engine='spark'")

    spark = None

    if engine == "spark":
        from pyspark.sql import SparkSession

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("featureforge-backfill")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )

    started_at = datetime.now(UTC)
    results: list[BackfillDayResult] = []

    try:
        for day in _daterange(start_date, end_date):
            observation_time = _observation_time_for_day(day)

            if engine == "pandas":
                user_batch, content_batch = _compute_feature_batches_pandas(
                    dataset,
                    observation_time,
                    window_days,
                )
            else:
                user_batch, content_batch = _compute_feature_batches_spark(
                    spark,
                    input_dir,
                    observation_time,
                    window_days,
                )

            observation_date_str = day.isoformat()

            user_path = write_feature_batch(
                records=[feature.model_dump() for feature in user_batch.features],
                feature_view="user_engagement_features",
                observation_date=observation_date_str,
                output_dir=output_dir,
            )
            content_path = write_feature_batch(
                records=[feature.model_dump() for feature in content_batch.features],
                feature_view="content_popularity_features",
                observation_date=observation_date_str,
                output_dir=output_dir,
            )

            results.append(
                BackfillDayResult(
                    observation_date=observation_date_str,
                    user_feature_count=len(user_batch.features),
                    content_feature_count=len(content_batch.features),
                    user_features_path=str(user_path),
                    content_features_path=str(content_path),
                )
            )

        completed_at = datetime.now(UTC)

        manifest = BackfillRunManifest.from_run(
            start_date=start_date,
            end_date=end_date,
            window_days=window_days,
            output_dir=output_dir,
            started_at=started_at,
            completed_at=completed_at,
            partitions=[
                {
                    "observation_date": result.observation_date,
                    "user_feature_count": result.user_feature_count,
                    "content_feature_count": result.content_feature_count,
                    "user_features_path": result.user_features_path,
                    "content_features_path": result.content_features_path,
                }
                for result in results
            ],
            engine=engine,
        )
        manifest_path = manifest.write(output_dir)

        return results, manifest_path

    finally:
        if spark is not None:
            spark.stop()
