"""Idempotent, date-parameterized feature backfills."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from .features import compute_content_popularity_features, compute_user_engagement_features
from .manifest import BackfillRunManifest
from .models import SyntheticDataset
from .storage import write_feature_batch


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


def run_backfill(
    dataset: SyntheticDataset,
    start_date: date,
    end_date: date,
    window_days: int,
    output_dir: Path,
) -> tuple[list[BackfillDayResult], Path]:
    """Compute and persist feature batches for every date in [start_date, end_date].

    The backfill uses deterministic partition paths and overwrite semantics.
    Therefore an identical run produces the same partition layout and values.
    A machine-readable run manifest records the completed run.
    """
    if window_days <= 0:
        raise ValueError(f"window_days must be positive, got {window_days}")

    started_at = datetime.now(UTC)
    results: list[BackfillDayResult] = []

    for day in _daterange(start_date, end_date):
        observation_time = _observation_time_for_day(day)

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
    )
    manifest_path = manifest.write(output_dir)

    return results, manifest_path
