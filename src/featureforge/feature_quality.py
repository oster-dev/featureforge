"""Quality validation for persisted offline feature-store partitions."""

from __future__ import annotations

from pathlib import Path
from typing import Final

import pandas as pd

from .models import FeatureQualityCheck, OfflineFeatureQualityReport

FEATURE_VIEW_SPECS: Final = {
    "user_engagement_features": {
        "entity_column": "user_id",
        "recency_column": "days_since_last_activity",
        "total_count_column": "event_count",
        "unique_count_column": "unique_content_count",
        "required_columns": (
            "user_id",
            "observation_time",
            "window_days",
            "event_count",
            "unique_content_count",
            "total_watch_seconds",
            "search_count",
            "play_count",
            "watch_count",
            "days_since_last_activity",
        ),
        "non_negative_columns": (
            "event_count",
            "unique_content_count",
            "total_watch_seconds",
            "search_count",
            "play_count",
            "watch_count",
        ),
    },
    "content_popularity_features": {
        "entity_column": "content_id",
        "recency_column": "days_since_last_view",
        "total_count_column": "view_count",
        "unique_count_column": "unique_viewer_count",
        "required_columns": (
            "content_id",
            "observation_time",
            "window_days",
            "view_count",
            "unique_viewer_count",
            "total_watch_seconds",
            "average_watch_seconds",
            "search_count",
            "play_count",
            "watch_count",
            "days_since_last_view",
        ),
        "non_negative_columns": (
            "view_count",
            "unique_viewer_count",
            "total_watch_seconds",
            "average_watch_seconds",
            "search_count",
            "play_count",
            "watch_count",
        ),
    },
}


def _check(
    *,
    name: str,
    passed: bool,
    message: str,
    affected_row_count: int = 0,
) -> FeatureQualityCheck:
    """Build one validated feature-quality check result."""
    return FeatureQualityCheck(
        name=name,
        passed=passed,
        message=message,
        affected_row_count=affected_row_count,
    )


def _partition_paths(feature_view_dir: Path) -> list[Path]:
    """Return deterministic feature partition paths for one feature view."""
    return sorted(feature_view_dir.glob("observation_date=*/features.parquet"))


def _validate_feature_view(
    *,
    offline_store_dir: Path,
    feature_view: str,
) -> tuple[list[FeatureQualityCheck], list[str]]:
    """Validate every persisted partition for one feature view."""
    spec = FEATURE_VIEW_SPECS[feature_view]
    entity_column: str = spec["entity_column"]
    recency_column: str = spec["recency_column"]
    total_count_column: str = spec["total_count_column"]
    unique_count_column: str = spec["unique_count_column"]
    required_columns: tuple[str, ...] = spec["required_columns"]
    non_negative_columns: tuple[str, ...] = spec["non_negative_columns"]

    checks: list[FeatureQualityCheck] = []
    checked_partition_paths: list[str] = []

    feature_view_dir = offline_store_dir / feature_view
    directory_exists = feature_view_dir.is_dir()

    checks.append(
        _check(
            name=f"{feature_view}.directory_exists",
            passed=directory_exists,
            message=(
                f"Feature-view directory exists: {feature_view_dir}"
                if directory_exists
                else f"Feature-view directory is missing: {feature_view_dir}"
            ),
        )
    )

    if not directory_exists:
        return checks, checked_partition_paths

    partition_paths = _partition_paths(feature_view_dir)
    has_partitions = bool(partition_paths)

    checks.append(
        _check(
            name=f"{feature_view}.partitions_exist",
            passed=has_partitions,
            message=(
                f"Found {len(partition_paths)} feature partition(s)."
                if has_partitions
                else f"No feature partitions found below {feature_view_dir}."
            ),
        )
    )

    if not has_partitions:
        return checks, checked_partition_paths

    for partition_path in partition_paths:
        checked_partition_paths.append(str(partition_path))
        partition_label = partition_path.parent.name

        frame = pd.read_parquet(partition_path)
        missing_columns = sorted(set(required_columns) - set(frame.columns))
        has_required_columns = not missing_columns

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.required_columns",
                passed=has_required_columns,
                message=(
                    "All required columns are present."
                    if has_required_columns
                    else f"Missing required columns: {missing_columns}."
                ),
            )
        )

        if not has_required_columns:
            continue

        row_count = len(frame)
        has_rows = row_count > 0

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.non_empty_partition",
                passed=has_rows,
                message=(
                    f"Partition contains {row_count} row(s)." if has_rows else "Partition is empty."
                ),
            )
        )

        null_entity_count = int(frame[entity_column].isna().sum())
        empty_entity_count = int(frame[entity_column].dropna().astype(str).str.strip().eq("").sum())
        invalid_entity_count = null_entity_count + empty_entity_count

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.entity_keys",
                passed=invalid_entity_count == 0,
                message=(
                    "All entity keys are non-null and non-empty."
                    if invalid_entity_count == 0
                    else (
                        f"Found {invalid_entity_count} invalid entity key(s): "
                        f"{null_entity_count} null and {empty_entity_count} empty."
                    )
                ),
                affected_row_count=invalid_entity_count,
            )
        )

        null_observation_count = int(frame["observation_time"].isna().sum())

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.observation_time_present",
                passed=null_observation_count == 0,
                message=(
                    "All observation timestamps are present."
                    if null_observation_count == 0
                    else f"Found {null_observation_count} null observation timestamp(s)."
                ),
                affected_row_count=null_observation_count,
            )
        )

        observation_time_dtype = frame["observation_time"].dtype
        observation_time_is_datetime = pd.api.types.is_datetime64_any_dtype(observation_time_dtype)
        observation_time_timezone = getattr(observation_time_dtype, "tz", None)
        observation_time_is_utc = (
            observation_time_timezone is not None
            and str(observation_time_timezone).upper() == "UTC"
        )
        observation_time_type_valid = observation_time_is_datetime and observation_time_is_utc

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.observation_time_type",
                passed=observation_time_type_valid,
                message=(
                    "observation_time uses a timezone-aware UTC datetime dtype."
                    if observation_time_type_valid
                    else (
                        "observation_time must use a timezone-aware UTC datetime dtype; "
                        f"found {observation_time_dtype!s}."
                    )
                ),
                affected_row_count=0 if observation_time_type_valid else row_count,
            )
        )

        invalid_window_count = int(
            frame["window_days"].isna().sum() + (frame["window_days"] <= 0).sum()
        )

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.positive_window_days",
                passed=invalid_window_count == 0,
                message=(
                    "All window_days values are positive."
                    if invalid_window_count == 0
                    else f"Found {invalid_window_count} non-positive or null window_days value(s)."
                ),
                affected_row_count=invalid_window_count,
            )
        )

        duplicate_key_count = int(
            frame.duplicated(subset=[entity_column, "observation_time"], keep=False).sum()
        )

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.unique_entity_observation_time",
                passed=duplicate_key_count == 0,
                message=(
                    "Entity and observation_time combinations are unique."
                    if duplicate_key_count == 0
                    else (
                        f"Found {duplicate_key_count} row(s) participating in duplicate "
                        "entity and observation_time combinations."
                    )
                ),
                affected_row_count=duplicate_key_count,
            )
        )

        feature_columns = tuple(
            column
            for column in required_columns
            if column
            not in {
                entity_column,
                "observation_time",
                "window_days",
                recency_column,
            }
        )
        null_feature_count = int(frame[list(feature_columns)].isna().sum().sum())

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.required_feature_values",
                passed=null_feature_count == 0,
                message=(
                    "All required feature values are present."
                    if null_feature_count == 0
                    else f"Found {null_feature_count} null required feature value(s)."
                ),
                affected_row_count=null_feature_count,
            )
        )

        numeric_columns = (*non_negative_columns, recency_column, "window_days")
        non_numeric_columns = [
            column for column in numeric_columns if not pd.api.types.is_numeric_dtype(frame[column])
        ]

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.numeric_feature_types",
                passed=not non_numeric_columns,
                message=(
                    "All numeric feature columns use numeric dtypes."
                    if not non_numeric_columns
                    else f"Non-numeric feature columns: {sorted(non_numeric_columns)}."
                ),
                affected_row_count=len(non_numeric_columns),
            )
        )

        if non_numeric_columns:
            continue

        negative_metric_count = int(
            sum((frame[column] < 0).sum() for column in non_negative_columns)
        )

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.non_negative_metrics",
                passed=negative_metric_count == 0,
                message=(
                    "All non-recency metrics are non-negative."
                    if negative_metric_count == 0
                    else f"Found {negative_metric_count} negative metric value(s)."
                ),
                affected_row_count=negative_metric_count,
            )
        )

        invalid_recency_count = int(
            ((frame[recency_column].notna()) & (frame[recency_column] < 0)).sum()
        )

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.recency_values",
                passed=invalid_recency_count == 0,
                message=(
                    "Recency values are null or non-negative."
                    if invalid_recency_count == 0
                    else f"Found {invalid_recency_count} negative recency value(s)."
                ),
                affected_row_count=invalid_recency_count,
            )
        )

        typed_count = frame["search_count"] + frame["play_count"] + frame["watch_count"]
        invalid_typed_count = int((typed_count > frame[total_count_column]).sum())

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.typed_counts_within_total",
                passed=invalid_typed_count == 0,
                message=(
                    "Typed event counts do not exceed the total count."
                    if invalid_typed_count == 0
                    else (
                        f"Found {invalid_typed_count} row(s) where search_count + "
                        f"play_count + watch_count exceeds {total_count_column}."
                    )
                ),
                affected_row_count=invalid_typed_count,
            )
        )

        invalid_unique_count = int((frame[unique_count_column] > frame[total_count_column]).sum())

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.unique_count_within_total",
                passed=invalid_unique_count == 0,
                message=(
                    "Unique counts do not exceed total counts."
                    if invalid_unique_count == 0
                    else (
                        f"Found {invalid_unique_count} row(s) where {unique_count_column} "
                        f"exceeds {total_count_column}."
                    )
                ),
                affected_row_count=invalid_unique_count,
            )
        )

        observation_time_count = int(frame["observation_time"].nunique(dropna=True))

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.single_observation_time",
                passed=observation_time_count == 1,
                message=(
                    "Partition contains exactly one observation_time."
                    if observation_time_count == 1
                    else (
                        f"Partition contains {observation_time_count} distinct "
                        "observation_time value(s)."
                    )
                ),
                affected_row_count=max(observation_time_count - 1, 0),
            )
        )

        window_days_count = int(frame["window_days"].nunique(dropna=True))

        checks.append(
            _check(
                name=f"{feature_view}.{partition_label}.single_window_days",
                passed=window_days_count == 1,
                message=(
                    "Partition contains exactly one window_days value."
                    if window_days_count == 1
                    else f"Partition contains {window_days_count} distinct window_days value(s)."
                ),
                affected_row_count=max(window_days_count - 1, 0),
            )
        )

    return checks, checked_partition_paths


def validate_offline_feature_store(
    offline_store_dir: Path,
) -> OfflineFeatureQualityReport:
    """Validate persisted user and content feature partitions before serving."""
    all_checks: list[FeatureQualityCheck] = []
    checked_partition_paths: list[str] = []

    for feature_view in FEATURE_VIEW_SPECS:
        checks, paths = _validate_feature_view(
            offline_store_dir=offline_store_dir,
            feature_view=feature_view,
        )
        all_checks.extend(checks)
        checked_partition_paths.extend(paths)

    return OfflineFeatureQualityReport(
        offline_store_dir=str(offline_store_dir),
        checked_partition_paths=checked_partition_paths,
        checks=all_checks,
    )
