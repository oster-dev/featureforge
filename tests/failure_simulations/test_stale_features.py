"""Failure simulation: stale offline features."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from featureforge.feature_quality import check_offline_feature_freshness


def test_stale_features_fail_freshness_check(tmp_path: Path) -> None:
    """Stale offline features should fail the freshness check."""
    offline_store = tmp_path / "offline_store"
    user_features_dir = offline_store / "user_engagement_features"
    content_features_dir = offline_store / "content_popularity_features"

    stale_date = (datetime.now(UTC) - timedelta(hours=48)).strftime("%Y-%m-%d")

    # Create stale user features partition
    stale_user_partition = user_features_dir / f"observation_date={stale_date}"
    stale_user_partition.mkdir(parents=True)
    features_path = stale_user_partition / "features.parquet"
    df = pd.DataFrame({"user_id": ["u1"], "feature_value": [1.0]})
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, features_path, coerce_timestamps="us")

    # Create stale content features partition
    stale_content_partition = content_features_dir / f"observation_date={stale_date}"
    stale_content_partition.mkdir(parents=True)
    features_path = stale_content_partition / "features.parquet"
    df = pd.DataFrame({"content_id": ["c1"], "feature_value": [1.0]})
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, features_path, coerce_timestamps="us")

    reference_time = datetime.now(UTC)
    max_lag = timedelta(hours=24)

    report = check_offline_feature_freshness(
        offline_store,
        reference_time=reference_time,
        max_lag=max_lag,
    )

    assert not report.passed
    assert "user_engagement_features.freshness" in report.failed_checks
    assert "content_popularity_features.freshness" in report.failed_checks


def test_fresh_features_pass_freshness_check(tmp_path: Path) -> None:
    """Fresh offline features should pass the freshness check."""
    offline_store = tmp_path / "offline_store"
    user_features_dir = offline_store / "user_engagement_features"
    content_features_dir = offline_store / "content_popularity_features"

    fresh_date = datetime.now(UTC).strftime("%Y-%m-%d")

    # Create fresh user features partition
    fresh_user_partition = user_features_dir / f"observation_date={fresh_date}"
    fresh_user_partition.mkdir(parents=True)
    features_path = fresh_user_partition / "features.parquet"
    df = pd.DataFrame({"user_id": ["u1"], "feature_value": [1.0]})
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, features_path, coerce_timestamps="us")

    # Create fresh content features partition
    fresh_content_partition = content_features_dir / f"observation_date={fresh_date}"
    fresh_content_partition.mkdir(parents=True)
    features_path = fresh_content_partition / "features.parquet"
    df = pd.DataFrame({"content_id": ["c1"], "feature_value": [1.0]})
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, features_path, coerce_timestamps="us")

    reference_time = datetime.now(UTC)
    max_lag = timedelta(hours=24)

    report = check_offline_feature_freshness(
        offline_store,
        reference_time=reference_time,
        max_lag=max_lag,
    )

    assert report.passed
    assert len(report.failed_checks) == 0
