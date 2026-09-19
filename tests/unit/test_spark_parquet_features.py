"""Parity tests for Parquet-native PySpark feature computation."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from featureforge.config import SyntheticDataConfig
from featureforge.features import (
    compute_content_popularity_features,
    compute_user_engagement_features,
)
from featureforge.spark_features import (
    compute_content_popularity_features_from_parquet,
    compute_user_engagement_features_from_parquet,
)
from featureforge.storage import write_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset


@pytest.fixture(scope="module")
def spark():
    """Provide one local SparkSession for all Parquet-native parity tests."""
    session = (
        SparkSession.builder.master("local[2]")
        .appName("featureforge-parquet-parity-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture()
def source_dataset(tmp_path: Path):
    """Generate and persist deterministic source data through the real writer."""
    config = SyntheticDataConfig(
        seed=42,
        start_time="2026-01-01T00:00:00Z",
        end_time="2026-01-31T23:59:59Z",
        num_users=5,
        num_content_items=4,
        num_base_events=30,
        num_observations=5,
        duplicate_rate=0.1,
        late_event_rate=0.1,
        label_horizon_days=7,
        max_late_arrival_hours=24,
    )
    dataset = generate_synthetic_dataset(config)
    input_dir = tmp_path / "source_data"
    write_synthetic_dataset(dataset, input_dir)
    return dataset, input_dir


def test_spark_reads_featureforge_events_parquet(spark, source_dataset):
    """FeatureForge-written source Parquet must be readable by Spark."""
    _, input_dir = source_dataset

    events = spark.read.parquet(str(input_dir / "events.parquet"))

    assert events.count() > 0
    assert {
        "event_id",
        "user_id",
        "content_id",
        "event_type",
        "event_time",
        "ingested_at",
        "watch_seconds",
    }.issubset(events.columns)


def test_user_features_from_parquet_match_pandas_reference(spark, source_dataset):
    """Spark Parquet user features must equal the Pandas reference batch."""
    dataset, input_dir = source_dataset
    observation_time = datetime(2026, 1, 20, tzinfo=UTC)

    pandas_batch = compute_user_engagement_features(
        dataset,
        observation_time=observation_time,
        window_days=7,
    )
    spark_batch = compute_user_engagement_features_from_parquet(
        spark,
        input_dir=input_dir,
        observation_time=observation_time,
        window_days=7,
    )

    pandas_by_user = {feature.user_id: feature for feature in pandas_batch.features}
    spark_by_user = {feature.user_id: feature for feature in spark_batch.features}

    assert pandas_by_user == spark_by_user


def test_content_features_from_parquet_match_pandas_reference(spark, source_dataset):
    """Spark Parquet content features must equal the Pandas reference batch."""
    dataset, input_dir = source_dataset
    observation_time = datetime(2026, 1, 20, tzinfo=UTC)

    pandas_batch = compute_content_popularity_features(
        dataset,
        observation_time=observation_time,
        window_days=7,
    )
    spark_batch = compute_content_popularity_features_from_parquet(
        spark,
        input_dir=input_dir,
        observation_time=observation_time,
        window_days=7,
    )

    pandas_by_content = {feature.content_id: feature for feature in pandas_batch.features}
    spark_by_content = {feature.content_id: feature for feature in spark_batch.features}

    assert pandas_by_content == spark_by_content


def test_parquet_native_spark_rejects_non_positive_window_days(spark, source_dataset):
    """The Parquet-native Spark path enforces the same window contract."""
    _, input_dir = source_dataset
    observation_time = datetime(2026, 1, 20, tzinfo=UTC)

    with pytest.raises(ValueError, match="window_days must be positive"):
        compute_user_engagement_features_from_parquet(
            spark,
            input_dir=input_dir,
            observation_time=observation_time,
            window_days=0,
        )

    with pytest.raises(ValueError, match="window_days must be positive"):
        compute_content_popularity_features_from_parquet(
            spark,
            input_dir=input_dir,
            observation_time=observation_time,
            window_days=-1,
        )
