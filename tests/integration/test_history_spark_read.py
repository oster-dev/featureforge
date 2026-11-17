"""Verify that Spark reads the history table that PyIceberg wrote (ADR-011)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import (
    CATALOG_NAME,
    SPARK_PACKAGES,
    Evidence,
    HistoryTable,
    catalog_db_path,
    open_local_catalog,
    warehouse_path,
)

pytestmark = pytest.mark.integration

SPEC = load_spec(Path(__file__).parents[2] / "feature_repo" / "specs" / "user_skip_rate_24h.yaml")
PACKAGES = SPARK_PACKAGES
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
OBSERVATION = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
PUBLISHED = datetime(2026, 1, 1, 12, 0, 5, tzinfo=UTC)


def record(plays, skips):
    return FeatureRecord(
        feature_name=SPEC.name,
        entity_key="user_id",
        entity_value="user_000001",
        observation_time=OBSERVATION,
        window_start_exclusive=OBSERVATION - timedelta(hours=24),
        counts={"play_count": plays, "skip_count": skips},
        derived={"skip_rate": skips / (plays + skips)},
    )


def micros(moment):
    return (moment - EPOCH) // timedelta(microseconds=1)


def test_spark_reads_what_pyiceberg_appended(tmp_path):
    history = HistoryTable(open_local_catalog(tmp_path), SPEC)
    late = PUBLISHED + timedelta(seconds=30)
    history.append([Evidence(record(1, 1), PUBLISHED)])
    history.append(
        [
            Evidence(record(2, 1), late),
            Evidence(record(1, 1), late + timedelta(minutes=1)),
        ]
    )

    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.jars.packages", PACKAGES)
        .config(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.catalog-impl",
            "org.apache.iceberg.jdbc.JdbcCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG_NAME}.uri", f"jdbc:sqlite:{catalog_db_path(tmp_path)}")
        .config(
            f"spark.sql.catalog.{CATALOG_NAME}.warehouse",
            f"file://{warehouse_path(tmp_path)}",
        )
        .getOrCreate()
    )
    try:
        result = spark.sql(
            "SELECT user_id, play_count, skip_count, skip_rate, "
            "unix_micros(published_at) AS published_us "
            f"FROM {CATALOG_NAME}.{SPEC.offline.table} ORDER BY play_count"
        ).collect()
    finally:
        spark.stop()

    assert [(row.user_id, row.play_count, row.skip_count) for row in result] == [
        ("user_000001", 1, 1),
        ("user_000001", 2, 1),
    ]
    assert result[0].skip_rate == pytest.approx(0.5)
    assert result[1].skip_rate == pytest.approx(1 / 3)
    assert [row.published_us for row in result] == [micros(PUBLISHED), micros(late)]
