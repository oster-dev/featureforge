"""The history table on the PostgreSQL catalog profile of ADR-011."""

from __future__ import annotations

import os
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyspark.sql import SparkSession

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import (
    DEFAULT_CATALOG_URL,
    SPARK_PACKAGES,
    Evidence,
    HistoryTable,
    open_sql_catalog,
    spark_catalog_config,
)

pytestmark = pytest.mark.integration

URL = os.environ.get("FEATUREFORGE_CATALOG_URL", DEFAULT_CATALOG_URL)
SPEC = load_spec(Path(__file__).parents[2] / "feature_repo" / "specs" / "user_skip_rate_24h.yaml")
PACKAGES = SPARK_PACKAGES
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


@pytest.fixture
def catalog_name():
    return f"history_{uuid.uuid4().hex[:12]}"


@pytest.fixture
def root(tmp_path, catalog_name):
    yield tmp_path
    catalog = open_sql_catalog(tmp_path, URL, catalog_name)
    try:
        catalog.drop_table(SPEC.offline.table)
    except NoSuchTableError:
        pass
    try:
        catalog.drop_namespace(SPEC.offline.table.split(".", 1)[0])
    except NoSuchNamespaceError:
        pass


def test_two_connections_share_one_history(root, catalog_name):
    first = HistoryTable(open_sql_catalog(root, URL, catalog_name), SPEC)
    first.append([Evidence(record(1, 1), PUBLISHED)])
    second = HistoryTable(open_sql_catalog(root, URL, catalog_name), SPEC)
    later = PUBLISHED + timedelta(seconds=30)
    second.append([Evidence(record(1, 1), later), Evidence(record(2, 1), later)])

    reader = HistoryTable(open_sql_catalog(root, URL, catalog_name), SPEC)
    rows = reader.table.scan().to_arrow().to_pylist()

    assert sorted((row["play_count"], row["published_at"]) for row in rows) == [
        (1, PUBLISHED),
        (2, later),
    ]


def test_spark_reads_the_postgres_catalog(root, catalog_name):
    history = HistoryTable(open_sql_catalog(root, URL, catalog_name), SPEC)
    late = PUBLISHED + timedelta(seconds=30)
    history.append([Evidence(record(1, 1), PUBLISHED), Evidence(record(2, 1), late)])

    builder = SparkSession.builder.master("local[1]").config("spark.jars.packages", PACKAGES)
    for key, value in spark_catalog_config(root, URL, catalog_name).items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    try:
        result = spark.sql(
            "SELECT user_id, play_count, skip_count "
            f"FROM {catalog_name}.{SPEC.offline.table} ORDER BY play_count"
        ).collect()
    finally:
        spark.stop()

    assert [(row.user_id, row.play_count, row.skip_count) for row in result] == [
        ("user_000001", 1, 1),
        ("user_000001", 2, 1),
    ]
