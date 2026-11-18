"""The Spark join of the history returns what published_as_of returns (ADR-014)."""

import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import (
    SPARK_PACKAGES,
    Evidence,
    HistoryTable,
    open_local_catalog,
    spark_catalog_config,
)
from featureforge.streaming.published_as_of import published_as_of
from featureforge.streaming.published_spark import (
    history_start,
    pinned_snapshot_id,
    published_as_of_frame,
    read_history,
)

pytestmark = pytest.mark.integration

SPEC = load_spec(Path(__file__).parents[2] / "feature_repo" / "specs" / "user_skip_rate_24h.yaml")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
BASE = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
MICRO = timedelta(microseconds=1)


def micros(moment):
    return (moment - EPOCH) // MICRO


def record(user, observation, plays, skips):
    return FeatureRecord(
        feature_name=SPEC.name,
        entity_key="user_id",
        entity_value=user,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=24),
        counts={"play_count": plays, "skip_count": skips},
        derived={"skip_rate": skips / (plays + skips)},
    )


def open_spark(root):
    builder = SparkSession.builder.master("local[1]").config("spark.jars.packages", SPARK_PACKAGES)
    for key, value in spark_catalog_config(root).items():
        builder = builder.config(key, value)
    spark = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def joined(spark, history, pairs):
    frame = spark.createDataFrame(pairs, "user_id string, observation_time timestamp")
    result = published_as_of_frame(
        SPEC, read_history(spark, SPEC, pinned_snapshot_id(history)), frame
    )
    rows = result.select(
        "user_id",
        F.unix_micros("observation_time").alias("us"),
        "play_count",
        "skip_count",
        "skip_rate",
        "status",
    ).collect()
    return {(row.user_id, row.us): row for row in rows}


def test_the_join_is_exact_at_the_publication_and_ttl_boundaries(tmp_path):
    ttl = SPEC.online.ttl
    published = BASE + timedelta(seconds=5)
    history = HistoryTable(open_local_catalog(tmp_path), SPEC)
    history.append([Evidence(record("user_000001", BASE, 3, 1), published)])
    times = [published - MICRO, published, published + ttl - MICRO, published + ttl]
    pairs = [("user_000001", moment) for moment in times] + [("user_999999", published)]

    spark = open_spark(tmp_path)
    try:
        rows = joined(spark, history, pairs)
        start = history_start(read_history(spark, SPEC, pinned_snapshot_id(history)))
    finally:
        spark.stop()

    statuses = [rows[("user_000001", micros(moment))].status for moment in times]
    assert statuses == ["not_published", "present", "present", "expired"]
    present = rows[("user_000001", micros(published))]
    assert (present.play_count, present.skip_count) == (3, 1)
    assert present.skip_rate == pytest.approx(0.25)
    for moment in (times[0], times[3]):
        row = rows[("user_000001", micros(moment))]
        assert (row.play_count, row.skip_count, row.skip_rate) == (None, None, None)
    assert rows[("user_999999", micros(published))].status == "not_published"
    assert start == published


def test_the_join_matches_the_oracle_on_random_histories(tmp_path):
    rng = random.Random(7)
    ttl = SPEC.online.ttl
    users = [f"user_{index:06d}" for index in range(4)]
    history = HistoryTable(open_local_catalog(tmp_path), SPEC)
    evidence = []
    for user in users:
        for _ in range(4):
            window_end = BASE + timedelta(minutes=rng.randrange(0, 600))
            published = window_end + timedelta(seconds=rng.randrange(0, 5400))
            plays, skips = rng.randrange(1, 6), rng.randrange(0, 4)
            evidence.append(Evidence(record(user, window_end, plays, skips), published))
    history.append(evidence)

    times = {BASE - timedelta(hours=1), BASE + timedelta(minutes=rng.randrange(0, 900))}
    for item in evidence:
        for edge in (item.published_at, item.published_at + ttl):
            times.update({edge - MICRO, edge, edge + MICRO})
    pairs = [(user, moment) for user in users for moment in sorted(times)]

    spark = open_spark(tmp_path)
    try:
        rows = joined(spark, history, pairs)
    finally:
        spark.stop()

    statuses = set()
    for moment in sorted(times):
        expected = published_as_of(history, users, moment)
        for user in users:
            oracle, actual = expected[user], rows[(user, micros(moment))]
            assert actual.status == oracle.status
            statuses.add(oracle.status)
            if oracle.record is None:
                assert (actual.play_count, actual.skip_count, actual.skip_rate) == (
                    None,
                    None,
                    None,
                )
            else:
                assert actual.play_count == oracle.record.counts["play_count"]
                assert actual.skip_count == oracle.record.counts["skip_count"]
                assert actual.skip_rate == pytest.approx(oracle.record.derived["skip_rate"])
    assert statuses == {"present", "not_published", "expired"}
