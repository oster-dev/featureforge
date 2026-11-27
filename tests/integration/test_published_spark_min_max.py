"""The Spark join and published_as_of agree on min and max, missing extremes and ties (ADR-017)."""

import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import FeatureRecord, component_directions
from featureforge.streaming.history import (
    SPARK_PACKAGES,
    Evidence,
    HistoryTable,
    open_local_catalog,
    spark_catalog_config,
)
from featureforge.streaming.published_as_of import published_as_of
from featureforge.streaming.published_spark import (
    pinned_snapshot_id,
    published_as_of_frame,
    read_history,
)

pytestmark = pytest.mark.integration

SPEC = load_spec(
    Path(__file__).parents[2] / "fixtures" / "specs" / "user_play_seconds_extremes_1h.yaml"
)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
BASE = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
MICRO = timedelta(microseconds=1)
USER = "user000001"


def micros(moment):
    return (moment - EPOCH) // MICRO


def record(user, observation, plays, smallest, largest):
    return FeatureRecord(
        feature_name=SPEC.name,
        entity_key="user_id",
        entity_value=user,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=1),
        counts={"play_count": plays, "min_play_seconds": smallest, "max_play_seconds": largest},
        derived={},
        directions=component_directions(SPEC),
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
        "min_play_seconds",
        "max_play_seconds",
        "status",
    ).collect()
    return {(row.user_id, row.us): row for row in rows}


def test_missing_extremes_come_back_as_null_with_the_status_present(tmp_path):
    published = BASE + timedelta(seconds=5)
    history = HistoryTable(open_local_catalog(tmp_path), SPEC)
    history.append([Evidence(record(USER, BASE, 0, None, None), published)])

    spark = open_spark(tmp_path)
    try:
        rows = joined(spark, history, [(USER, published)])
    finally:
        spark.stop()

    row = rows[(USER, micros(published))]
    assert row.status == "present"
    assert (row.play_count, row.min_play_seconds, row.max_play_seconds) == (0, None, None)


TIES = [
    pytest.param((2, 50, 100), (3, 40, 120), id="more-events"),
    pytest.param((2, 50, 100), (2, 40, 100), id="lower-minimum"),
    pytest.param((2, 50, 100), (2, 50, 130), id="higher-maximum"),
    pytest.param((0, None, None), (1, 100, 100), id="first-event"),
]


@pytest.mark.parametrize("reverse", [False, True], ids=["newer-first", "older-first"])
@pytest.mark.parametrize(("older", "newer"), TIES)
def test_a_tie_goes_to_the_more_advanced_vector(tmp_path, older, newer, reverse):
    published = BASE + timedelta(seconds=5)
    history = HistoryTable(open_local_catalog(tmp_path), SPEC)
    items = [
        Evidence(record(USER, BASE, *newer), published),
        Evidence(record(USER, BASE, *older), published),
    ]
    history.append(items[::-1] if reverse else items)

    spark = open_spark(tmp_path)
    try:
        rows = joined(spark, history, [(USER, published)])
    finally:
        spark.stop()

    row = rows[(USER, micros(published))]
    assert (row.play_count, row.min_play_seconds, row.max_play_seconds) == newer
    oracle = published_as_of(history, [USER], published)[USER]
    assert oracle.record is not None
    assert dict(oracle.record.counts) == {
        "play_count": newer[0],
        "min_play_seconds": newer[1],
        "max_play_seconds": newer[2],
    }


def test_the_join_matches_the_oracle_on_random_histories_with_missing_extremes(tmp_path):
    rng = random.Random(11)
    ttl = SPEC.online.ttl
    users = [f"user_{index:06d}" for index in range(4)]
    history = HistoryTable(open_local_catalog(tmp_path), SPEC)
    evidence = []
    for user in users:
        for _ in range(4):
            window_end = BASE + timedelta(minutes=rng.randrange(0, 600))
            published = window_end + timedelta(seconds=rng.randrange(0, 5400))
            plays = rng.randrange(0, 5)
            if plays == 0:
                smallest = largest = None
            else:
                smallest = rng.randrange(1, 300)
                largest = smallest + rng.randrange(0, 300)
            evidence.append(Evidence(record(user, window_end, plays, smallest, largest), published))
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
                assert (actual.play_count, actual.min_play_seconds, actual.max_play_seconds) == (
                    None,
                    None,
                    None,
                )
            else:
                assert actual.play_count == oracle.record.counts["play_count"]
                assert actual.min_play_seconds == oracle.record.counts["min_play_seconds"]
                assert actual.max_play_seconds == oracle.record.counts["max_play_seconds"]
    assert statuses == {"present", "not_published", "expired"}
