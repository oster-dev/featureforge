"""Both dataset modes over the same observations, with a real history (ADR-014)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from featureforge.spec.label_model import LabelSpec
from featureforge.spec.label_spark import label_observations_to_frame
from featureforge.spec.loader import load_spec
from featureforge.spec.spark_backfill import events_to_frame
from featureforge.spec.training_dataset import PinnedHistory, build_training_dataset
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import (
    SPARK_PACKAGES,
    Evidence,
    HistoryTable,
    open_local_catalog,
    spark_catalog_config,
)
from featureforge.streaming.published_spark import pinned_snapshot_id, read_history

pytestmark = pytest.mark.integration

SPEC = load_spec(Path(__file__).parents[2] / "feature_repo" / "specs" / "user_skip_rate_24h.yaml")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
MICRO = timedelta(microseconds=1)
BASE = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
HORIZON = timedelta(days=7)
USER = "user_000001"
PUBLISHED = BASE - timedelta(minutes=30)
PREFIX = SPEC.name


def micros(moment):
    return (moment - EPOCH) // MICRO


def make_event(n, when, event_type):
    return InteractionEventV1(
        event_id=UUID(int=n),
        event_time=when,
        ingested_at=when + timedelta(minutes=1),
        user_id=USER,
        content_id="content_000001",
        event_type=event_type,
        play_seconds=10,
        source="web",
        schema_version="v1",
    )


def make_label():
    return LabelSpec.model_validate(
        {
            "name": "label_under_test",
            "version": 1,
            "owner": "test",
            "description": "label under test",
            "entity": "user_id",
            "source": "interactions",
            "outcome": {"op": "count", "filter": "event_type == 'play'"},
            "horizon": "7d",
            "dedupe": {"key": "event_id"},
        }
    )


def open_spark(root):
    builder = SparkSession.builder.master("local[1]").config("spark.jars.packages", SPARK_PACKAGES)
    for key, value in spark_catalog_config(root).items():
        builder = builder.config(key, value)
    spark = builder.config("spark.sql.session.timeZone", "UTC").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "UTC")
    return spark


def read_rows(spark, output):
    frame = spark.read.parquet(str(output)).withColumn("t", F.unix_micros("observation_time"))
    return {row["t"]: row.asDict() for row in frame.collect()}


def test_the_modes_differ_where_serving_and_the_events_differ(tmp_path):
    ttl = SPEC.online.ttl
    t0, t1, t2 = BASE - timedelta(days=10), BASE, BASE + ttl + timedelta(hours=1)
    data_end = t2 + HORIZON + timedelta(seconds=1)
    label = make_label()

    history = HistoryTable(open_local_catalog(tmp_path / "history"), SPEC)
    window_end = PUBLISHED - timedelta(seconds=1)
    served = FeatureRecord(
        feature_name=SPEC.name,
        entity_key="user_id",
        entity_value=USER,
        observation_time=window_end,
        window_start_exclusive=window_end - timedelta(hours=24),
        counts={"play_count": 1, "skip_count": 1},
        derived={"skip_rate": 0.5},
    )
    history.append([Evidence(served, PUBLISHED)])
    snapshot = pinned_snapshot_id(history)

    events = [
        make_event(1, BASE - timedelta(hours=1), "play"),
        make_event(2, BASE - timedelta(minutes=40), "skip"),
        make_event(3, BASE - timedelta(minutes=20), "play"),
        make_event(4, BASE + timedelta(hours=1), "play"),
    ]
    spark = open_spark(tmp_path / "history")
    try:
        pairs = [({"user_id": USER}, moment) for moment in (t0, t1, t2)]
        events_frame = events_to_frame(spark, events)
        observations = label_observations_to_frame(spark, label, pairs)
        pinned = {SPEC.name: PinnedHistory(read_history(spark, SPEC, snapshot), snapshot)}
        common = {"data_end": data_end}

        published = build_training_dataset(
            spark,
            [SPEC],
            label,
            events_frame,
            observations,
            mode="published_as_of",
            output=tmp_path / "published",
            history=pinned,
            **common,
        )
        reconstructed = build_training_dataset(
            spark,
            [SPEC],
            label,
            events_frame,
            observations,
            mode="event_time",
            output=tmp_path / "event_time",
            **common,
        )
        published_rows = read_rows(spark, tmp_path / "published")
        event_rows = read_rows(spark, tmp_path / "event_time")
    finally:
        spark.stop()

    assert published["mode"] == "published_as_of"
    assert published["observations"] == 3
    assert published["excluded_before_history"] == 1
    assert published["excluded_incomplete"] == 0
    assert published["rows"] == 2
    assert published["history"] == [
        {
            "feature": SPEC.name,
            "snapshot_id": snapshot,
            "history_start": PUBLISHED.isoformat(),
        }
    ]
    assert sorted(published_rows) == [micros(t1), micros(t2)]
    served_row, expired_row = published_rows[micros(t1)], published_rows[micros(t2)]
    assert served_row[f"{PREFIX}__status"] == "present"
    assert (served_row[f"{PREFIX}__play_count"], served_row[f"{PREFIX}__skip_count"]) == (1, 1)
    assert served_row[f"{PREFIX}__skip_rate"] == pytest.approx(0.5)
    assert expired_row[f"{PREFIX}__status"] == "expired"
    assert expired_row[f"{PREFIX}__play_count"] is None
    assert expired_row[f"{PREFIX}__skip_rate"] is None
    assert served_row["label__label_under_test"] == 1

    assert reconstructed["mode"] == "event_time"
    assert reconstructed["rows"] == 3
    assert reconstructed["excluded_before_history"] == 0
    assert "history" not in reconstructed
    assert f"{PREFIX}__status" not in reconstructed["columns"]
    reconstructed_t1 = event_rows[micros(t1)]
    assert (
        reconstructed_t1[f"{PREFIX}__play_count"],
        reconstructed_t1[f"{PREFIX}__skip_count"],
    ) == (2, 1)
    assert event_rows[micros(t2)][f"{PREFIX}__play_count"] == 0

    assert pinned_snapshot_id(history) == snapshot
    stored = json.loads((tmp_path / "published" / "_manifest.json").read_text())
    assert stored == published


def test_the_latest_history_start_cuts_and_the_edge_stays(tmp_path):
    other = load_spec(
        Path(__file__).parents[2] / "feature_repo" / "specs" / "user_play_count_1h.yaml"
    )
    label = make_label()
    start_a = PUBLISHED
    start_b = BASE + timedelta(hours=1)
    root = tmp_path / "history"

    served_a = FeatureRecord(
        feature_name=SPEC.name,
        entity_key="user_id",
        entity_value=USER,
        observation_time=start_a - timedelta(seconds=1),
        window_start_exclusive=start_a - timedelta(hours=24, seconds=1),
        counts={"play_count": 1, "skip_count": 1},
        derived={"skip_rate": 0.5},
    )
    served_b = FeatureRecord(
        feature_name=other.name,
        entity_key="user_id",
        entity_value=USER,
        observation_time=start_b - timedelta(seconds=1),
        window_start_exclusive=start_b - timedelta(hours=1, seconds=1),
        counts={"play_count": 4},
        derived={},
    )
    history_a = HistoryTable(open_local_catalog(root), SPEC)
    history_b = HistoryTable(open_local_catalog(root), other)
    history_a.append([Evidence(served_a, start_a)])
    history_b.append([Evidence(served_b, start_b)])
    snapshots = {
        SPEC.name: pinned_snapshot_id(history_a),
        other.name: pinned_snapshot_id(history_b),
    }

    when = [start_a - MICRO, start_b - MICRO, start_b]
    spark = open_spark(root)
    try:
        pinned = {
            spec.name: PinnedHistory(
                read_history(spark, spec, snapshots[spec.name]), snapshots[spec.name]
            )
            for spec in (SPEC, other)
        }
        manifest = build_training_dataset(
            spark,
            [SPEC, other],
            label,
            events_to_frame(spark, [make_event(1, start_b + timedelta(hours=1), "play")]),
            label_observations_to_frame(spark, label, [({"user_id": USER}, t) for t in when]),
            mode="published_as_of",
            data_end=start_b + HORIZON + timedelta(seconds=1),
            output=tmp_path / "published",
            history=pinned,
        )
        rows = read_rows(spark, tmp_path / "published")
    finally:
        spark.stop()

    assert manifest["observations"] == 3
    assert manifest["excluded_before_history"] == 2
    assert manifest["excluded_incomplete"] == 0
    assert manifest["rows"] == 1
    assert manifest["history"] == [
        {
            "feature": SPEC.name,
            "snapshot_id": snapshots[SPEC.name],
            "history_start": start_a.isoformat(),
        },
        {
            "feature": other.name,
            "snapshot_id": snapshots[other.name],
            "history_start": start_b.isoformat(),
        },
    ]
    assert list(rows) == [micros(start_b)]
    row = rows[micros(start_b)]
    assert row[f"{SPEC.name}__status"] == "present"
    assert row[f"{other.name}__status"] == "present"
    assert row[f"{other.name}__play_count"] == 4
