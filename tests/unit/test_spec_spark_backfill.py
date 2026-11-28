"""The spec-driven Spark backfill must match the spec reference (ADR-008)."""

from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

import featureforge.spec.spark_backfill as backfill_module
from featureforge.spec.loader import load_spec
from featureforge.spec.model import Aggregation, Op
from featureforge.spec.reference import evaluate_spec
from featureforge.spec.spark_backfill import backfill_spec
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import (
    Evidence,
    HistoryTable,
    open_local_catalog,
    warehouse_path,
)
from featureforge.streaming.reference.user_play_count import EventIdentityConflictError

ROOT = Path(__file__).resolve().parents[2]
SPECS = ROOT / "feature_repo" / "specs"
FIXTURES = ROOT / "fixtures" / "streaming"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ONE_MICROSECOND = timedelta(microseconds=1)
T0 = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
SKIP_RATE = load_spec(SPECS / "user_skip_rate_24h.yaml")

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_time_us", LongType(), nullable=False),
        StructField("ingested_at_us", LongType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("content_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("play_seconds", LongType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("schema_version", StringType(), nullable=False),
    ]
)


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("featureforge-spec-backfill-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def micros(moment):
    return (moment - EPOCH) // ONE_MICROSECOND


def make_event(number, event_time, user, content, event_type="play", ingested_at=None):
    return InteractionEventV1(
        event_id=UUID(f"00000000-0000-4000-8000-{number:012d}"),
        event_time=event_time,
        ingested_at=ingested_at or event_time,
        user_id=user,
        content_id=content,
        event_type=event_type,
        play_seconds=0,
        source="web",
        schema_version="v1",
    )


def load_events(name):
    lines = (FIXTURES / name).read_text().splitlines()
    return [InteractionEventV1.model_validate_json(line) for line in lines if line.strip()]


def events_frame(spark, events):
    rows = [
        (
            str(e.event_id),
            micros(e.event_time),
            micros(e.ingested_at),
            e.user_id,
            e.content_id,
            e.event_type,
            e.play_seconds,
            e.source,
            e.schema_version,
        )
        for e in events
    ]
    frame = spark.createDataFrame(rows, EVENT_SCHEMA)
    return (
        frame.withColumn("event_time", F.timestamp_micros("event_time_us"))
        .withColumn("ingested_at", F.timestamp_micros("ingested_at_us"))
        .drop("event_time_us", "ingested_at_us")
    )


def observations_frame(spark, spec, pairs):
    schema = StructType(
        [StructField(key, StringType(), nullable=False) for key in spec.keys]
        + [StructField("observation_us", LongType(), nullable=False)]
    )
    rows = [(*(entity[key] for key in spec.keys), micros(moment)) for entity, moment in pairs]
    return (
        spark.createDataFrame(rows, schema)
        .withColumn("observation_time", F.timestamp_micros("observation_us"))
        .drop("observation_us")
    )


def boundary_times(events, size):
    times = {min(e.event_time for e in events) - size}
    for event in events:
        for base in (event.event_time, event.event_time + size):
            times |= {base - ONE_MICROSECOND, base, base + ONE_MICROSECOND}
    return sorted(times)


def assert_parity(spark, spec, events, entities, times):
    pairs = [(entity, moment) for entity in entities for moment in times]
    frame = backfill_spec(spec, events_frame(spark, events), observations_frame(spark, spec, pairs))
    rows = (
        frame.withColumn("obs_us", F.unix_micros("observation_time"))
        .withColumn("start_us", F.unix_micros("window_start_exclusive"))
        .collect()
    )
    got = {(tuple(row[key] for key in spec.keys), row["obs_us"]): row for row in rows}
    assert len(got) == len(entities) * len(times)
    for entity, moment in pairs:
        expected = evaluate_spec(spec, events, entity=entity, observation_time=moment)
        row = got[(tuple(entity[key] for key in spec.keys), micros(moment))]
        assert row["start_us"] == micros(expected.window_start_exclusive)
        for name, value in expected.values.items():
            assert row[name] == value, (spec.name, entity, moment, name)
    return got


@pytest.mark.parametrize(
    ("spec_file", "fixture_file"),
    [
        ("user_play_count_1h.yaml", "user_play_count_1h_v1.jsonl"),
        ("user_skip_rate_24h.yaml", "user_skip_rate_24h_v1.jsonl"),
    ],
)
def test_backfill_matches_reference_on_golden_fixtures(spark, spec_file, fixture_file):
    spec = load_spec(SPECS / spec_file)
    events = load_events(fixture_file)
    users = sorted({event.user_id for event in events})
    entities = [{"user_id": user} for user in [*users, "user_absent"]]
    assert_parity(spark, spec, events, entities, boundary_times(events, spec.window.size))


def test_backfill_matches_reference_for_a_content_entity(spark):
    spec = load_spec(SPECS / "content_play_count_1h.yaml")
    events = [
        make_event(1, T0, "user_000001", "content_000001"),
        make_event(2, T0 + timedelta(seconds=1), "user_000002", "content_000001"),
        make_event(3, T0 + timedelta(minutes=30), "user_000001", "content_000002"),
        make_event(4, T0 + timedelta(minutes=31), "user_000001", "content_000002", "skip"),
        make_event(5, T0 + timedelta(hours=2), "user_000003", "content_000001"),
    ]
    entities = [{"content_id": c} for c in ("content_000001", "content_000002", "content_absent")]
    assert_parity(spark, spec, events, entities, boundary_times(events, spec.window.size))


def test_redelivery_with_a_later_ingested_at_counts_once(spark):
    events = [
        make_event(1, T0, "user_000001", "content_000001"),
        make_event(1, T0, "user_000001", "content_000001", ingested_at=T0 + timedelta(seconds=5)),
    ]
    got = assert_parity(
        spark, SKIP_RATE, events, [{"user_id": "user_000001"}], [T0, T0 + timedelta(hours=1)]
    )
    assert got[(("user_000001",), micros(T0))]["play_count"] == 1


def test_conflicting_payload_fails_outside_window_and_for_other_entities(spark):
    events = [
        make_event(1, T0, "user_000001", "content_000001"),
        make_event(1, T0, "user_000002", "content_000001"),
    ]
    late = T0 + timedelta(days=365)
    with pytest.raises(EventIdentityConflictError):
        evaluate_spec(SKIP_RATE, events, entity={"user_id": "user_000001"}, observation_time=late)
    with pytest.raises(EventIdentityConflictError):
        backfill_spec(
            SKIP_RATE,
            events_frame(spark, events),
            observations_frame(spark, SKIP_RATE, [({"user_id": "user_000001"}, late)]),
        )


def test_unsupported_operators_fail_loudly(spark):
    spec = SKIP_RATE.model_copy(
        update={
            "aggregations": [
                Aggregation(name="distinct_content", op=Op.APPROX_DISTINCT, column="content_id")
            ],
            "derived": {},
        }
    )
    events = [make_event(1, T0, "user_000001", "content_000001")]
    with pytest.raises(NotImplementedError, match=r"unsupported: \['approx_distinct'\]"):
        backfill_spec(
            spec,
            events_frame(spark, events),
            observations_frame(spark, spec, [({"user_id": "user_000001"}, T0)]),
        )


def test_backfill_module_never_touches_history_or_publication_time():
    source = Path(backfill_module.__file__).read_text()
    tree = ast.parse(source)
    imported = [n.module or "" for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    imported += [a.name for n in ast.walk(tree) if isinstance(n, ast.Import) for a in n.names]
    assert not [name for name in imported if "history" in name]
    assert "published_at" not in source


def warehouse_files(root):
    return sorted(
        (str(item.relative_to(root)), item.stat().st_size)
        for item in warehouse_path(root).rglob("*")
        if item.is_file()
    )


def history_state(root):
    table = open_local_catalog(root).load_table(SKIP_RATE.offline.table)
    rows = sorted(table.scan().to_arrow().to_pylist(), key=repr)
    return table.current_snapshot().snapshot_id, rows, warehouse_files(root)


def test_backfill_leaves_recorded_evidence_untouched(spark, tmp_path):
    history = HistoryTable(open_local_catalog(tmp_path), SKIP_RATE)
    record = FeatureRecord(
        feature_name=SKIP_RATE.name,
        entity_key="user_id",
        entity_value="user_000001",
        observation_time=T0,
        window_start_exclusive=T0 - timedelta(hours=24),
        counts={"play_count": 1, "skip_count": 0},
        derived={"skip_rate": 0.0},
    )
    history.append([Evidence(record, T0 + timedelta(seconds=5))])
    before = history_state(tmp_path)

    events = [make_event(1, T0 - timedelta(minutes=10), "user_000001", "content_000001")]
    rows = backfill_spec(
        SKIP_RATE,
        events_frame(spark, events),
        observations_frame(spark, SKIP_RATE, [({"user_id": "user_000001"}, T0)]),
    ).collect()

    assert rows[0]["play_count"] == 1
    assert history_state(tmp_path) == before
