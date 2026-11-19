"""Verify that Spark reads the event table at a pinned snapshot (ADR-015)."""

import pytest

from featureforge.spec.spark_backfill import events_to_frame
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.event_log import (
    EventLog,
    open_event_table,
    pinned_events_snapshot_id,
)
from featureforge.streaming.events_spark import read_events
from featureforge.streaming.history import open_local_catalog
from featureforge.streaming.published_spark import create_history_session

pytestmark = pytest.mark.integration


def make_event(number=1, **overrides) -> InteractionEventV1:
    return InteractionEventV1.model_validate(
        {
            "event_id": f"00000000-0000-4000-8000-{number:012d}",
            "event_time": "2026-01-01T10:00:00Z",
            "ingested_at": "2026-01-01T10:01:00Z",
            "user_id": "user_000001",
            "content_id": "content_000001",
            "event_type": "play",
            "play_seconds": 30,
            "source": "web",
            "schema_version": "v1",
            **overrides,
        }
    )


def sorted_rows(frame):
    return sorted((row.asDict() for row in frame.collect()), key=lambda row: row["event_id"])


def test_spark_reads_the_pinned_snapshot_as_the_frame_of_the_same_events(tmp_path):
    first = [make_event(1), make_event(2, event_type="skip", play_seconds=5)]
    catalog = open_local_catalog(tmp_path)
    log = EventLog(catalog)
    log.append(first)
    pinned = pinned_events_snapshot_id(open_event_table(catalog))
    log.append([make_event(3)])
    latest = pinned_events_snapshot_id(open_event_table(catalog))

    spark = create_history_session(tmp_path)
    try:
        at_pin = read_events(spark, pinned)
        reference = events_to_frame(spark, first)

        assert at_pin.columns == reference.columns
        assert dict(at_pin.dtypes) == dict(reference.dtypes)
        assert sorted_rows(at_pin) == sorted_rows(reference)
        assert read_events(spark, latest).count() == 3
    finally:
        spark.stop()
