"""Read-only access to the event table and its pinned snapshot, without Spark (ADR-015)."""

import pytest

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.event_log import (
    EVENT_TABLE,
    EventLog,
    EventLogSchemaError,
    open_event_table,
    pinned_events_snapshot_id,
)
from featureforge.streaming.history import open_local_catalog


def make_event(number=1) -> InteractionEventV1:
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
        }
    )


def test_a_missing_table_is_an_error_and_is_not_created(tmp_path):
    catalog = open_local_catalog(tmp_path)

    with pytest.raises(ValueError, match="no event table"):
        open_event_table(catalog)

    assert not catalog.table_exists(EVENT_TABLE)


def test_an_existing_table_is_opened(tmp_path):
    catalog = open_local_catalog(tmp_path)
    EventLog(catalog)

    table = open_event_table(catalog)

    assert table.name() == ("facts", "interaction_events")


def test_a_table_with_another_schema_is_refused(tmp_path):
    catalog = open_local_catalog(tmp_path)
    catalog.create_namespace_if_not_exists("facts")
    from pyiceberg.schema import Schema
    from pyiceberg.types import NestedField, StringType

    catalog.create_table(
        EVENT_TABLE, schema=Schema(NestedField(1, "event_id", StringType(), required=True))
    )

    with pytest.raises(EventLogSchemaError, match="does not match"):
        open_event_table(catalog)


def test_an_empty_table_has_nothing_to_pin(tmp_path):
    table = EventLog(open_local_catalog(tmp_path)).table

    with pytest.raises(ValueError, match="no snapshot"):
        pinned_events_snapshot_id(table)


def test_the_pin_is_the_current_snapshot_and_a_later_append_does_not_move_it(tmp_path):
    catalog = open_local_catalog(tmp_path)
    log = EventLog(catalog)
    log.append([make_event(1)])
    pinned = pinned_events_snapshot_id(open_event_table(catalog))

    log.append([make_event(2)])
    later = pinned_events_snapshot_id(open_event_table(catalog))

    assert later != pinned
    table = open_event_table(catalog)
    table.refresh()
    assert table.scan(snapshot_id=pinned).to_arrow().num_rows == 1
    assert table.scan(snapshot_id=later).to_arrow().num_rows == 2
