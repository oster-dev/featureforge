"""Contracts of the event fact table without Spark (ADR-015)."""

from dataclasses import replace

import pytest
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.event_log import (
    EVENT_COLUMNS,
    EVENT_TABLE,
    EventLogSchemaError,
    ensure_event_table,
    event_arrow_schema,
    event_partition_spec,
)
from featureforge.streaming.history import open_local_catalog


def create_with(catalog, columns, *, partitioned=True):
    catalog.create_namespace_if_not_exists("facts")
    schema = Schema(
        *(
            NestedField(index, column.name, column.iceberg_type, required=column.required)
            for index, column in enumerate(columns, start=1)
        )
    )
    spec = event_partition_spec() if partitioned else PartitionSpec()
    catalog.create_table(EVENT_TABLE, schema=schema, partition_spec=spec)


def signature(catalog):
    table = catalog.load_table(EVENT_TABLE)
    return [(f.name, str(f.field_type), f.required) for f in table.schema().fields]


def test_columns_follow_the_event_model():
    assert [column.name for column in EVENT_COLUMNS] == list(InteractionEventV1.model_fields)


def test_every_column_is_required_like_the_model():
    assert all(column.required for column in EVENT_COLUMNS)
    assert all(not field.nullable for field in event_arrow_schema())


def test_the_table_is_created_in_the_facts_namespace_partitioned_by_event_day(tmp_path):
    table = ensure_event_table(open_local_catalog(tmp_path))

    assert table.name() == ("facts", "interaction_events")
    [partition] = table.spec().fields
    assert str(partition.transform) == "day"
    assert table.schema().find_field(partition.source_id).name == "event_time"


def test_a_matching_table_is_accepted_again(tmp_path):
    catalog = open_local_catalog(tmp_path)
    first = ensure_event_table(catalog)
    second = ensure_event_table(catalog)

    assert second.schema() == first.schema()
    assert second.current_snapshot() is None


MISMATCHES = {
    "missing column": lambda columns: columns[:-1],
    "wrong type": lambda columns: [
        replace(c, iceberg_type=IntegerType()) if c.name == "play_seconds" else c for c in columns
    ],
    "optional column": lambda columns: [
        replace(c, required=False) if c.name == "user_id" else c for c in columns
    ],
}


@pytest.mark.parametrize("case", MISMATCHES)
def test_a_table_with_another_schema_fails_and_stays_unchanged(tmp_path, case):
    catalog = open_local_catalog(tmp_path)
    create_with(catalog, MISMATCHES[case](list(EVENT_COLUMNS)))
    before = signature(catalog)

    with pytest.raises(EventLogSchemaError, match="does not match"):
        ensure_event_table(catalog)

    assert signature(catalog) == before


def test_a_table_without_the_day_partition_fails(tmp_path):
    catalog = open_local_catalog(tmp_path)
    create_with(catalog, list(EVENT_COLUMNS), partitioned=False)

    with pytest.raises(EventLogSchemaError, match="partitioned by day"):
        ensure_event_table(catalog)
