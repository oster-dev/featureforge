"""Event fact store: validated interaction events as Iceberg facts (ADR-015).

The table holds the fields of InteractionEventV1, including ingested_at. The columns are
listed explicitly because the model carries constraints and not storage types. A test
keeps the list in step with the model.
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

from featureforge.streaming.history import Column

EVENT_TABLE = "facts.interaction_events"
PARTITION_NAME = "event_time_day"
PARTITION_FIELD_ID = 1000
PARTITION_SOURCE = "event_time"

_TIMESTAMP = pa.timestamp("us", tz="UTC")

EVENT_COLUMNS: tuple[Column, ...] = (
    Column("event_id", StringType(), pa.string(), True),
    Column("event_time", TimestamptzType(), _TIMESTAMP, True),
    Column("ingested_at", TimestamptzType(), _TIMESTAMP, True),
    Column("user_id", StringType(), pa.string(), True),
    Column("content_id", StringType(), pa.string(), True),
    Column("event_type", StringType(), pa.string(), True),
    Column("play_seconds", LongType(), pa.int64(), True),
    Column("source", StringType(), pa.string(), True),
    Column("schema_version", StringType(), pa.string(), True),
)


class EventLogSchemaError(ValueError):
    """The event table does not match the contract."""


def event_arrow_schema() -> pa.Schema:
    return pa.schema(
        [
            pa.field(column.name, column.arrow_type, nullable=not column.required)
            for column in EVENT_COLUMNS
        ]
    )


def event_iceberg_schema() -> Schema:
    return Schema(
        *(
            NestedField(
                field_id=index,
                name=column.name,
                field_type=column.iceberg_type,
                required=column.required,
            )
            for index, column in enumerate(EVENT_COLUMNS, start=1)
        )
    )


def _source_id(name: str) -> int:
    names = [column.name for column in EVENT_COLUMNS]
    return names.index(name) + 1


def event_partition_spec() -> PartitionSpec:
    return PartitionSpec(
        PartitionField(
            source_id=_source_id(PARTITION_SOURCE),
            field_id=PARTITION_FIELD_ID,
            transform=DayTransform(),
            name=PARTITION_NAME,
        )
    )


def _expected_signature() -> list[tuple[str, str, bool]]:
    return [(column.name, str(column.iceberg_type), column.required) for column in EVENT_COLUMNS]


def ensure_event_table(catalog: Catalog, identifier: str = EVENT_TABLE):
    """Create the event table, or check the existing one against the contract.

    A table that differs is never altered: the call fails.
    """
    namespace = identifier.split(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)

    if not catalog.table_exists(identifier):
        return catalog.create_table(
            identifier,
            schema=event_iceberg_schema(),
            partition_spec=event_partition_spec(),
        )

    table = catalog.load_table(identifier)
    actual = [
        (field.name, str(field.field_type), field.required) for field in table.schema().fields
    ]
    if actual != _expected_signature():
        raise EventLogSchemaError(
            f"event table {identifier} does not match the event contract: "
            f"table has {actual}, contract expects {_expected_signature()}"
        )
    partitions = [(field.source_id, str(field.transform)) for field in table.spec().fields]
    expected = [(_source_id(PARTITION_SOURCE), "day")]
    if partitions != expected:
        raise EventLogSchemaError(
            f"event table {identifier} must be partitioned by day({PARTITION_SOURCE}), "
            f"found {partitions}"
        )
    return table
