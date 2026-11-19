"""Event fact store: validated interaction events as Iceberg facts (ADR-015).

The table holds the fields of InteractionEventV1, including ingested_at. The columns are
listed explicitly because the model carries constraints and not storage types. A test
keeps the list in step with the model.
"""

from __future__ import annotations

from collections.abc import Sequence

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.expressions import In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

from featureforge.streaming.contracts.events import InteractionEventV1
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

# ingested_at is receipt metadata and not part of the event identity (ADR-010).
PAYLOAD_FIELDS = tuple(
    column.name for column in EVENT_COLUMNS if column.name not in ("event_id", "ingested_at")
)


class EventLogSchemaError(ValueError):
    """The event table does not match the contract."""


class EventIdentityConflict(ValueError):
    """One event_id carries two different payloads (ADR-007, ADR-008)."""


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


def event_row(event: InteractionEventV1) -> dict:
    """Return the table row of an event. The id is stored as its canonical string."""
    row = event.model_dump()
    row["event_id"] = str(event.event_id)
    return row


def _require_same_payload(event_id: str, existing: dict, incoming: dict) -> None:
    differing = [name for name in PAYLOAD_FIELDS if existing[name] != incoming[name]]
    if differing:
        # Name the fields and never the values: payloads carry user identifiers.
        raise EventIdentityConflict(
            f"event {event_id} is already known with another payload, differing in {differing}"
        )


class EventLog:
    """Append validated events to the fact table idempotently (ADR-015).

    The first row of an event_id wins and keeps its ingested_at. The same id with another
    payload (ingested_at excluded) fails the whole call and writes nothing. The check and
    the append are not atomic, so only one writer per table is safe.
    """

    def __init__(self, catalog: Catalog, identifier: str = EVENT_TABLE) -> None:
        self._arrow_schema = event_arrow_schema()
        self._table = ensure_event_table(catalog, identifier)

    @property
    def table(self):
        return self._table

    def append(self, events: Sequence[InteractionEventV1]) -> int:
        """Append the events that the table does not hold yet. Return the rows added."""
        rows: dict[str, dict] = {}
        for event in events:
            row = event_row(event)
            first = rows.setdefault(row["event_id"], row)
            if first is not row:
                _require_same_payload(row["event_id"], first, row)

        if not rows:
            return 0

        self._table.refresh()
        stored = self._stored(list(rows))
        fresh = []
        for event_id, row in rows.items():
            if event_id in stored:
                _require_same_payload(event_id, stored[event_id], row)
            else:
                fresh.append(row)

        if not fresh:
            return 0

        self._table.append(pa.Table.from_pylist(fresh, schema=self._arrow_schema))
        return len(fresh)

    def _stored(self, event_ids: list[str]) -> dict[str, dict]:
        scan = self._table.scan(
            row_filter=In("event_id", event_ids),  # type: ignore[call-arg,arg-type]
        )
        return {row["event_id"]: row for row in scan.to_arrow().to_pylist()}
