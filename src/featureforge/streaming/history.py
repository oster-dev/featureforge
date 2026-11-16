"""History table of spec-driven streaming features (ADR-011).

One table per spec records the publication evidence of accepted online updates.
The table name comes from offline.table of the spec and the columns come from the
spec. The catalog name is part of every catalog row: a Spark catalog that reads
this table must use the same name (CATALOG_NAME).
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.expressions import In
from pyiceberg.partitioning import PartitionField, PartitionSpec
from pyiceberg.schema import Schema
from pyiceberg.transforms import DayTransform
from pyiceberg.types import (
    DoubleType,
    IcebergType,
    IntegerType,
    LongType,
    NestedField,
    StringType,
    TimestamptzType,
)

from featureforge.spec.model import FeatureSpec
from featureforge.streaming.contracts.feature_record import FeatureRecord, record_fields

CATALOG_NAME = "history"
RESERVED_COLUMNS = ("update_id", "spec_version", "published_at")
PARTITION_NAME = "published_at_day"
PARTITION_FIELD_ID = 1000


class HistorySchemaError(ValueError):
    """The existing history table does not match the schema derived from the spec."""


@dataclass(frozen=True)
class Column:
    name: str
    iceberg_type: IcebergType
    arrow_type: pa.DataType
    required: bool


@dataclass(frozen=True)
class Evidence:
    """One accepted online update and the time the store accepted it."""

    record: FeatureRecord
    published_at: datetime


def history_columns(spec: FeatureSpec) -> tuple[Column, ...]:
    """Return the columns of the history table of a spec, in table order."""
    entity_key, counters, derived = record_fields(spec)
    clash = sorted({entity_key, *counters, *derived} & set(RESERVED_COLUMNS))
    if clash:
        raise ValueError(f"column names of spec {spec.name} clash with history columns: {clash}")

    timestamp = pa.timestamp("us", tz="UTC")
    return (
        Column("update_id", StringType(), pa.string(), True),
        Column("spec_version", IntegerType(), pa.int32(), True),
        Column(entity_key, StringType(), pa.string(), True),
        Column("observation_time", TimestamptzType(), timestamp, True),
        Column("window_start_exclusive", TimestamptzType(), timestamp, True),
        *(Column(name, LongType(), pa.int64(), True) for name in counters),
        *(Column(name, DoubleType(), pa.float64(), False) for name in derived),
        Column("published_at", TimestamptzType(), timestamp, True),
    )


def history_arrow_schema(spec: FeatureSpec) -> pa.Schema:
    return pa.schema(
        [
            pa.field(column.name, column.arrow_type, nullable=not column.required)
            for column in history_columns(spec)
        ]
    )


def history_iceberg_schema(spec: FeatureSpec) -> Schema:
    return Schema(
        *(
            NestedField(
                field_id=index,
                name=column.name,
                field_type=column.iceberg_type,
                required=column.required,
            )
            for index, column in enumerate(history_columns(spec), start=1)
        )
    )


def history_partition_spec(spec: FeatureSpec) -> PartitionSpec:
    published_at_id = len(history_columns(spec))
    return PartitionSpec(
        PartitionField(
            source_id=published_at_id,
            field_id=PARTITION_FIELD_ID,
            transform=DayTransform(),
            name=PARTITION_NAME,
        )
    )


def table_name(spec: FeatureSpec) -> str:
    return spec.offline.table


def update_id(spec: FeatureSpec, record: FeatureRecord) -> str:
    """Hash the update identity of ADR-010: spec, version, entity, window and counts."""
    _, counters, _ = record_fields(spec)
    identity = {
        "spec_name": spec.name,
        "spec_version": spec.version,
        "entity": record.entity_value,
        "observation_time": record.observation_time.astimezone(UTC).isoformat(
            timespec="microseconds"
        ),
        "counts": [[name, record.counts[name]] for name in counters],
    }
    canonical = json.dumps(identity, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def catalog_db_path(root: Path) -> Path:
    return Path(root) / "catalog.db"


def warehouse_path(root: Path) -> Path:
    return Path(root) / "warehouse"


def open_local_catalog(root: Path, name: str = CATALOG_NAME) -> SqlCatalog:
    """Open the local profile catalog: a SQLite file and a filesystem warehouse.

    The local profile supports one writer process per catalog.
    """
    root = Path(root).resolve()
    warehouse_path(root).mkdir(parents=True, exist_ok=True)
    return SqlCatalog(
        name,
        uri=f"sqlite:///{catalog_db_path(root)}",
        warehouse=f"file://{warehouse_path(root)}",
    )


def _expected_signature(spec: FeatureSpec) -> list[tuple[str, str, bool]]:
    return [
        (column.name, str(column.iceberg_type), column.required) for column in history_columns(spec)
    ]


def ensure_table(catalog: Catalog, spec: FeatureSpec):
    """Create the history table of a spec, or check the existing one against the spec."""
    identifier = table_name(spec)
    namespace = identifier.split(".", 1)[0]
    catalog.create_namespace_if_not_exists(namespace)

    if not catalog.table_exists(identifier):
        return catalog.create_table(
            identifier,
            schema=history_iceberg_schema(spec),
            partition_spec=history_partition_spec(spec),
        )

    table = catalog.load_table(identifier)
    actual = [
        (field.name, str(field.field_type), field.required) for field in table.schema().fields
    ]
    if actual != _expected_signature(spec):
        raise HistorySchemaError(
            f"history table {identifier} does not match spec {spec.name}: "
            f"table has {actual}, spec expects {_expected_signature(spec)}"
        )
    partitions = [(field.source_id, str(field.transform)) for field in table.spec().fields]
    expected = [(len(history_columns(spec)), "day")]
    if partitions != expected:
        raise HistorySchemaError(
            f"history table {identifier} must be partitioned by day(published_at), "
            f"found {partitions}"
        )
    return table


class HistoryTable:
    """Append publication evidence of one spec idempotently (ADR-011).

    The first row of an update identity wins. The check and the append are not
    atomic, so only one appender per table is safe.
    """

    def __init__(self, catalog: Catalog, spec: FeatureSpec) -> None:
        self._spec = spec
        self._entity_key, self._counters, self._derived = record_fields(spec)
        self._arrow_schema = history_arrow_schema(spec)
        self._table = ensure_table(catalog, spec)

    @property
    def table(self):
        return self._table

    @property
    def spec(self) -> FeatureSpec:
        return self._spec

    def append(self, evidence: Sequence[Evidence]) -> int:
        """Append the evidence that the table does not hold yet. Return the rows added."""
        rows: dict[str, dict] = {}
        for item in evidence:
            if item.published_at.tzinfo is None or item.published_at.utcoffset() is None:
                raise ValueError("published_at must be timezone-aware")
            if item.record.feature_name != self._spec.name:
                raise ValueError(
                    f"record of feature {item.record.feature_name} cannot be added "
                    f"to the history of {self._spec.name}"
                )
            identity = update_id(self._spec, item.record)
            rows.setdefault(identity, self._row(identity, item))

        if not rows:
            return 0

        self._table.refresh()
        known = self._known(list(rows))
        fresh = [row for identity, row in rows.items() if identity not in known]
        if not fresh:
            return 0

        self._table.append(pa.Table.from_pylist(fresh, schema=self._arrow_schema))
        return len(fresh)

    def _known(self, identities: list[str]) -> set[str]:
        scan = self._table.scan(
            row_filter=In("update_id", identities),
            selected_fields=("update_id",),
        )
        return set(scan.to_arrow().column("update_id").to_pylist())

    def _row(self, identity: str, item: Evidence) -> dict:
        record = item.record
        return {
            "update_id": identity,
            "spec_version": self._spec.version,
            self._entity_key: record.entity_value,
            "observation_time": record.observation_time,
            "window_start_exclusive": record.window_start_exclusive,
            **{name: record.counts[name] for name in self._counters},
            **{name: float(record.derived[name]) for name in self._derived},
            "published_at": item.published_at.astimezone(UTC),
        }
