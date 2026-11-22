"""Schema of the Cassandra online store (ADR-013).

ensure_schema creates the keyspace and the table if they are absent and then checks
the table. It never alters an existing table, and a table that does not match fails
loudly. The keyspace uses SimpleStrategy, which fits one local node. A topology with
several datacenters needs NetworkTopologyStrategy and is not part of this module.
"""

import re
from collections.abc import Iterable, Sequence
from typing import Any, Protocol

DEFAULT_KEYSPACE = "featureforge"
TABLE = "online_features"

# column name -> (kind, position, type), as system_schema.columns reports them
EXPECTED_COLUMNS: dict[str, tuple[str, int, str]] = {
    "entity_key": ("partition_key", 0, "text"),
    "entity_value": ("partition_key", 1, "text"),
    "feature": ("clustering", 0, "text"),
    "payload": ("regular", -1, "text"),
}

_KEYSPACE = re.compile(r"[a-z][a-z0-9_]{0,47}")


class SchemaMismatchError(RuntimeError):
    """An existing table does not match the schema of ADR-013."""


class CqlSession(Protocol):
    def execute(self, query: str, parameters: Sequence[object] | None = None) -> Iterable[Any]: ...


def _describe(actual: dict[str, tuple[str, int, str]]) -> str:
    problems: list[str] = []
    for name, expected in EXPECTED_COLUMNS.items():
        if name not in actual:
            problems.append(f"missing column {name}")
        elif actual[name] != expected:
            problems.append(f"column {name} is {actual[name]}, expected {expected}")
    for name in actual.keys() - EXPECTED_COLUMNS.keys():
        problems.append(f"unexpected column {name}")
    return f"table {TABLE} does not match ADR-013: " + "; ".join(sorted(problems))


def check_keyspace(keyspace: str) -> None:
    """Raise ValueError unless the name is a plain lowercase CQL identifier."""
    if not isinstance(keyspace, str) or not _KEYSPACE.fullmatch(keyspace):
        raise ValueError("keyspace must match [a-z][a-z0-9_]{0,47}")


def ensure_schema(
    session: CqlSession,
    keyspace: str = DEFAULT_KEYSPACE,
    *,
    replication_factor: int = 1,
) -> None:
    """Create the keyspace and the table if absent, then verify the table."""
    check_keyspace(keyspace)
    if replication_factor < 1:
        raise ValueError("replication_factor must be positive")

    session.execute(
        f"CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = "
        f"{{'class': 'SimpleStrategy', 'replication_factor': {replication_factor}}}"
    )
    session.execute(
        f"CREATE TABLE IF NOT EXISTS {keyspace}.{TABLE} ("
        "entity_key text, entity_value text, feature text, payload text, "
        "PRIMARY KEY ((entity_key, entity_value), feature))"
    )
    rows = session.execute(
        "SELECT column_name, kind, position, type FROM system_schema.columns "
        "WHERE keyspace_name = %s AND table_name = %s",
        (keyspace, TABLE),
    )
    actual = {row.column_name: (row.kind, row.position, row.type) for row in rows}
    if actual != EXPECTED_COLUMNS:
        raise SchemaMismatchError(_describe(actual))
