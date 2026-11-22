"""The Cassandra schema is created once, never altered and checked loudly (ADR-013)."""

from types import SimpleNamespace

import pytest

from featureforge.streaming.cassandra_schema import (
    EXPECTED_COLUMNS,
    TABLE,
    SchemaMismatchError,
    ensure_schema,
)


def rows(overrides=None, drop=(), extra=()):
    columns = {**EXPECTED_COLUMNS, **(overrides or {})}
    for name in drop:
        columns.pop(name)
    for name in extra:
        columns[name] = ("regular", -1, "text")
    return [
        SimpleNamespace(column_name=name, kind=kind, position=position, type=type_)
        for name, (kind, position, type_) in columns.items()
    ]


class FakeSession:
    def __init__(self, schema_rows):
        self.schema_rows = schema_rows
        self.statements: list[str] = []

    def execute(self, query, parameters=None):
        self.statements.append(query)
        return self.schema_rows if "system_schema.columns" in query else []


def test_it_creates_the_keyspace_and_the_table_then_checks_the_schema():
    session = FakeSession(rows())

    ensure_schema(session, "ff_test")

    assert len(session.statements) == 3
    assert session.statements[0].startswith(
        "CREATE KEYSPACE IF NOT EXISTS ff_test WITH replication"
    )
    assert session.statements[1].startswith(f"CREATE TABLE IF NOT EXISTS ff_test.{TABLE} (")
    assert "system_schema.columns" in session.statements[2]
    assert not any("ALTER" in s or "DROP" in s for s in session.statements)


def test_a_matching_table_passes():
    ensure_schema(FakeSession(rows()), "ff_test")


@pytest.mark.parametrize(
    ("schema", "message"),
    [
        (rows({"payload": ("regular", -1, "blob")}), "payload"),
        (rows(drop=["feature"]), "missing column feature"),
        (rows(extra=["note"]), "unexpected column note"),
        (rows({"feature": ("partition_key", 2, "text")}), "feature"),
    ],
)
def test_a_table_that_does_not_match_fails_loudly(schema, message):
    with pytest.raises(SchemaMismatchError, match=message):
        ensure_schema(FakeSession(schema), "ff_test")


@pytest.mark.parametrize("keyspace", ["", "Upper", "1abc", "a b", "a;drop", "x" * 49])
def test_an_invalid_keyspace_name_raises_before_any_statement(keyspace):
    session = FakeSession(rows())

    with pytest.raises(ValueError, match="keyspace"):
        ensure_schema(session, keyspace)
    assert session.statements == []


def test_the_replication_factor_must_be_positive():
    session = FakeSession(rows())

    with pytest.raises(ValueError, match="replication_factor"):
        ensure_schema(session, "ff_test", replication_factor=0)
    assert session.statements == []
