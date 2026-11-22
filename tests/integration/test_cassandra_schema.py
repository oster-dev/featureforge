"""Verify the Cassandra schema and conditional writes against a local node (ADR-013)."""

import os
import time
from collections.abc import Iterator
from uuid import uuid4

import pytest
from cassandra import ConsistencyLevel
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy
from cassandra.query import SimpleStatement

from featureforge.streaming.cassandra_schema import TABLE, SchemaMismatchError, ensure_schema

pytestmark = pytest.mark.integration

KEY = ("user_id", "user_000001", "user_play_count_1h")


@pytest.fixture(scope="module")
def session() -> Iterator:
    host = os.environ.get("FEATUREFORGE_CASSANDRA_HOST", "127.0.0.1")
    profile = ExecutionProfile(
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1")
    )
    cluster = Cluster(
        [host],
        port=9042,
        connect_timeout=10,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    try:
        yield cluster.connect()
    finally:
        cluster.shutdown()


@pytest.fixture
def keyspace(session) -> Iterator[str]:
    name = f"ff_test_{uuid4().hex[:12]}"
    yield name
    session.execute(f"DROP KEYSPACE IF EXISTS {name}")


def conditional(session, cql, parameters):
    statement = SimpleStatement(
        cql,
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    )
    return session.execute(statement, parameters).one()


def insert_cql(keyspace: str, ttl: int) -> str:
    return (
        f"INSERT INTO {keyspace}.{TABLE} (entity_key, entity_value, feature, payload) "
        f"VALUES (%s, %s, %s, %s) IF NOT EXISTS USING TTL {ttl}"
    )


def test_the_schema_is_created_and_a_second_call_changes_nothing(session, keyspace):
    ensure_schema(session, keyspace)
    ensure_schema(session, keyspace)

    columns = session.execute(
        "SELECT column_name FROM system_schema.columns "
        "WHERE keyspace_name = %s AND table_name = %s",
        (keyspace, TABLE),
    )
    assert {row.column_name for row in columns} == {
        "entity_key",
        "entity_value",
        "feature",
        "payload",
    }


def test_a_table_that_does_not_match_fails_loudly(session, keyspace):
    session.execute(
        f"CREATE KEYSPACE {keyspace} WITH replication = "
        "{'class': 'SimpleStrategy', 'replication_factor': 1}"
    )
    session.execute(
        f"CREATE TABLE {keyspace}.{TABLE} (entity_key text, entity_value text, "
        "feature text, payload blob, PRIMARY KEY ((entity_key, entity_value), feature))"
    )

    with pytest.raises(SchemaMismatchError, match="payload"):
        ensure_schema(session, keyspace)


def test_insert_if_not_exists_applies_only_once(session, keyspace):
    ensure_schema(session, keyspace)
    arguments = (*KEY, '{"play_count": 3}')

    assert conditional(session, insert_cql(keyspace, 60), arguments)[0] is True
    assert conditional(session, insert_cql(keyspace, 60), arguments)[0] is False


def test_a_conditional_update_compares_the_whole_payload(session, keyspace):
    ensure_schema(session, keyspace)
    conditional(session, insert_cql(keyspace, 60), (*KEY, '{"play_count": 3}'))
    update = (
        f"UPDATE {keyspace}.{TABLE} USING TTL 60 SET payload = %s "
        "WHERE entity_key = %s AND entity_value = %s AND feature = %s IF payload = %s"
    )

    wrong = conditional(session, update, ('{"play_count": 5}', *KEY, '{"play_count": 9}'))
    assert wrong[0] is False
    assert wrong[1] == '{"play_count": 3}'

    right = conditional(session, update, ('{"play_count": 5}', *KEY, '{"play_count": 3}'))
    assert right[0] is True


def test_a_value_written_with_a_ttl_expires(session, keyspace):
    ensure_schema(session, keyspace)
    assert conditional(session, insert_cql(keyspace, 1), (*KEY, '{"play_count": 3}'))[0]
    select = (
        f"SELECT payload FROM {keyspace}.{TABLE} "
        "WHERE entity_key = %s AND entity_value = %s AND feature = %s"
    )

    assert session.execute(select, KEY).one() is not None
    time.sleep(2)
    assert session.execute(select, KEY).one() is None
