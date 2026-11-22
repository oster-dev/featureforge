"""CassandraServingReader against a local node, next to the store that writes (ADR-016)."""

import os
from collections.abc import Iterator
from uuid import uuid4

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy

from featureforge.cassandra_serving_reader import CassandraServingReader
from featureforge.streaming.cassandra_schema import TABLE, ensure_schema
from featureforge.streaming.cassandra_store import CassandraSpecStore
from featureforge.streaming.store_common import RecordRead
from tests.unit.test_cassandra_serving_reader import PLAY, SKIP, payload_for, prefix

pytestmark = pytest.mark.integration


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
    ensure_schema(session, name)
    yield name
    session.execute(f"DROP KEYSPACE IF EXISTS {name}")


def put(session, keyspace, spec, entity, count):
    session.execute(
        f"INSERT INTO {keyspace}.{TABLE} (entity_key, entity_value, feature, payload) "
        "VALUES (%s, %s, %s, %s)",
        (spec.keys[0], entity, prefix(spec), payload_for(entity, count)),
    )


def test_the_reader_returns_what_the_store_reads_in_the_order_of_the_plan(session, keyspace):
    put(session, keyspace, PLAY, "u1", 3)
    put(session, keyspace, PLAY, "u2", 5)
    store = CassandraSpecStore(session, PLAY, keyspace=keyspace)
    reader = CassandraServingReader(session, [PLAY], keyspace=keyspace, timeout=2.0)

    reads = reader.fetch([(PLAY, "u2"), (PLAY, "u3"), (PLAY, "u1")])

    assert reads[0].record == store.read("u2")
    assert reads[1] == RecordRead()
    assert reads[2].record == store.read("u1")
    assert reads[0].record.counts["play_count"] == 5


def test_two_features_of_one_entity_use_one_in_query_and_a_missing_one_is_missing(
    session, keyspace
):
    put(session, keyspace, PLAY, "u1", 3)
    reader = CassandraServingReader(session, [PLAY, SKIP], keyspace=keyspace, timeout=2.0)

    reads = reader.fetch([(PLAY, "u1"), (SKIP, "u1")])

    assert reads[0].record is not None
    assert reads[1] == RecordRead()


def test_a_missing_table_fails_when_the_reader_is_built(session):
    with pytest.raises(InvalidRequest):
        CassandraServingReader(session, [PLAY], keyspace=f"ff_missing_{uuid4().hex[:8]}")


def test_ping_reaches_the_node(session, keyspace):
    assert CassandraServingReader(session, [PLAY], keyspace=keyspace, timeout=2.0).ping() is True
