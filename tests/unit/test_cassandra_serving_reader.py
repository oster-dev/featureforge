"""CassandraServingReader asks once per entity partition and fails as a whole (ADR-016)."""

import json
from collections import namedtuple
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from cassandra import ConsistencyLevel

from featureforge.cassandra_serving_reader import CassandraServingReader
from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import RecordDecoder, stamp
from featureforge.streaming.spec_store import RedisSpecStore
from featureforge.streaming.store_common import (
    RecordRead,
    UnsupportedPolicyError,
    decode_read,
)

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)
ENTITY = "user_000001"


def on_cassandra(spec):
    return spec.model_copy(update={"online": spec.online.model_copy(update={"store": "cassandra"})})


PLAY_REDIS = load_spec(SPECS / "user_play_count_1h.yaml")
PLAY = on_cassandra(PLAY_REDIS)
SKIP = on_cassandra(load_spec(SPECS / "user_skip_rate_24h.yaml"))
CONTENT = on_cassandra(load_spec(SPECS / "content_play_count_1h.yaml"))

Row = namedtuple("Row", ["feature", "payload"])
PRESENT = object()


def prefix(spec):
    return f"v{spec.version}:{spec.name}"


def payload_for(entity, count):
    counts = {"play_count": count}
    data = {
        "feature_schema_version": "v1",
        "feature_name": PLAY.name,
        PLAY.keys[0]: entity,
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - PLAY.window.size),
        **counts,
    }
    for name, expression in PLAY.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return json.dumps(data)


class FakeFuture:
    def __init__(self, rows=None, error=None):
        self.rows = rows or []
        self.error = error

    def result(self):
        if self.error is not None:
            raise self.error
        return self.rows


class FakeSession:
    def __init__(self, stored=None, error=None, ping_row=PRESENT):
        self.stored = stored or {}
        self.error = error
        self.ping_row = ping_row
        self.prepared = []
        self.calls = []

    def prepare(self, cql):
        statement = SimpleNamespace(cql=cql, consistency_level=None)
        self.prepared.append(statement)
        return statement

    def execute_async(self, statement, parameters, timeout=None):
        self.calls.append((statement, parameters, timeout))
        entity_key, entity_value, features = parameters
        rows = [
            Row(feature, self.stored[(entity_key, entity_value, feature)])
            for feature in features
            if (entity_key, entity_value, feature) in self.stored
        ]
        return FakeFuture(rows, self.error)

    def execute(self, query, timeout=None):
        return SimpleNamespace(one=lambda: self.ping_row)


def test_the_statement_is_prepared_once_with_the_configured_consistency():
    session = FakeSession()

    CassandraServingReader(session, [PLAY])

    assert len(session.prepared) == 1
    assert session.prepared[0].cql == (
        "SELECT feature, payload FROM featureforge.online_features "
        "WHERE entity_key = ? AND entity_value = ? AND feature IN ?"
    )
    assert session.prepared[0].consistency_level == ConsistencyLevel.LOCAL_QUORUM


def test_a_spec_for_another_backend_is_rejected():
    with pytest.raises(ValueError, match="not cassandra"):
        CassandraServingReader(FakeSession(), [PLAY_REDIS])


def test_composite_entity_keys_are_rejected():
    composite = PLAY.model_copy(update={"entity": ["user_id", "content_id"]})

    with pytest.raises(UnsupportedPolicyError):
        CassandraServingReader(FakeSession(), [composite])


def test_a_non_positive_timeout_is_rejected():
    with pytest.raises(ValueError, match="timeout"):
        CassandraServingReader(FakeSession(), [PLAY], timeout=0)


def test_two_features_of_one_entity_are_one_query_over_both_prefixes():
    session = FakeSession({(PLAY.keys[0], ENTITY, prefix(PLAY)): payload_for(ENTITY, 3)})
    reader = CassandraServingReader(session, [PLAY, SKIP], timeout=0.2)

    reads = reader.fetch([(PLAY, ENTITY), (SKIP, ENTITY)])

    assert len(session.calls) == 1
    _, parameters, timeout = session.calls[0]
    assert parameters == (PLAY.keys[0], ENTITY, sorted([prefix(PLAY), prefix(SKIP)]))
    assert timeout == 0.2
    assert reads[0].record is not None
    assert reads[0].record.counts["play_count"] == 3
    assert reads[1] == RecordRead()


def test_entities_are_separate_queries_and_results_keep_the_order_of_the_plan():
    session = FakeSession(
        {
            (PLAY.keys[0], "u1", prefix(PLAY)): payload_for("u1", 1),
            (PLAY.keys[0], "u2", prefix(PLAY)): payload_for("u2", 2),
        }
    )
    reader = CassandraServingReader(session, [PLAY])

    reads = reader.fetch([(PLAY, "u2"), (PLAY, "u3"), (PLAY, "u1")])

    assert len(session.calls) == 3
    assert [read.record.counts["play_count"] if read.record else None for read in reads] == [
        2,
        None,
        1,
    ]


def test_specs_with_different_entity_keys_are_separate_queries():
    session = FakeSession()
    reader = CassandraServingReader(session, [PLAY, CONTENT])

    reader.fetch([(PLAY, "same"), (CONTENT, "same")])

    assert {call[1][0] for call in session.calls} == {PLAY.keys[0], CONTENT.keys[0]}


def test_a_failing_query_fails_the_whole_request():
    reader = CassandraServingReader(FakeSession(error=TimeoutError("slow")), [PLAY])

    with pytest.raises(TimeoutError):
        reader.fetch([(PLAY, "u1"), (PLAY, "u2")])


def test_a_payload_that_cannot_be_decoded_is_an_error_read_and_not_an_exception():
    session = FakeSession({(PLAY.keys[0], ENTITY, prefix(PLAY)): "not json"})
    reader = CassandraServingReader(session, [PLAY])

    read = reader.fetch([(PLAY, ENTITY)])[0]

    assert read.record is None
    assert read.error_type is not None


def test_a_payload_of_another_entity_is_an_error_read():
    session = FakeSession({(PLAY.keys[0], ENTITY, prefix(PLAY)): payload_for("someone_else", 1)})
    reader = CassandraServingReader(session, [PLAY])

    assert reader.fetch([(PLAY, ENTITY)])[0].error_type == "ValueError"


def test_ping_follows_the_answer_of_the_node():
    assert CassandraServingReader(FakeSession(ping_row=object()), [PLAY]).ping() is True
    assert CassandraServingReader(FakeSession(ping_row=None), [PLAY]).ping() is False


@pytest.mark.parametrize(
    "payload",
    [None, payload_for(ENTITY, 4), "not json", payload_for("someone_else", 4)],
)
def test_decoding_is_the_same_as_in_the_redis_store(payload):
    redis_store = RedisSpecStore(Mock(), PLAY_REDIS)

    assert decode_read(RecordDecoder(PLAY), payload, ENTITY) == redis_store.decode_read(
        payload, ENTITY
    )


class TupleSession(FakeSession):
    def execute_async(self, statement, parameters, timeout=None):
        future = super().execute_async(statement, parameters, timeout)
        future.rows = [tuple(row) for row in future.rows]
        return future


def test_plain_tuple_rows_give_the_same_reads_as_named_tuples():
    stored = {
        (PLAY.keys[0], "u1", prefix(PLAY)): payload_for("u1", 1),
        (PLAY.keys[0], "u2", prefix(PLAY)): payload_for("u2", 2),
    }
    plan = [(PLAY, "u2"), (PLAY, "u3"), (PLAY, "u1")]

    named = CassandraServingReader(FakeSession(stored), [PLAY]).fetch(plan)
    plain = CassandraServingReader(TupleSession(stored), [PLAY]).fetch(plan)

    assert plain == named
    assert plain[0].record is not None
    assert plain[0].record.counts["play_count"] == 2
    assert plain[1] == RecordRead()
