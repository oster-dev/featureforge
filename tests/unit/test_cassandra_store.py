"""CassandraSpecStore decides, writes conditionally and retries (ADR-013)."""

import json
from collections import namedtuple
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.cassandra_store import (
    MAX_ATTEMPTS,
    CassandraSpecStore,
    ConcurrentWriteError,
)
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)
ENTITY = "user_000001"

PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
CASSANDRA_PLAY = PLAY.model_copy(
    update={"online": PLAY.online.model_copy(update={"store": "cassandra"})}
)
KEY = (CASSANDRA_PLAY.keys[0], ENTITY, "p")

Row = namedtuple("Row", ["applied", "payload"])


def payload_for(count, *, minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    counts = {"play_count": count}
    data = {
        "feature_schema_version": "v1",
        "feature_name": PLAY.name,
        PLAY.keys[0]: ENTITY,
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - PLAY.window.size),
        **counts,
    }
    for name, expression in PLAY.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return json.dumps(data)


def record_for(count, *, minutes=0):
    return parse_feature_record(payload_for(count, minutes=minutes), PLAY)


class Result:
    def __init__(self, row):
        self._row = row

    def one(self):
        return self._row


class FakeSession:
    def __init__(self, serial_read=None, conditionals=()):
        self.serial_read = serial_read
        self.conditionals = list(conditionals)
        self.calls = []

    def execute(self, statement, parameters=None):
        query = statement.query_string
        self.calls.append((query, parameters))
        if query.startswith("SELECT"):
            row = None if self.serial_read is None else SimpleNamespace(payload=self.serial_read)
            return Result(row)
        outcome = self.conditionals.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return Result(outcome)


def store_for(session):
    return CassandraSpecStore(session, CASSANDRA_PLAY, keyspace="ff_test", key_prefix="p")


def test_a_first_value_is_inserted_with_the_ttl_from_the_spec():
    session = FakeSession(conditionals=[Row(True, None)])
    incoming = record_for(3)

    assert store_for(session).write(incoming) == "written"

    query, parameters = session.calls[1]
    assert query.startswith("INSERT INTO ff_test.online_features")
    assert "IF NOT EXISTS USING TTL 7200" in query
    assert parameters == (*KEY, incoming.to_json())


def test_a_replacement_compares_the_payload_that_was_read():
    stored = payload_for(1)
    incoming = record_for(3)
    session = FakeSession(serial_read=stored, conditionals=[Row(True, None)])

    assert store_for(session).write(incoming) == "written"

    query, parameters = session.calls[1]
    assert query.startswith("UPDATE ff_test.online_features USING TTL 7200 SET payload")
    assert query.endswith("IF payload = %s")
    assert parameters == (incoming.to_json(), *KEY, stored)


@pytest.mark.parametrize(("count", "outcome"), [(2, "stale"), (3, "unchanged")])
def test_stale_and_unchanged_deliveries_write_nothing(count, outcome):
    session = FakeSession(serial_read=payload_for(3))

    assert store_for(session).write(record_for(count)) == outcome
    assert len(session.calls) == 1


def test_a_lost_race_decides_again_from_the_value_that_came_back():
    session = FakeSession(conditionals=[Row(False, payload_for(5))])

    assert store_for(session).write(record_for(3)) == "stale"
    assert [call[0].split(" ")[0] for call in session.calls] == ["SELECT", "INSERT"]


def test_a_lost_race_retries_with_the_new_value_and_can_still_win():
    conditionals = [Row(False, payload_for(1)), Row(True, None)]
    session = FakeSession(conditionals=conditionals)
    incoming = record_for(3)

    assert store_for(session).write(incoming) == "written"
    assert [call[0].split(" ")[0] for call in session.calls] == ["SELECT", "INSERT", "UPDATE"]
    assert session.calls[2][1] == (incoming.to_json(), *KEY, payload_for(1))


def test_the_write_gives_up_after_the_maximum_number_of_lost_attempts():
    conditionals = [Row(False, payload_for(n)) for n in range(1, MAX_ATTEMPTS + 1)]
    session = FakeSession(conditionals=conditionals)

    with pytest.raises(ConcurrentWriteError, match=str(MAX_ATTEMPTS)):
        store_for(session).write(record_for(100))
    assert len(session.calls) == 1 + MAX_ATTEMPTS


def test_a_timeout_of_the_conditional_write_propagates():
    session = FakeSession(conditionals=[TimeoutError("cas timed out")])

    with pytest.raises(TimeoutError):
        store_for(session).write(record_for(3))


def test_a_stored_record_for_another_entity_is_rejected_on_read():
    other = payload_for(3).replace(ENTITY, "user_000002")
    session = FakeSession(serial_read=other)

    with pytest.raises(ValueError, match="does not match"):
        store_for(session).read(ENTITY)


def test_an_unknown_entity_reads_as_none():
    assert store_for(FakeSession()).read(ENTITY) is None


def test_a_redis_spec_is_rejected():
    with pytest.raises(ValueError, match="not cassandra"):
        CassandraSpecStore(FakeSession(), PLAY)


@pytest.mark.parametrize(
    ("keyspace", "prefix", "message"),
    [("Bad Name", "p", "keyspace"), ("ff_test", "has space", "key_prefix")],
)
def test_invalid_names_raise_at_construction(keyspace, prefix, message):
    with pytest.raises(ValueError, match=message):
        CassandraSpecStore(FakeSession(), CASSANDRA_PLAY, keyspace=keyspace, key_prefix=prefix)
