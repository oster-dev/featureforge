"""Readers fetch the values of a request in order and fail as a whole (ADR-016)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest

from featureforge.feature_service import FeatureRef, FeatureServer
from featureforge.redis_serving_reader import RedisServingReader
from featureforge.serving_reader import ProtocolError
from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import stamp
from featureforge.streaming.store_common import RecordRead

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")
ENTITY = "user_000001"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)


def play_payload(count):
    counts = {"play_count": count}
    data = {
        "feature_schema_version": "v1",
        "feature_name": PLAY.name,
        PLAY.keys[0]: ENTITY,
        "observation_time": stamp(OBSERVATION),
        "window_start_exclusive": stamp(OBSERVATION - timedelta(hours=1)),
        **counts,
    }
    for name, expression in PLAY.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return json.dumps(data)


def key(entity):
    return f"ns:v{PLAY.version}:{PLAY.name}:{entity}"


def test_the_redis_reader_makes_one_mget_over_all_keys_and_keeps_the_order():
    client = Mock()
    client.mget.return_value = [play_payload(3), None]
    reader = RedisServingReader(client, [PLAY], namespace="ns")

    reads = reader.fetch([(PLAY, ENTITY), (PLAY, "user_000002")])

    client.mget.assert_called_once_with([key(ENTITY), key("user_000002")])
    assert reads[0].record is not None
    assert reads[0].record.counts["play_count"] == 3
    assert reads[1] == RecordRead()


def test_the_redis_reader_rejects_an_answer_with_the_wrong_number_of_values():
    client = Mock()
    client.mget.return_value = [play_payload(3)]
    reader = RedisServingReader(client, [PLAY], namespace="ns")

    with pytest.raises(ProtocolError):
        reader.fetch([(PLAY, ENTITY), (PLAY, "user_000002")])


def test_a_failure_of_the_backend_propagates_out_of_the_redis_reader():
    client = Mock()
    client.mget.side_effect = ConnectionError("down")
    reader = RedisServingReader(client, [PLAY], namespace="ns")

    with pytest.raises(ConnectionError):
        reader.fetch([(PLAY, ENTITY)])


class FakeReader:
    def __init__(self, reads=None, error=None):
        self.reads = reads
        self.error = error
        self.plans = []

    def fetch(self, plan):
        self.plans.append(list(plan))
        if self.error is not None:
            raise self.error
        return self.reads

    def ping(self):
        return True


def lookup(reader, specs, entities):
    server = FeatureServer(reader, specs)
    return server.lookup([FeatureRef(spec.name) for spec in specs], entities)


def test_a_reader_that_raises_makes_every_entry_a_backend_error():
    reader = FakeReader(error=TimeoutError("slow"))

    result = lookup(reader, [PLAY], [{"user_id": ENTITY}, {"user_id": "user_000002"}])

    assert [entry.status for entry in result.entries] == ["backend_error", "backend_error"]
    assert {entry.error_type for entry in result.entries} == {"TimeoutError"}


def test_a_reader_that_returns_the_wrong_number_of_items_is_a_protocol_error():
    reader = FakeReader(reads=[RecordRead()])

    result = lookup(reader, [PLAY], [{"user_id": ENTITY}, {"user_id": "user_000002"}])

    assert {entry.error_type for entry in result.entries} == {"ProtocolError"}


def test_the_plan_runs_entity_by_entity_and_the_reader_is_asked_once():
    reader = FakeReader(reads=[RecordRead()] * 4)

    result = lookup(reader, [PLAY, SKIP], [{"user_id": "a"}, {"user_id": "b"}])

    assert len(reader.plans) == 1
    assert [(spec.name, value) for spec, value in reader.plans[0]] == [
        (PLAY.name, "a"),
        (SKIP.name, "a"),
        (PLAY.name, "b"),
        (SKIP.name, "b"),
    ]
    assert [entry.status for entry in result.entries] == ["missing"] * 4
