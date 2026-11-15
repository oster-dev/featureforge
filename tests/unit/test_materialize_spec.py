"""Unit tests for the spec-driven Kafka-to-Redis materializer without external services."""

import argparse
import json
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest

pytest.importorskip("confluent_kafka")


from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming import materialize_spec as module
from featureforge.streaming.contracts.feature_record import stamp

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)

PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")
CONTENT = load_spec(SPECS / "content_play_count_1h.yaml")

CASES = [
    pytest.param(
        PLAY,
        "user_000001",
        {"play_count": 3},
        "features.user-play-count-1h.v1",
        id="user_play_count_1h",
    ),
    pytest.param(
        SKIP,
        "user_000001",
        {"play_count": 2, "skip_count": 1},
        "features.user-skip-rate-24h.v1",
        id="user_skip_rate_24h",
    ),
    pytest.param(
        CONTENT,
        "content_000001",
        {"play_count": 4},
        "features.content-play-count-1h.v1",
        id="content_play_count_1h",
    ),
]


def make_arguments(**overrides):
    values = {
        "spec": Path("unused.yaml"),
        "bootstrap_servers": "127.0.0.1:9092",
        "redis_url": "redis://127.0.0.1:6379/0",
        "topic": "features.replay.test.v1",
        "group_id": "materializer-unit-test",
        "key_prefix": "featureforge-stream:test:spec",
        "commit_every": 1000,
        "bounded": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def payload(spec, entity, counts, *, minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    data = {
        "feature_schema_version": "v1",
        "feature_name": spec.name,
        spec.keys[0]: entity,
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - spec.window.size),
        **counts,
    }
    for name, expression in spec.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return json.dumps(data, sort_keys=True, separators=(",", ":")).encode()


class FakeMessage:
    def __init__(self, offset, payload):
        self._offset = offset
        self._payload = payload

    def offset(self):
        return self._offset

    def partition(self):
        return 0

    def value(self):
        return self._payload

    def error(self):
        return None


def play_message(offset):
    return FakeMessage(offset, payload(PLAY, "user_000001", {"play_count": 3}))


class FakeConsumer:
    def __init__(self, *, low, high, committed, messages, actions):
        self.low = low
        self.high = high
        self.committed_offset = committed
        self.messages = iter(messages)
        self.actions = actions
        self.assignments = []
        self.commits = []
        self.closed = False

    def list_topics(self, topic, timeout):
        partition_metadata = Mock()
        topic_metadata = Mock(
            error=None,
            partitions={0: partition_metadata},
        )
        return Mock(topics={topic: topic_metadata})

    def get_watermark_offsets(self, partition, timeout, cached):
        return self.low, self.high

    def committed(self, partitions, timeout):
        return [Mock(offset=self.committed_offset, error=None)]

    def assign(self, partitions):
        self.assignments.extend(partitions)

    def poll(self, timeout):
        try:
            return next(self.messages)
        except StopIteration as error:
            raise AssertionError("Materializer polled beyond the supplied test messages") from error

    def commit(self, *, offsets, asynchronous):
        assert asynchronous is False
        next_offset = offsets[0].offset
        self.commits.append(next_offset)
        self.actions.append(("commit", next_offset))
        return []

    def close(self):
        self.closed = True


class FakeStore:
    def __init__(self, actions, outcomes):
        self.actions = actions
        self.outcomes = iter(outcomes)
        self.records = []

    def write(self, record):
        self.actions.append(("write", record.entity_value))
        self.records.append(record)

        try:
            outcome = next(self.outcomes)
        except StopIteration as error:
            raise AssertionError("Unexpected additional Redis write") from error

        if isinstance(outcome, Exception):
            raise outcome

        return outcome


@pytest.fixture
def harness(monkeypatch):
    def build(
        *,
        low=0,
        high=1,
        committed=-1001,
        messages=None,
        outcomes=None,
    ):
        actions = []
        consumer = FakeConsumer(
            low=low,
            high=high,
            committed=committed,
            messages=[play_message(0)] if messages is None else messages,
            actions=actions,
        )
        store = FakeStore(
            actions,
            ["written"] if outcomes is None else outcomes,
        )

        redis_client = Mock()
        redis_client.ping.return_value = True
        redis_context = Mock()
        redis_context.__enter__ = Mock(return_value=redis_client)
        redis_context.__exit__ = Mock(return_value=False)

        redis_factory = Mock(return_value=redis_context)
        consumer_factory = Mock(return_value=consumer)
        store_factory = Mock(return_value=store)

        monkeypatch.setattr(module.Redis, "from_url", redis_factory)
        monkeypatch.setattr(module, "Consumer", consumer_factory)
        monkeypatch.setattr(module, "RedisSpecStore", store_factory)

        return argparse.Namespace(
            actions=actions,
            consumer=consumer,
            store=store,
            redis_client=redis_client,
            redis_factory=redis_factory,
            consumer_factory=consumer_factory,
            store_factory=store_factory,
        )

    return build


@pytest.mark.parametrize(("spec", "entity", "counts", "topic"), CASES)
def test_writes_before_committing_and_uses_ipv4(harness, spec, entity, counts, topic):
    state = harness(messages=[FakeMessage(0, payload(spec, entity, counts))])

    results = module.materialize(make_arguments(commit_every=1, topic=None), spec)

    assert dict(results) == {"written": 1}
    assert state.actions == [
        ("write", entity),
        ("commit", 1),
    ]
    assert state.consumer.closed is True
    assert state.consumer.assignments[0].topic == topic

    configuration = state.consumer_factory.call_args.args[0]
    assert configuration["broker.address.family"] == "v4"
    assert configuration["enable.auto.commit"] is False
    assert configuration["client.id"].endswith(spec.name)


@pytest.mark.parametrize(("spec", "entity", "counts", "topic"), CASES)
def test_default_topic_matches_the_hand_written_defaults(spec, entity, counts, topic):
    assert module.default_topic(spec) == topic


def test_explicit_topic_overrides_the_default(harness):
    state = harness()

    module.materialize(make_arguments(topic="custom.topic.v1"), PLAY)

    assert state.consumer.assignments[0].topic == "custom.topic.v1"


def test_store_receives_client_spec_and_prefix(harness):
    state = harness()

    module.materialize(make_arguments(key_prefix="custom"), PLAY)

    state.store_factory.assert_called_once_with(
        state.redis_client,
        PLAY,
        key_prefix="custom",
    )


def test_failed_write_does_not_commit_failed_record(harness):
    state = harness(
        high=2,
        messages=[play_message(0), play_message(1)],
        outcomes=["written", RuntimeError("Redis write failed")],
    )

    with pytest.raises(RuntimeError, match="Redis write failed"):
        module.materialize(make_arguments(commit_every=1), PLAY)

    assert state.consumer.commits == [1]
    assert state.consumer.closed is True


def test_bounded_run_stops_at_snapshot_end(harness):
    state = harness(
        low=10,
        high=12,
        messages=[play_message(10), play_message(11), play_message(12)],
        outcomes=["written", "written", "written"],
    )

    results = module.materialize(make_arguments(), PLAY)

    assert dict(results) == {"written": 2}
    assert state.consumer.commits == [12]


def test_resumes_from_the_committed_offset(harness):
    state = harness(
        low=10,
        high=13,
        committed=11,
        messages=[play_message(11), play_message(12)],
        outcomes=["written", "written"],
    )

    results = module.materialize(make_arguments(), PLAY)

    assert dict(results) == {"written": 2}
    assert state.consumer.assignments[0].offset == 11
    assert state.consumer.commits == [13]


def test_nothing_pending_returns_without_polling(harness):
    state = harness(low=0, high=3, committed=3, messages=[], outcomes=[])

    results = module.materialize(make_arguments(), PLAY)

    assert results == Counter()
    assert state.consumer.assignments == []
    assert state.consumer.commits == []
    assert state.consumer.closed is True


def test_empty_range_is_refused(harness):
    state = harness(low=5, high=5, messages=[], outcomes=[])

    with pytest.raises(RuntimeError, match="empty Kafka range"):
        module.materialize(make_arguments(), PLAY)

    assert state.consumer.closed is True


def test_tombstone_is_rejected_and_not_committed(harness):
    state = harness(messages=[FakeMessage(0, None)], outcomes=[])

    with pytest.raises(ValueError, match="tombstones"):
        module.materialize(make_arguments(commit_every=1), PLAY)

    assert state.consumer.commits == []
    assert state.consumer.closed is True


def test_invalid_record_is_rejected_and_not_committed(harness):
    state = harness(messages=[FakeMessage(0, b'{"unexpected": 1}')], outcomes=[])

    with pytest.raises(ValueError, match="record fields do not match"):
        module.materialize(make_arguments(commit_every=1), PLAY)

    assert state.consumer.commits == []
    assert state.consumer.closed is True


def test_multi_partition_topic_is_refused(harness):
    state = harness(messages=[], outcomes=[])
    state.consumer.list_topics = lambda topic, timeout: Mock(
        topics={topic: Mock(error=None, partitions={0: Mock(), 1: Mock()})}
    )

    with pytest.raises(RuntimeError, match="single-partition"):
        module.materialize(make_arguments(), PLAY)

    assert state.consumer.closed is True


def test_commit_every_must_be_positive():
    with pytest.raises(ValueError, match="positive"):
        module.materialize(make_arguments(commit_every=0), PLAY)


def test_parse_arguments_defaults():
    arguments = module.parse_arguments(["--spec", "a.yaml", "--group-id", "g"])

    assert arguments.spec == Path("a.yaml")
    assert arguments.group_id == "g"
    assert arguments.topic is None
    assert arguments.key_prefix is None
    assert arguments.commit_every == 1000
    assert arguments.bounded is False
    assert arguments.bootstrap_servers == "127.0.0.1:9092"


def test_parse_arguments_requires_spec_and_group():
    with pytest.raises(SystemExit):
        module.parse_arguments(["--group-id", "g"])
    with pytest.raises(SystemExit):
        module.parse_arguments(["--spec", "a.yaml"])
