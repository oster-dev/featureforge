"""Evidence append of the spec-driven materializer without external services (ADR-011)."""

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import pytest

pytest.importorskip("confluent_kafka")


from featureforge.spec.loader import load_spec
from featureforge.streaming import materialize_spec as module
from featureforge.streaming.contracts.feature_record import stamp
from featureforge.streaming.history import HistoryTable, open_local_catalog

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)


def make_arguments(**overrides):
    values = {
        "spec": Path("unused.yaml"),
        "bootstrap_servers": "127.0.0.1:9092",
        "redis_url": "redis://127.0.0.1:6379/0",
        "topic": "features.replay.test.v1",
        "group_id": "materializer-history-test",
        "key_prefix": "featureforge-stream:test:history",
        "commit_every": 1000,
        "bounded": True,
    }
    values.update(overrides)
    return argparse.Namespace(**values)


def payload(entity, plays):
    return json.dumps(
        {
            "feature_schema_version": "v1",
            "feature_name": PLAY.name,
            "user_id": entity,
            "observation_time": stamp(OBSERVATION),
            "window_start_exclusive": stamp(OBSERVATION - PLAY.window.size),
            "play_count": plays,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


class FakeMessage:
    def __init__(self, offset, value):
        self._offset = offset
        self._value = value

    def offset(self):
        return self._offset

    def partition(self):
        return 0

    def value(self):
        return self._value

    def error(self):
        return None


def message(offset, entity="user_000001", plays=3):
    return FakeMessage(offset, payload(entity, plays))


class FakeConsumer:
    def __init__(self, *, high, messages, actions):
        self.high = high
        self.messages = iter(messages)
        self.actions = actions
        self.commits = []

    def list_topics(self, topic, timeout):
        topic_metadata = Mock(error=None, partitions={0: Mock()})
        return Mock(topics={topic: topic_metadata})

    def get_watermark_offsets(self, partition, timeout, cached):
        return 0, self.high

    def committed(self, partitions, timeout):
        return [Mock(offset=-1001, error=None)]

    def assign(self, partitions):
        pass

    def poll(self, timeout):
        try:
            return next(self.messages)
        except StopIteration as error:
            raise AssertionError("Materializer polled beyond the supplied messages") from error

    def commit(self, *, offsets, asynchronous):
        assert asynchronous is False
        self.commits.append(offsets[0].offset)
        self.actions.append(("commit", offsets[0].offset))
        return []

    def close(self):
        pass


class FakeStore:
    def __init__(self, actions, outcomes):
        self.actions = actions
        self.outcomes = iter(outcomes)

    def write(self, record):
        self.actions.append(("write", record.entity_value))
        try:
            return next(self.outcomes)
        except StopIteration as error:
            raise AssertionError("Unexpected additional Redis write") from error


class FakeHistory:
    def __init__(self, actions, error=None):
        self.actions = actions
        self.error = error
        self.batches = []

    def append(self, evidence):
        self.actions.append(("append", [item.record.entity_value for item in evidence]))
        if self.error is not None:
            raise self.error
        self.batches.append(list(evidence))
        return len(evidence)


class Run:
    def __init__(self, monkeypatch, *, messages, outcomes, history_factory, commit_every=1000):
        self.actions = []
        self.consumer = FakeConsumer(high=len(messages), messages=messages, actions=self.actions)
        self.history = history_factory(self.actions)

        redis_client = Mock()
        redis_client.ping.return_value = True
        context = MagicMock()
        context.__enter__.return_value = redis_client
        context.__exit__.return_value = False

        monkeypatch.setattr(module, "Redis", SimpleNamespace(from_url=Mock(return_value=context)))
        monkeypatch.setattr(module, "Consumer", Mock(return_value=self.consumer))
        monkeypatch.setattr(
            module, "RedisSpecStore", Mock(return_value=FakeStore(self.actions, outcomes))
        )
        monkeypatch.setattr(module, "open_history", Mock(return_value=self.history))
        self.arguments = make_arguments(commit_every=commit_every)

    def run(self):
        return module.materialize(self.arguments, PLAY)


def fake_history(actions):
    return FakeHistory(actions)


def test_evidence_is_appended_after_the_writes_and_before_the_commit(monkeypatch):
    run = Run(
        monkeypatch,
        messages=[message(0, "user_000001"), message(1, "user_000002")],
        outcomes=["written", "written"],
        history_factory=fake_history,
    )

    run.run()

    assert run.actions == [
        ("write", "user_000001"),
        ("write", "user_000002"),
        ("append", ["user_000001", "user_000002"]),
        ("commit", 2),
    ]


def test_each_batch_is_appended_before_its_own_commit(monkeypatch):
    run = Run(
        monkeypatch,
        messages=[message(0, "user_1"), message(1, "user_2"), message(2, "user_3")],
        outcomes=["written", "written", "written"],
        history_factory=fake_history,
        commit_every=2,
    )

    run.run()

    assert run.actions == [
        ("write", "user_1"),
        ("write", "user_2"),
        ("append", ["user_1", "user_2"]),
        ("commit", 2),
        ("write", "user_3"),
        ("append", ["user_3"]),
        ("commit", 3),
    ]


def test_a_stale_update_adds_no_evidence(monkeypatch):
    run = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["stale"],
        history_factory=fake_history,
    )

    results = run.run()

    assert results["stale"] == 1
    assert run.history.batches == []
    assert run.actions == [("write", "user_000001"), ("commit", 1)]


def test_an_unchanged_update_is_offered_to_the_history(monkeypatch):
    run = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["unchanged"],
        history_factory=fake_history,
    )

    run.run()

    assert run.actions == [("write", "user_000001"), ("append", ["user_000001"]), ("commit", 1)]


def test_published_at_is_the_wall_clock_after_the_store_write(monkeypatch):
    run = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["written"],
        history_factory=fake_history,
    )
    before = datetime.now(UTC)

    run.run()

    after = datetime.now(UTC)
    (batch,) = run.history.batches
    assert before <= batch[0].published_at <= after


def test_a_failed_append_does_not_commit_the_offset(monkeypatch):
    run = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["written"],
        history_factory=lambda actions: FakeHistory(actions, error=RuntimeError("append failed")),
    )

    with pytest.raises(RuntimeError, match="append failed"):
        run.run()

    assert run.consumer.commits == []
    assert ("commit", 1) not in run.actions


def test_without_a_history_root_nothing_changes(monkeypatch):
    run = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["written"],
        history_factory=lambda actions: None,
    )

    run.run()

    assert run.actions == [("write", "user_000001"), ("commit", 1)]


def test_open_history_is_off_without_a_root_and_on_with_one(tmp_path):
    assert module.open_history(make_arguments(), PLAY) is None
    assert isinstance(
        module.open_history(make_arguments(history_root=tmp_path), PLAY), HistoryTable
    )


def test_a_replay_after_a_failed_append_records_a_later_published_at(monkeypatch, tmp_path):
    table = HistoryTable(open_local_catalog(tmp_path), PLAY)
    failing = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["written"],
        history_factory=lambda actions: FakeHistory(actions, error=RuntimeError("crash")),
    )
    with pytest.raises(RuntimeError, match="crash"):
        failing.run()

    before_replay = datetime.now(UTC)
    replay = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["unchanged"],
        history_factory=lambda actions: table,
    )
    replay.run()

    (row,) = table.table.scan().to_arrow().to_pylist()
    assert row["published_at"] >= before_replay
    assert replay.consumer.commits == [1]


def test_a_redelivered_update_keeps_the_first_published_at(monkeypatch, tmp_path):
    table = HistoryTable(open_local_catalog(tmp_path), PLAY)
    first = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["written"],
        history_factory=lambda actions: table,
    )
    first.run()
    (written,) = table.table.scan().to_arrow().to_pylist()

    replay = Run(
        monkeypatch,
        messages=[message(0)],
        outcomes=["unchanged"],
        history_factory=lambda actions: table,
    )
    replay.run()

    (kept,) = table.table.scan().to_arrow().to_pylist()
    assert kept["published_at"] == written["published_at"]
