"""The Kafka loop of the event writer against a fake consumer (ADR-015)."""

import json
from types import SimpleNamespace

import pytest

from featureforge.streaming import event_writer
from featureforge.streaming.event_log import EventIdentityConflict, EventLog
from featureforge.streaming.event_writer import parse_arguments, write_events
from featureforge.streaming.history import open_local_catalog

TOPIC = "events.test"


def payload(number=1, **overrides) -> bytes:
    return json.dumps(
        {
            "event_id": f"00000000-0000-4000-8000-{number:012d}",
            "event_time": "2026-01-01T10:00:00Z",
            "ingested_at": "2026-01-01T10:01:00Z",
            "user_id": "user_000001",
            "content_id": "content_000001",
            "event_type": "play",
            "play_seconds": 30,
            "source": "web",
            "schema_version": "v1",
            **overrides,
        }
    ).encode()


def stored_rows(root):
    return EventLog(open_local_catalog(root)).table.scan().to_arrow().num_rows


class PollingStoppedError(Exception):
    pass


class Message:
    def __init__(self, offset, value):
        self._offset = offset
        self._value = value

    def error(self):
        return None

    def value(self):
        return self._value

    def offset(self):
        return self._offset


class FakeConsumer:
    def __init__(self, values, root, *, committed=-1001, idle_stop=False, partitions=1):
        self.messages = [Message(offset, value) for offset, value in enumerate(values)]
        self.high = len(values)
        self.committed_offset = committed
        self.idle_stop = idle_stop
        self.partitions = partitions
        self.root = root
        self.commits = []
        self.assigned = None
        self.closed = False
        self.idle_polls = 0

    def list_topics(self, topic, **_kwargs):
        partitions = {number: None for number in range(self.partitions)}
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions=partitions)})

    def get_watermark_offsets(self, _partition, **_kwargs):
        return 0, self.high

    def committed(self, _partitions, **_kwargs):
        return [SimpleNamespace(error=None, offset=self.committed_offset)]

    def assign(self, partitions):
        self.assigned = partitions[0].offset
        self.messages = [m for m in self.messages if m.offset() >= self.assigned]

    def poll(self, **_kwargs):
        if self.messages:
            return self.messages.pop(0)
        self.idle_polls += 1
        if self.idle_stop and self.idle_polls > 1:
            raise PollingStoppedError
        return None

    def commit(self, offsets, **_kwargs):
        self.commits.append((offsets[0].offset, stored_rows(self.root)))
        return []

    def close(self):
        self.closed = True


def arguments(tmp_path, *extra):
    return parse_arguments(
        ["--topic", TOPIC, "--group-id", "g", "--events-root", str(tmp_path), *extra]
    )


def install(monkeypatch, consumer):
    monkeypatch.setattr(event_writer, "Consumer", lambda _config: consumer)


def test_the_topic_and_the_group_are_required(tmp_path):
    with pytest.raises(SystemExit):
        parse_arguments(["--group-id", "g", "--events-root", str(tmp_path)])


def test_a_batch_is_appended_before_its_offset_is_committed(tmp_path, monkeypatch):
    consumer = FakeConsumer([payload(1), payload(2), payload(3)], tmp_path)
    install(monkeypatch, consumer)

    results = write_events(arguments(tmp_path, "--batch-size", "2", "--bounded"))

    assert results == {"appended": 3}
    assert consumer.commits == [(2, 2), (3, 3)]
    assert consumer.closed


def test_rejected_payloads_advance_the_offset_and_are_counted(tmp_path, monkeypatch):
    consumer = FakeConsumer([payload(1), b"{not json", None], tmp_path)
    install(monkeypatch, consumer)

    results = write_events(arguments(tmp_path, "--bounded"))

    assert results == {"appended": 1, "rejected_invalid_json": 1, "rejected_tombstone": 1}
    assert consumer.commits == [(3, 1)]


def test_an_identity_conflict_commits_nothing_and_closes_the_consumer(tmp_path, monkeypatch):
    consumer = FakeConsumer([payload(1), payload(1, play_seconds=777)], tmp_path)
    install(monkeypatch, consumer)

    with pytest.raises(EventIdentityConflict):
        write_events(arguments(tmp_path, "--bounded"))

    assert consumer.commits == []
    assert consumer.closed
    assert stored_rows(tmp_path) == 0


def test_a_run_resumes_at_the_committed_offset(tmp_path, monkeypatch):
    consumer = FakeConsumer([payload(1), payload(2), payload(3)], tmp_path, committed=2)
    install(monkeypatch, consumer)

    results = write_events(arguments(tmp_path, "--bounded"))

    assert consumer.assigned == 2
    assert results == {"appended": 1}
    assert consumer.commits == [(3, 1)]


def test_a_fully_read_topic_is_a_run_without_events(tmp_path, monkeypatch):
    consumer = FakeConsumer([payload(1), payload(2)], tmp_path, committed=2)
    install(monkeypatch, consumer)

    assert write_events(arguments(tmp_path, "--bounded")) == {}
    assert consumer.assigned is None
    assert consumer.commits == []


def test_an_empty_topic_is_a_bounded_run_without_events(tmp_path, monkeypatch):
    consumer = FakeConsumer([], tmp_path)
    install(monkeypatch, consumer)

    assert write_events(arguments(tmp_path, "--bounded")) == {}
    assert consumer.closed


def test_an_idle_topic_flushes_a_partial_batch_in_an_unbounded_run(tmp_path, monkeypatch):
    consumer = FakeConsumer([payload(1), payload(2)], tmp_path, idle_stop=True)
    install(monkeypatch, consumer)

    with pytest.raises(PollingStoppedError):
        write_events(arguments(tmp_path, "--batch-size", "100"))

    assert consumer.commits == [(2, 2)]
    assert consumer.closed


def test_a_topic_with_several_partitions_is_refused(tmp_path, monkeypatch):
    consumer = FakeConsumer([payload(1)], tmp_path, partitions=2)
    install(monkeypatch, consumer)

    with pytest.raises(RuntimeError, match="single-partition"):
        write_events(arguments(tmp_path, "--bounded"))

    assert consumer.closed


def test_a_batch_size_below_one_is_refused(tmp_path):
    with pytest.raises(ValueError, match="batch-size"):
        write_events(arguments(tmp_path, "--batch-size", "0"))
