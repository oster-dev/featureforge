"""Contracts of the event writer batch without Kafka or Spark (ADR-015)."""

import json

import pytest

from featureforge.streaming import event_writer
from featureforge.streaming.event_log import EventIdentityConflict, EventLog
from featureforge.streaming.event_writer import consume_batch
from featureforge.streaming.history import open_local_catalog


def payload(number=1, **overrides) -> str:
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
    )


def open_log(tmp_path):
    return EventLog(open_local_catalog(tmp_path))


def rows(log):
    return log.table.scan().to_arrow().to_pylist()


def test_valid_payloads_become_facts_as_strings_or_bytes(tmp_path):
    log = open_log(tmp_path)

    result = consume_batch([payload(1), payload(2).encode()], log)

    assert result == {"appended": 2}
    assert len(rows(log)) == 2


def test_rejected_payloads_are_counted_by_reason_and_never_stored(tmp_path):
    log = open_log(tmp_path)
    before_event = payload(3, ingested_at="2026-01-01T09:00:00Z")

    result = consume_batch(
        [payload(1), "{not json", before_event, b"\xff\xfe", None],
        log,
    )

    assert result == {
        "appended": 1,
        "rejected_invalid_json": 1,
        "rejected_event_contract_violation": 1,
        "rejected_invalid_encoding": 1,
        "rejected_tombstone": 1,
    }
    assert len(rows(log)) == 1


def test_a_repeated_batch_counts_duplicates_and_adds_nothing(tmp_path):
    log = open_log(tmp_path)
    batch = [payload(1), payload(2)]
    consume_batch(batch, log)

    result = consume_batch(batch + [payload(1)], log)

    assert result == {"duplicate": 3}
    assert len(rows(log)) == 2


def test_a_batch_of_only_rejections_writes_no_snapshot(tmp_path):
    log = open_log(tmp_path)

    result = consume_batch(["{not json", None], log)

    assert result == {"rejected_invalid_json": 1, "rejected_tombstone": 1}
    log.table.refresh()
    assert log.table.current_snapshot() is None


def test_an_identity_conflict_propagates_and_writes_nothing(tmp_path):
    log = open_log(tmp_path)

    with pytest.raises(EventIdentityConflict):
        consume_batch([payload(2), payload(1), payload(1, play_seconds=777)], log)

    log.table.refresh()
    assert log.table.current_snapshot() is None


def test_unexpected_errors_are_not_turned_into_rejections(tmp_path, monkeypatch):
    log = open_log(tmp_path)

    def broken(_payload):
        raise RuntimeError("bug")

    monkeypatch.setattr(event_writer, "route_event_json", broken)

    with pytest.raises(RuntimeError, match="bug"):
        consume_batch([payload(1)], log)
