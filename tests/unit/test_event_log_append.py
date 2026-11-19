"""Contracts of the idempotent event append without Spark (ADR-015)."""

from datetime import UTC, datetime

import pytest

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.event_log import EventIdentityConflict, EventLog
from featureforge.streaming.history import open_local_catalog


def make_event(number=1, **overrides) -> InteractionEventV1:
    payload = {
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
    return InteractionEventV1.model_validate(payload)


def open_log(tmp_path):
    return EventLog(open_local_catalog(tmp_path))


def rows(log):
    return log.table.scan().to_arrow().to_pylist()


def snapshot_id(log):
    log.table.refresh()
    snapshot = log.table.current_snapshot()
    return None if snapshot is None else snapshot.snapshot_id


def test_new_events_are_appended_with_their_receipt_time(tmp_path):
    log = open_log(tmp_path)

    added = log.append([make_event(1), make_event(2)])

    stored = {row["event_id"]: row for row in rows(log)}
    assert added == 2
    assert set(stored) == {
        "00000000-0000-4000-8000-000000000001",
        "00000000-0000-4000-8000-000000000002",
    }
    first = stored["00000000-0000-4000-8000-000000000001"]
    assert first["ingested_at"] == datetime(2026, 1, 1, 10, 1, tzinfo=UTC)
    assert first["play_seconds"] == 30


def test_the_same_batch_twice_leaves_the_table_unchanged(tmp_path):
    log = open_log(tmp_path)
    batch = [make_event(1), make_event(2)]
    log.append(batch)
    before = snapshot_id(log)

    added = log.append(batch)

    assert added == 0
    assert len(rows(log)) == 2
    assert snapshot_id(log) == before


def test_a_later_receipt_of_the_same_event_is_dropped_and_the_first_one_stays(tmp_path):
    log = open_log(tmp_path)
    log.append([make_event(1)])

    added = log.append([make_event(1, ingested_at="2026-01-01T10:05:00Z")])

    [row] = rows(log)
    assert added == 0
    assert row["ingested_at"] == datetime(2026, 1, 1, 10, 1, tzinfo=UTC)


def test_a_second_writer_object_sees_what_the_first_one_wrote(tmp_path):
    open_log(tmp_path).append([make_event(1)])

    added = open_log(tmp_path).append([make_event(1), make_event(2)])

    assert added == 1
    assert len(rows(open_log(tmp_path))) == 2


def test_the_same_event_twice_in_one_batch_is_stored_once(tmp_path):
    log = open_log(tmp_path)

    added = log.append([make_event(1), make_event(1, ingested_at="2026-01-01T10:09:00Z")])

    [row] = rows(log)
    assert added == 1
    assert row["ingested_at"] == datetime(2026, 1, 1, 10, 1, tzinfo=UTC)


def test_another_payload_for_a_stored_id_fails_and_writes_nothing(tmp_path):
    log = open_log(tmp_path)
    log.append([make_event(1)])
    before = snapshot_id(log)

    with pytest.raises(EventIdentityConflict, match="play_seconds"):
        log.append([make_event(2), make_event(1, play_seconds=777)])

    assert [row["event_id"] for row in rows(log)] == ["00000000-0000-4000-8000-000000000001"]
    assert snapshot_id(log) == before


def test_another_payload_inside_one_batch_fails_and_creates_no_snapshot(tmp_path):
    log = open_log(tmp_path)

    with pytest.raises(EventIdentityConflict, match="user_id"):
        log.append([make_event(1), make_event(1, user_id="user_000002")])

    assert rows(log) == []
    assert snapshot_id(log) is None


def test_the_conflict_names_the_fields_and_never_the_values(tmp_path):
    log = open_log(tmp_path)
    log.append([make_event(1)])

    with pytest.raises(EventIdentityConflict) as conflict:
        log.append([make_event(1, play_seconds=777)])

    assert "777" not in str(conflict.value)


def test_an_empty_batch_writes_nothing(tmp_path):
    log = open_log(tmp_path)

    assert log.append([]) == 0
    assert snapshot_id(log) is None


def test_events_of_two_days_land_in_two_partitions(tmp_path):
    log = open_log(tmp_path)

    log.append(
        [
            make_event(1),
            make_event(2, event_time="2026-01-02T10:00:00Z", ingested_at="2026-01-02T10:01:00Z"),
        ]
    )

    assert len(list(log.table.scan().plan_files())) == 2


def test_without_the_stored_lookup_a_repeated_batch_would_duplicate(tmp_path, monkeypatch):
    """Mutation: dropping the duplicate check must break the idempotency contract."""
    log = open_log(tmp_path)
    monkeypatch.setattr(EventLog, "_stored", lambda self, event_ids: {})
    batch = [make_event(1), make_event(2)]
    log.append(batch)

    added = log.append(batch)

    assert added == 2
    assert len(rows(log)) == 4
