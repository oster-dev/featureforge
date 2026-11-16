"""Contracts of the streaming history table without Spark (ADR-011)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import (
    Evidence,
    HistorySchemaError,
    HistoryTable,
    history_columns,
    open_local_catalog,
    update_id,
)

SPECS = Path(__file__).parents[2] / "feature_repo" / "specs"
SKIP_RATE = load_spec(SPECS / "user_skip_rate_24h.yaml")
PLAY_COUNT = load_spec(SPECS / "user_play_count_1h.yaml")
CONTENT = load_spec(SPECS / "content_play_count_1h.yaml")

OBSERVATION = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
PUBLISHED = datetime(2026, 1, 1, 12, 0, 5, tzinfo=UTC)


def skip_record(plays, skips, *, user="user_000001", observation=OBSERVATION):
    total = plays + skips
    return FeatureRecord(
        feature_name=SKIP_RATE.name,
        entity_key="user_id",
        entity_value=user,
        observation_time=observation,
        window_start_exclusive=observation - timedelta(hours=24),
        counts={"play_count": plays, "skip_count": skips},
        derived={"skip_rate": skips / total if total else 0.0},
    )


def evidence(record, published_at=PUBLISHED):
    return Evidence(record=record, published_at=published_at)


def open_history(tmp_path, spec=SKIP_RATE):
    return HistoryTable(open_local_catalog(tmp_path), spec)


def rows(history):
    return history.table.scan().to_arrow().to_pylist()


def test_columns_follow_the_spec():
    names = [column.name for column in history_columns(SKIP_RATE)]

    assert names == [
        "update_id",
        "spec_version",
        "user_id",
        "observation_time",
        "window_start_exclusive",
        "play_count",
        "skip_count",
        "skip_rate",
        "published_at",
    ]


def test_play_count_and_content_specs_get_their_own_columns():
    play_names = [column.name for column in history_columns(PLAY_COUNT)]
    content_names = [column.name for column in history_columns(CONTENT)]

    assert play_names == [
        "update_id",
        "spec_version",
        "user_id",
        "observation_time",
        "window_start_exclusive",
        "play_count",
        "published_at",
    ]
    assert content_names[2] == "content_id"
    assert content_names[-1] == "published_at"


def test_column_names_must_not_clash_with_history_columns():
    clashing = SKIP_RATE.model_copy(update={"entity": "published_at"})

    with pytest.raises(ValueError, match="clash"):
        history_columns(clashing)


def test_update_id_is_stable_and_follows_the_identity():
    first = skip_record(1, 1)

    assert update_id(SKIP_RATE, first) == update_id(SKIP_RATE, skip_record(1, 1))
    assert len(update_id(SKIP_RATE, first)) == 64
    assert update_id(SKIP_RATE, first) != update_id(SKIP_RATE, skip_record(2, 1))
    assert update_id(SKIP_RATE, first) != update_id(SKIP_RATE, skip_record(1, 1, user="user_2"))
    assert update_id(SKIP_RATE, first) != update_id(
        SKIP_RATE, skip_record(1, 1, observation=OBSERVATION + timedelta(minutes=1))
    )


def test_table_is_created_from_the_spec_and_partitioned_by_day(tmp_path):
    history = open_history(tmp_path)

    fields = history.table.spec().fields
    assert [field.name for field in fields] == ["published_at_day"]
    assert str(fields[0].transform) == "day"
    assert [field.name for field in history.table.schema().fields] == [
        column.name for column in history_columns(SKIP_RATE)
    ]


def test_append_writes_one_row_per_update(tmp_path):
    history = open_history(tmp_path)

    assert history.append([evidence(skip_record(1, 1))]) == 1

    (row,) = rows(history)
    assert row["user_id"] == "user_000001"
    assert row["spec_version"] == 1
    assert row["play_count"] == 1
    assert row["skip_count"] == 1
    assert row["skip_rate"] == 0.5
    assert row["observation_time"] == OBSERVATION
    assert row["window_start_exclusive"] == OBSERVATION - timedelta(hours=24)
    assert row["published_at"] == PUBLISHED


def test_first_row_wins_inside_one_batch(tmp_path):
    history = open_history(tmp_path)
    record = skip_record(1, 1)

    added = history.append(
        [evidence(record, PUBLISHED), evidence(record, PUBLISHED + timedelta(seconds=30))]
    )

    assert added == 1
    assert [row["published_at"] for row in rows(history)] == [PUBLISHED]


def test_a_redelivery_adds_no_row_and_keeps_the_first_published_at(tmp_path):
    history = open_history(tmp_path)
    record = skip_record(1, 1)
    history.append([evidence(record, PUBLISHED)])

    added = history.append([evidence(record, PUBLISHED + timedelta(minutes=5))])

    assert added == 0
    assert [row["published_at"] for row in rows(history)] == [PUBLISHED]


def test_a_correction_with_a_higher_count_is_a_new_update(tmp_path):
    history = open_history(tmp_path)

    history.append([evidence(skip_record(1, 1), PUBLISHED)])
    history.append([evidence(skip_record(2, 1), PUBLISHED + timedelta(seconds=30))])

    stored = sorted(rows(history), key=lambda row: row["play_count"])
    assert [row["play_count"] for row in stored] == [1, 2]
    assert stored[1]["published_at"] == PUBLISHED + timedelta(seconds=30)


def test_reopening_the_catalog_keeps_the_evidence_and_the_idempotency(tmp_path):
    record = skip_record(1, 1)
    open_history(tmp_path).append([evidence(record)])

    reopened = open_history(tmp_path)

    assert reopened.append([evidence(record, PUBLISHED + timedelta(hours=1))]) == 0
    assert len(rows(reopened)) == 1


def test_an_empty_append_adds_nothing(tmp_path):
    history = open_history(tmp_path)

    assert history.append([]) == 0
    assert rows(history) == []


def test_a_naive_published_at_is_rejected(tmp_path):
    history = open_history(tmp_path)

    with pytest.raises(ValueError, match="timezone"):
        history.append([evidence(skip_record(1, 1), datetime(2026, 1, 1, 12, 0, 5))])


def test_a_record_of_another_feature_is_rejected(tmp_path):
    history = open_history(tmp_path)
    foreign = FeatureRecord(
        feature_name=PLAY_COUNT.name,
        entity_key="user_id",
        entity_value="user_000001",
        observation_time=OBSERVATION,
        window_start_exclusive=OBSERVATION - timedelta(hours=1),
        counts={"play_count": 1},
        derived={},
    )

    with pytest.raises(ValueError, match="cannot be added"):
        history.append([evidence(foreign)])


def test_a_table_that_does_not_match_the_spec_fails_loudly(tmp_path):
    open_history(tmp_path)
    other = PLAY_COUNT.model_copy(update={"offline": SKIP_RATE.offline})

    with pytest.raises(HistorySchemaError, match="does not match"):
        open_history(tmp_path, other)
