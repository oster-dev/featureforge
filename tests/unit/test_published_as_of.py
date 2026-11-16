"""Published-as-of retrieval over the history table (ADR-010)."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import Evidence, HistoryTable, open_local_catalog
from featureforge.streaming.published_as_of import published_as_of

PLAY = load_spec(Path(__file__).parents[2] / "feature_repo" / "specs" / "user_play_count_1h.yaml")
TTL = PLAY.online.ttl
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)
T0 = datetime(2026, 1, 2, 12, 0, 5, tzinfo=UTC)
MICRO = timedelta(microseconds=1)


def record(count, *, entity="user_1", minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    return FeatureRecord(
        feature_name=PLAY.name,
        entity_key="user_id",
        entity_value=entity,
        observation_time=observation,
        window_start_exclusive=observation - PLAY.window.size,
        counts={"play_count": count},
        derived={},
    )


@pytest.fixture
def history(tmp_path):
    return HistoryTable(open_local_catalog(tmp_path), PLAY)


def publish(history, count, published_at, **kwargs):
    history.append([Evidence(record=record(count, **kwargs), published_at=published_at)])


def read(history, as_of, entity="user_1"):
    return published_as_of(history, [entity], as_of)[entity]


def test_an_empty_history_has_nothing_published(history):
    result = read(history, T0)

    assert result.status == "not_published"
    assert result.record is None


def test_nothing_is_visible_before_the_first_publication(history):
    publish(history, 1, T0)

    result = read(history, T0 - MICRO)

    assert result.status == "not_published"
    assert result.record is None


def test_a_value_is_visible_from_its_published_at(history):
    publish(history, 1, T0)

    result = read(history, T0)

    assert result.status == "present"
    assert result.record.counts == {"play_count": 1}
    assert result.published_at == T0
    assert result.expires_at == T0 + TTL


def test_a_correction_replaces_the_value_only_from_its_own_published_at(history):
    late = T0 + timedelta(seconds=30)
    publish(history, 1, T0)
    publish(history, 2, late)

    assert read(history, late - MICRO).record.counts == {"play_count": 1}
    assert read(history, late).record.counts == {"play_count": 2}


def test_the_ttl_cutoff_is_exact(history):
    publish(history, 1, T0)

    assert read(history, T0 + TTL - MICRO).status == "present"

    expired = read(history, T0 + TTL)
    assert expired.status == "expired"
    assert expired.record is None
    assert expired.published_at == T0
    assert expired.expires_at == T0 + TTL


def test_a_later_version_extends_the_availability(history):
    assert TTL > timedelta(hours=1)
    second = T0 + timedelta(hours=1)
    publish(history, 1, T0)
    publish(history, 2, second)

    still_visible = read(history, T0 + TTL + timedelta(minutes=30))

    assert still_visible.status == "present"
    assert still_visible.record.counts == {"play_count": 2}
    assert read(history, second + TTL).status == "expired"


def test_a_newer_window_replaces_the_older_one_even_with_a_lower_count(history):
    publish(history, 3, T0, minutes=0)
    publish(history, 1, T0 + timedelta(minutes=1), minutes=1)

    result = read(history, T0 + timedelta(minutes=1))

    assert result.record.observation_time == OBSERVATION + timedelta(minutes=1)
    assert result.record.counts == {"play_count": 1}


def test_a_tie_in_published_at_prefers_the_newer_window(history):
    publish(history, 3, T0, minutes=0)
    publish(history, 1, T0, minutes=1)

    result = read(history, T0)

    assert result.record.observation_time == OBSERVATION + timedelta(minutes=1)


def test_entities_are_independent_and_unknown_entities_are_not_published(history):
    publish(history, 1, T0, entity="user_1")
    publish(history, 5, T0, entity="user_2")

    results = published_as_of(history, ["user_1", "user_2", "unknown"], T0)

    assert list(results) == ["user_1", "user_2", "unknown"]
    assert results["user_1"].record.counts == {"play_count": 1}
    assert results["user_2"].record.counts == {"play_count": 5}
    assert results["unknown"].status == "not_published"


def test_duplicate_entities_are_collapsed(history):
    publish(history, 1, T0)

    results = published_as_of(history, ["user_1", "user_1"], T0)

    assert list(results) == ["user_1"]


def test_no_entities_return_nothing(history):
    assert published_as_of(history, [], T0) == {}


def test_a_naive_as_of_is_rejected(history):
    with pytest.raises(ValueError, match="timezone"):
        published_as_of(history, ["user_1"], datetime(2026, 1, 2, 12, 0, 5))


def test_an_empty_entity_is_rejected(history):
    with pytest.raises(ValueError, match="non-empty"):
        published_as_of(history, [""], T0)
