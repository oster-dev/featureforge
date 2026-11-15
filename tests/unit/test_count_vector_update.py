"""Ordering policy for count-vector updates (ADR-009)."""

from datetime import UTC, datetime, timedelta
from itertools import product

import pytest

from featureforge.streaming.contracts.count_vector import (
    CountVector,
    UpdateConflictError,
    decide_count_vector_update,
)
from featureforge.streaming.contracts.user_skip_rate import (
    SkipRateUpdateConflictError,
    UserSkipRateFeatureV1,
    decide_skip_rate_update,
)

OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)
NAMES = ("play_count", "skip_count")

decide = decide_count_vector_update


def vector(*values, minutes=0, entity="user_000001", names=NAMES):
    return CountVector(
        entity=entity,
        observation_time=OBSERVATION + timedelta(minutes=minutes),
        counts=dict(zip(names, values, strict=True)),
    )


def single(value, *, minutes=0):
    return vector(value, minutes=minutes, names=("play_count",))


def skip_feature(plays, skips, *, minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    denominator = plays + skips
    return UserSkipRateFeatureV1.model_validate(
        {
            "user_id": "user_000001",
            "play_count": plays,
            "skip_count": skips,
            "skip_rate": skips / denominator if denominator else 0.0,
            "observation_time": observation,
            "window_start_exclusive": observation - timedelta(hours=24),
        }
    )


def test_first_write():
    assert decide(None, vector(1, 1)) == "written"


@pytest.mark.parametrize(
    ("plays", "skips", "decision"),
    [
        (1, 1, "unchanged"),
        (2, 1, "written"),
        (1, 2, "written"),
        (2, 2, "written"),
        (0, 1, "stale"),
        (1, 0, "stale"),
        (0, 0, "stale"),
    ],
)
def test_same_window_vector_ordering(plays, skips, decision):
    assert decide(vector(1, 1), vector(plays, skips)) == decision


@pytest.mark.parametrize(("plays", "skips"), [(2, 0), (0, 2)])
def test_incomparable_vectors_raise_conflict(plays, skips):
    with pytest.raises(UpdateConflictError, match="Incomparable"):
        decide(vector(1, 1), vector(plays, skips))


@pytest.mark.parametrize(
    ("incoming", "decision"),
    [(2, "stale"), (3, "unchanged"), (4, "written")],
)
def test_single_component_follows_the_append_only_rule(incoming, decision):
    assert decide(single(3), single(incoming)) == decision


def test_single_component_never_conflicts():
    for stored, incoming in product(range(6), repeat=2):
        if incoming == stored:
            expected = "unchanged"
        elif incoming > stored:
            expected = "written"
        else:
            expected = "stale"
        assert decide(single(stored), single(incoming)) == expected, (stored, incoming)


def test_newer_window_replaces_an_older_value_with_zero():
    assert decide(vector(5, 5), vector(0, 0, minutes=1)) == "written"


def test_older_window_is_stale_even_with_larger_counts():
    assert decide(vector(1, 1), vector(99, 99, minutes=-1)) == "stale"


def test_replayed_previous_value_cannot_undo_a_late_correction():
    initial, corrected = single(1), single(2)
    assert decide(initial, corrected) == "written"
    assert decide(corrected, initial) == "stale"
    assert decide(corrected, corrected) == "unchanged"


def test_identical_delivery_stays_unchanged():
    for _ in range(3):
        assert decide(vector(3, 1), vector(3, 1)) == "unchanged"


def test_component_order_does_not_matter():
    stored = CountVector("user_000001", OBSERVATION, {"play_count": 1, "skip_count": 1})
    incoming = CountVector("user_000001", OBSERVATION, {"skip_count": 2, "play_count": 2})
    assert decide(stored, incoming) == "written"


def test_different_entities_cannot_be_compared():
    with pytest.raises(ValueError, match="different entities"):
        decide(vector(1, 1), vector(1, 1, entity="other_user"))


def test_different_components_cannot_be_compared():
    with pytest.raises(ValueError, match="components differ"):
        decide(vector(1, 1), single(1))


def test_empty_counts_are_rejected():
    with pytest.raises(ValueError, match="must not be empty"):
        CountVector(entity="user_000001", observation_time=OBSERVATION, counts={})


def test_conflict_error_is_a_value_error():
    assert issubclass(UpdateConflictError, ValueError)


def test_matches_the_skip_rate_decision_exhaustively():
    values = list(product(range(4), repeat=2))
    for stored, incoming, minutes in product(values, values, (-1, 0, 1)):
        try:
            expected = decide_skip_rate_update(
                skip_feature(*stored),
                skip_feature(*incoming, minutes=minutes),
            )
        except SkipRateUpdateConflictError:
            expected = "conflict"
        try:
            actual = decide(vector(*stored), vector(*incoming, minutes=minutes))
        except UpdateConflictError:
            actual = "conflict"
        assert actual == expected, (stored, incoming, minutes)
