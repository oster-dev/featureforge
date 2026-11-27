"""Direction per component in the count-vector decision (ADR-017)."""

from datetime import UTC, datetime, timedelta
from itertools import product

import pytest

from featureforge.streaming.contracts.count_vector import (
    CountVector,
    UpdateConflictError,
    decide_count_vector_update,
)

OBSERVATION = datetime(2026, 1, 1, 11, tzinfo=UTC)
NAMES = ("play_count", "min_play_seconds", "max_play_seconds")
DIRECTION_LIST = ("up", "down", "up")
DIRECTIONS = dict(zip(NAMES, DIRECTION_LIST, strict=True))


def vector(count, smallest, largest, *, minutes=0):
    return CountVector(
        entity="user000001",
        observation_time=OBSERVATION + timedelta(minutes=minutes),
        counts={"play_count": count, "min_play_seconds": smallest, "max_play_seconds": largest},
        directions=DIRECTIONS,
    )


def test_a_late_event_can_lower_the_minimum_and_raise_the_maximum():
    assert decide_count_vector_update(vector(2, 50, 100), vector(3, 40, 120)) == "written"


def test_the_older_state_is_stale_in_the_other_direction():
    assert decide_count_vector_update(vector(3, 40, 120), vector(2, 50, 100)) == "stale"


def test_identical_vectors_are_unchanged():
    assert decide_count_vector_update(vector(3, 40, 120), vector(3, 40, 120)) == "unchanged"


def test_a_count_that_rises_while_the_minimum_rises_cannot_be_ordered():
    with pytest.raises(UpdateConflictError, match="Incomparable"):
        decide_count_vector_update(vector(2, 50, 100), vector(3, 60, 100))


def test_a_missing_extreme_is_before_the_first_event():
    empty = vector(0, None, None)

    assert decide_count_vector_update(empty, vector(1, 100, 100)) == "written"
    assert decide_count_vector_update(vector(1, 100, 100), empty) == "stale"
    assert decide_count_vector_update(empty, vector(0, None, None)) == "unchanged"


def test_a_newer_window_replaces_the_value_even_with_missing_extremes():
    assert decide_count_vector_update(vector(3, 40, 120), vector(0, None, None, minutes=1)) == (
        "written"
    )
    assert decide_count_vector_update(vector(3, 40, 120, minutes=1), vector(9, 1, 999)) == "stale"


def at_least(new, old, direction):
    if old is None:
        return True
    if new is None:
        return False
    return new >= old if direction == "up" else new <= old


def oracle(stored_values, incoming_values):
    if stored_values == incoming_values:
        return "unchanged"
    pairs = list(zip(incoming_values, stored_values, DIRECTION_LIST, strict=True))
    if all(at_least(new, old, direction) for new, old, direction in pairs):
        return "written"
    if all(at_least(old, new, direction) for new, old, direction in pairs):
        return "stale"
    return "conflict"


VECTORS = list(product([0, 1, 2], [None, 1, 2], [None, 1, 2]))


def test_the_decision_equals_the_oracle_on_every_pair_of_vectors():
    assert len(VECTORS) ** 2 == 729
    for stored_values, incoming_values in product(VECTORS, VECTORS):
        expected = oracle(stored_values, incoming_values)
        stored = vector(*stored_values)
        incoming = vector(*incoming_values)
        if expected == "conflict":
            with pytest.raises(UpdateConflictError):
                decide_count_vector_update(stored, incoming)
        else:
            assert decide_count_vector_update(stored, incoming) == expected, (
                stored_values,
                incoming_values,
            )
