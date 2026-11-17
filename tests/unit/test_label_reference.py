from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest

from featureforge.config import SyntheticDataConfig
from featureforge.spec.label_loader import load_label_spec
from featureforge.spec.label_reference import LabelObservation, evaluate_labels
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import EventIdentityConflictError
from featureforge.streaming.simulator import SimulatorConfig, simulate_interactions

LABELS = Path(__file__).resolve().parents[2] / "feature_repo" / "labels"
ACTIVE = load_label_spec(LABELS / "active_within_7d.yaml")
COMPLETED = load_label_spec(LABELS / "completed_within_7d.yaml")
COUNT = ACTIVE.model_copy(update={"outcome": ACTIVE.outcome.model_copy(update={"op": "count"})})

T = datetime(2026, 1, 10, 12, tzinfo=UTC)
HORIZON = timedelta(days=7)
END = T + HORIZON
USER = "user_000001"
OTHER = "user_000002"


def make_event(
    event_id: int = 1,
    event_type: str = "play",
    at: datetime = T,
    user: str = USER,
    **changes: object,
) -> InteractionEventV1:
    payload = {
        "event_id": UUID(int=event_id),
        "event_time": at,
        "ingested_at": at,
        "user_id": user,
        "content_id": "content_000001",
        "event_type": event_type,
        "play_seconds": 0,
        "source": "web",
        "schema_version": "v1",
    }
    payload.update(changes)
    return InteractionEventV1.model_validate(payload)


def observe(user: str = USER, at: datetime = T) -> LabelObservation:
    return LabelObservation({"user_id": user}, at)


def single(spec, events, data_end: datetime = END):
    result = evaluate_labels(spec, events, observations=[observe()], data_end=data_end)
    assert result.excluded_incomplete == 0
    return result.values[0].value


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (-timedelta(seconds=1), False),
        (timedelta(0), False),
        (timedelta(seconds=1), True),
        (HORIZON, True),
        (HORIZON + timedelta(seconds=1), False),
    ],
)
def test_window_is_open_at_t_and_closed_at_t_plus_horizon(offset, expected):
    assert single(ACTIVE, [make_event(at=T + offset)]) is expected


def test_exists_is_false_for_a_complete_window_without_events():
    assert single(ACTIVE, []) is False


def test_count_counts_the_events_in_the_window():
    events = [
        make_event(1, at=T),
        make_event(2, at=T + timedelta(seconds=1)),
        make_event(3, at=T + timedelta(days=3)),
        make_event(4, at=END),
        make_event(5, at=END + timedelta(seconds=1)),
    ]
    assert single(COUNT, events) == 3


def test_filter_restricts_the_outcome_to_complete_events():
    inside = T + timedelta(hours=1)
    without_complete = [make_event(1, "play", at=inside), make_event(2, "skip", at=inside)]
    with_complete = [*without_complete, make_event(3, "complete", at=inside, play_seconds=60)]

    assert single(COMPLETED, without_complete) is False
    assert single(COMPLETED, with_complete) is True
    assert single(ACTIVE, without_complete) is True


def test_complete_at_t_does_not_count():
    assert single(COMPLETED, [make_event(1, "complete", at=T, play_seconds=60)]) is False


def test_events_of_other_entities_do_not_count():
    assert single(ACTIVE, [make_event(1, user=OTHER, at=T + timedelta(hours=1))]) is False


def test_an_incomplete_window_is_excluded_and_counted_never_false():
    result = evaluate_labels(
        ACTIVE, [], observations=[observe()], data_end=END - timedelta(seconds=1)
    )

    assert result.values == []
    assert result.excluded_incomplete == 1


def test_a_window_ending_exactly_at_data_end_is_complete():
    result = evaluate_labels(ACTIVE, [], observations=[observe()], data_end=END)

    assert result.excluded_incomplete == 0
    assert [v.value for v in result.values] == [False]
    assert result.values[0].observable_at == END


def test_complete_and_incomplete_observations_are_split_in_input_order():
    late = T + timedelta(days=2)
    result = evaluate_labels(
        COUNT,
        [make_event(1, at=T + timedelta(hours=1))],
        observations=[observe(at=late), observe(at=T), observe(at=late + timedelta(days=1))],
        data_end=END + timedelta(days=2),
    )

    assert [v.observation_time for v in result.values] == [late, T]
    assert [v.value for v in result.values] == [0, 1]
    assert result.excluded_incomplete == 1


def test_duplicate_with_other_ingested_at_counts_once():
    first = make_event(1, at=T + timedelta(hours=1))
    again = make_event(1, at=T + timedelta(hours=1), ingested_at=T + timedelta(hours=2))
    assert single(COUNT, [first, again]) == 1


def test_identity_conflict_raises():
    first = make_event(1, at=T + timedelta(hours=1))
    other = make_event(1, at=T + timedelta(hours=1), play_seconds=5)
    with pytest.raises(EventIdentityConflictError):
        single(ACTIVE, [first, other])


def test_identity_is_checked_before_any_filtering():
    old = T - timedelta(days=30)
    conflicting = [
        make_event(2, user=OTHER, at=old),
        make_event(2, user=OTHER, at=old, play_seconds=3),
    ]
    with pytest.raises(EventIdentityConflictError):
        single(ACTIVE, conflicting)


def test_observation_time_in_another_timezone_is_the_same_instant():
    plus_two = datetime(2026, 1, 10, 14, tzinfo=timezone(timedelta(hours=2)))
    result = evaluate_labels(
        ACTIVE,
        [make_event(1, at=T + timedelta(hours=1))],
        observations=[observe(at=plus_two)],
        data_end=END,
    )
    assert result.values[0].observation_time == T
    assert result.values[0].value is True


def test_naive_times_are_rejected():
    naive = datetime(2026, 1, 10, 12)
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate_labels(ACTIVE, [], observations=[observe(at=naive)], data_end=END)
    with pytest.raises(ValueError, match="timezone-aware"):
        evaluate_labels(ACTIVE, [], observations=[observe()], data_end=naive)


def test_entity_must_match_the_spec_keys():
    bad = LabelObservation({"content_id": "content_000001"}, T)
    with pytest.raises(ValueError, match="exactly"):
        evaluate_labels(ACTIVE, [], observations=[bad], data_end=END)
    typed = LabelObservation({"user_id": 5}, T)  # type: ignore[dict-item]
    with pytest.raises(TypeError, match="must be a string"):
        evaluate_labels(ACTIVE, [], observations=[typed], data_end=END)


def test_events_must_be_contract_events():
    with pytest.raises(TypeError, match="InteractionEventV1"):
        single(ACTIVE, [{"event_id": 1}])


def test_a_label_with_an_unknown_event_field_is_rejected_at_evaluation():
    broken = ACTIVE.model_copy(
        update={"outcome": ACTIVE.outcome.model_copy(update={"filter": "genre == 'drama'"})}
    )
    with pytest.raises(ValueError, match="unknown event fields"):
        single(broken, [])


def test_simulated_source_gives_consistent_labels():
    config = SyntheticDataConfig(
        seed=7,
        start_time=datetime(2026, 1, 1, tzinfo=UTC),
        end_time=datetime(2026, 3, 1, tzinfo=UTC),
        num_users=50,
        num_content_items=20,
        num_base_events=10,
        num_observations=10,
        duplicate_rate=0.0,
        late_event_rate=0.0,
        label_horizon_days=7,
        max_late_arrival_hours=0,
        event_generation_mode="behavioral",
    )
    events = simulate_interactions(config, SimulatorConfig(seed=3, num_impressions=2_000))
    users = sorted({event.user_id for event in events})
    span = int((config.end_time - config.start_time).total_seconds())
    rng = random.Random(5)
    observations = [
        LabelObservation(
            {"user_id": rng.choice(users)},
            config.start_time + timedelta(seconds=rng.randrange(span)),
        )
        for _ in range(300)
    ]

    active = evaluate_labels(ACTIVE, events, observations=observations, data_end=config.end_time)
    completed = evaluate_labels(
        COMPLETED, events, observations=observations, data_end=config.end_time
    )

    assert active.excluded_incomplete == completed.excluded_incomplete > 0
    assert len(active.values) + active.excluded_incomplete == len(observations)
    for a, c in zip(active.values, completed.values, strict=True):
        assert (a.entity, a.observation_time) == (c.entity, c.observation_time)
        if c.value:
            assert a.value
    assert any(c.value for c in completed.values)
    assert any(not c.value for c in completed.values)
