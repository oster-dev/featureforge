import random
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
import yaml

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.spec.reference import evaluate_spec, validate_event_fields
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import (
    EventIdentityConflictError,
    count_user_plays_1h,
)
from featureforge.streaming.reference.user_skip_rate import calculate_user_skip_rate_24h

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")
PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)
USER = "user_000001"
USERS = ["user_000001", "user_000002", "user_000003"]
GOLDEN_FIXTURE = (
    Path(__file__).resolve().parents[2] / "fixtures" / "streaming" / "user_play_count_1h_v1.jsonl"
)
GOLDEN_OBSERVATION = datetime(2026, 1, 1, 12, tzinfo=UTC)


def make_event(event_id: int = 1, event_type: str = "skip", **changes: object):
    payload = {
        "event_id": UUID(int=event_id),
        "event_time": OBSERVATION,
        "ingested_at": OBSERVATION,
        "user_id": USER,
        "content_id": "content_000001",
        "event_type": event_type,
        "play_seconds": 0,
        "source": "web",
        "schema_version": "v1",
    }
    payload.update(changes)
    return InteractionEventV1.model_validate(payload)


def skip_rate(events, observation=OBSERVATION, user=USER):
    result = evaluate_spec(SKIP, events, entity={"user_id": user}, observation_time=observation)
    return result.values["skip_rate"]


@pytest.mark.parametrize(
    ("types", "expected"),
    [
        ([], 0.0),
        (["play"], 0.0),
        (["skip"], 1.0),
        (["play", "skip"], 0.5),
        (["play", "play", "skip"], 1 / 3),
        (["skip", "skip", "play"], 2 / 3),
        (["impression", "complete"], 0.0),
        (["play", "skip", "impression", "complete"], 0.5),
    ],
)
def test_hand_calculated_rates(types, expected):
    events = [make_event(index, kind) for index, kind in enumerate(types, start=1)]
    assert skip_rate(events) == pytest.approx(expected)


def test_result_exposes_counts_window_and_observation_time():
    events = [make_event(1, "play"), make_event(2, "skip")]
    result = evaluate_spec(SKIP, events, entity={"user_id": USER}, observation_time=OBSERVATION)
    assert result.values == {"play_count": 1, "skip_count": 1, "skip_rate": 0.5}
    assert result.observation_time == OBSERVATION
    assert result.window_start_exclusive == OBSERVATION - timedelta(hours=24)


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(hours=-24, microseconds=-1), 0.0),
        (timedelta(hours=-24), 0.0),
        (timedelta(hours=-24, microseconds=1), 1.0),
        (timedelta(0), 1.0),
        (timedelta(microseconds=1), 0.0),
    ],
)
def test_window_boundaries_are_right_closed(offset, expected):
    stamp = OBSERVATION + offset
    assert skip_rate([make_event(event_time=stamp, ingested_at=stamp)]) == expected


def test_equivalent_observation_offsets_give_the_same_result():
    local = OBSERVATION.astimezone(timezone(timedelta(hours=2)))
    assert skip_rate([make_event()], observation=local) == 1.0


def test_receipt_after_observation_does_not_exclude_an_event():
    event = make_event(ingested_at=OBSERVATION + timedelta(hours=2))
    assert skip_rate([event]) == 1.0


def test_expiry_recomputes_numerator_and_denominator():
    events = [
        make_event(1, "skip", event_time=OBSERVATION - timedelta(hours=23)),
        make_event(2, "play"),
    ]
    assert skip_rate(events) == 0.5
    assert skip_rate(events, observation=OBSERVATION + timedelta(hours=1)) == 0.0


def test_duplicates_count_once_and_ingested_at_is_not_identity():
    skip = make_event(1, "skip")
    repeated = make_event(1, "skip", ingested_at=OBSERVATION + timedelta(minutes=5))
    assert skip_rate([skip, repeated, skip, make_event(2, "play")]) == 0.5


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("content_id", "content_000002"),
        ("event_type", "play"),
        ("play_seconds", 10),
        ("source", "tv"),
        ("event_time", OBSERVATION - timedelta(seconds=1)),
        ("user_id", "other_user"),
    ],
)
def test_conflicting_payload_is_rejected(field, value):
    with pytest.raises(EventIdentityConflictError, match="event_id="):
        skip_rate([make_event(), make_event(**{field: value})])


@pytest.mark.parametrize("reverse", [False, True])
def test_conflict_outside_the_query_still_fails(reverse):
    stamp = OBSERVATION - timedelta(days=2)
    events = [
        make_event(event_time=stamp, ingested_at=stamp, user_id="other", event_type="impression"),
        make_event(event_time=stamp, ingested_at=stamp, user_id="other", event_type="complete"),
    ]
    if reverse:
        events.reverse()
    with pytest.raises(EventIdentityConflictError, match="event_id="):
        skip_rate(events)


def test_other_users_do_not_contribute_and_input_is_not_mutated():
    events = [make_event(1, "play"), make_event(2, "skip", user_id="other_user")]
    before = [event.model_dump() for event in events]
    assert skip_rate(events) == 0.0
    assert [event.model_dump() for event in events] == before


def test_generator_input_and_order_independence():
    events = [make_event(1, "skip"), make_event(2, "play"), make_event(3, "play")]
    assert skip_rate(event for event in reversed(events)) == pytest.approx(1 / 3)


def test_naive_observation_time_is_rejected():
    with pytest.raises(ValueError, match="timezone-aware"):
        skip_rate([], observation=datetime(2026, 1, 2, 12))


def test_non_datetime_observation_time_is_rejected():
    with pytest.raises(TypeError, match="observation_time"):
        skip_rate([], observation="2026-01-02T12:00:00Z")


def test_invalid_item_is_rejected_even_after_valid_events():
    with pytest.raises(TypeError, match="InteractionEventV1"):
        skip_rate([make_event(), {}])


def test_entity_must_match_spec_keys():
    with pytest.raises(ValueError, match="entity must provide"):
        evaluate_spec(SKIP, [], entity={"content_id": "c"}, observation_time=OBSERVATION)


@pytest.mark.parametrize("value", ["", " ", "user 1", "user_1\n"])
def test_entity_value_must_be_non_empty_without_whitespace(value):
    with pytest.raises(ValueError, match="non-empty"):
        evaluate_spec(SKIP, [], entity={"user_id": value}, observation_time=OBSERVATION)


def test_entity_value_must_be_a_string():
    with pytest.raises(TypeError, match="must be a string"):
        evaluate_spec(SKIP, [], entity={"user_id": 123}, observation_time=OBSERVATION)


def _ops_spec() -> FeatureSpec:
    data = yaml.safe_load((SPECS / "user_skip_rate_24h.yaml").read_text())
    only_plays = "event_type == 'play'"
    data["aggregations"] = [
        {"name": "n", "op": "count", "filter": only_plays},
        {"name": "total", "op": "sum", "column": "play_seconds", "filter": only_plays},
        {"name": "mean", "op": "avg", "column": "play_seconds", "filter": only_plays},
        {"name": "lowest", "op": "min", "column": "play_seconds", "filter": only_plays},
        {"name": "highest", "op": "max", "column": "play_seconds", "filter": only_plays},
        {"name": "recent", "op": "last_k", "column": "play_seconds", "k": 2, "filter": only_plays},
        {"name": "titles", "op": "approx_distinct", "column": "content_id", "filter": only_plays},
    ]
    data["derived"] = {"double_mean": "mean * 2"}
    data["online"]["update_policy"] = "window_replace"
    return FeatureSpec.model_validate(data)


def test_non_count_operators_on_known_values():
    events = [
        make_event(1, "play", play_seconds=10, event_time=OBSERVATION - timedelta(hours=3)),
        make_event(
            2,
            "play",
            play_seconds=20,
            content_id="content_000002",
            event_time=OBSERVATION - timedelta(hours=2),
        ),
        make_event(3, "play", play_seconds=30, event_time=OBSERVATION - timedelta(hours=1)),
        make_event(4, "skip", play_seconds=99),
    ]
    result = evaluate_spec(
        _ops_spec(), events, entity={"user_id": USER}, observation_time=OBSERVATION
    )
    assert result.values == {
        "n": 3,
        "total": 60,
        "mean": 20.0,
        "lowest": 10,
        "highest": 30,
        "recent": [20, 30],
        "titles": 2,
        "double_mean": 40.0,
    }


def test_empty_input_gives_missing_values_not_zero():
    values = evaluate_spec(
        _ops_spec(), [], entity={"user_id": USER}, observation_time=OBSERVATION
    ).values
    assert values["n"] == 0
    assert values["total"] == 0
    assert values["recent"] == []
    assert values["titles"] == 0
    assert values["mean"] is None
    assert values["lowest"] is None
    assert values["highest"] is None
    assert values["double_mean"] is None


def test_composite_entity_key_partitions_by_both_fields():
    data = yaml.safe_load((SPECS / "user_play_count_1h.yaml").read_text())
    data["entity"] = ["user_id", "content_id"]
    spec = FeatureSpec.model_validate(data)
    events = [make_event(1, "play"), make_event(2, "play", content_id="content_000002")]
    entity = {"user_id": USER, "content_id": "content_000002"}
    result = evaluate_spec(spec, events, entity=entity, observation_time=OBSERVATION)
    assert result.values["play_count"] == 1


def test_shipped_specs_only_reference_known_event_fields():
    validate_event_fields(SKIP)
    validate_event_fields(PLAY)


def test_misspelled_event_field_is_rejected():
    data = yaml.safe_load((SPECS / "user_skip_rate_24h.yaml").read_text())
    data["aggregations"][0]["filter"] = "event_typ == 'play'"
    with pytest.raises(ValueError, match="unknown event fields"):
        validate_event_fields(FeatureSpec.model_validate(data))


def test_unknown_column_is_rejected():
    data = yaml.safe_load((SPECS / "user_skip_rate_24h.yaml").read_text())
    data["online"]["update_policy"] = "window_replace"
    data["derived"] = {}
    data["aggregations"] = [{"name": "total", "op": "sum", "column": "watch_seconds"}]
    with pytest.raises(ValueError, match="unknown event fields"):
        validate_event_fields(FeatureSpec.model_validate(data))


def _random_events(seed: int, count: int = 400):
    rng = random.Random(seed)
    types = ["play", "skip", "impression", "complete"]
    events = []
    for index in range(1, count + 1):
        stamp = OBSERVATION + timedelta(seconds=rng.randint(-30 * 3600, 600))
        events.append(
            make_event(
                index,
                rng.choice(types),
                event_time=stamp,
                ingested_at=stamp + timedelta(seconds=rng.randint(0, 7200)),
                user_id=rng.choice(USERS),
            )
        )
    for original in rng.sample(events, 40):
        events.append(
            original.model_copy(update={"ingested_at": original.ingested_at + timedelta(minutes=5)})
        )
    rng.shuffle(events)
    return events


def _observation_times(events, seed: int):
    rng = random.Random(seed + 100)
    times = [OBSERVATION, OBSERVATION + timedelta(hours=1)]
    for event in rng.sample(events, 8):
        times += [
            event.event_time,
            event.event_time + timedelta(hours=1),
            event.event_time + timedelta(hours=24),
        ]
    return times


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_skip_rate_matches_the_hand_written_reference(seed):
    events = _random_events(seed)
    for observation in _observation_times(events, seed):
        for user in USERS:
            expected = calculate_user_skip_rate_24h(
                events, user_id=user, observation_time=observation
            )
            assert skip_rate(events, observation, user) == expected


@pytest.mark.parametrize("seed", [1, 2, 3, 4, 5])
def test_play_count_matches_the_hand_written_reference(seed):
    events = _random_events(seed)
    for observation in _observation_times(events, seed):
        for user in USERS:
            expected = count_user_plays_1h(events, user_id=user, observation_time=observation)
            actual = evaluate_spec(
                PLAY, events, entity={"user_id": user}, observation_time=observation
            )
            assert actual.values["play_count"] == expected


def test_conflicts_raise_in_both_implementations():
    events = [make_event(1, "skip"), make_event(1, "play")]
    with pytest.raises(EventIdentityConflictError):
        calculate_user_skip_rate_24h(events, user_id=USER, observation_time=OBSERVATION)
    with pytest.raises(EventIdentityConflictError):
        skip_rate(events)


@pytest.fixture
def golden_events():
    lines = GOLDEN_FIXTURE.read_text(encoding="utf-8").splitlines()
    return [InteractionEventV1.model_validate_json(line) for line in lines if line.strip()]


@pytest.mark.parametrize(
    ("user_id", "expected"),
    [("user_000001", 3), ("user_000002", 1), ("user_unknown", 0)],
)
def test_golden_play_counts_match_the_fixture(golden_events, user_id, expected):
    result = evaluate_spec(
        PLAY,
        golden_events,
        entity={"user_id": user_id},
        observation_time=GOLDEN_OBSERVATION,
    )
    assert result.values["play_count"] == expected


@pytest.mark.parametrize("user_id", ["user_000001", "user_000002", "user_unknown"])
def test_skip_rate_on_the_golden_fixture_matches_the_hand_written_reference(golden_events, user_id):
    expected = calculate_user_skip_rate_24h(
        golden_events, user_id=user_id, observation_time=GOLDEN_OBSERVATION
    )
    assert skip_rate(golden_events, GOLDEN_OBSERVATION, user_id) == expected
