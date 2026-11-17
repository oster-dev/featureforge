"""Spark label evaluation against the exact reference (ADR-012)."""

from __future__ import annotations

import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import featureforge.spec.label_spark as label_spark_module
from featureforge.spec.label_model import LabelSpec
from featureforge.spec.label_reference import LabelObservation, evaluate_labels
from featureforge.spec.label_spark import backfill_labels, label_observations_to_frame
from featureforge.spec.spark_backfill import create_local_session, events_to_frame
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import EventIdentityConflictError

BASE = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
HORIZON = timedelta(days=7)
USER = "user_000001"
MICRO = timedelta(microseconds=1)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

Row = tuple[tuple[str, ...], int, int, bool | int]


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return create_local_session("featureforge-label-spark-test")


def make_spec(op: str, filter: str | None = None) -> LabelSpec:
    outcome: dict[str, str] = {"op": op}
    if filter is not None:
        outcome["filter"] = filter
    return LabelSpec.model_validate(
        {
            "name": "label_under_test",
            "version": 1,
            "owner": "test",
            "description": "label under test",
            "entity": "user_id",
            "source": "interactions",
            "outcome": outcome,
            "horizon": "7d",
            "dedupe": {"key": "event_id"},
        }
    )


def make_event(
    n: int, when: datetime, event_type: str = "play", user: str = USER
) -> InteractionEventV1:
    return InteractionEventV1(
        event_id=UUID(int=n),
        event_time=when,
        ingested_at=when + timedelta(minutes=1),
        user_id=user,
        content_id="content_000001",
        event_type=event_type,
        play_seconds=10,
        source="web",
        schema_version="v1",
    )


def micros(moment: datetime) -> int:
    return (moment - EPOCH) // MICRO


def observation(when: datetime, user: str = USER) -> LabelObservation:
    return LabelObservation({"user_id": user}, when)


def run_spark(
    spark: SparkSession,
    spec: LabelSpec,
    events: list[InteractionEventV1],
    observations: list[LabelObservation],
    data_end: datetime,
) -> tuple[list[Row], int]:
    result = backfill_labels(
        spec,
        events_to_frame(spark, events),
        label_observations_to_frame(
            spark, spec, [(o.entity, o.observation_time) for o in observations]
        ),
        data_end=data_end,
    )
    rows = result.values.select(
        *spec.keys,
        F.unix_micros("observation_time").alias("t"),
        F.unix_micros("observable_at").alias("o"),
        "value",
    ).collect()
    got = sorted((tuple(r[k] for k in spec.keys), r["t"], r["o"], r["value"]) for r in rows)
    return got, result.excluded_incomplete


def run_reference(
    spec: LabelSpec,
    events: list[InteractionEventV1],
    observations: list[LabelObservation],
    data_end: datetime,
) -> tuple[list[Row], int]:
    result = evaluate_labels(spec, events, observations=observations, data_end=data_end)
    want = sorted(
        (
            tuple(v.entity[k] for k in spec.keys),
            micros(v.observation_time),
            micros(v.observable_at),
            v.value,
        )
        for v in result.values
    )
    return want, result.excluded_incomplete


def random_world(
    seed: int,
) -> tuple[list[InteractionEventV1], list[LabelObservation], datetime]:
    rng = random.Random(seed)
    users = [f"user_{i:06d}" for i in range(1, 61)]
    events = [
        make_event(
            n,
            BASE + timedelta(seconds=rng.randrange(0, 20 * 86400)),
            rng.choice(["play", "complete"]),
            rng.choice(users),
        )
        for n in range(1, 301)
    ]
    redeliveries = [
        e.model_copy(update={"ingested_at": e.ingested_at + timedelta(hours=3)})
        for e in rng.sample(events, 30)
    ]
    all_events = events + redeliveries
    rng.shuffle(all_events)
    data_end = BASE + timedelta(days=15)

    observations = [
        observation(BASE + timedelta(seconds=rng.randrange(0, 20 * 86400)), rng.choice(users))
        for _ in range(150)
    ]
    for event in rng.sample(events, 20):
        observations.append(observation(event.event_time, event.user_id))
        observations.append(observation(event.event_time - HORIZON, event.user_id))
    for user in users:
        for shift in (-MICRO, timedelta(0), MICRO):
            observations.append(observation(data_end - HORIZON + shift, user))
    observations.extend(rng.sample(observations, 10))
    return all_events, observations, data_end


@pytest.mark.parametrize("seed", [1, 2])
@pytest.mark.parametrize(
    ("op", "filter"),
    [
        ("exists", None),
        ("exists", "event_type == 'complete'"),
        ("count", None),
        ("count", "event_type == 'complete'"),
    ],
)
def test_spark_matches_the_reference(
    spark: SparkSession, op: str, filter: str | None, seed: int
) -> None:
    spec = make_spec(op, filter)
    events, observations, data_end = random_world(seed)

    got, got_excluded = run_spark(spark, spec, events, observations, data_end)
    want, want_excluded = run_reference(spec, events, observations, data_end)

    assert got == want
    assert got_excluded == want_excluded > 0
    assert any(row[3] for row in want)
    assert not all(row[3] for row in want)


def test_window_is_open_at_t_and_closed_at_t_plus_horizon(spark: SparkSession) -> None:
    spec = make_spec("count")
    events = [
        make_event(1, BASE),
        make_event(2, BASE + MICRO),
        make_event(3, BASE + HORIZON),
        make_event(4, BASE + HORIZON + MICRO),
    ]
    data_end = BASE + HORIZON + timedelta(seconds=1)

    got, excluded = run_spark(spark, spec, events, [observation(BASE)], data_end)

    assert excluded == 0
    assert [row[3] for row in got] == [2]


def test_a_window_ending_exactly_at_data_end_is_complete(spark: SparkSession) -> None:
    spec = make_spec("exists")
    events = [make_event(1, BASE + HORIZON)]

    got, excluded = run_spark(spark, spec, events, [observation(BASE)], BASE + HORIZON)
    assert (len(got), excluded) == (1, 0)
    assert got[0][3] is True

    got, excluded = run_spark(spark, spec, events, [observation(BASE)], BASE + HORIZON - MICRO)
    assert (len(got), excluded) == (0, 1)


def test_redelivery_with_a_later_ingested_at_counts_once(spark: SparkSession) -> None:
    spec = make_spec("count")
    first = make_event(1, BASE + timedelta(hours=1))
    again = first.model_copy(update={"ingested_at": first.ingested_at + timedelta(days=30)})

    got, _ = run_spark(spark, spec, [first, again], [observation(BASE)], BASE + 2 * HORIZON)

    assert [row[3] for row in got] == [1]


def test_a_conflict_outside_the_window_and_for_another_entity_still_fails(
    spark: SparkSession,
) -> None:
    spec = make_spec("exists")
    inside = make_event(1, BASE + timedelta(hours=1))
    conflicting = make_event(1, BASE - timedelta(days=30), user="user_000002")

    with pytest.raises(EventIdentityConflictError):
        run_spark(spark, spec, [inside, conflicting], [observation(BASE)], BASE + 2 * HORIZON)


@pytest.mark.parametrize(("op", "expected"), [("exists", False), ("count", 0)])
def test_no_events_gives_a_value_never_null(spark: SparkSession, op: str, expected: object) -> None:
    got, excluded = run_spark(spark, make_spec(op), [], [observation(BASE)], BASE + 2 * HORIZON)

    assert excluded == 0
    assert len(got) == 1
    assert got[0][3] == expected
    assert got[0][3] is not None


def test_a_repeated_observation_gives_a_repeated_value(spark: SparkSession) -> None:
    spec = make_spec("count")
    events = [make_event(1, BASE + timedelta(hours=1))]
    observations = [observation(BASE), observation(BASE)]
    data_end = BASE + 2 * HORIZON

    got, _ = run_spark(spark, spec, events, observations, data_end)
    want, _ = run_reference(spec, events, observations, data_end)

    assert len(got) == 2
    assert got == want


def test_invalid_input_fails_before_anything_is_evaluated(spark: SparkSession) -> None:
    spec = make_spec("exists")
    events = events_to_frame(spark, [make_event(1, BASE)])
    good = label_observations_to_frame(spark, spec, [({"user_id": USER}, BASE)])
    bad = label_observations_to_frame(spark, spec, [({"user_id": "user 1"}, BASE)])

    with pytest.raises(ValueError, match="whitespace"):
        backfill_labels(spec, events, bad, data_end=BASE + 2 * HORIZON)
    with pytest.raises(ValueError, match="timezone-aware"):
        backfill_labels(spec, events, good, data_end=datetime(2026, 2, 1))
    with pytest.raises(ValueError, match="missing columns"):
        backfill_labels(spec, events.drop("source"), good, data_end=BASE + 2 * HORIZON)


def test_the_module_never_touches_history_or_publication_time() -> None:
    source = Path(label_spark_module.__file__).read_text().lower()

    for forbidden in ("published_at", "history", "pyiceberg"):
        assert forbidden not in source
