"""The Spark backfill matches hand-calculated values and the spec reference for sum and avg."""

from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.spec.spark_backfill import backfill_spec, create_local_session, events_to_frame
from featureforge.streaming.contracts.events import InteractionEventV1

ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = ROOT / "fixtures" / "specs" / "user_play_seconds_1h.yaml"
FIXTURE_PATH = ROOT / "fixtures" / "streaming" / "user_play_seconds_1h_v1.jsonl"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
ONE_MICROSECOND = timedelta(microseconds=1)


def micros(moment: datetime) -> int:
    return (moment - EPOCH) // ONE_MICROSECOND


def at(hour: int, minute: int = 0) -> datetime:
    return datetime(2026, 1, 1, hour, minute, tzinfo=UTC)


CASES = [
    ("user000001", at(11), 3, 270, 90),
    ("user000002", at(11), 1, 200, 200),
    ("user000003", at(11), 0, 0, None),
    ("user000001", at(11, 30), 2, 590, 295),
    ("user000002", at(11, 30), 0, 0, None),
]


@pytest.fixture(scope="module")
def spark():
    session = create_local_session("featureforge-spark-sum-avg-tests")
    yield session
    session.stop()


@pytest.fixture(scope="module")
def spec():
    return load_spec(SPEC_PATH)


@pytest.fixture(scope="module")
def events() -> list[InteractionEventV1]:
    return [
        InteractionEventV1.model_validate_json(line)
        for line in FIXTURE_PATH.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


@pytest.fixture(scope="module")
def backfilled(spark, spec, events):
    event_frame = events_to_frame(spark, events)
    observations = (
        spark.createDataFrame(
            [(user, micros(observation)) for user, observation, *_ in CASES],
            ["user_id", "observation_us"],
        )
        .withColumn("observation_time", F.timestamp_micros("observation_us"))
        .drop("observation_us")
    )
    frame = backfill_spec(spec, event_frame, observations).withColumn(
        "observation_us", F.unix_micros("observation_time")
    )
    return {(row["user_id"], row["observation_us"]): row for row in frame.collect()}


@pytest.mark.parametrize(("user_id", "observation", "plays", "seconds", "average"), CASES)
def test_spark_matches_hand_calculated_values(
    backfilled, user_id, observation, plays, seconds, average
):
    row = backfilled[(user_id, micros(observation))]
    assert row["play_count"] == plays
    assert row["play_seconds_sum"] == seconds
    assert row["avg_play_seconds"] == average


@pytest.mark.parametrize(("user_id", "observation", "plays", "seconds", "average"), CASES)
def test_spark_matches_the_spec_reference(
    spec, events, backfilled, user_id, observation, plays, seconds, average
):
    reference = evaluate_spec(
        spec, events, entity={"user_id": user_id}, observation_time=observation
    ).values
    row = backfilled[(user_id, micros(observation))]
    for name in ("play_count", "play_seconds_sum", "avg_play_seconds"):
        assert row[name] == reference[name]
