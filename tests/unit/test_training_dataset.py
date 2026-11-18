"""Event-time training dataset against the references (ADR-014)."""

from __future__ import annotations

import json
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import featureforge.spec.training_dataset as training_dataset_module
from featureforge.spec.label_model import LabelSpec
from featureforge.spec.label_reference import LabelObservation, evaluate_labels
from featureforge.spec.label_spark import label_observations_to_frame
from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.spec.spark_backfill import create_local_session, events_to_frame
from featureforge.spec.training_dataset import (
    build_training_dataset,
    feature_columns,
)
from featureforge.streaming.contracts.events import InteractionEventV1

ROOT = Path(__file__).resolve().parents[2]
SPECS = ROOT / "feature_repo" / "specs"
BASE = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
HORIZON = timedelta(days=7)
MICRO = timedelta(microseconds=1)
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
USER = "user_000001"
LABEL_COLUMN = "label__label_under_test"

PLAY_COUNT = load_spec(SPECS / "user_play_count_1h.yaml")
SKIP_RATE = load_spec(SPECS / "user_skip_rate_24h.yaml")
CONTENT_PLAY_COUNT = load_spec(SPECS / "content_play_count_1h.yaml")


@pytest.fixture(scope="module")
def spark() -> SparkSession:
    return create_local_session("featureforge-training-dataset-test")


def make_label(op: str, filter: str | None = None) -> LabelSpec:
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


def build(
    spark: SparkSession,
    features: list[Any],
    label: LabelSpec,
    events: list[InteractionEventV1],
    when: list[datetime],
    data_end: datetime,
    output: Path,
) -> dict[str, Any]:
    pairs = [({"user_id": USER}, moment) for moment in when]
    return build_training_dataset(
        spark,
        features,
        label,
        events_to_frame(spark, events),
        label_observations_to_frame(spark, label, pairs),
        mode="event_time",
        data_end=data_end,
        output=output,
    )


def read_rows(spark: SparkSession, output: Path) -> list[dict[str, Any]]:
    frame = spark.read.parquet(str(output)).withColumn("t", F.unix_micros("observation_time"))
    return [row.asDict() for row in frame.collect()]


def test_the_event_at_t_is_a_feature_and_never_the_label(
    spark: SparkSession, tmp_path: Path
) -> None:
    events = [
        make_event(1, BASE - timedelta(hours=1)),
        make_event(2, BASE - timedelta(minutes=30)),
        make_event(3, BASE),
        make_event(4, BASE + MICRO),
        make_event(5, BASE + HORIZON),
        make_event(6, BASE + HORIZON + MICRO),
    ]
    label = make_label("count", "event_type == 'play'")
    output = tmp_path / "dataset"

    build(spark, [PLAY_COUNT], label, events, [BASE], BASE + HORIZON + timedelta(seconds=1), output)
    rows = read_rows(spark, output)

    assert len(rows) == 1
    assert rows[0]["user_play_count_1h__play_count"] == 2
    assert rows[0][LABEL_COLUMN] == 2


def test_columns_are_qualified_by_their_spec(spark: SparkSession, tmp_path: Path) -> None:
    label = make_label("exists")
    output = tmp_path / "dataset"

    manifest = build(
        spark, [PLAY_COUNT], label, [make_event(1, BASE)], [BASE], BASE + 2 * HORIZON, output
    )

    expected = [
        "user_id",
        "observation_time",
        "user_play_count_1h__play_count",
        LABEL_COLUMN,
        f"{LABEL_COLUMN}__observable_at",
    ]
    assert manifest["columns"] == expected
    assert spark.read.parquet(str(output)).columns == [*expected, "observation_date"]


def test_an_incomplete_label_window_excludes_the_row_and_is_counted(
    spark: SparkSession, tmp_path: Path
) -> None:
    label = make_label("exists")
    output = tmp_path / "dataset"
    data_end = BASE + timedelta(days=10)

    manifest = build(
        spark,
        [PLAY_COUNT],
        label,
        [make_event(1, BASE)],
        [BASE, BASE + timedelta(days=8)],
        data_end,
        output,
    )

    assert (manifest["observations"], manifest["rows"], manifest["excluded_incomplete"]) == (
        2,
        1,
        1,
    )
    assert len(read_rows(spark, output)) == 1


def test_no_complete_label_fails_before_anything_is_written(
    spark: SparkSession, tmp_path: Path
) -> None:
    output = tmp_path / "dataset"

    with pytest.raises(ValueError, match="no observation"):
        build(
            spark,
            [PLAY_COUNT],
            make_label("exists"),
            [make_event(1, BASE)],
            [BASE],
            BASE + timedelta(days=1),
            output,
        )

    assert not output.exists()


def test_the_manifest_lists_every_partition_and_the_rows_add_up(
    spark: SparkSession, tmp_path: Path
) -> None:
    label = make_label("exists")
    output = tmp_path / "dataset"
    when = [BASE, BASE + timedelta(hours=1), BASE + timedelta(days=1), BASE + timedelta(days=2)]

    manifest = build(
        spark, [PLAY_COUNT], label, [make_event(1, BASE)], when, BASE + timedelta(days=20), output
    )

    assert manifest["partitions"] == [
        {"observation_date": "2026-01-10", "rows": 2},
        {"observation_date": "2026-01-11", "rows": 1},
        {"observation_date": "2026-01-12", "rows": 1},
    ]
    assert manifest["rows"] == 4 == len(read_rows(spark, output))
    assert manifest["mode"] == "event_time"
    assert manifest["features"] == [{"name": PLAY_COUNT.name, "version": PLAY_COUNT.version}]
    assert manifest["data_end"] == (BASE + timedelta(days=20)).isoformat()
    assert json.loads((output / "_manifest.json").read_text()) == manifest


def test_existing_output_is_never_overwritten(spark: SparkSession, tmp_path: Path) -> None:
    output = tmp_path / "dataset"
    output.mkdir()
    (output / "marker").write_text("keep")

    with pytest.raises(FileExistsError):
        build(
            spark,
            [PLAY_COUNT],
            make_label("exists"),
            [make_event(1, BASE)],
            [BASE],
            BASE + 2 * HORIZON,
            output,
        )

    assert [path.name for path in output.iterdir()] == ["marker"]


def test_features_and_label_must_name_the_same_keys(spark: SparkSession, tmp_path: Path) -> None:
    output = tmp_path / "dataset"

    with pytest.raises(ValueError, match="keys"):
        build(
            spark,
            [CONTENT_PLAY_COUNT],
            make_label("exists"),
            [make_event(1, BASE)],
            [BASE],
            BASE + 2 * HORIZON,
            output,
        )

    assert not output.exists()


def test_a_feature_spec_may_appear_only_once(spark: SparkSession, tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="unique"):
        build(
            spark,
            [PLAY_COUNT, PLAY_COUNT],
            make_label("exists"),
            [make_event(1, BASE)],
            [BASE],
            BASE + 2 * HORIZON,
            tmp_path / "dataset",
        )


def test_the_spark_session_must_run_in_utc(spark: SparkSession, tmp_path: Path) -> None:
    other = spark.newSession()
    other.conf.set("spark.sql.session.timeZone", "Europe/Berlin")
    label = make_label("exists")

    with pytest.raises(ValueError, match="UTC"):
        build_training_dataset(
            other,
            [PLAY_COUNT],
            label,
            events_to_frame(spark, [make_event(1, BASE)]),
            label_observations_to_frame(spark, label, [({"user_id": USER}, BASE)]),
            mode="event_time",
            data_end=BASE + 2 * HORIZON,
            output=tmp_path / "dataset",
        )


def test_the_mode_is_required_and_checked(spark: SparkSession, tmp_path: Path) -> None:
    label = make_label("exists")
    events = events_to_frame(spark, [make_event(1, BASE)])
    observations = label_observations_to_frame(spark, label, [({"user_id": USER}, BASE)])
    call: Any = build_training_dataset
    common = {"data_end": BASE + 2 * HORIZON, "output": tmp_path / "dataset"}

    with pytest.raises(TypeError):
        call(spark, [PLAY_COUNT], label, events, observations, **common)
    with pytest.raises(ValueError, match="mode"):
        call(spark, [PLAY_COUNT], label, events, observations, mode="latest", **common)
    with pytest.raises(NotImplementedError):
        call(spark, [PLAY_COUNT], label, events, observations, mode="published_as_of", **common)


def test_rows_match_the_references(spark: SparkSession, tmp_path: Path) -> None:
    rng = random.Random(3)
    users = [f"user_{i:06d}" for i in range(1, 16)]
    events = [
        make_event(
            n,
            BASE + timedelta(seconds=rng.randrange(0, 12 * 86400)),
            rng.choice(["play", "skip", "complete"]),
            rng.choice(users),
        )
        for n in range(1, 201)
    ]
    chosen: dict[tuple[str, datetime], None] = {}
    while len(chosen) < 60:
        chosen[(rng.choice(users), BASE + timedelta(seconds=rng.randrange(0, 12 * 86400)))] = None
    pairs = list(chosen)
    data_end = BASE + timedelta(days=14)
    label = make_label("exists", "event_type == 'complete'")
    features = [PLAY_COUNT, SKIP_RATE]
    output = tmp_path / "dataset"

    build_training_dataset(
        spark,
        features,
        label,
        events_to_frame(spark, events),
        label_observations_to_frame(spark, label, [({"user_id": u}, w) for u, w in pairs]),
        mode="event_time",
        data_end=data_end,
        output=output,
    )
    rows = {(row["user_id"], row["t"]): row for row in read_rows(spark, output)}

    reference = evaluate_labels(
        label,
        events,
        observations=[LabelObservation({"user_id": u}, w) for u, w in pairs],
        data_end=data_end,
    )
    labels = {(v.entity["user_id"], micros(v.observation_time)): v.value for v in reference.values}
    assert reference.excluded_incomplete > 0
    assert set(rows) == set(labels)
    assert any(labels.values())
    assert not all(labels.values())

    for user, when in pairs:
        key = (user, micros(when))
        if key not in labels:
            continue
        assert rows[key][LABEL_COLUMN] == labels[key]
        for feature in features:
            expected = evaluate_spec(
                feature, events, entity={"user_id": user}, observation_time=when
            ).values
            for column in feature_columns(feature):
                actual = rows[key][f"{feature.name}__{column}"]
                if expected[column] is None:
                    assert actual is None
                else:
                    assert actual == pytest.approx(expected[column])


def test_the_module_never_touches_publication_evidence() -> None:
    source = Path(training_dataset_module.__file__).read_text()

    for forbidden in ("pyiceberg", "HistoryTable", "published_at", "streaming.history"):
        assert forbidden not in source
