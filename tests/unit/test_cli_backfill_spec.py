"""Command-line contract of backfill-spec."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pytest
from pyspark.sql import SparkSession

from featureforge.cli import build_parser, run_backfill_spec_command
from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.spec.spark_backfill import create_local_session
from featureforge.streaming.contracts.events import InteractionEventV1

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "feature_repo" / "specs" / "user_skip_rate_24h.yaml"
EVENTS = ROOT / "fixtures" / "streaming" / "user_skip_rate_24h_v1.jsonl"
TIMES = [
    datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    datetime(2026, 1, 1, 18, 0, tzinfo=UTC),
    datetime(2026, 1, 2, 18, 0, tzinfo=UTC),
]
SUM_SPEC = """name: user_play_seconds_1h
version: 1
owner: test
description: Total play seconds in the last hour
entity: user_id
source: interaction-events.validated.v1
aggregations:
  - {name: total_seconds, op: sum, column: play_seconds}
window: {size: 1h, slide: 1m, boundary: right_closed}
lateness: {watermark_delay: 10m, allowed: 30m}
dedupe: {key: event_id, ttl: 24h}
online: {store: redis, ttl: 2h, update_policy: window_replace}
offline: {store: iceberg, table: features.user_play_seconds_1h}
freshness_slo: 5m
"""


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("featureforge-cli-backfill-spec-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def read_events():
    lines = EVENTS.read_text().splitlines()
    return [InteractionEventV1.model_validate_json(line) for line in lines if line.strip()]


def write_rows(path, rows):
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))


def valid_observations(path):
    users = sorted({event.user_id for event in read_events()}) + ["user_absent"]
    rows = [
        {"user_id": user, "observation_time": moment.isoformat()}
        for user in users
        for moment in TIMES
    ]
    write_rows(path, rows)
    return len(rows)


def test_backfill_spec_writes_values_equal_to_the_reference(spark, tmp_path):
    observations = tmp_path / "observations.jsonl"
    expected_rows = valid_observations(observations)
    output = tmp_path / "out"

    status = run_backfill_spec_command(SPEC, EVENTS, observations, output, spark=spark)

    assert status == 0
    frame = pd.read_parquet(output)
    assert str(frame["observation_time"].dt.tz) == "UTC"
    assert str(frame["window_start_exclusive"].dt.tz) == "UTC"
    frame["observation_time"] = pd.to_datetime(frame["observation_time"], utc=True)
    assert len(frame) == expected_rows
    spec = load_spec(SPEC)
    events = read_events()
    for row in frame.to_dict("records"):
        expected = evaluate_spec(
            spec,
            events,
            entity={"user_id": row["user_id"]},
            observation_time=row["observation_time"].to_pydatetime(),
        )
        for name, value in expected.values.items():
            assert row[name] == value, (row["user_id"], row["observation_time"], name)


def test_missing_files_return_status_2(spark, tmp_path):
    observations = tmp_path / "observations.jsonl"
    valid_observations(observations)
    missing = tmp_path / "missing.jsonl"

    assert run_backfill_spec_command(SPEC, missing, observations, tmp_path / "a", spark) == 2
    assert run_backfill_spec_command(SPEC, EVENTS, missing, tmp_path / "b", spark) == 2
    assert not (tmp_path / "a").exists()
    assert not (tmp_path / "b").exists()


@pytest.mark.parametrize(
    "row",
    [
        {"user_id": "user000001"},
        {"user_id": "user000001", "observation_time": "2026-01-01T12:00:00"},
        {"user_id": "user 1", "observation_time": "2026-01-01T12:00:00+00:00"},
        {"user_id": 5, "observation_time": "2026-01-01T12:00:00+00:00"},
        {"user_id": "user000001", "observation_time": "2026-01-01T12:00:00+00:00", "x": 1},
    ],
    ids=["missing-time", "naive-time", "whitespace-entity", "non-string-entity", "extra-key"],
)
def test_invalid_observations_fail_before_anything_is_written(spark, tmp_path, row):
    observations = tmp_path / "observations.jsonl"
    write_rows(observations, [row])
    output = tmp_path / "out"

    assert run_backfill_spec_command(SPEC, EVENTS, observations, output, spark) == 1
    assert not output.exists()


def test_conflicting_event_identity_fails_without_output(spark, tmp_path):
    lines = [line for line in EVENTS.read_text().splitlines() if line.strip()]
    conflicting = {**json.loads(lines[0]), "event_type": "complete"}
    events = tmp_path / "events.jsonl"
    events.write_text("\n".join([*lines, json.dumps(conflicting)]) + "\n")
    observations = tmp_path / "observations.jsonl"
    valid_observations(observations)
    output = tmp_path / "out"

    assert run_backfill_spec_command(SPEC, events, observations, output, spark) == 1
    assert not output.exists()


def test_existing_output_is_never_overwritten(spark, tmp_path):
    observations = tmp_path / "observations.jsonl"
    valid_observations(observations)
    output = tmp_path / "out"
    output.mkdir()
    (output / "keep.txt").write_text("keep")

    assert run_backfill_spec_command(SPEC, EVENTS, observations, output, spark) == 1
    assert (output / "keep.txt").read_text() == "keep"


def test_unsupported_operator_is_reported(spark, tmp_path, capsys):
    spec = tmp_path / "sum_spec.yaml"
    spec.write_text(SUM_SPEC)
    observations = tmp_path / "observations.jsonl"
    valid_observations(observations)
    output = tmp_path / "out"

    assert run_backfill_spec_command(spec, EVENTS, observations, output, spark) == 1
    assert "sum" in capsys.readouterr().out
    assert not output.exists()


def test_parser_accepts_backfill_spec():
    args = build_parser().parse_args(
        [
            "backfill-spec",
            "spec.yaml",
            "--events",
            "events.jsonl",
            "--observations",
            "observations.jsonl",
            "--output",
            "out",
        ]
    )

    assert args.command == "backfill-spec"
    assert args.spec == Path("spec.yaml")
    assert args.output == Path("out")


def test_local_session_is_pinned_to_utc(spark):
    session = create_local_session()

    assert session.conf.get("spark.sql.session.timeZone") == "UTC"
