"""Command-line contract of build-training-dataset."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from featureforge.cli import build_parser, run_build_training_dataset_command
from featureforge.streaming.contracts.events import InteractionEventV1

ROOT = Path(__file__).resolve().parents[2]
SPEC = ROOT / "feature_repo" / "specs" / "user_skip_rate_24h.yaml"
LABEL = ROOT / "feature_repo" / "labels" / "completed_within_7d.yaml"
EVENTS = ROOT / "fixtures" / "streaming" / "user_skip_rate_24h_v1.jsonl"
TIMES = [
    datetime(2026, 1, 1, 12, 0, tzinfo=UTC),
    datetime(2026, 1, 1, 18, 0, tzinfo=UTC),
    datetime(2026, 1, 2, 18, 0, tzinfo=UTC),
]
DATA_END = datetime(2026, 1, 20, tzinfo=UTC).isoformat()


@pytest.fixture(scope="module")
def spark():
    session = (
        SparkSession.builder.master("local[2]")
        .appName("featureforge-cli-build-training-dataset-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    yield session
    session.stop()


def write_observations(path):
    lines = EVENTS.read_text().splitlines()
    events = [InteractionEventV1.model_validate_json(line) for line in lines if line.strip()]
    users = sorted({event.user_id for event in events})
    rows = [
        {"user_id": user, "observation_time": moment.isoformat()}
        for user in users
        for moment in TIMES
    ]
    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    return len(rows)


def run(tmp_path, **overrides):
    write_observations(tmp_path / "observations.jsonl")
    arguments = {
        "feature_spec_paths": [SPEC],
        "label_spec_path": LABEL,
        "events_path": EVENTS,
        "observations_path": tmp_path / "observations.jsonl",
        "mode": "event_time",
        "data_end": DATA_END,
        "output_path": tmp_path / "dataset",
    }
    arguments.update(overrides)
    return run_build_training_dataset_command(**arguments)


def test_parser_collects_repeated_feature_specs_and_the_mode() -> None:
    args = build_parser().parse_args(
        [
            "build-training-dataset",
            "--feature-spec", "a.yaml",
            "--feature-spec", "b.yaml",
            "--label-spec", "label.yaml",
            "--events", "events.jsonl",
            "--observations", "observations.jsonl",
            "--mode", "published_as_of",
            "--data-end", "2026-01-20T00:00:00+00:00",
            "--output", "out",
        ]
    )  # fmt: skip

    assert args.command == "build-training-dataset"
    assert args.feature_specs == [Path("a.yaml"), Path("b.yaml")]
    assert args.mode == "published_as_of"
    assert args.history_root is None


def test_parser_rejects_an_unknown_mode() -> None:
    with pytest.raises(SystemExit) as exc:
        build_parser().parse_args(
            [
                "build-training-dataset",
                "--feature-spec", "a.yaml",
                "--label-spec", "label.yaml",
                "--events", "events.jsonl",
                "--observations", "observations.jsonl",
                "--mode", "latest",
                "--data-end", "2026-01-20T00:00:00+00:00",
                "--output", "out",
            ]
        )  # fmt: skip

    assert exc.value.code == 2


def test_a_missing_events_file_exits_2(tmp_path) -> None:
    assert run(tmp_path, events_path=tmp_path / "missing.jsonl") == 2
    assert not (tmp_path / "dataset").exists()


def test_an_existing_output_path_exits_1(tmp_path, capsys) -> None:
    assert run(tmp_path, output_path=tmp_path) == 1
    assert "output already exists" in capsys.readouterr().out


def test_an_invalid_data_end_exits_1(tmp_path) -> None:
    assert run(tmp_path, data_end="tomorrow") == 1
    assert not (tmp_path / "dataset").exists()


def test_published_as_of_without_a_history_root_exits_1(tmp_path, capsys) -> None:
    assert run(tmp_path, mode="published_as_of") == 1
    assert "--history-root" in capsys.readouterr().out
    assert not (tmp_path / "dataset").exists()


def test_event_time_with_a_history_root_exits_1(tmp_path, capsys) -> None:
    assert run(tmp_path, history_root=tmp_path / "history") == 1
    assert "only used by mode published_as_of" in capsys.readouterr().out
    assert not (tmp_path / "history").exists()


def test_event_time_writes_a_dataset_with_a_manifest(spark, tmp_path, capsys) -> None:
    expected = write_observations(tmp_path / "observations.jsonl")

    assert run(tmp_path, spark=spark) == 0

    manifest = json.loads((tmp_path / "dataset" / "_manifest.json").read_text())
    assert manifest["mode"] == "event_time"
    assert manifest["observations"] == expected
    assert manifest["rows"] == expected
    assert sum(partition["rows"] for partition in manifest["partitions"]) == expected
    assert "history" not in manifest
    assert "✓" in capsys.readouterr().out


def test_published_as_of_with_a_missing_history_root_exits_2(tmp_path, capsys) -> None:
    assert run(tmp_path, mode="published_as_of", history_root=tmp_path / "history") == 2
    assert "history root not found" in capsys.readouterr().out
    assert not (tmp_path / "history").exists()
