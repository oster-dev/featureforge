"""build-training-dataset over a real history: the pinned snapshot and the missing table."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pandas as pd
import pytest

from featureforge.cli import run_build_training_dataset_command
from featureforge.spec.loader import load_spec
from featureforge.spec.training_loader import load_training_dataset
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.contracts.feature_record import FeatureRecord
from featureforge.streaming.history import (
    Evidence,
    HistoryTable,
    open_local_catalog,
    table_name,
)
from featureforge.streaming.published_spark import pinned_snapshot_id

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = ROOT / "feature_repo" / "specs" / "user_skip_rate_24h.yaml"
PLAY_SPEC_PATH = ROOT / "feature_repo" / "specs" / "user_play_count_1h.yaml"
LABEL_PATH = ROOT / "feature_repo" / "labels" / "completed_within_7d.yaml"
SPEC = load_spec(SPEC_PATH)
BASE = datetime(2026, 1, 10, 12, 0, tzinfo=UTC)
HORIZON = timedelta(days=7)
USER = "user_000001"
PUBLISHED = BASE - timedelta(minutes=30)


def make_event(n, when, event_type):
    return InteractionEventV1(
        event_id=UUID(int=n),
        event_time=when,
        ingested_at=when + timedelta(minutes=1),
        user_id=USER,
        content_id="content_000001",
        event_type=event_type,
        play_seconds=10,
        source="web",
        schema_version="v1",
    )


def write_inputs(tmp_path, moments):
    events = [
        make_event(1, BASE - timedelta(hours=1), "play"),
        make_event(2, BASE - timedelta(minutes=40), "skip"),
        make_event(3, BASE - timedelta(minutes=20), "play"),
    ]
    events_path = tmp_path / "events.jsonl"
    events_path.write_text("".join(event.model_dump_json() + "\n" for event in events))
    observations_path = tmp_path / "observations.jsonl"
    observations_path.write_text(
        "".join(
            json.dumps({"user_id": USER, "observation_time": moment.isoformat()}) + "\n"
            for moment in moments
        )
    )
    return events_path, observations_path


def serve_one_version(history):
    window_end = PUBLISHED - timedelta(seconds=1)
    served = FeatureRecord(
        feature_name=SPEC.name,
        entity_key="user_id",
        entity_value=USER,
        observation_time=window_end,
        window_start_exclusive=window_end - timedelta(hours=24),
        counts={"play_count": 1, "skip_count": 1},
        derived={"skip_rate": 0.5},
    )
    history.append([Evidence(served, PUBLISHED)])


def test_published_as_of_pins_the_history_and_records_the_snapshot(tmp_path, capsys):
    t0, t1, t2 = BASE - timedelta(days=10), BASE, BASE + SPEC.online.ttl + timedelta(hours=1)
    history_root = tmp_path / "history"
    history = HistoryTable(open_local_catalog(history_root), SPEC)
    serve_one_version(history)
    snapshot = pinned_snapshot_id(history)
    events_path, observations_path = write_inputs(tmp_path, (t0, t1, t2))

    status = run_build_training_dataset_command(
        [SPEC_PATH],
        LABEL_PATH,
        events_path,
        observations_path,
        "published_as_of",
        (t2 + HORIZON + timedelta(seconds=1)).isoformat(),
        tmp_path / "dataset",
        history_root,
    )

    assert status == 0, capsys.readouterr().out
    manifest = json.loads((tmp_path / "dataset" / "_manifest.json").read_text())
    assert manifest["mode"] == "published_as_of"
    assert manifest["observations"] == 3
    assert manifest["excluded_before_history"] == 1
    assert manifest["rows"] == 2
    assert [entry["snapshot_id"] for entry in manifest["history"]] == [snapshot]

    loaded = load_training_dataset(tmp_path / "dataset")
    assert len(loaded.frame) == 2
    assert list(loaded.frame.columns) == manifest["columns"]
    assert loaded.frame["observation_time"].tolist() == [pd.Timestamp(t1), pd.Timestamp(t2)]
    assert f"{SPEC.name}__status" in loaded.frame.columns
    assert "label__completed_within_7d" in loaded.frame.columns


def test_a_feature_without_a_history_table_fails_and_creates_nothing(tmp_path, capsys):
    history_root = tmp_path / "history"
    catalog = open_local_catalog(history_root)
    HistoryTable(catalog, load_spec(PLAY_SPEC_PATH))
    events_path, observations_path = write_inputs(tmp_path, (BASE,))

    status = run_build_training_dataset_command(
        [SPEC_PATH],
        LABEL_PATH,
        events_path,
        observations_path,
        "published_as_of",
        (BASE + HORIZON + timedelta(days=1)).isoformat(),
        tmp_path / "dataset",
        history_root,
    )

    assert status == 1
    assert f"no history table for {SPEC.name}" in capsys.readouterr().out
    assert not catalog.table_exists(table_name(SPEC))
    assert not (tmp_path / "dataset").exists()


def test_the_loader_repeats_the_constants_of_the_writer():
    from featureforge.spec import training_dataset, training_loader

    assert training_loader.MANIFEST_NAME == training_dataset.MANIFEST_NAME
    assert training_loader.SUPPORTED_MANIFEST_VERSION == training_dataset.MANIFEST_VERSION
    assert training_loader.PARTITION_COLUMN == training_dataset.PARTITION_COLUMN
