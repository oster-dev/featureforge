from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from featureforge.config import SyntheticDataConfig
from featureforge.spec.label_loader import (
    contract_event_types,
    event_types_in_filter,
    load_label_spec,
)
from featureforge.spec.label_model import LabelSpec
from featureforge.spec.model import FeatureSpec
from featureforge.streaming.simulator import SimulatorConfig, simulate_interactions

LABEL_DIR = Path(__file__).resolve().parents[2] / "feature_repo" / "labels"
EXAMPLES = sorted(LABEL_DIR.glob("*.yaml"))

BASE = {
    "name": "completed_within_7d",
    "version": 1,
    "owner": "personalization-team",
    "description": "Whether the user completed a play in the 7 days after t",
    "entity": "user_id",
    "source": "interaction-events.validated.v1",
    "outcome": {"op": "exists", "filter": "event_type == 'complete'"},
    "horizon": "7d",
    "dedupe": {"key": "event_id"},
}


def write_label(tmp_path: Path, **overrides: object) -> Path:
    path = tmp_path / "label.yaml"
    path.write_text(yaml.safe_dump({**BASE, **overrides}), encoding="utf-8")
    return path


def test_both_adr_examples_exist_and_load():
    assert {path.stem for path in EXAMPLES} == {"completed_within_7d", "active_within_7d"}
    for path in EXAMPLES:
        spec = load_label_spec(path)
        assert spec.name == path.stem
        assert spec.horizon == timedelta(days=7)


def test_outcome_without_filter_counts_every_event():
    spec = load_label_spec(LABEL_DIR / "active_within_7d.yaml")
    assert spec.outcome.op == "exists"
    assert spec.outcome.filter is None


def test_unknown_field_is_rejected(tmp_path):
    path = write_label(tmp_path, window="7d")
    with pytest.raises(ValidationError, match="window"):
        load_label_spec(path)


def test_filter_outside_the_expression_subset_is_rejected(tmp_path):
    path = write_label(tmp_path, outcome={"op": "exists", "filter": "len(event_type) > 3"})
    with pytest.raises(ValidationError, match="Call is not allowed"):
        load_label_spec(path)


def test_empty_filter_is_rejected(tmp_path):
    path = write_label(tmp_path, outcome={"op": "exists", "filter": " "})
    with pytest.raises(ValidationError, match="must not be empty"):
        load_label_spec(path)


def test_zero_horizon_is_rejected(tmp_path):
    with pytest.raises(ValidationError, match="horizon must be positive"):
        load_label_spec(write_label(tmp_path, horizon="0d"))


def test_invalid_duration_is_rejected(tmp_path):
    with pytest.raises(ValidationError, match="invalid duration"):
        load_label_spec(write_label(tmp_path, horizon="seven days"))


def test_unknown_event_field_is_rejected(tmp_path):
    path = write_label(tmp_path, outcome={"op": "exists", "filter": "genre == 'drama'"})
    with pytest.raises(ValueError, match="unknown event fields"):
        load_label_spec(path)


def test_event_type_outside_the_contract_is_rejected(tmp_path):
    path = write_label(tmp_path, outcome={"op": "exists", "filter": "event_type == 'browse'"})
    with pytest.raises(ValueError, match="outside the contract"):
        load_label_spec(path)


def test_event_type_check_reads_lists_and_reversed_comparisons():
    assert event_types_in_filter("event_type in ('skip', 'complete')") == {"skip", "complete"}
    assert event_types_in_filter("'play' == event_type") == {"play"}
    assert event_types_in_filter("play_seconds > 0") == set()


def test_feature_spec_model_does_not_accept_a_label():
    with pytest.raises(ValidationError):
        FeatureSpec.model_validate(BASE)


def test_label_spec_model_does_not_accept_a_feature_spec():
    feature = yaml.safe_load(
        (
            Path(__file__).resolve().parents[2] / "feature_repo/specs/content_play_count_1h.yaml"
        ).read_text(encoding="utf-8")
    )
    with pytest.raises(ValidationError):
        LabelSpec.model_validate(feature)


def test_example_labels_only_use_event_types_the_simulator_produces():
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
    produced = {
        event.event_type
        for event in simulate_interactions(config, SimulatorConfig(seed=3, num_impressions=2_000))
    }
    assert produced == contract_event_types()
    for path in EXAMPLES:
        spec = load_label_spec(path)
        if spec.outcome.filter:
            assert event_types_in_filter(spec.outcome.filter) <= produced
