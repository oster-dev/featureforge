from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"


def _skip_rate() -> dict:
    return yaml.safe_load((SPECS / "user_skip_rate_24h.yaml").read_text())


@pytest.mark.parametrize("path", sorted(SPECS.glob("*.yaml")), ids=lambda p: p.stem)
def test_shipped_specs_are_valid(path):
    assert load_spec(path).name == path.stem


def test_keys_normalizes_single_and_composite_entity():
    data = _skip_rate()
    assert FeatureSpec.model_validate(data).keys == ["user_id"]
    data["entity"] = ["user_id", "content_id"]
    assert FeatureSpec.model_validate(data).keys == ["user_id", "content_id"]


def _mutate(**changes):
    data = _skip_rate()
    data.update(changes)
    return data


@pytest.mark.parametrize(
    ("data", "fragment"),
    [
        (_mutate(window={"size": "24h", "slide": "7m", "boundary": "right_closed"}), "divide"),
        (_mutate(window={"size": "24h", "slide": "1m", "boundary": "left_closed"}), "right_closed"),
        (_mutate(dedupe={"key": "event_id", "ttl": "24h"}), "dedupe.ttl"),
        (
            _mutate(
                online={"store": "redis", "ttl": "1h", "update_policy": "componentwise_monotonic"}
            ),
            "online.ttl",
        ),
        (_mutate(derived={"skip_rate": "skip_count / total"}), "unknown names"),
        (_mutate(derived={"x": "__import__('os').system('id')"}), "not allowed"),
        (_mutate(derived={"play_count": "1"}), "clash"),
        (
            _mutate(window={"size": "24 hours", "slide": "1m", "boundary": "right_closed"}),
            "invalid duration",
        ),
        (_mutate(unexpected=True), "unexpected"),
    ],
)
def test_invalid_specs_are_rejected_with_actionable_message(data, fragment):
    with pytest.raises(ValidationError) as info:
        FeatureSpec.model_validate(data)
    assert fragment in str(info.value)


def test_non_count_aggregation_cannot_use_monotone_policy():
    data = _skip_rate()
    data["aggregations"] = [{"name": "avg_x", "op": "avg", "column": "x"}]
    data["derived"] = {}
    with pytest.raises(ValidationError, match="count-only"):
        FeatureSpec.model_validate(data)


def test_append_only_requires_exactly_one_aggregation():
    data = _skip_rate()
    data["online"]["update_policy"] = "append_only"
    with pytest.raises(ValidationError, match="exactly one"):
        FeatureSpec.model_validate(data)


def test_window_replace_allows_non_count_operators():
    data = _skip_rate()
    data["aggregations"] = [{"name": "mean", "op": "avg", "column": "play_seconds"}]
    data["derived"] = {}
    data["online"]["update_policy"] = "window_replace"
    assert FeatureSpec.model_validate(data).aggregations[0].op.value == "avg"


def test_last_k_requires_k_and_k_requires_last_k():
    data = _skip_rate()
    data["online"]["update_policy"] = "window_replace"
    data["derived"] = {}
    data["aggregations"] = [{"name": "recent", "op": "last_k", "column": "play_seconds"}]
    with pytest.raises(ValidationError, match="needs 'k'"):
        FeatureSpec.model_validate(data)
    data["aggregations"] = [{"name": "n", "op": "count", "k": 2}]
    with pytest.raises(ValidationError, match="only valid for last_k"):
        FeatureSpec.model_validate(data)
