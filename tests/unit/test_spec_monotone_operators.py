"""Update policy rules of the spec model for operators beyond count (ADR-017)."""

from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import validate_event_fields
from featureforge.streaming.contracts.events import InteractionEventV1

FILTER = "event_type == 'play'"
COUNT = f'  - {{name: play_count, op: count, filter: "{FILTER}"}}'
SUM = f'  - {{name: play_seconds_sum, op: sum, column: play_seconds, filter: "{FILTER}"}}'
AVG = f'  - {{name: avg_play_seconds, op: avg, column: play_seconds, filter: "{FILTER}"}}'
MIN = "  - {name: min_play_seconds, op: min, column: play_seconds}"
MAX = "  - {name: max_play_seconds, op: max, column: play_seconds}"
DERIVED_AVG = (
    'derived:\n  avg_play_seconds: "play_seconds_sum / play_count if play_count > 0 else None"\n'
)

TEMPLATE = """\
name: play_seconds_policy_test
version: 1
owner: test
description: Update policy rule test.
entity: user_id
source: interaction-events.validated.v1
aggregations:
{aggregations}
{derived}window: {{size: 1h, slide: 1m, boundary: right_closed}}
lateness: {{watermark_delay: 10m, allowed: 30m}}
dedupe: {{key: event_id, ttl: 24h}}
online: {{store: redis, ttl: 2h, update_policy: {policy}}}
offline: {{store: iceberg, table: features.play_seconds_policy_test}}
freshness_slo: 5m
"""

OTHER_OPERATORS = [
    pytest.param(AVG, id="avg"),
    pytest.param(MIN, id="min"),
    pytest.param(MAX, id="max"),
]


def load(tmp_path: Path, *aggregations: str, policy: str, derived: str = ""):
    path = tmp_path / "spec.yaml"
    path.write_text(
        TEMPLATE.format(aggregations="\n".join(aggregations), derived=derived, policy=policy),
        encoding="utf-8",
    )
    return load_spec(path)


def test_componentwise_monotonic_accepts_count_sum_and_derived_avg(tmp_path: Path) -> None:
    spec = load(tmp_path, COUNT, SUM, policy="componentwise_monotonic", derived=DERIVED_AVG)

    assert spec.online.update_policy.value == "componentwise_monotonic"
    assert set(spec.derived) == {"avg_play_seconds"}


@pytest.mark.parametrize("aggregation", OTHER_OPERATORS)
def test_componentwise_monotonic_rejects_other_operators(tmp_path: Path, aggregation: str) -> None:
    with pytest.raises(ValueError, match="componentwise_monotonic supports only count and sum"):
        load(tmp_path, COUNT, aggregation, policy="componentwise_monotonic")


def test_append_only_stays_count_only(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="append_only supports only count"):
        load(tmp_path, SUM, policy="append_only")


@pytest.mark.parametrize("aggregation", OTHER_OPERATORS)
def test_window_replace_still_accepts_other_operators(tmp_path: Path, aggregation: str) -> None:
    spec = load(tmp_path, COUNT, aggregation, policy="window_replace")

    assert spec.online.update_policy.value == "window_replace"


class SignedPlaySecondsEvent(InteractionEventV1):
    play_seconds: int


class FloatPlaySecondsEvent(InteractionEventV1):
    play_seconds: float


def test_sum_over_a_non_negative_integer_field_is_accepted(tmp_path: Path) -> None:
    spec = load(tmp_path, COUNT, SUM, policy="componentwise_monotonic", derived=DERIVED_AVG)

    validate_event_fields(spec)


@pytest.mark.parametrize("model", [SignedPlaySecondsEvent, FloatPlaySecondsEvent])
def test_monotone_sum_needs_a_non_negative_integer_field(
    tmp_path: Path, model: type[InteractionEventV1]
) -> None:
    spec = load(tmp_path, COUNT, SUM, policy="componentwise_monotonic", derived=DERIVED_AVG)

    with pytest.raises(ValueError, match="play_seconds_sum"):
        validate_event_fields(spec, model)


def test_window_replace_sum_may_use_a_signed_field(tmp_path: Path) -> None:
    spec = load(tmp_path, COUNT, SUM, policy="window_replace")

    validate_event_fields(spec, SignedPlaySecondsEvent)
