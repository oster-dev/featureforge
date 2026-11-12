"""Flink job plan derived from a FeatureSpec (ADR-008, WP1 stage 3).

The plan holds everything a generic job needs from a spec: window, lateness,
dedupe horizon, counters with their filters, and derived values. It supports
count aggregations only. Other operators are rejected with a clear message
instead of being computed wrongly. Nothing here imports Flink.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from featureforge.spec.model import FeatureSpec, Op
from featureforge.spec.reference import evaluate_expression

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MS = timedelta(milliseconds=1)


class UnsupportedSpecError(ValueError):
    """The spec is valid but outside what the generic Flink job supports."""


@dataclass(frozen=True)
class CounterPlan:
    name: str
    filter_expression: str | None


@dataclass(frozen=True)
class FlinkJobPlan:
    feature_name: str
    entity_key: str
    dedupe_key: str
    window_size_ms: int
    window_slide_ms: int
    watermark_delay_ms: int
    allowed_lateness_ms: int
    dedupe_ttl_ms: int
    counters: tuple[CounterPlan, ...]
    derived: tuple[tuple[str, str], ...]

    @property
    def zero(self) -> tuple[int, ...]:
        return (0,) * len(self.counters)


def build_flink_plan(spec: FeatureSpec) -> FlinkJobPlan:
    problems: list[str] = []
    if len(spec.keys) != 1:
        problems.append(f"composite entity keys {spec.keys} are not supported yet")
    for aggregation in spec.aggregations:
        if aggregation.op is not Op.COUNT:
            problems.append(
                f"aggregation '{aggregation.name}' uses op {aggregation.op.value}; "
                "only count is supported yet"
            )
    if problems:
        raise UnsupportedSpecError(
            f"spec '{spec.name}' is not supported by the generic Flink job: " + "; ".join(problems)
        )

    return FlinkJobPlan(
        feature_name=spec.name,
        entity_key=spec.keys[0],
        dedupe_key=spec.dedupe.key,
        window_size_ms=spec.window.size // _MS,
        window_slide_ms=spec.window.slide // _MS,
        watermark_delay_ms=spec.lateness.watermark_delay // _MS,
        allowed_lateness_ms=spec.lateness.allowed // _MS,
        dedupe_ttl_ms=spec.dedupe.ttl // _MS,
        counters=tuple(CounterPlan(a.name, a.filter) for a in spec.aggregations),
        derived=tuple(spec.derived.items()),
    )


def entity_value(plan: FlinkJobPlan, payload: str) -> str:
    return json.loads(payload)[plan.entity_key]


def dedupe_value(plan: FlinkJobPlan, payload: str) -> str:
    return str(json.loads(payload)[plan.dedupe_key])


def accumulate(
    plan: FlinkJobPlan, event: Mapping[str, Any], accumulator: tuple[int, ...]
) -> tuple[int, ...]:
    return tuple(
        count
        + int(
            counter.filter_expression is None
            or bool(evaluate_expression(counter.filter_expression, event))
        )
        for counter, count in zip(plan.counters, accumulator, strict=True)
    )


def merge(left: tuple[int, ...], right: tuple[int, ...]) -> tuple[int, ...]:
    return tuple(a + b for a, b in zip(left, right, strict=True))


def utc_timestamp(milliseconds: int) -> str:
    return (EPOCH + timedelta(milliseconds=milliseconds)).isoformat().replace("+00:00", "Z")


def feature_record(
    plan: FlinkJobPlan,
    key: str,
    window_start_ms: int,
    window_end_ms: int,
    accumulator: tuple[int, ...],
) -> str:
    counts = {c.name: n for c, n in zip(plan.counters, accumulator, strict=True)}
    values = dict(counts)
    for name, expression in plan.derived:
        values[name] = evaluate_expression(expression, counts)
    return json.dumps(
        {
            "feature_schema_version": "v1",
            "feature_name": plan.feature_name,
            plan.entity_key: key,
            "observation_time": utc_timestamp(window_end_ms),
            "window_start_exclusive": utc_timestamp(window_start_ms),
            **values,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def late_event_record(plan: FlinkJobPlan, payload: str) -> str:
    return json.dumps(
        {
            "error_schema_version": "v1",
            "reason_code": "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS",
            "feature_name": plan.feature_name,
            "raw_payload": payload,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
