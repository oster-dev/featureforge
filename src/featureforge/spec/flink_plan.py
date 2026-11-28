"""Flink job plan derived from a FeatureSpec (ADR-008).

The plan holds everything a generic job needs from a spec: window, lateness,
dedupe horizon, aggregations with their filters, and derived values. It supports
count, sum, avg, min, max and last_k aggregations (ADR-017). Other operators are
rejected with a clear message instead of being computed wrongly. Nothing here
imports Flink.

The state of a window is a flat tuple. Each aggregation owns one slot (count, sum,
last_k) or two slots (avg, min, max: the value and the number of events). A numeric
slot is an integer, and two states merge by adding it. A last_k slot holds a tuple of
(event time in microseconds, event id, value) entries, ordered ascending and at most k
long. A spec with the policy window_replace gets one more slot at the end, the events
seen, which counts every event of the window whatever the filters say (ADR-017).
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from featureforge.spec.model import FeatureSpec, Op, UpdatePolicy
from featureforge.spec.reference import evaluate_expression, names_in

EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_MS = timedelta(milliseconds=1)
_US = timedelta(microseconds=1)
SUPPORTED_OPS = frozenset({Op.COUNT, Op.SUM, Op.AVG, Op.MIN, Op.MAX, Op.LAST_K})
EVENTS_SEEN = "events_seen"

Accumulator = tuple[Any, ...]


class UnsupportedSpecError(ValueError):
    """The spec is valid but outside what the generic Flink job supports."""


@dataclass(frozen=True)
class AggregationPlan:
    name: str
    op: Op
    column: str | None
    filter_expression: str | None
    slot: int
    k: int | None = None

    @property
    def width(self) -> int:
        return 2 if self.op in (Op.AVG, Op.MIN, Op.MAX) else 1


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
    aggregations: tuple[AggregationPlan, ...]
    derived: tuple[tuple[str, str], ...]
    track_events_seen: bool = False

    @property
    def events_seen_slot(self) -> int | None:
        if not self.track_events_seen:
            return None
        return sum(aggregation.width for aggregation in self.aggregations)

    @property
    def slots(self) -> int:
        extra = 1 if self.track_events_seen else 0
        return sum(aggregation.width for aggregation in self.aggregations) + extra

    @property
    def buffered(self) -> bool:
        """True if a slot holds a buffer, so the state is no longer a tuple of integers."""
        return any(aggregation.op is Op.LAST_K for aggregation in self.aggregations)

    @property
    def zero(self) -> Accumulator:
        state: list[Any] = []
        for aggregation in self.aggregations:
            if aggregation.op is Op.LAST_K:
                state.append(())
            else:
                state.extend([0] * aggregation.width)
        if self.track_events_seen:
            state.append(0)
        return tuple(state)


def _aggregation_plans(spec: FeatureSpec) -> tuple[AggregationPlan, ...]:
    plans: list[AggregationPlan] = []
    slot = 0
    for aggregation in spec.aggregations:
        plan = AggregationPlan(
            aggregation.name,
            aggregation.op,
            aggregation.column,
            aggregation.filter,
            slot,
            aggregation.k,
        )
        plans.append(plan)
        slot += plan.width
    return tuple(plans)


def _column(aggregation: AggregationPlan) -> str:
    if aggregation.column is None:
        raise ValueError(f"aggregation '{aggregation.name}' needs a column")
    return aggregation.column


def _k(aggregation: AggregationPlan) -> int:
    if aggregation.k is None:
        raise ValueError(f"aggregation '{aggregation.name}' needs k")
    return aggregation.k


def _event_time_us(event: Mapping[str, Any]) -> int:
    return (datetime.fromisoformat(str(event["event_time"])) - EPOCH) // _US


def build_flink_plan(spec: FeatureSpec) -> FlinkJobPlan:
    problems: list[str] = []
    if len(spec.keys) != 1:
        problems.append(f"composite entity keys {spec.keys} are not supported yet")
    for aggregation in spec.aggregations:
        if aggregation.op not in SUPPORTED_OPS:
            problems.append(
                f"aggregation '{aggregation.name}' uses op {aggregation.op.value}; "
                "only count, sum, avg, min, max and last_k are supported yet"
            )
    track_events_seen = spec.online.update_policy is UpdatePolicy.WINDOW_REPLACE
    if track_events_seen:
        names = [aggregation.name for aggregation in spec.aggregations] + list(spec.derived)
        if EVENTS_SEEN in names:
            problems.append(
                f"the name '{EVENTS_SEEN}' is reserved for the events seen of a "
                "window_replace spec (ADR-017)"
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
        aggregations=_aggregation_plans(spec),
        derived=tuple(spec.derived.items()),
        track_events_seen=track_events_seen,
    )


def entity_value(plan: FlinkJobPlan, payload: str) -> str:
    return json.loads(payload)[plan.entity_key]


def dedupe_value(plan: FlinkJobPlan, payload: str) -> str:
    return str(json.loads(payload)[plan.dedupe_key])


def accumulate(
    plan: FlinkJobPlan, event: Mapping[str, Any], accumulator: Accumulator
) -> Accumulator:
    """Add one event to the state of a window.

    An event that fails the filter of an aggregation adds nothing to it. The events seen
    count every event, whatever the filters say.
    """
    if len(accumulator) != plan.slots:
        raise ValueError(f"accumulator has {len(accumulator)} slots, the plan needs {plan.slots}")
    state = list(accumulator)
    if plan.events_seen_slot is not None:
        state[plan.events_seen_slot] += 1
    for aggregation in plan.aggregations:
        if aggregation.filter_expression is not None and not bool(
            evaluate_expression(aggregation.filter_expression, event)
        ):
            continue
        if aggregation.op is Op.COUNT:
            state[aggregation.slot] += 1
            continue
        value = int(event[_column(aggregation)])
        slot = aggregation.slot
        if aggregation.op is Op.LAST_K:
            entry = (_event_time_us(event), str(event["event_id"]), value)
            state[slot] = tuple(sorted((*state[slot], entry))[-_k(aggregation) :])
        elif aggregation.op in (Op.MIN, Op.MAX):
            if state[slot + 1] == 0:
                state[slot] = value
            elif aggregation.op is Op.MIN:
                state[slot] = min(state[slot], value)
            else:
                state[slot] = max(state[slot], value)
            state[slot + 1] += 1
        else:
            state[slot] += value
            if aggregation.op is Op.AVG:
                state[slot + 1] += 1
    return tuple(state)


def merge(plan: FlinkJobPlan, left: Accumulator, right: Accumulator) -> Accumulator:
    """Combine two partial states of one window.

    Counts, sums, the parts of an avg and the events seen add up. An extreme takes the
    smaller or larger value and ignores a side that saw no matching event. A last_k buffer
    keeps the last k entries of both sides.
    """
    if len(left) != plan.slots or len(right) != plan.slots:
        raise ValueError(f"both accumulators need {plan.slots} slots for this plan")
    state = [() if isinstance(a, tuple) else a + b for a, b in zip(left, right, strict=True)]
    for aggregation in plan.aggregations:
        slot = aggregation.slot
        if aggregation.op is Op.LAST_K:
            state[slot] = tuple(sorted((*left[slot], *right[slot]))[-_k(aggregation) :])
        elif aggregation.op in (Op.MIN, Op.MAX):
            if left[slot + 1] == 0:
                state[slot] = right[slot]
            elif right[slot + 1] == 0:
                state[slot] = left[slot]
            elif aggregation.op is Op.MIN:
                state[slot] = min(left[slot], right[slot])
            else:
                state[slot] = max(left[slot], right[slot])
    return tuple(state)


def utc_timestamp(milliseconds: int) -> str:
    return (EPOCH + timedelta(milliseconds=milliseconds)).isoformat().replace("+00:00", "Z")


def aggregate_values(
    plan: FlinkJobPlan, accumulator: Accumulator
) -> dict[str, int | float | list[int] | None]:
    """The value of every aggregation.

    An avg, min or max is missing for a window without matching events, and a last_k is an
    empty list.
    """
    if len(accumulator) != plan.slots:
        raise ValueError(f"accumulator has {len(accumulator)} slots, the plan needs {plan.slots}")
    values: dict[str, int | float | list[int] | None] = {}
    for aggregation in plan.aggregations:
        if aggregation.op is Op.AVG:
            total, count = accumulator[aggregation.slot], accumulator[aggregation.slot + 1]
            values[aggregation.name] = total / count if count else None
        elif aggregation.op in (Op.MIN, Op.MAX):
            matched = accumulator[aggregation.slot + 1]
            values[aggregation.name] = accumulator[aggregation.slot] if matched else None
        elif aggregation.op is Op.LAST_K:
            values[aggregation.name] = [entry[2] for entry in accumulator[aggregation.slot]]
        else:
            values[aggregation.name] = accumulator[aggregation.slot]
    return values


def feature_record(
    plan: FlinkJobPlan,
    key: str,
    window_start_ms: int,
    window_end_ms: int,
    accumulator: Accumulator,
) -> str:
    aggregates = aggregate_values(plan, accumulator)
    values: dict[str, Any] = dict(aggregates)
    for name, expression in plan.derived:
        if any(aggregates.get(ref) is None for ref in names_in(expression)):
            values[name] = None
        else:
            values[name] = evaluate_expression(expression, aggregates)
    if plan.events_seen_slot is not None:
        values[EVENTS_SEEN] = accumulator[plan.events_seen_slot]
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
