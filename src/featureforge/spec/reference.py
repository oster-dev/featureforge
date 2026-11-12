"""Generic event-time reference evaluated directly from a FeatureSpec (ADR-008).

Exact and deliberately simple. It is the oracle that the Flink job and the
Spark backfill must match.
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from featureforge.spec.model import Aggregation, FeatureSpec, Op
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import EventIdentityConflictError

_COMPARE: dict[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.In: lambda left, right: left in right,
    ast.NotIn: lambda left, right: left not in right,
}
_ARITH: dict[type[ast.operator], Callable[[Any, Any], Any]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}


def names_in(expr: str) -> set[str]:
    tree = ast.parse(expr, mode="eval")
    return {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}


def evaluate_expression(expr: str, names: Mapping[str, Any]) -> Any:
    """Evaluate a validated expression without eval()."""
    return _eval(ast.parse(expr, mode="eval").body, names)


def _eval(node: ast.AST, names: Mapping[str, Any]) -> Any:
    if isinstance(node, ast.Constant):
        return node.value
    if isinstance(node, ast.Name):
        if node.id not in names:
            raise ValueError(f"unknown name '{node.id}'")
        return names[node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        return [_eval(item, names) for item in node.elts]
    if isinstance(node, ast.UnaryOp):
        operand = _eval(node.operand, names)
        if isinstance(node.op, ast.Not):
            return not operand
        if isinstance(node.op, ast.USub):
            return -operand
    if isinstance(node, ast.BoolOp):
        results = (_eval(value, names) for value in node.values)
        return all(results) if isinstance(node.op, ast.And) else any(results)
    if isinstance(node, ast.BinOp) and type(node.op) in _ARITH:
        return _ARITH[type(node.op)](_eval(node.left, names), _eval(node.right, names))
    if isinstance(node, ast.Compare):
        left = _eval(node.left, names)
        for op, comparator in zip(node.ops, node.comparators, strict=True):
            right = _eval(comparator, names)
            if not _COMPARE[type(op)](left, right):
                return False
            left = right
        return True
    if isinstance(node, ast.IfExp):
        branch = node.body if _eval(node.test, names) else node.orelse
        return _eval(branch, names)
    raise ValueError(f"unsupported expression node {type(node).__name__}")


def referenced_event_fields(spec: FeatureSpec) -> set[str]:
    fields = set(spec.keys) | {spec.dedupe.key, "event_time"}
    for agg in spec.aggregations:
        if agg.column:
            fields.add(agg.column)
        if agg.filter:
            fields |= names_in(agg.filter)
    return fields


def validate_event_fields(
    spec: FeatureSpec, model: type[InteractionEventV1] = InteractionEventV1
) -> None:
    """Reject specs that reference fields the event contract does not have."""
    unknown = referenced_event_fields(spec) - set(model.model_fields)
    if unknown:
        raise ValueError(
            f"spec '{spec.name}' references unknown event fields {sorted(unknown)}; "
            f"available: {sorted(model.model_fields)}"
        )


@dataclass(frozen=True)
class SpecResult:
    spec_name: str
    observation_time: datetime
    window_start_exclusive: datetime
    values: dict[str, Any]


def _check_entity(spec: FeatureSpec, entity: Mapping[str, str]) -> None:
    if not isinstance(entity, Mapping):
        raise TypeError("entity must be a mapping of key name to value")
    if set(entity) != set(spec.keys):
        raise ValueError(f"entity must provide exactly {spec.keys}, got {sorted(entity)}")
    for key, value in entity.items():
        if not isinstance(value, str):
            raise TypeError(f"entity value for '{key}' must be a string")
        if not value or any(character.isspace() for character in value):
            raise ValueError(f"entity value for '{key}' must be non-empty without whitespace")


def _aggregate(agg: Aggregation, matched: list[tuple[InteractionEventV1, dict[str, Any]]]) -> Any:
    if agg.op is Op.COUNT:
        return len(matched)
    values = [fields[agg.column] for _, fields in matched]
    if agg.op is Op.SUM:
        return sum(values)
    if agg.op is Op.AVG:
        return sum(values) / len(values) if values else None
    if agg.op is Op.MIN:
        return min(values) if values else None
    if agg.op is Op.MAX:
        return max(values) if values else None
    if agg.op is Op.APPROX_DISTINCT:
        return len(set(values))
    ordered = sorted(matched, key=lambda pair: (pair[0].event_time, str(pair[0].event_id)))
    return [fields[agg.column] for _, fields in ordered][-agg.k :]


def evaluate_spec(
    spec: FeatureSpec,
    events: Iterable[InteractionEventV1],
    *,
    entity: Mapping[str, str],
    observation_time: datetime,
) -> SpecResult:
    """Evaluate every aggregation and derived value over (t - size, t].

    Identities are validated across the entire input before any filtering.
    Missing values (an empty avg, min or max) are None, never zero.
    """
    validate_event_fields(spec)
    _check_entity(spec, entity)
    if not isinstance(observation_time, datetime):
        raise TypeError("observation_time must be a datetime")
    if observation_time.utcoffset() is None:
        raise ValueError("observation_time must be timezone-aware")

    cutoff = observation_time.astimezone(UTC)
    window_start = cutoff - spec.window.size

    unique: dict[object, InteractionEventV1] = {}
    for event in events:
        if not isinstance(event, InteractionEventV1):
            raise TypeError("events must contain InteractionEventV1 instances")
        identity = getattr(event, spec.dedupe.key)
        previous = unique.get(identity)
        if previous is None:
            unique[identity] = event
        elif previous.model_dump(exclude={"ingested_at"}) != event.model_dump(
            exclude={"ingested_at"}
        ):
            raise EventIdentityConflictError(
                f"Conflicting logical payload for {spec.dedupe.key}={identity}"
            )

    rows = [
        (event, event.model_dump())
        for event in unique.values()
        if all(getattr(event, key) == entity[key] for key in spec.keys)
        and window_start < event.event_time <= cutoff
    ]

    values: dict[str, Any] = {}
    for agg in spec.aggregations:
        matched = [
            (event, fields)
            for event, fields in rows
            if agg.filter is None or evaluate_expression(agg.filter, fields)
        ]
        values[agg.name] = _aggregate(agg, matched)

    for name, expr in spec.derived.items():
        refs = names_in(expr)
        values[name] = (
            None if any(values[ref] is None for ref in refs) else evaluate_expression(expr, values)
        )

    return SpecResult(spec.name, cutoff, window_start, values)
