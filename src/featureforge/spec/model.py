"""Declarative feature specification (ADR-008).

A spec is data. It is validated here before anything is executed, and the
expressions it contains are restricted to a small, side-effect-free subset.
"""

from __future__ import annotations

import ast
import re
from datetime import timedelta
from enum import StrEnum
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

_DURATION = re.compile(r"^(\d+)(s|m|h|d)$")
_UNIT_SECONDS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def parse_duration(value: object) -> timedelta:
    """Parse '<int><s|m|h|d>' (for example '24h') into a timedelta."""
    if isinstance(value, timedelta):
        return value
    match = _DURATION.match(value.strip()) if isinstance(value, str) else None
    if match is None:
        raise ValueError(f"invalid duration {value!r}; use <int><s|m|h|d>, for example '24h'")
    return timedelta(seconds=int(match[1]) * _UNIT_SECONDS[match[2]])


Duration = Annotated[timedelta, BeforeValidator(parse_duration)]

_BOOL_NODES: tuple[type[ast.AST], ...] = (
    ast.Expression,
    ast.BoolOp,
    ast.And,
    ast.Or,
    ast.UnaryOp,
    ast.Not,
    ast.Compare,
    ast.Eq,
    ast.NotEq,
    ast.Lt,
    ast.LtE,
    ast.Gt,
    ast.GtE,
    ast.In,
    ast.NotIn,
    ast.Name,
    ast.Load,
    ast.Constant,
    ast.Tuple,
    ast.List,
)

_ARITH_NODES: tuple[type[ast.AST], ...] = _BOOL_NODES + (
    ast.BinOp,
    ast.Add,
    ast.Sub,
    ast.Mult,
    ast.Div,
    ast.IfExp,
    ast.USub,
)


def check_expression(expr: str, allowed: tuple[type[ast.AST], ...]) -> set[str]:
    """Reject expressions outside the allowed node set; return referenced names."""
    try:
        tree = ast.parse(expr, mode="eval")
    except SyntaxError as exc:
        raise ValueError(f"invalid expression {expr!r}: {exc.msg}") from None
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, allowed):
            raise ValueError(f"{type(node).__name__} is not allowed in expression {expr!r}")
        if isinstance(node, ast.Name):
            names.add(node.id)
    return names


class Op(StrEnum):
    COUNT = "count"
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    LAST_K = "last_k"
    APPROX_DISTINCT = "approx_distinct"


class UpdatePolicy(StrEnum):
    APPEND_ONLY = "append_only"
    COMPONENTWISE_MONOTONIC = "componentwise_monotonic"
    WINDOW_REPLACE = "window_replace"


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class Aggregation(_Strict):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    op: Op
    column: str | None = None
    filter: str | None = None
    k: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _check(self) -> Aggregation:
        if self.op is not Op.COUNT and not self.column:
            raise ValueError(f"aggregation '{self.name}': op {self.op.value} needs 'column'")
        if self.op is Op.LAST_K and self.k is None:
            raise ValueError(f"aggregation '{self.name}': last_k needs 'k'")
        if self.op is not Op.LAST_K and self.k is not None:
            raise ValueError(f"aggregation '{self.name}': 'k' is only valid for last_k")
        if self.filter:
            check_expression(self.filter, _BOOL_NODES)
        return self


class Window(_Strict):
    size: Duration
    slide: Duration
    boundary: Literal["right_closed"]

    @model_validator(mode="after")
    def _check(self) -> Window:
        if self.size <= timedelta(0) or self.slide <= timedelta(0):
            raise ValueError("window size and slide must be positive")
        if self.slide > self.size or self.size % self.slide != timedelta(0):
            raise ValueError("window slide must divide window size")
        return self


class Lateness(_Strict):
    watermark_delay: Duration
    allowed: Duration


class Dedupe(_Strict):
    key: str
    ttl: Duration


class Online(_Strict):
    store: Literal["redis", "cassandra"]
    ttl: Duration
    update_policy: UpdatePolicy


class Offline(_Strict):
    store: Literal["iceberg"]
    table: str = Field(pattern=r"^[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*$")


class FeatureSpec(_Strict):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    version: int = Field(ge=1)
    owner: str
    description: str
    entity: str | list[str]
    source: str
    aggregations: list[Aggregation] = Field(min_length=1)
    derived: dict[str, str] = Field(default_factory=dict)
    window: Window
    lateness: Lateness
    dedupe: Dedupe
    online: Online
    offline: Offline
    freshness_slo: Duration

    @property
    def keys(self) -> list[str]:
        return [self.entity] if isinstance(self.entity, str) else list(self.entity)

    @model_validator(mode="after")
    def _check(self) -> FeatureSpec:
        agg_names = [a.name for a in self.aggregations]
        if len(set(agg_names)) != len(agg_names):
            raise ValueError("aggregation names must be unique")

        clash = set(agg_names) & set(self.derived)
        if clash:
            raise ValueError(f"derived names clash with aggregations: {sorted(clash)}")

        for dname, expr in self.derived.items():
            unknown = check_expression(expr, _ARITH_NODES) - set(agg_names)
            if unknown:
                raise ValueError(
                    f"derived '{dname}' references unknown names {sorted(unknown)}; "
                    f"available: {sorted(agg_names)}"
                )

        need = self.window.size + self.lateness.allowed
        if self.dedupe.ttl < need:
            raise ValueError(
                f"dedupe.ttl ({self.dedupe.ttl}) must be >= window.size + lateness.allowed ({need})"
            )
        if self.online.ttl < self.window.size:
            raise ValueError("online.ttl must be >= window.size")

        policy = self.online.update_policy
        monotone = policy in (UpdatePolicy.APPEND_ONLY, UpdatePolicy.COMPONENTWISE_MONOTONIC)
        if monotone and any(a.op is not Op.COUNT for a in self.aggregations):
            raise ValueError(
                f"update_policy {policy.value} requires count-only aggregations; "
                "use window_replace for other operators (see ADR-008)"
            )
        if policy is UpdatePolicy.APPEND_ONLY and len(self.aggregations) != 1:
            raise ValueError("append_only requires exactly one aggregation")
        return self
