"""Declarative label specification (ADR-012).

A label is data, like a feature spec, but it looks forward: its window is
(t, t + horizon]. It has its own model so that feature specs never see it.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Literal

from pydantic import Field, model_validator

from featureforge.spec.model import _BOOL_NODES, Duration, _Strict, check_expression


class LabelOutcome(_Strict):
    op: Literal["exists", "count"]
    filter: str | None = None

    @model_validator(mode="after")
    def _check(self) -> LabelOutcome:
        if self.filter is not None:
            if not self.filter.strip():
                raise ValueError("outcome.filter must not be empty; omit it to count every event")
            check_expression(self.filter, _BOOL_NODES)
        return self


class LabelDedupe(_Strict):
    key: str


class LabelSpec(_Strict):
    name: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    version: int = Field(ge=1)
    owner: str
    description: str
    entity: str | list[str]
    source: str
    outcome: LabelOutcome
    horizon: Duration
    dedupe: LabelDedupe

    @property
    def keys(self) -> list[str]:
        return [self.entity] if isinstance(self.entity, str) else list(self.entity)

    @model_validator(mode="after")
    def _check(self) -> LabelSpec:
        if self.horizon <= timedelta(0):
            raise ValueError("horizon must be positive")
        if not self.keys or len(set(self.keys)) != len(self.keys):
            raise ValueError("entity must name at least one key, without duplicates")
        return self
