"""Exact reference evaluation of a LabelSpec (ADR-012).

The window of an observation time t is (t, t + horizon], right closed. An event
at t belongs to the features and never to the label. The label of t is known
from t + horizon. An observation with t + horizon > data_end has no value: it
is excluded and counted, never evaluated as false or zero.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from featureforge.spec.label_loader import validate_label_against_contract
from featureforge.spec.label_model import LabelSpec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import EventIdentityConflictError


@dataclass(frozen=True)
class LabelObservation:
    entity: Mapping[str, str]
    observation_time: datetime


@dataclass(frozen=True)
class LabelValue:
    entity: dict[str, str]
    observation_time: datetime
    observable_at: datetime
    value: bool | int


@dataclass(frozen=True)
class LabelResult:
    label_name: str
    values: list[LabelValue]
    excluded_incomplete: int


def _utc(value: object, name: str) -> datetime:
    if not isinstance(value, datetime):
        raise TypeError(f"{name} must be a datetime")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


def _check_entity(keys: list[str], entity: Mapping[str, str]) -> None:
    if not isinstance(entity, Mapping):
        raise TypeError("entity must be a mapping of key name to value")
    if set(entity) != set(keys):
        raise ValueError(f"entity must provide exactly {keys}, got {sorted(entity)}")
    for key, value in entity.items():
        if not isinstance(value, str):
            raise TypeError(f"entity value for '{key}' must be a string")
        if not value or any(character.isspace() for character in value):
            raise ValueError(f"entity value for '{key}' must be non-empty without whitespace")


def _unique_events(
    spec: LabelSpec, events: Iterable[InteractionEventV1]
) -> list[InteractionEventV1]:
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
    return list(unique.values())


def evaluate_labels(
    spec: LabelSpec,
    events: Iterable[InteractionEventV1],
    *,
    observations: Iterable[LabelObservation],
    data_end: datetime,
) -> LabelResult:
    """Evaluate the label for every observable observation.

    Identities are validated across the entire input before any filtering.
    `data_end` is the time up to which the input is complete.
    """
    validate_label_against_contract(spec)
    end = _utc(data_end, "data_end")
    unique = _unique_events(spec, events)
    rows = [(event, event.model_dump()) for event in unique]

    values: list[LabelValue] = []
    excluded = 0
    for observation in observations:
        _check_entity(spec.keys, observation.entity)
        start = _utc(observation.observation_time, "observation_time")
        observable_at = start + spec.horizon
        if observable_at > end:
            excluded += 1
            continue

        matched = sum(
            1
            for event, fields in rows
            if all(getattr(event, key) == observation.entity[key] for key in spec.keys)
            and start < event.event_time <= observable_at
            and (spec.outcome.filter is None or evaluate_expression(spec.outcome.filter, fields))
        )
        values.append(
            LabelValue(
                entity=dict(observation.entity),
                observation_time=start,
                observable_at=observable_at,
                value=matched > 0 if spec.outcome.op == "exists" else matched,
            )
        )

    return LabelResult(spec.name, values, excluded)
