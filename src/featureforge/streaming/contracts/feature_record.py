"""Feature records for spec-driven count and ratio features (ADR-009).

A record is the JSON the generic Flink job emits for one entity and one window.
"""

import json
import math
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime

from featureforge.spec.compiled_expression import compile_expression
from featureforge.spec.model import FeatureSpec, Op
from featureforge.streaming.contracts.count_vector import CountVector

SCHEMA_VERSION = "v1"
BASE_FIELDS = (
    "feature_schema_version",
    "feature_name",
    "observation_time",
    "window_start_exclusive",
)
ENTITY_VALUE = re.compile(r"\S+")


def stamp(moment: datetime) -> str:
    """Format a timestamp the way the Flink job does: UTC with a Z suffix."""
    return moment.astimezone(UTC).isoformat().replace("+00:00", "Z")


@dataclass(frozen=True)
class FeatureRecord:
    """One validated feature value for one entity and one observation window."""

    feature_name: str
    entity_key: str
    entity_value: str
    observation_time: datetime
    window_start_exclusive: datetime
    counts: Mapping[str, int]
    derived: Mapping[str, float]

    def vector(self) -> CountVector:
        return CountVector(
            entity=self.entity_value,
            observation_time=self.observation_time,
            counts=dict(self.counts),
        )

    def to_json(self) -> str:
        return json.dumps(
            {
                "feature_schema_version": SCHEMA_VERSION,
                "feature_name": self.feature_name,
                self.entity_key: self.entity_value,
                "observation_time": stamp(self.observation_time),
                "window_start_exclusive": stamp(self.window_start_exclusive),
                **self.counts,
                **self.derived,
            },
            sort_keys=True,
            separators=(",", ":"),
        )


def counter_names(spec: FeatureSpec) -> tuple[str, ...]:
    names = []
    for aggregation in spec.aggregations:
        if aggregation.op is not Op.COUNT:
            raise ValueError(
                f"only count aggregations are supported, got {aggregation.op.value} "
                f"for {aggregation.name}"
            )
        names.append(aggregation.name)
    return tuple(names)


def record_fields(spec: FeatureSpec) -> tuple[str, tuple[str, ...], tuple[str, ...]]:
    """Return the entity key, counter names and derived names of a spec."""
    keys = spec.keys
    if len(keys) != 1:
        raise ValueError("composite entity keys are not supported")
    entity_key = keys[0]
    counters = counter_names(spec)
    derived = tuple(spec.derived)
    taken = (*BASE_FIELDS, entity_key, *counters, *derived)
    if len(set(taken)) != len(taken):
        raise ValueError(f"record field names of spec {spec.name} must be unique")
    return entity_key, counters, derived


def parse_time(value: object, name: str) -> datetime:
    if not isinstance(value, str):
        raise ValueError(f"{name} must be a string")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an ISO 8601 timestamp") from exc
    if parsed.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return parsed.astimezone(UTC)


class RecordDecoder:
    """Parse and validate the records of one spec.

    Built once per spec, so that a read does not recompute the field layout. It
    returns what parse_feature_record returns and raises what it raises.
    """

    def __init__(self, spec: FeatureSpec) -> None:
        self._spec = spec
        self._entity_key, self._counters, self._derived_names = record_fields(spec)
        self._expected = {
            *BASE_FIELDS,
            self._entity_key,
            *self._counters,
            *self._derived_names,
        }
        self._expressions = tuple((name, spec.derived[name]) for name in self._derived_names)

    def parse(self, payload: str | bytes) -> FeatureRecord:
        """Parse and validate one record against its spec. Raises ValueError."""
        spec = self._spec
        entity_key = self._entity_key
        expected = self._expected

        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("record must be a JSON object")

        if data.keys() != expected:
            raise ValueError(
                f"record fields do not match spec {spec.name}: "
                f"missing {sorted(expected - data.keys())}, "
                f"unexpected {sorted(data.keys() - expected)}"
            )
        if data["feature_schema_version"] != SCHEMA_VERSION:
            raise ValueError(f"feature_schema_version must be {SCHEMA_VERSION}")
        if data["feature_name"] != spec.name:
            raise ValueError(f"feature_name must be {spec.name}")

        entity_value = data[entity_key]
        if not isinstance(entity_value, str) or not ENTITY_VALUE.fullmatch(entity_value):
            raise ValueError(f"{entity_key} must be a non-empty string without whitespace")

        observation_time = parse_time(data["observation_time"], "observation_time")
        window_start = parse_time(data["window_start_exclusive"], "window_start_exclusive")
        if observation_time - window_start != spec.window.size:
            raise ValueError(f"record window must be exactly {spec.window.size}")

        counts: dict[str, int] = {}
        for name in self._counters:
            value = data[name]
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} must be a non-negative integer")
            counts[name] = value

        derived: dict[str, float] = {}
        for name, expression in self._expressions:
            value = data[name]
            if isinstance(value, bool) or not isinstance(value, int | float):
                raise ValueError(f"{name} must be a number")
            if not math.isfinite(value):
                raise ValueError(f"{name} must be finite")
            try:
                expected_value = compile_expression(expression)(counts)
            except ZeroDivisionError as exc:
                raise ValueError(f"{name} cannot be evaluated for these counts") from exc
            if not math.isclose(value, expected_value, rel_tol=1e-12, abs_tol=1e-15):
                raise ValueError(f"{name} must match its spec expression")
            derived[name] = value

        return FeatureRecord(
            feature_name=spec.name,
            entity_key=entity_key,
            entity_value=entity_value,
            observation_time=observation_time,
            window_start_exclusive=window_start,
            counts=counts,
            derived=derived,
        )


def parse_feature_record(payload: str | bytes, spec: FeatureSpec) -> FeatureRecord:
    """Parse and validate one record against its spec. Raises ValueError."""
    return RecordDecoder(spec).parse(payload)
