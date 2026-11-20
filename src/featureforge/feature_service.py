"""Feature serving core (ADR-016): one status per value, one round trip, explicit fallback."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from typing import Any, Literal, cast

from redis import Redis

from featureforge.spec.model import FallbackPolicy, FeatureSpec
from featureforge.streaming.contracts.feature_record import ENTITY_VALUE
from featureforge.streaming.spec_store import RecordRead, RedisSpecStore

Status = Literal["ok", "missing", "stale", "backend_error"]
Freshness = Literal["fresh", "stale", "future"]

MAX_ENTITIES = 100
MAX_FEATURES = 50
DEFAULT_NAMESPACE = "featureforge-stream:online"


class RequestError(ValueError):
    """The request is malformed. The service answers 422."""


class FailPolicyError(RuntimeError):
    """A feature with the fail policy had a backend error. The service answers 503."""

    def __init__(self, features: Sequence[str]) -> None:
        self.features = tuple(features)
        super().__init__("backend error for features with the fail policy")


@dataclass(frozen=True)
class FeatureRef:
    name: str
    version: int | None = None


@dataclass(frozen=True)
class FeatureEntry:
    """One value for one entity and one feature. Never dropped from a response."""

    feature: str
    version: int
    entity: Mapping[str, str]
    status: Status
    values: Mapping[str, float] | None
    observation_time: datetime | None = None
    age_seconds: float | None = None
    freshness: Freshness | None = None
    published_at: datetime | None = None
    defaulted: bool = False
    error_type: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "feature": self.feature,
            "version": self.version,
            "entity": dict(self.entity),
            "status": self.status,
            "values": None if self.values is None else dict(self.values),
            "observation_time": _iso(self.observation_time),
            "age_seconds": self.age_seconds,
            "freshness": self.freshness,
            "published_at": _iso(self.published_at),
            "defaulted": self.defaulted,
            "error_type": self.error_type,
        }


@dataclass(frozen=True)
class LookupResult:
    """A combined read, not an atomic snapshot across keys."""

    reference_time: datetime
    entries: tuple[FeatureEntry, ...]
    elapsed_ms: float
    atomic: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "reference_time": self.reference_time.isoformat(),
            "atomic": self.atomic,
            "elapsed_ms": round(self.elapsed_ms, 3),
            "entries": [entry.to_dict() for entry in self.entries],
        }


def _iso(moment: datetime | None) -> str | None:
    return None if moment is None else moment.isoformat()


def _reference_time(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(UTC)
    if value.tzinfo is None or value.utcoffset() is None:
        raise RequestError("reference_time must include an explicit time zone")
    return value.astimezone(UTC)


def _entity_value(entity: Mapping[str, str], spec: FeatureSpec) -> str:
    key = spec.keys[0]
    value = entity.get(key)
    if value is None:
        raise RequestError(f"entity lacks the key {key} required by feature {spec.name}")
    if not isinstance(value, str) or not ENTITY_VALUE.fullmatch(value):
        raise RequestError(f"{key} must be a non-empty string without whitespace")
    return value


def _build_entry(
    spec: FeatureSpec,
    value: str,
    read: RecordRead | None,
    error_type: str | None,
    reference: datetime,
) -> FeatureEntry:
    entity = {spec.keys[0]: value}
    failure = error_type if error_type is not None else (read.error_type if read else None)
    if failure is not None or read is None:
        return FeatureEntry(
            feature=spec.name,
            version=spec.version,
            entity=entity,
            status="backend_error",
            values=None,
            error_type=failure or "NoRead",
        )
    record = read.record
    if record is None:
        return FeatureEntry(
            feature=spec.name,
            version=spec.version,
            entity=entity,
            status="missing",
            values=None,
        )

    age = (reference - record.observation_time).total_seconds()
    freshness: Freshness
    status: Status
    if age < 0:
        status, freshness = "stale", "future"
    elif age > spec.freshness_slo.total_seconds():
        status, freshness = "stale", "stale"
    else:
        status, freshness = "ok", "fresh"
    return FeatureEntry(
        feature=spec.name,
        version=spec.version,
        entity=entity,
        status=status,
        values={**record.counts, **record.derived},
        observation_time=record.observation_time,
        age_seconds=age,
        freshness=freshness,
    )


def _apply_fallback(spec: FeatureSpec, entry: FeatureEntry) -> tuple[FeatureEntry, bool]:
    """Return the entry after the fallback policy and whether it fails the request."""
    if entry.status not in ("missing", "backend_error"):
        return entry, False
    fallback = spec.online.fallback
    if fallback.policy is FallbackPolicy.DEFAULT_VALUE:
        return replace(entry, values=dict(fallback.defaults), defaulted=True), False
    if fallback.policy is FallbackPolicy.FAIL and entry.status == "backend_error":
        return entry, True
    return entry, False


class FeatureServer:
    """Serve spec-defined features from Redis with an explicit status per value.

    A request makes one MGET over the keys of all requested features. Connection
    errors and timeouts become backend_error entries. Exception messages are not
    returned. The caller owns the Redis client and sets its socket timeouts.
    """

    def __init__(
        self,
        client: Redis,
        specs: Sequence[FeatureSpec],
        *,
        namespace: str = DEFAULT_NAMESPACE,
    ) -> None:
        self._client = client
        self._specs: dict[tuple[str, int], FeatureSpec] = {}
        self._stores: dict[tuple[str, int], RedisSpecStore] = {}
        for spec in specs:
            identity = (spec.name, spec.version)
            if identity in self._specs:
                raise ValueError(f"duplicate spec {spec.name} version {spec.version}")
            self._specs[identity] = spec
            self._stores[identity] = RedisSpecStore(
                client, spec, key_prefix=f"{namespace}:v{spec.version}:{spec.name}"
            )
        if not self._specs:
            raise ValueError("at least one spec is required")

    def ping(self) -> bool:
        return bool(self._client.ping())

    def _resolve(self, ref: FeatureRef) -> FeatureSpec:
        versions = sorted(version for name, version in self._specs if name == ref.name)
        if not versions:
            raise RequestError(f"unknown feature {ref.name}")
        version = ref.version if ref.version is not None else versions[-1]
        if version not in versions:
            raise RequestError(f"unknown version {version} of feature {ref.name}")
        return self._specs[(ref.name, version)]

    def lookup(
        self,
        features: Sequence[FeatureRef],
        entities: Sequence[Mapping[str, str]],
        *,
        reference_time: datetime | None = None,
    ) -> LookupResult:
        started = time.perf_counter()
        reference = _reference_time(reference_time)
        if not 1 <= len(features) <= MAX_FEATURES:
            raise RequestError(f"features must hold 1 to {MAX_FEATURES} items")
        if not 1 <= len(entities) <= MAX_ENTITIES:
            raise RequestError(f"entities must hold 1 to {MAX_ENTITIES} items")

        specs = [self._resolve(ref) for ref in features]
        identities = [(spec.name, spec.version) for spec in specs]
        if len(set(identities)) != len(identities):
            raise RequestError("a feature is requested twice")

        plan = [(spec, _entity_value(entity, spec)) for entity in entities for spec in specs]
        keys = [self._stores[(spec.name, spec.version)].key_for(value) for spec, value in plan]

        error_type: str | None = None
        payloads: list[Any] = []
        try:
            payloads = list(cast("list[Any]", self._client.mget(keys)))
            if len(payloads) != len(keys):
                error_type = "ProtocolError"
        except Exception as error:
            error_type = type(error).__name__

        entries: list[FeatureEntry] = []
        failed: list[str] = []
        for index, (spec, value) in enumerate(plan):
            read = (
                None
                if error_type is not None
                else self._stores[(spec.name, spec.version)].decode_read(payloads[index], value)
            )
            entry, fails = _apply_fallback(
                spec, _build_entry(spec, value, read, error_type, reference)
            )
            entries.append(entry)
            if fails and spec.name not in failed:
                failed.append(spec.name)
        if failed:
            raise FailPolicyError(failed)

        return LookupResult(
            reference_time=reference,
            entries=tuple(entries),
            elapsed_ms=(time.perf_counter() - started) * 1000,
        )
