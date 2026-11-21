"""Guarded Redis writes for spec-driven count-vector features (ADR-009)."""

import re
from dataclasses import dataclass
from typing import cast

from redis import Redis
from redis.exceptions import WatchError

from featureforge.spec.model import FeatureSpec, UpdatePolicy
from featureforge.streaming.contracts.count_vector import (
    WriteOutcome,
    decide_count_vector_update,
)
from featureforge.streaming.contracts.feature_record import (
    FeatureRecord,
    parse_feature_record,
    record_fields,
)

SUPPORTED_POLICIES = (
    UpdatePolicy.APPEND_ONLY,
    UpdatePolicy.COMPONENTWISE_MONOTONIC,
)
MAX_ATTEMPTS = 16


@dataclass(frozen=True)
class RecordRead:
    """One decoded read: a record, no record, or the type of a decoding error."""

    record: FeatureRecord | None = None
    error_type: str | None = None


class UnsupportedPolicyError(ValueError):
    """The spec asks for an update policy this store does not implement."""


_WHITESPACE = re.compile(r"\s")


def check_text(value: object, name: str) -> None:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    if not value or _WHITESPACE.search(value):
        raise ValueError(f"{name} must be non-empty and contain no whitespace")


class RedisSpecStore:
    """Store the latest window of one spec-defined feature per entity.

    Newer windows replace older ones. Within one window the count components
    only grow. Writes are guarded with WATCH/MULTI and retried at most 16 times.
    The Redis expiry comes from online.ttl, runs in wall-clock time from the last
    accepted write, and is not extended by unchanged or stale deliveries.
    Independent runs require independent namespaces. Retractions are unsupported.
    The caller owns the Redis connection lifecycle.
    """

    def __init__(
        self,
        client: Redis,
        spec: FeatureSpec,
        *,
        key_prefix: str | None = None,
    ) -> None:
        if spec.online.store != "redis":
            raise ValueError(f"spec {spec.name} targets {spec.online.store}, not redis")
        if spec.online.update_policy not in SUPPORTED_POLICIES:
            raise UnsupportedPolicyError(
                f"update_policy {spec.online.update_policy.value} is not supported"
            )
        record_fields(spec)

        prefix = (
            f"featureforge-stream:online:v{spec.version}:{spec.name}"
            if key_prefix is None
            else key_prefix
        )
        check_text(prefix, "key_prefix")

        self._client = client
        self._spec = spec
        self._key_prefix = prefix
        self._ttl_seconds = int(spec.online.ttl.total_seconds())

    def key_for(self, entity_value: str) -> str:
        """Return the Redis key used for this entity."""
        check_text(entity_value, "entity_value")
        return f"{self._key_prefix}:{entity_value}"

    def _decode(self, payload: str | bytes, *, entity_value: str) -> FeatureRecord:
        record = parse_feature_record(payload, self._spec)
        if record.entity_value != entity_value:
            raise ValueError("stored feature entity does not match requested entity")
        return record

    def read(self, entity_value: str) -> FeatureRecord | None:
        """Read a validated record, or return None when no key exists."""
        payload = cast("str | bytes | None", self._client.get(self.key_for(entity_value)))
        if payload is None:
            return None
        return self._decode(payload, entity_value=entity_value)

    def decode_read(self, payload: str | bytes | None, entity_value: str) -> RecordRead:
        """Decode one payload from a multi-key read without raising on bad data."""
        if payload is None:
            return RecordRead()
        try:
            return RecordRead(record=self._decode(payload, entity_value=entity_value))
        except (ValueError, TypeError) as error:
            return RecordRead(error_type=type(error).__name__)

    def write(self, record: FeatureRecord) -> WriteOutcome:
        """Apply window ordering and componentwise same-window count ordering.

        The record is validated against the spec before Redis is touched.
        Connection errors, conflicts and exhausted retries propagate.
        """
        payload = record.to_json()
        incoming = parse_feature_record(payload, self._spec)
        key = self.key_for(incoming.entity_value)

        for _ in range(MAX_ATTEMPTS):
            with self._client.pipeline() as pipeline:
                try:
                    pipeline.watch(key)
                    stored_payload = cast("str | bytes | None", pipeline.get(key))
                    stored = (
                        self._decode(stored_payload, entity_value=incoming.entity_value)
                        if stored_payload is not None
                        else None
                    )
                    outcome = decide_count_vector_update(
                        stored.vector() if stored is not None else None,
                        incoming.vector(),
                    )
                    if outcome != "written":
                        pipeline.unwatch()
                        return outcome

                    pipeline.multi()
                    pipeline.set(key, payload, ex=self._ttl_seconds)
                    pipeline.execute()
                    return "written"
                except WatchError:
                    continue

        raise WatchError(
            f"Feature write aborted after {MAX_ATTEMPTS} concurrent modification retries"
        )
