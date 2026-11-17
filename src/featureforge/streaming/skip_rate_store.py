"""Guarded Redis writes for user skip-rate count vectors.

A thin wrapper over RedisSpecStore (ADR-009, decision 10). The constructor, the
methods and the typed model are unchanged. The ordering, the WATCH/MULTI retries
and the online.ttl expiry come from the generic store and the spec.
"""

from functools import cache
from pathlib import Path

from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.streaming.contracts.count_vector import UpdateConflictError
from featureforge.streaming.contracts.feature_record import FeatureRecord, record_fields
from featureforge.streaming.contracts.user_skip_rate import (
    SkipRateUpdateConflictError,
    UpdateDecision,
    UserSkipRateFeatureV1,
)
from featureforge.streaming.spec_store import RedisSpecStore

SPEC_PATH = (
    Path(__file__).resolve().parents[3] / "feature_repo" / "specs" / "user_skip_rate_24h.yaml"
)


@cache
def skip_rate_spec() -> FeatureSpec:
    return load_spec(SPEC_PATH)


class RedisUserSkipRateStore:
    """Store the latest window within one consistent run namespace.

    Same-window updates follow componentwise count ordering, not rate ordering.
    Independent runs require independent namespaces. Retractions are unsupported.
    The caller owns the Redis connection lifecycle.
    """

    def __init__(self, client: Redis, *, key_prefix: str | None = None) -> None:
        spec = skip_rate_spec()
        self._store = RedisSpecStore(client, spec, key_prefix=key_prefix)
        self._name = spec.name
        self._entity_key, self._counters, self._derived = record_fields(spec)

    def key_for(self, user_id: str) -> str:
        """Return the Redis key used for this user."""
        return self._store.key_for(user_id)

    def read(self, user_id: str) -> UserSkipRateFeatureV1 | None:
        """Read a validated feature, or return None when no key exists."""
        record = self._store.read(user_id)
        return None if record is None else self._to_feature(record)

    def write(self, feature: UserSkipRateFeatureV1) -> UpdateDecision:
        """Apply window ordering and componentwise same-window count ordering.

        Retry at most 16 times when concurrent writers change the key.
        Connection errors and exhausted retries propagate to the caller.
        """
        if not isinstance(feature, UserSkipRateFeatureV1):
            raise TypeError("feature must be a UserSkipRateFeatureV1 instance")
        try:
            return self._store.write(self._to_record(feature))
        except UpdateConflictError as exc:
            raise SkipRateUpdateConflictError(str(exc)) from exc

    def _to_record(self, feature: UserSkipRateFeatureV1) -> FeatureRecord:
        return FeatureRecord(
            feature_name=self._name,
            entity_key=self._entity_key,
            entity_value=getattr(feature, self._entity_key),
            observation_time=feature.observation_time,
            window_start_exclusive=feature.window_start_exclusive,
            counts={name: getattr(feature, name) for name in self._counters},
            derived={name: getattr(feature, name) for name in self._derived},
        )

    def _to_feature(self, record: FeatureRecord) -> UserSkipRateFeatureV1:
        return UserSkipRateFeatureV1.model_validate(
            {
                self._entity_key: record.entity_value,
                "observation_time": record.observation_time,
                "window_start_exclusive": record.window_start_exclusive,
                **record.counts,
                **record.derived,
            }
        )
