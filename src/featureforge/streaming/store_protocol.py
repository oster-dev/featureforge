"""The contract of an online store and the factory that picks its backend (ADR-013)."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

from featureforge.spec.model import FeatureSpec
from featureforge.streaming.cassandra_schema import DEFAULT_KEYSPACE
from featureforge.streaming.contracts.count_vector import WriteOutcome
from featureforge.streaming.contracts.feature_record import FeatureRecord

if TYPE_CHECKING:
    from redis import Redis


class OnlineStore(Protocol):
    """The latest window of one spec-defined feature per entity.

    Unknown entities read as None, never as zero. A record that does not match the
    spec raises before the store is touched. Outcomes follow the decision function
    of ADR-009. The expiry comes from online.ttl and is set only on a written
    update. Backend details such as key_for stay outside this contract.
    """

    def read(self, entity_value: str) -> FeatureRecord | None: ...

    def write(self, record: FeatureRecord) -> WriteOutcome: ...


def create_online_store(
    spec: FeatureSpec,
    *,
    redis_client: Redis | None = None,
    cassandra_session: Any = None,
    keyspace: str = DEFAULT_KEYSPACE,
    key_prefix: str | None = None,
) -> OnlineStore:
    """Return the store for the online.store backend of the spec.

    Each backend loads its client library only when its spec asks for it, so an
    installation needs the library of the backends it uses and no other.
    """
    backend = spec.online.store
    if backend == "redis":
        if redis_client is None:
            raise ValueError("a redis client is required for a redis store")
        from featureforge.streaming.spec_store import RedisSpecStore

        return RedisSpecStore(redis_client, spec, key_prefix=key_prefix)
    if backend == "cassandra":
        if cassandra_session is None:
            raise ValueError("a cassandra session is required for a cassandra store")
        from featureforge.streaming.cassandra_store import CassandraSpecStore

        return CassandraSpecStore(cassandra_session, spec, keyspace=keyspace, key_prefix=key_prefix)
    raise ValueError(f"online store {backend} is not supported")
