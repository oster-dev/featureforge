"""Guarded Cassandra writes for spec-driven count-vector features (ADR-013)."""

from typing import Any, cast

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from featureforge.spec.model import FeatureSpec
from featureforge.streaming.cassandra_schema import DEFAULT_KEYSPACE, TABLE, check_keyspace
from featureforge.streaming.contracts.count_vector import (
    WriteOutcome,
    decide_count_vector_update,
)
from featureforge.streaming.contracts.feature_record import FeatureRecord, RecordDecoder
from featureforge.streaming.store_common import (
    SUPPORTED_POLICIES,
    UnsupportedPolicyError,
    check_text,
)

MAX_ATTEMPTS = 16


class ConcurrentWriteError(RuntimeError):
    """The conditional write lost to other writers in every attempt."""


class CassandraSpecStore:
    """Store the latest window of one spec-defined feature per entity in Cassandra.

    One row per entity and feature: the partition is (entity key, entity value) and the
    clustering column is the key prefix. Newer windows replace older ones, and within one
    window the count components only grow. A write reads the current value at the serial
    consistency, decides and then writes with a lightweight transaction: INSERT IF NOT
    EXISTS for a first value and UPDATE IF payload = <value read> for a replacement. When
    the condition is not applied, the store decides again from the value that came back,
    at most 16 times. A timeout of a conditional write propagates, and a replay decides
    again. The expiry comes from online.ttl, is set on every written update and is not
    extended by unchanged or stale deliveries. The caller owns the session.
    """

    def __init__(
        self,
        session: Any,
        spec: FeatureSpec,
        *,
        keyspace: str = DEFAULT_KEYSPACE,
        key_prefix: str | None = None,
        consistency: int = ConsistencyLevel.LOCAL_QUORUM,
        serial_consistency: int = ConsistencyLevel.LOCAL_SERIAL,
    ) -> None:
        if spec.online.store != "cassandra":
            raise ValueError(f"spec {spec.name} targets {spec.online.store}, not cassandra")
        if len(spec.keys) != 1:
            raise UnsupportedPolicyError("composite entity keys are not supported")
        if spec.online.update_policy not in SUPPORTED_POLICIES:
            raise UnsupportedPolicyError(
                f"update_policy {spec.online.update_policy.value} is not supported"
            )
        check_keyspace(keyspace)
        prefix = f"v{spec.version}:{spec.name}" if key_prefix is None else key_prefix
        check_text(prefix, "key_prefix")

        ttl = int(spec.online.ttl.total_seconds())
        table = f"{keyspace}.{TABLE}"
        where = "entity_key = %s AND entity_value = %s AND feature = %s"
        select = f"SELECT payload FROM {table} WHERE {where}"

        self._session = session
        self._decoder = RecordDecoder(spec)
        self._entity_key = spec.keys[0]
        self._prefix = prefix
        self._read = SimpleStatement(select, consistency_level=consistency)
        self._serial_read = SimpleStatement(select, consistency_level=serial_consistency)
        self._insert = SimpleStatement(
            f"INSERT INTO {table} (entity_key, entity_value, feature, payload) "
            f"VALUES (%s, %s, %s, %s) IF NOT EXISTS USING TTL {ttl}",
            consistency_level=consistency,
            serial_consistency_level=serial_consistency,
        )
        self._update = SimpleStatement(
            f"UPDATE {table} USING TTL {ttl} SET payload = %s WHERE {where} IF payload = %s",
            consistency_level=consistency,
            serial_consistency_level=serial_consistency,
        )

    def _key(self, entity_value: str) -> tuple[str, str, str]:
        check_text(entity_value, "entity_value")
        return (self._entity_key, entity_value, self._prefix)

    def _decode(self, payload: str, *, entity_value: str) -> FeatureRecord:
        record = self._decoder.parse(payload)
        if record.entity_value != entity_value:
            raise ValueError("stored feature entity does not match requested entity")
        return record

    def _payload(self, statement: SimpleStatement, key: tuple[str, str, str]) -> str | None:
        row = self._session.execute(statement, key).one()
        return None if row is None else cast("str | None", row.payload)

    def read(self, entity_value: str) -> FeatureRecord | None:
        """Read a validated record, or return None when no row exists."""
        payload = self._payload(self._read, self._key(entity_value))
        if payload is None:
            return None
        return self._decode(payload, entity_value=entity_value)

    def write(self, record: FeatureRecord) -> WriteOutcome:
        """Apply window ordering and componentwise same-window count ordering.

        The record is validated against the spec before Cassandra is touched. Timeouts,
        conflicts and exhausted attempts propagate.
        """
        payload = record.to_json()
        incoming = self._decoder.parse(payload)
        key = self._key(incoming.entity_value)

        stored_payload = self._payload(self._serial_read, key)
        for _ in range(MAX_ATTEMPTS):
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
                return outcome

            if stored_payload is None:
                row = self._session.execute(self._insert, (*key, payload)).one()
            else:
                row = self._session.execute(self._update, (payload, *key, stored_payload)).one()
            if row[0]:
                return "written"
            stored_payload = cast("str | None", row.payload)

        raise ConcurrentWriteError(
            f"Feature write aborted after {MAX_ATTEMPTS} lost conditional writes"
        )
