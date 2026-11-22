"""Cassandra reader of the feature serving service (ADR-016)."""

from collections.abc import Sequence
from typing import Any

from cassandra import ConsistencyLevel

from featureforge.spec.model import FeatureSpec
from featureforge.streaming.cassandra_schema import DEFAULT_KEYSPACE, TABLE, check_keyspace
from featureforge.streaming.contracts.feature_record import RecordDecoder
from featureforge.streaming.store_common import (
    RecordRead,
    UnsupportedPolicyError,
    decode_read,
)


class CassandraServingReader:
    """Read the values of a request with one query per entity partition.

    The partition is (entity key, entity value) and the clustering column is the key prefix
    of a feature, so all features of one entity are one partition read. The queries of a
    request run concurrently and the request waits for all of them. A failure of any query
    raises, because a partial result would depend on timing. The timeout is the budget of
    the request. It is a client-side timeout: the node may keep working on a query after
    the driver gave up, and the node has its own read timeout. The statement is prepared
    when the reader is built, so a missing table fails at startup. The caller owns the
    session and the schema.
    """

    def __init__(
        self,
        session: Any,
        specs: Sequence[FeatureSpec],
        *,
        keyspace: str = DEFAULT_KEYSPACE,
        consistency: int = ConsistencyLevel.LOCAL_QUORUM,
        timeout: float = 0.05,
    ) -> None:
        check_keyspace(keyspace)
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        self._specs: dict[tuple[str, int], tuple[str, str, RecordDecoder]] = {}
        for spec in specs:
            if spec.online.store != "cassandra":
                raise ValueError(f"spec {spec.name} targets {spec.online.store}, not cassandra")
            if len(spec.keys) != 1:
                raise UnsupportedPolicyError("composite entity keys are not supported")
            self._specs[(spec.name, spec.version)] = (
                spec.keys[0],
                f"v{spec.version}:{spec.name}",
                RecordDecoder(spec),
            )
        self._session = session
        self._timeout = timeout
        self._select = session.prepare(
            f"SELECT feature, payload FROM {keyspace}.{TABLE} "
            "WHERE entity_key = ? AND entity_value = ? AND feature IN ?"
        )
        self._select.consistency_level = consistency

    def ping(self) -> bool:
        row = self._session.execute("SELECT now() FROM system.local", timeout=self._timeout).one()
        return row is not None

    def fetch(self, plan: Sequence[tuple[FeatureSpec, str]]) -> list[RecordRead]:
        wanted: dict[tuple[str, str], set[str]] = {}
        for spec, value in plan:
            entity_key, prefix, _ = self._specs[(spec.name, spec.version)]
            wanted.setdefault((entity_key, value), set()).add(prefix)
        futures = [
            (
                group,
                self._session.execute_async(
                    self._select,
                    (group[0], group[1], sorted(prefixes)),
                    timeout=self._timeout,
                ),
            )
            for group, prefixes in wanted.items()
        ]
        payloads: dict[tuple[str, str, str], str | None] = {}
        for (entity_key, value), future in futures:
            for row in future.result():
                payloads[(entity_key, value, row.feature)] = row.payload
        reads: list[RecordRead] = []
        for spec, value in plan:
            entity_key, prefix, decoder = self._specs[(spec.name, spec.version)]
            reads.append(decode_read(decoder, payloads.get((entity_key, value, prefix)), value))
        return reads
