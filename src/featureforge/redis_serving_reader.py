"""Redis reader of the feature serving service (ADR-016)."""

from collections.abc import Sequence
from typing import Any, cast

from redis import Redis

from featureforge.serving_reader import DEFAULT_NAMESPACE, ProtocolError
from featureforge.spec.model import FeatureSpec
from featureforge.streaming.spec_store import RedisSpecStore
from featureforge.streaming.store_common import RecordRead


class RedisServingReader:
    """Read the values of a request with one MGET over the keys of all pairs.

    The caller owns the Redis client and sets its socket timeouts.
    """

    def __init__(
        self,
        client: Redis,
        specs: Sequence[FeatureSpec],
        *,
        namespace: str = DEFAULT_NAMESPACE,
    ) -> None:
        self._client = client
        self._stores = {
            (spec.name, spec.version): RedisSpecStore(
                client, spec, key_prefix=f"{namespace}:v{spec.version}:{spec.name}"
            )
            for spec in specs
        }

    def ping(self) -> bool:
        return bool(self._client.ping())

    def fetch(self, plan: Sequence[tuple[FeatureSpec, str]]) -> list[RecordRead]:
        stores = self._stores
        keys = [stores[(spec.name, spec.version)].key_for(value) for spec, value in plan]
        payloads = list(cast("list[Any]", self._client.mget(keys)))
        if len(payloads) != len(keys):
            raise ProtocolError("backend answered with a different number of values")
        return [
            stores[(spec.name, spec.version)].decode_read(payload, value)
            for payload, (spec, value) in zip(payloads, plan, strict=True)
        ]
