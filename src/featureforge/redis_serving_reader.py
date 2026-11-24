"""Redis reader of the feature serving service (ADR-016)."""

from collections.abc import Sequence
from typing import Any, cast

from redis import Redis
from redis.asyncio import Redis as AsyncRedis

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
        async_client: AsyncRedis | None = None,
    ) -> None:
        self._client = client
        self._async_client = async_client
        self._stores = {
            (spec.name, spec.version): RedisSpecStore(
                client, spec, key_prefix=f"{namespace}:v{spec.version}:{spec.name}"
            )
            for spec in specs
        }

    def ping(self) -> bool:
        return bool(self._client.ping())

    @property
    def has_async(self) -> bool:
        return self._async_client is not None

    def _keys(self, plan: Sequence[tuple[FeatureSpec, str]]) -> list[str]:
        stores = self._stores
        return [stores[(spec.name, spec.version)].key_for(value) for spec, value in plan]

    def _decode(
        self, payloads: list[Any], plan: Sequence[tuple[FeatureSpec, str]]
    ) -> list[RecordRead]:
        if len(payloads) != len(plan):
            raise ProtocolError("backend answered with a different number of values")
        stores = self._stores
        return [
            stores[(spec.name, spec.version)].decode_read(payload, value)
            for payload, (spec, value) in zip(payloads, plan, strict=True)
        ]

    def fetch(self, plan: Sequence[tuple[FeatureSpec, str]]) -> list[RecordRead]:
        payloads = list(cast("list[Any]", self._client.mget(self._keys(plan))))
        return self._decode(payloads, plan)

    async def fetch_async(self, plan: Sequence[tuple[FeatureSpec, str]]) -> list[RecordRead]:
        if self._async_client is None:
            raise RuntimeError("this reader has no asynchronous client")
        payloads = list(cast("list[Any]", await self._async_client.mget(self._keys(plan))))
        return self._decode(payloads, plan)
