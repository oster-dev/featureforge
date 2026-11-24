"""The reading side of feature serving: one interface and one reader per backend (ADR-016)."""

from collections.abc import Sequence
from typing import Protocol

from featureforge.spec.model import FeatureSpec
from featureforge.streaming.store_common import RecordRead

DEFAULT_NAMESPACE = "featureforge-stream:online"


class ProtocolError(RuntimeError):
    """A backend answered with a different number of values than keys were asked for."""


class ServingReader(Protocol):
    """Read the values of one request. A reader raises when its backend fails.

    fetch takes the pairs of spec and entity value and returns one RecordRead per pair,
    in the same order. It never returns a partial result.
    """

    def fetch(self, plan: Sequence[tuple[FeatureSpec, str]]) -> list[RecordRead]: ...

    def ping(self) -> bool: ...


class AsyncServingReader(ServingReader, Protocol):
    """A reader that can also fetch without a worker thread (ADR-016)."""

    has_async: bool

    async def fetch_async(self, plan: Sequence[tuple[FeatureSpec, str]]) -> list[RecordRead]: ...
