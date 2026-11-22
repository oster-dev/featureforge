"""Pieces that every online store backend shares (ADR-013).

The module imports no client library, so one backend can use it without installing the
client library of another.
"""

import re
from dataclasses import dataclass

from featureforge.spec.model import UpdatePolicy
from featureforge.streaming.contracts.feature_record import FeatureRecord

SUPPORTED_POLICIES = (
    UpdatePolicy.APPEND_ONLY,
    UpdatePolicy.COMPONENTWISE_MONOTONIC,
)


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
