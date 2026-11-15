"""Ordering policy for count-vector features in the online store (ADR-009)."""

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

WriteOutcome = Literal["written", "unchanged", "stale"]


class UpdateConflictError(ValueError):
    """Same-window count vectors cannot be ordered componentwise."""


@dataclass(frozen=True)
class CountVector:
    """The count components of one feature value for one entity and window."""

    entity: str
    observation_time: datetime
    counts: Mapping[str, int]

    def __post_init__(self) -> None:
        if not self.counts:
            raise ValueError("counts must not be empty")


def decide_count_vector_update(
    stored: CountVector | None,
    incoming: CountVector,
) -> WriteOutcome:
    """Order windows first, then compare same-window count vectors componentwise.

    A newer window replaces an older one, including with smaller counts. Within
    one window an incoming vector with all components greater or equal is a
    correction, one with all components less or equal is stale, and identical
    vectors are unchanged. Anything else cannot be ordered and raises.

    A vector with one component is the append-only rule. Derived values such as
    a rate are never ordered, because a falling rate can be a valid correction.
    Callers must isolate independent runs in separate namespaces. This does not
    prove event-set identity or repair inflated counts.
    """
    if stored is None:
        return "written"
    if incoming.entity != stored.entity:
        raise ValueError("cannot compare features for different entities")
    if incoming.counts.keys() != stored.counts.keys():
        raise ValueError(
            f"count components differ: stored {sorted(stored.counts)}, "
            f"incoming {sorted(incoming.counts)}"
        )

    if incoming.observation_time > stored.observation_time:
        return "written"
    if incoming.observation_time < stored.observation_time:
        return "stale"

    names = sorted(stored.counts)
    current = [stored.counts[name] for name in names]
    candidate = [incoming.counts[name] for name in names]

    if candidate == current:
        return "unchanged"
    if all(new >= old for new, old in zip(candidate, current, strict=True)):
        return "written"
    if all(new <= old for new, old in zip(candidate, current, strict=True)):
        return "stale"

    raise UpdateConflictError(
        f"Incomparable counts for entity={incoming.entity}, "
        f"observation_time={incoming.observation_time.isoformat()}: "
        f"stored={dict(zip(names, current, strict=True))}, "
        f"incoming={dict(zip(names, candidate, strict=True))}"
    )
