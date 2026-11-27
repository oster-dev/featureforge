"""Ordering policy for count-vector features in the online store (ADR-009, ADR-017)."""

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

WriteOutcome = Literal["written", "unchanged", "stale"]
Direction = Literal["up", "down"]


class UpdateConflictError(ValueError):
    """Same-window component vectors cannot be ordered componentwise."""


@dataclass(frozen=True)
class CountVector:
    """The components of one feature value for one entity and window.

    Counts, sums and maxima grow with added events (direction "up"), a minimum falls
    ("down"). A missing component (None) is the state before the first matching event.
    A component without a direction grows.
    """

    entity: str
    observation_time: datetime
    counts: Mapping[str, int | None]
    directions: Mapping[str, Direction] = field(default_factory=dict, compare=False)

    def __post_init__(self) -> None:
        if not self.counts:
            raise ValueError("counts must not be empty")


def _progress(value: int | None, direction: Direction) -> float:
    """Map a component value to a number where a larger one means more events seen."""
    if value is None:
        return float("-inf")
    return value if direction == "up" else -value


def decide_count_vector_update(
    stored: CountVector | None,
    incoming: CountVector,
) -> WriteOutcome:
    """Order windows first, then compare same-window component vectors componentwise.

    A newer window replaces an older one, including with smaller counts. Within
    one window an incoming vector with every component at least as advanced is a
    correction, one with every component at most as advanced is stale, and
    identical vectors are unchanged. Anything else cannot be ordered and raises.
    A component is more advanced when it is larger, or smaller for a minimum, and
    a missing extreme is behind every value.

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

    directions: list[Direction] = [
        incoming.directions.get(name) or stored.directions.get(name) or "up" for name in names
    ]
    old_scores = [
        _progress(value, direction) for value, direction in zip(current, directions, strict=True)
    ]
    new_scores = [
        _progress(value, direction) for value, direction in zip(candidate, directions, strict=True)
    ]
    if all(new >= old for new, old in zip(new_scores, old_scores, strict=True)):
        return "written"
    if all(new <= old for new, old in zip(new_scores, old_scores, strict=True)):
        return "stale"

    raise UpdateConflictError(
        f"Incomparable counts for entity={incoming.entity}, "
        f"observation_time={incoming.observation_time.isoformat()}: "
        f"stored={dict(zip(names, current, strict=True))}, "
        f"incoming={dict(zip(names, candidate, strict=True))}"
    )
