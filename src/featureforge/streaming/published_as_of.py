"""Published-as-of retrieval of spec-driven streaming features (ADR-010, ADR-011).

A version is available at T if its published_at is at or before T, no later accepted
version has a published_at at or before T, and T is before published_at plus
online.ttl. Past that the value is missing, never zero.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

from pyiceberg.expressions import In

from featureforge.streaming.contracts.feature_record import FeatureRecord, record_fields
from featureforge.streaming.history import HistoryTable

Status = Literal["present", "not_published", "expired"]


@dataclass(frozen=True)
class PublishedValue:
    """The value of one entity as serving had it at the requested time.

    record is set only for status present. For expired, published_at and
    expires_at show the version whose online.ttl ran out.
    """

    entity: str
    status: Status
    record: FeatureRecord | None = None
    published_at: datetime | None = None
    expires_at: datetime | None = None


def _rank(row: dict, counters: tuple[str, ...]) -> tuple:
    """Order versions: the latest published_at wins, ties go to the newer window."""
    return (
        row["published_at"],
        row["observation_time"],
        tuple(row[name] for name in counters),
        row["update_id"],
    )


def published_as_of(
    history: HistoryTable,
    entities: Sequence[str],
    as_of: datetime,
) -> dict[str, PublishedValue]:
    """Return what serving held for each entity at as_of, from recorded evidence."""
    if as_of.tzinfo is None or as_of.utcoffset() is None:
        raise ValueError("as_of must be timezone-aware")
    wanted = list(dict.fromkeys(entities))
    for entity in wanted:
        if not isinstance(entity, str) or not entity:
            raise ValueError("entities must be non-empty strings")
    if not wanted:
        return {}

    spec = history.spec
    entity_key, counters, derived = record_fields(spec)

    table = history.table
    table.refresh()
    rows = table.scan(row_filter=In(entity_key, wanted)).to_arrow().to_pylist()

    latest: dict[str, dict] = {}
    for row in rows:
        if row["published_at"] > as_of:
            continue
        entity = row[entity_key]
        if entity not in latest or _rank(row, counters) > _rank(latest[entity], counters):
            latest[entity] = row

    results: dict[str, PublishedValue] = {}
    for entity in wanted:
        row = latest.get(entity)
        if row is None:
            results[entity] = PublishedValue(entity=entity, status="not_published")
            continue

        published_at = row["published_at"]
        expires_at = published_at + spec.online.ttl
        if as_of >= expires_at:
            results[entity] = PublishedValue(
                entity=entity,
                status="expired",
                published_at=published_at,
                expires_at=expires_at,
            )
            continue

        record = FeatureRecord(
            feature_name=spec.name,
            entity_key=entity_key,
            entity_value=entity,
            observation_time=row["observation_time"],
            window_start_exclusive=row["window_start_exclusive"],
            counts={name: row[name] for name in counters},
            derived={name: row[name] for name in derived},
        )
        results[entity] = PublishedValue(
            entity=entity,
            status="present",
            record=record,
            published_at=published_at,
            expires_at=expires_at,
        )
    return results
