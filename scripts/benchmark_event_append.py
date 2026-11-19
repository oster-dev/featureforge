"""Benchmark: append events to the event fact table (ADR-015).

The script fills a table in a temporary SQLite catalog with a growing number of events,
one append per UTC day, and measures EventLog.append for a batch of new events and for a
batch of events the table already holds. The second run is almost only the lookup by
event_id, the first adds the write. It also reports how many data files the lookup reads.

    python scripts/benchmark_event_append.py --sizes 10000,100000,1000000 --repeats 3

The event ids are random, so the file statistics cannot prune them. The numbers describe
one process on a local disk. Compare the growth with the table size, not the absolute time.
"""

from __future__ import annotations

import argparse
import platform
import statistics
import sys
import tempfile
import time
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyiceberg
from pyiceberg.expressions import In

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.event_log import EventLog, event_arrow_schema, event_row
from featureforge.streaming.history import open_local_catalog

BASE = datetime(2026, 1, 1, tzinfo=UTC)


def sample_event() -> InteractionEventV1:
    return InteractionEventV1(
        event_id=uuid.UUID(int=1),
        event_time=BASE,
        ingested_at=BASE,
        user_id="user_1",
        content_id="content_1",
        event_type="play",
        play_seconds=30,
        source="web",
        schema_version="v1",
    )


def make_events(
    rng: np.random.Generator, template: InteractionEventV1, count: int, day_start: datetime
) -> list[InteractionEventV1]:
    offsets = rng.integers(0, 86_400, count)
    return [
        template.model_copy(
            update={
                "event_id": uuid.UUID(bytes=rng.bytes(16)),
                "event_time": day_start + timedelta(seconds=int(offset)),
            }
        )
        for offset in offsets
    ]


def fill(
    log: EventLog,
    rng: np.random.Generator,
    template: InteractionEventV1,
    rows: int,
    days: int,
    batch: int,
) -> tuple[int, list[InteractionEventV1]]:
    per_day = rows // days
    sample: list[InteractionEventV1] = []
    for day in range(days):
        events = make_events(rng, template, per_day, BASE + timedelta(days=day))
        if day == 0:
            sample = events[:batch]
        table = pa.Table.from_pylist(
            [event_row(event) for event in events], schema=event_arrow_schema()
        )
        log.table.append(table)
    return per_day * days, sample


def files(table, ids: list[str] | None = None) -> int:
    scan = table.scan() if ids is None else table.scan(row_filter=In("event_id", ids))
    return len(list(scan.plan_files()))


def measure(rows: int, days: int, batch: int, repeats: int) -> dict[str, float | int]:
    rng = np.random.default_rng(0)
    template = sample_event()
    with tempfile.TemporaryDirectory() as directory:
        log = EventLog(open_local_catalog(Path(directory)))
        print(f"filling {rows:,} events", file=sys.stderr)
        stored, sample = fill(log, rng, template, rows, days, batch)
        ids = [str(event.event_id) for event in sample]
        files_total = files(log.table)
        files_read = files(log.table, ids)

        assert log.append(sample) == 0
        new_times: list[float] = []
        duplicate_times: list[float] = []
        for _ in range(repeats):
            fresh = make_events(rng, template, batch, BASE + timedelta(days=days))
            start = time.perf_counter()
            added = log.append(fresh)
            new_times.append(time.perf_counter() - start)
            assert added == batch

            start = time.perf_counter()
            added = log.append(sample)
            duplicate_times.append(time.perf_counter() - start)
            assert added == 0

    return {
        "rows": stored,
        "files_total": files_total,
        "files_read": files_read,
        "new_median": statistics.median(new_times),
        "new_min": min(new_times),
        "new_max": max(new_times),
        "duplicate_median": statistics.median(duplicate_times),
        "duplicate_min": min(duplicate_times),
        "duplicate_max": max(duplicate_times),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument("--sizes", default="10000,100000,1000000")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--batch", type=int, default=1000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()

    results = [
        measure(int(size), args.days, args.batch, args.repeats) for size in args.sizes.split(",")
    ]
    print(
        f"Python {platform.python_version()}, pyiceberg {pyiceberg.__version__}, "
        f"{platform.platform()}, {args.days} daily appends, batch of {args.batch:,}, "
        f"{args.repeats} repeats"
    )
    print()
    print(
        f"| Rows in table | Data files | Files read by the lookup "
        f"| New {args.batch:,} events: median s (min to max) "
        f"| Duplicate {args.batch:,} events: median s (min to max) |"
    )
    print("|---:|---:|---:|---:|---:|")
    for r in results:
        print(
            f"| {r['rows']:,} | {r['files_total']} | {r['files_read']} "
            f"| {r['new_median']:.2f} ({r['new_min']:.2f} to {r['new_max']:.2f}) "
            f"| {r['duplicate_median']:.2f} ({r['duplicate_min']:.2f} to {r['duplicate_max']:.2f}) |"
        )


if __name__ == "__main__":
    main()
