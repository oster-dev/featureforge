"""Benchmark: read a training dataset with the pandas and the Arrow loader (ADR-014).

The script writes a synthetic dataset in the layout of build-training-dataset into a
temporary directory and reads it with each loader in its own process. It reports the
time to the first batch, the rows per second and the peak memory the read adds.

    python scripts/benchmark_training_loaders.py --rows 5000000 --days 30 --repeats 3

The operating system caches the files, so the numbers describe a warm read. The pandas
loader also sorts the rows by key, which the Arrow loader does not do.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import resource
import statistics
import subprocess
import sys
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from featureforge.spec.training_batches import DEFAULT_BATCH_SIZE, open_training_batches
from featureforge.spec.training_loader import load_training_dataset

PREFIX = "user_skip_rate_24h__"
COLUMNS = [
    "user_id",
    "observation_time",
    f"{PREFIX}play_count",
    f"{PREFIX}skip_count",
    f"{PREFIX}skip_rate",
    "label__completed_within_7d",
    "label__completed_within_7d__observable_at",
]
DAY_MICROS = 86_400_000_000
TIME_TYPE = pa.timestamp("us", tz="UTC")


def generate(directory: Path, rows: int, days: int) -> None:
    rng = np.random.default_rng(0)
    per_day = rows // days
    partitions = []
    for offset in range(days):
        day = date(2026, 1, 1) + timedelta(days=offset)
        day_start = int(datetime(day.year, day.month, day.day, tzinfo=UTC).timestamp()) * 1_000_000
        micros = day_start + rng.integers(0, DAY_MICROS, per_day)
        plays = rng.integers(1, 200, per_day)
        skips = np.minimum(plays, rng.integers(0, 100, per_day))
        users = np.char.add("user", np.char.zfill(rng.integers(0, 100_000, per_day).astype(str), 6))
        table = pa.table(
            {
                "user_id": pa.array(users),
                "observation_time": pa.array(micros, pa.int64()).cast(TIME_TYPE),
                f"{PREFIX}play_count": plays,
                f"{PREFIX}skip_count": skips,
                f"{PREFIX}skip_rate": skips / plays,
                "label__completed_within_7d": rng.random(per_day) < 0.5,
                "label__completed_within_7d__observable_at": pa.array(
                    micros + 7 * DAY_MICROS, pa.int64()
                ).cast(TIME_TYPE),
            }
        )
        part_dir = directory / f"observation_date={day.isoformat()}"
        part_dir.mkdir(parents=True)
        pq.write_table(table, part_dir / "part-00000.snappy.parquet", compression="snappy")
        partitions.append({"observation_date": day.isoformat(), "rows": per_day})
    manifest = {
        "manifest_version": 1,
        "mode": "event_time",
        "columns": COLUMNS,
        "rows": per_day * days,
        "partitions": partitions,
    }
    (directory / "_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")


def peak_mb() -> float:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / 1024**2 if sys.platform == "darwin" else peak / 1024


def measure(loader: str, directory: Path, batch_size: int) -> dict[str, Any]:
    baseline = peak_mb()
    started = time.perf_counter()
    first: float | None = None
    rows = 0
    if loader == "pandas":
        frame = load_training_dataset(directory).frame
        first = time.perf_counter() - started
        rows = len(frame)
    elif loader == "pandas-raw":
        frame = pd.read_parquet(directory)
        first = time.perf_counter() - started
        rows = len(frame)
    else:
        for batch in open_training_batches(directory).batches(batch_size):
            if first is None:
                first = time.perf_counter() - started
            rows += batch.num_rows
    total = time.perf_counter() - started
    return {
        "rows": rows,
        "first": first,
        "total": total,
        "added_mb": peak_mb() - baseline,
    }


def run_child(loader: str, directory: Path, batch_size: int) -> dict[str, Any]:
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--read",
        loader,
        "--directory",
        str(directory),
        "--batch-size",
        str(batch_size),
    ]
    result = subprocess.run(command, capture_output=True, text=True, check=True)
    parsed: dict[str, Any] = json.loads(result.stdout.strip().splitlines()[-1])
    return parsed


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0] if __doc__ else "")
    parser.add_argument("--rows", type=int, default=5_000_000)
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--repeats", type=int, default=3)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument(
        "--read", choices=["pandas", "pandas-raw", "arrow"], help="internal: one measured read"
    )
    parser.add_argument("--directory", help="internal: dataset directory for --read")
    args = parser.parse_args()

    if args.read:
        print(json.dumps(measure(args.read, Path(args.directory), args.batch_size)))
        return

    with tempfile.TemporaryDirectory() as tmp:
        directory = Path(tmp) / "dataset"
        directory.mkdir()
        generate(directory, args.rows, args.days)
        size_mb = sum(p.stat().st_size for p in directory.rglob("*.parquet")) / 1024**2
        runs = {
            loader: [run_child(loader, directory, args.batch_size) for _ in range(args.repeats)]
            for loader in ("pandas", "pandas-raw", "arrow")
        }

    rows = runs["pandas"][0]["rows"]
    expected = args.rows // args.days * args.days
    for loader, loader_runs in runs.items():
        assert all(run["rows"] == expected for run in loader_runs), loader
    print(
        f"Environment: {platform.platform()}, Python {platform.python_version()}, "
        f"pandas {pd.__version__}, pyarrow {pa.__version__}, {os.cpu_count()} CPUs"
    )
    print(
        f"Dataset: {rows:,} rows in {args.days} partitions, {size_mb:.0f} MB Parquet, "
        f"batch size {args.batch_size:,}, median of {args.repeats} runs, warm file cache"
    )
    print()
    print(
        "| Loader | Time to first batch (s) | Total (s) | Rows per second | Added peak memory (MB) |"
    )
    print(
        "|--------|-------------------------|-----------|-----------------|------------------------|"
    )
    for loader, loader_runs in runs.items():
        first = statistics.median(run["first"] for run in loader_runs)
        total = statistics.median(run["total"] for run in loader_runs)
        added = statistics.median(run["added_mb"] for run in loader_runs)
        print(f"| {loader} | {first:.2f} | {total:.2f} | {rows / total:,.0f} | {added:.0f} |")


if __name__ == "__main__":
    main()
