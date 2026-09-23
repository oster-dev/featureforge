#!/usr/bin/env python3
"""End-to-end orchestration for FeatureForge.

This script runs a complete feature pipeline:

1. Generate synthetic source data.
2. Backfill offline features into the canonical offline feature store.
3. Materialize features into the Redis online store through Feast.
4. Optionally run incremental materialization.

Run artifacts and the canonical offline feature store are intentionally separate:

- ``output_dir`` contains run-scoped source data and materialization manifests.
- ``offline_store_dir`` contains canonical partitioned feature data read by
  Feast FileSources.

All steps use explicit UTC timestamps and produce machine-readable manifests.
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path


def run_command(
    args: list[str],
    *,
    description: str,
    check: bool = True,
) -> None:
    """Run a FeatureForge CLI command and print an execution summary."""
    print(f"\n[INFO] {description}")
    print(f"[INFO] Running: {' '.join(args)}\n")

    result = subprocess.run(args, check=False)

    if result.returncode != 0:
        print(f"\n[ERROR] {description} failed with exit code {result.returncode}")
        if check:
            sys.exit(result.returncode)
        return

    print(f"[OK] {description} completed successfully.")


def build_parser() -> argparse.ArgumentParser:
    """Build the end-to-end orchestration argument parser."""
    parser = argparse.ArgumentParser(
        prog="run_end_to_end",
        description=(
            "Run a complete FeatureForge pipeline: generate → backfill → "
            "materialize → materialize-incremental."
        ),
    )

    parser.add_argument(
        "--config",
        type=Path,
        default=Path("configs/synthetic_data.yaml"),
        help="Path to the synthetic data YAML configuration file.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("output"),
        help=(
            "Root directory for run-scoped artifacts such as source data and "
            "materialization manifests (default: output)."
        ),
    )
    parser.add_argument(
        "--offline-store-dir",
        type=Path,
        default=Path("output/offline_store"),
        help=(
            "Canonical offline feature store written by backfill and read by "
            "Feast FileSources (default: output/offline_store)."
        ),
    )
    parser.add_argument(
        "--backfill-start",
        type=str,
        default="2026-03-20",
        help="First backfill date, inclusive, in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--backfill-end",
        type=str,
        default="2026-03-25",
        help="Last backfill date, inclusive, in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Lookback window in days for feature computation (default: 7).",
    )
    parser.add_argument(
        "--materialize-start",
        type=str,
        default=None,
        help=(
            "Start time for full materialization in ISO 8601 format "
            "(default: backfill-start at midnight UTC)."
        ),
    )
    parser.add_argument(
        "--materialize-end",
        type=str,
        default=None,
        help=(
            "End time for full materialization in ISO 8601 format "
            "(default: backfill-end at midnight UTC)."
        ),
    )
    parser.add_argument(
        "--repo",
        type=Path,
        default=Path("feature_repo"),
        help="Path to the Feast repository (default: feature_repo).",
    )
    parser.add_argument(
        "--skip-incremental",
        action="store_true",
        help="Skip the final incremental materialization step.",
    )

    return parser


def main() -> None:
    """Run the complete FeatureForge pipeline."""
    parser = build_parser()
    args = parser.parse_args()

    output_dir: Path = args.output_dir
    offline_store_dir: Path = args.offline_store_dir

    data_dir = output_dir / "data"
    manifests_dir = output_dir / "manifests"

    output_dir.mkdir(parents=True, exist_ok=True)
    data_dir.mkdir(parents=True, exist_ok=True)
    manifests_dir.mkdir(parents=True, exist_ok=True)
    offline_store_dir.mkdir(parents=True, exist_ok=True)

    # 1. Generate deterministic synthetic source data.
    run_command(
        [
            "featureforge",
            "generate",
            "--config",
            str(args.config),
            "--output",
            str(data_dir),
        ],
        description="Generate synthetic source dataset",
    )

    # 2. Compute and persist canonical offline feature partitions.
    run_command(
        [
            "featureforge",
            "backfill",
            "--input",
            str(data_dir),
            "--output",
            str(offline_store_dir),
            "--start-date",
            args.backfill_start,
            "--end-date",
            args.backfill_end,
            "--window-days",
            str(args.window_days),
            "--engine",
            "pandas",
        ],
        description="Backfill offline features into canonical offline store",
    )

    # 3. Full materialization over the explicit UTC interval.
    materialize_start = (
        f"{args.backfill_start}T00:00:00+00:00"
        if args.materialize_start is None
        else args.materialize_start
    )
    materialize_end = (
        f"{args.backfill_end}T00:00:00+00:00"
        if args.materialize_end is None
        else args.materialize_end
    )

    run_command(
        [
            "featureforge",
            "materialize",
            "--repo",
            str(args.repo),
            "--start-time",
            materialize_start,
            "--end-time",
            materialize_end,
            "--manifest-output",
            str(output_dir),
        ],
        description="Materialize canonical offline features into online store (full)",
    )

    # 4. Optionally materialize records newer than Feast's stored watermark.
    if not args.skip_incremental:
        incremental_end = datetime.now(UTC).isoformat()

        run_command(
            [
                "featureforge",
                "materialize-incremental",
                "--repo",
                str(args.repo),
                "--end-time",
                incremental_end,
                "--manifest-output",
                str(output_dir),
            ],
            description="Run incremental materialization up to current UTC time",
        )

    print("\n[INFO] End-to-end pipeline completed successfully.")
    print(f"[INFO] Run artifacts: {output_dir}")
    print(f"[INFO] Source data: {data_dir}")
    print(f"[INFO] Offline serving features: {offline_store_dir}")
    print(f"[INFO] Run manifests: {manifests_dir}")


if __name__ == "__main__":
    main()
