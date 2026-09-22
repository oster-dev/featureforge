#!/usr/bin/env python3
"""End-to-end orchestration for FeatureForge.

This script runs a complete feature pipeline:
1. Generate synthetic source data
2. Backfill offline features for a date range
3. Materialize features into the online store (Redis)
4. Optionally run an incremental materialization

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
    """Run a featureforge CLI command and print a status line."""
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
        help="Root directory for all generated artifacts.",
    )

    parser.add_argument(
        "--backfill-start",
        type=str,
        default="2026-03-20",
        help="First backfill date (inclusive) in YYYY-MM-DD format.",
    )

    parser.add_argument(
        "--backfill-end",
        type=str,
        default="2026-03-25",
        help="Last backfill date (inclusive) in YYYY-MM-DD format.",
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
            "End time for materialization in ISO 8601 format "
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
    """Run the end-to-end feature pipeline."""
    parser = build_parser()
    args = parser.parse_args()

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    data_dir = output_dir / "data"
    features_dir = output_dir / "features"
    manifests_dir = output_dir / "manifests"

    data_dir.mkdir(exist_ok=True)
    features_dir.mkdir(exist_ok=True)
    manifests_dir.mkdir(exist_ok=True)

    # 1. Generate synthetic dataset
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

    # 2. Backfill offline features
    run_command(
        [
            "featureforge",
            "backfill",
            "--input",
            str(data_dir),
            "--output",
            str(features_dir),
            "--start-date",
            args.backfill_start,
            "--end-date",
            args.backfill_end,
            "--window-days",
            str(args.window_days),
            "--engine",
            "pandas",
        ],
        description="Backfill offline features for date range",
    )

    # 3. Full materialization
    if args.materialize_start is None:
        materialize_start = f"{args.backfill_start}T00:00:00+00:00"
    else:
        materialize_start = args.materialize_start

    if args.materialize_end is None:
        materialize_end = f"{args.backfill_end}T00:00:00+00:00"
    else:
        materialize_end = args.materialize_end

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
        description="Materialize features into online store (full)",
    )

    # 4. Optional incremental materialization
    if not args.skip_incremental:
        now_utc = datetime.now(UTC)
        incremental_end = now_utc.isoformat()

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
    print(f"[INFO] Artifacts written to: {output_dir}")
    print(f"[INFO] Data: {data_dir}")
    print(f"[INFO] Features: {features_dir}")
    print(f"[INFO] Manifests: {manifests_dir}")


if __name__ == "__main__":
    main()
