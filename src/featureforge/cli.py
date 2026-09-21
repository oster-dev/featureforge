"""Command-line interface for FeatureForge."""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from rich.console import Console
from rich.table import Table

from featureforge.backfill import run_backfill
from featureforge.config import load_synthetic_data_config
from featureforge.features import compute_user_engagement_features
from featureforge.manifest import GenerationRunManifest
from featureforge.quality import validate_synthetic_dataset
from featureforge.storage import read_synthetic_dataset, write_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset

BackfillEngine = Literal["pandas", "spark"]


def build_parser() -> argparse.ArgumentParser:
    """Build and return the FeatureForge command-line parser."""
    parser = argparse.ArgumentParser(
        prog="featureforge",
        description=(
            "Generate deterministic synthetic feature-store datasets and backfill features."
        ),
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    generate_parser = subparsers.add_parser(
        "generate",
        help="Generate and persist a synthetic dataset as Parquet files.",
    )
    generate_parser.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the synthetic data YAML configuration file.",
    )
    generate_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Directory where source Parquet files will be written.",
    )

    compute_parser = subparsers.add_parser(
        "compute-features",
        help="Compute user engagement features from a generated dataset.",
    )
    compute_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Directory containing the generated source Parquet files.",
    )
    compute_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Directory where feature Parquet files will be written.",
    )
    compute_parser.add_argument(
        "--observation-time",
        type=str,
        required=True,
        help="Observation time for feature computation in ISO 8601 format.",
    )
    compute_parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Lookback window in days (default: 7).",
    )

    backfill_parser = subparsers.add_parser(
        "backfill",
        help="Compute and persist point-in-time feature batches for a date range.",
    )
    backfill_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Directory containing the generated source Parquet files.",
    )
    backfill_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Root directory for partitioned feature datasets and manifests.",
    )
    backfill_parser.add_argument(
        "--start-date",
        type=date.fromisoformat,
        required=True,
        help="First observation date, inclusive, in YYYY-MM-DD format.",
    )
    backfill_parser.add_argument(
        "--end-date",
        type=date.fromisoformat,
        required=True,
        help="Last observation date, inclusive, in YYYY-MM-DD format.",
    )
    backfill_parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Lookback window in days (default: 7).",
    )
    backfill_parser.add_argument(
        "--engine",
        choices=("pandas", "spark"),
        default="pandas",
        help="Feature-computation engine (default: pandas).",
    )

    return parser


def print_generation_summary(
    console: Console,
    output_paths: dict[str, Path],
    user_count: int,
    content_count: int,
    event_count: int,
    duplicate_count: int,
    late_event_count: int,
    label_count: int,
    manifest_path: Path | None = None,
) -> None:
    """Print a summary of a completed synthetic-data generation run."""
    table = Table(title="FeatureForge dataset generated")
    table.add_column("Table", style="cyan")
    table.add_column("Rows", justify="right", style="green")
    table.add_column("Path", style="dim")

    table.add_row("users", str(user_count), str(output_paths["users"]))
    table.add_row("content", str(content_count), str(output_paths["content"]))
    table.add_row("events", str(event_count), str(output_paths["events"]))
    table.add_row("labels", str(label_count), str(output_paths["labels"]))

    console.print(table)
    console.print(f"Duplicate events: {duplicate_count}")
    console.print(f"Late events: {late_event_count}")

    if manifest_path is not None:
        console.print(f"Run manifest: {manifest_path}")


def run_generate(config_path: Path, output_dir: Path) -> None:
    """Generate, validate, persist, and describe a synthetic dataset."""
    config = load_synthetic_data_config(config_path)
    dataset = generate_synthetic_dataset(config)

    quality_report = validate_synthetic_dataset(dataset, config)
    output_paths = write_synthetic_dataset(dataset, output_dir)

    manifest = GenerationRunManifest.from_run(
        config=config,
        dataset=dataset,
        quality_report=quality_report,
        output_paths=output_paths,
    )
    manifest_path = manifest.write(output_dir)

    console = Console()
    print_generation_summary(
        console=console,
        output_paths=output_paths,
        user_count=len(dataset.users),
        content_count=len(dataset.content_items),
        event_count=len(dataset.events),
        duplicate_count=sum(event.is_duplicate for event in dataset.events),
        late_event_count=sum(event.is_late for event in dataset.events),
        label_count=len(dataset.labels),
        manifest_path=manifest_path,
    )


def run_compute_features(
    input_dir: Path,
    output_dir: Path,
    observation_time: str,
    window_days: int,
) -> None:
    """Compute and persist user engagement features for one observation time."""
    dataset = read_synthetic_dataset(input_dir)
    observation_datetime = datetime.fromisoformat(observation_time).replace(tzinfo=UTC)

    batch = compute_user_engagement_features(
        dataset,
        observation_time=observation_datetime,
        window_days=window_days,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    features_path = output_dir / "user_engagement_features.parquet"
    features_df = pd.DataFrame([feature.model_dump() for feature in batch.features])
    features_df.to_parquet(features_path, index=False)

    console = Console()
    console.print(f"[green]✓[/green] Computed features for {len(batch.features)} users")
    console.print(f"Observation time: {observation_datetime.isoformat()}")
    console.print(f"Window: {batch.window_days} days")
    console.print(f"Output: {features_path}")


def run_backfill_command(
    input_dir: Path,
    output_dir: Path,
    start_date: date,
    end_date: date,
    window_days: int,
    engine: BackfillEngine,
) -> None:
    """Run a date-parameterized offline feature backfill."""
    # run_backfill still receives the dataset for its current API contract.
    # In Spark mode, feature computation itself reads source Parquet from input_dir.
    dataset = read_synthetic_dataset(input_dir)

    results, manifest_path = run_backfill(
        dataset=dataset,
        start_date=start_date,
        end_date=end_date,
        window_days=window_days,
        output_dir=output_dir,
        engine=engine,
        input_dir=input_dir if engine == "spark" else None,
    )

    console = Console()
    console.print(f"[green]✓[/green] Backfilled {len(results)} observation-date partition(s)")
    console.print(f"Date range: {start_date.isoformat()} to {end_date.isoformat()}")
    console.print(f"Window: {window_days} days")
    console.print(f"Engine: {engine}")
    console.print(f"Output: {output_dir}")
    console.print(f"Run manifest: {manifest_path}")


def main() -> None:
    """Parse command-line arguments and run the selected command."""
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "generate":
        run_generate(args.config, args.output)
    elif args.command == "compute-features":
        run_compute_features(
            input_dir=args.input,
            output_dir=args.output,
            observation_time=args.observation_time,
            window_days=args.window_days,
        )
    elif args.command == "backfill":
        run_backfill_command(
            input_dir=args.input,
            output_dir=args.output,
            start_date=args.start_date,
            end_date=args.end_date,
            window_days=args.window_days,
            engine=args.engine,
        )


if __name__ == "__main__":
    main()
