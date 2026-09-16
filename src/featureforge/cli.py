from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
from rich.console import Console
from rich.table import Table

from featureforge.config import load_synthetic_data_config
from featureforge.features import compute_user_engagement_features
from featureforge.manifest import GenerationRunManifest
from featureforge.quality import validate_synthetic_dataset
from featureforge.storage import read_synthetic_dataset, write_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="featureforge",
        description="Generate deterministic synthetic feature-store datasets.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # generate command
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
        help="Directory where Parquet files will be written.",
    )

    # compute-features command
    compute_parser = subparsers.add_parser(
        "compute-features",
        help="Compute user engagement features from a generated dataset.",
    )
    compute_parser.add_argument(
        "--input",
        type=Path,
        required=True,
        help="Directory containing the generated Parquet files.",
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
        help="Observation time for feature computation (ISO 8601 format).",
    )
    compute_parser.add_argument(
        "--window-days",
        type=int,
        default=7,
        help="Lookback window in days (default: 7).",
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
    config = load_synthetic_data_config(config_path)
    dataset = generate_synthetic_dataset(config)

    # Quality validation
    quality_report = validate_synthetic_dataset(dataset, config)

    # Persist Parquet files
    output_paths = write_synthetic_dataset(dataset, output_dir)

    # Write run manifest
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
    """Compute and persist user engagement features."""
    dataset = read_synthetic_dataset(input_dir)
    obs_time = datetime.fromisoformat(observation_time).replace(tzinfo=UTC)

    batch = compute_user_engagement_features(
        dataset,
        observation_time=obs_time,
        window_days=window_days,
    )

    # Write features as Parquet
    output_dir.mkdir(parents=True, exist_ok=True)
    features_df = pd.DataFrame([f.model_dump() for f in batch.features])
    features_df.to_parquet(output_dir / "user_engagement_features.parquet", index=False)

    console = Console()
    console.print(f"[green]✓[/green] Computed features for {len(batch.features)} users")
    console.print(f"Observation time: {obs_time}")
    console.print(f"Window: {batch.window_days} days")
    console.print(f"Output: {output_dir / 'user_engagement_features.parquet'}")


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "generate":
        run_generate(args.config, args.output)
    elif args.command == "compute-features":
        run_compute_features(
            args.input,
            args.output,
            args.observation_time,
            args.window_days,
        )
