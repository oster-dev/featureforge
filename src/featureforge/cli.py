from __future__ import annotations

import argparse
from pathlib import Path

from rich.console import Console
from rich.table import Table

from featureforge.config import load_synthetic_data_config
from featureforge.storage import write_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="featureforge",
        description="Generate deterministic synthetic feature-store datasets.",
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
        help="Directory where Parquet files will be written.",
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


def run_generate(config_path: Path, output_dir: Path) -> None:
    config = load_synthetic_data_config(config_path)
    dataset = generate_synthetic_dataset(config)
    output_paths = write_synthetic_dataset(dataset, output_dir)

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
    )


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "generate":
        run_generate(args.config, args.output)
