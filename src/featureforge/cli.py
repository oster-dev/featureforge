"""Command-line interface for FeatureForge."""

from __future__ import annotations

import argparse
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Literal

import pandas as pd
from rich.console import Console
from rich.table import Table

from featureforge.backfill import run_backfill
from featureforge.config import load_synthetic_data_config
from featureforge.feature_quality import check_offline_feature_freshness
from featureforge.features import compute_user_engagement_features
from featureforge.manifest import GenerationRunManifest
from featureforge.materialization import (
    _CANONICAL_OFFLINE_STORE_DIR,
    MaterializationBlockedError,
    materialize,
    materialize_incremental,
)
from featureforge.quality import validate_synthetic_dataset
from featureforge.storage import read_synthetic_dataset, write_synthetic_dataset
from featureforge.synthetic_data import generate_synthetic_dataset

# Opt-in to future pandas behavior to silence Dask/Feast downcasting warnings.
pd.set_option("future.no_silent_downcasting", True)


if TYPE_CHECKING:
    from pyspark.sql import SparkSession

BackfillEngine = Literal["pandas", "spark"]


def build_parser() -> argparse.ArgumentParser:
    """Build and return the FeatureForge command-line parser."""
    parser = argparse.ArgumentParser(
        prog="featureforge",
        description=(
            "Generate deterministic feature-store datasets, backfill features, "
            "validate offline freshness, and materialize online feature values."
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

    freshness_parser = subparsers.add_parser(
        "check-freshness",
        help="Check canonical offline feature-store freshness against a UTC reference time.",
    )
    freshness_parser.add_argument(
        "--reference-time",
        type=str,
        default=None,
        help=("UTC reference time in ISO 8601 format. Defaults to the current UTC time."),
    )
    freshness_parser.add_argument(
        "--max-lag-hours",
        type=int,
        default=24,
        help="Maximum allowed latest-partition lag in hours (default: 24).",
    )

    validate_spec_parser = subparsers.add_parser(
        "validate-spec",
        help="Validate feature spec YAML files against the spec model and event contract.",
    )
    validate_spec_parser.add_argument(
        "paths",
        nargs="*",
        type=Path,
        help=("Spec files to validate. Defaults to every YAML file in feature_repo/specs."),
    )

    test_spec_parser = subparsers.add_parser(
        "test-spec",
        help="Evaluate a feature spec on an event file with the reference implementation.",
    )
    test_spec_parser.add_argument(
        "spec",
        type=Path,
        help="Path to the feature spec YAML file.",
    )
    test_spec_parser.add_argument(
        "--events",
        type=Path,
        required=True,
        help="JSONL file with interaction events.",
    )
    test_spec_parser.add_argument(
        "--entity",
        action="append",
        required=True,
        metavar="KEY=VALUE",
        help="Entity to evaluate, for example user_id=user_000001. Repeat for composite keys.",
    )
    test_spec_parser.add_argument(
        "--at",
        required=True,
        help="Observation time in UTC, ISO 8601, for example 2026-01-01T12:00:00+00:00.",
    )
    test_spec_parser.add_argument(
        "--expect",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="Expected value of a spec output. Exit code 1 if it differs. Repeatable.",
    )

    backfill_spec_parser = subparsers.add_parser(
        "backfill-spec",
        help="Evaluate a feature spec for many entities and times with Spark and write Parquet.",
    )
    backfill_spec_parser.add_argument(
        "spec",
        type=Path,
        help="Path to the feature spec YAML file.",
    )
    backfill_spec_parser.add_argument(
        "--events",
        type=Path,
        required=True,
        help="JSONL file with interaction events.",
    )
    backfill_spec_parser.add_argument(
        "--observations",
        type=Path,
        required=True,
        help=(
            "JSONL file with one object per observation: the spec keys and "
            "observation_time in UTC, ISO 8601."
        ),
    )
    backfill_spec_parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="New directory for the Parquet result. An existing path is never overwritten.",
    )

    materialize_parser = subparsers.add_parser(
        "materialize",
        help="Materialize Feast offline feature values into the online store.",
    )
    materialize_parser.add_argument(
        "--repo",
        type=Path,
        default=Path("feature_repo"),
        help="Path to the Feast repository (default: feature_repo).",
    )
    materialize_parser.add_argument(
        "--start-time",
        type=str,
        required=True,
        help="Inclusive UTC start time in ISO 8601 format.",
    )
    materialize_parser.add_argument(
        "--end-time",
        type=str,
        required=True,
        help="Exclusive UTC end time in ISO 8601 format.",
    )
    materialize_parser.add_argument(
        "--manifest-output",
        type=Path,
        default=Path("output"),
        help="Directory where materialization manifests are written (default: output).",
    )

    incremental_materialize_parser = subparsers.add_parser(
        "materialize-incremental",
        help="Materialize feature values newer than Feast's stored watermark.",
    )
    incremental_materialize_parser.add_argument(
        "--repo",
        type=Path,
        default=Path("feature_repo"),
        help="Path to the Feast repository (default: feature_repo).",
    )
    incremental_materialize_parser.add_argument(
        "--end-time",
        type=str,
        required=True,
        help="UTC end time in ISO 8601 format.",
    )
    incremental_materialize_parser.add_argument(
        "--manifest-output",
        type=Path,
        default=Path("output"),
        help="Directory where materialization manifests are written (default: output).",
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


def parse_utc_datetime(value: str) -> datetime:
    """Parse an ISO 8601 timestamp and require an explicit timezone."""
    parsed = datetime.fromisoformat(value)

    if parsed.tzinfo is None:
        raise ValueError("Timestamp must include an explicit timezone, for example +00:00.")

    return parsed.astimezone(UTC)


def run_check_freshness_command(
    reference_time: str | None,
    max_lag_hours: int,
) -> None:
    """Check canonical offline feature-store freshness and return a process status."""
    if max_lag_hours < 0:
        raise ValueError("max_lag_hours must not be negative.")

    normalized_reference_time = (
        datetime.now(UTC) if reference_time is None else parse_utc_datetime(reference_time)
    )
    max_lag = timedelta(hours=max_lag_hours)
    report = check_offline_feature_freshness(
        _CANONICAL_OFFLINE_STORE_DIR,
        reference_time=normalized_reference_time,
        max_lag=max_lag,
    )

    console = Console()

    if report.passed:
        console.print("[green]✓[/green] Offline feature freshness check passed")
    else:
        console.print("[red]✗[/red] Offline feature freshness check failed")

    console.print(f"Offline store: {report.offline_store_dir}")
    console.print(f"Reference time: {report.reference_time.isoformat()}")
    console.print(f"Maximum lag: {max_lag}")

    for check in report.checks:
        status = "[green]✓[/green]" if check.passed else "[red]✗[/red]"
        console.print(f"{status} {check.name}: {check.message}")

    if not report.passed:
        console.print(f"Failed checks: {', '.join(report.failed_checks)}")
        raise SystemExit(1)


def run_materialize_command(
    repo_path: Path,
    start_time: str,
    end_time: str,
    manifest_output_dir: Path,
) -> None:
    """Materialize a full explicit Feast time range and print its manifest."""
    try:
        result = materialize(
            repo_path=repo_path,
            start_time=parse_utc_datetime(start_time),
            end_time=parse_utc_datetime(end_time),
            manifest_output_dir=manifest_output_dir,
        )
    except MaterializationBlockedError as exc:
        console = Console()
        console.print("[red]✗[/red] Feast materialization blocked by quality gate")
        console.print(f"Failed checks: {', '.join(exc.failed_checks)}")
        console.print(f"Blocked manifest: {exc.manifest_path}")
        raise SystemExit(1) from exc

    console = Console()
    console.print("[green]✓[/green] Feast full materialization completed")
    console.print(f"Repository: {result.repo_path}")
    start_text = result.start_time.isoformat() if result.start_time else "n/a"
    console.print(f"Start time: {start_text}")
    console.print(f"End time: {result.end_time.isoformat()}")
    console.print(f"Run manifest: {result.manifest_path}")


def run_materialize_incremental_command(
    repo_path: Path,
    end_time: str,
    manifest_output_dir: Path,
) -> None:
    """Materialize new Feast data up to an explicit UTC end time."""
    try:
        result = materialize_incremental(
            repo_path=repo_path,
            end_time=parse_utc_datetime(end_time),
            manifest_output_dir=manifest_output_dir,
        )
    except MaterializationBlockedError as exc:
        console = Console()
        console.print("[red]✗[/red] Feast materialization blocked by quality gate")
        console.print(f"Failed checks: {', '.join(exc.failed_checks)}")
        console.print(f"Blocked manifest: {exc.manifest_path}")
        raise SystemExit(1) from exc

    console = Console()
    console.print("[green]✓[/green] Feast incremental materialization completed")
    console.print(f"Repository: {result.repo_path}")
    console.print(f"End time: {result.end_time.isoformat()}")
    console.print(f"Run manifest: {result.manifest_path}")


_DEFAULT_SPEC_DIR = Path("feature_repo/specs")


def run_validate_spec_command(paths: list[Path]) -> int:
    """Validate feature specs and return a process status.

    Exit codes follow the spec loader: 0 valid, 1 invalid, 2 usage or missing file.
    """
    from featureforge.spec.loader import main as validate_specs

    spec_paths = paths or sorted(_DEFAULT_SPEC_DIR.glob("*.yaml"))
    if not spec_paths:
        Console().print(f"[red]✗[/red] No spec files found in {_DEFAULT_SPEC_DIR}")
        return 2
    return validate_specs([str(spec_path) for spec_path in spec_paths])


def _parse_pairs(items: list[str], flag: str) -> dict[str, str]:
    pairs: dict[str, str] = {}
    for item in items:
        key, separator, value = item.partition("=")
        if not separator or not key:
            raise ValueError(f"{flag} expects KEY=VALUE, got {item!r}")
        pairs[key] = value
    return pairs


def _read_events(path: Path) -> list:
    from pydantic import ValidationError

    from featureforge.streaming.contracts.events import InteractionEventV1

    events = []
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                events.append(InteractionEventV1.model_validate_json(line))
            except ValidationError as exc:
                first = exc.errors()[0]
                where = ".".join(str(part) for part in first["loc"]) or "<event>"
                raise ValueError(
                    f"{path.name}, line {line_number}: invalid event, {where}: {first['msg']}"
                ) from exc
    return events


def _values_match(actual: object, expected: str) -> bool:
    if expected.lower() == "none":
        return actual is None
    if isinstance(actual, int | float) and not isinstance(actual, bool):
        try:
            return abs(actual - float(expected)) < 1e-9
        except ValueError:
            return False
    return str(actual) == expected


def run_test_spec_command(
    spec_path: Path,
    events_path: Path,
    entity_args: list[str],
    at: str,
    expect_args: list[str],
) -> int:
    """Evaluate a spec on an event file with the reference and return a process status.

    Exit codes: 0 ok, 1 invalid input or a failed expectation, 2 a file is missing.
    """
    from pydantic import ValidationError
    from rich.markup import escape
    from rich.table import Table

    from featureforge.spec.loader import format_errors, load_spec
    from featureforge.spec.reference import EventIdentityConflictError, evaluate_spec

    console = Console()
    try:
        spec = load_spec(spec_path)
        entity = _parse_pairs(entity_args, "--entity")
        expected = _parse_pairs(expect_args, "--expect")
        observation_time = parse_utc_datetime(at)
        events = _read_events(events_path)
        result = evaluate_spec(spec, events, entity=entity, observation_time=observation_time)
    except FileNotFoundError as exc:
        console.print(f"[red]✗[/red] file not found: {exc.filename}", soft_wrap=True)
        return 2
    except ValidationError as exc:
        console.print(f"[red]✗[/red] {spec_path.name}", soft_wrap=True)
        for line in format_errors(exc):
            console.print(f"    {escape(line)}", soft_wrap=True)
        return 1
    except (ValueError, TypeError, EventIdentityConflictError) as exc:
        console.print(f"[red]✗[/red] {escape(str(exc))}", soft_wrap=True)
        return 1

    entity_text = ", ".join(f"{key}={value}" for key, value in entity.items())
    console.print(f"{escape(result.spec_name)} · {escape(entity_text)}", soft_wrap=True)
    console.print(
        f"Window ({result.window_start_exclusive.isoformat()}, "
        f"{result.observation_time.isoformat()}] · {len(events)} events read",
        soft_wrap=True,
    )
    table = Table(show_header=True, header_style="bold")
    table.add_column("value")
    table.add_column("result")
    for name, value in result.values.items():
        table.add_row(escape(name), escape(str(value)))
    console.print(table)

    failed = False
    for name, wanted in expected.items():
        if name not in result.values:
            console.print(
                f"[red]✗[/red] {escape(name)}: not an output of this spec, "
                f"available: {escape(str(sorted(result.values)))}",
                soft_wrap=True,
            )
            failed = True
            continue
        actual = result.values[name]
        if _values_match(actual, wanted):
            console.print(
                f"[green]✓[/green] {escape(name)} = {escape(str(actual))}", soft_wrap=True
            )
        else:
            console.print(
                f"[red]✗[/red] {escape(name)}: expected {escape(wanted)}, got {escape(str(actual))}",
                soft_wrap=True,
            )
            failed = True
    return 1 if failed else 0


def _read_observations(path: Path, keys: list[str]) -> list[tuple[dict[str, str], datetime]]:
    import json

    allowed = {*keys, "observation_time"}
    pairs: list[tuple[dict[str, str], datetime]] = []
    with open(path, encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            where = f"{path.name}, line {line_number}"
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{where}: invalid JSON, {exc.msg}") from exc
            if not isinstance(record, dict) or set(record) != allowed:
                raise ValueError(f"{where}: expected exactly the keys {sorted(allowed)}")
            entity = {key: record[key] for key in keys}
            for key, value in entity.items():
                if not isinstance(value, str) or not value or any(c.isspace() for c in value):
                    raise ValueError(
                        f"{where}: {key} must be a non-empty string without whitespace"
                    )
            moment = record["observation_time"]
            if not isinstance(moment, str):
                raise ValueError(f"{where}: observation_time must be a string")
            try:
                pairs.append((entity, parse_utc_datetime(moment)))
            except ValueError as exc:
                raise ValueError(f"{where}: {exc}") from exc
    if not pairs:
        raise ValueError(f"{path.name}: no observations")
    return pairs


def run_backfill_spec_command(
    spec_path: Path,
    events_path: Path,
    observations_path: Path,
    output_path: Path,
    spark: SparkSession | None = None,
) -> int:
    """Evaluate a spec for many entities and times with Spark and write Parquet.

    Exit codes: 0 ok, 1 invalid input, identity conflict, unsupported operator or an
    existing output path, 2 a file is missing or PySpark is not installed.
    """
    from pydantic import ValidationError
    from rich.markup import escape

    from featureforge.spec.loader import format_errors, load_spec
    from featureforge.spec.reference import EventIdentityConflictError

    console = Console()
    try:
        spec = load_spec(spec_path)
        events = _read_events(events_path)
        pairs = _read_observations(observations_path, spec.keys)
    except FileNotFoundError as exc:
        console.print(f"[red]✗[/red] file not found: {exc.filename}", soft_wrap=True)
        return 2
    except ValidationError as exc:
        console.print(f"[red]✗[/red] {spec_path.name}", soft_wrap=True)
        for line in format_errors(exc):
            console.print(f"    {escape(line)}", soft_wrap=True)
        return 1
    except (ValueError, TypeError) as exc:
        console.print(f"[red]✗[/red] {escape(str(exc))}", soft_wrap=True)
        return 1

    if output_path.exists():
        console.print(
            f"[red]✗[/red] output already exists: {escape(str(output_path))}", soft_wrap=True
        )
        return 1

    try:
        from featureforge.spec.spark_backfill import (
            backfill_spec,
            create_local_session,
            events_to_frame,
            observations_to_frame,
        )
    except ImportError:
        console.print(
            "[red]✗[/red] PySpark is not installed. Run: "
            + escape("pip install 'featureforge[spark]'"),
            soft_wrap=True,
        )
        return 2

    owns_session = spark is None
    session = create_local_session() if spark is None else spark
    try:
        result = backfill_spec(
            spec,
            events_to_frame(session, events),
            observations_to_frame(session, spec, pairs),
        )
        session.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        result.write.mode("error").parquet(str(output_path))
        rows = session.read.parquet(str(output_path)).count()
    except (ValueError, TypeError, NotImplementedError, EventIdentityConflictError) as exc:
        console.print(f"[red]✗[/red] {escape(str(exc))}", soft_wrap=True)
        return 1
    finally:
        if owns_session:
            session.stop()

    console.print(
        f"[green]✓[/green] {escape(spec.name)}: {rows} rows written to {escape(str(output_path))}",
        soft_wrap=True,
    )
    return 0


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
    elif args.command == "check-freshness":
        run_check_freshness_command(
            reference_time=args.reference_time,
            max_lag_hours=args.max_lag_hours,
        )
    elif args.command == "backfill-spec":
        raise SystemExit(
            run_backfill_spec_command(args.spec, args.events, args.observations, args.output)
        )
    elif args.command == "validate-spec":
        raise SystemExit(run_validate_spec_command(args.paths))
    elif args.command == "test-spec":
        raise SystemExit(
            run_test_spec_command(
                spec_path=args.spec,
                events_path=args.events,
                entity_args=args.entity,
                at=args.at,
                expect_args=args.expect,
            )
        )
    elif args.command == "materialize":
        run_materialize_command(
            repo_path=args.repo,
            start_time=args.start_time,
            end_time=args.end_time,
            manifest_output_dir=args.manifest_output,
        )
    elif args.command == "materialize-incremental":
        run_materialize_incremental_command(
            repo_path=args.repo,
            end_time=args.end_time,
            manifest_output_dir=args.manifest_output,
        )


if __name__ == "__main__":
    main()
