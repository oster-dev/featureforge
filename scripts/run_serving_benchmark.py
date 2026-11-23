"""Run the feature serving benchmark with a preset and safety checks (ADR-016).

    python scripts/run_serving_benchmark.py smoke --backend redis
    python scripts/run_serving_benchmark.py record --backend cassandra

`smoke` uses 2,000 users, two rates and 5 s per run. It shows that the tool and the backend
work and gives no numbers to publish. `record` uses 1,000,000 users, a warm-up of 30 s and three
runs of 30 s per rate. It needs a clean working tree and only the services of the backend, it
seeds fresh data, and with Cassandra it flushes the table and waits until no compaction is
pending. The report goes to `output/benchmarks/`, and the script says how to keep it. The
measurement itself is `scripts/benchmark_feature_serving.py`.
"""

from __future__ import annotations

import argparse
import re
import shlex
import shutil
import subprocess
import sys
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BENCHMARK = ROOT / "scripts" / "benchmark_feature_serving.py"
OUTPUT_DIR = ROOT / "output" / "benchmarks"
KEYSPACE = "ff_loadtest"
SMOKE_KEYSPACE = "ff_loadtest_smoke"
REQUIRED_SERVICES = {"redis": {"redis"}, "cassandra": {"cassandra"}}
ALLOWED_SERVICES = {"redis": {"redis"}, "cassandra": {"cassandra", "redis"}}
SETTLE_SECONDS = 30
COMPACTION_TIMEOUT = 900


@dataclass(frozen=True)
class Preset:
    entities: int
    rates: dict[str, str]
    repeats: int
    duration: int
    warmup: int
    strict: bool


PRESETS = {
    "smoke": Preset(2_000, {"redis": "200,400", "cassandra": "200,400"}, 1, 5, 2, False),
    "record": Preset(
        1_000_000,
        {"redis": "500,1000,2000,3000,4000", "cassandra": "500,1000,1500,2000,2500"},
        3,
        30,
        30,
        True,
    ),
}


@dataclass(frozen=True)
class Finding:
    level: str
    message: str


def check_environment(
    preset: Preset,
    backend: str,
    *,
    dirty: list[str],
    services: set[str] | None,
) -> list[Finding]:
    level = "error" if preset.strict else "warning"
    found: list[Finding] = []
    if dirty:
        found.append(
            Finding(
                level,
                f"The working tree has {len(dirty)} uncommitted changes. "
                "The report would say 'dirty tree'.",
            )
        )
    if services is not None:
        for name in sorted(REQUIRED_SERVICES[backend] - services):
            found.append(
                Finding(
                    "error",
                    f"The service {name} is not running. Start it with: "
                    f"docker compose up -d {name}",
                )
            )
        extra = sorted(services - ALLOWED_SERVICES[backend])
        if extra:
            found.append(
                Finding(
                    level,
                    f"These services are running and disturb the measurement: {', '.join(extra)}. "
                    f"Stop them with: docker compose stop {' '.join(extra)}",
                )
            )
    return found


def uncommitted_changes() -> list[str]:
    result = subprocess.run(
        ["git", "status", "--porcelain"], cwd=ROOT, capture_output=True, text=True, check=True
    )
    return [line for line in result.stdout.splitlines() if line.strip()]


def head_commit() -> str:
    result = subprocess.run(
        ["git", "rev-parse", "--short", "HEAD"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def running_services() -> set[str] | None:
    if shutil.which("docker") is None:
        return None
    result = subprocess.run(
        ["docker", "compose", "ps", "--services", "--status", "running"],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return None
    return {line.strip() for line in result.stdout.splitlines() if line.strip()}


def sleep_inhibitor() -> list[str]:
    if shutil.which("caffeinate"):
        return ["caffeinate", "-i"]
    if shutil.which("systemd-inhibit"):
        return ["systemd-inhibit", "--what=idle:sleep", "--why=featureforge benchmark"]
    return []


def benchmark_command(
    backend: str,
    *,
    entities: int,
    rates: str,
    repeats: int,
    duration: int,
    warmup: int,
    output: Path,
    reseed: bool,
    inhibitor: Sequence[str],
    extra: Sequence[str] = (),
) -> list[str]:
    command = [
        *inhibitor,
        sys.executable,
        str(BENCHMARK),
        "--backend",
        backend,
        "--entities",
        str(entities),
        "--rates",
        rates,
        "--repeats",
        str(repeats),
        "--duration",
        str(duration),
        "--warmup",
        str(warmup),
        "--output",
        str(output),
        *extra,
    ]
    if reseed:
        command.append("--reseed")
    return command


def parse_compactionstats(text: str) -> tuple[int, int] | None:
    pending = re.search(r"pending tasks\s+(\d+)", text)
    completed = re.search(r"compactions completed\s+(\d+)", text)
    if pending is None or completed is None:
        return None
    return int(pending.group(1)), int(completed.group(1))


def nodetool(*arguments: str) -> str:
    result = subprocess.run(
        ["docker", "compose", "exec", "-T", "cassandra", "nodetool", *arguments],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout


def settle_cassandra() -> None:
    print("Flushing the table and waiting until no compaction is pending ...", file=sys.stderr)
    nodetool("flush", KEYSPACE)
    deadline = time.monotonic() + COMPACTION_TIMEOUT
    quiet_since: float | None = None
    last_completed = -1
    while time.monotonic() < deadline:
        stats = parse_compactionstats(nodetool("compactionstats"))
        if stats is None or stats[0] != 0:
            quiet_since = None
        elif stats[1] != last_completed or quiet_since is None:
            last_completed = stats[1]
            quiet_since = time.monotonic()
        elif time.monotonic() - quiet_since >= SETTLE_SECONDS:
            return
        time.sleep(5)
    raise SystemExit("Cassandra still compacts after 15 minutes. Wait and run again.")


def run(command: list[str]) -> None:
    print("+ " + shlex.join(command), file=sys.stderr)
    subprocess.run(command, cwd=ROOT, check=True)


def output_path(preset: str, backend: str, commit: str) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    return OUTPUT_DIR / f"{preset}-{backend}-{commit}-{stamp}.json"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the feature serving benchmark with a preset.")
    parser.add_argument("preset", choices=sorted(PRESETS))
    parser.add_argument("--backend", choices=("redis", "cassandra"), default="redis")
    parser.add_argument("--rates", help="replace the rates of the preset, for example 500,1000")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--force", action="store_true", help="continue despite errors")
    parser.add_argument("--dry-run", action="store_true", help="print the plan, run nothing")
    parser.add_argument(
        "--no-compose-check", action="store_true", help="skip the check of the Compose services"
    )
    parser.add_argument(
        "--no-inhibit", action="store_true", help="do not keep the machine from sleeping"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    preset = PRESETS[args.preset]
    rates = args.rates or preset.rates[args.backend]
    services = None if args.no_compose_check else running_services()
    if services is None and not args.no_compose_check:
        print(
            "NOTE: the Compose services could not be read, so that check was skipped.",
            file=sys.stderr,
        )
    findings = check_environment(
        preset, args.backend, dirty=uncommitted_changes(), services=services
    )
    for finding in findings:
        print(f"{finding.level.upper()}: {finding.message}", file=sys.stderr)
    if any(finding.level == "error" for finding in findings) and not args.force:
        print("Stopped. Fix the errors above or use --force.", file=sys.stderr)
        return 2

    commit = head_commit()
    output = args.output or output_path(args.preset, args.backend, commit)
    inhibitor = [] if args.no_inhibit else sleep_inhibitor()
    seed_first = preset.strict and args.backend == "cassandra"
    extra: list[str] = []
    if args.preset == "smoke" and args.backend == "cassandra":
        extra = ["--cassandra-keyspace", SMOKE_KEYSPACE]
    plan: list[tuple[str, list[str]]] = []
    if seed_first:
        seed_output = Path(tempfile.gettempdir()) / "ff-seed.json"
        plan.append(
            (
                "seed",
                benchmark_command(
                    args.backend,
                    entities=preset.entities,
                    rates="100",
                    repeats=1,
                    duration=5,
                    warmup=2,
                    output=seed_output,
                    reseed=True,
                    inhibitor=inhibitor,
                ),
            )
        )
        plan.append(("settle", []))
    plan.append(
        (
            "measure",
            benchmark_command(
                args.backend,
                entities=preset.entities,
                rates=rates,
                repeats=preset.repeats,
                duration=preset.duration,
                warmup=preset.warmup,
                output=output,
                reseed=not seed_first,
                inhibitor=inhibitor,
                extra=extra,
            ),
        )
    )
    if args.dry_run:
        for step, command in plan:
            text = shlex.join(command) if command else "flush the table, wait for compaction"
            print(f"{step}: {text}")
        return 0

    output.parent.mkdir(parents=True, exist_ok=True)
    for step, command in plan:
        if step == "settle":
            settle_cassandra()
        else:
            run(command)
    print(f"\nReport: {output}", file=sys.stderr)
    if preset.strict:
        print(
            "To keep it: cp the file to docs/benchmarks/<topic>-<backend>-<commit>.json.\n"
            "To compare two reports: make bench-compare BASE=<a.json> NEW=<b.json>",
            file=sys.stderr,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
