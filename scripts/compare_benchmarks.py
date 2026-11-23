"""Compare two feature serving benchmark reports (ADR-016).

    python scripts/compare_benchmarks.py base.json new.json

Both files come from `scripts/benchmark_feature_serving.py`. The runs are grouped by target
rate, and the repeats of one rate are reduced to their median. A change counts only when it
is at least 10 percent and the ranges of the repeats of the two reports do not overlap. With
fewer than three repeats on one side the verdict is inconclusive. The script also says up to
which rate each report stays within the latency target. It exits with 1 when the new report
has entries that are not ok, beyond a small budget, at a rate where the base had none. Speed
never changes the exit code, because one machine varies from run to run. Compare reports
from the same machine only.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

MIN_CHANGE = 0.10
MIN_REPEATS = 3
SLO_P99_MS = 25.0
MAX_NOT_OK_RATIO = 0.001
HELD_FRACTION = 0.99
ENVIRONMENT_KEYS = ("platform", "cpu", "logical_cpus", "memory_bytes", "python")
SETUP_KEYS = ("entities", "entities_per_request", "budget_ms")

Row = dict[str, Any]


@dataclass(frozen=True)
class Report:
    path: Path
    label: str
    setup: dict[str, Any]
    environment: dict[str, Any]
    rows: dict[float, list[Row]]

    @property
    def backend(self) -> str:
        return str(self.setup.get("backend", "redis"))

    @property
    def commit(self) -> str:
        git = self.environment.get("git")
        if not git:
            return "unknown commit"
        state = "dirty tree" if git.get("dirty") else "clean tree"
        return f"{str(git.get('commit', 'unknown'))[:7]}, {state}"


@dataclass(frozen=True)
class Delta:
    base: float
    new: float
    change: float | None
    verdict: str


@dataclass(frozen=True)
class Capacity:
    within_slo: float | None
    first_outside: float | None
    max_achieved: float


def load_report(path: Path, label: str | None = None) -> Report:
    data = json.loads(path.read_text())
    rows: dict[float, list[Row]] = {}
    for row in data.get("results", []):
        rows.setdefault(float(row["target_rps"]), []).append(row)
    if not rows:
        raise ValueError(f"{path} has no results")
    return Report(
        path, label or path.stem, data.get("setup", {}), data.get("environment", {}), rows
    )


def achieved(row: Row) -> float:
    return float(row["achieved_rps"])


def p50(row: Row) -> float:
    return float(row["latency_ms"]["p50"])


def p99(row: Row) -> float:
    return float(row["latency_ms"]["p99"])


def cpu_ms_per_request(row: Row) -> float:
    return float(row["server_cpu_cores"]) / float(row["achieved_rps"]) * 1000.0


def not_ok_ratio(row: Row) -> float:
    total = row.get("entries_total") or 0
    return float(row.get("entries_not_ok", 0)) / total if total else 0.0


def series(rows: list[Row], getter: Callable[[Row], float]) -> list[float]:
    values: list[float] = []
    for row in rows:
        try:
            value = getter(row)
        except (KeyError, TypeError, ZeroDivisionError):
            continue
        if not math.isnan(value):
            values.append(value)
    return values


def _disjoint(first: list[float], second: list[float]) -> bool:
    return max(first) < min(second) or max(second) < min(first)


def compare_values(
    base: list[float],
    new: list[float],
    *,
    lower_is_better: bool,
    min_change: float = MIN_CHANGE,
    min_repeats: int = MIN_REPEATS,
) -> Delta | None:
    if not base or not new:
        return None
    base_median = statistics.median(base)
    new_median = statistics.median(new)
    change = (new_median - base_median) / base_median if base_median else None
    if len(base) < min_repeats or len(new) < min_repeats:
        verdict = "inconclusive"
    elif change is None or abs(change) < min_change or not _disjoint(base, new):
        verdict = "same"
    else:
        verdict = "better" if (change < 0) == lower_is_better else "worse"
    return Delta(base_median, new_median, change, verdict)


def rate_is_ok(target: float, rows: list[Row], slo_p99_ms: float, max_not_ok_ratio: float) -> bool:
    return all(
        int(row.get("errors", 0)) == 0
        and not_ok_ratio(row) <= max_not_ok_ratio
        and achieved(row) >= HELD_FRACTION * target
        and p99(row) <= slo_p99_ms
        for row in rows
    )


def capacity(
    report: Report,
    *,
    slo_p99_ms: float = SLO_P99_MS,
    max_not_ok_ratio: float = MAX_NOT_OK_RATIO,
) -> Capacity:
    within: float | None = None
    first_outside: float | None = None
    for target in sorted(report.rows):
        if rate_is_ok(target, report.rows[target], slo_p99_ms, max_not_ok_ratio):
            within = target
        else:
            first_outside = target
            break
    best = max(achieved(row) for rows in report.rows.values() for row in rows)
    return Capacity(within, first_outside, best)


def failures(base: Report, new: Report, max_not_ok_ratio: float = MAX_NOT_OK_RATIO) -> list[str]:
    found: list[str] = []
    for target in sorted(set(base.rows) & set(new.rows)):
        base_rows, new_rows = base.rows[target], new.rows[target]
        base_ratio = max(not_ok_ratio(row) for row in base_rows)
        new_ratio = max(not_ok_ratio(row) for row in new_rows)
        if new_ratio > max_not_ok_ratio >= base_ratio:
            found.append(
                f"{target:g} req/s: up to {new_ratio * 100:.2f} percent of the entries are "
                f"not ok in {new.label}, against {base_ratio * 100:.2f} percent in {base.label}."
            )
        new_errors = sum(int(row.get("errors", 0)) for row in new_rows)
        base_errors = sum(int(row.get("errors", 0)) for row in base_rows)
        if new_errors and not base_errors:
            found.append(
                f"{target:g} req/s: {new_errors} request errors in {new.label}, "
                f"none in {base.label}."
            )
    return found


def warnings(base: Report, new: Report) -> list[str]:
    found: list[str] = []
    for key in ENVIRONMENT_KEYS:
        before, after = base.environment.get(key), new.environment.get(key)
        if before != after:
            found.append(f"The environment differs in {key}: {before} and {after}.")
    for key in SETUP_KEYS:
        before, after = base.setup.get(key), new.setup.get(key)
        if before != after:
            found.append(f"The setup differs in {key}: {before} and {after}.")
    for report in (base, new):
        if (report.environment.get("git") or {}).get("dirty"):
            found.append(f"{report.label} was measured on a dirty working tree.")
    for report, other in ((base, new), (new, base)):
        missing = sorted(set(report.rows) - set(other.rows))
        if missing:
            found.append(
                f"Rates only in {report.label}: {', '.join(f'{rate:g}' for rate in missing)}."
            )
    return found


def fmt_ms(value: float) -> str:
    return f"{value / 1000:.2f} s" if value >= 1000 else f"{value:.2f}"


def fmt_rate(value: float) -> str:
    return f"{value:.0f}"


def fmt_cpu(value: float) -> str:
    return f"{value:.2f}"


def cell(delta: Delta | None, fmt: Callable[[float], str]) -> str:
    if delta is None:
        return "n/a"
    text = f"{fmt(delta.base)} to {fmt(delta.new)}"
    if delta.verdict in ("better", "worse") and delta.change is not None:
        size = (
            f"x{delta.new / delta.base:.1f}"
            if delta.change >= 1
            else f"{delta.change * 100:+.0f} %"
        )
        return f"{text} ({size}) {delta.verdict}"
    return f"{text} ({delta.verdict})"


COLUMNS: tuple[tuple[str, Callable[[Row], float], bool, Callable[[float], str]], ...] = (
    ("Achieved req/s", achieved, False, fmt_rate),
    ("p50 ms", p50, True, fmt_ms),
    ("p99 ms", p99, True, fmt_ms),
    ("CPU ms per request", cpu_ms_per_request, True, fmt_cpu),
)


def describe(report: Report, slo_p99_ms: float, max_not_ok_ratio: float) -> str:
    result = capacity(report, slo_p99_ms=slo_p99_ms, max_not_ok_ratio=max_not_ok_ratio)
    within = "no rate" if result.within_slo is None else f"up to {result.within_slo:g} req/s"
    outside = "none" if result.first_outside is None else f"{result.first_outside:g} req/s"
    return (
        f"- {report.label}: within the target {within}, first rate outside {outside}, "
        f"highest achieved {result.max_achieved:.0f} req/s."
    )


def render(
    base: Report,
    new: Report,
    *,
    slo_p99_ms: float = SLO_P99_MS,
    min_change: float = MIN_CHANGE,
    min_repeats: int = MIN_REPEATS,
    max_not_ok_ratio: float = MAX_NOT_OK_RATIO,
) -> str:
    lines = [
        "## Benchmark comparison",
        "",
        f"- Base: `{base.label}`, {base.backend}, commit {base.commit}, file `{base.path.name}`.",
        f"- New: `{new.label}`, {new.backend}, commit {new.commit}, file `{new.path.name}`.",
        (
            f"- Rule: medians of the repeats. A change counts when it is at least "
            f"{min_change * 100:.0f} percent and the ranges of the repeats do not overlap, "
            f"with at least {min_repeats} repeats per side."
        ),
        (
            f"- Latency target: p99 at most {slo_p99_ms:g} ms in every repeat, at most "
            f"{max_not_ok_ratio * 100:g} percent of the entries not ok, and at least "
            f"{HELD_FRACTION * 100:.0f} percent of the rate achieved."
        ),
        describe(base, slo_p99_ms, max_not_ok_ratio),
        describe(new, slo_p99_ms, max_not_ok_ratio),
    ]
    lines += [f"- Warning: {line}" for line in warnings(base, new)]
    header = ["Target req/s", *(name for name, *_ in COLUMNS), "Entries not ok"]
    lines += ["", "| " + " | ".join(header) + " |", "|" + "---|" * len(header)]
    for target in sorted(set(base.rows) & set(new.rows)):
        base_rows, new_rows = base.rows[target], new.rows[target]
        cells = [f"{target:g}"]
        for _, getter, lower_is_better, fmt in COLUMNS:
            delta = compare_values(
                series(base_rows, getter),
                series(new_rows, getter),
                lower_is_better=lower_is_better,
                min_change=min_change,
                min_repeats=min_repeats,
            )
            cells.append(cell(delta, fmt))
        before = sum(int(row.get("entries_not_ok", 0)) for row in base_rows)
        after = sum(int(row.get("entries_not_ok", 0)) for row in new_rows)
        cells.append(f"{before} to {after}")
        lines.append("| " + " | ".join(cells) + " |")
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compare two feature serving benchmark reports.")
    parser.add_argument("base", type=Path, help="report of the code before the change")
    parser.add_argument("new", type=Path, help="report of the code after the change")
    parser.add_argument("--label-base")
    parser.add_argument("--label-new")
    parser.add_argument("--min-change", type=float, default=MIN_CHANGE)
    parser.add_argument("--min-repeats", type=int, default=MIN_REPEATS)
    parser.add_argument("--slo-p99-ms", type=float, default=SLO_P99_MS)
    parser.add_argument("--max-not-ok-ratio", type=float, default=MAX_NOT_OK_RATIO)
    parser.add_argument("--output", type=Path, help="also write the Markdown to this file")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    base = load_report(args.base, args.label_base)
    new = load_report(args.new, args.label_new)
    text = render(
        base,
        new,
        slo_p99_ms=args.slo_p99_ms,
        min_change=args.min_change,
        min_repeats=args.min_repeats,
        max_not_ok_ratio=args.max_not_ok_ratio,
    )
    problems = failures(base, new, args.max_not_ok_ratio)
    text += "\n### Correctness\n\n"
    text += (
        "\n".join(f"- {line}" for line in problems) + "\n"
        if problems
        else ("No new entries that are not ok.\n")
    )
    print(text)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text)
    return 1 if problems else 0


if __name__ == "__main__":
    raise SystemExit(main())
