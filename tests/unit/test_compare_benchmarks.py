"""Tests for the comparison of feature serving benchmark reports (ADR-016)."""

import importlib.util
import json
import sys
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "compare_benchmarks.py"
DOCS = ROOT / "docs" / "benchmarks"
_spec = importlib.util.spec_from_file_location("compare_benchmarks", SCRIPT)
assert _spec is not None and _spec.loader is not None
compare = importlib.util.module_from_spec(_spec)
sys.modules["compare_benchmarks"] = compare
_spec.loader.exec_module(compare)


def row(
    target: int,
    achieved: float | None = None,
    p50: float = 2.0,
    p99: float = 10.0,
    cores: float = 0.5,
    not_ok: int = 0,
    errors: int = 0,
) -> dict[str, Any]:
    return {
        "target_rps": target,
        "achieved_rps": target if achieved is None else achieved,
        "errors": errors,
        "entries_total": target * 30 * 10,
        "entries_not_ok": not_ok,
        "latency_ms": {"p50": p50, "p99": p99},
        "server_cpu_cores": cores,
    }


def write(
    tmp_path: Path,
    name: str,
    rows: list[dict[str, Any]],
    environment: dict[str, Any] | None = None,
    setup: dict[str, Any] | None = None,
) -> Path:
    data = {
        "setup": setup or {"backend": "redis", "entities": 1000},
        "environment": environment or {"cpu": "M4", "git": {"commit": "a" * 40, "dirty": False}},
        "results": rows,
    }
    path = tmp_path / f"{name}.json"
    path.write_text(json.dumps(data))
    return path


def report_of(tmp_path: Path, name: str, rows: list[dict[str, Any]], **kwargs: Any) -> Any:
    return compare.load_report(write(tmp_path, name, rows, **kwargs))


def test_a_change_needs_ten_percent_and_ranges_that_do_not_overlap():
    worse = compare.compare_values([10.0, 10.2, 10.1], [12.0, 12.1, 12.2], lower_is_better=True)
    small = compare.compare_values([10.0, 10.2, 10.1], [10.5, 10.6, 10.55], lower_is_better=True)
    overlapping = compare.compare_values(
        [8.0, 10.0, 12.0], [11.0, 13.0, 15.0], lower_is_better=True
    )

    assert worse.verdict == "worse"
    assert worse.change == pytest.approx(0.2, abs=0.01)
    assert small.verdict == "same"
    assert overlapping.verdict == "same"


def test_the_direction_decides_between_better_and_worse():
    faster = compare.compare_values([10.0, 10.1, 10.2], [8.0, 8.1, 8.2], lower_is_better=True)
    more = compare.compare_values(
        [1000.0, 1001.0, 999.0], [1200.0, 1190.0, 1210.0], lower_is_better=False
    )
    fewer = compare.compare_values(
        [1200.0, 1190.0, 1210.0], [1000.0, 1001.0, 999.0], lower_is_better=False
    )

    assert (faster.verdict, more.verdict, fewer.verdict) == ("better", "better", "worse")


def test_fewer_than_three_repeats_are_inconclusive_and_empty_series_have_no_delta():
    single = compare.compare_values([1.0], [2.0], lower_is_better=True)

    assert single.verdict == "inconclusive"
    assert compare.compare_values([], [1.0, 2.0, 3.0], lower_is_better=True) is None


def test_capacity_stops_at_the_first_rate_outside_the_target(tmp_path):
    rows = [row(500, p99=5.0), row(1000, p99=8.0), row(2000, p99=40.0), row(3000, p99=5.0)]

    result = compare.capacity(report_of(tmp_path, "a", rows))

    assert result.within_slo == 1000
    assert result.first_outside == 2000
    assert result.max_achieved == 3000


@pytest.mark.parametrize(
    "bad",
    [row(1000, p99=30.0), row(1000, achieved=900), row(1000, not_ok=1000), row(1000, errors=1)],
)
def test_a_rate_is_outside_the_target_for_each_kind_of_miss(tmp_path, bad):
    result = compare.capacity(report_of(tmp_path, "a", [row(500), bad]))

    assert (result.within_slo, result.first_outside) == (500, 1000)


def test_a_small_blip_of_entries_that_are_not_ok_stays_within_the_budget(tmp_path):
    report = report_of(tmp_path, "a", [row(500, not_ok=10), row(1000)])

    assert compare.capacity(report).within_slo == 1000


def test_the_worst_repeat_decides_the_capacity(tmp_path):
    report = report_of(tmp_path, "a", [row(1000, p99=10.0), row(1000, p99=30.0)])

    assert compare.capacity(report).within_slo is None


def test_new_entries_that_are_not_ok_fail_only_where_the_base_had_none(tmp_path):
    base = report_of(tmp_path, "base", [row(1000), row(2000, not_ok=5000)])
    new = report_of(tmp_path, "new", [row(1000, not_ok=5000), row(2000, not_ok=5000)])

    problems = compare.failures(base, new)

    assert len(problems) == 1
    assert problems[0].startswith("1000 req/s")


def test_request_errors_that_the_base_did_not_have_are_a_failure(tmp_path):
    base = report_of(tmp_path, "base", [row(1000)])
    new = report_of(tmp_path, "new", [row(1000, errors=3)])

    assert "3 request errors" in compare.failures(base, new)[0]


def test_warnings_name_other_machines_dirty_trees_and_missing_rates(tmp_path):
    base = report_of(
        tmp_path,
        "base",
        [row(500), row(1000)],
        environment={"cpu": "M4", "git": {"commit": "a" * 40, "dirty": False}},
    )
    new = report_of(
        tmp_path,
        "new",
        [row(500)],
        environment={"cpu": "Xeon", "git": {"commit": "b" * 40, "dirty": True}},
        setup={"backend": "redis", "entities": 2000},
    )

    text = "\n".join(compare.warnings(base, new))

    assert "differs in cpu" in text
    assert "differs in entities" in text
    assert "new was measured on a dirty working tree" in text
    assert "Rates only in base: 1000" in text


def test_main_prints_the_table_and_returns_zero_without_new_failures(tmp_path, capsys):
    base = write(tmp_path, "base", [row(500, p50=2.0), row(500, p50=2.1), row(500, p50=2.2)])
    new = write(tmp_path, "new", [row(500, p50=3.0), row(500, p50=3.1), row(500, p50=3.2)])
    output = tmp_path / "out" / "report.md"

    code = compare.main([str(base), str(new), "--output", str(output)])

    text = capsys.readouterr().out
    assert code == 0
    assert "| 500 |" in text
    assert "worse" in text
    assert output.read_text().strip() == text.strip()


def test_main_returns_one_when_the_new_report_has_new_entries_that_are_not_ok(tmp_path, capsys):
    base = write(tmp_path, "base", [row(1000)] * 3)
    new = write(tmp_path, "new", [row(1000, not_ok=5000)] * 3)

    assert compare.main([str(base), str(new)]) == 1
    assert "Correctness" in capsys.readouterr().out


def test_the_committed_baselines_give_the_documented_capacity():
    redis = compare.load_report(DOCS / "adr016-redis-5ae169a.json")
    cassandra = compare.load_report(DOCS / "adr016-cassandra-5ae169a.json")

    redis_capacity = compare.capacity(redis)
    cassandra_capacity = compare.capacity(cassandra)

    assert (redis_capacity.within_slo, redis_capacity.first_outside) == (3000, 4000)
    assert (cassandra_capacity.within_slo, cassandra_capacity.first_outside) == (1000, 1250)
    assert 3990 < redis_capacity.max_achieved < 4000
    assert 1660 < cassandra_capacity.max_achieved < 1680


def test_comparing_a_report_with_itself_shows_no_change(capsys):
    path = DOCS / "adr016-redis-5ae169a.json"

    assert compare.main([str(path), str(path)]) == 0

    text = capsys.readouterr().out
    assert "better" not in text
    assert "worse" not in text


def test_redis_against_cassandra_fails_on_entries_that_are_not_ok_at_2000(capsys):
    code = compare.main(
        [
            str(DOCS / "adr016-redis-5ae169a.json"),
            str(DOCS / "adr016-cassandra-5ae169a.json"),
            "--label-base",
            "redis",
            "--label-new",
            "cassandra",
        ]
    )

    assert code == 1
    assert "2000 req/s" in capsys.readouterr().out


def test_older_reports_in_the_repository_can_be_compared():
    before = compare.load_report(DOCS / "change4-before-3746261.json")
    after = compare.load_report(DOCS / "change4-after-54ae65f.json")

    assert "| 2000 |" in compare.render(before, after)


def test_increases_of_a_hundred_percent_or_more_are_shown_as_factors():
    delta = compare.compare_values([1.0, 1.1, 1.2], [10.0, 11.0, 12.0], lower_is_better=True)

    assert "(x10.0) worse" in compare.cell(delta, compare.fmt_cpu)


def test_a_change_of_seven_percent_is_noise_by_default_and_counts_with_a_lower_threshold():
    base = [10.0, 10.0, 10.0]
    new = [10.7, 10.7, 10.7]

    default = compare.compare_values(base, new, lower_is_better=True)
    strict = compare.compare_values(base, new, lower_is_better=True, min_change=0.05)

    assert default.verdict == "same"
    assert strict.verdict == "worse"


def test_by_median_one_bad_repeat_does_not_decide_the_capacity(tmp_path):
    rows = [row(1000, p99=10.0), row(1000, p99=10.0, not_ok=400), row(1000, p99=10.0)]
    report = report_of(tmp_path, "a", rows)

    assert compare.capacity(report).within_slo is None
    assert compare.capacity(report, by_median=True).within_slo == 1000


def test_by_median_still_fails_when_most_repeats_miss_the_target(tmp_path):
    rows = [row(1000, p99=30.0), row(1000, p99=30.0), row(1000, p99=10.0)]
    report = report_of(tmp_path, "a", rows)

    assert compare.capacity(report, by_median=True).within_slo is None


def test_by_median_stops_at_the_first_rate_outside_the_target(tmp_path):
    rows = [row(500) for _ in range(3)] + [row(1000, p99=40.0) for _ in range(3)]
    result = compare.capacity(report_of(tmp_path, "a", rows), by_median=True)

    assert result.within_slo == 500
    assert result.first_outside == 1000


def test_the_rendered_comparison_states_the_capacity_by_the_median(tmp_path):
    rows = [row(1000), row(1000, not_ok=400), row(1000)]
    base = report_of(tmp_path, "base", rows)
    new = report_of(tmp_path, "new", rows)

    text = compare.render(base, new)

    assert "within the target no rate" in text
    assert "by the median of the repeats, within the target up to 1000 req/s" in text
