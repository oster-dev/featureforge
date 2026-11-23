"""Tests for the benchmark runner with presets and safety checks (ADR-016)."""

import importlib.util
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[2] / "scripts" / "run_serving_benchmark.py"
_spec = importlib.util.spec_from_file_location("run_serving_benchmark", SCRIPT)
assert _spec is not None and _spec.loader is not None
runner = importlib.util.module_from_spec(_spec)
sys.modules["run_serving_benchmark"] = runner
_spec.loader.exec_module(runner)

SMOKE = runner.PRESETS["smoke"]
RECORD = runner.PRESETS["record"]
COMPACTIONSTATS = """concurrent compactors            2
pending tasks                    0
compactions completed            217
data compacted                   3303339443
"""


def levels(findings):
    return [finding.level for finding in findings]


def patch_environment(monkeypatch, dirty, services):
    monkeypatch.setattr(runner, "uncommitted_changes", lambda: dirty)
    monkeypatch.setattr(runner, "running_services", lambda: services)
    monkeypatch.setattr(runner, "head_commit", lambda: "abc1234")


def test_a_clean_tree_with_only_the_backend_services_has_no_findings():
    assert runner.check_environment(RECORD, "redis", dirty=[], services={"redis"}) == []
    cassandra = runner.check_environment(
        RECORD, "cassandra", dirty=[], services={"cassandra", "redis"}
    )
    assert cassandra == []


def test_a_dirty_tree_is_an_error_for_record_and_a_warning_for_smoke():
    dirty = [" M scripts/x.py"]

    record = runner.check_environment(RECORD, "redis", dirty=dirty, services={"redis"})
    smoke = runner.check_environment(SMOKE, "redis", dirty=dirty, services={"redis"})

    assert levels(record) == ["error"]
    assert levels(smoke) == ["warning"]


def test_other_services_are_listed_with_the_command_to_stop_them():
    findings = runner.check_environment(
        RECORD, "redis", dirty=[], services={"redis", "kafka", "grafana"}
    )

    assert levels(findings) == ["error"]
    assert "docker compose stop grafana kafka" in findings[0].message


def test_a_missing_backend_service_is_always_an_error():
    findings = runner.check_environment(SMOKE, "cassandra", dirty=[], services={"redis"})

    assert levels(findings) == ["error"]
    assert "docker compose up -d cassandra" in findings[0].message


def test_without_compose_information_the_service_check_is_skipped():
    assert runner.check_environment(RECORD, "redis", dirty=[], services=None) == []


def test_compactionstats_gives_pending_and_completed():
    assert runner.parse_compactionstats(COMPACTIONSTATS) == (0, 217)
    assert runner.parse_compactionstats("nothing here") is None


def test_the_benchmark_command_carries_the_preset_and_reseeds_on_request(tmp_path):
    options = {
        "entities": 2000,
        "rates": "200,400",
        "repeats": 1,
        "duration": 5,
        "warmup": 2,
        "output": tmp_path / "r.json",
    }

    reseeding = runner.benchmark_command(
        "redis", reseed=True, inhibitor=["caffeinate", "-i"], **options
    )
    plain = runner.benchmark_command("redis", reseed=False, inhibitor=[], **options)

    assert reseeding[:2] == ["caffeinate", "-i"]
    assert reseeding[2:4] == [sys.executable, str(runner.BENCHMARK)]
    assert reseeding[reseeding.index("--rates") + 1] == "200,400"
    assert "--reseed" in reseeding
    assert "--reseed" not in plain
    assert plain[0] == sys.executable


@pytest.mark.parametrize("name", ["smoke", "record"])
@pytest.mark.parametrize("backend", ["redis", "cassandra"])
def test_the_rates_of_a_preset_rise(name, backend):
    rates = [int(rate) for rate in runner.PRESETS[name].rates[backend].split(",")]

    assert rates == sorted(rates)
    assert len(set(rates)) == len(rates)


def test_a_dry_run_of_record_with_cassandra_seeds_settles_and_measures(monkeypatch, capsys):
    patch_environment(monkeypatch, [], {"cassandra"})
    monkeypatch.setattr(runner, "run", lambda command: pytest.fail("must not run"))

    code = runner.main(["record", "--backend", "cassandra", "--dry-run", "--no-inhibit"])

    out = capsys.readouterr().out
    assert code == 0
    assert out.count("benchmark_feature_serving.py") == 2
    assert out.index("seed:") < out.index("settle:") < out.index("measure:")
    assert "--rates 500,1000,1500,2000,2500" in out


def test_main_stops_on_errors_unless_forced(monkeypatch):
    patch_environment(monkeypatch, [" M x"], {"redis"})
    arguments = ["record", "--backend", "redis", "--dry-run", "--no-inhibit"]

    assert runner.main(arguments) == 2
    assert runner.main([*arguments, "--force"]) == 0


def test_smoke_runs_with_a_dirty_tree_reseeds_and_leaves_the_big_keyspace_alone(
    monkeypatch, capsys
):
    patch_environment(monkeypatch, [" M x"], {"cassandra"})

    code = runner.main(["smoke", "--backend", "cassandra", "--dry-run", "--no-inhibit"])

    out = capsys.readouterr().out
    assert code == 0
    assert "--reseed" in out
    assert "--entities 2000" in out
    assert "ff_loadtest_smoke" in out


def test_the_compose_check_can_be_skipped(monkeypatch):
    patch_environment(monkeypatch, [], None)
    monkeypatch.setattr(runner, "running_services", lambda: pytest.fail("must not be called"))

    code = runner.main(["smoke", "--no-compose-check", "--dry-run", "--no-inhibit"])

    assert code == 0
