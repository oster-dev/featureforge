"""Tests for the quickstart script (ADR-016)."""

import importlib.util
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import parse_feature_record

ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = ROOT / "feature_repo" / "specs" / "user_play_count_1h.yaml"
_spec = importlib.util.spec_from_file_location("quickstart", ROOT / "scripts" / "quickstart.py")
assert _spec is not None and _spec.loader is not None
quickstart = importlib.util.module_from_spec(_spec)
sys.modules["quickstart"] = quickstart
_spec.loader.exec_module(quickstart)


def test_the_demo_writes_two_records_that_the_spec_accepts() -> None:
    spec = load_spec(SPEC_PATH)
    now = datetime(2026, 11, 24, 12, 0, tzinfo=UTC)

    records = quickstart.build_records(spec, now)

    assert [r.entity_value for r in records] == ["demo_user_fresh", "demo_user_old"]
    assert [now - r.observation_time for r in records] == [
        timedelta(minutes=1),
        timedelta(minutes=20),
    ]
    for record in records:
        assert record.entity_key == "user_id"
        assert record.window_start_exclusive == record.observation_time - spec.window.size
        parse_feature_record(record.to_json(), spec)


def test_the_fresh_value_is_inside_and_the_old_value_outside_the_freshness_slo() -> None:
    spec = load_spec(SPEC_PATH)

    assert timedelta(minutes=1) < spec.freshness_slo < timedelta(minutes=20)


def test_the_demo_expects_ok_stale_and_missing_in_the_order_of_the_request() -> None:
    assert quickstart.EXPECTED == ["ok", "stale", "missing"]
    body = quickstart.lookup_body(load_spec(SPEC_PATH))
    assert [e["user_id"] for e in body["entities"]] == [
        "demo_user_fresh",
        "demo_user_old",
        "demo_user_unknown",
    ]


def test_the_table_shows_status_value_age_and_the_reason() -> None:
    spec = load_spec(SPEC_PATH)
    entries = [
        {
            "entity": {"user_id": "demo_user_fresh"},
            "status": "ok",
            "values": {"play_count": 7},
            "age_seconds": 60.2,
            "freshness": "fresh",
            "error_type": None,
        },
        {
            "entity": {"user_id": "demo_user_unknown"},
            "status": "missing",
            "values": None,
            "age_seconds": None,
            "freshness": None,
            "error_type": None,
        },
        {
            "entity": {"user_id": "x"},
            "status": "backend_error",
            "values": None,
            "age_seconds": None,
            "freshness": None,
            "error_type": "ConnectionError",
        },
    ]

    lines = quickstart.render(spec, entries).splitlines()

    assert lines[0].split() == ["entity", "status", "play_count", "age_s", "note"]
    assert lines[1].split() == ["demo_user_fresh", "ok", "7", "60", "fresh"]
    assert lines[2].split() == ["demo_user_unknown", "missing", "-", "-", "-"]
    assert lines[3].split() == ["x", "backend_error", "-", "-", "ConnectionError"]


def test_check_reports_a_difference_in_the_statuses() -> None:
    entries = [{"status": "ok"}, {"status": "ok"}, {"status": "missing"}]

    assert quickstart.check(entries, ["ok", "ok", "missing"]) == []
    assert "expected" in quickstart.check(entries, ["ok", "stale", "missing"])[0]
