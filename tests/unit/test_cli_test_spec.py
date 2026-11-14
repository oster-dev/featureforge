"""Unit tests for the test-spec command."""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path

import pytest

from featureforge.cli import build_parser, run_test_spec_command
from featureforge.spec.loader import load_spec

_SPEC = Path("feature_repo/specs/user_play_count_1h.yaml")
_ENTITY = ["user_id=user_000001"]


def _event(number: int, event_time: str, user_id: str = "user_000001") -> dict[str, object]:
    return {
        "event_id": f"00000000-0000-4000-8000-{number:012d}",
        "event_time": event_time,
        "ingested_at": event_time,
        "user_id": user_id,
        "content_id": "content_000001",
        "event_type": "play",
        "play_seconds": 0,
        "source": "web",
        "schema_version": "v1",
    }


def _write_events(path: Path, events: list[dict[str, object]]) -> Path:
    path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")
    return path


@pytest.fixture
def events_file(tmp_path: Path) -> Path:
    second = _event(2, "2026-01-01T11:00:01Z")
    return _write_events(
        tmp_path / "events.jsonl",
        [
            _event(1, "2026-01-01T11:00:00Z"),
            second,
            _event(3, "2026-01-01T11:30:00Z"),
            _event(4, "2026-01-01T11:30:00Z", user_id="user_000002"),
            second,
        ],
    )


@pytest.fixture
def counter() -> str:
    spec = load_spec(_SPEC)
    assert spec.window.size == timedelta(hours=1)
    assert list(spec.keys) == ["user_id"]
    return spec.aggregations[0].name


def test_parser_accepts_test_spec_arguments() -> None:
    args = build_parser().parse_args(
        [
            "test-spec",
            "spec.yaml",
            "--events",
            "events.jsonl",
            "--entity",
            "user_id=u1",
            "--at",
            "2026-01-01T12:00:00+00:00",
            "--expect",
            "play_count=3",
        ]
    )

    assert args.command == "test-spec"
    assert args.spec == Path("spec.yaml")
    assert args.entity == ["user_id=u1"]
    assert args.expect == ["play_count=3"]


@pytest.mark.parametrize(
    ("at", "expected"),
    [
        ("2026-01-01T11:00:00+00:00", 1),
        ("2026-01-01T11:30:00+00:00", 3),
        ("2026-01-01T12:00:00+00:00", 2),
        ("2026-01-01T12:30:01+00:00", 0),
    ],
)
def test_counts_match_hand_calculated_windows(
    events_file: Path,
    counter: str,
    at: str,
    expected: int,
    capsys: pytest.CaptureFixture[str],
) -> None:
    status = run_test_spec_command(_SPEC, events_file, _ENTITY, at, [f"{counter}={expected}"])

    assert status == 0
    assert f"✓ {counter} = {expected}" in capsys.readouterr().out


def test_wrong_expectation_returns_one(
    events_file: Path, counter: str, capsys: pytest.CaptureFixture[str]
) -> None:
    status = run_test_spec_command(
        _SPEC, events_file, _ENTITY, "2026-01-01T11:30:00+00:00", [f"{counter}=99"]
    )

    assert status == 1
    assert "expected 99, got 3" in capsys.readouterr().out


def test_unknown_expectation_name_returns_one(
    events_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = run_test_spec_command(
        _SPEC, events_file, _ENTITY, "2026-01-01T11:30:00+00:00", ["nope=1"]
    )

    assert status == 1
    assert "not an output of this spec" in capsys.readouterr().out


def test_missing_events_file_returns_two(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = run_test_spec_command(
        _SPEC, tmp_path / "missing.jsonl", _ENTITY, "2026-01-01T11:30:00+00:00", []
    )

    assert status == 2
    assert "file not found" in capsys.readouterr().out


def test_invalid_event_names_the_line(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    events = tmp_path / "events.jsonl"
    events.write_text(
        json.dumps(_event(1, "2026-01-01T11:00:00Z")) + '\n{"event_id": 1}\n',
        encoding="utf-8",
    )

    status = run_test_spec_command(_SPEC, events, _ENTITY, "2026-01-01T11:30:00+00:00", [])

    assert status == 1
    assert "line 2" in capsys.readouterr().out


def test_malformed_entity_returns_one(
    events_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = run_test_spec_command(
        _SPEC, events_file, ["user_000001"], "2026-01-01T11:30:00+00:00", []
    )

    assert status == 1
    assert "--entity expects KEY=VALUE" in capsys.readouterr().out


def test_entity_with_wrong_key_returns_one(
    events_file: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    status = run_test_spec_command(
        _SPEC, events_file, ["content_id=content_000001"], "2026-01-01T11:30:00+00:00", []
    )

    assert status == 1
    assert "entity must provide exactly" in capsys.readouterr().out
