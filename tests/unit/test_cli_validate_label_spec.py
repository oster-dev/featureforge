"""Unit tests for the validate-label-spec command."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from featureforge.cli import (
    build_parser,
    run_validate_label_spec_command,
    run_validate_spec_command,
)

_LABEL_DIR = Path("feature_repo/labels")

_VALID = {
    "name": "completed_within_7d",
    "version": 1,
    "owner": "personalization-team",
    "description": "Whether the user completed a play in the 7 days after t",
    "entity": "user_id",
    "source": "interaction-events.validated.v1",
    "outcome": {"op": "exists", "filter": "event_type == 'complete'"},
    "horizon": "7d",
    "dedupe": {"key": "event_id"},
}


def test_parser_accepts_validate_label_spec_paths() -> None:
    args = build_parser().parse_args(["validate-label-spec", "a.yaml", "b.yaml"])

    assert args.command == "validate-label-spec"
    assert args.paths == [Path("a.yaml"), Path("b.yaml")]


def test_parser_defaults_to_no_explicit_paths() -> None:
    args = build_parser().parse_args(["validate-label-spec"])

    assert args.paths == []


def test_committed_labels_are_valid(capsys: pytest.CaptureFixture[str]) -> None:
    status = run_validate_label_spec_command(sorted(_LABEL_DIR.glob("*.yaml")))

    assert status == 0
    assert "✗" not in capsys.readouterr().out


def test_default_validates_every_committed_label_and_no_feature_spec(
    capsys: pytest.CaptureFixture[str],
) -> None:
    status = run_validate_label_spec_command([])

    out = capsys.readouterr().out
    assert status == 0
    assert "completed_within_7d" in out
    assert "active_within_7d" in out
    assert "feature_repo/specs" not in out


def test_validate_spec_default_never_sees_labels(
    capsys: pytest.CaptureFixture[str],
) -> None:
    status = run_validate_spec_command([])

    out = capsys.readouterr().out
    assert status == 0
    assert "completed_within_7d" not in out
    assert "active_within_7d" not in out


def test_event_type_outside_the_contract_fails_with_status_one(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    broken = {**_VALID, "outcome": {"op": "exists", "filter": "event_type == 'browse'"}}
    path = tmp_path / "broken.yaml"
    path.write_text(yaml.safe_dump(broken), encoding="utf-8")

    status = run_validate_label_spec_command([path])

    assert status == 1
    assert "outside the contract" in capsys.readouterr().out


def test_missing_file_fails_with_status_two(capsys: pytest.CaptureFixture[str]) -> None:
    status = run_validate_label_spec_command([Path("does-not-exist.yaml")])

    assert status == 2
    assert "file not found" in capsys.readouterr().out
