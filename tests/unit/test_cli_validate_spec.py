"""Unit tests for the validate-spec command."""

from __future__ import annotations

from pathlib import Path

import pytest

from featureforge.cli import build_parser, run_validate_spec_command

_SPEC_DIR = Path("feature_repo/specs")


def test_parser_accepts_validate_spec_paths() -> None:
    args = build_parser().parse_args(["validate-spec", "a.yaml", "b.yaml"])

    assert args.command == "validate-spec"
    assert args.paths == [Path("a.yaml"), Path("b.yaml")]


def test_parser_defaults_to_no_explicit_paths() -> None:
    args = build_parser().parse_args(["validate-spec"])

    assert args.paths == []


def test_committed_specs_are_valid(capsys: pytest.CaptureFixture[str]) -> None:
    status = run_validate_spec_command(sorted(_SPEC_DIR.glob("*.yaml")))

    assert status == 0
    assert "✗" not in capsys.readouterr().out


def test_default_validates_every_committed_spec(
    capsys: pytest.CaptureFixture[str],
) -> None:
    status = run_validate_spec_command([])

    out = capsys.readouterr().out
    assert status == 0
    for spec_path in _SPEC_DIR.glob("*.yaml"):
        assert str(spec_path) in out


def test_invalid_spec_returns_one_and_names_the_problem(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    broken = tmp_path / "broken.yaml"
    broken.write_text("name: only_a_name\n", encoding="utf-8")

    status = run_validate_spec_command([broken])

    out = capsys.readouterr().out
    assert status == 1
    assert f"✗ {broken}" in out


def test_missing_file_returns_two(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    missing = tmp_path / "missing.yaml"

    status = run_validate_spec_command([missing])

    assert status == 2
    assert "file not found" in capsys.readouterr().out


def test_empty_default_directory_returns_two(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    status = run_validate_spec_command([])

    assert status == 2
    assert "No spec files found" in capsys.readouterr().out
