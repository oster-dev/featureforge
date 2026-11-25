"""The benchmark reports the git state of its own tree, not of the working directory."""

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "benchmark_feature_serving.py"


def load_benchmark():
    spec = importlib.util.spec_from_file_location("benchmark_feature_serving_under_test", SCRIPT)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def git(directory: Path, *arguments: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(directory), *arguments],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def test_the_commit_is_the_one_of_the_script_tree_and_not_of_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    other = tmp_path / "other"
    other.mkdir()
    git(other, "init", "-q")
    git(
        other,
        "-c",
        "user.name=test",
        "-c",
        "user.email=test@example.com",
        "commit",
        "--allow-empty",
        "-q",
        "-m",
        "another repository",
    )
    other_commit = git(other, "rev-parse", "HEAD")
    expected = git(ROOT, "rev-parse", "HEAD")
    assert other_commit != expected
    monkeypatch.chdir(other)

    state = load_benchmark().git_state()

    assert state["commit"] == expected
    assert state["dirty"] == bool(git(ROOT, "status", "--porcelain"))
