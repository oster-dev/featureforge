"""The dev container configuration stays valid and keeps what the quickstart needs."""

import json
from pathlib import Path

CONFIG = Path(__file__).resolve().parents[2] / ".devcontainer" / "devcontainer.json"


def test_the_configuration_is_valid_json_and_keeps_what_the_quickstart_needs() -> None:
    config = json.loads(CONFIG.read_text())

    assert any("docker-in-docker" in name for name in config["features"])
    assert any("features/python" in name for name in config["features"])
    assert 8080 in config["forwardPorts"]
    assert ".[serve]" in config["postCreateCommand"]
