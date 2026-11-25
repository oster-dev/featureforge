"""The service measures how late its event loop runs (ADR-016)."""

import asyncio
import time
from contextlib import suppress
from pathlib import Path
from unittest.mock import Mock

import pytest
from fastapi.testclient import TestClient
from prometheus_client import values

from featureforge.feature_service import FeatureServer
from featureforge.service_api import create_app
from featureforge.service_metrics import ServiceMetrics, watch_event_loop_lag
from featureforge.spec.loader import load_spec

SPEC = load_spec(
    Path(__file__).resolve().parents[2] / "feature_repo" / "specs" / "user_play_count_1h.yaml"
)
COUNT = "featureforge_event_loop_lag_seconds_count"
TOTAL = "featureforge_event_loop_lag_seconds_sum"


@pytest.fixture(autouse=True)
def _default_value_class(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(values, "ValueClass", values.MutexValue)


def sample(text: str, name: str) -> float:
    for line in text.splitlines():
        if line.startswith(name + " "):
            return float(line.split()[1])
    raise AssertionError(f"{name} not found")


async def watch(metrics: ServiceMetrics, block: float) -> None:
    watcher = asyncio.create_task(watch_event_loop_lag(metrics, interval=0.01))
    await asyncio.sleep(0.1)
    if block:
        time.sleep(block)
    await asyncio.sleep(0.05)
    watcher.cancel()
    with suppress(asyncio.CancelledError):
        await watcher


def test_a_blocked_event_loop_shows_up_as_lag() -> None:
    metrics = ServiceMetrics()

    asyncio.run(watch(metrics, block=0.15))

    text = metrics.render().decode()
    assert sample(text, COUNT) > 0
    assert sample(text, TOTAL) >= 0.1


def test_a_quiet_event_loop_has_a_small_lag() -> None:
    metrics = ServiceMetrics()

    asyncio.run(watch(metrics, block=0))

    text = metrics.render().decode()
    count = sample(text, COUNT)
    assert count > 0
    assert sample(text, TOTAL) / count < 0.05


def test_the_metrics_endpoint_exposes_the_lag_while_the_app_runs() -> None:
    app = create_app(FeatureServer(Mock(), [SPEC]))

    with TestClient(app) as client:
        time.sleep(0.15)
        body = client.get("/metrics").text

    assert sample(body, COUNT) > 0
