"""Metrics of the feature service (ADR-016)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient
from prometheus_client import values

from featureforge.feature_service import FeatureServer
from featureforge.service_api import create_app
from featureforge.service_metrics import ServiceMetrics
from featureforge.spec.loader import load_spec
from featureforge.spec.model import FallbackPolicy, FeatureSpec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import SCHEMA_VERSION, record_fields, stamp

SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "feature_repo" / "specs" / "user_play_count_1h.yaml"
)
REFERENCE = datetime(2026, 11, 21, 12, 0, tzinfo=UTC)
ENTRIES = 'featureforge_lookup_entries_total{backend="redis",status="%s"}'
REQUESTS = 'featureforge_lookup_requests_total{outcome="%s"}'
DURATION_COUNT = 'featureforge_lookup_duration_seconds_count{backend="redis"}'


class FakeRedis:
    def __init__(self, payloads: dict[str, str] | None = None, error: Exception | None = None):
        self.payloads = payloads or {}
        self.error = error

    def mget(self, keys: list[str]) -> list[str | None]:
        if self.error is not None:
            raise self.error
        return [self.payloads.get(key) for key in keys]

    def ping(self) -> bool:
        return True


@pytest.fixture(autouse=True)
def _default_value_class(monkeypatch: pytest.MonkeyPatch) -> None:
    """Keep one counter value per registry.

    A test that imports feast makes feast set PROMETHEUS_MULTIPROC_DIR. In that
    mode prometheus_client shares counter values between registries, which would
    carry counts from one test into the next.
    """
    monkeypatch.setattr(values, "ValueClass", values.MutexValue)


@pytest.fixture(scope="module")
def spec() -> FeatureSpec:
    return load_spec(SPEC_PATH)


def _payload(spec: FeatureSpec, entity_value: str, observation: datetime) -> str:
    entity_key, counters, derived_names = record_fields(spec)
    counts = {name: 3 for name in counters}
    derived = {name: evaluate_expression(spec.derived[name], counts) for name in derived_names}
    return json.dumps(
        {
            "feature_schema_version": SCHEMA_VERSION,
            "feature_name": spec.name,
            entity_key: entity_value,
            "observation_time": stamp(observation),
            "window_start_exclusive": stamp(observation - spec.window.size),
            **counts,
            **derived,
        }
    )


def _client(spec: FeatureSpec, redis: Any) -> TestClient:
    return TestClient(create_app(FeatureServer(redis, [spec]), ServiceMetrics()))


def _key(spec: FeatureSpec, entity_value: str) -> str:
    server = FeatureServer(FakeRedis(), [spec])
    return server._stores[(spec.name, spec.version)].key_for(entity_value)


def _lookup(client: TestClient, spec: FeatureSpec, entities: list[str], feature: str | None = None):
    body = {
        "features": [{"name": feature or spec.name}],
        "entities": [{spec.keys[0]: value} for value in entities],
        "reference_time": REFERENCE.isoformat(),
    }
    return client.post("/v1/features/lookup", json=body)


def _value(client: TestClient, series: str) -> float:
    for line in client.get("/metrics").text.splitlines():
        if line.startswith(series + " "):
            return float(line.split()[-1])
    raise AssertionError(f"series not found: {series}")


def test_metrics_endpoint_serves_prometheus_text(spec: FeatureSpec) -> None:
    response = _client(spec, FakeRedis()).get("/metrics")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "# TYPE featureforge_lookup_duration_seconds histogram" in response.text


def test_missing_entries_are_counted(spec: FeatureSpec) -> None:
    client = _client(spec, FakeRedis())
    assert _lookup(client, spec, ["user_000001", "user_000002"]).status_code == 200
    assert _value(client, ENTRIES % "missing") == 2.0
    assert _value(client, REQUESTS % "ok") == 1.0
    assert _value(client, DURATION_COUNT) == 1.0


def test_ok_and_stale_entries_are_counted(spec: FeatureSpec) -> None:
    fresh = REFERENCE - timedelta(seconds=10)
    old = REFERENCE - spec.freshness_slo - timedelta(minutes=5)
    redis = FakeRedis(
        {
            _key(spec, "user_000001"): _payload(spec, "user_000001", fresh),
            _key(spec, "user_000002"): _payload(spec, "user_000002", old),
        }
    )
    client = _client(spec, redis)
    assert _lookup(client, spec, ["user_000001", "user_000002", "user_000003"]).status_code == 200
    assert _value(client, ENTRIES % "ok") == 1.0
    assert _value(client, ENTRIES % "stale") == 1.0
    assert _value(client, ENTRIES % "missing") == 1.0


def test_a_backend_error_is_counted(spec: FeatureSpec) -> None:
    client = _client(spec, FakeRedis(error=ConnectionError("down")))
    response = _lookup(client, spec, ["user_000001", "user_000002"])
    if spec.online.fallback.policy is FallbackPolicy.FAIL:
        assert response.status_code == 503
        assert _value(client, REQUESTS % "fail_policy") == 1.0
    else:
        assert response.status_code == 200
        assert _value(client, ENTRIES % "backend_error") == 2.0
        assert _value(client, REQUESTS % "ok") == 1.0


def test_a_rejected_request_is_counted(spec: FeatureSpec) -> None:
    client = _client(spec, FakeRedis())
    assert _lookup(client, spec, ["user_000001"], feature="no_such_feature").status_code == 422
    assert _value(client, REQUESTS % "request_error") == 1.0
    assert _value(client, REQUESTS % "ok") == 0.0


def test_two_apps_do_not_share_counters(spec: FeatureSpec) -> None:
    first = _client(spec, FakeRedis())
    second = _client(spec, FakeRedis())
    _lookup(first, spec, ["user_000001"])
    assert _value(first, REQUESTS % "ok") == 1.0
    assert _value(second, REQUESTS % "ok") == 0.0
