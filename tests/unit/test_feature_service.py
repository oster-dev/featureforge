"""Tests for the feature serving core and HTTP layer (ADR-016)."""

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast

import pytest
import yaml
from fastapi.testclient import TestClient
from pydantic import ValidationError
from redis import Redis

from featureforge.feature_service import (
    MAX_ENTITIES,
    FailPolicyError,
    FeatureRef,
    FeatureServer,
    RequestError,
)
from featureforge.service_api import create_app
from featureforge.spec.loader import load_spec
from featureforge.spec.model import Fallback, FallbackPolicy, FeatureSpec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import FeatureRecord

SPEC_DIR = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
REFERENCE = datetime(2026, 11, 20, 12, 0, tzinfo=UTC)
SKIP_RATE = load_spec(SPEC_DIR / "user_skip_rate_24h.yaml")
PLAY_COUNT = load_spec(SPEC_DIR / "user_play_count_1h.yaml")


class FakeRedis:
    """A stand-in that records MGET calls and can fail on demand."""

    def __init__(self, data: dict[str, str] | None = None, error: Exception | None = None) -> None:
        self.data = dict(data or {})
        self.error = error
        self.mget_calls: list[list[str]] = []

    def mget(self, keys: list[str]) -> list[str | None]:
        self.mget_calls.append(list(keys))
        if self.error is not None:
            raise self.error
        return [self.data.get(key) for key in keys]

    def ping(self) -> bool:
        if self.error is not None:
            raise self.error
        return True


def as_redis(fake: FakeRedis) -> Redis:
    return cast("Redis", fake)


def key_of(spec: FeatureSpec, entity_value: str) -> str:
    return f"featureforge-stream:online:v{spec.version}:{spec.name}:{entity_value}"


def make_record(
    spec: FeatureSpec, entity_value: str, observation_time: datetime, base: int = 3
) -> FeatureRecord:
    counts = {a.name: base for a in spec.aggregations}
    derived = {name: evaluate_expression(expr, counts) for name, expr in spec.derived.items()}
    return FeatureRecord(
        feature_name=spec.name,
        entity_key=spec.keys[0],
        entity_value=entity_value,
        observation_time=observation_time,
        window_start_exclusive=observation_time - spec.window.size,
        counts=counts,
        derived=derived,
    )


def fresh_time(spec: FeatureSpec) -> datetime:
    return REFERENCE - spec.freshness_slo / 2


def store_record(fake: FakeRedis, spec: FeatureSpec, entity_value: str, when: datetime) -> None:
    fake.data[key_of(spec, entity_value)] = make_record(spec, entity_value, when).to_json()


def with_fallback(
    spec: FeatureSpec, policy: FallbackPolicy, defaults: dict[str, float] | None = None
) -> FeatureSpec:
    fallback = Fallback(policy=policy, defaults=defaults or {})
    online = spec.online.model_copy(update={"fallback": fallback})
    return spec.model_copy(update={"online": online})


def lookup(
    fake: FakeRedis,
    specs: list[FeatureSpec],
    features: list[FeatureRef],
    entities: list[dict[str, str]],
) -> Any:
    server = FeatureServer.for_redis(as_redis(fake), specs)
    return server.lookup(features, entities, reference_time=REFERENCE)


def test_fresh_record_is_ok_with_all_fields() -> None:
    fake = FakeRedis()
    store_record(fake, SKIP_RATE, "u1", fresh_time(SKIP_RATE))
    result = lookup(fake, [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}])
    (entry,) = result.entries
    assert entry.status == "ok"
    assert entry.freshness == "fresh"
    assert entry.values == {"play_count": 3, "skip_count": 3, "skip_rate": 0.5}
    assert entry.age_seconds == pytest.approx(SKIP_RATE.freshness_slo.total_seconds() / 2)
    assert entry.version == 1
    assert entry.published_at is None
    assert result.atomic is False


def test_unknown_entity_is_missing_and_never_a_silent_null() -> None:
    result = lookup(FakeRedis(), [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "nobody"}])
    (entry,) = result.entries
    assert entry.status == "missing"
    assert entry.values is None
    assert entry.error_type is None


def test_value_older_than_the_slo_is_stale_and_keeps_its_value() -> None:
    fake = FakeRedis()
    old = REFERENCE - SKIP_RATE.freshness_slo - timedelta(seconds=1)
    store_record(fake, SKIP_RATE, "u1", old)
    (entry,) = lookup(fake, [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}]).entries
    assert entry.status == "stale"
    assert entry.freshness == "stale"
    assert entry.values is not None
    assert entry.age_seconds is not None
    assert entry.age_seconds > SKIP_RATE.freshness_slo.total_seconds()


def test_age_equal_to_the_slo_is_still_ok() -> None:
    fake = FakeRedis()
    store_record(fake, SKIP_RATE, "u1", REFERENCE - SKIP_RATE.freshness_slo)
    (entry,) = lookup(fake, [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}]).entries
    assert entry.status == "ok"


def test_observation_in_the_future_is_stale_with_reason_future() -> None:
    fake = FakeRedis()
    store_record(fake, SKIP_RATE, "u1", REFERENCE + timedelta(minutes=1))
    (entry,) = lookup(fake, [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}]).entries
    assert entry.status == "stale"
    assert entry.freshness == "future"


def test_a_corrupt_record_fails_only_its_own_entry() -> None:
    fake = FakeRedis()
    store_record(fake, SKIP_RATE, "good", fresh_time(SKIP_RATE))
    fake.data[key_of(SKIP_RATE, "bad")] = "{not json"
    result = lookup(
        fake, [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "bad"}, {"user_id": "good"}]
    )
    bad, good = result.entries
    assert bad.status == "backend_error"
    assert bad.error_type == "JSONDecodeError"
    assert bad.values is None
    assert good.status == "ok"


def test_a_record_for_another_entity_is_a_backend_error() -> None:
    fake = FakeRedis()
    other = make_record(SKIP_RATE, "u2", fresh_time(SKIP_RATE)).to_json()
    fake.data[key_of(SKIP_RATE, "u1")] = other
    (entry,) = lookup(fake, [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}]).entries
    assert entry.status == "backend_error"


def test_a_store_failure_marks_every_entry_and_hides_the_message() -> None:
    fake = FakeRedis(error=ConnectionError("secret-host:6379 refused"))
    result = lookup(
        fake,
        [SKIP_RATE, PLAY_COUNT],
        [FeatureRef(SKIP_RATE.name), FeatureRef(PLAY_COUNT.name)],
        [{"user_id": "u1"}, {"user_id": "u2"}],
    )
    assert len(result.entries) == 4
    assert {entry.status for entry in result.entries} == {"backend_error"}
    assert {entry.error_type for entry in result.entries} == {"ConnectionError"}
    assert "secret-host" not in json.dumps(result.to_dict())


def test_a_request_makes_one_mget_for_all_keys() -> None:
    fake = FakeRedis()
    result = lookup(
        fake,
        [SKIP_RATE, PLAY_COUNT],
        [FeatureRef(SKIP_RATE.name), FeatureRef(PLAY_COUNT.name)],
        [{"user_id": "u1"}, {"user_id": "u2"}],
    )
    assert len(fake.mget_calls) == 1
    assert len(fake.mget_calls[0]) == 4
    assert len(result.entries) == 4


def test_every_pair_has_an_entry_in_entity_major_order() -> None:
    fake = FakeRedis()
    store_record(fake, PLAY_COUNT, "u2", fresh_time(PLAY_COUNT))
    result = lookup(
        fake,
        [SKIP_RATE, PLAY_COUNT],
        [FeatureRef(SKIP_RATE.name), FeatureRef(PLAY_COUNT.name)],
        [{"user_id": "u1"}, {"user_id": "u2"}],
    )
    pairs = [(e.entity["user_id"], e.feature, e.status) for e in result.entries]
    assert pairs == [
        ("u1", SKIP_RATE.name, "missing"),
        ("u1", PLAY_COUNT.name, "missing"),
        ("u2", SKIP_RATE.name, "missing"),
        ("u2", PLAY_COUNT.name, "ok"),
    ]


def test_default_value_policy_fills_values_but_keeps_the_status() -> None:
    spec = with_fallback(
        SKIP_RATE,
        FallbackPolicy.DEFAULT_VALUE,
        {"play_count": 0, "skip_count": 0, "skip_rate": 0.0},
    )
    (entry,) = lookup(FakeRedis(), [spec], [FeatureRef(spec.name)], [{"user_id": "u1"}]).entries
    assert entry.status == "missing"
    assert entry.defaulted is True
    assert entry.values == {"play_count": 0, "skip_count": 0, "skip_rate": 0.0}


def test_default_value_policy_does_not_touch_ok_or_stale_values() -> None:
    spec = with_fallback(
        SKIP_RATE,
        FallbackPolicy.DEFAULT_VALUE,
        {"play_count": 0, "skip_count": 0, "skip_rate": 0.0},
    )
    fake = FakeRedis()
    store_record(fake, spec, "fresh", fresh_time(spec))
    store_record(fake, spec, "old", REFERENCE - spec.freshness_slo - timedelta(minutes=1))
    fresh, old = lookup(
        fake, [spec], [FeatureRef(spec.name)], [{"user_id": "fresh"}, {"user_id": "old"}]
    ).entries
    assert (fresh.status, fresh.defaulted, (fresh.values or {}).get("play_count")) == (
        "ok",
        False,
        3,
    )
    assert (old.status, old.defaulted, (old.values or {}).get("play_count")) == ("stale", False, 3)


def test_fail_policy_raises_on_a_backend_error_but_not_on_missing() -> None:
    spec = with_fallback(SKIP_RATE, FallbackPolicy.FAIL)
    missing = lookup(FakeRedis(), [spec], [FeatureRef(spec.name)], [{"user_id": "u1"}])
    assert missing.entries[0].status == "missing"
    broken = FakeRedis(error=TimeoutError("slow"))
    with pytest.raises(FailPolicyError) as raised:
        lookup(
            broken,
            [spec, PLAY_COUNT],
            [FeatureRef(spec.name), FeatureRef(PLAY_COUNT.name)],
            [{"user_id": "u1"}],
        )
    assert raised.value.features == (spec.name,)


def test_omit_is_the_default_and_keeps_status_and_null_values() -> None:
    assert SKIP_RATE.online.fallback.policy is FallbackPolicy.OMIT
    broken = FakeRedis(error=TimeoutError("slow"))
    (entry,) = lookup(
        broken, [SKIP_RATE], [FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}]
    ).entries
    assert entry.status == "backend_error"
    assert entry.error_type == "TimeoutError"
    assert entry.values is None
    assert entry.defaulted is False


def test_the_highest_version_is_used_unless_one_is_named() -> None:
    v2 = SKIP_RATE.model_copy(update={"version": 2})
    fake = FakeRedis()
    store_record(fake, v2, "u1", fresh_time(v2))
    server = FeatureServer.for_redis(as_redis(fake), [SKIP_RATE, v2])
    entity = [{"user_id": "u1"}]
    latest = server.lookup([FeatureRef(SKIP_RATE.name)], entity, reference_time=REFERENCE)
    pinned = server.lookup([FeatureRef(SKIP_RATE.name, 1)], entity, reference_time=REFERENCE)
    assert (latest.entries[0].version, latest.entries[0].status) == (2, "ok")
    assert (pinned.entries[0].version, pinned.entries[0].status) == (1, "missing")


@pytest.mark.parametrize(
    ("features", "entities", "reference"),
    [
        ([FeatureRef("no_such_feature")], [{"user_id": "u1"}], REFERENCE),
        ([FeatureRef(SKIP_RATE.name, 9)], [{"user_id": "u1"}], REFERENCE),
        ([FeatureRef(SKIP_RATE.name), FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}], REFERENCE),
        ([], [{"user_id": "u1"}], REFERENCE),
        ([FeatureRef(SKIP_RATE.name)], [], REFERENCE),
        ([FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}] * (MAX_ENTITIES + 1), REFERENCE),
        ([FeatureRef(SKIP_RATE.name)], [{"content_id": "c1"}], REFERENCE),
        ([FeatureRef(SKIP_RATE.name)], [{"user_id": "has space"}], REFERENCE),
        ([FeatureRef(SKIP_RATE.name)], [{"user_id": ""}], REFERENCE),
        ([FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}], datetime(2026, 11, 20, 12, 0)),
    ],
)
def test_malformed_requests_are_request_errors(
    features: list[FeatureRef], entities: list[dict[str, str]], reference: datetime
) -> None:
    server = FeatureServer.for_redis(as_redis(FakeRedis()), [SKIP_RATE])
    with pytest.raises(RequestError):
        server.lookup(features, entities, reference_time=reference)


def test_a_malformed_request_never_reaches_the_store() -> None:
    fake = FakeRedis()
    server = FeatureServer.for_redis(as_redis(fake), [SKIP_RATE])
    with pytest.raises(RequestError):
        server.lookup([FeatureRef("no_such_feature")], [{"user_id": "u1"}])
    assert fake.mget_calls == []


def test_two_specs_with_the_same_name_and_version_are_rejected() -> None:
    with pytest.raises(ValueError, match="duplicate spec"):
        FeatureServer.for_redis(as_redis(FakeRedis()), [SKIP_RATE, SKIP_RATE])


def _spec_data() -> dict[str, Any]:
    data = yaml.safe_load((SPEC_DIR / "user_skip_rate_24h.yaml").read_text())
    assert isinstance(data, dict)
    return data


def test_default_value_policy_must_name_every_field() -> None:
    data = _spec_data()
    data["online"]["fallback"] = {"policy": "default_value", "defaults": {"play_count": 0}}
    with pytest.raises(ValidationError):
        FeatureSpec.model_validate(data)
    data["online"]["fallback"]["defaults"] = {"play_count": 0, "skip_count": 0, "skip_rate": 0.0}
    assert FeatureSpec.model_validate(data).online.fallback.policy is FallbackPolicy.DEFAULT_VALUE


def test_defaults_without_the_default_value_policy_are_rejected() -> None:
    data = _spec_data()
    data["online"]["fallback"] = {"policy": "omit", "defaults": {"play_count": 0}}
    with pytest.raises(ValidationError):
        FeatureSpec.model_validate(data)


# --- HTTP layer ---

BODY: dict[str, Any] = {
    "features": [{"name": SKIP_RATE.name}],
    "entities": [{"user_id": "u1"}],
    "reference_time": REFERENCE.isoformat(),
}


def make_client(fake: FakeRedis, specs: list[FeatureSpec] | None = None) -> TestClient:
    return TestClient(create_app(FeatureServer.for_redis(as_redis(fake), specs or [SKIP_RATE])))


def test_http_lookup_returns_one_entry_with_a_status() -> None:
    fake = FakeRedis()
    store_record(fake, SKIP_RATE, "u1", fresh_time(SKIP_RATE))
    response = make_client(fake).post("/v1/features/lookup", json=BODY)
    assert response.status_code == 200
    body = response.json()
    assert body["atomic"] is False
    assert [entry["status"] for entry in body["entries"]] == ["ok"]
    assert body["entries"][0]["values"]["skip_rate"] == 0.5


def test_http_store_failure_is_a_200_with_backend_error_entries() -> None:
    fake = FakeRedis(error=ConnectionError("secret-host"))
    response = make_client(fake).post("/v1/features/lookup", json=BODY)
    assert response.status_code == 200
    assert response.json()["entries"][0]["status"] == "backend_error"
    assert "secret-host" not in response.text


def test_http_fail_policy_answers_503_with_the_feature_name() -> None:
    spec = with_fallback(SKIP_RATE, FallbackPolicy.FAIL)
    fake = FakeRedis(error=TimeoutError("slow"))
    response = make_client(fake, [spec]).post("/v1/features/lookup", json=BODY)
    assert response.status_code == 503
    assert response.json()["features"] == [spec.name]


@pytest.mark.parametrize(
    "body",
    [
        {**BODY, "features": [{"name": "no_such_feature"}]},
        {**BODY, "features": [{"name": SKIP_RATE.name, "version": 9}]},
        {**BODY, "reference_time": "2026-11-20T12:00:00"},
        {**BODY, "unexpected": 1},
        {**BODY, "features": [{"name": SKIP_RATE.name, "version": 0}]},
    ],
)
def test_http_malformed_requests_answer_422(body: dict[str, Any]) -> None:
    fake = FakeRedis()
    response = make_client(fake).post("/v1/features/lookup", json=body)
    assert response.status_code == 422
    assert fake.mget_calls == []


def test_http_healthz_does_not_touch_the_store() -> None:
    fake = FakeRedis(error=ConnectionError("down"))
    response = make_client(fake).get("/healthz")
    assert response.status_code == 200
    assert fake.mget_calls == []


def test_http_readyz_follows_the_store() -> None:
    assert make_client(FakeRedis()).get("/readyz").status_code == 200
    down = make_client(FakeRedis(error=ConnectionError("down"))).get("/readyz")
    assert down.status_code == 503
    assert down.json()["status"] == "not_ready"


def test_the_namespace_sets_the_key_prefix() -> None:
    fake = FakeRedis()
    server = FeatureServer.for_redis(as_redis(fake), [SKIP_RATE], namespace="custom:ns")
    server.lookup([FeatureRef(SKIP_RATE.name)], [{"user_id": "u1"}], reference_time=REFERENCE)
    assert fake.mget_calls == [[f"custom:ns:v{SKIP_RATE.version}:{SKIP_RATE.name}:u1"]]
