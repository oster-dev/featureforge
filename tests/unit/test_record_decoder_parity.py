"""RecordDecoder returns what the previous parse_feature_record returned, errors included."""

import json
import math
import random
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import (
    BASE_FIELDS,
    ENTITY_VALUE,
    SCHEMA_VERSION,
    FeatureRecord,
    RecordDecoder,
    parse_feature_record,
    parse_time,
    record_fields,
    stamp,
)

SPECS_DIR = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
BAD_VALUES: list[Any] = [None, True, -1, 1.5, "x", "", [], {}, float("nan"), float("inf")]


def _record_specs() -> list[FeatureSpec]:
    found: list[FeatureSpec] = []
    if SPECS_DIR.is_dir():
        for path in sorted(SPECS_DIR.glob("*.yaml")):
            spec = load_spec(path)
            try:
                record_fields(spec)
            except ValueError:
                continue
            found.append(spec)
    return found


SPECS = _record_specs()


def _outcome(function: Callable[..., Any], *args: Any) -> tuple[Any, ...]:
    try:
        return ("ok", function(*args))
    except Exception as error:
        return ("error", type(error), str(error))


def _old_parse(payload: str | bytes, spec: FeatureSpec) -> FeatureRecord:
    """Copy of parse_feature_record as it was before RecordDecoder."""
    entity_key, counters, derived_names = record_fields(spec)

    data = json.loads(payload)
    if not isinstance(data, dict):
        raise ValueError("record must be a JSON object")

    expected = {*BASE_FIELDS, entity_key, *counters, *derived_names}
    if data.keys() != expected:
        raise ValueError(
            f"record fields do not match spec {spec.name}: "
            f"missing {sorted(expected - data.keys())}, "
            f"unexpected {sorted(data.keys() - expected)}"
        )
    if data["feature_schema_version"] != SCHEMA_VERSION:
        raise ValueError(f"feature_schema_version must be {SCHEMA_VERSION}")
    if data["feature_name"] != spec.name:
        raise ValueError(f"feature_name must be {spec.name}")

    entity_value = data[entity_key]
    if not isinstance(entity_value, str) or not ENTITY_VALUE.fullmatch(entity_value):
        raise ValueError(f"{entity_key} must be a non-empty string without whitespace")

    observation_time = parse_time(data["observation_time"], "observation_time")
    window_start = parse_time(data["window_start_exclusive"], "window_start_exclusive")
    if observation_time - window_start != spec.window.size:
        raise ValueError(f"record window must be exactly {spec.window.size}")

    counts: dict[str, int] = {}
    for name in counters:
        value = data[name]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise ValueError(f"{name} must be a non-negative integer")
        counts[name] = value

    derived: dict[str, float] = {}
    for name in derived_names:
        value = data[name]
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError(f"{name} must be a number")
        if not math.isfinite(value):
            raise ValueError(f"{name} must be finite")
        try:
            expected_value = evaluate_expression(spec.derived[name], counts)
        except ZeroDivisionError as exc:
            raise ValueError(f"{name} cannot be evaluated for these counts") from exc
        if not math.isclose(value, expected_value, rel_tol=1e-12, abs_tol=1e-15):
            raise ValueError(f"{name} must match its spec expression")
        derived[name] = value

    return FeatureRecord(
        feature_name=spec.name,
        entity_key=entity_key,
        entity_value=entity_value,
        observation_time=observation_time,
        window_start_exclusive=window_start,
        counts=counts,
        derived=derived,
    )


def _valid_record(spec: FeatureSpec, rng: random.Random) -> dict[str, Any]:
    entity_key, counters, derived_names = record_fields(spec)
    for _ in range(100):
        counts = {name: rng.randint(0, 40) for name in counters}
        try:
            derived = {
                name: evaluate_expression(spec.derived[name], counts) for name in derived_names
            }
        except ZeroDivisionError:
            continue
        break
    else:
        raise AssertionError("no counts without a division by zero")
    observation = datetime(2026, 11, 20, 12, 0, tzinfo=UTC) + timedelta(
        seconds=rng.randint(0, 100000)
    )
    return {
        "feature_schema_version": SCHEMA_VERSION,
        "feature_name": spec.name,
        entity_key: f"user_{rng.randint(0, 999999):06d}",
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - spec.window.size),
        **counts,
        **derived,
    }


def _variants(valid: dict[str, Any], spec: FeatureSpec) -> list[dict[str, Any]]:
    entity_key = record_fields(spec)[0]
    variants: list[dict[str, Any]] = [valid]
    for key in valid:
        variants.append({k: v for k, v in valid.items() if k != key})
        variants.extend({**valid, key: bad} for bad in BAD_VALUES)
    variants.extend(
        [
            {**valid, "extra": 1},
            {**valid, "feature_schema_version": "v2"},
            {**valid, "feature_name": "other"},
            {**valid, entity_key: "has space"},
            {**valid, "window_start_exclusive": valid["observation_time"]},
            {**valid, "observation_time": "2026-11-20T12:00:00"},
        ]
    )
    return variants


def test_specs_were_found() -> None:
    assert SPECS


@pytest.mark.parametrize("spec", SPECS, ids=[spec.name for spec in SPECS])
def test_decoder_matches_the_previous_parser(spec: FeatureSpec) -> None:
    rng = random.Random(17)
    decoder = RecordDecoder(spec)
    ok = failed = 0
    for _ in range(10):
        for variant in _variants(_valid_record(spec, rng), spec):
            text = json.dumps(variant)
            for payload in (text, text.encode()):
                expected = _outcome(_old_parse, payload, spec)
                assert _outcome(decoder.parse, payload) == expected
                assert _outcome(parse_feature_record, payload, spec) == expected
                if expected[0] == "ok":
                    ok += 1
                else:
                    failed += 1
    assert ok > 0
    assert failed > 500
    for raw in ("", "{", "[]", "{}", "null", "1", '"x"'):
        assert _outcome(decoder.parse, raw) == _outcome(_old_parse, raw, spec)
