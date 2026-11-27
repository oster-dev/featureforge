"""Parse and serialize spec-driven feature records (ADR-009)."""

import json
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.model import Aggregation, Op
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.online_store import UserPlayCountFeatureV1
from featureforge.streaming.skip_rate_store import UserSkipRateFeatureV1

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)

PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
SKIP = load_spec(SPECS / "user_skip_rate_24h.yaml")
CONTENT = load_spec(SPECS / "content_play_count_1h.yaml")

MISSING = object()


def payload(spec, entity, counts, *, minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    data = {
        "feature_schema_version": "v1",
        "feature_name": spec.name,
        spec.keys[0]: entity,
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - spec.window.size),
        **counts,
    }
    for name, expression in spec.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return data


def parse(spec, data):
    return parse_feature_record(json.dumps(data), spec)


@pytest.mark.parametrize(
    ("spec", "entity", "counts"),
    [
        (PLAY, "user_000001", {"play_count": 3}),
        (SKIP, "user_000001", {"play_count": 2, "skip_count": 1}),
        (SKIP, "user_000001", {"play_count": 0, "skip_count": 0}),
        (CONTENT, "content_000001", {"play_count": 4}),
    ],
)
def test_round_trip_keeps_the_flink_wire_format(spec, entity, counts):
    text = json.dumps(payload(spec, entity, counts), sort_keys=True, separators=(",", ":"))
    record = parse_feature_record(text, spec)
    assert record.to_json() == text
    assert record.entity_value == entity
    assert dict(record.counts) == counts


def test_record_exposes_derived_values():
    record = parse(SKIP, payload(SKIP, "user_000001", {"play_count": 3, "skip_count": 1}))
    assert record.derived == {"skip_rate": pytest.approx(0.25)}


def test_reads_what_the_hand_written_play_count_model_writes():
    feature = UserPlayCountFeatureV1(
        user_id="user_000001",
        play_count=3,
        observation_time=OBSERVATION,
        window_start_exclusive=OBSERVATION - timedelta(hours=1),
    )
    record = parse_feature_record(feature.model_dump_json(), PLAY)
    assert dict(record.counts) == {"play_count": 3}
    assert record.entity_value == "user_000001"
    assert UserPlayCountFeatureV1.model_validate_json(record.to_json()) == feature


def test_reads_what_the_hand_written_skip_rate_model_writes():
    feature = UserSkipRateFeatureV1(
        user_id="user_000001",
        play_count=2,
        skip_count=1,
        skip_rate=1 / 3,
        observation_time=OBSERVATION,
        window_start_exclusive=OBSERVATION - timedelta(hours=24),
    )
    record = parse_feature_record(feature.model_dump_json(), SKIP)
    assert dict(record.counts) == {"play_count": 2, "skip_count": 1}
    assert UserSkipRateFeatureV1.model_validate_json(record.to_json()) == feature


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("feature_schema_version", "v2"),
        ("feature_name", "user_skip_rate_24h"),
        ("user_id", ""),
        ("user_id", "user 1"),
        ("user_id", "user_1\n"),
        ("user_id", 123),
        ("user_id", MISSING),
        ("play_count", -1),
        ("play_count", True),
        ("play_count", 1.0),
        ("play_count", "1"),
        ("play_count", MISSING),
        ("observation_time", "2026-01-02T12:00:00"),
        ("observation_time", "not a time"),
        ("observation_time", 12),
        ("window_start_exclusive", stamp(OBSERVATION - timedelta(hours=2))),
        ("unexpected_field", "value"),
    ],
)
def test_rejects_invalid_play_count_records(field, value):
    data = payload(PLAY, "user_000001", {"play_count": 3})
    if value is MISSING:
        del data[field]
    else:
        data[field] = value
    with pytest.raises(ValueError):
        parse(PLAY, data)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("skip_rate", 0.9),
        ("skip_rate", float("nan")),
        ("skip_rate", "0.5"),
        ("skip_rate", True),
        ("skip_rate", MISSING),
        ("skip_count", -1),
        ("window_start_exclusive", stamp(OBSERVATION - timedelta(hours=1))),
    ],
)
def test_rejects_invalid_skip_rate_records(field, value):
    data = payload(SKIP, "user_000001", {"play_count": 2, "skip_count": 1})
    if value is MISSING:
        del data[field]
    else:
        data[field] = value
    with pytest.raises(ValueError):
        parse(SKIP, data)


def test_zero_denominator_requires_zero_rate():
    data = payload(SKIP, "user_000001", {"play_count": 0, "skip_count": 0})
    data["skip_rate"] = 0.5
    with pytest.raises(ValueError, match="skip_rate"):
        parse(SKIP, data)


def test_equivalent_timestamp_offsets_normalize_to_utc():
    data = payload(PLAY, "user_000001", {"play_count": 1})
    offset = timezone(timedelta(hours=2))
    data["observation_time"] = OBSERVATION.astimezone(offset).isoformat()
    data["window_start_exclusive"] = (OBSERVATION - PLAY.window.size).astimezone(offset).isoformat()
    record = parse(PLAY, data)
    assert record.observation_time == OBSERVATION
    assert record.observation_time.tzinfo == UTC


def test_composite_entity_keys_are_rejected():
    spec = PLAY.model_copy(update={"entity": ["user_id", "content_id"]})
    with pytest.raises(ValueError, match="composite"):
        parse_feature_record("{}", spec)


def test_non_count_aggregations_are_rejected():
    average = Aggregation(name="avg_play_seconds", op=Op.AVG, column="play_seconds")
    spec = PLAY.model_copy(update={"aggregations": [average]})
    with pytest.raises(ValueError, match="count and sum aggregations"):
        parse_feature_record("{}", spec)


def test_record_must_be_a_json_object():
    with pytest.raises(ValueError, match="JSON object"):
        parse_feature_record("[]", PLAY)


def test_invalid_json_is_a_value_error():
    with pytest.raises(ValueError):
        parse_feature_record("{", PLAY)
