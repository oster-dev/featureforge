"""Contracts for unified online access without backend infrastructure."""

from datetime import UTC, datetime, timedelta

import pytest

from featureforge.online import BATCH_FEATURES, OnlineFeatureClient
from featureforge.streaming.contracts.user_skip_rate import UserSkipRateFeatureV1
from featureforge.streaming.online_store import UserPlayCountFeatureV1

REFERENCE = datetime(2026, 3, 25, tzinfo=UTC)


def batch_response(timestamp=REFERENCE):
    return {
        key: value
        for _, _, names in BATCH_FEATURES.values()
        for name in names
        for key, value in (
            (name, [0]),
            (f"{name}__ts", [int(timestamp.timestamp())]),
        )
    }


def stream_feature():
    return UserPlayCountFeatureV1(
        user_id="user_000290",
        play_count=0,
        observation_time=REFERENCE,
        window_start_exclusive=REFERENCE - timedelta(hours=1),
    )


def read(client, **kwargs):
    arguments = {
        "user_id": "user_000290",
        "reference_time": REFERENCE,
        "batch_max_age": timedelta(days=1),
        "stream_max_age": timedelta(minutes=30),
    }
    arguments.update(kwargs)
    return client.get_online_features(**arguments)


def make_client(batch=None, stream=None):
    return OnlineFeatureClient(
        feast_reader=lambda service, entity: batch_response() if batch is None else batch,
        stream_reader=lambda user: stream,
    )


def test_combines_user_content_and_stream_without_atomicity_claim():
    response = read(
        make_client(stream=stream_feature()),
        content_id="content_000016",
    )

    assert len(response.features) == 16
    assert response.atomic is False
    assert {item.backend for item in response.features} == {
        "feast",
        "streaming_redis",
    }
    assert all(item.availability == "present" for item in response.features)
    assert all(item.freshness == "fresh" for item in response.features)


def test_zero_is_present_not_missing():
    response = read(make_client(stream=stream_feature()))

    assert all(item.value == 0 for item in response.features)
    assert all(item.availability == "present" for item in response.features)


def test_partial_batch_missing_value_is_not_replaced_with_zero():
    payload = batch_response()
    payload["play_count"] = [None]
    response = read(make_client(batch=payload))

    feature = next(item for item in response.features if item.name == "user_engagement.play_count")
    assert feature.availability == "missing"
    assert feature.value is None


def test_missing_stream_is_explicit():
    feature = read(make_client()).features[-1]

    assert feature.name == "user_play_count_1h"
    assert feature.availability == "missing"
    assert feature.freshness == "unknown"
    assert feature.value is None


def test_absent_timestamp_does_not_mean_fresh():
    payload = batch_response()
    del payload["play_count__ts"]
    response = read(make_client(batch=payload))

    feature = next(item for item in response.features if item.name == "user_engagement.play_count")
    assert feature.availability == "present"
    assert feature.observation_time is None
    assert feature.freshness == "unknown"


@pytest.mark.parametrize(
    ("offset", "expected"),
    [
        (timedelta(0), "fresh"),
        (timedelta(days=1), "fresh"),
        (timedelta(days=1, seconds=1), "stale"),
        (timedelta(seconds=-1), "future"),
    ],
)
def test_batch_freshness_boundary(offset, expected):
    response = read(make_client(batch=batch_response(REFERENCE - offset)))

    assert response.features[0].freshness == expected


def test_backend_failure_is_not_missing_and_does_not_block_stream():
    def broken_feast(service, entity):
        raise ConnectionError("private backend details")

    client = OnlineFeatureClient(
        feast_reader=broken_feast,
        stream_reader=lambda user: stream_feature(),
    )
    response = read(client)

    assert all(item.availability == "error" for item in response.features[:-1])
    assert response.features[-1].availability == "present"
    assert "private backend details" not in str(response.to_dict())


def test_stream_failure_does_not_block_batch():
    def broken_stream(user):
        raise ConnectionError("Redis unavailable")

    client = OnlineFeatureClient(
        feast_reader=lambda service, entity: batch_response(),
        stream_reader=broken_stream,
    )
    response = read(client)

    assert all(item.availability == "present" for item in response.features[:-1])
    assert response.features[-1].availability == "error"


def test_invalid_numeric_value_is_reported_as_error():
    payload = batch_response()
    payload["play_count"] = [True]
    response = read(make_client(batch=payload))

    feature = next(item for item in response.features if item.name == "user_engagement.play_count")
    assert feature.availability == "error"


def test_serialization_keeps_explicit_time_and_schema_metadata():
    payload = read(make_client(stream=stream_feature())).to_dict()

    assert payload["reference_time"] == REFERENCE.isoformat()
    assert payload["atomic"] is False
    assert payload["features"][-1]["schema_version"] == "v1"
    assert payload["features"][-1]["observation_time"] == REFERENCE.isoformat()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"user_id": None},
        {"user_id": " "},
        {"user_id": "user 1"},
        {"reference_time": datetime(2026, 3, 25)},
        {"batch_max_age": timedelta(seconds=-1)},
        {"stream_max_age": timedelta(seconds=-1)},
    ],
)
def test_invalid_request_fails_before_backend_access(kwargs):
    def unexpected(*args):
        raise AssertionError("Backend must not be accessed")

    client = OnlineFeatureClient(
        feast_reader=unexpected,
        stream_reader=unexpected,
    )
    with pytest.raises(ValueError):
        read(client, **kwargs)


def skip_rate_feature(*, user_id="user_000290", play_count=3, skip_count=1):
    total = play_count + skip_count
    return UserSkipRateFeatureV1(
        user_id=user_id,
        play_count=play_count,
        skip_count=skip_count,
        skip_rate=skip_count / total if total else 0.0,
        observation_time=REFERENCE,
        window_start_exclusive=REFERENCE - timedelta(hours=24),
    )


def make_skip_client(skip_reader):
    return OnlineFeatureClient(
        feast_reader=lambda service, entity: batch_response(),
        stream_reader=lambda user: stream_feature(),
        skip_rate_reader=skip_reader,
    )


def skip_item(response):
    return next(item for item in response.features if item.name == "user_skip_rate_24h")


def test_skip_rate_is_read_next_to_play_count():
    response = read(make_skip_client(lambda user: skip_rate_feature()))

    names = [item.name for item in response.features]
    assert names[-2:] == ["user_play_count_1h", "user_skip_rate_24h"]
    item = skip_item(response)
    assert item.value == 0.25
    assert item.availability == "present"
    assert item.freshness == "fresh"
    assert item.backend == "streaming_redis"
    assert item.schema_version == "v1"
    assert item.observation_time == REFERENCE


def test_skip_rate_zero_is_present_not_missing():
    response = read(make_skip_client(lambda user: skip_rate_feature(play_count=0, skip_count=0)))

    item = skip_item(response)
    assert item.value == 0.0
    assert item.availability == "present"


def test_missing_skip_rate_is_explicit_and_not_zero():
    item = skip_item(read(make_skip_client(lambda user: None)))

    assert item.availability == "missing"
    assert item.value is None
    assert item.freshness == "unknown"


def test_skip_rate_failure_is_isolated_and_hides_details():
    def broken(user):
        raise ConnectionError("private skip-rate details")

    response = read(make_skip_client(broken))

    item = skip_item(response)
    assert item.availability == "error"
    assert item.error_type == "ConnectionError"
    assert item.value is None
    assert all(other.availability == "present" for other in response.features if other is not item)
    assert "private skip-rate details" not in str(response.to_dict())


def test_skip_rate_for_another_entity_is_an_error():
    item = skip_item(read(make_skip_client(lambda user: skip_rate_feature(user_id="someone_else"))))

    assert item.availability == "error"
    assert item.error_type == "ValueError"
    assert item.value is None


def test_skip_rate_older_than_the_stream_limit_is_stale():
    response = read(
        make_skip_client(lambda user: skip_rate_feature()),
        reference_time=REFERENCE + timedelta(hours=1),
        batch_max_age=timedelta(days=2),
        stream_max_age=timedelta(minutes=30),
    )

    assert skip_item(response).freshness == "stale"


def test_skip_rate_is_not_read_without_a_reader():
    response = read(make_client(stream=stream_feature()))

    assert "user_skip_rate_24h" not in [item.name for item in response.features]
