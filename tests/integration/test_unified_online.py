"""Verify unified reads against prepared Feast and real Redis."""

import os
from datetime import UTC, timedelta
from pathlib import Path
from uuid import uuid4

import pandas as pd
import pytest
from redis import Redis

from featureforge.online import OnlineFeatureClient
from featureforge.streaming.contracts.user_skip_rate import UserSkipRateFeatureV1
from featureforge.streaming.online_store import (
    RedisUserPlayCountStore,
    UserPlayCountFeatureV1,
)
from featureforge.streaming.skip_rate_store import RedisUserSkipRateStore

pytestmark = pytest.mark.integration

PROJECT_ROOT = Path(__file__).parents[2]
OFFLINE_STORE = PROJECT_ROOT / "output" / "offline_store"


def latest_observation_time(feature_view):
    paths = sorted((OFFLINE_STORE / feature_view).glob("observation_date=*/features.parquet"))
    assert paths, f"No prepared offline snapshots for {feature_view}"

    frame = pd.read_parquet(paths[-1], columns=["observation_time"])
    timestamps = pd.to_datetime(frame["observation_time"], utc=True)

    assert not timestamps.empty
    assert timestamps.notna().all()
    assert timestamps.nunique() == 1

    return timestamps.iloc[0].to_pydatetime().astimezone(UTC)


def test_unified_read_combines_materialized_batch_and_streaming_features():
    user_time = latest_observation_time("user_engagement_features")
    content_time = latest_observation_time("content_popularity_features")
    reference = max(user_time, content_time)

    redis_url = os.environ.get(
        "FEATUREFORGE_REDIS_URL",
        "redis://127.0.0.1:6379/0",
    )
    prefix = f"featureforge-stream:integration:unified:{uuid4().hex}"
    skip_prefix = f"featureforge-stream:integration:unified-skip:{uuid4().hex}"
    user_id = "user_000290"

    feature = UserPlayCountFeatureV1(
        user_id=user_id,
        play_count=0,
        observation_time=reference,
        window_start_exclusive=reference - timedelta(hours=1),
    )
    skip_feature = UserSkipRateFeatureV1(
        user_id=user_id,
        play_count=3,
        skip_count=1,
        skip_rate=0.25,
        observation_time=reference,
        window_start_exclusive=reference - timedelta(hours=24),
    )

    with Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        assert redis_client.ping() is True
        store = RedisUserPlayCountStore(redis_client, key_prefix=prefix)
        skip_store = RedisUserSkipRateStore(redis_client, key_prefix=skip_prefix)

        try:
            assert store.write(feature) == "written"
            assert skip_store.write(skip_feature) == "written"

            client = OnlineFeatureClient.from_local(
                repo_path=PROJECT_ROOT / "feature_repo",
                redis_url=redis_url,
                stream_key_prefix=prefix,
                skip_rate_key_prefix=skip_prefix,
            )
            response = client.get_online_features(
                user_id=user_id,
                content_id="content_000016",
                reference_time=reference,
                batch_max_age=reference - min(user_time, content_time),
                stream_max_age=timedelta(minutes=30),
            )

            assert len(response.features) == 17
            assert response.atomic is False

            for item in response.features:
                assert item.availability == "present", item
                assert item.freshness == "fresh", item

                if item.name.startswith("user_engagement."):
                    assert item.observation_time == user_time
                elif item.name.startswith("content_popularity."):
                    assert item.observation_time == content_time
                elif item.name == "user_skip_rate_24h":
                    assert item.observation_time == reference
                    assert item.value == 0.25
                    assert item.schema_version == "v1"
                else:
                    assert item.name == "user_play_count_1h"
                    assert item.observation_time == reference
                    assert item.value == 0
                    assert item.schema_version == "v1"

            unknown = client.get_online_features(
                user_id=f"unknown_{uuid4().hex}",
                reference_time=reference,
                batch_max_age=timedelta(days=1),
                stream_max_age=timedelta(minutes=30),
            )
            assert all(item.availability == "missing" for item in unknown.features)
        finally:
            redis_client.delete(store.key_for(user_id))
            redis_client.delete(skip_store.key_for(user_id))
