"""Live integration tests for Feast Redis online serving.

These tests verify the current local serving environment. They require:

1. Redis running through Docker Compose.
2. Feast definitions applied from feature_repo/.
3. Offline feature snapshots in output/offline_store/.
4. Feature views materialized to Redis.

They skip cleanly when that local integration environment is unavailable.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest
from redis import Redis
from redis.exceptions import RedisError

from featureforge.serving import (
    get_content_popularity_features,
    get_content_popularity_features_batch,
    get_user_engagement_features,
    rank_online_content_candidates,
)

PROJECT_ROOT = Path(__file__).parents[2]
FEATURE_REPO_PATH = PROJECT_ROOT / "feature_repo"
OFFLINE_STORE_PATH = PROJECT_ROOT / "output" / "offline_store"

KNOWN_USER_ID = "user_000290"
KNOWN_CONTENT_ID = "content_000016"
UNKNOWN_USER_ID = "user_unknown_999"
UNKNOWN_CONTENT_ID = "content_unknown_999"

USER_FEATURE_COLUMNS = (
    "event_count",
    "unique_content_count",
    "total_watch_seconds",
    "search_count",
    "play_count",
    "watch_count",
    "days_since_last_activity",
)

CONTENT_FEATURE_COLUMNS = (
    "view_count",
    "unique_viewer_count",
    "total_watch_seconds",
    "average_watch_seconds",
    "search_count",
    "play_count",
    "watch_count",
    "days_since_last_view",
)


def _latest_feature_snapshot_path(
    offline_store_path: Path,
    feature_view: str,
) -> Path | None:
    """Return the newest available canonical feature partition path."""
    feature_view_dir = offline_store_path / feature_view

    candidates = sorted(
        feature_view_dir.glob("observation_date=*/features.parquet"),
        key=lambda path: path.parent.name,
    )

    if not candidates:
        return None

    return candidates[-1]


def _local_serving_environment_available() -> bool:
    """Return whether required local files and Redis are available."""
    if not FEATURE_REPO_PATH.is_dir():
        return False

    user_features_path = _latest_feature_snapshot_path(
        OFFLINE_STORE_PATH,
        "user_engagement_features",
    )
    content_features_path = _latest_feature_snapshot_path(
        OFFLINE_STORE_PATH,
        "content_popularity_features",
    )

    if user_features_path is None or content_features_path is None:
        return False

    try:
        return bool(Redis(host="localhost", port=6379).ping())
    except RedisError:
        return False


pytestmark = pytest.mark.skipif(
    not _local_serving_environment_available(),
    reason=(
        "Requires local Redis plus materialized FeatureForge offline snapshots. "
        "Run Docker Compose, feast apply, and feast materialize first."
    ),
)


@pytest.fixture(scope="module")
def user_offline_snapshot() -> pd.DataFrame:
    """Load the newest canonical user feature snapshot once per module."""
    path = _latest_feature_snapshot_path(
        OFFLINE_STORE_PATH,
        "user_engagement_features",
    )

    assert path is not None
    return pd.read_parquet(path)


@pytest.fixture(scope="module")
def content_offline_snapshot() -> pd.DataFrame:
    """Load the newest canonical content feature snapshot once per module."""
    path = _latest_feature_snapshot_path(
        OFFLINE_STORE_PATH,
        "content_popularity_features",
    )

    assert path is not None
    return pd.read_parquet(path)


def _expected_row(
    snapshot: pd.DataFrame,
    *,
    entity_column: str,
    entity_id: str,
) -> pd.Series:
    """Return one expected offline row or fail with a clear test assertion."""
    matched = snapshot.loc[snapshot[entity_column] == entity_id]

    assert len(matched) == 1, (
        f"Expected exactly one offline row for {entity_column}={entity_id!r}, found {len(matched)}."
    )

    return matched.iloc[0]


def _assert_online_value_matches_offline(
    *,
    online_value: int | float | None,
    offline_value: int | float,
    feature_name: str,
) -> None:
    """Compare numeric values while treating offline NaN as online None."""
    if pd.isna(offline_value):
        assert online_value is None, (
            f"{feature_name}: expected online None for offline NaN, got {online_value!r}."
        )
        return

    assert online_value is not None, (
        f"{feature_name}: expected online value {offline_value!r}, got None."
    )
    assert online_value == pytest.approx(float(offline_value)), (
        f"{feature_name}: online={online_value!r}, offline={offline_value!r}."
    )


def test_user_online_features_match_latest_offline_snapshot(
    user_offline_snapshot: pd.DataFrame,
) -> None:
    """Materialized user features must equal the latest canonical offline snapshot."""
    online = get_user_engagement_features(
        KNOWN_USER_ID,
        repo_path=FEATURE_REPO_PATH,
    )

    assert online is not None
    assert online.user_id == KNOWN_USER_ID

    expected = _expected_row(
        user_offline_snapshot,
        entity_column="user_id",
        entity_id=KNOWN_USER_ID,
    )

    for feature_name in USER_FEATURE_COLUMNS:
        _assert_online_value_matches_offline(
            online_value=getattr(online, feature_name),
            offline_value=expected[feature_name],
            feature_name=feature_name,
        )


def test_content_online_features_match_latest_offline_snapshot(
    content_offline_snapshot: pd.DataFrame,
) -> None:
    """Materialized content features must equal the latest canonical offline snapshot."""
    online = get_content_popularity_features(
        KNOWN_CONTENT_ID,
        repo_path=FEATURE_REPO_PATH,
    )

    assert online is not None
    assert online.content_id == KNOWN_CONTENT_ID

    expected = _expected_row(
        content_offline_snapshot,
        entity_column="content_id",
        entity_id=KNOWN_CONTENT_ID,
    )

    for feature_name in CONTENT_FEATURE_COLUMNS:
        _assert_online_value_matches_offline(
            online_value=getattr(online, feature_name),
            offline_value=expected[feature_name],
            feature_name=feature_name,
        )


def test_unknown_entities_are_not_returned_from_online_store() -> None:
    """Unknown entities must return None instead of fabricated defaults."""
    assert (
        get_user_engagement_features(
            UNKNOWN_USER_ID,
            repo_path=FEATURE_REPO_PATH,
        )
        is None
    )
    assert (
        get_content_popularity_features(
            UNKNOWN_CONTENT_ID,
            repo_path=FEATURE_REPO_PATH,
        )
        is None
    )


def test_batch_content_lookup_skips_unknown_candidate() -> None:
    """Batch retrieval returns known features and explicitly skips unknown IDs."""
    found, skipped = get_content_popularity_features_batch(
        [KNOWN_CONTENT_ID, UNKNOWN_CONTENT_ID, KNOWN_CONTENT_ID],
        repo_path=FEATURE_REPO_PATH,
    )

    assert [item.content_id for item in found] == [KNOWN_CONTENT_ID]
    assert skipped == (UNKNOWN_CONTENT_ID,)


def test_online_candidate_ranking_returns_ranked_materialized_content() -> None:
    """Ranking must use online features and report skipped content candidates."""
    result = rank_online_content_candidates(
        KNOWN_USER_ID,
        [
            "content_000016",
            "content_000089",
            "content_000108",
            UNKNOWN_CONTENT_ID,
        ],
        top_k=3,
        repo_path=FEATURE_REPO_PATH,
    )

    assert result.user_id == KNOWN_USER_ID
    assert len(result.ranked_content) == 3
    assert result.skipped_content_ids == (UNKNOWN_CONTENT_ID,)

    scores = [item.score for item in result.ranked_content]
    assert scores == sorted(scores, reverse=True)
