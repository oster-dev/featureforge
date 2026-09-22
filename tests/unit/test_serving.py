"""Unit tests for online serving score and ranking contracts."""

from __future__ import annotations

import pytest

from featureforge.serving import (
    ContentPopularityOnlineFeatures,
    UserEngagementOnlineFeatures,
    content_popularity_score,
    rank_content_candidates,
    user_engagement_score,
)


def build_user_features(
    *,
    user_id: str = "user_000001",
    event_count: int = 0,
    unique_content_count: int = 0,
    total_watch_seconds: int = 0,
    search_count: int = 0,
    play_count: int = 0,
    watch_count: int = 0,
    days_since_last_activity: float | None = None,
) -> UserEngagementOnlineFeatures:
    """Build a user feature record with explicit values for one test."""
    return UserEngagementOnlineFeatures(
        user_id=user_id,
        event_count=event_count,
        unique_content_count=unique_content_count,
        total_watch_seconds=total_watch_seconds,
        search_count=search_count,
        play_count=play_count,
        watch_count=watch_count,
        days_since_last_activity=days_since_last_activity,
    )


def build_content_features(
    *,
    content_id: str,
    view_count: int = 0,
    unique_viewer_count: int = 0,
    total_watch_seconds: int = 0,
    average_watch_seconds: float = 0.0,
    search_count: int = 0,
    play_count: int = 0,
    watch_count: int = 0,
    days_since_last_view: float | None = None,
) -> ContentPopularityOnlineFeatures:
    """Build a content feature record with explicit values for one test."""
    return ContentPopularityOnlineFeatures(
        content_id=content_id,
        view_count=view_count,
        unique_viewer_count=unique_viewer_count,
        total_watch_seconds=total_watch_seconds,
        average_watch_seconds=average_watch_seconds,
        search_count=search_count,
        play_count=play_count,
        watch_count=watch_count,
        days_since_last_view=days_since_last_view,
    )


def test_user_engagement_score_is_zero_without_activity() -> None:
    """No activity and no recency signal must produce a zero user score."""
    assert user_engagement_score(build_user_features()) == 0.0


def test_user_engagement_score_is_one_at_all_feature_caps() -> None:
    """All user features at their caps with immediate activity score one."""
    features = build_user_features(
        event_count=10,
        unique_content_count=10,
        total_watch_seconds=3_600,
        search_count=3,
        play_count=3,
        watch_count=3,
        days_since_last_activity=0.0,
    )

    assert user_engagement_score(features) == 1.0


def test_user_engagement_score_caps_large_feature_values() -> None:
    """Values above caps must not increase the normalized user score."""
    at_cap = build_user_features(
        event_count=10,
        unique_content_count=10,
        total_watch_seconds=3_600,
        search_count=3,
        play_count=3,
        watch_count=3,
        days_since_last_activity=0.0,
    )
    above_cap = build_user_features(
        event_count=100,
        unique_content_count=100,
        total_watch_seconds=36_000,
        search_count=30,
        play_count=30,
        watch_count=30,
        days_since_last_activity=0.0,
    )

    assert user_engagement_score(above_cap) == user_engagement_score(at_cap)


def test_user_engagement_score_penalizes_stale_activity() -> None:
    """A seven-day-old event contributes zero to the recency component."""
    recent = build_user_features(days_since_last_activity=0.0)
    stale = build_user_features(days_since_last_activity=7.0)

    assert user_engagement_score(recent) == 0.1
    assert user_engagement_score(stale) == 0.0


def test_content_popularity_score_is_zero_without_activity() -> None:
    """No popularity signal and no recency signal must produce zero."""
    content = build_content_features(content_id="content_000001")

    assert content_popularity_score(content) == 0.0


def test_content_popularity_score_is_one_at_all_feature_caps() -> None:
    """All content features at their caps with immediate activity score one."""
    content = build_content_features(
        content_id="content_000001",
        view_count=50,
        unique_viewer_count=50,
        total_watch_seconds=18_000,
        average_watch_seconds=1_800.0,
        search_count=10,
        play_count=15,
        watch_count=15,
        days_since_last_view=0.0,
    )

    assert content_popularity_score(content) == 1.0


def test_content_popularity_score_caps_large_feature_values() -> None:
    """Values above caps must not increase normalized popularity score."""
    at_cap = build_content_features(
        content_id="content_000001",
        view_count=50,
        unique_viewer_count=50,
        total_watch_seconds=18_000,
        average_watch_seconds=1_800.0,
        search_count=10,
        play_count=15,
        watch_count=15,
        days_since_last_view=0.0,
    )
    above_cap = build_content_features(
        content_id="content_000001",
        view_count=500,
        unique_viewer_count=500,
        total_watch_seconds=180_000,
        average_watch_seconds=18_000.0,
        search_count=100,
        play_count=150,
        watch_count=150,
        days_since_last_view=0.0,
    )

    assert content_popularity_score(above_cap) == content_popularity_score(at_cap)


def test_rank_content_candidates_orders_descending_by_score() -> None:
    """Candidates must be sorted by descending combined ranking score."""
    user = build_user_features(event_count=5, days_since_last_activity=1.0)

    low_popularity = build_content_features(
        content_id="content_low",
        view_count=2,
        days_since_last_view=6.0,
    )
    high_popularity = build_content_features(
        content_id="content_high",
        view_count=40,
        unique_viewer_count=35,
        total_watch_seconds=15_000,
        average_watch_seconds=1_200.0,
        play_count=12,
        watch_count=10,
        days_since_last_view=0.5,
    )

    ranked = rank_content_candidates(
        user,
        [low_popularity, high_popularity],
        top_k=2,
    )

    assert [item.content_id for item in ranked] == [
        "content_high",
        "content_low",
    ]
    assert ranked[0].score > ranked[1].score


def test_rank_content_candidates_uses_content_id_as_stable_tie_breaker() -> None:
    """Equal-score candidates must sort deterministically by content ID."""
    user = build_user_features()
    content_b = build_content_features(content_id="content_b")
    content_a = build_content_features(content_id="content_a")

    ranked = rank_content_candidates(
        user,
        [content_b, content_a],
        top_k=2,
    )

    assert [item.content_id for item in ranked] == ["content_a", "content_b"]


def test_rank_content_candidates_limits_result_to_top_k() -> None:
    """Ranking must return no more than the requested number of candidates."""
    user = build_user_features(event_count=1)
    content_features = [
        build_content_features(content_id=f"content_{index:03d}", view_count=index)
        for index in range(1, 6)
    ]

    ranked = rank_content_candidates(user, content_features, top_k=3)

    assert len(ranked) == 3
    assert [item.content_id for item in ranked] == [
        "content_005",
        "content_004",
        "content_003",
    ]


@pytest.mark.parametrize("top_k", [0, -1])
def test_rank_content_candidates_rejects_non_positive_top_k(top_k: int) -> None:
    """Ranking must reject invalid requested result sizes."""
    with pytest.raises(ValueError, match="top_k must be greater than zero"):
        rank_content_candidates(
            build_user_features(),
            [build_content_features(content_id="content_000001")],
            top_k=top_k,
        )
