"""Online feature retrieval and deterministic candidate ranking for FeatureForge."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from feast import FeatureStore

USER_FEATURE_SERVICE = "user_engagement_service"
CONTENT_FEATURE_SERVICE = "content_popularity_service"

USER_FEATURE_NAMES = (
    "event_count",
    "unique_content_count",
    "total_watch_seconds",
    "search_count",
    "play_count",
    "watch_count",
    "days_since_last_activity",
)

CONTENT_FEATURE_NAMES = (
    "view_count",
    "unique_viewer_count",
    "total_watch_seconds",
    "average_watch_seconds",
    "search_count",
    "play_count",
    "watch_count",
    "days_since_last_view",
)


@dataclass(frozen=True)
class UserEngagementOnlineFeatures:
    """Current materialized user engagement features."""

    user_id: str
    event_count: int
    unique_content_count: int
    total_watch_seconds: int
    search_count: int
    play_count: int
    watch_count: int
    days_since_last_activity: float | None


@dataclass(frozen=True)
class ContentPopularityOnlineFeatures:
    """Current materialized content popularity features."""

    content_id: str
    view_count: int
    unique_viewer_count: int
    total_watch_seconds: int
    average_watch_seconds: float
    search_count: int
    play_count: int
    watch_count: int
    days_since_last_view: float | None


@dataclass(frozen=True)
class RankedContent:
    """A candidate item ranked using current online user and content features."""

    content_id: str
    score: float
    user_engagement_score: float
    content_popularity_score: float


@dataclass(frozen=True)
class RankingResult:
    """Ranked candidates and candidates skipped for missing online features."""

    user_id: str
    ranked_content: tuple[RankedContent, ...]
    skipped_content_ids: tuple[str, ...]


def _require_non_empty_identifier(value: str, field_name: str) -> None:
    """Validate a non-empty entity identifier."""
    if not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string.")


def _first_value(
    response: dict[str, list[Any]],
    feature_name: str,
) -> Any:
    """Read the first value of a feature from a Feast online response."""
    values = response.get(feature_name)

    if values is None or not values:
        return None

    return values[0]


def _deduplicate_preserving_order(values: Sequence[str]) -> tuple[str, ...]:
    """Deduplicate candidate IDs while keeping their first-occurrence order."""
    return tuple(dict.fromkeys(values))


def _is_missing_feature_record(values: Sequence[Any]) -> bool:
    """Return whether all expected serving features are missing."""
    return all(value is None for value in values)


def _bounded_ratio(value: float, cap: float) -> float:
    """Normalize non-negative values to the inclusive range [0.0, 1.0]."""
    return min(max(value, 0.0) / cap, 1.0)


def _recency_score(days_since_activity: float | None) -> float:
    """Score recency over a seven-day horizon, with missing activity as zero."""
    if days_since_activity is None:
        return 0.0

    return max(0.0, 1.0 - min(float(days_since_activity), 7.0) / 7.0)


def user_engagement_score(features: UserEngagementOnlineFeatures) -> float:
    """Return a transparent normalized engagement score for one user."""
    score = (
        0.30 * _bounded_ratio(features.event_count, 10.0)
        + 0.20 * _bounded_ratio(features.unique_content_count, 10.0)
        + 0.15 * _bounded_ratio(features.total_watch_seconds, 3_600.0)
        + 0.10 * _bounded_ratio(features.search_count, 3.0)
        + 0.10 * _bounded_ratio(features.play_count, 3.0)
        + 0.05 * _bounded_ratio(features.watch_count, 3.0)
        + 0.10 * _recency_score(features.days_since_last_activity)
    )
    return round(score, 6)


def content_popularity_score(features: ContentPopularityOnlineFeatures) -> float:
    """Return a transparent normalized popularity score for one content item."""
    score = (
        0.30 * _bounded_ratio(features.view_count, 50.0)
        + 0.20 * _bounded_ratio(features.unique_viewer_count, 50.0)
        + 0.15 * _bounded_ratio(features.total_watch_seconds, 18_000.0)
        + 0.10 * _bounded_ratio(features.average_watch_seconds, 1_800.0)
        + 0.05 * _bounded_ratio(features.search_count, 10.0)
        + 0.10 * _bounded_ratio(features.play_count, 15.0)
        + 0.05 * _bounded_ratio(features.watch_count, 15.0)
        + 0.05 * _recency_score(features.days_since_last_view)
    )
    return round(score, 6)


def rank_content_candidates(
    user_features: UserEngagementOnlineFeatures,
    content_features: Sequence[ContentPopularityOnlineFeatures],
    *,
    top_k: int,
) -> tuple[RankedContent, ...]:
    """Rank content candidates with stable score and content-ID tie-breaking."""
    if top_k <= 0:
        raise ValueError("top_k must be greater than zero.")

    user_score = user_engagement_score(user_features)

    ranked = [
        RankedContent(
            content_id=content.content_id,
            score=round(
                0.45 * user_score + 0.55 * content_popularity_score(content),
                6,
            ),
            user_engagement_score=user_score,
            content_popularity_score=content_popularity_score(content),
        )
        for content in content_features
    ]

    return tuple(sorted(ranked, key=lambda item: (-item.score, item.content_id))[:top_k])


def get_user_engagement_features(
    user_id: str,
    *,
    repo_path: Path,
) -> UserEngagementOnlineFeatures | None:
    """Fetch one user's current engagement features from Feast's online store."""
    _require_non_empty_identifier(user_id, "user_id")

    store = FeatureStore(repo_path=str(repo_path))
    response = store.get_online_features(
        features=store.get_feature_service(USER_FEATURE_SERVICE),
        entity_rows=[{"user_id": user_id}],
    ).to_dict()

    values = {name: _first_value(response, name) for name in USER_FEATURE_NAMES}

    if _is_missing_feature_record(tuple(values.values())):
        return None

    return UserEngagementOnlineFeatures(
        user_id=user_id,
        event_count=int(values["event_count"] or 0),
        unique_content_count=int(values["unique_content_count"] or 0),
        total_watch_seconds=int(values["total_watch_seconds"] or 0),
        search_count=int(values["search_count"] or 0),
        play_count=int(values["play_count"] or 0),
        watch_count=int(values["watch_count"] or 0),
        days_since_last_activity=(
            None
            if values["days_since_last_activity"] is None
            else float(values["days_since_last_activity"])
        ),
    )


def get_content_popularity_features(
    content_id: str,
    *,
    repo_path: Path,
) -> ContentPopularityOnlineFeatures | None:
    """Fetch one content item's current popularity features from Feast."""
    _require_non_empty_identifier(content_id, "content_id")

    store = FeatureStore(repo_path=str(repo_path))
    response = store.get_online_features(
        features=store.get_feature_service(CONTENT_FEATURE_SERVICE),
        entity_rows=[{"content_id": content_id}],
    ).to_dict()

    values = {name: _first_value(response, name) for name in CONTENT_FEATURE_NAMES}

    if _is_missing_feature_record(tuple(values.values())):
        return None

    return ContentPopularityOnlineFeatures(
        content_id=content_id,
        view_count=int(values["view_count"] or 0),
        unique_viewer_count=int(values["unique_viewer_count"] or 0),
        total_watch_seconds=int(values["total_watch_seconds"] or 0),
        average_watch_seconds=float(values["average_watch_seconds"] or 0.0),
        search_count=int(values["search_count"] or 0),
        play_count=int(values["play_count"] or 0),
        watch_count=int(values["watch_count"] or 0),
        days_since_last_view=(
            None
            if values["days_since_last_view"] is None
            else float(values["days_since_last_view"])
        ),
    )


def get_content_popularity_features_batch(
    content_ids: Sequence[str],
    *,
    repo_path: Path,
) -> tuple[
    tuple[ContentPopularityOnlineFeatures, ...],
    tuple[str, ...],
]:
    """Fetch materialized content features and report missing candidates."""
    if not content_ids:
        raise ValueError("content_ids must contain at least one candidate.")

    unique_content_ids = _deduplicate_preserving_order(content_ids)

    for content_id in unique_content_ids:
        _require_non_empty_identifier(content_id, "content_id")

    store = FeatureStore(repo_path=str(repo_path))
    response = store.get_online_features(
        features=store.get_feature_service(CONTENT_FEATURE_SERVICE),
        entity_rows=[{"content_id": content_id} for content_id in unique_content_ids],
    ).to_dict()

    found: list[ContentPopularityOnlineFeatures] = []
    skipped: list[str] = []

    for index, content_id in enumerate(unique_content_ids):
        values = {
            name: response.get(name, [None] * len(unique_content_ids))[index]
            for name in CONTENT_FEATURE_NAMES
        }

        if _is_missing_feature_record(tuple(values.values())):
            skipped.append(content_id)
            continue

        found.append(
            ContentPopularityOnlineFeatures(
                content_id=content_id,
                view_count=int(values["view_count"] or 0),
                unique_viewer_count=int(values["unique_viewer_count"] or 0),
                total_watch_seconds=int(values["total_watch_seconds"] or 0),
                average_watch_seconds=float(values["average_watch_seconds"] or 0.0),
                search_count=int(values["search_count"] or 0),
                play_count=int(values["play_count"] or 0),
                watch_count=int(values["watch_count"] or 0),
                days_since_last_view=(
                    None
                    if values["days_since_last_view"] is None
                    else float(values["days_since_last_view"])
                ),
            )
        )

    return tuple(found), tuple(skipped)


def rank_online_content_candidates(
    user_id: str,
    candidate_content_ids: Sequence[str],
    *,
    top_k: int,
    repo_path: Path,
) -> RankingResult:
    """Retrieve online features and rank materialized content candidates."""
    user_features = get_user_engagement_features(user_id, repo_path=repo_path)

    if user_features is None:
        raise LookupError(f"User {user_id!r} is not materialized in the online feature store.")

    content_features, skipped_content_ids = get_content_popularity_features_batch(
        candidate_content_ids,
        repo_path=repo_path,
    )

    if not content_features:
        raise LookupError("None of the requested content candidates are materialized.")

    ranked_content = rank_content_candidates(
        user_features,
        content_features,
        top_k=top_k,
    )

    return RankingResult(
        user_id=user_id,
        ranked_content=ranked_content,
        skipped_content_ids=skipped_content_ids,
    )
