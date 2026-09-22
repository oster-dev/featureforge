"""Fetch current user engagement features from Feast's Redis online store."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

from feast import FeatureStore

REPO_PATH = Path(__file__).parent
FEATURE_SERVICE_NAME = "user_engagement_service"
DEFAULT_USER_ID = "user_000290"

FEATURE_ORDER = (
    "event_count",
    "unique_content_count",
    "total_watch_seconds",
    "search_count",
    "play_count",
    "watch_count",
    "days_since_last_activity",
)


def parse_args() -> argparse.Namespace:
    """Parse the user entity requested for online feature lookup."""
    parser = argparse.ArgumentParser(description="Fetch user engagement features from Feast Redis.")
    parser.add_argument(
        "--user-id",
        default=DEFAULT_USER_ID,
        help=f"User entity ID to retrieve (default: {DEFAULT_USER_ID}).",
    )
    return parser.parse_args()


def get_user_features(user_id: str) -> dict[str, Any] | None:
    """Retrieve current user engagement features from Feast's online store."""
    store = FeatureStore(repo_path=str(REPO_PATH))

    response = store.get_online_features(
        features=store.get_feature_service(FEATURE_SERVICE_NAME),
        entity_rows=[{"user_id": user_id}],
    ).to_dict()

    feature_values = {
        feature_name: response.get(feature_name, [None])[0] for feature_name in FEATURE_ORDER
    }

    if all(value is None for value in feature_values.values()):
        return None

    return feature_values


def engagement_score(features: dict[str, Any]) -> float:
    """Calculate a transparent deterministic score from online feature values."""
    event_count = float(features["event_count"] or 0)
    unique_content_count = float(features["unique_content_count"] or 0)
    total_watch_seconds = float(features["total_watch_seconds"] or 0)
    search_count = float(features["search_count"] or 0)
    play_count = float(features["play_count"] or 0)
    watch_count = float(features["watch_count"] or 0)
    days_since_last_activity = features["days_since_last_activity"]

    recency_component = (
        0.0
        if days_since_last_activity is None
        else max(0.0, 1.0 - min(float(days_since_last_activity), 7.0) / 7.0)
    )

    raw_score = (
        0.30 * min(event_count / 10.0, 1.0)
        + 0.20 * min(unique_content_count / 10.0, 1.0)
        + 0.15 * min(total_watch_seconds / 3_600.0, 1.0)
        + 0.10 * min(search_count / 3.0, 1.0)
        + 0.10 * min(play_count / 3.0, 1.0)
        + 0.05 * min(watch_count / 3.0, 1.0)
        + 0.10 * recency_component
    )

    return round(raw_score, 4)


def engagement_segment(score: float) -> str:
    """Map the demo score to a human-readable deterministic segment."""
    if score >= 0.65:
        return "high_engagement"
    if score >= 0.30:
        return "medium_engagement"
    return "low_engagement"


def print_result(
    user_id: str,
    features: dict[str, Any] | None,
) -> None:
    """Print lookup values and the resulting demo decision."""
    print("=== Feast Online Feature Lookup ===")
    print(f"Feature service: {FEATURE_SERVICE_NAME}")
    print(f"User ID:         {user_id}")

    if features is None:
        print("\nStatus: user not materialized in the online store.")
        return

    print("\n=== Current User Engagement Features ===")
    for feature_name in FEATURE_ORDER:
        print(f"{feature_name:28} {features[feature_name]}")

    score = engagement_score(features)
    segment = engagement_segment(score)

    print("\n=== Deterministic Engagement Decision ===")
    print(f"Engagement score: {score:.4f}")
    print(f"Segment:          {segment}")


def main() -> None:
    """Run an online feature lookup and a transparent demo decision."""
    args = parse_args()
    features = get_user_features(args.user_id)
    print_result(args.user_id, features)


if __name__ == "__main__":
    main()
