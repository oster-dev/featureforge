"""Build a point-in-time-correct historical training dataset with Feast."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from feast import FeatureStore

REPO_PATH = Path(__file__).parent
PROJECT_ROOT = REPO_PATH.parent

LABELS_PATH = PROJECT_ROOT / "data" / "generated" / "labels.parquet"
OUTPUT_PATH = PROJECT_ROOT / "data" / "historical_features.parquet"

FEATURE_REFS = [
    "user_engagement_features:window_days",
    "user_engagement_features:event_count",
    "user_engagement_features:unique_content_count",
    "user_engagement_features:total_watch_seconds",
    "user_engagement_features:search_count",
    "user_engagement_features:play_count",
    "user_engagement_features:watch_count",
    "user_engagement_features:days_since_last_activity",
]


def build_historical_features() -> pd.DataFrame:
    """Retrieve user features at observation times using point-in-time joins."""
    store = FeatureStore(repo_path=str(REPO_PATH))
    labels = pd.read_parquet(LABELS_PATH)

    entity_df = labels[["user_id", "observation_time", "is_active_next_7d"]].rename(
        columns={"observation_time": "event_timestamp"}
    )

    entity_df["event_timestamp"] = pd.to_datetime(
        entity_df["event_timestamp"],
        utc=True,
    )

    return store.get_historical_features(
        entity_df=entity_df,
        features=FEATURE_REFS,
        full_feature_names=True,
    ).to_df()


def main() -> None:
    """Retrieve, validate, and persist historical training features."""
    historical_features = build_historical_features()

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    historical_features.to_parquet(OUTPUT_PATH, index=False)

    input_labels = len(pd.read_parquet(LABELS_PATH))
    retrieved_rows = len(historical_features)
    retrieval_rate = retrieved_rows / input_labels

    positive_rate = historical_features["is_active_next_7d"].mean()

    print("=== Feast Historical Retrieval ===")
    print(f"Input labels:      {input_labels:,}")
    print(f"Retrieved rows:    {retrieved_rows:,}")
    print(f"Retrieval rate:    {retrieval_rate:.1%}")
    print(f"Positive rate:     {positive_rate:.1%}")
    print(
        "Time range:        "
        f"{historical_features['event_timestamp'].min()} to "
        f"{historical_features['event_timestamp'].max()}"
    )
    print(f"Output:            {OUTPUT_PATH}")
    print()
    print("=== Sample Rows ===")
    print(historical_features.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
