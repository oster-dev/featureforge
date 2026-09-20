"""Run a point-in-time historical feature retrieval with Feast."""

from pathlib import Path

import pandas as pd
from feast import FeatureStore


REPO_PATH = Path(__file__).parent
LABELS_PATH = REPO_PATH.parent / "data" / "generated" / "labels.parquet"


def main() -> None:
    """Retrieve user engagement features at label observation times."""
    store = FeatureStore(repo_path=str(REPO_PATH))

    labels = pd.read_parquet(LABELS_PATH)

    entity_df = labels[
        ["user_id", "observation_time", "is_active_next_7d"]
    ].rename(columns={"observation_time": "event_timestamp"})

    entity_df["event_timestamp"] = pd.to_datetime(
        entity_df["event_timestamp"],
        utc=True,
    )

    historical_features = store.get_historical_features(
        entity_df=entity_df,
        features=[
            "user_engagement_features:window_days",
            "user_engagement_features:event_count",
            "user_engagement_features:unique_content_count",
            "user_engagement_features:total_watch_seconds",
            "user_engagement_features:search_count",
            "user_engagement_features:play_count",
            "user_engagement_features:watch_count",
            "user_engagement_features:days_since_last_activity",
        ],
        full_feature_names=True,
    ).to_df()

    print(f"Input labels: {len(entity_df)}")
    print(f"Retrieved rows: {len(historical_features)}")
    print()
    print(historical_features.head(10).to_string(index=False))


if __name__ == "__main__":
    main()