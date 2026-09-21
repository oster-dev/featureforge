"""Train and evaluate a point-in-time baseline model."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import precision_score, recall_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

DATA_PATH = Path("data/historical_features.parquet")
OUTPUT_DIR = Path("output/models")

TIME_COL = "event_timestamp"
TARGET_COL = "is_active_next_7d"

FEATURE_COLS = [
    "user_engagement_features__event_count",
    "user_engagement_features__unique_content_count",
    "user_engagement_features__total_watch_seconds",
    "user_engagement_features__search_count",
    "user_engagement_features__play_count",
    "user_engagement_features__watch_count",
    "user_engagement_features__days_since_last_activity",
]

MISSING_ACTIVITY_FILL_VALUE = 999.0
TOP_K = 25


def load_data() -> pd.DataFrame:
    """Load historical features and apply deterministic missing-value handling."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Training dataset not found: {DATA_PATH}. "
            "Run feature_repo/historical_retrieval_demo.py first."
        )

    df = pd.read_parquet(DATA_PATH).copy()
    required_columns = [TIME_COL, TARGET_COL, *FEATURE_COLS]
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        raise ValueError(
            "Training dataset is missing required columns: " + ", ".join(missing_columns)
        )

    df[TIME_COL] = pd.to_datetime(df[TIME_COL], utc=True)
    df[TARGET_COL] = df[TARGET_COL].astype(bool)
    df["user_engagement_features__days_since_last_activity"] = df[
        "user_engagement_features__days_since_last_activity"
    ].fillna(MISSING_ACTIVITY_FILL_VALUE)

    return df


def time_based_split(
    df: pd.DataFrame,
    train_ratio: float = 0.70,
    validation_ratio: float = 0.15,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Create chronological train, validation, and test partitions."""
    if train_ratio <= 0 or validation_ratio <= 0:
        raise ValueError("train_ratio and validation_ratio must be positive.")

    if train_ratio + validation_ratio >= 1:
        raise ValueError("train_ratio + validation_ratio must be less than 1.")

    ordered = df.sort_values(TIME_COL).reset_index(drop=True)

    train_end = int(len(ordered) * train_ratio)
    validation_end = int(len(ordered) * (train_ratio + validation_ratio))

    train = ordered.iloc[:train_end].copy()
    validation = ordered.iloc[train_end:validation_end].copy()
    test = ordered.iloc[validation_end:].copy()

    if min(len(train), len(validation), len(test)) == 0:
        raise ValueError("Time split produced an empty dataset partition.")

    return train, validation, test


def precision_at_k(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    k: int,
) -> float:
    """Return the positive-label fraction among the k highest scores."""
    effective_k = min(k, len(y_true))

    if effective_k == 0:
        return 0.0

    top_indices = np.argsort(probabilities)[-effective_k:]
    return float(y_true[top_indices].mean())


def recall_at_k(
    y_true: np.ndarray,
    probabilities: np.ndarray,
    k: int,
) -> float:
    """Return the fraction of all positives captured in the top-k scores."""
    total_positives = int(y_true.sum())

    if total_positives == 0:
        return 0.0

    effective_k = min(k, len(y_true))
    top_indices = np.argsort(probabilities)[-effective_k:]

    return float(y_true[top_indices].sum() / total_positives)


def evaluate(
    model: Pipeline,
    split: pd.DataFrame,
    split_name: str,
) -> dict[str, Any]:
    """Evaluate one chronological dataset partition."""
    X = split[FEATURE_COLS]
    y = split[TARGET_COL].to_numpy()

    probabilities = model.predict_proba(X)[:, 1]
    predictions = model.predict(X)

    auc = float(roc_auc_score(y, probabilities)) if len(np.unique(y)) == 2 else None

    metrics = {
        "rows": int(len(split)),
        "positive_rate": float(y.mean()),
        "auc": auc,
        "precision": float(precision_score(y, predictions, zero_division=0)),
        "recall": float(recall_score(y, predictions, zero_division=0)),
        f"precision_at_{TOP_K}": precision_at_k(y, probabilities, TOP_K),
        f"recall_at_{TOP_K}": recall_at_k(y, probabilities, TOP_K),
        "start_time": str(split[TIME_COL].min()),
        "end_time": str(split[TIME_COL].max()),
    }

    print(f"\n{split_name} Metrics")
    print("-" * (len(split_name) + 8))
    print(f"Rows:            {metrics['rows']}")
    print(f"Positive rate:   {metrics['positive_rate']:.1%}")
    print(f"AUC:             {metrics['auc']:.4f}" if auc is not None else "AUC: n/a")
    print(f"Precision:       {metrics['precision']:.4f}")
    print(f"Recall:          {metrics['recall']:.4f}")
    print(f"Precision@{TOP_K}:  {metrics[f'precision_at_{TOP_K}']:.4f}")
    print(f"Recall@{TOP_K}:     {metrics[f'recall_at_{TOP_K}']:.4f}")

    return metrics


def print_feature_coefficients(model: Pipeline) -> None:
    """Print standardized Logistic Regression coefficients by absolute magnitude."""
    classifier = model.named_steps["classifier"]

    coefficients = sorted(
        zip(FEATURE_COLS, classifier.coef_[0], strict=True),
        key=lambda item: abs(item[1]),
        reverse=True,
    )

    print("\nFeature Coefficients")
    print("-" * 20)

    for feature_name, coefficient in coefficients:
        print(f"{feature_name:60} {coefficient:+.4f}")


def main() -> None:
    """Train, evaluate, and persist the baseline model pipeline."""
    print("=== Loading Historical Training Data ===")
    df = load_data()
    print(f"Rows: {len(df):,}")
    print(f"Time range: {df[TIME_COL].min()} to {df[TIME_COL].max()}")
    print(f"Overall positive rate: {df[TARGET_COL].mean():.1%}")

    print("\n=== Time-Based Split ===")
    train, validation, test = time_based_split(df)

    for split_name, split in (
        ("Train", train),
        ("Validation", validation),
        ("Test", test),
    ):
        print(
            f"{split_name:10} {len(split):4} rows | "
            f"{split[TIME_COL].min()} to {split[TIME_COL].max()} | "
            f"positive rate={split[TARGET_COL].mean():.1%}"
        )

    model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            (
                "classifier",
                LogisticRegression(
                    max_iter=1_000,
                    class_weight="balanced",
                    random_state=42,
                ),
            ),
        ]
    )

    print("\n=== Training Logistic Regression Baseline ===")
    model.fit(train[FEATURE_COLS], train[TARGET_COL])

    print_feature_coefficients(model)

    print("\n=== Evaluation ===")
    all_metrics = {
        "train": evaluate(model, train, "Train"),
        "validation": evaluate(model, validation, "Validation"),
        "test": evaluate(model, test, "Test"),
        "features": FEATURE_COLS,
        "missing_activity_fill_value": MISSING_ACTIVITY_FILL_VALUE,
        "top_k": TOP_K,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    model_path = OUTPUT_DIR / "baseline_logreg_pipeline.joblib"
    metrics_path = OUTPUT_DIR / "baseline_logreg_metrics.json"

    joblib.dump(model, model_path)

    with metrics_path.open("w", encoding="utf-8") as file:
        json.dump(all_metrics, file, indent=2)

    print("\n=== Artifacts ===")
    print(f"Model pipeline: {model_path}")
    print(f"Metrics:        {metrics_path}")


if __name__ == "__main__":
    main()
