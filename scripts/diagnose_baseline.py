"""Diagnose weak out-of-time baseline model performance."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score

DATA_PATH = Path("data/historical_features.parquet")

TIME_COL = "event_timestamp"
TARGET_COL = "is_active_next_7d"

FEATURE_COLS = [
    "user_engagement_features__window_days",
    "user_engagement_features__event_count",
    "user_engagement_features__unique_content_count",
    "user_engagement_features__total_watch_seconds",
    "user_engagement_features__search_count",
    "user_engagement_features__play_count",
    "user_engagement_features__watch_count",
    "user_engagement_features__days_since_last_activity",
]

MISSING_ACTIVITY_FILL_VALUE = 999.0


def load_data() -> pd.DataFrame:
    """Load and validate the historical feature dataset."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(
            f"Dataset not found: {DATA_PATH}. Generate it before running this diagnostic."
        )

    df = pd.read_parquet(DATA_PATH)
    required_columns = [TIME_COL, TARGET_COL, *FEATURE_COLS]
    missing_columns = sorted(set(required_columns) - set(df.columns))

    if missing_columns:
        raise ValueError("Dataset is missing required columns: " + ", ".join(missing_columns))

    df = df.copy()
    df[TIME_COL] = pd.to_datetime(df[TIME_COL], utc=True)

    if df[TIME_COL].isna().any():
        raise ValueError(f"{TIME_COL} contains invalid or missing timestamps.")

    if df[TARGET_COL].isna().any():
        raise ValueError(f"{TARGET_COL} contains missing labels.")

    df[TARGET_COL] = df[TARGET_COL].astype(bool)
    return df


def time_based_split(
    df: pd.DataFrame,
    train_ratio: float = 0.68,
    val_ratio: float = 0.17,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Reproduce the split used by train_baseline.py."""
    if not 0 < train_ratio < 1:
        raise ValueError("train_ratio must be between 0 and 1.")

    if not 0 < val_ratio < 1:
        raise ValueError("val_ratio must be between 0 and 1.")

    if train_ratio + val_ratio >= 1:
        raise ValueError("train_ratio + val_ratio must be less than 1.")

    df = df.sort_values(TIME_COL).reset_index(drop=True)

    train_end = int(len(df) * train_ratio)
    val_end = int(len(df) * (train_ratio + val_ratio))

    train = df.iloc[:train_end].copy()
    val = df.iloc[train_end:val_end].copy()
    test = df.iloc[val_end:].copy()

    return train, val, test


def prepare_feature_values(df: pd.DataFrame) -> pd.DataFrame:
    """Apply the same missing-value policy used in train_baseline.py."""
    features = df[FEATURE_COLS].copy()

    features["user_engagement_features__days_since_last_activity"] = features[
        "user_engagement_features__days_since_last_activity"
    ].fillna(MISSING_ACTIVITY_FILL_VALUE)

    return features


def safe_auc(y_true: pd.Series, scores: pd.Series) -> float | None:
    """Compute AUC only when both classes exist."""
    if y_true.nunique() < 2:
        return None

    return float(roc_auc_score(y_true, scores))


def format_auc(value: float | None) -> str:
    """Format optional AUC values consistently."""
    if value is None:
        return "n/a"

    return f"{value:.4f}"


def report_split(name: str, df: pd.DataFrame) -> None:
    """Print sample, timeframe, and label-distribution information."""
    positives = int(df[TARGET_COL].sum())
    negatives = int((~df[TARGET_COL]).sum())
    positive_rate = float(df[TARGET_COL].mean())

    print(f"\n{name}")
    print("-" * len(name))
    print(f"Rows:          {len(df)}")
    print(f"Time range:    {df[TIME_COL].min()} to {df[TIME_COL].max()}")
    print(f"Positives:     {positives} ({positive_rate:.1%})")
    print(f"Negatives:     {negatives} ({1 - positive_rate:.1%})")


def report_label_rate_by_week(df: pd.DataFrame) -> None:
    """Show label prevalence over time."""
    print("\n=== Label Rate by 7-Day Window ===")

    weekly = (
        df.set_index(TIME_COL)
        .resample("7D")[TARGET_COL]
        .agg(["count", "sum", "mean"])
        .rename(
            columns={
                "count": "rows",
                "sum": "positives",
                "mean": "positive_rate",
            }
        )
    )

    weekly["negatives"] = weekly["rows"] - weekly["positives"]

    print(
        weekly.to_string(
            formatters={
                "positive_rate": "{:.1%}".format,
            }
        )
    )


def report_feature_quality(df: pd.DataFrame) -> None:
    """Report null rates, cardinality, constant features, and numeric range."""
    print("\n=== Feature Quality ===")

    rows: list[dict[str, object]] = []

    for column in FEATURE_COLS:
        values = df[column]
        unique_count = int(values.nunique(dropna=False))
        null_rate = float(values.isna().mean())
        is_constant = unique_count <= 1

        rows.append(
            {
                "feature": column,
                "dtype": str(values.dtype),
                "null_rate": null_rate,
                "unique_values": unique_count,
                "constant": is_constant,
                "min": values.min(skipna=True),
                "max": values.max(skipna=True),
            }
        )

    quality = pd.DataFrame(rows)

    print(
        quality.to_string(
            index=False,
            formatters={
                "null_rate": "{:.1%}".format,
            },
        )
    )

    constant_features = quality.loc[quality["constant"], "feature"].tolist()

    if constant_features:
        print("\nConstant features to remove from training:")
        for feature in constant_features:
            print(f"  - {feature}")
    else:
        print("\nNo constant features found.")


def report_feature_drift(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
) -> None:
    """Compare summary statistics across time splits."""
    print("\n=== Feature Drift: Train vs Validation vs Test ===")

    train_features = prepare_feature_values(train)
    validation_features = prepare_feature_values(validation)
    test_features = prepare_feature_values(test)

    rows: list[dict[str, object]] = []

    for column in FEATURE_COLS:
        train_values = train_features[column]
        validation_values = validation_features[column]
        test_values = test_features[column]

        train_mean = float(train_values.mean())
        validation_mean = float(validation_values.mean())
        test_mean = float(test_values.mean())

        train_std = float(train_values.std(ddof=0))

        if train_std == 0:
            validation_shift = np.nan
            test_shift = np.nan
        else:
            validation_shift = (validation_mean - train_mean) / train_std
            test_shift = (test_mean - train_mean) / train_std

        rows.append(
            {
                "feature": column,
                "train_mean": train_mean,
                "val_mean": validation_mean,
                "test_mean": test_mean,
                "val_shift_std": validation_shift,
                "test_shift_std": test_shift,
            }
        )

    drift = pd.DataFrame(rows).sort_values(
        by="test_shift_std",
        key=lambda series: series.abs(),
        ascending=False,
        na_position="last",
    )

    print(
        drift.to_string(
            index=False,
            formatters={
                "train_mean": "{:.3f}".format,
                "val_mean": "{:.3f}".format,
                "test_mean": "{:.3f}".format,
                "val_shift_std": "{:+.3f}".format,
                "test_shift_std": "{:+.3f}".format,
            },
        )
    )

    print(
        "\nInterpretation: absolute standardized mean shifts above 0.5 deserve "
        "attention; values above 1.0 suggest substantial distribution change."
    )


def report_single_feature_auc(
    train: pd.DataFrame,
    validation: pd.DataFrame,
    test: pd.DataFrame,
) -> None:
    """Measure ranking signal of every feature individually."""
    print("\n=== Single-Feature AUC ===")

    train_features = prepare_feature_values(train)
    validation_features = prepare_feature_values(validation)
    test_features = prepare_feature_values(test)

    rows: list[dict[str, object]] = []

    for column in FEATURE_COLS:
        if train_features[column].nunique(dropna=False) <= 1:
            rows.append(
                {
                    "feature": column,
                    "train_auc": None,
                    "val_auc": None,
                    "test_auc": None,
                    "best_test_direction_auc": None,
                    "note": "constant feature",
                }
            )
            continue

        train_auc = safe_auc(train[TARGET_COL], train_features[column])
        validation_auc = safe_auc(
            validation[TARGET_COL],
            validation_features[column],
        )
        test_auc = safe_auc(test[TARGET_COL], test_features[column])

        best_test_direction_auc = None if test_auc is None else max(test_auc, 1.0 - test_auc)

        rows.append(
            {
                "feature": column,
                "train_auc": train_auc,
                "val_auc": validation_auc,
                "test_auc": test_auc,
                "best_test_direction_auc": best_test_direction_auc,
                "note": "",
            }
        )

    auc_summary = pd.DataFrame(rows).sort_values(
        by="best_test_direction_auc",
        ascending=False,
        na_position="last",
    )

    print(
        auc_summary.to_string(
            index=False,
            formatters={
                "train_auc": format_auc,
                "val_auc": format_auc,
                "test_auc": format_auc,
                "best_test_direction_auc": format_auc,
            },
        )
    )

    print(
        "\nNote: best_test_direction_auc reports the stronger direction of a "
        "single feature. For example, an AUC of 0.30 becomes 0.70 when lower "
        "values are associated with the positive label."
    )


def report_label_by_feature_quantiles(
    train: pd.DataFrame,
    test: pd.DataFrame,
) -> None:
    """Check whether training feature-label relationships hold in the test period."""
    print("\n=== Train vs Test Label Rate by Feature Quartile ===")

    train_features = prepare_feature_values(train)
    test_features = prepare_feature_values(test)

    for column in FEATURE_COLS:
        if train_features[column].nunique() < 4:
            print(f"\n{column}")
            print("Skipped: fewer than four distinct values in training.")
            continue

        try:
            train_bins = pd.qcut(
                train_features[column],
                q=4,
                duplicates="drop",
            )
        except ValueError:
            print(f"\n{column}")
            print("Skipped: could not construct quartiles.")
            continue

        bin_edges = sorted(
            {interval.left for interval in train_bins.cat.categories}
            | {interval.right for interval in train_bins.cat.categories}
        )

        if len(bin_edges) < 2:
            continue

        test_bins = pd.cut(
            test_features[column],
            bins=bin_edges,
            include_lowest=True,
        )

        train_rates = (
            pd.DataFrame(
                {
                    "bin": train_bins,
                    TARGET_COL: train[TARGET_COL].to_numpy(),
                }
            )
            .groupby("bin", observed=False)[TARGET_COL]
            .agg(["count", "mean"])
        )

        test_rates = (
            pd.DataFrame(
                {
                    "bin": test_bins,
                    TARGET_COL: test[TARGET_COL].to_numpy(),
                }
            )
            .groupby("bin", observed=False)[TARGET_COL]
            .agg(["count", "mean"])
        )

        comparison = train_rates.join(
            test_rates,
            how="outer",
            lsuffix="_train",
            rsuffix="_test",
        ).rename(
            columns={
                "count_train": "train_rows",
                "mean_train": "train_positive_rate",
                "count_test": "test_rows",
                "mean_test": "test_positive_rate",
            }
        )

        print(f"\n{column}")
        print(
            comparison.to_string(
                formatters={
                    "train_positive_rate": "{:.1%}".format,
                    "test_positive_rate": "{:.1%}".format,
                }
            )
        )


def report_boundary_risk(df: pd.DataFrame) -> None:
    """Flag whether labels near the end may lack a full seven-day future horizon."""
    print("\n=== Label Horizon Boundary Check ===")

    latest_timestamp = df[TIME_COL].max()
    cutoff_timestamp = latest_timestamp - pd.Timedelta(days=7)

    rows_in_last_seven_days = int((df[TIME_COL] > cutoff_timestamp).sum())

    print(f"Latest observation timestamp: {latest_timestamp}")
    print(f"Last-7-days cutoff:           {cutoff_timestamp}")
    print(f"Rows observed in final 7 days: {rows_in_last_seven_days}")

    print(
        "\nImportant: `is_active_next_7d` needs a complete seven-day future "
        "observation window after each event_timestamp. If the source event "
        "data ends close to the latest label timestamp, those final labels may "
        "be structurally different or incomplete. Verify this against the "
        "source-data end timestamp."
    )


def main() -> None:
    """Run all baseline-dataset diagnostics."""
    print("=== Loading Historical Features ===")
    df = load_data()

    print(f"Rows: {len(df)}")
    print(f"Time range: {df[TIME_COL].min()} to {df[TIME_COL].max()}")

    train, validation, test = time_based_split(df)

    print("\n=== Split Audit ===")
    report_split("Train", train)
    report_split("Validation", validation)
    report_split("Test", test)

    report_label_rate_by_week(df)
    report_feature_quality(df)
    report_feature_drift(train, validation, test)
    report_single_feature_auc(train, validation, test)
    report_label_by_feature_quantiles(train, test)
    report_boundary_risk(df)


if __name__ == "__main__":
    main()
