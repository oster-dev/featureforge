"""Training datasets from feature specs and one label spec (ADR-014).

A row exists per observation with a complete label. Features use only information up
to the observation time t, the label only events strictly after t. An observation whose
label window ends after data_end is excluded and counted.

Mode event_time reads events and observation times only. Mode published_as_of takes
the history of every feature as a frame at a pinned snapshot and never reads or writes
the history itself (ADR-010, ADR-011).
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from featureforge.spec.label_model import LabelSpec
from featureforge.spec.label_spark import backfill_labels
from featureforge.spec.model import FeatureSpec
from featureforge.spec.spark_backfill import backfill_spec

MANIFEST_NAME = "_manifest.json"
MANIFEST_VERSION = 1
PARTITION_COLUMN = "observation_date"
STATUS_COLUMN = "status"
MODES = ("event_time", "published_as_of")
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class PinnedHistory:
    """The history of one feature, read at one snapshot."""

    frame: DataFrame
    snapshot_id: int


def feature_columns(feature: FeatureSpec) -> list[str]:
    """Return the value columns of a feature spec, in the order of the backfill."""
    return [*(aggregation.name for aggregation in feature.aggregations), *feature.derived]


def output_columns(
    features: Sequence[FeatureSpec], label: LabelSpec, *, with_status: bool = False
) -> list[str]:
    """Return the dataset columns: keys, time, spec-qualified features, then the label."""
    feature_names: list[str] = []
    for feature in features:
        feature_names.extend(f"{feature.name}__{column}" for column in feature_columns(feature))
        if with_status:
            feature_names.append(f"{feature.name}__{STATUS_COLUMN}")
    return [
        *label.keys,
        "observation_time",
        *feature_names,
        f"label__{label.name}",
        f"label__{label.name}__observable_at",
    ]


def _check_specs(features: Sequence[FeatureSpec], label: LabelSpec) -> None:
    if not features:
        raise ValueError("at least one feature spec is required")
    names = [feature.name for feature in features]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        raise ValueError(f"feature specs must be unique, repeated: {repeated}")
    for feature in features:
        if feature.keys != label.keys:
            raise ValueError(
                f"feature {feature.name} has keys {feature.keys} "
                f"but label {label.name} has keys {label.keys}"
            )
    columns = output_columns(features, label, with_status=True)
    if len(set(columns)) != len(columns):
        raise ValueError("the dataset would contain duplicate column names")


class TrainingFrame:
    """The joined dataset frame and the counts that belong to it."""

    def __init__(
        self,
        frame: DataFrame,
        observations: int,
        excluded_incomplete: int,
        excluded_before_history: int = 0,
        history_starts: Mapping[str, datetime] | None = None,
    ) -> None:
        self.frame = frame
        self.observations = observations
        self.excluded_incomplete = excluded_incomplete
        self.excluded_before_history = excluded_before_history
        self.history_starts = dict(history_starts or {})


def training_dataset_frame(
    features: Sequence[FeatureSpec],
    label: LabelSpec,
    events: DataFrame,
    observations: DataFrame,
    *,
    data_end: datetime,
    histories: Mapping[str, DataFrame] | None = None,
) -> TrainingFrame:
    """Join the feature values and the label of every observation with a complete label.

    Without histories the features come from the events. With one history frame per
    feature name they come from the history, and an observation before the start of any
    history is excluded and counted.
    """
    _check_specs(features, label)
    on = [*label.keys, "observation_time"]

    labels = backfill_labels(label, events, observations, data_end=data_end)
    complete = labels.values
    starts: dict[str, datetime] = {}
    excluded_before_history = 0
    if histories is not None:
        from featureforge.streaming.published_spark import history_start

        starts = {feature.name: history_start(histories[feature.name]) for feature in features}
        cutoff_us = (max(starts.values()) - EPOCH) // timedelta(microseconds=1)
        before = complete.count()
        complete = complete.where(F.unix_micros("observation_time") >= cutoff_us)
        excluded_before_history = before - complete.count()

    joined = complete.select(
        *on,
        F.col("value").alias(f"label__{label.name}"),
        F.col("observable_at").alias(f"label__{label.name}__observable_at"),
    )
    for feature in features:
        if histories is None:
            values = backfill_spec(feature, events, observations)
            names = feature_columns(feature)
        else:
            from featureforge.streaming.published_spark import published_as_of_frame

            values = published_as_of_frame(feature, histories[feature.name], observations)
            names = [*feature_columns(feature), STATUS_COLUMN]
        qualified = [F.col(c).alias(f"{feature.name}__{c}") for c in names]
        joined = joined.join(values.select(*on, *qualified), on=on, how="left")

    return TrainingFrame(
        joined.select(*output_columns(features, label, with_status=histories is not None)),
        observations.count(),
        labels.excluded_incomplete,
        excluded_before_history,
        starts,
    )


def build_training_dataset(
    spark: SparkSession,
    features: Sequence[FeatureSpec],
    label: LabelSpec,
    events: DataFrame,
    observations: DataFrame,
    *,
    mode: str,
    data_end: datetime,
    output: Path,
    history: Mapping[str, PinnedHistory] | None = None,
) -> dict[str, Any]:
    """Write the dataset as Parquet partitioned by the UTC day and return its manifest."""
    if mode not in MODES:
        raise ValueError(f"mode must be event_time or published_as_of, got {mode!r}")
    if mode == "event_time" and history is not None:
        raise ValueError("history is only used by mode published_as_of")
    given = history or {}
    if mode == "published_as_of":
        if len(label.keys) != 1:
            raise ValueError(f"mode published_as_of needs exactly one key, got {label.keys}")
        missing = sorted({feature.name for feature in features} - set(given))
        if missing:
            raise ValueError(f"mode published_as_of needs a pinned history for: {missing}")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("the Spark session must run in UTC")
    if output.exists():
        raise FileExistsError(f"output already exists: {output}")

    published = mode == "published_as_of"
    result = training_dataset_frame(
        features,
        label,
        events,
        observations,
        data_end=data_end,
        histories={name: pin.frame for name, pin in given.items()} if published else None,
    )
    if result.observations == result.excluded_incomplete + result.excluded_before_history:
        raise ValueError(
            "no observation has a complete label window up to data_end"
            + (" and history evidence" if published else "")
        )

    spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    dated = result.frame.withColumn(PARTITION_COLUMN, F.to_date("observation_time"))
    dated.write.mode("error").partitionBy(PARTITION_COLUMN).parquet(str(output))

    written = spark.read.parquet(str(output))
    counts = written.groupBy(F.col(PARTITION_COLUMN).cast("string").alias("day")).count().collect()
    partitions: list[dict[str, Any]] = sorted(
        ({"observation_date": row["day"], "rows": row["count"]} for row in counts),
        key=lambda partition: str(partition["observation_date"]),
    )

    manifest: dict[str, Any] = {
        "manifest_version": MANIFEST_VERSION,
        "mode": mode,
        "features": [{"name": f.name, "version": f.version} for f in features],
        "label": {
            "name": label.name,
            "version": label.version,
            "horizon_seconds": int(label.horizon.total_seconds()),
        },
        "data_end": data_end.astimezone(UTC).isoformat(),
        "observations": result.observations,
        "rows": sum(int(partition["rows"]) for partition in partitions),
        "excluded_incomplete": result.excluded_incomplete,
        "excluded_before_history": result.excluded_before_history,
        "columns": output_columns(features, label, with_status=published),
        "partitions": partitions,
        "spark_version": spark.version,
    }
    if published:
        manifest["history"] = [
            {
                "feature": feature.name,
                "snapshot_id": given[feature.name].snapshot_id,
                "history_start": result.history_starts[feature.name].isoformat(),
            }
            for feature in features
        ]
    (output / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest
