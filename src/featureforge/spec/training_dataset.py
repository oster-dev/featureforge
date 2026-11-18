"""Training datasets from feature specs and one label spec, event-time mode (ADR-014).

A row exists per observation with a complete label. Features use only events at or
before the observation time t, the label only events strictly after t. An
observation whose label window ends after data_end is excluded and counted.

The module reads events and observation times only. It never reads or writes
publication evidence (ADR-010).
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import UTC, datetime
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


def feature_columns(feature: FeatureSpec) -> list[str]:
    """Return the value columns of a feature spec, in the order of the backfill."""
    return [*(aggregation.name for aggregation in feature.aggregations), *feature.derived]


def output_columns(features: Sequence[FeatureSpec], label: LabelSpec) -> list[str]:
    """Return the dataset columns: keys, time, spec-qualified features, then the label."""
    return [
        *label.keys,
        "observation_time",
        *(
            f"{feature.name}__{column}"
            for feature in features
            for column in feature_columns(feature)
        ),
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
    columns = output_columns(features, label)
    if len(set(columns)) != len(columns):
        raise ValueError("the dataset would contain duplicate column names")


class TrainingFrame:
    """The joined dataset frame and the counts that belong to it."""

    def __init__(self, frame: DataFrame, observations: int, excluded_incomplete: int) -> None:
        self.frame = frame
        self.observations = observations
        self.excluded_incomplete = excluded_incomplete


def training_dataset_frame(
    features: Sequence[FeatureSpec],
    label: LabelSpec,
    events: DataFrame,
    observations: DataFrame,
    *,
    data_end: datetime,
) -> TrainingFrame:
    """Join the feature values and the label of every observation with a complete label."""
    _check_specs(features, label)
    on = [*label.keys, "observation_time"]

    labels = backfill_labels(label, events, observations, data_end=data_end)
    joined = labels.values.select(
        *on,
        F.col("value").alias(f"label__{label.name}"),
        F.col("observable_at").alias(f"label__{label.name}__observable_at"),
    )
    for feature in features:
        values = backfill_spec(feature, events, observations)
        qualified = [F.col(c).alias(f"{feature.name}__{c}") for c in feature_columns(feature)]
        joined = joined.join(values.select(*on, *qualified), on=on, how="left")

    return TrainingFrame(
        joined.select(*output_columns(features, label)),
        observations.count(),
        labels.excluded_incomplete,
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
) -> dict[str, Any]:
    """Write the dataset as Parquet partitioned by the UTC day and return its manifest."""
    if mode == "published_as_of":
        raise NotImplementedError("mode published_as_of is not implemented yet (ADR-014)")
    if mode != "event_time":
        raise ValueError(f"mode must be event_time or published_as_of, got {mode!r}")
    if spark.conf.get("spark.sql.session.timeZone") != "UTC":
        raise ValueError("the Spark session must run in UTC")
    if output.exists():
        raise FileExistsError(f"output already exists: {output}")

    result = training_dataset_frame(features, label, events, observations, data_end=data_end)
    if result.observations == result.excluded_incomplete:
        raise ValueError("no observation has a complete label window up to data_end")

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
        "excluded_before_history": 0,
        "columns": output_columns(features, label),
        "partitions": partitions,
        "spark_version": spark.version,
    }
    (output / MANIFEST_NAME).write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return manifest
