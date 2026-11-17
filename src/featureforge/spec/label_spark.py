"""Spark evaluation of a LabelSpec (ADR-012), the scaled twin of evaluate_labels.

Window (t, t + horizon], right closed. An observation with t + horizon > data_end
has no value: it is excluded and counted, never evaluated as false or zero.
Identity conflicts are checked across the whole input before any filtering.
Observations are kept one to one, so a repeated observation gives a repeated value.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from featureforge.spec.label_loader import validate_label_against_contract
from featureforge.spec.label_model import LabelSpec
from featureforge.spec.spark_backfill import compile_expression
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import EventIdentityConflictError

_ONE_MICROSECOND = timedelta(microseconds=1)
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


@dataclass(frozen=True)
class LabelBackfill:
    values: DataFrame
    excluded_incomplete: int


def _micros(moment: object, name: str) -> int:
    if not isinstance(moment, datetime):
        raise TypeError(f"{name} must be a datetime")
    if moment.utcoffset() is None:
        raise ValueError(f"{name} must be timezone-aware")
    return (moment.astimezone(UTC) - _EPOCH) // _ONE_MICROSECOND


def _require_columns(label: str, frame: DataFrame, required: set[str]) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{label} frame is missing columns {sorted(missing)}")


def _reject_identity_conflicts(key: str, events: DataFrame) -> None:
    payload = [name for name in InteractionEventV1.model_fields if name != "ingested_at"]
    conflict = (
        events.groupBy(key)
        .agg(F.size(F.collect_set(F.struct(*payload))).alias("variants"))
        .filter(F.col("variants") > 1)
        .limit(1)
        .collect()
    )
    if conflict:
        raise EventIdentityConflictError(
            f"Conflicting logical payload for {key}={conflict[0][key]}"
        )


def _reject_invalid_observations(spec: LabelSpec, observations: DataFrame) -> None:
    invalid = F.col("observation_time").isNull()
    for key in spec.keys:
        column = F.col(key)
        invalid = invalid | column.isNull() | (column == "") | column.rlike(r"\s")
    if observations.filter(invalid).limit(1).count():
        raise ValueError("observations need a time and a non-empty entity value without whitespace")


def backfill_labels(
    spec: LabelSpec,
    events: DataFrame,
    observations: DataFrame,
    *,
    data_end: datetime,
) -> LabelBackfill:
    """Evaluate the label for every observable observation.

    events carries the InteractionEventV1 fields with event_time as timestamp.
    observations carries the spec keys and observation_time as timestamp.
    The values frame has the keys, observation_time, observable_at and value.
    """
    validate_label_against_contract(spec)
    end_us = _micros(data_end, "data_end")
    _require_columns("events", events, set(InteractionEventV1.model_fields))
    _require_columns("observations", observations, {*spec.keys, "observation_time"})
    _reject_identity_conflicts(spec.dedupe.key, events)
    _reject_invalid_observations(spec, observations)

    horizon_us = spec.horizon // _ONE_MICROSECOND
    pairs = (
        observations.select(*spec.keys, "observation_time")
        .withColumn("_obs_id", F.monotonically_increasing_id())
        .withColumn("_obs_us", F.unix_micros("observation_time"))
        .withColumn("_end_us", F.col("_obs_us") + F.lit(horizon_us))
    )
    excluded = pairs.filter(F.col("_end_us") > F.lit(end_us)).count()
    complete = pairs.filter(F.col("_end_us") <= F.lit(end_us))

    deduplicated = events.dropDuplicates([spec.dedupe.key]).withColumn(
        "_event_us", F.unix_micros("event_time")
    )
    joined = complete.join(deduplicated, on=spec.keys, how="left")
    condition = (F.col("_event_us") > F.col("_obs_us")) & (F.col("_event_us") <= F.col("_end_us"))
    if spec.outcome.filter is not None:
        event_columns = {name: F.col(name) for name in InteractionEventV1.model_fields}
        condition = condition & compile_expression(spec.outcome.filter, event_columns)

    matched = joined.groupBy(*spec.keys, "observation_time", "_obs_id", "_end_us").agg(
        F.count(F.when(condition, F.lit(1))).alias("_matched")
    )
    value = (F.col("_matched") > 0) if spec.outcome.op == "exists" else F.col("_matched")
    values = matched.select(
        *spec.keys,
        "observation_time",
        F.timestamp_micros("_end_us").alias("observable_at"),
        value.alias("value"),
    )
    return LabelBackfill(values, excluded)


def label_observations_to_frame(
    spark: SparkSession,
    spec: LabelSpec,
    pairs: Iterable[tuple[Mapping[str, str], datetime]],
) -> DataFrame:
    """Build the observations frame of backfill_labels from entity and time pairs."""
    schema = StructType(
        [StructField(key, StringType(), nullable=False) for key in spec.keys]
        + [StructField("observation_us", LongType(), nullable=False)]
    )
    rows = [
        (*(entity[key] for key in spec.keys), _micros(moment, "observation_time"))
        for entity, moment in pairs
    ]
    return (
        spark.createDataFrame(rows, schema)
        .withColumn("observation_time", F.timestamp_micros("observation_us"))
        .drop("observation_us")
    )
