"""Spark join of the history: what serving held at each observation time (ADR-014).

One scan of the pinned history snapshot serves all observations. The rule is the one
of published_as_of (ADR-010), which stays the oracle: the version with the highest
rank among those with published_at at or before t, and a value past
published_at + online.ttl is expired. Counts and derived values of a feature that is
not present are null, never zero.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from featureforge.spec.model import FeatureSpec
from featureforge.streaming.contracts.feature_record import record_fields
from featureforge.streaming.history import CATALOG_NAME, HistoryTable

STATUS_COLUMN = "status"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


def pinned_snapshot_id(history: HistoryTable) -> int:
    """Return the current snapshot id of the history table, the one a dataset reads."""
    history.table.refresh()
    snapshot = history.table.current_snapshot()
    if snapshot is None:
        raise ValueError(f"history table of {history.spec.name} has no snapshot")
    return int(snapshot.snapshot_id)


def read_history(spark: SparkSession, spec: FeatureSpec, snapshot_id: int) -> DataFrame:
    """Read the history table of a spec at one pinned snapshot."""
    return (
        spark.read.format("iceberg")
        .option("snapshot-id", str(snapshot_id))
        .load(f"{CATALOG_NAME}.{spec.offline.table}")
    )


def history_start(history_frame: DataFrame) -> datetime:
    """Return the earliest published_at of the history, the start of its evidence."""
    first = history_frame.agg(F.min(F.unix_micros("published_at"))).collect()[0][0]
    if first is None:
        raise ValueError("the history holds no evidence")
    return EPOCH + timedelta(microseconds=int(first))


def published_as_of_frame(
    spec: FeatureSpec, history_frame: DataFrame, observations: DataFrame
) -> DataFrame:
    """Return, for every distinct observation, the feature values serving held at t.

    The result has the entity key, observation_time, the counts, the derived values
    and status (present, not_published or expired).
    """
    entity_key, counters, derived = record_fields(spec)
    ttl_us = int(spec.online.ttl / timedelta(microseconds=1))
    values = [*counters, *derived]

    asked = observations.select(F.col(entity_key), F.col("observation_time").alias("_t")).distinct()
    versions = history_frame.select(
        F.col(entity_key).alias("_h_entity"),
        F.col("observation_time").alias("_window"),
        "update_id",
        "published_at",
        *values,
    )
    joined = asked.join(
        versions,
        (F.col(entity_key) == F.col("_h_entity")) & (F.col("published_at") <= F.col("_t")),
        "left",
    )
    order = [F.col(name).desc() for name in ("published_at", "_window", *counters, "update_id")]
    latest = joined.withColumn(
        "_rank", F.row_number().over(Window.partitionBy(entity_key, "_t").orderBy(*order))
    ).where(F.col("_rank") == 1)

    expired = F.unix_micros("_t") >= F.unix_micros("published_at") + F.lit(ttl_us)
    status = (
        F.when(F.col("published_at").isNull(), "not_published")
        .when(expired, "expired")
        .otherwise("present")
    )
    marked = latest.withColumn(STATUS_COLUMN, status)
    return marked.select(
        entity_key,
        F.col("_t").alias("observation_time"),
        *(F.when(F.col(STATUS_COLUMN) == "present", F.col(name)).alias(name) for name in values),
        STATUS_COLUMN,
    )
