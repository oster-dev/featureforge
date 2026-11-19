"""Read the event fact table with Spark at a pinned snapshot (ADR-015)."""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from featureforge.spec.spark_backfill import events_to_frame
from featureforge.streaming.event_log import EVENT_TABLE
from featureforge.streaming.history import CATALOG_NAME


def read_events(spark: SparkSession, snapshot_id: int) -> DataFrame:
    """Read the event table at one snapshot as the events frame of backfill_spec.

    The columns, their order and their types follow events_to_frame, so a dataset from the
    table and one from the same events as JSONL start from the same frame.
    """
    frame = (
        spark.read.format("iceberg")
        .option("snapshot-id", str(snapshot_id))
        .load(f"{CATALOG_NAME}.{EVENT_TABLE}")
    )
    reference = events_to_frame(spark, []).schema
    return frame.select(
        *(F.col(field.name).cast(field.dataType).alias(field.name) for field in reference.fields)
    )
