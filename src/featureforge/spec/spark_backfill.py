"""Spec-driven Spark backfill evaluated at explicit observation times.

Computes the values of evaluate_spec (the oracle of ADR-008) for many entities
and observation times at once: window (t - size, t], identity conflicts checked
across the whole input before any filtering, a missing derived value is null,
never zero.

A backfill reads events and observation times only. It never reads or writes
availability time (ADR-010) and has no access to the history table.
"""

from __future__ import annotations

import ast
import operator
from collections.abc import Iterable, Mapping
from datetime import UTC, datetime, timedelta
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from featureforge.spec.model import FeatureSpec, Op
from featureforge.spec.reference import names_in, validate_event_fields
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import EventIdentityConflictError

_ONE_MICROSECOND = timedelta(microseconds=1)
_COMPARE = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}
_ARITH = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}


def compile_expression(expr: str, columns: dict[str, Column]) -> Column:
    """Translate a validated spec expression into a Spark column without eval()."""
    return _compile(ast.parse(expr, mode="eval").body, columns)


def _compile(node: ast.AST, columns: dict[str, Column]) -> Column:
    if isinstance(node, ast.Constant):
        return F.lit(node.value)
    if isinstance(node, ast.Name):
        if node.id not in columns:
            raise ValueError(f"unknown name '{node.id}'")
        return columns[node.id]
    if isinstance(node, ast.UnaryOp):
        operand = _compile(node.operand, columns)
        if isinstance(node.op, ast.Not):
            return ~operand
        if isinstance(node.op, ast.USub):
            return -operand
    if isinstance(node, ast.BoolOp):
        parts = [_compile(value, columns) for value in node.values]
        combine = operator.and_ if isinstance(node.op, ast.And) else operator.or_
        return reduce(combine, parts)
    if isinstance(node, ast.BinOp) and type(node.op) in _ARITH:
        left = _compile(node.left, columns)
        return _ARITH[type(node.op)](left, _compile(node.right, columns))
    if isinstance(node, ast.Compare):
        return _compile_compare(node, columns)
    if isinstance(node, ast.IfExp):
        return F.when(_compile(node.test, columns), _compile(node.body, columns)).otherwise(
            _compile(node.orelse, columns)
        )
    raise ValueError(f"unsupported expression node {type(node).__name__}")


def _compile_compare(node: ast.Compare, columns: dict[str, Column]) -> Column:
    left_node = node.left
    parts: list[Column] = []
    for op, right_node in zip(node.ops, node.comparators, strict=True):
        left = _compile(left_node, columns)
        if isinstance(op, (ast.In, ast.NotIn)):
            if not isinstance(right_node, (ast.Tuple, ast.List)):
                raise ValueError("in and not in need a literal list or tuple")
            constants = [item.value for item in right_node.elts if isinstance(item, ast.Constant)]
            if len(constants) != len(right_node.elts):
                raise ValueError("in and not in only support constants")
            member = left.isin(constants)
            parts.append(member if isinstance(op, ast.In) else ~member)
        elif type(op) in _COMPARE:
            parts.append(_COMPARE[type(op)](left, _compile(right_node, columns)))
        else:
            raise ValueError(f"unsupported comparison {type(op).__name__}")
        left_node = right_node
    return reduce(operator.and_, parts)


def _require_columns(label: str, frame: DataFrame, required: set[str]) -> None:
    missing = required - set(frame.columns)
    if missing:
        raise ValueError(f"{label} frame is missing columns {sorted(missing)}")


def _reject_identity_conflicts(spec: FeatureSpec, events: DataFrame) -> None:
    key = spec.dedupe.key
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


_SUPPORTED_OPS = frozenset({Op.COUNT, Op.SUM, Op.AVG})


def backfill_spec(spec: FeatureSpec, events: DataFrame, observations: DataFrame) -> DataFrame:
    """Evaluate the spec for every row of observations over (t - size, t].

    events carries the InteractionEventV1 fields with event_time as timestamp.
    observations carries the spec keys and observation_time as timestamp.
    """
    validate_event_fields(spec)
    unsupported = sorted({a.op.value for a in spec.aggregations if a.op not in _SUPPORTED_OPS})
    if unsupported:
        raise NotImplementedError(
            "spark backfill supports count, sum and avg aggregations only; "
            f"unsupported: {unsupported}"
        )
    _require_columns("events", events, set(InteractionEventV1.model_fields))
    _require_columns("observations", observations, {*spec.keys, "observation_time"})
    _reject_identity_conflicts(spec, events)

    size_us = spec.window.size // _ONE_MICROSECOND
    deduplicated = events.dropDuplicates([spec.dedupe.key]).withColumn(
        "_event_us", F.unix_micros("event_time")
    )
    pairs = observations.select(*spec.keys, "observation_time").withColumn(
        "_obs_us", F.unix_micros("observation_time")
    )
    joined = pairs.join(deduplicated, on=spec.keys, how="left")
    in_window = (F.col("_event_us") > F.col("_obs_us") - F.lit(size_us)) & (
        F.col("_event_us") <= F.col("_obs_us")
    )

    event_columns = {name: F.col(name) for name in InteractionEventV1.model_fields}
    aggregates = []
    for agg in spec.aggregations:
        condition = in_window
        if agg.filter is not None:
            condition = in_window & compile_expression(agg.filter, event_columns)
        if agg.op is Op.COUNT:
            aggregate = F.count(F.when(condition, F.lit(1)))
        else:
            if agg.column is None:
                raise ValueError(f"aggregation '{agg.name}' needs a column")
            values = F.when(condition, F.col(agg.column))
            # count and sum over no events are 0, avg over no events is missing (ADR-008)
            if agg.op is Op.SUM:
                aggregate = F.coalesce(F.sum(values), F.lit(0))
            else:
                aggregate = F.avg(values)
        aggregates.append(aggregate.alias(agg.name))
    result = joined.groupBy(*spec.keys, "observation_time").agg(*aggregates)

    aggregate_columns = {agg.name: F.col(agg.name) for agg in spec.aggregations}
    for name, expr in spec.derived.items():
        refs = sorted(names_in(expr))
        missing = reduce(operator.or_, [F.col(ref).isNull() for ref in refs], F.lit(False))
        result = result.withColumn(
            name,
            F.when(missing, F.lit(None)).otherwise(compile_expression(expr, aggregate_columns)),
        )

    window_start = F.timestamp_micros(F.unix_micros("observation_time") - F.lit(size_us))
    return result.select(
        *spec.keys,
        "observation_time",
        window_start.alias("window_start_exclusive"),
        *[agg.name for agg in spec.aggregations],
        *spec.derived,
    )


_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("event_time_us", LongType(), nullable=False),
        StructField("ingested_at_us", LongType(), nullable=False),
        StructField("user_id", StringType(), nullable=False),
        StructField("content_id", StringType(), nullable=False),
        StructField("event_type", StringType(), nullable=False),
        StructField("play_seconds", LongType(), nullable=False),
        StructField("source", StringType(), nullable=False),
        StructField("schema_version", StringType(), nullable=False),
    ]
)


def _micros(moment: datetime) -> int:
    return (moment - _EPOCH) // _ONE_MICROSECOND


def create_local_session(app_name: str = "featureforge-spec-backfill") -> SparkSession:
    """Create or reuse a local Spark session pinned to UTC."""
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def events_to_frame(spark: SparkSession, events: Iterable[InteractionEventV1]) -> DataFrame:
    """Build the events frame of backfill_spec from validated events, in exact microseconds."""
    rows = [
        (
            str(event.event_id),
            _micros(event.event_time),
            _micros(event.ingested_at),
            event.user_id,
            event.content_id,
            event.event_type,
            event.play_seconds,
            event.source,
            event.schema_version,
        )
        for event in events
    ]
    return (
        spark.createDataFrame(rows, _EVENT_SCHEMA)
        .withColumn("event_time", F.timestamp_micros("event_time_us"))
        .withColumn("ingested_at", F.timestamp_micros("ingested_at_us"))
        .drop("event_time_us", "ingested_at_us")
    )


def observations_to_frame(
    spark: SparkSession,
    spec: FeatureSpec,
    pairs: Iterable[tuple[Mapping[str, str], datetime]],
) -> DataFrame:
    """Build the observations frame of backfill_spec from entity and time pairs."""
    schema = StructType(
        [StructField(key, StringType(), nullable=False) for key in spec.keys]
        + [StructField("observation_us", LongType(), nullable=False)]
    )
    rows = [(*(entity[key] for key in spec.keys), _micros(moment)) for entity, moment in pairs]
    return (
        spark.createDataFrame(rows, schema)
        .withColumn("observation_time", F.timestamp_micros("observation_us"))
        .drop("observation_us")
    )
