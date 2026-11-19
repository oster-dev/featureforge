"""Verify the event writer against Kafka and the table against Spark (ADR-015)."""

import contextlib
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import SparkSession

from featureforge.streaming.event_log import EVENT_TABLE, EventLog
from featureforge.streaming.history import (
    CATALOG_NAME,
    SPARK_PACKAGES,
    open_local_catalog,
    spark_catalog_config,
)

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
FIRST_RECEIPT = datetime(2026, 1, 1, 10, 1, tzinfo=UTC)


def payload(number=1, **overrides) -> str:
    return json.dumps(
        {
            "event_id": f"00000000-0000-4000-8000-{number:012d}",
            "event_time": "2026-01-01T10:00:00Z",
            "ingested_at": "2026-01-01T10:01:00Z",
            "user_id": "user_000001",
            "content_id": "content_000001",
            "event_type": "play",
            "play_seconds": 30,
            "source": "web",
            "schema_version": "v1",
            **overrides,
        }
    )


def create_topic(topic):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1)],
        request_timeout=15,
        operation_timeout=15,
    )
    futures[topic].result(timeout=30)


def delete_topic(topic):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    with contextlib.suppress(Exception):
        admin.delete_topics([topic], operation_timeout=15)[topic].result(timeout=30)


def produce(topic, payloads):
    errors = []

    def on_delivery(error, _message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for value in payloads:
        producer.produce(topic, value=value.encode("utf-8"), on_delivery=on_delivery)
    assert producer.flush(15) == 0, "Kafka producer still has pending messages"
    assert not errors, f"Kafka delivery failed: {errors}"


def run_writer(topic, group_id, root):
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "featureforge.streaming.event_writer",
            "--bootstrap-servers",
            BOOTSTRAP_SERVERS,
            "--topic",
            topic,
            "--group-id",
            group_id,
            "--events-root",
            str(root),
            "--bounded",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode == 0, (
        f"event writer failed\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    return json.loads(completed.stdout)["event_results"]


def table_rows(root):
    return EventLog(open_local_catalog(root)).table.scan().to_arrow().to_pylist()


def write_one_topic(tmp_path, run_id):
    topic = f"events.test-writer.{run_id}"
    create_topic(topic)
    produce(
        topic,
        [
            payload(1),
            payload(2),
            payload(2, ingested_at="2026-01-01T10:05:00Z"),
            "{not json",
            payload(3),
        ],
    )
    return topic


def test_events_reach_the_table_once_and_a_replay_changes_nothing(tmp_path):
    run_id = uuid4().hex
    topic = write_one_topic(tmp_path, run_id)
    try:
        first = run_writer(topic, f"writer-first-{run_id}", tmp_path)

        assert first == {"appended": 3, "duplicate": 1, "rejected_invalid_json": 1}
        rows = {row["event_id"]: row for row in table_rows(tmp_path)}
        assert len(rows) == 3
        assert rows["00000000-0000-4000-8000-000000000002"]["ingested_at"] == FIRST_RECEIPT

        same_group = run_writer(topic, f"writer-first-{run_id}", tmp_path)
        assert same_group == {}

        new_group = run_writer(topic, f"writer-second-{run_id}", tmp_path)
        assert new_group == {"duplicate": 4, "rejected_invalid_json": 1}
        assert len(table_rows(tmp_path)) == 3
    finally:
        delete_topic(topic)


def test_spark_reads_the_events_and_the_receipt_time_the_writer_stored(tmp_path):
    run_id = uuid4().hex
    topic = write_one_topic(tmp_path, run_id)
    try:
        run_writer(topic, f"writer-spark-{run_id}", tmp_path)

        builder = SparkSession.builder.master("local[1]").config(
            "spark.jars.packages", SPARK_PACKAGES
        )
        for key, value in spark_catalog_config(tmp_path).items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        try:
            result = spark.sql(
                "SELECT event_id, unix_micros(ingested_at) AS micros "
                f"FROM {CATALOG_NAME}.{EVENT_TABLE}"
            ).collect()
        finally:
            spark.stop()

        receipt = (FIRST_RECEIPT - EPOCH) // timedelta(microseconds=1)
        assert {row["event_id"]: row["micros"] for row in result} == {
            f"00000000-0000-4000-8000-{number:012d}": receipt for number in (1, 2, 3)
        }
    finally:
        delete_topic(topic)
