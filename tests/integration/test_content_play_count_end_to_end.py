"""Prove that a spec without feature code reaches Redis from events (ADR-009).

content_play_count_1h has no Flink job, store, materializer or record type of
its own. Events go through the validation job, the generic spec-driven Flink job
and the generic materializer, and the value is read back through the generic store.
"""

import contextlib
import json
import os
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from pyspark.sql import functions as F
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.spec.spark_backfill import backfill_spec, observations_to_frame
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.event_log import open_event_table, pinned_events_snapshot_id
from featureforge.streaming.events_spark import read_events
from featureforge.streaming.history import open_local_catalog
from featureforge.streaming.published_spark import create_history_session
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_IN_COMPOSE = "kafka:29092"
REDIS_URL = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "feature_repo" / "specs" / ("content_play_count_1h.yaml")
)


def compose(*arguments, timeout=600):
    return subprocess.run(
        ["docker", "compose", *arguments],
        check=False,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def run_in_jobmanager(script, arguments, description):
    completed = compose(
        "exec", "-T", "jobmanager", "python", f"/opt/flink/jobs/{script}", *arguments
    )
    assert completed.returncode == 0, (
        f"{description} failed:\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
    )


def create_topics(topics):
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    futures = admin.create_topics(
        [
            NewTopic(
                topic,
                num_partitions=1,
                replication_factor=1,
                config={"message.timestamp.type": "LogAppendTime"},
            )
            for topic in topics
        ]
    )
    for topic, future in futures.items():
        try:
            future.result()
        except Exception as error:
            if "TOPIC_ALREADY_EXISTS" not in str(error):
                raise RuntimeError(f"Could not create topic {topic!r}") from error


def publish(topic, events):
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for event in events:
        producer.produce(
            topic, value=event.model_dump_json().encode("utf-8"), on_delivery=delivered
        )
    assert producer.flush(15) == 0
    assert not errors, errors


def play(user_id, content_id, hour, minute, event_id=None):
    moment = datetime(2026, 1, 1, hour, minute, tzinfo=UTC)
    return InteractionEventV1(
        event_id=event_id or uuid.uuid4(),
        event_time=moment,
        ingested_at=moment,
        user_id=user_id,
        content_id=content_id,
        event_type="play",
        play_seconds=0,
        source="web",
        schema_version="v1",
    )


def run_materializer(topic, key_prefix, group_id):
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "featureforge.streaming.materialize_spec",
            "--spec",
            str(SPEC_PATH),
            "--bootstrap-servers",
            BOOTSTRAP_SERVERS,
            "--redis-url",
            REDIS_URL,
            "--topic",
            topic,
            "--group-id",
            group_id,
            "--key-prefix",
            key_prefix,
            "--bounded",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, (
        f"materializer failed\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    return json.loads(completed.stdout)["store_results"]


def test_content_play_count_reaches_redis_from_events_without_feature_code():
    spec = load_spec(SPEC_PATH)
    run_id = uuid.uuid4().hex
    topics = {
        kind: f"featureforge-stream.integration.proof.{kind}.{run_id}"
        for kind in ("input", "validated", "rejected", "features", "late")
    }
    key_prefix = f"featureforge-stream:integration:proof:{run_id}"
    container_spec = "/opt/flink/specs/content_play_count_1h.yaml"

    duplicated = play("user_000002", "content_000001", 11, 40)
    events = [
        play("user_000001", "content_000001", 11, 30),
        play("user_000001", "content_000002", 11, 30),
        duplicated,
        duplicated,
    ]

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        try:
            create_topics(list(topics.values()))
            publish(topics["input"], events)

            run_in_jobmanager(
                "validate_and_route_events.py",
                [
                    "--bootstrap-servers",
                    KAFKA_IN_COMPOSE,
                    "--input-topic",
                    topics["input"],
                    "--validated-topic",
                    topics["validated"],
                    "--rejected-topic",
                    topics["rejected"],
                    "--group-id",
                    f"validation-{run_id}",
                    "--bounded",
                ],
                "validation job",
            )
            run_in_jobmanager(
                "feature_from_spec.py",
                [
                    "--spec",
                    container_spec,
                    "--bootstrap-servers",
                    KAFKA_IN_COMPOSE,
                    "--input-topic",
                    topics["validated"],
                    "--feature-topic",
                    topics["features"],
                    "--late-topic",
                    topics["late"],
                    "--group-id",
                    f"content-play-count-{run_id}",
                    "--job-name",
                    f"content-play-count-{run_id}",
                    "--bounded",
                ],
                "generic spec-driven job",
            )

            results = run_materializer(topics["features"], key_prefix, f"proof-{run_id}")

            # content_000001: windows ending 11:30..12:39 (70), content_000002: 11:30..12:29 (60).
            assert set(results) <= {"written", "unchanged", "stale"}
            assert sum(results.values()) == 130

            store = RedisSpecStore(client, spec, key_prefix=key_prefix)

            first = store.read("content_000001")
            assert first is not None
            assert first.counts == {"play_count": 1}
            assert first.observation_time == datetime(2026, 1, 1, 12, 39, tzinfo=UTC)
            assert first.window_start_exclusive == datetime(2026, 1, 1, 11, 39, tzinfo=UTC)

            second = store.read("content_000002")
            assert second is not None
            assert second.counts == {"play_count": 1}
            assert second.observation_time == datetime(2026, 1, 1, 12, 29, tzinfo=UTC)
            assert second.window_start_exclusive == datetime(2026, 1, 1, 11, 29, tzinfo=UTC)

            assert store.read("content_000003") is None
            ttl = int(spec.online.ttl.total_seconds())
            assert 0 < client.ttl(store.key_for("content_000001")) <= ttl

            assert run_materializer(topics["features"], key_prefix, f"proof-{run_id}") == {}

            replay = run_materializer(topics["features"], key_prefix, f"proof-replay-{run_id}")
            assert replay == {"stale": 128, "unchanged": 2}
            assert store.read("content_000001") == first
            assert store.read("content_000002") == second
        finally:
            keys = list(client.scan_iter(match=f"{key_prefix}:*"))
            if keys:
                client.delete(*keys)
            with contextlib.suppress(Exception):
                for future in admin.delete_topics(
                    list(topics.values()), operation_timeout=15
                ).values():
                    future.result(timeout=30)


def micros(moment):
    return (moment - datetime(1970, 1, 1, tzinfo=UTC)) // timedelta(microseconds=1)


def publish_raw(topic, values):
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for value in values:
        producer.produce(topic, value=value, on_delivery=delivered)
    assert producer.flush(15) == 0
    assert not errors, errors


def run_event_writer(topic, group_id, root):
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
        timeout=180,
    )
    assert completed.returncode == 0, (
        f"event writer failed\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    return json.loads(completed.stdout)["event_results"]


def test_redis_value_equals_the_event_table_evaluation_with_duplicates_and_invalid_payloads(
    tmp_path,
):
    spec = load_spec(SPEC_PATH)
    run_id = uuid.uuid4().hex
    topics = {
        kind: f"featureforge-stream.integration.parity.{kind}.{run_id}"
        for kind in ("input", "validated", "rejected", "features", "late")
    }
    key_prefix = f"featureforge-stream:integration:parity:{run_id}"
    container_spec = "/opt/flink/specs/content_play_count_1h.yaml"
    content_a = f"content_a_{run_id}"
    content_b = f"content_b_{run_id}"

    shared = play("user_a2", content_a, 11, 30)
    events = [
        play("user_a1", content_a, 10, 15),
        play("user_a1", content_a, 11, 30),
        shared,
        shared,
        play("user_a3", content_a, 11, 30),
        play("user_b1", content_b, 11, 45),
        play("user_b2", content_b, 11, 45),
    ]
    payloads = [event.model_dump_json().encode("utf-8") for event in events]
    payloads.append(b"{not valid json")
    payloads.append(json.dumps({"event_id": str(uuid.uuid4())}).encode("utf-8"))

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        try:
            create_topics(list(topics.values()))
            publish_raw(topics["input"], payloads)

            run_in_jobmanager(
                "validate_and_route_events.py",
                [
                    "--bootstrap-servers",
                    KAFKA_IN_COMPOSE,
                    "--input-topic",
                    topics["input"],
                    "--validated-topic",
                    topics["validated"],
                    "--rejected-topic",
                    topics["rejected"],
                    "--group-id",
                    f"validation-{run_id}",
                    "--bounded",
                ],
                "validation job",
            )
            run_in_jobmanager(
                "feature_from_spec.py",
                [
                    "--spec",
                    container_spec,
                    "--bootstrap-servers",
                    KAFKA_IN_COMPOSE,
                    "--input-topic",
                    topics["validated"],
                    "--feature-topic",
                    topics["features"],
                    "--late-topic",
                    topics["late"],
                    "--group-id",
                    f"content-play-count-{run_id}",
                    "--job-name",
                    f"content-play-count-{run_id}",
                    "--bounded",
                ],
                "generic spec-driven job",
            )
            run_materializer(topics["features"], key_prefix, f"parity-{run_id}")

            store = RedisSpecStore(client, spec, key_prefix=key_prefix)
            redis_values = {content: store.read(content) for content in (content_a, content_b)}
            assert redis_values[content_a].counts == {"play_count": 3}
            assert redis_values[content_b].counts == {"play_count": 2}

            results = run_event_writer(topics["input"], f"parity-writer-{run_id}", tmp_path)
            assert results.get("duplicate") == 1, results
            assert results.get("rejected_invalid_json") == 1, results
            assert results.get("rejected_event_contract_violation") == 1, results

            table = open_event_table(open_local_catalog(tmp_path))
            assert table.scan().to_arrow().num_rows == 6
            snapshot = pinned_events_snapshot_id(table)

            spark = create_history_session(tmp_path)
            try:
                observations = observations_to_frame(
                    spark,
                    spec,
                    [
                        ({"content_id": content}, redis_values[content].observation_time)
                        for content in redis_values
                    ],
                )
                frame = backfill_spec(spec, read_events(spark, snapshot), observations).withColumn(
                    "observation_us", F.unix_micros("observation_time")
                )
                from_table = {
                    (row["content_id"], row["observation_us"]): row["play_count"]
                    for row in frame.collect()
                }
            finally:
                spark.stop()

            for content, value in redis_values.items():
                key = (content, micros(value.observation_time))
                assert from_table[key] == value.counts["play_count"], (content, from_table)
        finally:
            keys = list(client.scan_iter(match=f"{key_prefix}:*"))
            if keys:
                client.delete(*keys)
            with contextlib.suppress(Exception):
                for future in admin.delete_topics(
                    list(topics.values()), operation_timeout=15
                ).values():
                    future.result(timeout=30)
