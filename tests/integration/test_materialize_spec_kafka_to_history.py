"""Verify the evidence append of the generic materializer and its parity with published_as_of."""

import contextlib
import json
import os
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.feature_record import stamp
from featureforge.streaming.history import HistoryTable, open_local_catalog
from featureforge.streaming.published_as_of import published_as_of
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
REDIS_URL = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "feature_repo" / "specs" / "user_play_count_1h.yaml"
)
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)


def record_json(spec, entity, count):
    return json.dumps(
        {
            "feature_schema_version": "v1",
            "feature_name": spec.name,
            spec.keys[0]: entity,
            "observation_time": stamp(OBSERVATION),
            "window_start_exclusive": stamp(OBSERVATION - spec.window.size),
            "play_count": count,
        },
        sort_keys=True,
        separators=(",", ":"),
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

    def on_delivery(error, message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for value in payloads:
        producer.produce(topic, value=value.encode("utf-8"), on_delivery=on_delivery)
    assert producer.flush(15) == 0, "Kafka producer still has pending messages"
    assert not errors, f"Kafka delivery failed: {errors}"


def run_materializer(topic, key_prefix, group_id, history_root):
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
            "--history-root",
            str(history_root),
            "--bounded",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode == 0, (
        f"materializer failed\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    return json.loads(completed.stdout)["store_results"]


def history_rows(spec, root):
    table = HistoryTable(open_local_catalog(root), spec).table
    return sorted(table.scan().to_arrow().to_pylist(), key=lambda row: row["play_count"])


def test_evidence_is_recorded_per_accepted_update_and_survives_a_replay(tmp_path):
    spec = load_spec(SPEC_PATH)
    run_id = uuid4().hex
    topic = f"features.test-history.{run_id}.v1"
    key_prefix = f"featureforge-stream:integration:history:{run_id}"
    entity = f"user_{run_id}"

    create_topic(topic)
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    store = RedisSpecStore(redis_client, spec, key_prefix=key_prefix)
    try:
        produce(
            topic,
            [
                record_json(spec, entity, 1),
                record_json(spec, entity, 2),
                record_json(spec, entity, 1),
            ],
        )

        before = datetime.now(UTC)
        first = run_materializer(topic, key_prefix, f"history-first-{run_id}", tmp_path)
        after = datetime.now(UTC)

        assert first == {"stale": 1, "written": 2}
        assert store.read(entity).counts == {"play_count": 2}
        rows = history_rows(spec, tmp_path)
        assert [row["play_count"] for row in rows] == [1, 2]
        assert {row["user_id"] for row in rows} == {entity}
        assert rows[0]["published_at"] <= rows[1]["published_at"]
        assert all(before - timedelta(seconds=1) <= row["published_at"] <= after for row in rows)

        second = run_materializer(topic, key_prefix, f"history-second-{run_id}", tmp_path)

        assert second == {"stale": 2, "unchanged": 1}
        assert [
            (row["play_count"], row["published_at"]) for row in history_rows(spec, tmp_path)
        ] == [(row["play_count"], row["published_at"]) for row in rows]
    finally:
        redis_client.delete(store.key_for(entity))
        redis_client.close()
        delete_topic(topic)


def test_published_as_of_matches_redis_at_every_point_in_time(tmp_path):
    spec = load_spec(SPEC_PATH)
    run_id = uuid4().hex
    topic = f"features.test-parity.{run_id}.v1"
    key_prefix = f"featureforge-stream:integration:parity:{run_id}"
    entity = f"user_{run_id}"
    history = HistoryTable(open_local_catalog(tmp_path), spec)

    create_topic(topic)
    redis_client = Redis.from_url(REDIS_URL, decode_responses=True)
    store = RedisSpecStore(redis_client, spec, key_prefix=key_prefix)
    try:
        produce(topic, [record_json(spec, entity, 1), record_json(spec, entity, 2)])
        before = datetime.now(UTC)
        run_materializer(topic, key_prefix, f"parity-first-{run_id}", tmp_path)
        after_first = datetime.now(UTC)

        online = store.read(entity)
        oracle = published_as_of(history, [entity], after_first)[entity]
        assert oracle.status == "present"
        assert oracle.record.counts == online.counts == {"play_count": 2}
        assert oracle.record.observation_time == online.observation_time
        early = published_as_of(history, [entity], before - timedelta(seconds=2))
        assert early[entity].status == "not_published"

        time.sleep(0.05)
        produce(topic, [record_json(spec, entity, 3)])
        run_materializer(topic, key_prefix, f"parity-second-{run_id}", tmp_path)
        after_second = datetime.now(UTC)

        assert store.read(entity).counts == {"play_count": 3}
        then = published_as_of(history, [entity], after_first)[entity]
        now = published_as_of(history, [entity], after_second)[entity]
        assert then.record.counts == {"play_count": 2}
        assert now.record.counts == store.read(entity).counts == {"play_count": 3}
    finally:
        redis_client.delete(store.key_for(entity))
        redis_client.close()
        delete_topic(topic)
