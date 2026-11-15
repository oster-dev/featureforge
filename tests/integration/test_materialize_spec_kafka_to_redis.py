"""Verify the generic materializer from Kafka to Redis for a spec without feature code."""

import contextlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
REDIS_URL = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "feature_repo" / "specs" / ("content_play_count_1h.yaml")
)
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)


def record_json(spec, entity, counts, *, minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    data = {
        "feature_schema_version": "v1",
        "feature_name": spec.name,
        spec.keys[0]: entity,
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - spec.window.size),
        **counts,
    }
    for name, expression in spec.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


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
        timeout=120,
    )
    assert completed.returncode == 0, (
        f"materializer failed\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    return json.loads(completed.stdout)["store_results"]


def test_content_play_count_flows_from_kafka_to_redis_without_feature_code():
    spec = load_spec(SPEC_PATH)
    run_id = uuid4().hex
    topic = f"featureforge-stream.integration.materialize-spec.{run_id}"
    key_prefix = f"featureforge-stream:integration:materialize-spec:{run_id}"
    group_id = f"materialize-spec-{run_id}"

    messages = [
        record_json(spec, "content_000001", {"play_count": 1}),
        record_json(spec, "content_000001", {"play_count": 2}),
        record_json(spec, "content_000001", {"play_count": 2}),
        record_json(spec, "content_000002", {"play_count": 7}),
        record_json(spec, "content_000001", {"play_count": 1}, minutes=1),
        record_json(spec, "content_000001", {"play_count": 9}),
    ]

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        try:
            futures = admin.create_topics(
                [
                    NewTopic(
                        topic,
                        num_partitions=1,
                        replication_factor=1,
                        config={"message.timestamp.type": "LogAppendTime"},
                    )
                ],
                request_timeout=15,
                operation_timeout=15,
            )
            for future in futures.values():
                future.result(timeout=30)

            errors = []

            def delivered(error, message):
                if error is not None:
                    errors.append(str(error))

            producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
            for text in messages:
                producer.produce(topic, value=text.encode("utf-8"), on_delivery=delivered)
            assert producer.flush(15) == 0
            assert not errors, errors

            first = run_materializer(topic, key_prefix, group_id)
            assert first == {"stale": 1, "unchanged": 1, "written": 4}

            store = RedisSpecStore(client, spec, key_prefix=key_prefix)
            assert store.read("content_000001") == parse_feature_record(
                record_json(spec, "content_000001", {"play_count": 1}, minutes=1),
                spec,
            )
            assert store.read("content_000002") == parse_feature_record(
                record_json(spec, "content_000002", {"play_count": 7}),
                spec,
            )
            ttl = int(spec.online.ttl.total_seconds())
            assert 0 < client.ttl(store.key_for("content_000001")) <= ttl

            second = run_materializer(topic, key_prefix, group_id)
            assert second == {}
        finally:
            keys = list(client.scan_iter(match=f"{key_prefix}:*"))
            if keys:
                client.delete(*keys)
            with contextlib.suppress(Exception):
                for future in admin.delete_topics([topic], operation_timeout=15).values():
                    future.result(timeout=30)
