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
from datetime import UTC, datetime
from pathlib import Path

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.events import InteractionEventV1
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
