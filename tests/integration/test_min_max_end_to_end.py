"""Prove that min and max reach Redis from events (ADR-017).

The golden events go through the validation job, the generic spec-driven Flink job and the
generic materializer, and the value is read back through the generic store.
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
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.spec_store import RedisSpecStore

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
KAFKA_IN_COMPOSE = "kafka:29092"
REDIS_URL = os.environ.get("FEATUREFORGE_REDIS_URL", "redis://127.0.0.1:6379/0")
ROOT = Path(__file__).resolve().parents[2]
SPEC_PATH = ROOT / "fixtures" / "specs" / "user_play_seconds_extremes_1h.yaml"
FIXTURE = ROOT / "fixtures" / "streaming" / "user_play_seconds_1h_v1.jsonl"
CONTAINER_SPEC = "/opt/flink/fixture-specs/user_play_seconds_extremes_1h.yaml"


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


def test_min_max_and_missing_extremes_reach_redis_from_events():
    spec = load_spec(SPEC_PATH)
    lines = [line for line in FIXTURE.read_text(encoding="utf-8").splitlines() if line.strip()]
    events = [InteractionEventV1.model_validate_json(line) for line in lines]
    ordered = sorted(lines, key=lambda line: json.loads(line)["event_time"])

    run_id = uuid.uuid4().hex
    topics = {
        kind: f"featureforge-stream.integration.extremes-proof.{kind}.{run_id}"
        for kind in ("input", "validated", "rejected", "features", "late")
    }
    key_prefix = f"featureforge-stream:integration:extremes-proof:{run_id}"
    expected = {
        "user000001": (1, 500, 500, datetime(2026, 1, 1, 12, 0, tzinfo=UTC)),
        "user000002": (1, 200, 200, None),
        "user000003": (0, None, None, datetime(2026, 1, 1, 11, 29, tzinfo=UTC)),
    }

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as client:
        try:
            create_topics(list(topics.values()))
            publish_raw(topics["input"], [line.encode("utf-8") for line in ordered])

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
                    CONTAINER_SPEC,
                    "--bootstrap-servers",
                    KAFKA_IN_COMPOSE,
                    "--input-topic",
                    topics["validated"],
                    "--feature-topic",
                    topics["features"],
                    "--late-topic",
                    topics["late"],
                    "--group-id",
                    f"extremes-proof-{run_id}",
                    "--job-name",
                    f"extremes-proof-{run_id}",
                    "--bounded",
                ],
                "generic spec-driven job",
            )

            group_id = f"extremes-proof-materializer-{run_id}"
            results = run_materializer(topics["features"], key_prefix, group_id)
            assert set(results) <= {"written", "unchanged", "stale"}
            assert sum(results.values()) > 200

            store = RedisSpecStore(client, spec, key_prefix=key_prefix)
            finals = {}
            for user, (count, smallest, largest, last) in expected.items():
                stored = store.read(user)
                assert stored is not None, user
                assert dict(stored.counts) == {
                    "play_count": count,
                    "min_play_seconds": smallest,
                    "max_play_seconds": largest,
                }
                if last is not None:
                    assert stored.observation_time == last
                reference = evaluate_spec(
                    spec,
                    events,
                    entity={"user_id": user},
                    observation_time=stored.observation_time,
                )
                assert reference.values == {**stored.counts, **stored.derived}
                finals[user] = stored

            ttl = int(spec.online.ttl.total_seconds())
            assert 0 < client.ttl(store.key_for("user000001")) <= ttl

            assert run_materializer(topics["features"], key_prefix, group_id) == {}

            replay = run_materializer(topics["features"], key_prefix, f"{group_id}-replay")
            assert set(replay) <= {"stale", "unchanged"}
            for user, stored in finals.items():
                assert store.read(user) == stored
        finally:
            keys = list(client.scan_iter(match=f"{key_prefix}:*"))
            if keys:
                client.delete(*keys)
            with contextlib.suppress(Exception):
                for future in admin.delete_topics(
                    list(topics.values()), operation_timeout=15
                ).values():
                    future.result(timeout=30)
