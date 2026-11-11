"""Verify the joined Kafka-to-Flink-to-Redis user play-count flow."""

import subprocess
import sys
import uuid
from datetime import UTC, datetime

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from redis import Redis

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.online_store import RedisUserPlayCountStore

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
REDIS_URL = "redis://127.0.0.1:6379/0"
EVENT_TIME = datetime(2026, 1, 1, 11, 30, tzinfo=UTC)


def create_topics(bootstrap_servers: str, topics: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
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


def run_flink_job(arguments: list[str]) -> None:
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "jobmanager",
            "python",
            "/opt/flink/jobs/validate_and_route_events.py",
            *arguments,
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    if completed.returncode != 0:
        raise AssertionError(
            "Flink validation job failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def run_play_count_job(arguments: list[str]) -> None:
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "jobmanager",
            "python",
            "/opt/flink/jobs/user_play_count_1h.py",
            *arguments,
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    if completed.returncode != 0:
        raise AssertionError(
            "Flink user play-count job failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def run_materializer(arguments: list[str]) -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "featureforge.streaming.materialize",
            *arguments,
        ],
        check=False,
        text=True,
        capture_output=True,
    )

    if completed.returncode != 0:
        raise AssertionError(
            "Kafka-to-Redis materializer failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


@pytest.mark.integration
def test_user_play_count_flows_from_kafka_to_redis() -> None:
    run_id = uuid.uuid4().hex
    input_topic = f"featureforge-stream.integration.input.{run_id}"
    validated_topic = f"featureforge-stream.integration.validated.{run_id}"
    feature_topic = f"featureforge-stream.integration.features.{run_id}"
    late_topic = f"featureforge-stream.integration.late.{run_id}"
    key_prefix = f"featureforge-stream:integration:kafka-to-redis:{run_id}"

    user_id = f"integration_user_{run_id}"
    event = InteractionEventV1(
        event_id=uuid.uuid4(),
        event_time=EVENT_TIME,
        ingested_at=EVENT_TIME,
        user_id=user_id,
        content_id=f"content_{run_id}",
        event_type="play",
        play_seconds=0,
        source="web",
        schema_version="v1",
    )
    payload = event.model_dump_json()

    create_topics(
        BOOTSTRAP_SERVERS,
        [input_topic, validated_topic, feature_topic, late_topic],
    )

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    producer.produce(input_topic, value=payload.encode("utf-8"))
    producer.flush(10)
    assert producer.flush(10) == 0

    run_flink_job(
        [
            "--bootstrap-servers",
            "kafka:29092",
            "--input-topic",
            input_topic,
            "--validated-topic",
            validated_topic,
            "--rejected-topic",
            late_topic,
            "--group-id",
            f"validation-{run_id}",
            "--bounded",
        ]
    )

    run_play_count_job(
        [
            "--bootstrap-servers",
            "kafka:29092",
            "--input-topic",
            validated_topic,
            "--feature-topic",
            feature_topic,
            "--late-topic",
            late_topic,
            "--group-id",
            f"user-play-count-{run_id}",
            "--bounded",
        ]
    )

    run_materializer(
        [
            "--bootstrap-servers",
            BOOTSTRAP_SERVERS,
            "--redis-url",
            REDIS_URL,
            "--topic",
            feature_topic,
            "--group-id",
            f"materializer-{run_id}",
            "--key-prefix",
            key_prefix,
            "--bounded",
        ]
    )

    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        store = RedisUserPlayCountStore(redis_client, key_prefix=key_prefix)
        feature = store.read(user_id)

    assert feature is not None
    assert feature.user_id == user_id
    assert feature.play_count == 1
    assert feature.observation_time == datetime(2026, 1, 1, 12, 29, tzinfo=UTC)
    assert feature.window_start_exclusive == datetime(2026, 1, 1, 11, 29, tzinfo=UTC)
