"""Verify replay-safe Kafka-to-Flink-to-Redis user play-count materialization."""

import json
import subprocess
import sys
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from redis import Redis

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.online_store import RedisUserPlayCountStore

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
REDIS_URL = "redis://127.0.0.1:6379/0"
EVENT_TIME = datetime(2026, 1, 1, 11, 30, tzinfo=UTC)
SUBPROCESS_TIMEOUT_SECONDS = 180
CONFLICT_TIMEOUT_SECONDS = 30


def create_topics(bootstrap_servers: str, topics: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics]
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
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )

    if completed.returncode != 0:
        raise AssertionError(
            "Flink validation job failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def run_play_count_job(
    arguments: list[str],
    timeout_seconds: int = SUBPROCESS_TIMEOUT_SECONDS,
) -> None:
    try:
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
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as error:
        stderr = (error.stderr or b"").decode("utf-8", errors="replace")

        if "EventIdentityConflictError" in stderr:
            return

        raise AssertionError(
            "Flink user play-count job timed out without the expected "
            "event-identity conflict:\n"
            f"stdout:\n{error.stdout}\n"
            f"stderr:\n{stderr}"
        ) from error

    if completed.returncode != 0:
        if "EventIdentityConflictError" in completed.stderr:
            return

        raise AssertionError(
            "Flink user play-count job failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )


def run_materializer(arguments: list[str]) -> dict[str, dict[str, int]]:
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
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )

    if completed.returncode != 0:
        raise AssertionError(
            "Kafka-to-Redis materializer failed:\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )

    return json.loads(completed.stdout)


def produce_events(topic: str, payloads: list[str]) -> None:
    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})

    for payload in payloads:
        producer.produce(topic, value=payload.encode("utf-8"))

    assert producer.flush(10) == 0


def make_event(
    event_id: uuid.UUID,
    ingested_at: datetime,
    user_id: str,
    content_id: str,
) -> InteractionEventV1:
    return InteractionEventV1(
        event_id=event_id,
        event_time=EVENT_TIME,
        ingested_at=ingested_at,
        user_id=user_id,
        content_id=content_id,
        event_type="play",
        play_seconds=0,
        source="web",
        schema_version="v1",
    )


@pytest.mark.integration
def test_replayed_play_event_does_not_change_redis_value() -> None:
    run_id = uuid.uuid4().hex
    input_topic = f"featureforge-stream.integration.replay.input.{run_id}"
    validated_topic = f"featureforge-stream.integration.replay.validated.{run_id}"
    feature_topic = f"featureforge-stream.integration.replay.features.{run_id}"
    late_topic = f"featureforge-stream.integration.replay.late.{run_id}"
    key_prefix = f"featureforge-stream:integration:replay:{run_id}"

    user_id = f"integration_user_{run_id}"
    content_id = f"content_{run_id}"
    event_id = uuid.uuid4()

    create_topics(
        BOOTSTRAP_SERVERS,
        [input_topic, validated_topic, feature_topic, late_topic],
    )

    original_event = make_event(
        event_id=event_id,
        ingested_at=EVENT_TIME,
        user_id=user_id,
        content_id=content_id,
    )
    replayed_event = make_event(
        event_id=event_id,
        ingested_at=EVENT_TIME + timedelta(minutes=1),
        user_id=user_id,
        content_id=content_id,
    )

    produce_events(
        input_topic,
        [
            original_event.model_dump_json(),
            replayed_event.model_dump_json(),
        ],
    )

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

    materializer_arguments = [
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

    first_result = run_materializer(materializer_arguments)
    assert first_result == {"store_results": {"written": 60}}

    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        store = RedisUserPlayCountStore(redis_client, key_prefix=key_prefix)
        first_feature = store.read(user_id)

    assert first_feature is not None
    assert first_feature.play_count == 1
    assert first_feature.observation_time == datetime(2026, 1, 1, 12, 29, tzinfo=UTC)
    assert first_feature.window_start_exclusive == datetime(
        2026, 1, 1, 11, 29, tzinfo=UTC
    )

    replayed_again = make_event(
        event_id=event_id,
        ingested_at=EVENT_TIME + timedelta(minutes=2),
        user_id=user_id,
        content_id=content_id,
    )

    produce_events(input_topic, [replayed_again.model_dump_json()])

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
            f"validation-replay-{run_id}",
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
            f"user-play-count-replay-{run_id}",
            "--bounded",
        ]
    )

    second_result = run_materializer(materializer_arguments)
    assert second_result == {"store_results": {"stale": 59, "unchanged": 1}}

    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        store = RedisUserPlayCountStore(redis_client, key_prefix=key_prefix)
        replayed_feature = store.read(user_id)

    assert replayed_feature == first_feature


@pytest.mark.integration
def test_conflicting_event_identity_fails_the_play_count_job() -> None:
    run_id = uuid.uuid4().hex
    input_topic = f"featureforge-stream.integration.conflict.input.{run_id}"
    validated_topic = f"featureforge-stream.integration.conflict.validated.{run_id}"
    feature_topic = f"featureforge-stream.integration.conflict.features.{run_id}"
    late_topic = f"featureforge-stream.integration.conflict.late.{run_id}"

    user_id = f"integration_user_{run_id}"
    event_id = uuid.uuid4()

    create_topics(
        BOOTSTRAP_SERVERS,
        [input_topic, validated_topic, feature_topic, late_topic],
    )

    original_event = make_event(
        event_id=event_id,
        ingested_at=EVENT_TIME,
        user_id=user_id,
        content_id=f"content_{run_id}",
    )
    conflicting_event = make_event(
        event_id=event_id,
        ingested_at=EVENT_TIME + timedelta(minutes=1),
        user_id=user_id,
        content_id=f"conflicting_content_{run_id}",
    )

    produce_events(
        input_topic,
        [
            original_event.model_dump_json(),
            conflicting_event.model_dump_json(),
        ],
    )

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
            f"validation-conflict-{run_id}",
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
            f"user-play-count-conflict-{run_id}",
            "--bounded",
        ],
        timeout_seconds=CONFLICT_TIMEOUT_SECONDS,
    )