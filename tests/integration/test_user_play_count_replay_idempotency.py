"""Verify bounded replay idempotency and explicit event-identity failures."""

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


def create_topics(bootstrap_servers: str, topics: list[str]) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=1, replication_factor=1) for topic in topics],
        request_timeout=15,
        operation_timeout=15,
    )

    for topic, future in futures.items():
        try:
            future.result(timeout=30)
        except Exception as error:
            raise RuntimeError(f"Could not create topic {topic!r}") from error


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    """Capture process results; timeouts remain failures."""
    return subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
    )


def assert_success(
    completed: subprocess.CompletedProcess[str],
    description: str,
) -> None:
    assert completed.returncode == 0, (
        f"{description} failed with exit code {completed.returncode}:\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )


def flink_command(script: str, arguments: list[str]) -> list[str]:
    return [
        "docker",
        "compose",
        "exec",
        "-T",
        "jobmanager",
        "python",
        f"/opt/flink/jobs/{script}",
        *arguments,
    ]


def run_validation(
    input_topic: str,
    validated_topic: str,
    rejected_topic: str,
    run_id: str,
) -> None:
    completed = run_command(
        flink_command(
            "validate_and_route_events.py",
            [
                "--bootstrap-servers",
                "kafka:29092",
                "--input-topic",
                input_topic,
                "--validated-topic",
                validated_topic,
                "--rejected-topic",
                rejected_topic,
                "--group-id",
                f"validation-{run_id}",
                "--job-name",
                f"validation-{run_id}",
                "--bounded",
            ],
        )
    )
    assert_success(completed, "Flink validation job")


def run_play_count(
    validated_topic: str,
    feature_topic: str,
    late_topic: str,
    run_id: str,
    *,
    no_restart: bool = False,
) -> subprocess.CompletedProcess[str]:
    arguments = [
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
        "--job-name",
        f"user-play-count-{run_id}",
        "--bounded",
    ]
    if no_restart:
        arguments.append("--no-restart")

    return run_command(flink_command("user_play_count_1h.py", arguments))


def run_materializer(arguments: list[str]) -> dict[str, dict[str, int]]:
    completed = run_command(
        [
            sys.executable,
            "-m",
            "featureforge.streaming.materialize",
            *arguments,
        ]
    )
    assert_success(completed, "Kafka-to-Redis materializer")
    return json.loads(completed.stdout)


def produce_events(topic: str, events: list[InteractionEventV1]) -> None:
    errors: list[str] = []

    def on_delivery(error, message) -> None:
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for event in events:
        producer.produce(
            topic,
            value=event.model_dump_json().encode("utf-8"),
            on_delivery=on_delivery,
        )

    assert producer.flush(15) == 0, "Kafka producer still has pending messages"
    assert not errors, f"Kafka delivery failed: {errors}"


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
    rejected_topic = f"featureforge-stream.integration.replay.rejected.{run_id}"
    key_prefix = f"featureforge-stream:integration:replay:{run_id}"

    user_id = f"integration_user_{run_id}"
    content_id = f"content_{run_id}"
    event_id = uuid.uuid4()

    create_topics(
        BOOTSTRAP_SERVERS,
        [input_topic, validated_topic, feature_topic, late_topic, rejected_topic],
    )

    original = make_event(event_id, EVENT_TIME, user_id, content_id)
    replay = make_event(
        event_id,
        EVENT_TIME + timedelta(minutes=1),
        user_id,
        content_id,
    )
    produce_events(input_topic, [original, replay])

    run_validation(input_topic, validated_topic, rejected_topic, run_id)
    completed = run_play_count(
        validated_topic,
        feature_topic,
        late_topic,
        run_id,
    )
    assert_success(completed, "Initial Flink user play-count job")

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

    with Redis.from_url(
        REDIS_URL,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        store = RedisUserPlayCountStore(redis_client, key_prefix=key_prefix)
        key = store.key_for(user_id)

        try:
            first_result = run_materializer(materializer_arguments)
            assert first_result == {"store_results": {"written": 60}}

            first_feature = store.read(user_id)
            first_payload = redis_client.get(key)

            assert first_feature is not None
            assert first_feature.user_id == user_id
            assert first_feature.play_count == 1
            assert first_feature.observation_time == datetime(2026, 1, 1, 12, 29, tzinfo=UTC)
            assert first_feature.window_start_exclusive == datetime(2026, 1, 1, 11, 29, tzinfo=UTC)

            replayed_again = make_event(
                event_id,
                EVENT_TIME + timedelta(minutes=2),
                user_id,
                content_id,
            )
            produce_events(input_topic, [replayed_again])

            replay_run_id = f"replay-{run_id}"
            run_validation(
                input_topic,
                validated_topic,
                rejected_topic,
                replay_run_id,
            )
            completed = run_play_count(
                validated_topic,
                feature_topic,
                late_topic,
                replay_run_id,
            )
            assert_success(completed, "Replay Flink user play-count job")

            second_result = run_materializer(materializer_arguments)
            assert second_result == {"store_results": {"stale": 59, "unchanged": 1}}

            assert store.read(user_id) == first_feature
            assert redis_client.get(key) == first_payload
        finally:
            redis_client.delete(key)


@pytest.mark.integration
def test_conflicting_event_identity_fails_the_play_count_job() -> None:
    run_id = uuid.uuid4().hex
    input_topic = f"featureforge-stream.integration.conflict.input.{run_id}"
    validated_topic = f"featureforge-stream.integration.conflict.validated.{run_id}"
    feature_topic = f"featureforge-stream.integration.conflict.features.{run_id}"
    late_topic = f"featureforge-stream.integration.conflict.late.{run_id}"
    rejected_topic = f"featureforge-stream.integration.conflict.rejected.{run_id}"

    user_id = f"integration_user_{run_id}"
    event_id = uuid.uuid4()

    create_topics(
        BOOTSTRAP_SERVERS,
        [input_topic, validated_topic, feature_topic, late_topic, rejected_topic],
    )

    original = make_event(
        event_id,
        EVENT_TIME,
        user_id,
        f"content_{run_id}",
    )
    conflicting = make_event(
        event_id,
        EVENT_TIME + timedelta(minutes=1),
        user_id,
        f"conflicting_content_{run_id}",
    )
    produce_events(input_topic, [original, conflicting])

    run_validation(input_topic, validated_topic, rejected_topic, run_id)
    completed = run_play_count(
        validated_topic,
        feature_topic,
        late_topic,
        run_id,
        no_restart=True,
    )

    output = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, (
        f"Conflicting event identity unexpectedly completed successfully:\n{output}"
    )
    assert "EventIdentityConflictError" in output, output
    assert f"Conflicting logical payload for event_id={event_id}" in output, output
