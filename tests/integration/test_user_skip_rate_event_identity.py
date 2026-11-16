"""Verify that a conflicting event identity fails the skip-rate job explicitly."""

import subprocess
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from featureforge.streaming.contracts.events import InteractionEventV1

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
EVENT_TIME = datetime(2026, 1, 1, 11, 30, tzinfo=UTC)
SUBPROCESS_TIMEOUT_SECONDS = 180

SKIP_RATE_JOB = (
    "feature_from_spec.py",
    ["--spec", "/opt/flink/specs/user_skip_rate_24h.yaml"],
)


def create_topics(topics: list[str]) -> None:
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
        ],
        request_timeout=15,
        operation_timeout=15,
    )
    for topic, future in futures.items():
        try:
            future.result(timeout=30)
        except Exception as error:
            raise RuntimeError(f"Could not create topic {topic!r}") from error


def run_command(command: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
        timeout=SUBPROCESS_TIMEOUT_SECONDS,
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


def make_skip_event(
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
        event_type="skip",
        play_seconds=0,
        source="web",
        schema_version="v1",
    )


@pytest.mark.integration
def test_conflicting_event_identity_fails_the_skip_rate_job() -> None:
    run_id = uuid.uuid4().hex
    input_topic = f"featureforge-stream.integration.skip-conflict.input.{run_id}"
    validated_topic = f"featureforge-stream.integration.skip-conflict.validated.{run_id}"
    feature_topic = f"featureforge-stream.integration.skip-conflict.features.{run_id}"
    late_topic = f"featureforge-stream.integration.skip-conflict.late.{run_id}"
    rejected_topic = f"featureforge-stream.integration.skip-conflict.rejected.{run_id}"

    user_id = f"integration_skip_user_{run_id}"
    event_id = uuid.uuid4()

    create_topics([input_topic, validated_topic, feature_topic, late_topic, rejected_topic])

    original = make_skip_event(event_id, EVENT_TIME, user_id, f"content_{run_id}")
    conflicting = make_skip_event(
        event_id,
        EVENT_TIME + timedelta(minutes=1),
        user_id,
        f"conflicting_content_{run_id}",
    )
    produce_events(input_topic, [original, conflicting])

    validation = run_command(
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
    assert validation.returncode == 0, (
        f"Flink validation job failed:\n{validation.stdout}\n{validation.stderr}"
    )

    job_script, job_arguments = SKIP_RATE_JOB
    completed = run_command(
        flink_command(
            job_script,
            [
                *job_arguments,
                "--bootstrap-servers",
                "kafka:29092",
                "--input-topic",
                validated_topic,
                "--feature-topic",
                feature_topic,
                "--late-topic",
                late_topic,
                "--group-id",
                f"user-skip-rate-{run_id}",
                "--job-name",
                f"user-skip-rate-{run_id}",
                "--bounded",
                "--no-restart",
            ],
        )
    )
    output = f"{completed.stdout}\n{completed.stderr}"

    assert completed.returncode != 0, (
        f"Conflicting event identity unexpectedly completed successfully:\n{output}"
    )
    assert "EventIdentityConflictError" in output, output
    assert f"Conflicting logical payload for event_id={event_id}" in output, output
