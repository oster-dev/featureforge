"""Verify late correction and too-late routing in a continuous Flink job."""

import json
import re
import subprocess
import time
from datetime import UTC, datetime
from urllib.request import urlopen
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from featureforge.streaming.contracts.events import InteractionEventV1

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
FLINK_API = "http://127.0.0.1:8081"
WAIT_SECONDS = 90

HAND_WRITTEN_JOB = (
    "/opt/flink/jobs/user_play_count_1h.py",
    ["--bootstrap-servers", "kafka:29092"],
)
GENERIC_JOB = (
    "/opt/flink/jobs/feature_from_spec.py",
    ["--spec", "/opt/flink/specs/user_play_count_1h.yaml"],
)


def run_flink(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        [
            "docker",
            "compose",
            "exec",
            "-T",
            "jobmanager",
            "/opt/flink/bin/flink",
            *arguments,
        ],
        check=False,
        text=True,
        capture_output=True,
        timeout=180,
    )
    assert completed.returncode == 0, (
        f"Flink command failed: {arguments}\n"
        f"stdout:\n{completed.stdout}\n"
        f"stderr:\n{completed.stderr}"
    )
    return completed


def flink_api(path: str) -> dict:
    with urlopen(f"{FLINK_API}/{path}", timeout=10) as response:
        return json.load(response)


def wait_for_free_slot(timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    overview = None
    while time.monotonic() < deadline:
        overview = flink_api("overview")
        if overview["slots-available"] >= 1:
            return
        time.sleep(1)
    raise AssertionError(f"No free Flink slot within {timeout}s: {overview}")


def wait_for_job_terminal(job_id: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = flink_api(f"jobs/{job_id}")["state"]
        if state in {"CANCELED", "FAILED", "FINISHED"}:
            return
        time.sleep(1)
    raise AssertionError(f"Job {job_id} did not reach a terminal state; last={state}")


def make_event(user_id: str, hour: int, minute: int) -> InteractionEventV1:
    timestamp = datetime(2026, 1, 1, hour, minute, tzinfo=UTC)
    return InteractionEventV1(
        event_id=uuid4(),
        event_time=timestamp,
        ingested_at=timestamp,
        user_id=user_id,
        content_id=f"content_{uuid4().hex}",
        event_type="play",
        play_seconds=0,
        source="web",
        schema_version="v1",
    )


def publish(topic: str, event: InteractionEventV1) -> None:
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    producer.produce(
        topic,
        partition=0,
        value=event.model_dump_json().encode("utf-8"),
        on_delivery=delivered,
    )
    assert producer.flush(15) == 0, "Kafka producer has pending records"
    assert not errors, errors


def make_consumer(topic: str) -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"late-event-audit-{uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(topic, 0, 0)])
    return consumer


def wait_for_record(consumer, predicate, *, description):
    deadline = time.monotonic() + WAIT_SECONDS
    observed = []

    while time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None:
            continue

        assert message.error() is None, str(message.error())
        payload = message.value()
        assert payload is not None, "Unexpected Kafka tombstone"

        record = json.loads(payload.decode("utf-8"))
        observed.append(record)
        if predicate(record):
            return record

    raise AssertionError(
        f"Timed out waiting for {description}. Last observed records: {observed[-10:]}"
    )


def matches_feature(record, *, user_id, observation_time, play_count):
    return (
        record.get("feature_name") == "user_play_count_1h"
        and record.get("user_id") == user_id
        and record.get("observation_time") == observation_time
        and record.get("play_count") == play_count
    )


@pytest.mark.parametrize(
    ("job_script", "job_arguments"),
    [
        pytest.param(*HAND_WRITTEN_JOB, id="hand_written"),
        pytest.param(*GENERIC_JOB, id="generic_spec"),
    ],
)
def test_late_event_corrects_emitted_window_and_expired_event_is_routed(
    job_script: str, job_arguments: list[str]
):
    run_id = uuid4().hex
    input_topic = f"featureforge-stream.integration.lateness.input.{run_id}"
    feature_topic = f"featureforge-stream.integration.lateness.features.{run_id}"
    late_topic = f"featureforge-stream.integration.lateness.late.{run_id}"
    topics = [input_topic, feature_topic, late_topic]

    user_id = f"late_user_{run_id}"
    progress_user = f"progress_user_{run_id}"
    advance_user = f"advance_user_{run_id}"

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    feature_consumer = None
    late_consumer = None
    job_id = None

    try:
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
        for future in futures.values():
            future.result(timeout=30)

        feature_consumer = make_consumer(feature_topic)
        late_consumer = make_consumer(late_topic)

        wait_for_free_slot()
        completed = run_flink(
            [
                "run",
                "--detached",
                "--parallelism",
                "1",
                "--python",
                job_script,
                *job_arguments,
                "--input-topic",
                input_topic,
                "--feature-topic",
                feature_topic,
                "--late-topic",
                late_topic,
                "--group-id",
                f"late-event-group-{run_id}",
                "--job-name",
                f"late-event-job-{run_id}",
                "--no-restart",
            ]
        )

        output = f"{completed.stdout}\n{completed.stderr}"
        match = re.search(
            r"Job has been submitted with JobID\s+([0-9a-fA-F]{32})",
            output,
        )
        assert match is not None, f"Could not resolve submitted job ID:\n{output}"
        job_id = match.group(1)

        original = make_event(user_id, 11, 30)
        publish(input_topic, original)

        # Advance event time far enough to emit the noon window.
        publish(input_topic, make_event(progress_user, 12, 11))

        initial = wait_for_record(
            feature_consumer,
            lambda record: matches_feature(
                record,
                user_id=user_id,
                observation_time="2026-01-01T12:00:00Z",
                play_count=1,
            ),
            description="initial noon window with count one",
        )
        assert initial["window_start_exclusive"] == "2026-01-01T11:00:00Z"

        # A new event ID is an additional play, not a duplicate.
        correction = make_event(user_id, 11, 30)
        assert correction.event_id != original.event_id
        publish(input_topic, correction)

        corrected = wait_for_record(
            feature_consumer,
            lambda record: matches_feature(
                record,
                user_id=user_id,
                observation_time="2026-01-01T12:00:00Z",
                play_count=2,
            ),
            description="late correction of the already emitted noon window",
        )
        assert corrected["window_start_exclusive"] == initial["window_start_exclusive"]

        # Advance past all windows for an 11:30 event and their lateness.
        publish(input_topic, make_event(advance_user, 14, 0))

        wait_for_record(
            feature_consumer,
            lambda record: matches_feature(
                record,
                user_id=progress_user,
                observation_time="2026-01-01T13:10:00Z",
                play_count=1,
            ),
            description=("watermark-driven emission of the 13:10 progress window"),
        )

        too_late = make_event(user_id, 11, 30)
        assert too_late.event_id not in {
            original.event_id,
            correction.event_id,
        }
        publish(input_topic, too_late)

        rejection = wait_for_record(
            late_consumer,
            lambda record: (
                record.get("reason_code") == "TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS"
                and json.loads(record.get("raw_payload", "{}")).get("event_id")
                == str(too_late.event_id)
            ),
            description="too-late side-output record for the new event",
        )

        assert rejection["error_schema_version"] == "v1"
        assert rejection["feature_name"] == "user_play_count_1h"
        assert InteractionEventV1.model_validate_json(rejection["raw_payload"]) == too_late
    finally:
        try:
            if job_id is not None:
                run_flink(["cancel", job_id])
                wait_for_job_terminal(job_id)
        finally:
            if feature_consumer is not None:
                feature_consumer.close()
            if late_consumer is not None:
                late_consumer.close()

            existing_topics = admin.list_topics(timeout=10).topics
            cleanup_topics = [topic for topic in topics if topic in existing_topics]
            if cleanup_topics:
                futures = admin.delete_topics(
                    cleanup_topics,
                    request_timeout=15,
                    operation_timeout=15,
                )
                for future in futures.values():
                    future.result(timeout=30)
