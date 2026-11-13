"""Verify that the generic job recovers from a TaskManager crash on its own."""

import contextlib
import json
import re
import subprocess
import time
from datetime import UTC, datetime, timedelta
from urllib.request import urlopen
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from featureforge.streaming.contracts.events import InteractionEventV1

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
FLINK_API = "http://127.0.0.1:8081"
SPEC = "/opt/flink/specs/user_play_count_1h.yaml"
WAIT_SECONDS = 90


def compose(*arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        ["docker", "compose", *arguments],
        check=False,
        text=True,
        capture_output=True,
        timeout=180,
    )
    if check:
        assert completed.returncode == 0, (
            f"docker compose {arguments} failed\n"
            f"stdout:\n{completed.stdout}\n"
            f"stderr:\n{completed.stderr}"
        )
    return completed


def run_flink(arguments: list[str]) -> subprocess.CompletedProcess[str]:
    return compose("exec", "-T", "jobmanager", "/opt/flink/bin/flink", *arguments)


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


def wait_for_job_terminal(job_id: str, timeout: float = 120.0) -> str:
    deadline = time.monotonic() + timeout
    state = None
    while time.monotonic() < deadline:
        state = flink_api(f"jobs/{job_id}")["state"]
        if state in {"CANCELED", "FAILED", "FINISHED"}:
            return state
        time.sleep(1)
    raise AssertionError(f"Job {job_id} did not reach a terminal state; last={state}")


def wait_for_job_running(job_id: str, timeout: float = 240.0) -> None:
    deadline = time.monotonic() + timeout
    job = None
    while time.monotonic() < deadline:
        job = flink_api(f"jobs/{job_id}")
        assert job["state"] not in {"FAILED", "CANCELED", "FINISHED"}, job
        vertices = job.get("vertices", [])
        if (
            job["state"] == "RUNNING"
            and vertices
            and all(vertex["status"] == "RUNNING" for vertex in vertices)
        ):
            return
        time.sleep(1)
    raise AssertionError(f"Job {job_id} not RUNNING within {timeout}s: {job}")


def checkpoint_info(job_id: str) -> dict:
    return flink_api(f"jobs/{job_id}/checkpoints")


def wait_for_checkpoint_after(job_id: str, triggered_before: int, timeout: float = 90.0) -> int:
    """Wait for a completed checkpoint that was triggered after `triggered_before`."""
    deadline = time.monotonic() + timeout
    info = None
    while time.monotonic() < deadline:
        info = checkpoint_info(job_id)
        completed = info.get("latest", {}).get("completed")
        if completed is not None and completed["id"] > triggered_before:
            return completed["id"]
        time.sleep(1)
    raise AssertionError(
        f"No checkpoint completed after id {triggered_before} within {timeout}s: {info}"
    )


def wait_for_restore(job_id: str, timeout: float = 240.0) -> dict:
    deadline = time.monotonic() + timeout
    job = None
    while time.monotonic() < deadline:
        job = flink_api(f"jobs/{job_id}")
        assert job["state"] not in {"FAILED", "CANCELED", "FINISHED"}, job
        restored = checkpoint_info(job_id).get("latest", {}).get("restored")
        if restored is not None:
            return restored
        time.sleep(1)
    raise AssertionError(f"Job {job_id} never restored a checkpoint: {job}")


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


def redelivered(event: InteractionEventV1) -> InteractionEventV1:
    """The same logical event delivered again later: only ingested_at differs."""
    return event.model_copy(update={"ingested_at": event.ingested_at + timedelta(minutes=5)})


def publish(topic: str, events: list[InteractionEventV1]) -> None:
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for event in events:
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
            "group.id": f"checkpoint-audit-{uuid4().hex}",
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


def is_window(record, *, user_id, observation_time, minimum_count=None):
    return (
        record.get("feature_name") == "user_play_count_1h"
        and record.get("user_id") == user_id
        and record.get("observation_time") == observation_time
        and (minimum_count is None or record.get("play_count", 0) >= minimum_count)
    )


def submit(
    *,
    run_id: str,
    input_topic: str,
    feature_topic: str,
    late_topic: str,
) -> str:
    """Submit the generic job with the cluster restart strategy (no --no-restart)."""
    completed = run_flink(
        [
            "run",
            "--detached",
            "--parallelism",
            "1",
            "--python",
            "/opt/flink/jobs/feature_from_spec.py",
            "--spec",
            SPEC,
            "--input-topic",
            input_topic,
            "--feature-topic",
            feature_topic,
            "--late-topic",
            late_topic,
            "--group-id",
            f"checkpoint-group-{run_id}",
            "--job-name",
            f"checkpoint-recovery-{run_id}",
        ]
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    match = re.search(r"Job has been submitted with JobID\s+([0-9a-fA-F]{32})", output)
    assert match is not None, f"Could not resolve submitted job ID:\n{output}"
    return match.group(1)


def test_generic_job_recovers_from_taskmanager_crash_with_checkpoint_state():
    run_id = uuid4().hex
    input_topic = f"featureforge-stream.integration.checkpoint.input.{run_id}"
    feature_topic = f"featureforge-stream.integration.checkpoint.features.{run_id}"
    late_topic = f"featureforge-stream.integration.checkpoint.late.{run_id}"
    topics = [input_topic, feature_topic, late_topic]

    user_id = f"checkpoint_user_{run_id}"
    progress_user = f"progress_user_{run_id}"
    noon = "2026-01-01T12:00:00Z"

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    feature_consumer = None
    late_consumer = None
    running_jobs: list[str] = []

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
        job_id = submit(
            run_id=run_id,
            input_topic=input_topic,
            feature_topic=feature_topic,
            late_topic=late_topic,
        )
        running_jobs.append(job_id)
        wait_for_job_running(job_id)
        assert checkpoint_info(job_id).get("latest", {}).get("restored") is None, (
            "A fresh job must not report a restored checkpoint"
        )

        original = make_event(user_id, 11, 30)
        publish(input_topic, [original, make_event(progress_user, 12, 11)])

        before = wait_for_record(
            feature_consumer,
            lambda record: is_window(record, user_id=user_id, observation_time=noon),
            description="noon window before the crash",
        )
        assert before["play_count"] == 1, before

        # Only a checkpoint triggered after this output contains the state of the
        # original event. Ids are assigned in trigger order.
        triggered = checkpoint_info(job_id)["counts"]["total"]
        covering_checkpoint = wait_for_checkpoint_after(job_id, triggered)

        compose("kill", "taskmanager")
        compose("up", "-d", "taskmanager")

        restored = wait_for_restore(job_id)
        assert restored.get("is_savepoint") is False, restored
        assert restored["id"] >= covering_checkpoint, (
            f"Restored checkpoint {restored['id']} is older than the checkpoint "
            f"{covering_checkpoint} that holds the state: {restored}"
        )
        wait_for_job_running(job_id)

        # A redelivery of the first event must not count again (dedupe state).
        # The correction is a new event in the same window (window state).
        correction = make_event(user_id, 11, 30)
        assert correction.event_id != original.event_id
        publish(
            input_topic,
            [redelivered(original), correction, make_event(progress_user, 12, 11)],
        )

        after = wait_for_record(
            feature_consumer,
            lambda record: is_window(
                record, user_id=user_id, observation_time=noon, minimum_count=2
            ),
            description="noon window with a count of at least 2 after the crash",
        )
        assert after["play_count"] == 2, (
            "Expected 2 (original once, correction once). 3 means the dedupe state "
            f"was lost. Got: {after}"
        )

        low, high = late_consumer.get_watermark_offsets(
            TopicPartition(late_topic, 0), timeout=10, cached=False
        )
        assert low == high, f"Unexpected late events: offsets {low}..{high}"
    finally:
        try:
            compose("up", "-d", "taskmanager", check=False)
            for running in running_jobs:
                run_flink(["cancel", running])
                wait_for_job_terminal(running)
        finally:
            if feature_consumer is not None:
                feature_consumer.close()
            if late_consumer is not None:
                late_consumer.close()
            with contextlib.suppress(Exception):
                for future in admin.delete_topics(topics, operation_timeout=15).values():
                    future.result(timeout=30)
