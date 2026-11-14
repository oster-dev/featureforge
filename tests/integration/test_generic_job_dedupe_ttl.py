"""Verify the generic job's dedupe state: it protects inside the TTL and expires after it."""

import contextlib
import json
import re
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

import pytest
import yaml
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from featureforge.spec.flink_plan import build_flink_plan
from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.events import InteractionEventV1

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
FLINK_API = "http://127.0.0.1:8081"
SOURCE_SPEC = Path(__file__).resolve().parents[2] / "feature_repo/specs/user_play_count_1h.yaml"
FEATURE_NAME = "user_play_count_1h"
WAIT_SECONDS = 90

TTL_SECONDS = 30
DUPLICATE_AFTER = 15
EXPIRED_AFTER = 36

EVENT_TIME = datetime(2026, 1, 1, 11, 30, 0, tzinfo=UTC)
WINDOW_END = "2026-01-01T11:30:00Z"


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


def make_event(user_id: str, timestamp: datetime) -> InteractionEventV1:
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
    """The same logical event delivered again: only ingested_at differs."""
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
            "group.id": f"dedupe-ttl-audit-{uuid4().hex}",
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


def assert_no_record(consumer, predicate, *, seconds, description):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        message = consumer.poll(1.0)
        if message is None:
            continue

        assert message.error() is None, str(message.error())
        payload = message.value()
        assert payload is not None, "Unexpected Kafka tombstone"

        record = json.loads(payload.decode("utf-8"))
        assert not predicate(record), f"Unexpected {description}: {record}"


def is_window(record, *, user_id, minimum_count=None):
    return (
        record.get("feature_name") == FEATURE_NAME
        and record.get("user_id") == user_id
        and record.get("observation_time") == WINDOW_END
        and (minimum_count is None or record.get("play_count", 0) >= minimum_count)
    )


def short_ttl_spec(directory: Path) -> Path:
    """The real play-count spec with small windows so a short TTL passes validation."""
    spec = yaml.safe_load(SOURCE_SPEC.read_text())
    spec["window"] = {"size": "20s", "slide": "5s", "boundary": "right_closed"}
    spec["lateness"] = {"watermark_delay": "1s", "allowed": "10s"}
    spec["dedupe"] = {"key": "event_id", "ttl": f"{TTL_SECONDS}s"}
    path = directory / "user_play_count_dedupe_ttl.yaml"
    path.write_text(yaml.safe_dump(spec, sort_keys=False))
    assert build_flink_plan(load_spec(str(path))).dedupe_ttl_ms == TTL_SECONDS * 1000
    return path


def submit(
    *,
    run_id: str,
    spec_path: str,
    input_topic: str,
    feature_topic: str,
    late_topic: str,
) -> str:
    completed = run_flink(
        [
            "run",
            "--detached",
            "--parallelism",
            "1",
            "--python",
            "/opt/flink/jobs/feature_from_spec.py",
            "--spec",
            spec_path,
            "--input-topic",
            input_topic,
            "--feature-topic",
            feature_topic,
            "--late-topic",
            late_topic,
            "--group-id",
            f"dedupe-ttl-group-{run_id}",
            "--job-name",
            f"dedupe-ttl-{run_id}",
            "--no-restart",
        ]
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    match = re.search(r"Job has been submitted with JobID\s+([0-9a-fA-F]{32})", output)
    assert match is not None, f"Could not resolve submitted job ID:\n{output}"
    return match.group(1)


def test_dedupe_protects_inside_ttl_and_expires_after_it(tmp_path):
    run_id = uuid4().hex
    input_topic = f"featureforge-stream.integration.dedupe-ttl.input.{run_id}"
    feature_topic = f"featureforge-stream.integration.dedupe-ttl.features.{run_id}"
    late_topic = f"featureforge-stream.integration.dedupe-ttl.late.{run_id}"
    topics = [input_topic, feature_topic, late_topic]

    user_id = f"dedupe_ttl_user_{run_id}"
    progress_user = f"progress_user_{run_id}"
    container_spec = f"/tmp/user_play_count_dedupe_ttl_{run_id}.yaml"

    local_spec = short_ttl_spec(tmp_path)

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

        compose("cp", str(local_spec), f"jobmanager:{container_spec}")

        wait_for_free_slot()
        job_id = submit(
            run_id=run_id,
            spec_path=container_spec,
            input_topic=input_topic,
            feature_topic=feature_topic,
            late_topic=late_topic,
        )
        running_jobs.append(job_id)
        wait_for_job_running(job_id)

        # The progress event moves the watermark just past the window end (11:30:00)
        # but stays inside the 10 s allowed lateness. No later event is published, so
        # the window stays open for the redeliveries below.
        original = make_event(user_id, EVENT_TIME)
        publish(
            input_topic,
            [original, make_event(progress_user, EVENT_TIME + timedelta(seconds=2))],
        )

        first = wait_for_record(
            feature_consumer,
            lambda record: is_window(record, user_id=user_id),
            description="the first window value",
        )
        assert first["play_count"] == 1, first
        first_seen = time.monotonic()

        # Half way through the TTL a redelivery must be ignored. Duplicates must not
        # refresh the retention either, which the third delivery below depends on.
        time.sleep(max(0.0, first_seen + DUPLICATE_AFTER - time.monotonic()))
        publish(input_topic, [redelivered(original)])
        assert_no_record(
            feature_consumer,
            lambda record: is_window(record, user_id=user_id, minimum_count=2),
            seconds=5,
            description="count of 2 from a redelivery inside the TTL",
        )

        # The state was created before first_seen, so it has expired by now. A refresh
        # by the duplicate above would keep it alive until first_seen + 45 s.
        time.sleep(max(0.0, first_seen + EXPIRED_AFTER - time.monotonic()))
        publish(input_topic, [redelivered(original)])

        after = wait_for_record(
            feature_consumer,
            lambda record: is_window(record, user_id=user_id, minimum_count=2),
            description="count of at least 2 after the dedupe TTL expired",
        )
        assert after["play_count"] == 2, (
            f"Expected 2: an event redelivered after the TTL is counted again. Got: {after}"
        )

        low, high = late_consumer.get_watermark_offsets(
            TopicPartition(late_topic, 0), timeout=10, cached=False
        )
        assert low == high, f"Unexpected late events: offsets {low}..{high}"
    finally:
        try:
            for running in running_jobs:
                run_flink(["cancel", running])
                wait_for_job_terminal(running)
        finally:
            compose("exec", "-T", "jobmanager", "rm", "-f", container_spec, check=False)
            if feature_consumer is not None:
                feature_consumer.close()
            if late_consumer is not None:
                late_consumer.close()
            with contextlib.suppress(Exception):
                for future in admin.delete_topics(topics, operation_timeout=15).values():
                    future.result(timeout=30)
