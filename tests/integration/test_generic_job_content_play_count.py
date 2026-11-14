"""A feature defined only by a YAML spec runs on the generic job with another entity key."""

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

from featureforge.spec.flink_plan import build_flink_plan
from featureforge.spec.loader import load_spec
from featureforge.streaming.contracts.events import InteractionEventV1

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
FLINK_API = "http://127.0.0.1:8081"
SPEC_FILE = "feature_repo/specs/content_play_count_1h.yaml"
CONTAINER_SPEC = "/opt/flink/specs/content_play_count_1h.yaml"
FEATURE_NAME = "content_play_count_1h"
WAIT_SECONDS = 120

HOUR = timedelta(hours=1)
MINUTE = timedelta(minutes=1)
START = datetime(2026, 1, 1, 10, 0, 0, tzinfo=UTC)

EXPECTED_FIELDS = {
    "feature_schema_version",
    "feature_name",
    "content_id",
    "observation_time",
    "window_start_exclusive",
    "play_count",
}

# Plays per content: (user, minutes after START). The same user plays content A twice.
PLAYS = {
    "a": [("user_a1", 0), ("user_a2", 20), ("user_a1", 40)],
    "b": [("user_b1", 30)],
}

# Hand-calculated windows (t - 1 h, t]: (content, minutes after START) -> play count.
# A at 60 min excludes the play at 0 min (lower edge). A at 80 min excludes the play at
# 20 min. 99 and 89 minutes are the last windows containing the last play of A and B.
HAND_CALCULATED = {
    ("a", 0): 1,
    ("a", 20): 2,
    ("a", 40): 3,
    ("a", 60): 2,
    ("a", 80): 1,
    ("a", 99): 1,
    ("b", 30): 1,
    ("b", 89): 1,
}


def iso(timestamp: datetime) -> str:
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


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


def make_event(user_id: str, content_id: str, timestamp: datetime) -> InteractionEventV1:
    return InteractionEventV1(
        event_id=uuid4(),
        event_time=timestamp,
        ingested_at=timestamp,
        user_id=user_id,
        content_id=content_id,
        event_type="play",
        play_seconds=0,
        source="web",
        schema_version="v1",
    )


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
            "group.id": f"content-play-count-audit-{uuid4().hex}",
            "enable.auto.commit": False,
        }
    )
    consumer.assign([TopicPartition(topic, 0, 0)])
    return consumer


def window_ends(minutes: list[int]) -> list[datetime]:
    """Right-closed windows: a play at t belongs to the windows ending in [t, t + 1 h)."""
    ends = set()
    for offset in minutes:
        end = START + timedelta(minutes=offset)
        while end < START + timedelta(minutes=offset) + HOUR:
            ends.add(end)
            end += MINUTE
    return sorted(ends)


def expected_count(minutes: list[int], observation: datetime) -> int:
    return sum(
        1
        for offset in minutes
        if observation - HOUR < START + timedelta(minutes=offset) <= observation
    )


def submit(*, run_id: str, input_topic: str, feature_topic: str, late_topic: str) -> str:
    completed = run_flink(
        [
            "run",
            "--detached",
            "--parallelism",
            "1",
            "--python",
            "/opt/flink/jobs/feature_from_spec.py",
            "--spec",
            CONTAINER_SPEC,
            "--input-topic",
            input_topic,
            "--feature-topic",
            feature_topic,
            "--late-topic",
            late_topic,
            "--group-id",
            f"content-play-count-group-{run_id}",
            "--job-name",
            f"content-play-count-{run_id}",
            "--no-restart",
        ]
    )
    output = f"{completed.stdout}\n{completed.stderr}"
    match = re.search(r"Job has been submitted with JobID\s+([0-9a-fA-F]{32})", output)
    assert match is not None, f"Could not resolve submitted job ID:\n{output}"
    return match.group(1)


def collect_windows(consumer, contents, expected_keys, *, duplicates, records, seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline and not expected_keys <= records.keys():
        drain(consumer, contents, duplicates, records, 1.0)


def drain(consumer, contents, duplicates, records, timeout):
    message = consumer.poll(timeout)
    if message is None:
        return
    assert message.error() is None, str(message.error())
    payload = message.value()
    assert payload is not None, "Unexpected Kafka tombstone"
    record = json.loads(payload.decode("utf-8"))
    if record.get("content_id") not in contents:
        return
    key = (record["content_id"], record["observation_time"])
    if key in records:
        duplicates.append(key)
    records[key] = record


def test_content_play_count_runs_from_yaml_only():
    plan = build_flink_plan(load_spec(SPEC_FILE))
    assert plan.entity_key == "content_id"
    assert plan.feature_name == FEATURE_NAME

    run_id = uuid4().hex
    input_topic = f"featureforge-stream.integration.content-play.input.{run_id}"
    feature_topic = f"featureforge-stream.integration.content-play.features.{run_id}"
    late_topic = f"featureforge-stream.integration.content-play.late.{run_id}"
    topics = [input_topic, feature_topic, late_topic]

    contents = {"a": f"content_a_{run_id}", "b": f"content_b_{run_id}"}
    progress_content = f"content_progress_{run_id}"
    minutes = {label: [offset for _, offset in plays] for label, plays in PLAYS.items()}

    expected_keys = {
        (contents[label], iso(end))
        for label, offsets in minutes.items()
        for end in window_ends(offsets)
    }

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

        events = [
            make_event(user, contents[label], START + timedelta(minutes=offset))
            for label, plays in PLAYS.items()
            for user, offset in plays
        ]
        # A later event moves the watermark past the last window of both contents.
        events.append(
            make_event(f"progress_user_{run_id}", progress_content, START + timedelta(minutes=131))
        )
        publish(input_topic, events)

        records: dict = {}
        duplicates: list = []
        collect_windows(
            feature_consumer,
            set(contents.values()),
            expected_keys,
            duplicates=duplicates,
            records=records,
            seconds=WAIT_SECONDS,
        )
        # Keep reading briefly to catch a window that is emitted twice.
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            drain(feature_consumer, set(contents.values()), duplicates, records, 1.0)

        assert set(records) == expected_keys, (
            f"missing: {sorted(expected_keys - set(records))[:5]}, "
            f"unexpected: {sorted(set(records) - expected_keys)[:5]}"
        )
        assert not duplicates, f"windows emitted more than once: {duplicates[:5]}"

        content_to_label = {content: label for label, content in contents.items()}
        for (content_id, observation_text), record in records.items():
            observation = datetime.strptime(observation_text, "%Y-%m-%dT%H:%M:%SZ").replace(
                tzinfo=UTC
            )
            label = content_to_label[content_id]
            assert set(record) == EXPECTED_FIELDS, record
            assert record["feature_schema_version"] == "v1", record
            assert record["feature_name"] == FEATURE_NAME, record
            assert record["window_start_exclusive"] == iso(observation - HOUR), record
            assert record["play_count"] == expected_count(minutes[label], observation), record

        for (label, offset), count in HAND_CALCULATED.items():
            key = (contents[label], iso(START + timedelta(minutes=offset)))
            assert records[key]["play_count"] == count, (label, offset, records[key])

        # Content A counts plays of different users together: 3 plays from 2 users.
        peak = records[(contents["a"], iso(START + timedelta(minutes=40)))]
        assert peak["play_count"] == 3, peak

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
            if feature_consumer is not None:
                feature_consumer.close()
            if late_consumer is not None:
                late_consumer.close()
            with contextlib.suppress(Exception):
                for future in admin.delete_topics(topics, operation_timeout=15).values():
                    future.result(timeout=30)
