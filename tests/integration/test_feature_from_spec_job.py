"""The generic spec-driven job must reproduce the golden fixtures of both features."""

import json
import re
import subprocess
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.request import urlopen
from uuid import uuid4

import pytest

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.reference.user_play_count import count_user_plays_1h
from featureforge.streaming.reference.user_skip_rate import calculate_user_skip_rate_24h

pytestmark = pytest.mark.integration

ROOT = Path(__file__).resolve().parents[2]
SPECS = ROOT / "feature_repo" / "specs"
FIXTURES = ROOT / "fixtures" / "streaming"
CONTAINER_SPECS = "/opt/flink/specs"

BASE_FIELDS = {
    "feature_schema_version",
    "feature_name",
    "user_id",
    "observation_time",
    "window_start_exclusive",
}


def skip_rate_reference(
    events: list[InteractionEventV1], user_id: str, observation: datetime
) -> dict[str, float]:
    return {
        "skip_rate": calculate_user_skip_rate_24h(
            events, user_id=user_id, observation_time=observation
        )
    }


def play_count_reference(
    events: list[InteractionEventV1], user_id: str, observation: datetime
) -> dict[str, float]:
    return {
        "play_count": count_user_plays_1h(events, user_id=user_id, observation_time=observation)
    }


@dataclass(frozen=True)
class FeatureCase:
    spec_file: str
    fixture_file: str
    fixture_lines: int
    users: frozenset[str]
    required_windows: dict[tuple[str, str], dict[str, float]]
    last_window: tuple[str, str]
    min_records: int
    hand_written: Callable[[list[InteractionEventV1], str, datetime], dict[str, float]]


SKIP_RATE = FeatureCase(
    spec_file="user_skip_rate_24h.yaml",
    fixture_file="user_skip_rate_24h_v1.jsonl",
    fixture_lines=10,
    users=frozenset({"user000001", "user000002"}),
    required_windows={
        ("user000001", "2026-01-02T12:00:00Z"): {"play_count": 2, "skip_count": 2},
        ("user000001", "2026-01-02T12:01:00Z"): {"play_count": 1, "skip_count": 3},
        ("user000001", "2026-01-02T18:00:00Z"): {"play_count": 1, "skip_count": 2},
        ("user000001", "2026-01-03T12:00:00Z"): {"play_count": 0, "skip_count": 1},
        ("user000002", "2026-01-02T12:00:00Z"): {"play_count": 0, "skip_count": 1},
    },
    last_window=("user000001", "2026-01-03T12:00:00Z"),
    min_records=1000,
    hand_written=skip_rate_reference,
)

PLAY_COUNT = FeatureCase(
    spec_file="user_play_count_1h.yaml",
    fixture_file="user_play_count_1h_v1.jsonl",
    fixture_lines=8,
    users=frozenset({"user_000001", "user_000002"}),
    required_windows={
        ("user_000001", "2026-01-01T11:00:00Z"): {"play_count": 1},
        ("user_000001", "2026-01-01T11:01:00Z"): {"play_count": 2},
        ("user_000001", "2026-01-01T12:00:00Z"): {"play_count": 3},
        ("user_000001", "2026-01-01T12:01:00Z"): {"play_count": 3},
        ("user_000001", "2026-01-01T12:30:00Z"): {"play_count": 2},
        ("user_000001", "2026-01-01T13:00:00Z"): {"play_count": 1},
        ("user_000002", "2026-01-01T12:00:00Z"): {"play_count": 1},
    },
    last_window=("user_000001", "2026-01-01T13:00:00Z"),
    min_records=100,
    hand_written=play_count_reference,
)


def run_command(*arguments: str) -> str:
    result = subprocess.run(
        arguments,
        check=True,
        capture_output=True,
        text=True,
        timeout=120,
    )
    return result.stdout + result.stderr


def flink_api(path: str) -> dict:
    with urlopen(f"http://127.0.0.1:8081/{path}", timeout=10) as response:
        return json.load(response)


def wait_for_job_terminal(job_id: str, timeout: float = 120.0) -> None:
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = flink_api(f"jobs/{job_id}")
        if last["state"] in {"CANCELED", "FAILED", "FINISHED"}:
            return
        time.sleep(1)
    raise AssertionError(f"Job {job_id} did not reach a terminal state; last={last['state']}")


def job_root_cause(job_id: str) -> str:
    try:
        exceptions = flink_api(f"jobs/{job_id}/exceptions")
    except OSError as error:
        return f"<could not read job exceptions: {error}>"
    entries = exceptions.get("exceptionHistory", {}).get("entries", [])
    root = exceptions.get("root-exception")
    if not root and entries:
        root = entries[0].get("stacktrace")
    lines = (root or "<no exception recorded>").splitlines()
    relevant = [line for line in lines if not line.lstrip().startswith(("at ", "..."))]
    return "\n".join(relevant)[:6000]


def cancel_job(job_id: str) -> None:
    result = subprocess.run(
        (
            "docker",
            "compose",
            "exec",
            "-T",
            "jobmanager",
            "/opt/flink/bin/flink",
            "cancel",
            job_id,
        ),
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    print(result.stdout + result.stderr)
    if flink_api(f"jobs/{job_id}")["state"] == "FAILED":
        print(f"Job {job_id} root cause:\n{job_root_cause(job_id)}")


def fresh_taskmanager(timeout: float = 180.0) -> None:
    previous = {manager["id"] for manager in flink_api("taskmanagers")["taskmanagers"]}
    run_command(
        "docker",
        "compose",
        "up",
        "-d",
        "--force-recreate",
        "--no-deps",
        "taskmanager",
    )
    deadline = time.monotonic() + timeout
    managers = None
    while time.monotonic() < deadline:
        try:
            managers = flink_api("taskmanagers")["taskmanagers"]
        except OSError:
            time.sleep(2)
            continue
        ids = {manager["id"] for manager in managers}
        free = sum(manager["freeSlots"] for manager in managers)
        if ids and not ids & previous and free >= 1:
            return
        time.sleep(2)
    raise AssertionError(f"No fresh TaskManager registered; taskmanagers={managers}")


def create_topic(topic: str) -> None:
    run_command(
        "docker",
        "compose",
        "exec",
        "-T",
        "kafka",
        "/opt/kafka/bin/kafka-topics.sh",
        "--bootstrap-server",
        "kafka:29092",
        "--create",
        "--topic",
        topic,
        "--partitions",
        "1",
        "--replication-factor",
        "1",
        "--config",
        "message.timestamp.type=LogAppendTime",
    )


def delete_topics(topics: list[str]) -> None:
    for topic in topics:
        subprocess.run(
            (
                "docker",
                "compose",
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "kafka:29092",
                "--delete",
                "--topic",
                topic,
            ),
            check=False,
            capture_output=True,
            text=True,
            timeout=60,
        )


@pytest.mark.parametrize(
    "case",
    [
        pytest.param(SKIP_RATE, id="user_skip_rate_24h"),
        pytest.param(PLAY_COUNT, id="user_play_count_1h"),
    ],
)
def test_generic_job_reproduces_golden_fixture(case: FeatureCase) -> None:
    from confluent_kafka import Consumer, Producer, TopicPartition

    spec = load_spec(SPECS / case.spec_file)
    lines = [
        line
        for line in (FIXTURES / case.fixture_file).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    events = [InteractionEventV1.model_validate_json(line) for line in lines]
    assert len(lines) == case.fixture_lines

    ordered = sorted(lines, key=lambda line: json.loads(line)["event_time"])

    fresh_taskmanager()

    namespace = uuid4().hex
    prefix = f"test.spec-job.{namespace}"
    input_topic = f"{prefix}.input"
    output_topic = f"{prefix}.features"
    late_topic = f"{prefix}.late"
    topics = [input_topic, output_topic, late_topic]
    for topic in topics:
        create_topic(topic)

    progress = InteractionEventV1.model_validate(
        {
            "event_id": uuid4(),
            "event_time": datetime(2026, 1, 5, tzinfo=UTC),
            "ingested_at": datetime(2026, 1, 5, tzinfo=UTC),
            "user_id": f"progress_{namespace}",
            "content_id": "content_spec_job_test",
            "event_type": "impression",
            "play_seconds": 0,
            "source": "web",
            "schema_version": "v1",
        }
    )

    producer = Producer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "broker.address.family": "v4",
            "enable.idempotence": True,
            "message.timeout.ms": 30000,
        }
    )
    consumer = Consumer(
        {
            "bootstrap.servers": "127.0.0.1:9092",
            "broker.address.family": "v4",
            "group.id": prefix,
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )

    records: dict[tuple[str, str], dict] = {}
    duplicates: list[tuple[str, str]] = []

    def collect(timeout: float) -> bool:
        message = consumer.poll(timeout)
        if message is None:
            return False
        assert message.error() is None, str(message.error())
        record = json.loads(message.value())
        if record["user_id"] not in case.users:
            return True
        key = (record["user_id"], record["observation_time"])
        if key in records:
            duplicates.append(key)
        records[key] = record
        return True

    job_id = None
    try:
        consumer.assign([TopicPartition(output_topic, 0, 0)])

        submission = run_command(
            "docker",
            "compose",
            "exec",
            "-T",
            "jobmanager",
            "/opt/flink/bin/flink",
            "run",
            "--detached",
            "--parallelism",
            "1",
            "--python",
            "/opt/flink/jobs/feature_from_spec.py",
            "--spec",
            f"{CONTAINER_SPECS}/{case.spec_file}",
            "--input-topic",
            input_topic,
            "--feature-topic",
            output_topic,
            "--late-topic",
            late_topic,
            "--group-id",
            prefix,
            "--job-name",
            prefix,
            "--no-restart",
        )
        print(submission)
        match = re.search(r"JobID\s+([0-9a-fA-F]{32})", submission)
        assert match is not None, submission
        job_id = match.group(1)

        deadline = time.monotonic() + 240
        while True:
            job = flink_api(f"jobs/{job_id}")
            assert job["state"] not in {"FAILED", "CANCELED", "FINISHED"}, (
                f"{job}\nroot cause:\n{job_root_cause(job_id)}"
            )
            vertices = job.get("vertices", [])
            if (
                job["state"] == "RUNNING"
                and vertices
                and all(vertex["status"] == "RUNNING" for vertex in vertices)
            ):
                break
            assert time.monotonic() < deadline, job
            time.sleep(1)

        errors: list[str] = []

        def delivered(error, message):
            if error is not None:
                errors.append(str(error))

        for payload in [*ordered, progress.model_dump_json()]:
            producer.produce(input_topic, value=payload, on_delivery=delivered)
        assert producer.flush(35) == 0, "Producer delivery timed out"
        assert not errors, errors

        deadline = time.monotonic() + 240
        while case.last_window not in records:
            if not collect(1.0):
                state = flink_api(f"jobs/{job_id}")["state"]
                if state in {"FAILED", "CANCELED", "FINISHED"}:
                    raise AssertionError(
                        f"Job {job_id} is {state}; root cause:\n{job_root_cause(job_id)}"
                    )
            assert time.monotonic() < deadline, (
                f"Last window not emitted; {len(records)} records collected"
            )

        quiet_until = time.monotonic() + 5
        while time.monotonic() < quiet_until:
            if collect(0.5):
                quiet_until = time.monotonic() + 5

        assert not duplicates, duplicates[:5]
        assert len(records) > case.min_records, len(records)

        for window, values in case.required_windows.items():
            assert window in records, f"Missing window {window}"
            for name, value in values.items():
                assert records[window][name] == value, records[window]

        for (user_id, observation_text), record in records.items():
            observation = datetime.fromisoformat(observation_text)
            expected = evaluate_spec(
                spec,
                events,
                entity={"user_id": user_id},
                observation_time=observation,
            )
            assert record["feature_schema_version"] == "v1", record
            assert record["feature_name"] == spec.name, record
            assert set(record) == BASE_FIELDS | set(expected.values), record
            for name, value in expected.values.items():
                assert record[name] == pytest.approx(value), record
            for name, value in case.hand_written(events, user_id, observation).items():
                assert record[name] == pytest.approx(value), record
            assert (
                datetime.fromisoformat(record["window_start_exclusive"])
                == expected.window_start_exclusive
            ), record

        low, high = consumer.get_watermark_offsets(
            TopicPartition(late_topic, 0), timeout=10, cached=False
        )
        assert low == high, f"Unexpected late events: offsets {low}..{high}"
        assert flink_api(f"jobs/{job_id}")["state"] == "RUNNING"
    finally:
        consumer.close()
        if job_id is not None:
            cancel_job(job_id)
            wait_for_job_terminal(job_id)
        delete_topics(topics)
