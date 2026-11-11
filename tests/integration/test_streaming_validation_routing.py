"""Verify rejection routing and valid follow-up events through real Flink."""

import json
import subprocess
import time
from collections import Counter
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from confluent_kafka import Consumer, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.contracts.routing import route_event_json

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
COMMAND_TIMEOUT_SECONDS = 180
READ_TIMEOUT_SECONDS = 30


def produce_records(topic: str, payloads: list[str]) -> None:
    errors = []

    def delivered(error, message):
        if error is not None:
            errors.append(str(error))

    producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
    for payload in payloads:
        producer.produce(
            topic,
            partition=0,
            value=payload.encode("utf-8"),
            on_delivery=delivered,
        )

    assert producer.flush(15) == 0, "Kafka producer has pending records"
    assert not errors, f"Kafka delivery failed: {errors}"


def read_completed_topic(topic: str) -> list[str]:
    """Read the complete existing range after the bounded producer job exits."""
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": f"routing-audit-{uuid4().hex}",
            "enable.auto.commit": False,
            "enable.partition.eof": False,
        }
    )

    try:
        partition = TopicPartition(topic, 0)
        low, high = consumer.get_watermark_offsets(
            partition,
            timeout=10,
            cached=False,
        )

        if low == high:
            return []

        consumer.assign([TopicPartition(topic, 0, low)])
        deadline = time.monotonic() + READ_TIMEOUT_SECONDS
        payloads = []
        next_offset = low

        while next_offset < high:
            remaining = deadline - time.monotonic()
            assert remaining > 0, (
                f"Timed out reading {topic}: next_offset={next_offset}, end_offset={high}"
            )

            message = consumer.poll(min(1.0, remaining))
            if message is None:
                continue

            assert message.error() is None, str(message.error())
            assert message.topic() == topic
            assert message.partition() == 0
            assert low <= message.offset() < high

            payload = message.value()
            assert payload is not None, "Unexpected Kafka tombstone"
            payloads.append(payload.decode("utf-8"))
            next_offset = message.offset() + 1

        return payloads
    finally:
        consumer.close()


def canonical_json(payload: str) -> str:
    return json.dumps(
        json.loads(payload),
        sort_keys=True,
        separators=(",", ":"),
    )


def make_event(user_id: str) -> InteractionEventV1:
    timestamp = datetime(2026, 1, 1, 11, 30, tzinfo=UTC)
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


def test_invalid_events_are_rejected_and_valid_followups_are_processed():
    run_id = uuid4().hex
    topics = [
        f"featureforge-stream.integration.routing.input.{run_id}",
        f"featureforge-stream.integration.routing.validated.{run_id}",
        f"featureforge-stream.integration.routing.rejected.{run_id}",
    ]
    input_topic, validated_topic, rejected_topic = topics
    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    first_valid = make_event(f"routing_user_first_{run_id}")
    second_valid = make_event(f"routing_user_second_{run_id}")

    invalid_json = '{"broken":'
    invalid_contract = json.dumps(
        {
            **first_valid.model_dump(mode="json"),
            "event_id": str(uuid4()),
            "event_type": "unsupported",
        }
    )

    payloads = [
        invalid_json,
        first_valid.model_dump_json(),
        invalid_contract,
        second_valid.model_dump_json(),
    ]

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

        produce_records(input_topic, payloads)

        completed = subprocess.run(
            [
                "docker",
                "compose",
                "exec",
                "-T",
                "jobmanager",
                "python",
                "/opt/flink/jobs/validate_and_route_events.py",
                "--bootstrap-servers",
                "kafka:29092",
                "--input-topic",
                input_topic,
                "--validated-topic",
                validated_topic,
                "--rejected-topic",
                rejected_topic,
                "--group-id",
                f"routing-validation-{run_id}",
                "--job-name",
                f"routing-validation-{run_id}",
                "--bounded",
            ],
            check=False,
            text=True,
            capture_output=True,
            timeout=COMMAND_TIMEOUT_SECONDS,
        )
        assert completed.returncode == 0, (
            f"Validation job failed:\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )

        validated = read_completed_topic(validated_topic)
        rejected = read_completed_topic(rejected_topic)

        assert len(validated) == 2, validated
        assert len(rejected) == 2, rejected

        parsed_validated = [
            InteractionEventV1.model_validate_json(payload) for payload in validated
        ]
        assert {event.event_id for event in parsed_validated} == {
            first_valid.event_id,
            second_valid.event_id,
        }
        assert {event.user_id for event in parsed_validated} == {
            first_valid.user_id,
            second_valid.user_id,
        }

        expected_validated = []
        expected_rejected = []

        for payload in payloads:
            valid, rejection = route_event_json(payload)
            if valid is not None:
                expected_validated.append(valid)
            if rejection is not None:
                expected_rejected.append(rejection)

        assert Counter(map(canonical_json, validated)) == Counter(
            map(canonical_json, expected_validated)
        )
        assert Counter(map(canonical_json, rejected)) == Counter(
            map(canonical_json, expected_rejected)
        )

        assert any("INVALID_JSON" in payload for payload in rejected)
        assert any("EVENT_CONTRACT_VIOLATION" in payload for payload in rejected)
    finally:
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
