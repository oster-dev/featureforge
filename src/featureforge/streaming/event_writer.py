"""Event-log writer: raw event payloads from Kafka into the event fact table (ADR-015).

consume_batch is the part that needs no Kafka: it validates every payload with
route_event_json and appends the valid events. write_events reads one single-partition
topic and commits the offset of a batch only after the batch was appended, so a crash
leads to a redelivery that the idempotent append absorbs (failure order of ADR-010).
The writer runs in its own consumer group, independent of the Flink job.
"""

from __future__ import annotations

import argparse
import json
import logging
import time
from collections import Counter
from collections.abc import Sequence
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition

from featureforge.streaming.contracts.events import InteractionEventV1
from featureforge.streaming.contracts.routing import route_event_json
from featureforge.streaming.event_log import EventLog
from featureforge.streaming.history import open_catalog
from featureforge.streaming.materialize_spec import commit_offset

LOGGER = logging.getLogger(__name__)


def consume_batch(payloads: Sequence[str | bytes | None], log: EventLog) -> Counter[str]:
    """Append the valid events of a batch and count what happened to every payload.

    Result keys: appended, duplicate, rejected_invalid_json,
    rejected_event_contract_violation, rejected_invalid_encoding, rejected_tombstone.
    A key whose count is zero is absent.
    Rejected payloads are counted and not stored: they are not facts. An identity conflict
    or any unexpected error propagates, and the caller must not commit the batch.
    """
    results: Counter[str] = Counter()
    events: list[InteractionEventV1] = []

    for payload in payloads:
        if payload is None:
            results["rejected_tombstone"] += 1
            continue
        if isinstance(payload, bytes):
            try:
                payload = payload.decode("utf-8")
            except UnicodeDecodeError:
                results["rejected_invalid_encoding"] += 1
                continue

        valid, rejection = route_event_json(payload)
        if valid is not None:
            events.append(InteractionEventV1.model_validate_json(valid))
        elif rejection is not None:
            reason = json.loads(rejection)["reason_code"]
            results[f"rejected_{reason.lower()}"] += 1

    added = log.append(events)
    duplicates = len(events) - added
    if added:
        results["appended"] = added
    if duplicates:
        results["duplicate"] = duplicates
    return results


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Write the validated events of a raw Kafka topic into the event fact table."
    )
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--group-id", required=True)
    parser.add_argument(
        "--events-root",
        type=Path,
        required=True,
        help="Directory of the local Iceberg catalog that holds the event table.",
    )
    parser.add_argument(
        "--events-catalog-url",
        default=None,
        help="SQL catalog URL, for example the PostgreSQL profile. "
        "Omit to use the SQLite file under --events-root.",
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument(
        "--max-batch-seconds",
        type=float,
        default=0.0,
        help="A partial batch is written on an idle poll once its oldest event has "
        "waited this long. 0 writes it at the first idle poll. A larger value writes "
        "fewer, larger files at a low event rate.",
    )
    parser.add_argument("--bounded", action="store_true")
    return parser.parse_args(argv)


def idle_flush_due(first_pending_at: float, now: float, max_batch_seconds: float) -> bool:
    """A partial batch is written on an idle poll once its oldest event waited this long."""
    return now - first_pending_at >= max_batch_seconds


def _flush(
    consumer: Consumer,
    partition: TopicPartition,
    next_offset: int,
    payloads: list[str | bytes | None],
    log: EventLog,
    results: Counter[str],
) -> None:
    """Append the batch, then commit its offset."""
    if not payloads:
        return
    batch = consume_batch(payloads, log)
    results.update(batch)
    LOGGER.info("Batch of %s payloads: %s", len(payloads), dict(sorted(batch.items())))
    payloads.clear()
    commit_offset(consumer, partition, next_offset)


def write_events(arguments: argparse.Namespace) -> Counter[str]:
    """Read the topic from the committed offset and append its events to the table.

    A bounded run stops at the end offset seen at the start, and an empty or fully read
    topic is a run with no events. An unbounded run keeps polling and flushes a partial
    batch when the topic is idle.
    """
    if arguments.batch_size < 1:
        raise ValueError("--batch-size must be positive")
    if arguments.max_batch_seconds < 0:
        raise ValueError("--max-batch-seconds must not be negative")

    topic = arguments.topic
    log = EventLog(open_catalog(Path(arguments.events_root), arguments.events_catalog_url))
    results: Counter[str] = Counter()

    consumer = Consumer(
        {
            "bootstrap.servers": arguments.bootstrap_servers,
            "broker.address.family": "v4",
            "group.id": arguments.group_id,
            "client.id": "featureforge-event-log-writer",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
            "allow.auto.create.topics": False,
        }
    )

    try:
        metadata = consumer.list_topics(topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)

        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Topic is not available: {topic}")

        if len(topic_metadata.partitions) != 1:
            raise RuntimeError("Only single-partition event topics are supported")

        partition_id = next(iter(topic_metadata.partitions))
        partition = TopicPartition(topic, partition_id)

        low, high = consumer.get_watermark_offsets(partition, timeout=10, cached=False)

        committed = consumer.committed([partition], timeout=10)[0]
        if committed.error is not None:
            raise KafkaException(committed.error)

        start = max(committed.offset if committed.offset >= 0 else low, low)

        LOGGER.info(
            "Start: topic=%s group=%s start_offset=%s end_offset=%s bounded=%s",
            topic,
            arguments.group_id,
            start,
            high,
            arguments.bounded,
        )

        if arguments.bounded and start >= high:
            return results

        consumer.assign([TopicPartition(topic, partition_id, start)])

        payloads: list[str | bytes | None] = []
        last_offset: int | None = None
        first_pending_at = 0.0

        while True:
            message = consumer.poll(timeout=1.0)

            if message is None:
                if (
                    payloads
                    and last_offset is not None
                    and idle_flush_due(
                        first_pending_at, time.monotonic(), arguments.max_batch_seconds
                    )
                ):
                    _flush(consumer, partition, last_offset + 1, payloads, log, results)
                continue

            if message.error():
                raise KafkaException(message.error())

            message_offset = message.offset()
            if message_offset is None:
                raise ValueError("Kafka message carries no offset")
            payloads.append(message.value())
            last_offset = message_offset
            if len(payloads) == 1:
                first_pending_at = time.monotonic()

            if len(payloads) >= arguments.batch_size:
                _flush(consumer, partition, last_offset + 1, payloads, log, results)

            if arguments.bounded and last_offset + 1 >= high:
                break

        if last_offset is not None:
            _flush(consumer, partition, last_offset + 1, payloads, log, results)

        return results
    finally:
        consumer.close()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    results = write_events(parse_arguments(argv))
    print(json.dumps({"event_results": dict(sorted(results.items()))}, indent=2))


if __name__ == "__main__":
    main()
