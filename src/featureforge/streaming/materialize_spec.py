"""Materialize the Kafka updates of any spec-driven feature into Redis (ADR-009)."""

import argparse
import json
import logging
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, TopicPartition
from redis import Redis

from featureforge.spec.loader import load_spec
from featureforge.spec.model import FeatureSpec
from featureforge.streaming.contracts.feature_record import parse_feature_record
from featureforge.streaming.history import Evidence, HistoryTable, open_catalog
from featureforge.streaming.spec_store import RedisSpecStore

LOGGER = logging.getLogger(__name__)


def default_topic(spec: FeatureSpec) -> str:
    """Return the feature topic name derived from the spec."""
    return f"features.{spec.name.replace('_', '-')}.v{spec.version}"


def open_history(arguments: argparse.Namespace, spec: FeatureSpec) -> HistoryTable | None:
    """Open the history table when --history-root is set (ADR-011), else record no evidence."""
    root = getattr(arguments, "history_root", None)
    if root is None:
        return None
    catalog_url = getattr(arguments, "history_catalog_url", None)
    return HistoryTable(open_catalog(Path(root), catalog_url), spec)


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize spec-driven feature updates from Kafka into Redis."
    )
    parser.add_argument("--spec", type=Path, required=True)
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--key-prefix", default=None)
    parser.add_argument("--commit-every", type=int, default=1000)
    parser.add_argument("--bounded", action="store_true")
    parser.add_argument(
        "--history-root",
        type=Path,
        default=None,
        help="Directory of the local Iceberg history catalog. Omit to record no evidence.",
    )
    parser.add_argument(
        "--history-catalog-url",
        default=None,
        help="SQL catalog URL of the history, for example the PostgreSQL profile. "
        "Omit to use the SQLite file under --history-root.",
    )
    return parser.parse_args(argv)


def commit_offset(
    consumer: Consumer,
    partition: TopicPartition,
    next_offset: int,
) -> None:
    result = consumer.commit(
        offsets=[
            TopicPartition(
                partition.topic,
                partition.partition,
                next_offset,
            )
        ],
        asynchronous=False,
    )

    for committed in result or []:
        if committed.error is not None:
            raise KafkaException(committed.error)


def settle(
    consumer: Consumer,
    partition: TopicPartition,
    next_offset: int,
    history: HistoryTable | None,
    pending: list[Evidence],
) -> None:
    """Append the pending evidence, then commit the offset (failure order of ADR-010)."""
    if history is not None and pending:
        added = history.append(pending)
        LOGGER.info("History: %s of %s evidence rows added", added, len(pending))
        pending.clear()
    commit_offset(consumer, partition, next_offset)


def materialize(arguments: argparse.Namespace, spec: FeatureSpec) -> Counter:
    if arguments.commit_every < 1:
        raise ValueError("--commit-every must be positive")

    topic = arguments.topic or default_topic(spec)
    results: Counter = Counter()

    with Redis.from_url(
        arguments.redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    ) as redis_client:
        if redis_client.ping() is not True:
            raise RuntimeError("Redis connectivity check failed")

        store = RedisSpecStore(
            redis_client,
            spec,
            key_prefix=arguments.key_prefix,
        )
        history = open_history(arguments, spec)
        pending: list[Evidence] = []

        consumer = Consumer(
            {
                "bootstrap.servers": arguments.bootstrap_servers,
                "broker.address.family": "v4",
                "group.id": arguments.group_id,
                "client.id": f"featureforge-stream-redis-materializer-{spec.name}",
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
                raise RuntimeError("Only single-partition feature topics are supported")

            partition_id = next(iter(topic_metadata.partitions))
            partition = TopicPartition(topic, partition_id)

            low, high = consumer.get_watermark_offsets(
                partition,
                timeout=10,
                cached=False,
            )

            if low == high:
                raise RuntimeError(
                    f"No readable records in topic {topic!r}: "
                    f"earliest_available_offset={low}, end_offset={high}. "
                    "Refusing to materialize an empty Kafka range."
                )

            committed = consumer.committed([partition], timeout=10)[0]
            if committed.error is not None:
                raise KafkaException(committed.error)

            start = committed.offset if committed.offset >= 0 else low
            start = max(start, low)

            LOGGER.info(
                "Start: spec=%s topic=%s group=%s start_offset=%s end_offset=%s bounded=%s",
                spec.name,
                topic,
                arguments.group_id,
                start,
                high,
                arguments.bounded,
            )

            if arguments.bounded and start >= high:
                LOGGER.info(
                    "No pending records for group=%s at end_offset=%s",
                    arguments.group_id,
                    high,
                )
                return results

            consumer.assign(
                [
                    TopicPartition(
                        topic,
                        partition_id,
                        start,
                    )
                ]
            )

            since_commit = 0
            last_offset = None

            while True:
                message = consumer.poll(timeout=1.0)

                if message is None:
                    continue

                if message.error():
                    raise KafkaException(message.error())

                try:
                    payload = message.value()
                    if payload is None:
                        raise ValueError("Feature tombstones are not supported")

                    record = parse_feature_record(payload, spec)
                    outcome = store.write(record)
                    results[outcome] += 1
                    if history is not None and outcome != "stale":
                        pending.append(Evidence(record=record, published_at=datetime.now(UTC)))
                except Exception:
                    LOGGER.exception(
                        "Materialization failed at partition=%s offset=%s",
                        message.partition(),
                        message.offset(),
                    )
                    raise

                message_offset = message.offset()
                if message_offset is None:
                    raise ValueError("Kafka message carries no offset")
                last_offset = message_offset
                since_commit += 1

                if since_commit >= arguments.commit_every:
                    settle(consumer, partition, last_offset + 1, history, pending)
                    since_commit = 0

                if arguments.bounded and last_offset + 1 >= high:
                    break

            if since_commit and last_offset is not None:
                settle(consumer, partition, last_offset + 1, history, pending)

            return results
        finally:
            consumer.close()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    arguments = parse_arguments(argv)
    spec = load_spec(arguments.spec)
    results = materialize(arguments, spec)
    print(
        json.dumps(
            {"store_results": dict(sorted(results.items()))},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
