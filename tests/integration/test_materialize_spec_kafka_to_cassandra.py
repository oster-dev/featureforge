"""Verify the generic materializer from Kafka to Cassandra for a spec without feature code.

The spec is the file of the Redis test. Only online.store differs.
"""

import contextlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_expression
from featureforge.streaming.cassandra_connection import open_session
from featureforge.streaming.cassandra_schema import TABLE
from featureforge.streaming.contracts.feature_record import parse_feature_record, stamp
from featureforge.streaming.store_protocol import create_online_store

pytestmark = pytest.mark.integration

BOOTSTRAP_SERVERS = "127.0.0.1:9092"
CASSANDRA_HOST = os.environ.get("FEATUREFORGE_CASSANDRA_HOST", "127.0.0.1")
SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "feature_repo" / "specs" / "content_play_count_1h.yaml"
)
OBSERVATION = datetime(2026, 1, 2, 12, tzinfo=UTC)


def record_json(spec, entity, counts, *, minutes=0):
    observation = OBSERVATION + timedelta(minutes=minutes)
    data = {
        "feature_schema_version": "v1",
        "feature_name": spec.name,
        spec.keys[0]: entity,
        "observation_time": stamp(observation),
        "window_start_exclusive": stamp(observation - spec.window.size),
        **counts,
    }
    for name, expression in spec.derived.items():
        data[name] = evaluate_expression(expression, counts)
    return json.dumps(data, sort_keys=True, separators=(",", ":"))


def run_materializer(spec_file, topic, key_prefix, group_id, keyspace):
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "featureforge.streaming.materialize_spec",
            "--spec",
            str(spec_file),
            "--bootstrap-servers",
            BOOTSTRAP_SERVERS,
            "--cassandra-host",
            CASSANDRA_HOST,
            "--keyspace",
            keyspace,
            "--topic",
            topic,
            "--group-id",
            group_id,
            "--key-prefix",
            key_prefix,
            "--bounded",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode == 0, (
        f"materializer failed\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    return json.loads(completed.stdout)["store_results"]


def test_content_play_count_flows_from_kafka_to_cassandra_without_feature_code(tmp_path):
    text = SPEC_PATH.read_text()
    assert text.count("store: redis") == 1, "the spec must name redis exactly once"
    spec_file = tmp_path / SPEC_PATH.name
    spec_file.write_text(text.replace("store: redis", "store: cassandra"))
    spec = load_spec(spec_file)
    assert spec.online.store == "cassandra"

    run_id = uuid4().hex
    topic = f"featureforge-stream.integration.materialize-spec-cassandra.{run_id}"
    key_prefix = f"v{spec.version}:{spec.name}:{run_id[:8]}"
    keyspace = f"ff_e2e_{run_id[:12]}"
    group_id = f"materialize-spec-cassandra-{run_id}"

    messages = [
        record_json(spec, "content_000001", {"play_count": 1}),
        record_json(spec, "content_000001", {"play_count": 2}),
        record_json(spec, "content_000001", {"play_count": 2}),
        record_json(spec, "content_000002", {"play_count": 7}),
        record_json(spec, "content_000001", {"play_count": 1}, minutes=1),
        record_json(spec, "content_000001", {"play_count": 9}),
    ]

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})

    with open_session(CASSANDRA_HOST) as session:
        try:
            futures = admin.create_topics(
                [
                    NewTopic(
                        topic,
                        num_partitions=1,
                        replication_factor=1,
                        config={"message.timestamp.type": "LogAppendTime"},
                    )
                ],
                request_timeout=15,
                operation_timeout=15,
            )
            for future in futures.values():
                future.result(timeout=30)

            errors = []

            def delivered(error, message):
                if error is not None:
                    errors.append(str(error))

            producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
            for message in messages:
                producer.produce(topic, value=message.encode("utf-8"), on_delivery=delivered)
            assert producer.flush(15) == 0
            assert not errors, errors

            first = run_materializer(spec_file, topic, key_prefix, group_id, keyspace)
            assert first == {"stale": 1, "unchanged": 1, "written": 4}

            store = create_online_store(
                spec, cassandra_session=session, keyspace=keyspace, key_prefix=key_prefix
            )
            assert store.read("content_000001") == parse_feature_record(
                record_json(spec, "content_000001", {"play_count": 1}, minutes=1),
                spec,
            )
            assert store.read("content_000002") == parse_feature_record(
                record_json(spec, "content_000002", {"play_count": 7}),
                spec,
            )
            row = session.execute(
                f"SELECT TTL(payload) FROM {keyspace}.{TABLE} "
                "WHERE entity_key = %s AND entity_value = %s AND feature = %s",
                (spec.keys[0], "content_000001", key_prefix),
            ).one()
            assert 0 < row[0] <= int(spec.online.ttl.total_seconds())

            second = run_materializer(spec_file, topic, key_prefix, group_id, keyspace)
            assert second == {}
        finally:
            session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
            with contextlib.suppress(Exception):
                for future in admin.delete_topics([topic], operation_timeout=15).values():
                    future.result(timeout=30)
