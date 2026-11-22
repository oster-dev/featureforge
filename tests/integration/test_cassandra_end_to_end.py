"""Prove that a spec without feature code reaches the service on Cassandra (ADR-013, ADR-016).

The spec is the file of the Redis end-to-end test. Only online.store differs. Events go
through the validation job, the generic Flink job and the generic materializer into
Cassandra, and the service reads them back through CassandraServingReader.
"""

import contextlib
import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient

from featureforge.spec.loader import load_spec
from featureforge.streaming.cassandra_connection import open_session
from tests.integration.test_content_play_count_end_to_end import (
    BOOTSTRAP_SERVERS,
    KAFKA_IN_COMPOSE,
    create_topics,
    play,
    publish,
    run_in_jobmanager,
)

pytestmark = pytest.mark.integration

CASSANDRA_HOST = os.environ.get("FEATUREFORGE_CASSANDRA_HOST", "127.0.0.1")
SPEC_PATH = (
    Path(__file__).resolve().parents[2] / "feature_repo" / "specs" / "content_play_count_1h.yaml"
)
CONTAINER_SPEC = "/opt/flink/specs/content_play_count_1h.yaml"


def run_materializer(spec_file, topic, group_id, keyspace):
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
            "--bounded",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=180,
    )
    assert completed.returncode == 0, (
        f"materializer failed\nstdout={completed.stdout}\nstderr={completed.stderr}"
    )
    return json.loads(completed.stdout)["store_results"]


def free_port():
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


@contextlib.contextmanager
def serving(specs_dir, keyspace, log_path):
    """Run featureforge-serve against Cassandra and wait until it is ready."""
    port = free_port()
    with log_path.open("w") as log:
        process = subprocess.Popen(
            [
                sys.executable,
                "-m",
                "featureforge.service_api",
                "--specs-dir",
                str(specs_dir),
                "--cassandra-host",
                CASSANDRA_HOST,
                "--cassandra-keyspace",
                keyspace,
                "--budget-ms",
                "1000",
                "--port",
                str(port),
                "--quiet",
            ],
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        try:
            deadline = time.monotonic() + 60
            while True:
                assert process.poll() is None, f"the service stopped early:\n{log_path.read_text()}"
                try:
                    url = f"http://127.0.0.1:{port}/readyz"
                    with urllib.request.urlopen(url, timeout=2) as response:
                        if response.status == 200:
                            break
                except OSError:
                    pass
                assert time.monotonic() < deadline, f"not ready:\n{log_path.read_text()}"
                time.sleep(0.5)
            yield port
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()


def post_lookup(port, body):
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/v1/features/lookup",
        data=json.dumps(body).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        return json.load(response)


def lookup_body(spec, entity, reference_time):
    return {
        "features": [{"name": spec.name, "version": spec.version}],
        "entities": [{spec.keys[0]: entity}],
        "reference_time": reference_time.isoformat(),
    }


def test_content_play_count_reaches_the_service_from_events_on_cassandra(tmp_path):
    text = SPEC_PATH.read_text()
    assert text.count("store: redis") == 1, "the spec must name redis exactly once"
    specs_dir = tmp_path / "specs"
    specs_dir.mkdir()
    spec_file = specs_dir / SPEC_PATH.name
    spec_file.write_text(text.replace("store: redis", "store: cassandra"))
    spec = load_spec(spec_file)
    assert spec.online.store == "cassandra"

    run_id = uuid.uuid4().hex
    topics = {
        kind: f"featureforge-stream.integration.cassandra-e2e.{kind}.{run_id}"
        for kind in ("input", "validated", "rejected", "features", "late")
    }
    keyspace = f"ff_e2e_{run_id[:12]}"

    duplicated = play("user_000002", "content_000001", 11, 40)
    events = [
        play("user_000001", "content_000001", 11, 30),
        play("user_000001", "content_000002", 11, 30),
        duplicated,
        duplicated,
    ]

    admin = AdminClient({"bootstrap.servers": BOOTSTRAP_SERVERS})
    with open_session(CASSANDRA_HOST) as session:
        try:
            create_topics(list(topics.values()))
            publish(topics["input"], events)

            run_in_jobmanager(
                "validate_and_route_events.py",
                [
                    "--bootstrap-servers",
                    KAFKA_IN_COMPOSE,
                    "--input-topic",
                    topics["input"],
                    "--validated-topic",
                    topics["validated"],
                    "--rejected-topic",
                    topics["rejected"],
                    "--group-id",
                    f"validation-{run_id}",
                    "--bounded",
                ],
                "validation job",
            )
            run_in_jobmanager(
                "feature_from_spec.py",
                [
                    "--spec",
                    CONTAINER_SPEC,
                    "--bootstrap-servers",
                    KAFKA_IN_COMPOSE,
                    "--input-topic",
                    topics["validated"],
                    "--feature-topic",
                    topics["features"],
                    "--late-topic",
                    topics["late"],
                    "--group-id",
                    f"content-play-count-{run_id}",
                    "--job-name",
                    f"content-play-count-{run_id}",
                    "--bounded",
                ],
                "generic spec-driven job",
            )

            results = run_materializer(spec_file, topics["features"], f"e2e-{run_id}", keyspace)
            assert set(results) <= {"written", "unchanged", "stale"}
            assert sum(results.values()) == 130

            observed = {
                "content_000001": datetime(2026, 1, 1, 12, 39, tzinfo=UTC),
                "content_000002": datetime(2026, 1, 1, 12, 29, tzinfo=UTC),
            }
            answers = {}
            with serving(specs_dir, keyspace, tmp_path / "service.log") as port:
                for entity, moment in observed.items():
                    body = lookup_body(spec, entity, moment + spec.freshness_slo / 2)
                    answers[entity] = post_lookup(port, body)
                body = lookup_body(spec, "content_000003", datetime(2026, 1, 1, 13, tzinfo=UTC))
                answers["content_000003"] = post_lookup(port, body)
                with urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5) as page:
                    metrics_text = page.read().decode("utf-8")

            for entity, moment in observed.items():
                (entry,) = answers[entity]["entries"]
                assert entry["status"] == "ok", answers[entity]
                assert entry["freshness"] == "fresh", answers[entity]
                assert entry["values"]["play_count"] == 1, answers[entity]
                assert datetime.fromisoformat(entry["observation_time"]) == moment, answers[entity]
            (unknown,) = answers["content_000003"]["entries"]
            assert unknown["status"] == "missing", answers["content_000003"]
            assert unknown["values"] is None
            assert 'backend="cassandra"' in metrics_text
            assert 'backend="redis"' not in metrics_text
        finally:
            session.execute(f"DROP KEYSPACE IF EXISTS {keyspace}")
            with contextlib.suppress(Exception):
                for future in admin.delete_topics(
                    list(topics.values()), operation_timeout=15
                ).values():
                    future.result(timeout=30)
