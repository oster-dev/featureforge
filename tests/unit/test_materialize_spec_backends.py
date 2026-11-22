"""The materializer writes to the online store that the spec names (ADR-013)."""

import argparse
import contextlib
from pathlib import Path
from unittest.mock import Mock

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming import cassandra_connection
from featureforge.streaming import materialize_spec as module
from featureforge.streaming.cassandra_store import CassandraSpecStore

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
CASSANDRA_PLAY = PLAY.model_copy(
    update={"online": PLAY.online.model_copy(update={"store": "cassandra"})}
)


def arguments():
    return argparse.Namespace(
        cassandra_host="cass",
        cassandra_port=9043,
        cassandra_local_dc="dc9",
        keyspace="ff_test",
        key_prefix="p",
    )


@pytest.fixture
def events(monkeypatch):
    log = []
    session = Mock(name="session")

    @contextlib.contextmanager
    def fake_open_session(host, port, *, local_dc):
        log.append(("open", host, port, local_dc))
        try:
            yield session
        finally:
            log.append(("close",))

    monkeypatch.setattr(cassandra_connection, "open_session", fake_open_session)
    monkeypatch.setattr(
        module, "ensure_schema", lambda s, keyspace: log.append(("schema", s, keyspace))
    )
    log.append(("session", session))
    return log


def test_a_cassandra_spec_opens_a_session_ensures_the_schema_and_builds_the_store(events):
    session = events.pop(0)[1]

    with module.open_online_store(arguments(), CASSANDRA_PLAY) as store:
        assert isinstance(store, CassandraSpecStore)
        assert events == [("open", "cass", 9043, "dc9"), ("schema", session, "ff_test")]

    assert events[-1] == ("close",)


def test_the_session_is_closed_when_the_run_fails(events):
    events.pop(0)

    with pytest.raises(RuntimeError, match="boom"):  # noqa: SIM117
        with module.open_online_store(arguments(), CASSANDRA_PLAY):
            raise RuntimeError("boom")

    assert events[-1] == ("close",)


def test_the_cassandra_arguments_have_local_defaults():
    parsed = module.parse_arguments(["--spec", "spec.yaml", "--group-id", "group"])

    assert parsed.cassandra_host == "127.0.0.1"
    assert parsed.cassandra_port == 9042
    assert parsed.cassandra_local_dc == "datacenter1"
    assert parsed.keyspace == "featureforge"
