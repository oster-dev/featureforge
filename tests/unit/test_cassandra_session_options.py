"""The Cassandra session options of the serving service (ADR-016)."""

import contextlib
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import cassandra.cluster
import pytest
import uvicorn
from cassandra.cluster import EXEC_PROFILE_DEFAULT
from cassandra.query import named_tuple_factory, tuple_factory

from featureforge import service_api
from featureforge.streaming import cassandra_connection
from featureforge.streaming.cassandra_connection import open_session

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
created: list[Any] = []


class FakeCluster:
    def __init__(self, hosts: list[str], **options: Any) -> None:
        self.hosts = hosts
        self.options = options
        self.shut_down = False
        created.append(self)

    def connect(self) -> str:
        return "session"

    def shutdown(self) -> None:
        self.shut_down = True


@pytest.fixture(autouse=True)
def fake_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    created.clear()
    monkeypatch.setattr(cassandra.cluster, "Cluster", FakeCluster)


def default_profile() -> Any:
    return created[0].options["execution_profiles"][EXEC_PROFILE_DEFAULT]


def test_rows_are_named_tuples_unless_a_row_factory_is_given():
    with open_session("127.0.0.1") as session:
        assert session == "session"

    assert default_profile().row_factory is named_tuple_factory
    assert created[0].shut_down


def test_a_row_factory_goes_to_the_default_execution_profile():
    with open_session("127.0.0.1", row_factory=tuple_factory):
        pass

    assert default_profile().row_factory is tuple_factory
    assert created[0].shut_down


def test_the_service_opens_its_cassandra_session_with_plain_tuples(tmp_path, monkeypatch):
    spec = (SPECS / "user_play_count_1h.yaml").read_text()
    (tmp_path / "user_play_count_1h.yaml").write_text(
        spec.replace("store: redis", "store: cassandra")
    )
    opened: dict[str, Any] = {}

    @contextlib.contextmanager
    def fake_open_session(host, port, *, local_dc, row_factory=None):
        opened["row_factory"] = row_factory
        yield Mock()

    monkeypatch.setattr(cassandra_connection, "open_session", fake_open_session)
    monkeypatch.setattr(service_api, "build_cassandra_server", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(service_api, "create_app", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(service_api, "ServiceMetrics", lambda *args, **kwargs: Mock())
    monkeypatch.setattr(uvicorn, "run", lambda *args, **kwargs: None)

    service_api.main(["--specs-dir", str(tmp_path)])

    assert opened["row_factory"] is tuple_factory
