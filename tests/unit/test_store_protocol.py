"""The store factory picks the backend of the spec (ADR-013)."""

from pathlib import Path
from unittest.mock import Mock

import pytest

from featureforge.spec.loader import load_spec
from featureforge.streaming.cassandra_store import CassandraSpecStore
from featureforge.streaming.spec_store import RedisSpecStore
from featureforge.streaming.store_protocol import OnlineStore, create_online_store

SPECS = Path(__file__).resolve().parents[2] / "feature_repo" / "specs"
PLAY = load_spec(SPECS / "user_play_count_1h.yaml")
CASSANDRA_PLAY = PLAY.model_copy(
    update={"online": PLAY.online.model_copy(update={"store": "cassandra"})}
)


def test_a_redis_spec_gets_the_redis_store_with_its_prefix() -> None:
    store = create_online_store(PLAY, redis_client=Mock(), key_prefix="p")

    assert isinstance(store, RedisSpecStore)
    assert store.key_for("user_000001") == "p:user_000001"


def test_the_redis_store_satisfies_the_protocol() -> None:
    store: OnlineStore = RedisSpecStore(Mock(), PLAY)

    assert callable(store.read)
    assert callable(store.write)


def test_a_redis_store_needs_a_client() -> None:
    with pytest.raises(ValueError, match="redis client"):
        create_online_store(PLAY)


def test_a_cassandra_store_needs_a_session() -> None:
    with pytest.raises(ValueError, match="cassandra session"):
        create_online_store(CASSANDRA_PLAY, redis_client=Mock())


def test_a_cassandra_spec_gets_the_cassandra_store() -> None:
    store = create_online_store(
        CASSANDRA_PLAY, cassandra_session=Mock(), keyspace="ff_test", key_prefix="p"
    )

    assert isinstance(store, CassandraSpecStore)
