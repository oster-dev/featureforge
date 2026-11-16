"""The play-count materializer is an entry point over the generic one (ADR-009)."""

import pytest

pytest.importorskip("confluent_kafka")


from featureforge.streaming.materialize import DEFAULT_KEY_PREFIX, DEFAULT_TOPIC
from featureforge.streaming.materialize_spec import default_topic
from featureforge.streaming.online_store import play_count_spec


def test_defaults_match_the_spec():
    spec = play_count_spec()
    assert DEFAULT_TOPIC == default_topic(spec)
    assert DEFAULT_KEY_PREFIX == f"featureforge-stream:online:v{spec.version}:{spec.name}"
