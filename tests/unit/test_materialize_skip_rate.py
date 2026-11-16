"""The skip-rate materializer is an entry point over the generic one (ADR-009)."""

from featureforge.streaming.materialize_skip_rate import (
    DEFAULT_KEY_PREFIX,
    DEFAULT_TOPIC,
)
from featureforge.streaming.materialize_spec import default_topic
from featureforge.streaming.skip_rate_store import skip_rate_spec


def test_defaults_match_the_spec():
    spec = skip_rate_spec()
    assert DEFAULT_TOPIC == default_topic(spec)
    assert DEFAULT_KEY_PREFIX == f"featureforge-stream:online:v{spec.version}:{spec.name}"
