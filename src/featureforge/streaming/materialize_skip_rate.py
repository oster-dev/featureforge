"""Materialize user_skip_rate_24h Kafka updates into Redis.

A thin entry point over materialize_spec (ADR-009, decision 10). The command line
and materialize() keep their shape. The consumer loop, the record validation and
the store come from the generic code and the spec.
"""

import argparse
import json
import logging
from collections import Counter

from featureforge.streaming import materialize_spec
from featureforge.streaming.skip_rate_store import skip_rate_spec

DEFAULT_TOPIC = "features.user-skip-rate-24h.v1"
DEFAULT_KEY_PREFIX = "featureforge-stream:online:v1:user_skip_rate_24h"


def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Materialize user_skip_rate_24h Kafka updates into Redis."
    )
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--redis-url", default="redis://127.0.0.1:6379/0")
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--group-id", required=True)
    parser.add_argument("--key-prefix", default=DEFAULT_KEY_PREFIX)
    parser.add_argument("--commit-every", type=int, default=1000)
    parser.add_argument("--bounded", action="store_true")
    return parser.parse_args()


def materialize(arguments: argparse.Namespace) -> Counter:
    return materialize_spec.materialize(arguments, skip_rate_spec())


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    results = materialize(parse_arguments())
    print(
        json.dumps(
            {"store_results": dict(sorted(results.items()))},
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
