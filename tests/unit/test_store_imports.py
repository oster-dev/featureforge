"""Each store backend loads without the client library of the other (ADR-013)."""

import subprocess
import sys


def run_without(blocked: str, *modules: str) -> None:
    code = f"import sys\nsys.modules['{blocked}'] = None\n" + "".join(
        f"import {module}\n" for module in modules
    )
    subprocess.run([sys.executable, "-c", code], check=True)


def test_the_cassandra_backend_and_the_factory_load_without_the_redis_client():
    run_without(
        "redis",
        "featureforge.streaming.store_protocol",
        "featureforge.streaming.cassandra_store",
    )


def test_the_redis_backend_and_the_factory_load_without_the_cassandra_driver():
    run_without(
        "cassandra",
        "featureforge.streaming.store_protocol",
        "featureforge.streaming.spec_store",
    )
