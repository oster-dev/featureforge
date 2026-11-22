"""Open a Cassandra session with an explicit load-balancing policy (ADR-013)."""

import contextlib
from collections.abc import Iterator
from typing import Any


@contextlib.contextmanager
def open_session(
    host: str,
    port: int = 9042,
    *,
    local_dc: str = "datacenter1",
    connect_timeout: float = 10.0,
) -> Iterator[Any]:
    """Yield a session and shut the cluster down afterwards. The caller owns nothing else."""
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile
    from cassandra.policies import DCAwareRoundRobinPolicy

    profile = ExecutionProfile(load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=local_dc))
    cluster = Cluster(
        [host],
        port=port,
        connect_timeout=connect_timeout,
        execution_profiles={EXEC_PROFILE_DEFAULT: profile},
    )
    try:
        yield cluster.connect()
    finally:
        cluster.shutdown()
