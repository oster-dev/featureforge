"""Prometheus metrics of the feature serving service (ADR-016)."""

from __future__ import annotations

import asyncio
from collections import Counter
from collections.abc import Sequence

from prometheus_client import CollectorRegistry, Histogram, generate_latest
from prometheus_client import Counter as PrometheusCounter

from featureforge.feature_service import FeatureEntry

STATUSES = ("ok", "missing", "stale", "backend_error")
OUTCOMES = ("ok", "request_error", "fail_policy")
DURATION_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25)
LAG_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0)
LAG_INTERVAL = 0.02


class ServiceMetrics:
    """Metrics of one app, in a registry of its own.

    The labelled children are bound once, so a lookup only increments plain counters.
    """

    def __init__(self, backend: str = "redis") -> None:
        self.registry = CollectorRegistry()
        duration = Histogram(
            "featureforge_lookup_duration_seconds",
            "Time the service spent on one lookup, measured inside the lookup.",
            ["backend"],
            buckets=DURATION_BUCKETS,
            registry=self.registry,
        )
        entries = PrometheusCounter(
            "featureforge_lookup_entries_total",
            "Entries returned by lookups, by status.",
            ["backend", "status"],
            registry=self.registry,
        )
        requests = PrometheusCounter(
            "featureforge_lookup_requests_total",
            "Lookup requests, by outcome.",
            ["outcome"],
            registry=self.registry,
        )
        self._duration = duration.labels(backend)
        self._entries = {status: entries.labels(backend, status) for status in STATUSES}
        self._requests = {outcome: requests.labels(outcome) for outcome in OUTCOMES}
        self._loop_lag = Histogram(
            "featureforge_event_loop_lag_seconds",
            "How late the event loop woke a task that sleeps for a fixed interval.",
            buckets=LAG_BUCKETS,
            registry=self.registry,
        )

    def record_lookup(self, entries: Sequence[FeatureEntry], elapsed_ms: float) -> None:
        self._duration.observe(elapsed_ms / 1000)
        for status, count in Counter(entry.status for entry in entries).items():
            self._entries[status].inc(count)
        self._requests["ok"].inc()

    def record_rejected(self, outcome: str) -> None:
        self._requests[outcome].inc()

    def record_loop_lag(self, seconds: float) -> None:
        self._loop_lag.observe(seconds)

    def render(self) -> bytes:
        return generate_latest(self.registry)


async def watch_event_loop_lag(metrics: ServiceMetrics, interval: float = LAG_INTERVAL) -> None:
    """Record how late the event loop wakes this task.

    The task sleeps for a fixed interval and compares the time at which it was woken with the time
    it asked for. Everything else that the loop does in between shows up as lag.
    """
    loop = asyncio.get_running_loop()
    while True:
        expected = loop.time() + interval
        await asyncio.sleep(interval)
        metrics.record_loop_lag(max(0.0, loop.time() - expected))
