# Observability of the feature service

Prometheus and Grafana run in the Compose stack next to the feature service.
They cover the serving path only. Alert rules, runbooks and drills do not exist yet.

## Start

```bash
docker compose up -d --build feature-service prometheus grafana
python scripts/smoke_feature_service.py
```

| Component | Address | Notes |
|---|---|---|
| Feature service | http://127.0.0.1:8080 | `/metrics`, `/readyz`, `/healthz` |
| Prometheus v3.16.0 | http://127.0.0.1:9090 | scrapes every 5 s, keeps 7 days |
| Grafana 13.3.1 | http://127.0.0.1:3000 | anonymous viewers, admin password from `GRAFANA_ADMIN_PASSWORD` (default `admin`, local only) |

All ports are bound to 127.0.0.1. The dashboard "FeatureForge feature serving"
and the Prometheus data source are provisioned from files in `docker/grafana`.

## Metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `featureforge_lookup_duration_seconds` | histogram | `backend` | time inside one lookup |
| `featureforge_lookup_entries_total` | counter | `backend`, `status` | entries returned, by `ok`, `missing`, `stale`, `backend_error` |
| `featureforge_lookup_requests_total` | counter | `outcome` | requests by `ok`, `request_error`, `fail_policy` |

## Dashboard

Five panels: whether the target is up, requests per second by outcome, the
lookup duration (p50, p95, p99), entries per second by status and the share of
entries that are not ok. The variable "Target" selects the scrape job.

## Watching a benchmark

`scripts/benchmark_feature_serving.py` starts its own service on the host
(port 18080). Prometheus scrapes it as the job `feature-service-benchmark`.
Select that target in the dashboard before a run. The target is DOWN outside of
a run, which is expected.

## What the latency panel measures

The histogram measures the time inside `FeatureServer.lookup`: the Redis call and
the assembly of the entries. It does not include HTTP parsing, validation of the
request body, the wait for a thread of the pool or the serialization of the
response. The benchmark reports two other numbers. Its service time counts from
the actual send until the response, and its latency counts from the time a
request was due. The three numbers differ by design, and the histogram is the
lowest of them.

The buckets are 0.5, 1, 2.5, 5, 10, 25, 50, 100 and 250 ms. Prometheus
interpolates a quantile inside a bucket, so a p99 between 2.5 and 5 ms is an
estimate.

## Limits

- No alert rules and no runbooks yet.
- The 422 that FastAPI returns for a body that fails validation is not counted,
  because it happens before the handler.
- No metrics of Redis itself.
- The metrics cost about 3 percent of the saturation (see
  [benchmarks.md](benchmarks.md), "Change 3").
- The screenshot was taken during a benchmark run with the stack and a browser
  open, on a laptop. The left curve is an earlier run. Such a run is a
  demonstration and not a measurement: Prometheus, Grafana and the browser
  compete for the same CPU. The tail of the two runs with the stack open was
  worse than in the clean runs at 3000 req/s (p99 6.88 and 20.44 ms against
  5.80 to 9.16 ms, p99.9 21.03 and 115.56 ms against 11.82 to 15.19 ms, see
  [benchmarks.md](benchmarks.md)). Stop the stack before a benchmark that is
  meant to be recorded: `docker compose stop prometheus grafana`.

![Dashboard during a benchmark run](images/feature-serving-dashboard.png)
