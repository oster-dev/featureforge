# Feature serving benchmarks

Status: first baseline. Numbers describe one laptop, one service process and
the commit named below. They are measurements, not guarantees.

## Setup

| Item | Value |
|---|---|
| Machine | Apple M4 Pro, 14 logical CPUs, 24 GB, macOS 26.6.2 |
| Python | 3.13.9 |
| Packages | fastapi 0.141.1, uvicorn 0.53.0, starlette 1.6.0, redis 5.3.1, pydantic 2.13.5, hiredis 3.4.1, uvloop 0.22.1, httptools 0.8.0 |
| Redis | 7.4.12 in Docker Desktop, AOF enabled |
| Commit | 26c9a8fc746266041b889070f33c2cb1dc312201, clean tree |
| Service | one uvicorn process, access log off, `--budget-ms 50` |
| Data | 1,000,000 users, 2 features each, 2,000,001 keys (one is the seed marker) |
| Request | 2 features for 5 users, 10 entries, Zipf-like access (exponent about 1) |
| Method | open load, 4 client processes, 10 s warm-up, 30 s per run, 3 runs per rate |

Latency counts from the time a request was due, so a queue shows up in the
numbers. Service time counts from the actual send. The machine was on AC power
and kept awake with `caffeinate -dimsu`.

## Results

Main run: `docs/benchmarks/feature-serving-20261120T233907Z.json`. Ranges cover
the three runs per rate.

| Target req/s | Achieved req/s | p50 ms | p99 ms |
|---|---|---|---|
| 100 | 100 | 8.45 to 8.93 | 17.5 to 26.0 |
| 500 | 500 | 3.18 to 3.61 | 12.8 to 19.5 |
| 1000 | 1000 | 1.55 to 1.66 | 10.5 to 14.8 |
| 2000 | 2000 | 1.12 to 1.38 | 9.6 to 13.0 |
| 3000 | 2999 | 2.60 to 3.34 | 19.1 to 28.8 |
| 4000 | 3250 to 3256 | 3.5 s | 6.8 s |
| 6000 | 2816 to 3264 | 13 to 16 s | 24.9 to 33.6 s |

Findings:

- From 500 to 2000 req/s the p99 stayed below 20 ms in every run.
- The process saturates at about 3,250 req/s. Above that the queue grows and
  the latency from the due time reaches seconds, while the service time stays
  near 36 ms (p50) and 55 ms (p99) at about 1.2 CPU cores.
- Errors: 0 in all runs. Entries that are not ok: 40 (all `backend_error`) in
  the 100 req/s run 2, from 4 requests whose Redis call hit the 50 ms socket
  timeout.

## Second run

`docs/benchmarks/feature-serving-20261120T231933Z.json`, same commit, taken
before the main run. At 100 req/s the p50 was 4.7 to 5.1 ms, against 8.5 to
8.9 ms in the main run, and at 4000 req/s run 3 had 80 `backend_error` entries.
Both runs show the same saturation point. The spread at low load shows how much
this machine varies between runs.

## Change 1: parse each spec expression once

Commit 4b1d4cc against main at b4f0306. Both runs on a clean tree, same
machine, same method, 3 runs per rate. Raw data:
`docs/benchmarks/opt1-before-b4f0306.json` and
`docs/benchmarks/opt1-after-4b1d4cc.json`.

A profile of the lookup (cProfile with a CPU timer, 3,000 requests) showed that
every read of a derived value parsed the expression again (`builtins.compile`,
5 calls per request) and that the text check walked each entity value
character by character. The change caches the parsed tree per expression and
replaces the character loop with one regular expression. The oracle `_eval` is
unchanged. A parity test compares cached and fresh evaluation on every spec
expression and on synthetic ones, and compares the new text check with
`str.isspace` for every Unicode code point.

| Target req/s | Version | Achieved req/s | p50 ms | p99 ms | Service p50 ms | Service p99 ms | Server CPU cores |
|---|---|---|---|---|---|---|---|
| 2000 | before | 2000 | 1.04 to 1.39 | 8.61 to 12.29 | 0.89 to 1.27 | 8.04 to 11.67 | 0.64 to 0.71 |
| 2000 | after | 2000 | 1.07 to 1.15 | 8.16 to 10.67 | 0.92 to 1.01 | 7.80 to 10.41 | 0.60 to 0.62 |
| 3000 | before | 2999 | 2.80 to 3.27 | 15.61 to 58.46 | 2.67 to 3.13 | 15.38 to 38.87 | 1.14 to 1.15 |
| 3000 | after | 2999 | 1.13 to 1.81 | 6.73 to 11.43 | 1.04 to 1.69 | 6.34 to 10.55 | 0.90 to 0.95 |
| 4000 | before | 3303 to 3320 | overload | overload | 35.54 to 35.71 | 54.57 to 55.15 | 1.20 to 1.21 |
| 4000 | after | 3793 to 3809 | overload | overload | 30.81 to 30.98 | 48.58 to 49.15 | 1.24 |

Findings:

- Saturation moved from 3,303 to 3,320 req/s to 3,793 to 3,809 req/s, about
  15 percent. The service time at saturation fell from about 35.6 to about
  30.9 ms (p50).
- At 3000 req/s the p99 fell from 15.6 to 58.5 ms to 6.7 to 11.4 ms. The
  service used about 0.92 instead of 1.15 cores, so about 0.31 instead of
  0.38 ms of CPU per request.
- At 2000 req/s the p99 ranges overlap. No improvement is claimed there.
- At 4000 req/s the latency from the due time measures the backlog, which
  depends on offered load minus capacity. Compare the service time instead.
- The CPU per request of the stand-alone profiling script varied between 123
  and 211 us across runs of the same or nearly the same code, so it is not used
  for this comparison.

## Change 2: compile spec expressions once, decode records per spec

Commit a91f732 against main at ab6b3e5. Both runs on a clean tree, same machine,
same method, 3 runs per rate. Raw data:
`docs/benchmarks/opt2-before-ab6b3e5.json`,
`docs/benchmarks/opt2-after-a91f732.json` and
`docs/benchmarks/opt2-after-high-a91f732.json` (higher rates, after only).

After change 1 the profile of the lookup showed two more costs on every read.
The derived value of a record was checked by walking the expression tree
(`_eval`, about 1.1 million `isinstance` calls per 3,000 requests), and the
field layout of the spec was recomputed (`record_fields`). The change compiles
each expression once into a function (`spec/compiled_expression.py`) and builds
a `RecordDecoder` once per spec. `_eval`, the oracle for Flink and Spark, is
unchanged. `parse_feature_record` keeps its signature. Parity tests compare the
compiled evaluator with the oracle on every spec expression and on 3,000 random
expressions, errors included, and compare the decoder with a copy of the
previous parser on several thousand mutated payloads per spec.

In the profile (cProfile with a CPU timer, 3,000 requests) the function calls
fell from 3.26 to 2.33 million and the `isinstance` calls from 1.14 million to
447,000.

| Target req/s | Version | Achieved req/s | p50 ms | p99 ms | Service p50 ms | Service p99 ms | Server CPU cores |
|---|---|---|---|---|---|---|---|
| 2000 | before | 2000 | 0.94 to 1.40 | 5.84 to 12.26 | 0.79 to 1.28 | 5.40 to 11.94 | 0.58 to 0.66 |
| 2000 | after | 2000 | 0.85 to 1.21 | 2.74 to 10.37 | 0.67 to 1.07 | 2.53 to 10.04 | 0.53 to 0.63 |
| 3000 | before | 2999 | 1.13 to 1.25 | 6.55 to 9.62 | 1.04 to 1.16 | 6.17 to 9.25 | 0.91 to 0.94 |
| 3000 | after | 2999 | 1.02 to 1.11 | 4.76 to 5.33 | 0.92 to 1.02 | 4.27 to 4.78 | 0.84 to 0.86 |
| 4000 | before | 3802 to 3828 | overload | overload | 30.72 to 30.86 | 48.38 to 48.58 | 1.24 |
| 4000 | after | 3997 to 3998 | 23.47 to 26.73 | 41.50 to 97.81 | 23.32 to 26.55 | 37.98 to 40.47 | 1.26 |

Higher rates after the change (one process):

| Target req/s | Achieved req/s | Service p50 ms | Service p99 ms | Server CPU cores |
|---|---|---|---|---|
| 4000 | 3538 to 3996 | 28.00 to 37.85 | 41.18 to 44.20 | 1.26 |
| 4500 | 4092 to 4105 | 28.62 to 28.72 | 45.66 to 45.80 | 1.26 |
| 5000 | 4074 to 4080 | 28.85 to 28.87 | 45.86 to 46.20 | 1.26 |
| 6000 | 4094 to 4112 | 28.66 to 28.77 | 45.52 to 45.80 | 1.26 |

Findings:

- One process saturates at about 4,090 req/s (4,074 to 4,112 in the nine runs at
  4500 to 6000), up from about 3,815 before this change and from about 3,300
  before change 1, so about 24 percent in total.
- 4000 req/s is at the edge. In five of six runs the service kept up (3,995 to
  3,998). In one run it fell behind (3,538 req/s, service p50 37.9 ms). The cause
  is not known.
- At 3000 req/s the p99 fell from 6.6 to 9.6 ms to 4.8 to 5.3 ms. The service
  used about 0.85 instead of 0.92 cores, so about 0.28 instead of 0.31 ms of CPU
  per request. In saturation it is about 0.31 instead of 0.33 ms.
- At 2000 req/s the p99 ranges overlap. No improvement is claimed there.
- The client used 0.32 to 0.36 cores, so it is not the limit.

## Change 3: metrics endpoint

Commit 9e4220b against main at a9dc5bc. Both runs on a clean tree, same machine,
same method, 3 runs per rate. Raw data:
`docs/benchmarks/metrics-before-a9dc5bc.json` and
`docs/benchmarks/metrics-after-9e4220b.json`.

The service now exports `/metrics`: a latency histogram per backend, a counter
per entry status and a counter per request outcome. The counters are bound once
and a lookup increments at most four of them. This change adds work to the hot
path, so the benchmark measures what it costs. The rule was fixed before the
run: acceptable if the saturation falls by at most 5 percent, and runs with a
service p50 above 35 ms are excluded as machine slowdowns.

| Target req/s | Version | Achieved req/s | p50 ms | p99 ms | Service p50 ms | Service p99 ms | Server CPU cores |
|---|---|---|---|---|---|---|---|
| 2000 | before | 2000 | 0.87 to 1.14 | 2.83 to 10.64 | 0.70 to 1.00 | 2.62 to 9.98 | 0.53 to 0.65 |
| 2000 | after | 2000 | 0.89 to 1.34 | 6.66 to 10.05 | 0.71 to 1.21 | 6.21 to 9.59 | 0.52 to 0.64 |
| 3000 | before | 2999 | 0.94 to 1.00 | 5.96 to 6.34 | 0.85 to 0.90 | 5.48 to 5.76 | 0.84 to 0.87 |
| 3000 | after | 2999 | 0.91 to 1.16 | 5.80 to 9.16 | 0.81 to 1.07 | 5.40 to 8.69 | 0.85 to 0.88 |
| 4000 | before | 3998 | 13.90 to 15.49 | 39.44 to 68.72 | 13.74 to 15.33 | 36.46 to 38.97 | 1.26 |
| 4000 | after | 3374 to 3997 | overload | overload | 29.07 to 39.27 | 43.24 to 47.77 | 1.25 to 1.26 |
| 4500 | before | 4134 to 4160 | overload | overload | 28.16 to 28.37 | 44.93 to 45.57 | 1.27 |
| 4500 | after | 4014 to 4033 | overload | overload | 29.08 to 29.25 | 46.65 to 46.87 | 1.26 |
| 5000 | before | 3457 to 4129 | overload | overload | 28.46 to 38.29 | 45.28 to 46.24 | 1.26 to 1.27 |
| 5000 | after | 4004 to 4020 | overload | overload | 29.26 to 29.31 | 46.62 to 47.05 | 1.25 to 1.26 |

Findings:

- Saturation of one process: about 4,140 req/s before (five runs at 4500 and
  5000; the run at 5000/2 with 3,457 req/s and a service p50 of 38.3 ms is
  excluded by the rule above) and about 4,020 req/s after (six runs, 4,004 to
  4,033). That is about 3 percent, inside the limit of 5 percent. The noise
  between two measurement days is 1 to 2 percent.
- At 2000 and 3000 req/s the CPU per request is the same within the noise
  (about 0.29 ms).
- 4000 req/s is now at the knee. One run kept up (3,997 req/s), one reached
  3,882 req/s with a p99 of 852 ms, one fell to 3,374 req/s (service p50
  39.3 ms). Before, all three runs kept up.
- The service runs here as a host process. The container is not measured.

## Limits

- Client and service share one machine.
- Redis runs in Docker Desktop, so its latency includes a VM.
- The 50 ms budget is a socket timeout per Redis operation, not an end-to-end
  deadline.
- The cause of the slow Redis calls is not known.
- Three of 36 runs at 4000 req/s or more since change 2 fell to about 85 percent of the
  capacity with a service p50 near 38 ms (3,538, 3,457 and 3,374 req/s). The cause is
  not known.
- One service process only. No claim for several processes or hosts.
- The low-load latency varies between runs. Do not read the 100 req/s p50 as a
  property of the service.
- An earlier run with uncommitted tool code was discarded. A later run was
  repeated after the display went to sleep.

## SLO target

Target, not commitment: p99 of at most 25 ms from 500 to 2000 req/s per
process on this setup. It held in every run above.

## Repeat

```bash
docker compose up -d redis
caffeinate -dimsu python scripts/benchmark_feature_serving.py
```

The script writes a JSON file to `output/benchmarks/`.

## Change 4: ServingReader (ADR-016)

Question: does moving the Redis read behind a `ServingReader` slow the service down?

Method: `scripts/benchmark_feature_serving.py --rates 2000,3000,4000,4500,5000 --repeats 3`,
one uvicorn process, Redis in Docker on the same laptop, 1,000,000 users. Before: commit
`3746261`. After: commit `54ae65f`. Raw data is in `docs/benchmarks/`.

| Target req/s | Achieved before | Achieved after | p99 ms before | p99 ms after | Server cores before | Server cores after |
|---|---|---|---|---|---|---|
| 2000 | 2000 | 2000 | 4.8 | 6.7 | 0.56 | 0.59 |
| 3000 | 2999 | 2999 | 10.0 | 7.9 | 0.91 | 0.86 |
| 4000 | 3969 | 3989 | 332 | 104 | 1.25 | 1.25 |
| 4500 | 4030 | 3952 | overload | overload | 1.26 | 1.25 |
| 5000 | 4033 | 4023 | overload | overload | 1.26 | 1.25 |

Values are means of three runs. Saturation of one process: 4,032 req/s before, 3,988 after
(-1.1 %; -0.5 % without one slow run). The acceptance limit was -3 %. CPU per request and
latency below saturation do not differ beyond run-to-run noise.

Limits: one laptop, three runs per rate, differences below about 1 % cannot be resolved.
An earlier after-run (`change4-after-54ae65f-first-run.json`) had one 2000 req/s run with
1850 not-ok entries and a p99 of 65 ms. The cause is not known, it did not repeat, and the
run is excluded from the numbers above.

## Cassandra against Redis (ADR-016)

Question: how does the service behave when the `ServingReader` reads from Cassandra instead of
Redis, on the same laptop and under the same load?

Method: `scripts/benchmark_feature_serving.py --backend redis|cassandra`, one uvicorn process
with `--budget-ms 50`, commit `5ae169a` with a clean tree, 1,000,000 users with 2 features
each, open load from 4 client processes, 10 s warm-up, 30 s per run, 3 runs per rate. Both
backends ran one after the other in the same session. Prometheus, Grafana, Kafka, Flink and the
Compose feature service were stopped, and the machine was kept awake with `caffeinate -i`.

- Redis: 7.4.12 in Docker Desktop, AOF enabled, one `MGET` per request, 2,000,001 keys,
  seeded in 8 s.
- Cassandra: 5.0.9 in Docker Desktop, one node, keyspace `ff_loadtest`, 2,000,000 rows plus one
  seed marker, driver cassandra-driver 3.30.1, one partition read per entity (5 per request,
  run concurrently) at LOCAL_QUORUM. Seeded in 147 s, followed by `nodetool flush` and a wait
  until no compaction was pending.

Raw data: `docs/benchmarks/adr016-redis-5ae169a.json`,
`docs/benchmarks/adr016-cassandra-5ae169a.json` and the container CPU samples
`docs/benchmarks/adr016-cassandra-container-cpu-5ae169a.txt` (one line every 10 s).
Ranges cover the three runs per rate.

Redis:

| Target req/s | Achieved req/s | p50 ms | p99 ms | Server cores |
|---|---|---|---|---|
| 500 | 500 | 2.56 to 3.89 | 6.54 to 7.98 | 0.37 to 0.39 |
| 1000 | 1000 | 1.66 to 1.97 | 3.86 to 7.49 | 0.46 to 0.51 |
| 1500 | 1500 | 1.21 to 1.34 | 4.46 to 7.47 | 0.52 to 0.56 |
| 2000 | 1999 to 2000 | 1.08 to 1.36 | 5.79 to 11.77 | 0.57 to 0.67 |
| 3000 | 2999 | 1.03 to 1.10 | 8.64 to 9.46 | 0.89 to 0.91 |
| 4000 | 3995 to 3997 | 37.0 to 38.9 | 59.2 to 133.4 | 1.25 |

Cassandra:

| Target req/s | Achieved req/s | p50 | p99 | Server cores |
|---|---|---|---|---|
| 500 | 500 | 3.88 to 4.49 ms | 11.96 to 15.71 ms | 0.48 to 0.51 |
| 1000 | 1000 | 2.96 to 3.78 ms | 17.48 to 19.36 ms | 0.69 to 0.72 |
| 1250 | 1250 | 4.45 to 4.80 ms | 24.14 to 26.76 ms | 0.80 to 0.83 |
| 1500 | 1499 to 1500 | 8.01 to 9.60 ms | 40.19 to 46.61 ms | 0.91 to 0.99 |
| 1750 | 1664 to 1674 | 0.67 to 0.92 s | 1.43 to 1.58 s | 0.98 |
| 2000 | 1662 to 1667 | 3.1 to 3.2 s | 5.96 to 6.06 s | 0.98 |

Findings:

- Redis held 3,000 req/s with a p99 below 10 ms in every run. At 4,000 req/s it reached
  3,995 to 3,997 req/s with a queue (service p50 29 ms, 1.25 cores), in line with the
  saturation of about 4,000 req/s in change 4.
- The service with Cassandra held 1,500 req/s and saturated at 1,662 to 1,674 req/s, at 0.98
  CPU cores. At saturation the service time was 72 ms (p50) and 116 to 121 ms (p99).
- Below the limit the p99 with Cassandra was higher at every rate: 12.0 to 15.7 ms against
  6.5 to 8.0 ms at 500 req/s, and 40.2 to 46.6 ms against 4.5 to 7.5 ms at 1,500 req/s.
- CPU per request in the service: 0.35 to 0.37 ms with Redis and 0.61 to 0.66 ms with
  Cassandra at 1,500 req/s.
- The SLO target of p99 at most 25 ms held with Cassandra up to 1,000 req/s. At 1,250 req/s two
  runs were below 25 ms and one was at 26.76 ms. At 1,500 req/s it did not hold.
- The Cassandra container used 33 to 79 percent of one core during the runs, mostly 40 to 65
  percent, and 2.4 to 2.6 GiB of memory. The service process was at 0.98 cores at saturation.
  The limit of this setup is therefore in the service process, not in the Cassandra container.
- Errors: 0 in all runs. Entries that are not ok: 0 with Redis. With Cassandra: 10 in run 1 at
  500 req/s, none from 1,000 to 1,500 req/s, 1,730 to 2,350 at 1,750 req/s and 2,800 to
  3,130 at 2,000 req/s. Every count is a multiple of 10, so whole requests failed, which
  matches a reader that raises. All of them are `backend_error`.

Limits:

- Client and service share one machine. Cassandra is one node in Docker Desktop, so there is no
  replication and no network.
- Cassandra's own capacity was not measured. The container stayed below one core.
- The cause of the `backend_error` requests is not known. The benchmark counts statuses and
  does not record the `error_type` that the service returns.
- The container CPU samples cannot be assigned to single rates, because each sample takes
  about 12 s.
- Earlier trial runs, taken right after seeding or with the monitoring stack running, are not
  part of these numbers. They showed the same saturation point (1,601 to 1,626 req/s).
- At 500 req/s the p50 is higher than at 1,000 req/s with both backends. Do not read the low-load
  latency as a property of the service.
- The CPU cost per request was not broken down. It is not known whether the five partition
  reads, the driver or the conversion of the rows takes most of it.
- One service process only. No claim for several processes or hosts.

Repeat for Cassandra:

```bash
docker compose up -d cassandra
python scripts/benchmark_feature_serving.py --backend cassandra --entities 1000000 --rates 100 --repeats 1 --duration 5 --warmup 2 --reseed
docker compose exec cassandra nodetool flush ff_loadtest
caffeinate -i python scripts/benchmark_feature_serving.py --backend cassandra --entities 1000000 --rates 500,1000,1250,1500,1750,2000 --repeats 3 --duration 30 --warmup 10
```

## Compare two reports

`scripts/compare_benchmarks.py` compares two JSON reports of the benchmark script, for example
before and after a change, or two backends:

```bash
python scripts/compare_benchmarks.py docs/benchmarks/adr016-redis-5ae169a.json docs/benchmarks/adr016-cassandra-5ae169a.json --label-base redis --label-new cassandra
```

How to read it:

- The repeats of one rate are reduced to their median. A change is marked `better` or `worse`
  only when it is at least 10 percent and the ranges of the repeats do not overlap. Otherwise the
  cell says `same`. With fewer than three repeats on one side it says `inconclusive`.
  The threshold is above the 7 percent by which two runs of the same code differed in the
  p50 near saturation (`docs/benchmarks/explore-baseline-1-f4076ce-dirty.json` against
  `explore-baseline-2-f4076ce-dirty.json`).
- The lines above the table say up to which rate each report stays within the latency target:
  p99 at most 25 ms in every repeat, at most 0.1 percent of the entries not ok, and at least
  99 percent of the rate achieved.
- The exit code is 1 only when the new report has entries that are not ok, beyond that budget, at
  a rate where the base had none, or request errors where the base had none. Speed never changes
  the exit code.
- Compare reports from one machine only. The script warns when the machine, the setup or the
  rates differ and when a report was measured on a dirty working tree.

## Change 5: plain tuple rows from Cassandra (ADR-016)

Question: does reading the Cassandra rows as plain tuples (`tuple_factory`) instead of named
tuples raise the saturation point of the service?

Method: an A/B on a clean tree, run alternately as base, new, base, new. Base is `f4076ce`, new
is `209da76` (the merge commit of the change). Each run had 1,000,000 users, a warm-up of 30 s at
1,500 req/s and then 3 runs of 30 s at 1,500, 2,000, 2,250 and 2,500 req/s, with one service
process. Cassandra was seeded in 146 s and flushed, and no compaction was pending. Prometheus,
Grafana, Kafka, Flink and the Compose feature service were stopped. Raw data is in
`docs/benchmarks/change5-*.json`. Values are the medians of the three runs, first of round 1 and
then of round 2.

Throughput, CPU and entries that are not ok:

| Target req/s | Achieved base | Achieved new | CPU ms per request base | CPU ms per request new | Not ok base | Not ok new |
|---|---|---|---|---|---|---|
| 1500 | 1500, 1500 | 1500, 1500 | 0.65, 0.65 | 0.58, 0.59 | 0, 180 | 0, 1230 |
| 2000 | 1621, 1635 | 1998, 1998 | 0.60, 0.60 | 0.50, 0.50 | 10240, 9360 | 0, 150 |
| 2250 | 1647, 1647 | 2105, 2113 | 0.59, 0.60 | 0.47, 0.46 | 10600, 11420 | 500, 1130 |
| 2500 | 1642, 1644 | 2104, 2104 | 0.59, 0.60 | 0.47, 0.46 | 13000, 13780 | 130, 60 |

Latency, counted from the time a request was due:

| Target req/s | p50 base | p50 new | p99 base | p99 new |
|---|---|---|---|---|
| 1500 | 8.96, 9.28 ms | 2.95, 3.07 ms | 45.6, 42.4 ms | 15.9, 42.8 ms |
| 2000 | 3.55, 3.45 s | 18.5, 23.9 ms | 6.96, 6.67 s | 70.1, 108.5 ms |
| 2250 | 5.55, 5.54 s | 1.03, 1.08 s | 10.9, 10.9 s | 2.07, 1.98 s |
| 2500 | 7.82, 7.85 s | 2.86, 2.87 s | 15.5, 15.5 s | 5.62, 5.56 s |

Findings:

- The saturation point rose from about 1,660 req/s (1,662 and 1,655 highest achieved) to about
  2,110 req/s (2,107 and 2,118), 27 percent more. At 2,000 req/s the new code held the rate with a
  p99 of 70 to 109 ms, and the base reached 1,621 to 1,635 req/s with a queue of seconds.
- The CPU time per request in the service fell by about 10 percent at 1,500 req/s and by 16 to
  22 percent from 2,000 req/s.
- The p50 at 1,500 req/s fell from 9.0 to 9.3 ms to 3.0 to 3.1 ms in both rounds.
- The p99 at 1,500 req/s was 15.9 ms in round 1 and 42.8 ms in round 2, the base had 45.6 and
  42.4 ms. The gain in the tail at this rate did not repeat.
- Entries that are not ok at 1,500 req/s: none in round 1, and in round 2 up to 0.18 percent of the
  entries in one run of the new code against 0.04 percent for the base. The cause is not known.
- Two runs of the base against each other (`base-1` and `base-2`, commit `f4076ce`) show no
  change at any rate with a threshold of 10 percent. The p50 at 1,500 req/s differs by 4 percent
  and the highest achieved rate by 0.4 percent.

Variants that were tried on a dirty tree before this change (`docs/benchmarks/explore-*`, one
baseline before and one after the variants, 1,250 to 2,000 req/s) and not adopted: protocol
version 4 instead of 5 changed the CPU per request by at most 6 percent, within the noise of the
two baselines, and a switch interval of 1 ms was worse from 1,750 req/s on (1,556 and 1,407
req/s achieved at 1,750 and 2,000 req/s, against 1,650 and 1,646).

Limits:

- Client and service share one machine, and Cassandra is one node in Docker Desktop.
- Two rounds of three runs. The p99 at 1,500 req/s differs between the rounds.
- The rates 1,250 and 1,750 were not part of this series, so it is not measured where between
  1,000 and 2,000 req/s the p99 target of 25 ms stops holding with the new code.
- It is not known why the saturation point rose by more (27 percent) than the CPU per request
  fell (up to 22 percent).
- One service process only.

Compare the reports:

```bash
python scripts/compare_benchmarks.py docs/benchmarks/change5-base-1-f4076ce.json docs/benchmarks/change5-new-1-209da76.json --label-base base-1 --label-new new-1 --min-change 0.10
```

## Run a benchmark

You need `make setup` and Docker. Start the services of the backend (`docker compose up -d redis` or
`docker compose up -d cassandra`), then:

```bash
make bench-smoke                          # about 1 minute, checks that the tool and Redis work
make bench-smoke BENCH_BACKEND=cassandra  # the same with Cassandra, in its own keyspace
make bench-record BENCH_BACKEND=cassandra # about 20 minutes, a full report
```

What the runner does and why:

- `smoke` uses 2,000 users and 5 s per run. It shows that the tool works and its numbers are not
  published. With Redis it replaces the data of the benchmark database.
- `record` needs a clean working tree and only the services of the backend, because other
  services (Kafka, Flink, Grafana) change the result. If something is wrong it says what and how to
  fix it. It seeds fresh data, because the data expires after two hours. With Cassandra it flushes
  the table and waits until no compaction is pending, because compaction right after the seed
  caused errors in the first runs.
- The report is written to `output/benchmarks/`. To keep it, copy it to `docs/benchmarks/` with the
  commit in the name, and compare two reports with `make bench-compare BASE=a.json NEW=b.json`
  (see "Compare two reports").
- Compare reports from one machine only. The numbers of a laptop are not the numbers of a server.

## Baselines after change 5 (ADR-016)

Question: where do the two backends stand today, measured with the runner (`make bench-record`)?

Method: commit `8989796`, clean tree, one uvicorn process, 1,000,000 users, a warm-up of 30 s, then
3 runs of 30 s per rate. Cassandra was measured first (seeded in 146 s, flushed, no compaction
pending), then Redis (seeded in 8 s). Prometheus, Grafana, Kafka, Flink and the Compose feature
service were stopped. Raw data: `docs/benchmarks/baseline-cassandra-8989796.json` and
`docs/benchmarks/baseline-redis-8989796.json`. Ranges cover the three runs, and "not ok" lists the
entries that are not ok of each run.

Redis:

| Target req/s | Achieved req/s | p50 ms | p99 ms | CPU ms per request | Not ok |
|---|---|---|---|---|---|
| 500 | 500 | 2.49 to 3.30 | 6.59 to 7.62 | 0.68 to 0.76 | 0, 0, 0 |
| 1000 | 1000 | 1.69 to 1.92 | 4.19 to 39.34 | 0.50 to 0.53 | 0, 400, 0 |
| 2000 | 2000 | 0.95 to 1.29 | 5.44 to 9.61 | 0.28 to 0.33 | 0, 0, 0 |
| 3000 | 2999 | 1.04 to 1.10 | 6.77 to 7.75 | 0.29 to 0.30 | 0, 0, 0 |
| 4000 | 3993 to 3996 | 35.0 to 37.5 | 59.5 to 82.6 | 0.31 | 0, 0, 0 |

Cassandra:

| Target req/s | Achieved req/s | p50 | p99 | CPU ms per request | Not ok |
|---|---|---|---|---|---|
| 500 | 500 | 3.83 to 4.23 ms | 9.56 to 21.31 ms | 0.94 to 1.00 | 520, 430, 0 |
| 1000 | 1000 | 2.61 to 3.12 ms | 10.30 to 11.41 ms | 0.62 to 0.65 | 0, 0, 0 |
| 1500 | 1500 | 2.97 to 3.35 ms | 16.31 to 23.21 ms | 0.58 to 0.61 | 0, 0, 240 |
| 2000 | 1987 to 1999 | 17.8 to 62.1 ms | 68.9 to 216.2 ms | 0.48 to 0.50 | 150, 0, 0 |
| 2500 | 2085 to 2098 | 2.84 to 3.06 s | 5.72 to 5.95 s | 0.46 to 0.47 | 0, 0, 0 |

Findings:

- Redis held 3,000 req/s with a p99 of at most 7.75 ms in every run. At 4,000 req/s it kept the rate
  (3,993 to 3,996) with a queue (service p50 29 ms, 1.25 CPU cores), as in change 4.
- Cassandra held 1,500 req/s with a p99 of 16 to 23 ms and kept the rate at 2,000 req/s with a p99
  of 69 to 216 ms. It saturated at about 2,090 req/s (2,085 to 2,098 at 2,500 req/s).
- The p99 target of 25 ms held at every measured rate up to 3,000 req/s for Redis (by the median of
  the runs) and at 500, 1,000 and 1,500 req/s for Cassandra (by the median of the runs).
- CPU time per request in the service from 2,000 req/s on: 0.28 to 0.33 ms with Redis and 0.46 to
  0.50 ms with Cassandra, about 1.5 times as much. Before change 5 it was about twice as much.
- Single runs had short bursts of requests with `backend_error` and a high p99.9, with both
  backends: Redis at 1,000 req/s (run 2: 400 entries, p99 39.3 ms), Cassandra at 500 req/s (runs 1
  and 2: 520 and 430 entries), at 1,500 req/s (run 3: 240) and at 2,000 req/s (run 1: 150). The
  cause is not known. They hit rates far from saturation and both backends, so the machine
  (macOS, Docker Desktop) is a suspect.

Limits:

- Client and service share one machine. Both databases run in Docker Desktop, and Cassandra is one
  node.
- Cassandra had its episodes at the first rate in two of three runs. The same happened in the
  other Cassandra series of this day (30 s or less of warm-up after the seed). Whether a longer
  warm-up avoids it was not tested.
- The comparison script reports a failure for Cassandra against Redis at 500 req/s because of
  these episodes. The medians above are the better summary.
- Cassandra was measured before Redis, 17 minutes apart.
- One service process only.
