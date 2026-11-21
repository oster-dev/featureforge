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

## Limits

- Client and service share one machine.
- Redis runs in Docker Desktop, so its latency includes a VM.
- The 50 ms budget is a socket timeout per Redis operation, not an end-to-end
  deadline.
- The cause of the slow Redis calls is not known.
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
