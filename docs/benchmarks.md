# Feature serving benchmarks

Status: first baseline. Numbers describe one laptop, one service process and
the commit named below. They are measurements, not guarantees.

## Setup

| Item | Value |
|---|---|
| Machine | Apple M4 Pro, 14 logical CPUs, 24 GB, macOS 26.6.2 |
| Python | 3.13.9 |
| Packages | fastapi 0.141.1, uvicorn 0.53.0, starlette 1.6.0, redis 5.3.1, pydantic 2.13.5 |
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
