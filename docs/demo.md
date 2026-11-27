<a id="top"></a>

<div align="center">
  <img src="images/header-demo.svg" alt="FeatureForge demo guide: five walkthroughs that run from a clean checkout" width="100%">
</div>

# FeatureForge Demo Guide

<div align="center">

[Prerequisites](#prerequisites) · [A: batch](#demo-a-batch-path) · [B: streaming](#demo-b-streaming-path) · [C: feature spec](#demo-c-feature-spec) · [D: training dataset](#demo-d-training-dataset) · [E: feature serving](#demo-e-feature-serving) · [CI evidence](#ci-evidence) · [Troubleshooting](#troubleshooting) · [Cleanup](#cleanup)

</div>

Five reviewer walkthroughs, each runnable from a clean checkout.

> [!TIP]
> Short on time? [Demo E](#demo-e-feature-serving) starts with one command and takes under a
> minute after the first image build. The
> [tour notebook](../notebooks/featureforge-tour.ipynb) needs no Docker and renders on GitHub.

| Demo | Shows | Needs | Local runtime |
|---|---|---|---|
| [A: batch path](#demo-a-batch-path) | Synthetic data to Feast-materialized online features | Redis | About 2 minutes |
| [B: streaming path](#demo-b-streaming-path) | Kafka to Flink to Redis, verified end to end | Full Compose stack | Several minutes |
| [C: feature spec](#demo-c-feature-spec) | A declarative spec validated, evaluated against golden fixtures, run as a generic Flink job and served from Redis without feature-specific code | Nothing for parts 1 to 3, the stack for parts 4 to 7 | Seconds for the evaluation, about a minute for the generic job, about ten seconds for the YAML-only end-to-end test on a warm stack |
| [D: training dataset](#demo-d-training-dataset) | Features and a label joined in two explicit modes, event time and published-as-of, written as Parquet with a manifest | No Docker | About 20 seconds once the Spark packages are cached |
| [E: feature serving](#demo-e-feature-serving) | The feature serving service: a status for every value, a stopped Redis, metrics and a benchmark smoke run | Redis and the service container | Under a minute after the first image build |

In CI each Flink test module starts from a fresh TaskManager, and the skip-rate suites recreate it per test.

```mermaid
flowchart TB
    subgraph A["Demo A: batch"]
        direction LR
        A1[Generate] --> A2[Backfill] --> A3{Correctness<br/>gate} --> A4[Feast<br/>materialize] --> A5[Freshness<br/>check] --> A6[Online lookup<br/>and ranking]
    end
    subgraph B["Demo B: streaming"]
        direction LR
        B1[Kafka events] --> B2[Flink validate<br/>and route] --> B3[Flink feature job<br/>spec-driven] --> B4[Materializer] --> B5[(Redis)] --> B6[Unified online<br/>client]
    end
    subgraph C["Demo C: feature spec"]
        direction LR
        C1[YAML spec] --> C2[Validate<br/>and field check] --> C3[Exact reference<br/>evaluation] --> C4[Golden fixtures<br/>and Python<br/>references]
        C2 --> C5[Generic Flink job<br/>for all features] --> C4
        C5 --> C6[Spec-driven<br/>materializer<br/>and store] --> C7[(Redis)]
    end
    subgraph D["Demo D: training dataset"]
        direction LR
        D1[Events and<br/>pinned history] --> D2[Spark join and<br/>backfills] --> D3[Parquet by UTC day<br/>and manifest]
    end
    subgraph E["Demo E: feature serving"]
        direction LR
        E1[Spec store<br/>writes demo values] --> E2[(Redis)] --> E3[Feature serving<br/>service] --> E4[Status per value:<br/>ok, stale, missing,<br/>backend_error]
        E3 --> E5[Prometheus<br/>and Grafana]
    end
    A ~~~ B ~~~ C ~~~ D ~~~ E
```

## Prerequisites

- Python 3.11, 3.12 or 3.13
- Docker Desktop running
- GNU Make

```bash
git clone https://github.com/oster-dev/featureforge.git
cd featureforge
python3 -m venv .venv && source .venv/bin/activate
python -m pip install --upgrade pip
python -m pip install -e ".[dev,feature-store,spark,streaming,serve]"
```

Demo E alone needs only `python -m pip install -e ".[serve]"`. In a GitHub Codespace or a VS Code
dev container Python, Docker and `make` are set up already.

---

## Demo A: batch path

Local profile: Parquet under `output/offline_store/` as the canonical offline
store, Redis via Docker Compose as the online store, Feast for definitions,
materialization and lookup.

```bash
docker compose up -d redis

cd feature_repo
feast apply
cd ..

make demo
```

`make demo` generates deterministic data, backfills canonical partitions,
validates them **before** materialization, materializes into Redis, checks
freshness, retrieves online features and produces a deterministic ranking. Run
artifacts go to `output_e2e/`, canonical features to `output/offline_store/`.
Nothing generated is committed.

### Expected signals

```text
FeatureForge dataset generated
✓ Backfilled 6 observation-date partition(s)
✓ Feast full materialization completed
✓ Offline feature freshness check passed
=== Feast Online Feature Lookup ===
=== Deterministic Engagement Decision ===
=== FeatureForge Online Personalization Demo ===
=== Ranked Content ===
```

Default data: 500 users, 250 content items, 10,200 events, 1,000 observation
labels, 6 canonical partitions. Scores are deterministic for the current
configuration and may change intentionally when feature definitions or
generation contracts evolve.

![Local pipeline and materialization succeeded](images/local-demo-pipeline-and-materialization.png)

![Local freshness and ranking succeeded](images/local-demo-freshness-and-ranking.png)

### Verify the layers

```bash
docker compose exec -T redis redis-cli ping            # PONG
find output/offline_store -name features.parquet       # six partitions per view
pytest -v tests/integration/test_online_serving.py     # 5 passed
```

The serving suite checks user and content parity against the latest offline
partition, unknown entities, batch candidates and deterministic ranking.

<div align="right"><a href="#top">Back to top</a></div>

---

## Demo B: streaming path

```bash
docker compose up -d --build
docker compose ps            # kafka, jobmanager, taskmanager, redis healthy
```

Run the joined end-to-end tests:

```bash
pytest -v -m integration \
  tests/integration/test_user_play_count_kafka_to_redis.py \
  tests/integration/test_skip_rate_flink_to_redis.py
```

Each test creates isolated Kafka topics, produces events, runs the Flink jobs,
materializes the feature topic into Redis and reads the final online value
against the reference result.

### What the streaming tests show

| Behavior | Test |
|---|---|
| Invalid events go to a rejected topic without blocking valid ones | `test_streaming_validation_routing.py` |
| Late events correct an already emitted window | `test_user_play_count_late_events.py` |
| Events too late for all windows are routed out | `test_user_play_count_late_events.py`, `test_user_skip_rate_job.py` |
| Replay and duplicates do not change results | `test_user_play_count_replay_idempotency.py` |
| A job resumed from a savepoint keeps its dedupe and window state | `test_generic_job_recovery.py` |
| A job recovers from a TaskManager crash with its dedupe and window state | `test_generic_job_checkpoint_recovery.py` |
| The dedupe state expires after its TTL (shortened TTL, play-count spec) | `test_generic_job_dedupe_ttl.py` |
| Exact window boundaries are respected | `test_user_skip_rate_job.py` |
| Stale and conflicting online writes are rejected | `test_redis_skip_rate_store.py` |

Run any of them with `pytest -v -m integration tests/integration/<file>`.

### Read a feature through the unified client

```bash
python -m featureforge.online \
  --user-id user_000290 \
  --reference-time 2026-03-25T00:00:00+00:00 \
  --batch-max-age-seconds 172800 \
  --stream-max-age-seconds 7200
```

Every value reports its backend, availability, observation time and freshness.
A missing value is shown as missing, never as `0`. The streaming play count
and skip rate are read from Redis, an expired key shows as missing. The skip-rate
key prefix defaults to the key of its spec and can be set with
`--skip-rate-key-prefix`. Exit codes: `0` success, `1` feature errors, `2` invalid
request.

### What this demo does not prove

- JobManager failure is not covered, and dedupe expiry is shown only with a
  shortened TTL. Resuming from a savepoint, recovering from a TaskManager crash
  and dedupe expiry are covered by `test_generic_job_recovery.py`,
  `test_generic_job_checkpoint_recovery.py` and `test_generic_job_dedupe_ttl.py`.
- The Flink Kafka sinks are at-least-once. The path is idempotent, not exactly-once.
- Multi-partition Kafka is not demonstrated. This demo neither writes nor reads
  the history table, see Demo D.

See [Known limits](../ARCHITECTURE.md#known-limits).

<div align="right"><a href="#top">Back to top</a></div>

---

## Demo C: feature spec

Features can be described declaratively (see
[ADR-008](adr/ADR-008-feature-spec-design.md)). Parts 1 to 3 need no
infrastructure. Parts 4 to 7 need the streaming stack from Demo B.

### 1. Validate the specs

Validate the shipped specs, including a check that every referenced event field
exists in the event contract:

```bash
python -m featureforge.spec.loader feature_repo/specs/*.yaml
```

Expected output:

```text
✓ feature_repo/specs/content_play_count_1h.yaml: content_play_count_1h v1
✓ feature_repo/specs/user_play_count_1h.yaml: user_play_count_1h v1
✓ feature_repo/specs/user_skip_rate_24h.yaml: user_skip_rate_24h v1
```

`python -m featureforge.cli validate-spec` does the same for every spec in
`feature_repo/specs` and exits `0` when all are valid, `1` when a spec is invalid
(each violated field is listed) and `2` when a file is missing.

### 2. Evaluate the play-count spec on its golden fixture

Evaluate the play-count spec on the golden fixture at `2026-01-01T12:00Z`:

```bash
python - <<'PY'
from datetime import UTC, datetime
from pathlib import Path

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1

spec = load_spec("feature_repo/specs/user_play_count_1h.yaml")
lines = Path("fixtures/streaming/user_play_count_1h_v1.jsonl").read_text().splitlines()
events = [InteractionEventV1.model_validate_json(line) for line in lines if line.strip()]
at = datetime(2026, 1, 1, 12, tzinfo=UTC)
for user in ("user_000001", "user_000002", "user_unknown"):
    result = evaluate_spec(spec, events, entity={"user_id": user}, observation_time=at)
    print(user, result.values)
PY
```

Expected output:

```text
user_000001 {'play_count': 3}
user_000002 {'play_count': 1}
user_unknown {'play_count': 0}
```

The `test-spec` command evaluates one entity at one time without writing any
code and can check an expected value. It runs the spec reference, not the Flink
job:

```bash
python -m featureforge.cli test-spec feature_repo/specs/user_play_count_1h.yaml \
  --events fixtures/streaming/user_play_count_1h_v1.jsonl \
  --entity user_id=user_000001 --at 2026-01-01T11:30:00+00:00 \
  --expect play_count=3
```

Expected output, exit code `0`:

```text
user_play_count_1h · user_id=user_000001
Window (2026-01-01T10:30:00+00:00, 2026-01-01T11:30:00+00:00] · 8 events read
┏━━━━━━━━━━━━┳━━━━━━━━┓
┃ value      ┃ result ┃
┡━━━━━━━━━━━━╇━━━━━━━━┩
│ play_count │ 3      │
└────────────┴────────┘
✓ play_count = 3
```

With a wrong expectation such as `--expect play_count=99` the same command prints
`✗ play_count: expected 99, got 3` and exits with `1`. Exit code `2` means a file
is missing.

The same values come from the Python reference. The test suite checks
that agreement on randomized inputs placed exactly on window boundaries:

```bash
pytest -v tests/unit/test_spec_reference.py
```

The `backfill-spec` command evaluates a spec for many entities and observation
times at once with Spark and writes the values as Parquet. It needs the `spark`
extra, supports count, sum, avg, min and max aggregations only and never overwrites an existing
output. The observations are a JSONL file with the spec keys and
`observation_time`:

```bash
printf '%s\n' \
  '{"user_id": "user_000001", "observation_time": "2026-01-01T11:30:00+00:00"}' \
  '{"user_id": "user_unknown", "observation_time": "2026-01-01T11:30:00+00:00"}' \
  > /tmp/backfill_observations.jsonl
rm -rf /tmp/backfill_demo
python -m featureforge.cli backfill-spec feature_repo/specs/user_play_count_1h.yaml \
  --events fixtures/streaming/user_play_count_1h_v1.jsonl \
  --observations /tmp/backfill_observations.jsonl \
  --output /tmp/backfill_demo
python -c "import pandas as pd; print(pd.read_parquet('/tmp/backfill_demo').sort_values('user_id')[['user_id', 'play_count']].to_string(index=False))"
```

The command prints `✓ user_play_count_1h: 2 rows written to /tmp/backfill_demo` and
exits with `0`. The result has one row per observation: `play_count` is 3 for
`user_000001` and 0 for `user_unknown`, the same values as `test-spec`. The
timestamps are UTC with microsecond precision, and the command never reads or
writes publication time. Exit code `1` means invalid input, an identity
conflict, an unsupported operator or an existing output path, and `2` means a
file is missing or PySpark is not installed. The test suite checks the agreement
with the reference at every window boundary:

```bash
pytest -v tests/unit/test_spec_spark_backfill.py tests/unit/test_cli_backfill_spec.py
```

### 3. Evaluate the skip-rate spec on its golden fixture

The skip-rate fixture holds ten events with hand-calculated expected values:
events exactly on both window edges, a duplicate delivery, `impression` and
`complete` events that count toward nothing, a second user, and expiry down to
an empty window.

```bash
python - <<'PY'
from datetime import UTC, datetime
from pathlib import Path

from featureforge.spec.loader import load_spec
from featureforge.spec.reference import evaluate_spec
from featureforge.streaming.contracts.events import InteractionEventV1

spec = load_spec("feature_repo/specs/user_skip_rate_24h.yaml")
lines = Path("fixtures/streaming/user_skip_rate_24h_v1.jsonl").read_text().splitlines()
events = [InteractionEventV1.model_validate_json(line) for line in lines if line.strip()]
cases = [
    ("user000001", datetime(2026, 1, 2, 12, 0, 0, tzinfo=UTC)),
    ("user000001", datetime(2026, 1, 2, 12, 0, 1, tzinfo=UTC)),
    ("user000001", datetime(2026, 1, 2, 18, 0, 0, tzinfo=UTC)),
    ("user000001", datetime(2026, 1, 3, 12, 0, 1, tzinfo=UTC)),
    ("user000002", datetime(2026, 1, 2, 12, 0, 0, tzinfo=UTC)),
]
for user, at in cases:
    result = evaluate_spec(spec, events, entity={"user_id": user}, observation_time=at)
    print(user, at.isoformat(), result.values)
PY
```

Expected output:

```text
user000001 2026-01-02T12:00:00+00:00 {'play_count': 2, 'skip_count': 2, 'skip_rate': 0.5}
user000001 2026-01-02T12:00:01+00:00 {'play_count': 1, 'skip_count': 3, 'skip_rate': 0.75}
user000001 2026-01-02T18:00:00+00:00 {'play_count': 1, 'skip_count': 2, 'skip_rate': 0.6666666666666666}
user000001 2026-01-03T12:00:01+00:00 {'play_count': 0, 'skip_count': 0, 'skip_rate': 0.0}
user000002 2026-01-02T12:00:00+00:00 {'play_count': 0, 'skip_count': 1, 'skip_rate': 1.0}
```

Between the first and second line, one event leaves the window on its lower
edge and one enters on its upper edge, and the rate moves from `0.5` to `0.75`.
The tests that hold these values, including a sweep over every event boundary
against the Python reference:

```bash
pytest -v tests/unit/test_streaming_user_skip_rate_golden.py tests/unit/test_spec_golden_parity.py   # 14 passed
```

### 4. Run the generic Flink job on the fixtures

The generic job reads a spec and builds the windowed computation from it. The
test sends the golden fixture of each feature through the job and compares
**every** emitted window with the spec reference and the Python reference:

```bash
docker compose up -d --build
pytest -v -m integration tests/integration/test_feature_from_spec_job.py   # 2 passed
```

The test also requires hand-calculated windows (five for skip rate, seven for
play count), no window emitted twice, and an empty late topic. A spec outside
what the job supports, such as a `min` aggregation or a composite key, is
rejected with a clear error before the job starts:

```bash
pytest -v tests/unit/test_spec_flink_plan.py   # 22 passed
```

### 5. Stop the job with a savepoint and resume it

The generic job for the play-count spec is stopped with a savepoint. A new job
resumes from that savepoint. The test shows that the new job restored the
savepoint, that a redelivered event is not counted again, that a late
correction lands in the restored window, and that the late topic stays empty:

```bash
docker compose up -d --build
pytest -v -m integration tests/integration/test_generic_job_recovery.py   # 1 passed
```

Both the JobManager and the TaskManager mount the `flink-savepoints` volume,
so the savepoint written by one is readable by the other.

### 6. Kill the TaskManager and let the job recover

The test kills the TaskManager while the generic job runs for the play-count
spec. The same job restarts on its own and restores a checkpoint that is not a
savepoint. A redelivered event is not counted again, a late correction lands in
the restored window, and the late topic stays empty:

```bash
docker compose up -d --build
pytest -v -m integration tests/integration/test_generic_job_checkpoint_recovery.py   # 1 passed
```

The test restarts the TaskManager in its cleanup. The JobManager keeps running
and holds the checkpoints in memory, so this does not show recovery from a
JobManager failure.

### 7. Run a feature defined by YAML alone from events to Redis

`content_play_count_1h` has no job, store, materializer or record type of its
own. The test sends plays for two contents through the validation job, the
generic Flink job on `feature_repo/specs/content_play_count_1h.yaml`, the
spec-driven materializer and the spec-driven store, and reads the values back:

```bash
docker compose up -d --build
pytest -v -m integration tests/integration/test_content_play_count_end_to_end.py   # 1 passed
```

The expected values are hand-calculated. One content has plays at 11:30 and
11:40, and the second event is delivered twice. It gives 70 windows ending 11:30
to 12:39. The other content has one play at 11:30 and gives 60 windows ending
11:30 to 12:29. All 130 records pass through the store. The final windows 12:39
and 12:29 hold a count of 1 (a counted duplicate would show 2), the stored value
has an expiry within `online.ttl`, a second run does nothing, and a replay with
a new consumer group yields 128 `stale` and 2 `unchanged` results, so a replay
cannot undo the stored state.

The two halves can be run on their own:

```bash
docker compose up -d kafka redis
pytest -v -m integration \
  tests/integration/test_spec_store_redis.py \
  tests/integration/test_materialize_spec_kafka_to_redis.py   # 10 passed
```

The first checks the store against a real Redis: round trips for three specs,
window and correction policy, incomparable counts and the TTL. The second
publishes six records for `content_play_count_1h` to a Kafka topic and runs the
materializer as a subprocess.

The materializer also runs by hand. It derives the topic from the spec
(`features.content-play-count-1h.v1`), validates every record against the spec
and prints a JSON summary such as `{"store_results": {"written": 4}}`:

```bash
python -m featureforge.streaming.materialize_spec \
  --spec feature_repo/specs/content_play_count_1h.yaml \
  --group-id my-group --bounded
```

### What this demo does not prove

- The generic job is verified on the golden fixtures, late corrections, replay
  and the Redis path for both original features, and resumes from a savepoint
  and recovers from a TaskManager crash for the play-count spec, and its dedupe
  state expires after a shortened TTL. JobManager failure and the production TTL
  values are not proven for it.
- The two original features run on the spec-driven store and materializer
  through thin wrappers. The path is proven end to end from events to Redis by
  its own test for `content_play_count_1h`. A Spark backfill from a spec covers count, sum, avg, min and max aggregations only (`backfill-spec`),
  and the two original Spark features are hand-written.
- `online.ttl` is a Redis expiry from the last accepted write for all three
  streaming features. Keys written before the wrappers have no expiry until
  their next written update.
- The `WATCH` retry of the spec-driven store has unit tests with a mocked
  pipeline and runs against a real concurrent writer through the two wrappers.

<div align="right"><a href="#top">Back to top</a></div>

---

## Demo D: training dataset


A dataset joins the features and one label of every observation, in one of two
explicit modes that never replace each other
([ADR-014](adr/ADR-014-training-datasets.md)). The `build-training-dataset` command writes a dataset from JSONL files, and the tests below build real datasets with Spark.
It needs no Docker. The first run downloads the Spark packages.


### 1. Event-time mode


```bash
pytest -v tests/unit/test_training_dataset.py   # 12 passed
```


Features come from events over `(t - size, t]`, the label from events over
`(t, t + horizon]`. The hand-calculated fixture puts events at `t - 1h`, `t`,
just after `t`, at `t + horizon` and just after it. The event at `t` is a feature
and never the label. An observation whose label window ends after `data_end` is
excluded and counted. The manifest lists every partition, the row counts add up,
existing output is never overwritten, and 60 random observations match the
feature and label references.


### 2. Published-as-of mode and the history


```bash
pytest -v -m integration \
  tests/integration/test_published_spark.py \
  tests/integration/test_training_dataset_published.py   # 4 passed
```


The first file runs the Spark join of the history. One version is published at
`P`, and `online.ttl` is the retention:


| Observation time | Status |
|---|---|
| `P` minus 1 microsecond | `not_published` |
| `P` | `present` |
| `P + ttl` minus 1 microsecond | `present` |
| `P + ttl` | `expired` |


The same file compares the join with `published_as_of` on randomized histories
at every boundary. The second file builds both modes over the same observations.
A late event separates them: the event-time dataset counts it, the history does
not hold it. An observation before the history start is excluded and counted, an
observation exactly at the start stays, and with two features the later start
cuts. A feature that is not published or expired has null values and a
`<spec>__status` column, never zero. The test also checks that a run leaves the
history snapshot unchanged.


The manifest of the published-as-of dataset in the first test has this shape
(values elided where they depend on the run):


```json
{
  "manifest_version": 1,
  "mode": "published_as_of",
  "features": [{"name": "user_skip_rate_24h", "version": 1}],
  "label": {"name": "label_under_test", "version": 1, "horizon_seconds": 604800},
  "observations": 3,
  "rows": 2,
  "excluded_incomplete": 0,
  "excluded_before_history": 1,
  "history": [{"feature": "user_skip_rate_24h", "snapshot_id": "...", "history_start": "..."}]
}
```


### 3. Write a dataset with the command


The `build-training-dataset` command runs either mode from JSONL files. It needs
the `spark` extra, never overwrites an existing output and never creates a
history table. The observations are a JSONL file with the label keys and
`observation_time` in UTC:

```bash
user=$(head -n 1 fixtures/streaming/user_skip_rate_24h_v1.jsonl | python -c "import sys, json; print(json.loads(sys.stdin.readline())['user_id'])")
printf '{"user_id": "%s", "observation_time": "2026-01-01T12:00:00+00:00"}\n' "$user" > /tmp/dataset_observations.jsonl
rm -rf /tmp/dataset_demo
python -m featureforge.cli build-training-dataset \
  --feature-spec feature_repo/specs/user_skip_rate_24h.yaml \
  --label-spec feature_repo/labels/completed_within_7d.yaml \
  --events fixtures/streaming/user_skip_rate_24h_v1.jsonl \
  --observations /tmp/dataset_observations.jsonl \
  --mode event_time \
  --data-end 2026-01-20T00:00:00+00:00 \
  --output /tmp/dataset_demo
cat /tmp/dataset_demo/_manifest.json
python -c "import pandas as pd; print(pd.read_parquet('/tmp/dataset_demo').to_string(index=False))"
```


The command prints `✓ completed_within_7d (event_time): 1 rows in 1 partitions, 0
observations excluded, written to /tmp/dataset_demo` and exits with `0`. The row
holds the three feature columns `user_skip_rate_24h__play_count`,
`user_skip_rate_24h__skip_count` and `user_skip_rate_24h__skip_rate` with the
values 1, 0 and 0.0, and the label `label__completed_within_7d` is true. The column
`label__completed_within_7d__observable_at` shows the label became observable at
2026-01-08 12:00 UTC, seven days after the observation. The manifest lists the
mode, the feature and label versions, `data_end`, the observation and row counts,
both exclusion counts, the columns and the partitions. Running the same command
again prints `✗ output already exists: /tmp/dataset_demo` and exits with `1`.

Mode `published_as_of` also needs `--history-root DIR` and optionally
`--history-catalog-url`. The command pins the current snapshot of the history of
every feature and records the snapshot ids in the manifest under `history`. A
feature without a history table fails with exit code `1` and creates nothing.
Exit code `1` also means invalid input, an existing output path or no
observation with a complete label window. Exit code `2` means a file or the
history root is missing, or PySpark is not installed:

```bash
pytest -v tests/unit/test_cli_build_training_dataset.py   # 9 passed
pytest -v -m integration tests/integration/test_cli_training_dataset.py   # 2 passed
```


### 4. Read the dataset back with the loader

`load_training_dataset` reads a dataset with pandas and pyarrow, so a training job
does not start Spark. It checks the files against the manifest before it returns a
frame: the manifest exists and has a known version, every manifest column is in the
files, and the row count and the rows per day match. A mismatch raises
`TrainingDatasetError`, a missing directory raises `FileNotFoundError`. The frame
holds the manifest columns in manifest order, sorted by the keys and the observation
time, and both time columns are `datetime64[us, UTC]`. The loader reads the whole
dataset into memory:

```bash
python -c "from pathlib import Path; from featureforge.spec.training_loader import load_training_dataset; loaded = load_training_dataset(Path('/tmp/dataset_demo')); print(loaded.frame.dtypes); print(loaded.frame.to_string(index=False))"
```


### 5. Read the dataset in Arrow batches

`open_training_batches` makes the same checks as the loader above from the Parquet
metadata, so it reads no row before it returns. Its batches come in the order of the
partition days and are not sorted by key. Use it when a dataset does not fit into memory
or when the first batch should arrive at once:

```bash
python -c "from pathlib import Path; from featureforge.spec.training_batches import open_training_batches; opened = open_training_batches(Path('/tmp/dataset_demo')); print([batch.num_rows for batch in opened.batches()])"
```

The dataset of step 3 holds one row, so the command prints `[1]`. ADR-014 has the read
numbers of both loaders.


### What this demo does not prove


- The dataset loader reads a whole dataset into memory. `open_training_batches` reads in batches, and ADR-014 holds the numbers of both.
- The parity between a Redis value and the `published_as_of` value is tested over two bounded runs of one entity, not for several entities or at TTL expiry. The dataset reflects the recorded evidence.
- The `published_as_of` mode needs one entity key. `history_start` marks only the
  beginning of the evidence, so a later gap reads as `not_published`.
- There are no throughput numbers for the Spark join.


<div align="right"><a href="#top">Back to top</a></div>

---

## Demo E: feature serving

The feature serving service answers for features that are defined by specs
([ADR-016](adr/ADR-016-feature-serving.md)). Every value comes back with a status, and a missing
or failed read is never a zero.

<div align="center">
  <img src="images/quickstart.svg" alt="Animated terminal: the quickstart writes two demo values, the feature service answers ok, stale and missing, and after Redis is stopped every entry is backend_error and never a zero." width="820">
</div>

### 1. Start the service and read three users

```bash
make quickstart
```

The command builds the service image, starts Redis and the service, writes two demo values for
`user_play_count_1h` through the spec store and asks the service about three users:

```text
wrote demo_user_fresh: written
wrote demo_user_old: written

entity               status         play_count    age_s  note
demo_user_fresh      ok             7                60  fresh
demo_user_old        stale          4              1200  stale
demo_user_unknown    missing        -                 -  -
```

| User | Status | Why |
|---|---|---|
| `demo_user_fresh` | `ok` | The value is younger than the freshness SLO of 5 minutes |
| `demo_user_old` | `stale` | The value is older than the SLO. It is still returned, marked as stale |
| `demo_user_unknown` | `missing` | Nothing was written. The entry has no value, not a zero |

### 2. Stop Redis and ask again

```bash
make quickstart-failure
```

The command repeats step 1, stops Redis and asks again. Every entry is `backend_error` with the
error type, and the command starts Redis again when it is done:

```text
Stopping Redis and asking again ...

entity               status         play_count    age_s  note
demo_user_fresh      backend_error  -                 -  ConnectionError
demo_user_old        backend_error  -                 -  ConnectionError
demo_user_unknown    backend_error  -                 -  ConnectionError
```

### 3. Ask the service yourself

```bash
curl -s -X POST http://127.0.0.1:8080/v1/features/lookup \
  -H 'Content-Type: application/json' \
  -d '{
    "features": [{"name": "user_play_count_1h"}],
    "entities": [{"user_id": "demo_user_fresh"}, {"user_id": "demo_user_unknown"}]
  }'
curl -s http://127.0.0.1:8080/readyz
curl -s http://127.0.0.1:8080/metrics | grep featureforge_lookup_entries_total
```

The response holds one entry per entity and feature with `status`, `values`, `age_seconds` and
`freshness`, and `"atomic": false` for the whole response. A request with more than 100 entities
or 50 features is rejected with 422.

### 4. Watch the metrics

```bash
docker compose up -d --build feature-service prometheus grafana
```

Grafana runs at http://127.0.0.1:3000 with the dashboard "FeatureForge feature serving", and
Prometheus at http://127.0.0.1:9090. What each panel measures is in
[observability.md](observability.md). The screenshot shows the dashboard during a benchmark run.

![Dashboard during a benchmark run](images/feature-serving-dashboard.png)

### 5. Run the benchmark smoke test

```bash
docker compose stop prometheus grafana
make bench-smoke
```

The smoke run takes about a minute and shows that the benchmark tool and the backend work. It
only warns about other running services, and its numbers are never quoted. The recorded numbers, their setup and their limits are in
[benchmarks.md](benchmarks.md).

### 6. Run the tests behind the contract

```bash
docker compose --profile fault up -d redis toxiproxy
pytest -v -m integration \
  tests/integration/test_feature_service_redis.py \
  tests/integration/test_feature_service_faults.py
```

The first file checks the statuses, the expiry and one `MGET` per request against a real Redis.
The second puts Toxiproxy between the service and Redis: a stopped and a slow Redis give
`backend_error` within the budget, and the service recovers afterwards.

The same spec runs on Cassandra with only `online.store` changed. On the full stack with the
Cassandra profile, one test goes from events through the validation job, the generic Flink job
and the materializer to the service:

```bash
COMPOSE_PROFILES=cassandra docker compose up -d --build
pytest -v -m integration tests/integration/test_cassandra_end_to_end.py
```

### What this demo does not prove

- The quickstart writes its values through the spec store directly. It does not run Kafka or
  Flink. Demo C part 7 shows the path from events.
- The service is one process on one machine, and the local Cassandra is one node. Nothing here
  shows replication, several processes or a production load.
- The budget of 50 ms is a socket timeout per operation, not a deadline over the whole request.
- There are no alert rules, and the smoke run is not a measurement.

<div align="right"><a href="#top">Back to top</a></div>

---


## CI evidence


Six GitHub Actions jobs run on every push and pull request to `main`. A
scheduled nightly run on `main` repeats them.

| Job | Proves |
|---|---|
| Baseline lint and test | Clean install, Ruff, mypy, all infrastructure-free tests |
| Python 3.11/3.12 compatibility | The infrastructure-free suite on Python 3.11 and 3.12 |
| Redis and Feast serving integration | Redis, Feast, materialization, online parity and ranking, the history, the published-as-of join, the training datasets, and the feature service against a real, a stopped and a slow Redis. Fails on a skipped test |
| Cassandra schema, store and contract suite | The schema check, the Cassandra store and the same contract suite as Redis on a real node. Fails on a skipped test |
| Flink Kafka-to-Redis integration | Full Compose stack with Cassandra, joined Kafka, Flink and store tests including the generic spec-driven job, each Flink test module on a fresh TaskManager |
| Benchmark smoke (Redis) | The benchmark tool runs end to end against a real Redis. Its numbers are not used |

Current status: see the badge in the [README](../README.md) and the
[Actions page](https://github.com/oster-dev/featureforge/actions).

![Baseline lint and test succeeded](images/ci-baseline-lint-and-test.png)

![Redis and Feast serving integration succeeded](images/ci-redis-and-feast-serving-integration.png)

The baseline job skips the five online-serving tests on purpose, because they
need Redis, applied Feast definitions and materialized values. The serving job
runs them with zero skips.

---

## Troubleshooting

| Symptom | Fix |
|---|---|
| `make quickstart` stops with `the service ... did not become ready` | `docker compose ps` and `docker compose logs feature-service --tail 50`. Port 8080 must be free |
| Every entry is `backend_error` outside the failure demo | Redis is stopped: `docker compose start redis` |
| Redis unavailable | `docker compose down && docker compose up -d`, then `docker compose exec -T redis redis-cli ping` |
| Serving tests skipped | Redis reachable, `feast apply` run, canonical partitions present, features materialized (see below) |
| Materialization blocked | Inspect `find output -path "*materialization_manifests*" -name "*.json"`, then the [runbook](runbooks/failed-materialization.md) |
| Streaming tests cannot connect | `docker compose ps`, Redis published on `127.0.0.1:6379`, nothing else on port 6379 |
| Flink job fails or hangs | `docker compose logs jobmanager --tail 100` and `docker compose logs taskmanager --tail 100` |
| TaskManager missing after an interrupted recovery test | `docker compose up -d taskmanager`, then `curl -s http://127.0.0.1:8081/overview` shows one TaskManager and a free slot |
| Generic job fails with `ModuleNotFoundError` | A dependency is missing in `docker/flink/Dockerfile`. Rebuild with `docker compose up -d --build` |
| Changed code under `src/` not picked up by Flink | `jobs/` and `feature_repo/specs/` are mounted, `src/` is copied into the image. Rebuild with `docker compose up -d --build` |
| Savepoint cannot be written or read | JobManager and TaskManager must both mount the `flink-savepoints` volume. A volume with old permissions: `docker compose down`, `docker volume rm featureforge-stream_flink-savepoints`, `docker compose up -d --build` |
| `ClassNotFoundException: iceberg.DefaultSource` | A plain Spark session started earlier in the same `pytest` call. Run the history and dataset tests in their own call, as in Demo D |
| The dataset tests fail offline | Spark downloads the Iceberg and JDBC packages on the first run. Run once with network access |
| Spec validation fails | Read the printed field path and message. An unknown event field is reported with the available fields |
| `materialize_spec` stops with `Topic is not available` or `Refusing to materialize an empty Kafka range` | Run the Flink job for that spec first. The feature topic must exist and contain records |
| `materialize_spec` stops with `record fields do not match` | The record on the topic and the spec disagree. Compare the payload with the spec fields and version. Never edit Redis by hand |

Prepare serving explicitly if tests skip:

```bash
docker compose up -d redis
(cd feature_repo && feast apply)
featureforge materialize \
  --repo feature_repo \
  --start-time 2026-03-20T00:00:00+00:00 \
  --end-time 2026-03-25T00:00:00+00:00 \
  --manifest-output output
pytest -v tests/integration/test_online_serving.py
```

A blocked materialization means the correctness gate found invalid persisted
data. Fix the data, do not bypass the gate.

---

## Cleanup

```bash
make docker-down                 # stop services
rm -rf output_e2e output/source_data output/offline_store \
  output/materialization_manifests
```

Do not remove `feature_repo/data/registry.db` while Redis or Feast commands
are running. For a complete reset, stop Redis first, then use a fresh checkout
or re-apply the Feast repository.

---

<div align="center">

[README](../README.md) · [Architecture](../ARCHITECTURE.md) · [Status](../STATUS.md) · [Contributing](../CONTRIBUTING.md) · [Benchmarks](benchmarks.md)

<a href="#top">Back to top</a>

</div>
