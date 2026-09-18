# FeatureForge

FeatureForge is a production-inspired feature platform for reproducible offline
training data and future low-latency online ML feature serving.

It is built as a portfolio project for Data Infrastructure, Feature
Infrastructure, and ML Platform Engineering. The project focuses on the
hard parts that make feature platforms trustworthy:

- deterministic data generation
- executable data contracts
- event-time correctness
- point-in-time feature computation
- partitioned offline feature datasets
- reproducible backfills
- idempotency
- audit manifests
- quality validation
- unit and integration tests
- engine-independent feature correctness (Pandas and PySpark parity)

## Project Goal

The long-term goal is to provide trusted, versioned, point-in-time-correct
features for both offline model training and online inference.

The target architecture will use:

- PySpark for scalable batch feature computation
- S3-backed Parquet for the offline feature store
- Feast for feature definitions and historical retrieval
- Redis for local online serving
- DynamoDB as the documented AWS online-store profile
- GitHub Actions for continuous integration

The current implementation establishes the local reference foundation needed
to build those components correctly, and now includes a parity-tested PySpark
execution engine alongside the original Pandas reference.

## Current Status

### Implemented

- Pydantic contracts for users, content, events, observation labels, feature
  records, feature batches, and synthetic-data configuration
- YAML-backed synthetic-data configuration with validation for time ranges,
  event rates, and late-arrival constraints
- Deterministic synthetic user, content, event, duplicate-delivery, late-event,
  and observation-label generation
- Explicit event-time and ingestion-time modeling
- Quality validation for referential integrity, temporal validity, event
  semantics, late-event semantics, label validity, and expected volumes
- Source dataset persistence as Parquet:
  - `users.parquet`
  - `content.parquet`
  - `events.parquet`
  - `labels.parquet`
- User engagement feature computation
- Content popularity feature computation
- Explicit point-in-time lookback windows
- Deterministic, partitioned offline feature datasets
- Date-parameterized backfills
- Idempotent feature-partition writes
- JSON run manifests for generation and backfill auditability
- CLI commands for generation, single snapshot feature calculation, and
  multi-day backfills
- A PySpark implementation of both feature views, parity-tested against the
  Pandas reference implementation
- Local Redis service through Docker Compose for the future online-serving stage
- Unit and integration tests
- Ruff formatting and linting

### Current Quality Gate

The current repository state passes:

```text
ruff format --check .
ruff check .
pytest -v

89 passed
```

## Current Data Flow

```text
validated YAML configuration
        ↓
deterministic synthetic data generation
        ↓
duplicate and late-event injection
        ↓
observation-label generation
        ↓
quality validation
        ↓
source Parquet datasets
        ↓
point-in-time feature computation (Pandas reference, PySpark parity-tested)
        ↓
partitioned offline feature datasets
        ↓
date-parameterized idempotent backfills
        ↓
JSON run manifests
```

## Core Architecture

| Area | Current technology | Role |
|---|---|---|
| Platform language | Python | Pipeline orchestration, contracts, CLI, tests |
| Data contracts | Pydantic | Executable validation for source and feature records |
| Local transformation reference | Pandas | Deterministic feature aggregation and Parquet inspection |
| Scalable transformation engine | PySpark | Parity-tested feature computation for future scale |
| Source and offline format | Parquet with PyArrow | Typed, columnar offline datasets |
| Configuration | YAML | Reproducible synthetic-data generation |
| CLI display | Rich | Human-readable local command output |
| Testing | pytest | Unit and integration coverage |
| Code quality | Ruff | Formatting and linting |
| Local online-store preparation | Redis via Docker Compose | Future low-latency feature serving |
| Planned feature platform | Feast | Feature definitions, historical retrieval, materialization |
| Planned cloud profile | AWS S3 and DynamoDB | Offline and online production-oriented storage |

## Quick Start

### Prerequisites

- Python 3.11, 3.12, or 3.13
- Docker Desktop
- GNU Make

### Clone and Install

```bash
git clone https://github.com/oster-dev/featureforge.git
cd featureforge

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
python -m pip install pyspark
```

Verify the CLI:

```bash
featureforge --help
```

Expected commands:

```text
generate
compute-features
backfill
```

## Generate Source Data

Generate deterministic source data from the YAML configuration:

```bash
featureforge generate \
  --config configs/synthetic_data.yaml \
  --output output/source_data
```

The command writes:

```text
output/source_data/
├── users.parquet
├── content.parquet
├── events.parquet
├── labels.parquet
└── run_manifest.json
```

Generated artifacts are ignored by Git and can be recreated from configuration.

### Source Tables

| Table | Description |
|---|---|
| `users.parquet` | User entities and signup attributes |
| `content.parquet` | Content catalog entities and metadata |
| `events.parquet` | Behavioral events including duplicate and late-event flags |
| `labels.parquet` | Observation timestamps and future-activity labels |

## Compute Features

### Single User-Feature Snapshot

Compute user engagement features for one explicit observation timestamp:

```bash
featureforge compute-features \
  --input output/source_data \
  --output output/single_snapshot \
  --observation-time 2026-03-12T00:00:00+00:00 \
  --window-days 7
```

This writes:

```text
output/single_snapshot/
└── user_engagement_features.parquet
```

### Feature Views

FeatureForge currently provides two local reference feature views. Both are
implemented twice: once in Pandas (`features.py`, the correctness reference)
and once in PySpark (`spark_features.py`, parity-tested against it).

#### User Engagement Features

Computed once per `user_id`:

| Feature | Description |
|---|---|
| `event_count` | Number of in-window events |
| `unique_content_count` | Number of distinct content items interacted with |
| `total_watch_seconds` | Sum of in-window watch seconds |
| `search_count` | Number of search events |
| `play_count` | Number of play events |
| `watch_count` | Number of watch events |
| `days_since_last_activity` | Recency of the latest in-window event |

#### Content Popularity Features

Computed once per `content_id`:

| Feature | Description |
|---|---|
| `view_count` | Number of in-window content events |
| `unique_viewer_count` | Number of distinct users interacting with content |
| `total_watch_seconds` | Sum of in-window watch seconds |
| `average_watch_seconds` | Total watch seconds divided by view count |
| `search_count` | Number of search events associated with content |
| `play_count` | Number of play events |
| `watch_count` | Number of watch events |
| `days_since_last_view` | Recency of the latest in-window content event |

## Run a Backfill

Backfills build user and content feature partitions for an inclusive date range.

```bash
featureforge backfill \
  --input output/source_data \
  --output output/offline_store \
  --start-date 2026-03-10 \
  --end-date 2026-03-12 \
  --window-days 7
```

Example command output:

```text
✓ Backfilled 3 observation-date partition(s)
Date range: 2026-03-10 to 2026-03-12
Window: 7 days
Output: output/offline_store
Run manifest: output/offline_store/manifests/backfill-2026-03-10-to-2026-03-12.json
```

### Offline Feature Dataset Layout

```text
output/offline_store/
├── user_engagement_features/
│   ├── observation_date=2026-03-10/
│   │   └── features.parquet
│   ├── observation_date=2026-03-11/
│   │   └── features.parquet
│   └── observation_date=2026-03-12/
│       └── features.parquet
├── content_popularity_features/
│   ├── observation_date=2026-03-10/
│   │   └── features.parquet
│   ├── observation_date=2026-03-11/
│   │   └── features.parquet
│   └── observation_date=2026-03-12/
│       └── features.parquet
└── manifests/
    └── backfill-2026-03-10-to-2026-03-12.json
```

Each partition represents a feature snapshot for a single observation date.
The backfill runner currently executes this using the Pandas reference engine.

## PySpark Parity Layer

`src/featureforge/spark_features.py` computes the same two feature views using
PySpark DataFrames instead of Pandas. It exists to prove that feature logic is
engine-independent before Spark becomes the production execution path.

Run just the parity suite:

```bash
pytest tests/unit/test_spark_features.py -v
```

The parity tests assert that `compute_user_engagement_features_spark` and
`compute_content_popularity_features_spark` return output identical to their
Pandas counterparts across empty datasets, exact window boundaries, typed
event counts, floating-point averages, and randomized multi-entity datasets.

Building this layer surfaced a real timezone bug: Spark's `TimestampType`
silently shifted timestamps by one hour when round-tripped through the JVM on
a non-UTC machine, independent of the `spark.sql.session.timeZone` setting.
The fix encodes every timestamp as UTC epoch microseconds — a plain integer —
before it enters Spark, removing the ambiguity at the source. Full details are
in [ARCHITECTURE.md](ARCHITECTURE.md#pyspark-parity-layer).

## Temporal Semantics

### Event Time and Ingestion Time

FeatureForge models two different clocks:

| Field | Meaning |
|---|---|
| `event_time` | When the user action actually happened |
| `ingested_at` | When the platform received or processed the event |

Normal events satisfy:

```text
event_time == ingested_at
is_late == false
```

Late events satisfy:

```text
ingested_at > event_time
is_late == true
```

### Feature Window

Both feature views use this point-in-time event window:

```text
(observation_time - window_days, observation_time]
```

This means:

- events at the lower lookback boundary are excluded
- events exactly at `observation_time` are included
- events after `observation_time` are excluded

For a daily backfill, every partition is computed at UTC midnight:

```text
observation_date=2026-03-12
observation_time=2026-03-12T00:00:00+00:00
```

Therefore, the partition represents the feature state available as of that
timestamp. This boundary is enforced identically in both the Pandas and
PySpark implementations.

### Label Window

Observation labels use event time:

```text
observation_time < event_time <= label_window_end
```

Labels represent future behavior and are deliberately separated from historical
feature windows.

## Idempotency and Auditability

Backfill feature outputs use deterministic paths:

```text
<output>/<feature_view>/observation_date=YYYY-MM-DD/features.parquet
```

Running the same backfill again with the same source data, dates, window, and
code overwrites the same canonical files. It does not append duplicate part
files or create random output names.

Every backfill also writes a JSON manifest containing:

- run type
- start and completion time
- status
- date range
- lookback window
- output directory
- per-date User and Content feature counts
- concrete output paths

Example shape:

```json
{
  "run_type": "feature_backfill",
  "status": "completed",
  "start_date": "2026-03-10",
  "end_date": "2026-03-12",
  "window_days": 7,
  "partitions": [
    {
      "observation_date": "2026-03-10",
      "user_feature_count": 500,
      "content_feature_count": 250,
      "user_features_path": "...",
      "content_features_path": "..."
    }
  ]
}
```

## Quality Validation

Every source generation run performs validation before persistence.

Current checks include:

- Referential integrity: all user and content references exist.
- Temporal validity: `ingested_at >= event_time`.
- Event semantics: search events have no content reference; play/watch events
  have positive watch duration.
- Late-event semantics: late events have an actual positive ingestion delay.
- Label validity: label windows end after their observation timestamps.
- Volume validation: event and label counts match configured expectations.

Generation results are included in `run_manifest.json`.

## Validation

Run the complete test suite:

```bash
pytest -v
```

Run format and lint checks:

```bash
ruff format --check .
ruff check .
```

Or use Make targets:

```bash
make validate
make lint
make test
make docker-config
```

## Local Infrastructure

FeatureForge includes Redis through Docker Compose for the future online-store
stage.

Start Redis:

```bash
make docker-up
```

Verify it:

```bash
docker exec featureforge-redis redis-cli ping
```

Expected output:

```text
PONG
```

Stop it:

```bash
make docker-down
```

## Roadmap

### Completed Foundation

- Deterministic synthetic source-data generation.
- Executable source-data contracts.
- Quality validation.
- Parquet source datasets.
- User engagement feature contract and computation.
- Content popularity feature contract and computation.
- Point-in-time feature windows.
- Partitioned feature datasets.
- Parameterized, idempotent local backfills.
- Generation and backfill audit manifests.
- CLI and test foundation.
- PySpark implementation of both feature views.
- Parity tests between the Pandas reference implementation and Spark output.

### Next Steps

1. Read source Parquet directly as Spark DataFrames instead of constructing
   them from an in-memory `SyntheticDataset`.
2. Add an engine parameter to the backfill runner so backfills can execute on
   either the Pandas or the Spark implementation, with the manifest recording
   which engine produced each partition.
3. Create Feast entities, batch sources, feature views, and a feature service.
4. Build point-in-time historical retrieval against observation labels.
5. Add Redis materialization and online feature lookup.
6. Add online/offline parity tests.
7. Add quality gates before materialization.
8. Add freshness checks, richer run manifests, failure simulations, and runbooks.
9. Document the AWS S3 and DynamoDB production profile.
10. Add GitHub Actions CI and a one-command demo.

## Project Scope

Version 1 focuses on:

- event-time-aware feature computation
- offline and online feature separation
- point-in-time historical retrieval
- reproducible backfills
- online materialization
- data-quality validation
- freshness monitoring
- tests, CI, and operational documentation

Kafka, Flink, Kubernetes, Terraform-heavy infrastructure, and complex model
training are intentionally outside the initial version of FeatureForge.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md).

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.
