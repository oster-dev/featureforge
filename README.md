# FeatureForge

FeatureForge is a production-inspired feature platform for reproducible offline
training data and low-latency online ML feature serving.

It is built as a portfolio project for Data Infrastructure, Feature
Infrastructure, and ML Platform Engineering. The project focuses on the
hard parts that make feature platforms trustworthy:

- deterministic data generation (independent and behavioral modes)
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
- Feast integration for historical retrieval and online serving
- ML-ready training pipelines with time-based evaluation
- Full and incremental online materialization into Redis
- Online feature lookup and deterministic ranking
- One-command end-to-end platform demonstration

## Project Goal

The long-term goal is to provide trusted, versioned, point-in-time-correct
features for both offline model training and online inference.

The target architecture will use:

- PySpark for scalable batch feature computation
- S3-backed Parquet for the offline feature store
- Feast for feature definitions, historical retrieval, and online serving
- Redis for local online serving
- DynamoDB as the documented AWS online-store profile
- GitHub Actions for continuous integration
- sklearn / XGBoost for baseline and advanced models

The current implementation establishes the local reference foundation needed
to build those components correctly, and now includes a parity-tested PySpark
execution engine alongside the original Pandas reference, plus a complete
ML training pipeline with historical retrieval and online serving.

## Current Status

### Implemented

- Pydantic contracts for users, content, events, observation labels, feature
  records, feature batches, and synthetic-data configuration
- YAML-backed synthetic-data configuration with validation for time ranges,
  event rates, late-arrival constraints, and event generation mode
- Deterministic synthetic user, content, event, duplicate-delivery, late-event,
  and observation-label generation
- Behavioral mode with persistent per-user activity weights
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
- Feast integration:
  - Entities (`user`, `content`)
  - Batch sources (FileSource from partitioned Parquet)
  - Feature views (7-day lookback windows)
  - Feature services
  - Historical retrieval demo
  - Full and incremental materialization into Redis
  - Online feature lookup
- ML training pipeline:
  - Historical retrieval producing `data/historical_features.parquet`
  - Time-based train/val/test splits
  - sklearn Pipeline (StandardScaler + LogisticRegression)
  - Persisted model and metrics JSON
  - Diagnostic analysis script
- Online serving:
  - Redis local online store via Docker Compose
  - Online feature lookup for user engagement and content popularity
  - Deterministic content ranking with transparent scoring
  - End-to-end demo with `make demo`
- Local Redis service through Docker Compose for the online-serving stage
- Unit and integration tests
- Ruff formatting and linting

### Current Quality Gate

The current repository state passes:

```text
ruff format --check .
ruff check .
pytest -v

121 passed
```

## Current Data Flow

```text
validated YAML configuration
        ↓
deterministic synthetic data generation (independent or behavioral mode)
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
Feast FileSource configuration
        ↓
historical retrieval with point-in-time joins
        ↓
time-based train/val/test split
        ↓
sklearn Pipeline training
        ↓
persisted model + metrics JSON
        ↓
date-parameterized idempotent backfills
        ↓
Feast full or incremental materialization into Redis
        ↓
online feature lookup
        ↓
deterministic content ranking
        ↓
JSON run manifests for generation, backfill, and materialization
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
| Local online store | Redis via Docker Compose | Low-latency feature serving |
| Feature platform | Feast | Feature definitions, historical retrieval, materialization, online serving |
| ML training | sklearn | Baseline model with Pipeline persistence |
| Planned cloud profile | AWS S3 and DynamoDB | Offline and online production-oriented storage |

## Quick Start

### Prerequisites

- Python 3.11, 3.12, or 3.13
- Docker Desktop
- GNU Make

### Clone and Install

```bash
git clone [https://github.com/oster-dev/featureforge.git](https://github.com/oster-dev/featureforge.git)
cd featureforge

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
python -m pip install pyspark
python -m pip install feast
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
materialize
materialize-incremental
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

### Behavioral Mode

The default configuration uses behavioral mode, which introduces persistent
latent activity propensity per user:

- Each user receives a fixed `activity_weight` sampled once at generation time
- Event counts are drawn from `Poisson(activity_weight * base_rate)`
- Users with higher weights generate more events consistently
- This creates predictable engagement heterogeneity without label leakage

Result: historical engagement features gain genuine predictive signal for
future activity (test AUC ~0.70, Precision@25 ~0.84).

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

## Feast Integration

FeatureForge integrates with Feast for production-ready feature serving and
historical retrieval.

### Setup

Ensure Feast is installed:

```bash
python -m pip install feast
```

### Historical Retrieval

Run the historical retrieval demo:

```bash
python feature_repo/historical_retrieval_demo.py
```

This produces:

```text
data/historical_features.parquet
```

Contains:

- 893 point-in-time-correct feature rows (from 1,000 input labels)
- 56.2% positive rate
- Time range: 2026-01-10 to 2026-03-24
- All user engagement features with full feature names

### Feature Repository Structure

```text
feature_repo/
├── entities.py           # User and Content entities
├── sources.py            # FileSource definitions
├── feature_views.py      # Feature view definitions
├── feature_services.py   # Feature service definitions
└── historical_retrieval_demo.py  # Historical retrieval demo
```

### Materialization

Run full materialization with an explicit UTC time range:

```bash
featureforge materialize \
  --repo feature_repo \
  --start-time 2026-03-20T00:00:00+00:00 \
  --end-time 2026-03-25T00:00:00+00:00 \
  --manifest-output output
```

Run incremental materialization using Feast's stored watermark:

```bash
featureforge materialize-incremental \
  --repo feature_repo \
  --end-time 2026-03-25T00:00:00+00:00 \
  --manifest-output output
```

## ML Training Pipeline

FeatureForge includes a complete ML training pipeline for baseline model
development.

### Model Training

Train the baseline model:

```bash
python scripts/train_baseline.py
```

This produces:

```text
output/models/baseline_logreg_pipeline.joblib
output/models/baseline_logreg_metrics.json
```

### Training Results

Typical results (behavioral mode):

| Split | Rows | Positive Rate | AUC | Precision@25 | Recall@25 |
|---|---:|---:|---:|---:|---:|
| Train | 625 | 57.4% | 0.7725 | 1.00 | 0.0696 |
| Validation | 134 | 56.7% | 0.7688 | 0.88 | 0.2895 |
| Test | 134 | 50.0% | 0.6973 | 0.84 | 0.3134 |

### Diagnostic Analysis

Run diagnostic analysis:

```bash
python scripts/diagnose_baseline.py
```

Provides:

- Feature distribution analysis
- Train vs test drift detection
- Coefficient stability checks
- Precision/Recall curves
- Calibration analysis

## Online Serving

FeatureForge provides online feature serving through Feast and Redis.

### Start Redis

```bash
docker compose up -d
```

Verify it:

```bash
docker exec featureforge-redis redis-cli ping
```

Expected output:

```text
PONG
```

### Online Feature Lookup

Run the online feature lookup demo for a specific user:

```bash
python feature_repo/online_lookup_demo.py --user-id user_000290
```

This retrieves current feature values from Feast and displays them.

### Personalization Ranking

Run the deterministic content ranking demo:

```bash
python feature_repo/personalization_demo.py \
  --user-id user_000290 \
  --top-k 5
```

This retrieves user engagement features, evaluates candidate content, and
returns a ranked list.

### End-to-End Platform Demo

Run the complete local workflow:

```bash
docker compose up -d
make demo
```

This executes:

```text
generate
  → backfill
  → Feast full materialization into Redis
  → online feature lookup
  → deterministic content ranking
```

Customize the demo:

```bash
make demo USER_ID=user_000290 TOP_K=5
make demo OUTPUT_DIR=output_demo
```

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

Materialization runs also write JSON manifests under:

```text
<manifest-output>/materialization_manifests/
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

FeatureForge includes Redis through Docker Compose for the online-serving
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
- Feast integration with historical retrieval.
- ML training pipeline with time-based evaluation.
- Behavioral mode with persistent activity weights.
- Feast full and incremental materialization.
- Online feature lookup and deterministic ranking.
- End-to-end platform demo with `make demo`.

### Next Steps

1. Read source Parquet directly as Spark DataFrames instead of constructing
   them from an in-memory `SyntheticDataset`.
2. Add an engine parameter to the backfill runner so backfills can execute on
   either the Pandas or the Spark implementation, with the manifest recording
   which engine produced each partition.
3. Implement pre-materialization quality gates.
4. Add freshness checks, richer run manifests, failure simulations, and runbooks.
5. Document the AWS S3 and DynamoDB production profile.
6. Add GitHub Actions CI.
7. Implement hyperparameter tuning and advanced models (XGBoost, LightGBM).
8. Add MLflow experiment tracking and model registry.

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
- ML-ready training pipelines

Kafka, Flink, Kubernetes, Terraform-heavy infrastructure, and complex model
training are intentionally outside the initial version of FeatureForge.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md).

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.