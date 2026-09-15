# FeatureForge

FeatureForge is a production-inspired feature platform for reproducible offline
training data and low-latency online ML feature serving.

The project starts with a deterministic synthetic-data foundation: it generates
behavioral event data, intentionally models duplicate and late-arriving events,
creates future-activity labels, and persists the result as typed Parquet tables.

FeatureForge is built as a portfolio project for Data Infrastructure, Feature
Infrastructure, and ML Platform Engineering.

## Project Goal

The long-term goal is to provide trusted, versioned, point-in-time-correct
features for both offline model training and online inference.

The current implementation delivers the dataset foundation required for that
goal:

```text
validated YAML configuration
        ↓
deterministic synthetic data generation
        ↓
duplicate and late-event injection
        ↓
observation-label generation
        ↓
Parquet offline datasets
        ↓
reproducible CLI execution
```

## Current Status

### Implemented

- Pydantic contracts for users, content, events, observation labels, and the
  complete synthetic dataset
- YAML-backed configuration with validation for time ranges, event rates, and
  late-arrival constraints
- Deterministic synthetic user and content generation
- Deterministic behavioral event generation
- Controlled duplicate-event delivery injection
- Controlled late-event injection with separate event and ingestion timestamps
- Observation labels for future user activity
- In-memory dataset orchestration
- Parquet persistence for users, content, events, and labels
- Command-line dataset generation
- Unit, persistence, and CLI integration tests
- Local Redis service through Docker Compose for future online-serving work

### Planned

1. PySpark feature transformations.
2. Point-in-time-correct feature computation.
3. Feature definitions and historical retrieval with Feast.
4. Offline and online feature-store integration.
5. Redis materialization and online feature lookup.
6. Data-quality and freshness checks.
7. AWS S3 and DynamoDB production profile.
8. GitHub Actions continuous integration.

## Core Architecture

- Python for platform and pipeline code
- Pydantic for executable data contracts
- Pandas and PyArrow for local Parquet persistence
- YAML for reproducible synthetic-data configuration
- Rich for CLI output
- Redis through Docker Compose for future local online serving
- pytest for unit and integration tests
- Ruff for formatting and linting
- PySpark, Feast, AWS S3, DynamoDB, and GitHub Actions as planned extensions

## Generated Dataset

Run the generator with:

```bash
featureforge generate \
  --config configs/synthetic_data.yaml \
  --output data/generated
```

The command writes four Parquet tables:

| Table | Description |
|---|---|
| `users.parquet` | User entities and signup attributes |
| `content.parquet` | Content catalog entities and metadata |
| `events.parquet` | Behavioral events, including duplicate and late-event flags |
| `labels.parquet` | User observation timestamps and future-activity labels |

Generated artifacts are ignored by Git and can be recreated at any time from
the YAML configuration.

## Quality Validation

Every generation run includes automatic quality validation:

- **Referential integrity**: All user and content references exist
- **Temporal validity**: `ingested_at >= event_time` for all events
- **Event semantics**: Search events have no content reference; play/watch have positive duration
- **Late-event semantics**: Late events have positive delay between event and ingestion time
- **Label validity**: Label windows end after observation time
- **Volume validation**: Event and label counts match expected values

Quality results are available in `run_manifest.json`:

```json
{
  "quality_report": {
    "passed": true,
    "unknown_event_user_reference_count": 0,
    "unknown_event_content_reference_count": 0,
    "invalid_search_content_reference_count": 0,
    "invalid_watch_semantics_count": 0,
    "invalid_event_time_order_count": 0,
    "invalid_late_event_count": 0,
    "unknown_label_user_reference_count": 0,
    "invalid_label_window_count": 0,
    "duplicate_count_matches_expected": true,
    "late_event_count_matches_expected": true,
    "event_count_matches_expected": true,
    "label_count_matches_expected": true
  }
}
```

## Run Manifest

Every generation run produces a `run_manifest.json` for auditability:

```json
{
  "generated_at": "2026-09-15T09:13:06.952601+00:00",
  "config": { ... },
  "row_counts": {
    "users": 500,
    "content": 250,
    "events": 10200,
    "labels": 1000
  },
  "quality_report": { ... },
  "output_paths": {
    "users": "output/users.parquet",
    "content": "output/content.parquet",
    "events": "output/events.parquet",
    "labels": "output/labels.parquet"
  }
}
```

The manifest captures the full configuration, row counts, quality report, and output paths for reproducibility.

### Example output

```text
FeatureForge dataset generated

users      500  data/generated/users.parquet
content    250  data/generated/content.parquet
events   10200  data/generated/events.parquet
labels    1000  data/generated/labels.parquet

Duplicate events: 200
Late events: 306
```

The exact counts are controlled by `configs/synthetic_data.yaml`.

## Data Semantics

### Event time and ingestion time

FeatureForge explicitly models two timestamps:

| Field | Meaning |
|---|---|
| `event_time` | When the user action actually occurred |
| `ingested_at` | When the platform received or processed the event |

For normal base events:

```text
ingested_at == event_time
is_late == false
```

For late events:

```text
ingested_at > event_time
is_late == true
```

This distinction is required for later point-in-time-correct feature
computation, late-data handling, backfills, and feature freshness monitoring.

### Duplicate events

Duplicate events represent an additional delivery of an existing behavioral
event.

```text
original event:
event_id=event_00000042
is_duplicate=false

duplicate delivery:
event_id=event_00000042_duplicate_01
is_duplicate=true
```

The duplicate preserves the original behavioral payload while receiving its own
delivery identifier.

### Observation labels

Each label answers the following question:

> Did this user have at least one event during the configured future label
> window?

The label is calculated using event time:

```text
observation_time < event_time <= label_window_end
```

This makes the label suitable for later point-in-time-safe training-dataset
construction.

## Development Setup

### Prerequisites

- Python 3.11, 3.12, or 3.13
- Docker Desktop
- GNU Make

### Clone and install

```bash
git clone [https://github.com/oster-dev/featureforge.git](https://github.com/oster-dev/featureforge.git)
cd featureforge

python3 -m venv .venv
source .venv/bin/activate

python -m pip install --upgrade pip
pip install -e ".[dev]"
```

Verify the CLI:

```bash
featureforge --help
```

## Validation

Run the complete test suite:

```bash
pytest -v
```

Run formatting and lint checks:

```bash
ruff format --check src tests
ruff check src tests
```

Or use the project Make targets:

```bash
make validate
make lint
make test
make docker-config
```

## Local Infrastructure

FeatureForge includes a local Redis service for the later online feature-store
stage.

Start Redis:

```bash
make docker-up
```

Verify connectivity:

```bash
docker exec featureforge-redis redis-cli ping
```

Expected output:

```text
PONG
```

Stop local infrastructure:

```bash
make docker-down
```

## Project Scope

Version 1 focuses on:

- Event-time-aware feature computation
- Offline and online feature separation
- Point-in-time-correct historical retrieval
- Reproducible backfills
- Online materialization
- Data-quality validation
- Freshness monitoring
- Tests, CI, and operational documentation

Kafka, Flink, Kubernetes, and complex model training are intentionally outside
the first version of this project.

## License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for
details.