# Contributing to FeatureForge

Thank you for contributing to FeatureForge.

FeatureForge is a production-inspired project for Data Infrastructure, Feature
Infrastructure, and ML Platform Engineering. The project prioritizes
reproducibility, event-time correctness, testability, data contracts, and
clear operational behavior.

## Development Setup

Create and activate a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install the package with development dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Install PySpark for the parity-tested feature-computation engine:

```bash
python -m pip install pyspark
```

Verify the CLI:

```bash
featureforge --help
```

## Local Infrastructure

FeatureForge includes Redis through Docker Compose for the later online-serving
stage.

Start Redis:

```bash
make docker-up
```

Stop Redis:

```bash
make docker-down
```

Check the Redis service:

```bash
docker exec featureforge-redis redis-cli ping
```

Expected output:

```text
PONG
```

## Local Development Flows

### Generate Synthetic Source Data

Generate deterministic source Parquet datasets:

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

### Compute a Single User Feature Snapshot

The existing single-snapshot command computes user engagement features at one
explicit observation timestamp:

```bash
featureforge compute-features \
  --input output/source_data \
  --output output/single_snapshot \
  --observation-time 2026-03-12T00:00:00+00:00 \
  --window-days 7
```

### Run a Feature Backfill

Run a point-in-time user and content feature backfill over an inclusive date
range:

```bash
featureforge backfill \
  --input output/source_data \
  --output output/offline_store \
  --start-date 2026-03-10 \
  --end-date 2026-03-12 \
  --window-days 7
```

The output layout is deterministic:

```text
output/offline_store/
├── user_engagement_features/
│   └── observation_date=YYYY-MM-DD/
│       └── features.parquet
├── content_popularity_features/
│   └── observation_date=YYYY-MM-DD/
│       └── features.parquet
└── manifests/
    └── backfill-YYYY-MM-DD-to-YYYY-MM-DD.json
```

Backfills overwrite the same canonical feature partitions when invoked with
the same input data, date range, window, and code. This provides the current
V1 idempotency behavior. The backfill runner currently executes on the Pandas
reference engine.

### Run the PySpark Parity Suite

`src/featureforge/spark_features.py` computes the same two feature views as
`features.py` using PySpark instead of Pandas. Before changing either
implementation, run the parity suite:

```bash
pytest tests/unit/test_spark_features.py -v
```

If you modify feature logic in `features.py`, the equivalent change must also
be made in `spark_features.py`, and the parity suite must still pass. A change
that passes `test_features.py` but breaks `test_spark_features.py` is not
complete — the two engines are required to stay provably identical.

When working with timestamps inside `spark_features.py`, never pass a Python
`datetime` directly into a Spark `TimestampType` column and never rely on
`spark.sql.session.timeZone` alone to guarantee UTC correctness. Convert to
UTC epoch microseconds (`_to_epoch_micros`) before the value enters Spark, and
convert back (`_from_epoch_micros`) only after `collect()`. This project hit a
real one-hour timezone bug from skipping this step; see
[ARCHITECTURE.md](ARCHITECTURE.md#pyspark-parity-layer) for the full story.

## Validation Before a Commit

Run all checks before opening a pull request or creating a commit:

```bash
ruff format --check .
ruff check .
pytest -v
```

You may also use the existing Make targets where appropriate:

```bash
make validate
make lint
make test
make docker-config
```

All relevant checks should pass before a pull request is opened.

## Testing Guidelines

Every behavior change should include an appropriate test.

Use `tests/unit/` for isolated contracts and domain logic:

- Pydantic validation
- configuration validation
- synthetic-data generation
- feature calculations
- date-range behavior
- partition-path behavior
- manifest content
- idempotency at the backfill-function level
- Pandas-vs-PySpark parity for feature calculations

Use `tests/integration/` for executable multi-component paths:

- CLI argument parsing and execution
- source-Parquet read/write flow
- CLI-to-backfill-to-partitioned-Parquet flow
- CLI-to-manifest flow
- end-to-end idempotency behavior

Do not remove a temporal, quality, idempotency, or parity test merely to make
a failing suite pass. Understand and fix the underlying contract violation.

## Development Principles

- Keep changes small and focused.
- Prefer explicit, readable code over clever abstractions.
- Keep business feature logic independent from CLI and filesystem code.
- Preserve event-time correctness.
- Make time windows explicit and tested.
- Keep generation and transformations deterministic.
- Prefer idempotent writes for backfills and materialization.
- Add tests for new behavior and failure modes.
- Keep the Pandas reference and PySpark engine provably equivalent, not just
  similar.
- Never trust implicit timezone handling across a process or JVM boundary;
  encode time as UTC epoch integers at those boundaries instead.
- Update documentation when architecture or behavior changes.
- Keep data contracts versioned and reviewable.
- Never commit credentials, private data, or generated local artifacts.

## Generated Data

Generated outputs are intentionally ignored by Git:

```text
output/
*.parquet
```

Do not commit:

- generated Parquet datasets
- generated run manifests under `output/`
- virtual environments
- cache directories
- credentials
- `.env` files
- local editor settings unless the change is intentionally project-wide

Synthetic source data can be regenerated from YAML configuration. Backfill
outputs can be regenerated from source data and explicit date parameters.

## Commit Messages

Use short, imperative Conventional Commit-style messages:

```text
feat: add idempotent partitioned feature backfills
feat: add point-in-time content popularity features
feat: add PySpark parity layer for point-in-time features
fix: prevent future events from entering feature windows
fix: convert Spark timestamps to UTC epoch micros to avoid timezone drift
test: add backfill manifest coverage
test: add Pandas-vs-PySpark feature parity suite
docs: document offline feature partition layout
chore: ignore generated pipeline outputs
```

A good commit should represent one coherent change. Avoid mixing unrelated
refactors, generated data, formatting-only changes, and functional changes in
one commit.

## Pull Requests

A pull request should explain:

1. What changed.
2. Why the change was needed.
3. How it was tested.
4. Which data, feature, temporal, idempotency, or parity contract is affected.
5. Whether an architecture decision changed.
6. Whether documentation was updated.
7. Any backward-compatibility or migration concern.

For changes affecting feature computation, include:

- the entity key,
- the feature window semantics,
- behavior for missing activity,
- validation rules,
- expected partitioning behavior,
- tests for time-boundary cases where applicable,
- confirmation that Pandas and PySpark outputs still match, where both
  engines implement the affected feature.

## Data and Privacy

FeatureForge must use synthetic or publicly distributable data only.

Never commit:

- credentials
- private customer data
- access tokens
- API keys
- local environment files
- production identifiers
- personally identifiable information

## Code of Conduct

Contributors should communicate respectfully, review changes constructively, and
prioritize correctness over speed. The project values clear ownership,
documented trade-offs, and reliable engineering practices.
