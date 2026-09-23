# Contributing to FeatureForge

Thank you for contributing to FeatureForge.

FeatureForge is a production-inspired project for Data Infrastructure, Feature
Infrastructure, and ML Platform Engineering. The project prioritizes
reproducibility, event-time correctness, testability, data contracts, explicit
operational behavior, and one canonical offline feature-store contract.

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

Install Feast for historical retrieval and feature serving:

```bash
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

## Local Infrastructure

FeatureForge includes Redis through Docker Compose for the online-serving stage.

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

Apply Feast definitions after Redis is running:

```bash
cd feature_repo
feast apply
cd ..
```

## Canonical Offline Store

FeatureForge V1 has one canonical local offline feature-store root:

```text
output/offline_store/
```

The relevant partitioned layout is:

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

This is a cross-component contract, not just a default directory:

```text
Backfill output
    =
Persisted offline feature-quality validation input
    =
Feast FileSource input
    =
Materialization source
    =
Online/offline serving-parity source
```

Do not change Feast source paths, materialization behavior, backfill output
ownership, or parity-test locations independently. A change to this contract
requires coordinated code, tests, documentation, and usually an ADR update.

Relevant decisions:

- [ADR-001: Offline/Online Feature Store Split](docs/adr/ADR-001-offline-online-feature-store-split.md)
- [ADR-002: Canonical Offline Store Contract](docs/adr/ADR-002-canonical-offline-store-contract.md)

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

The single-snapshot command computes user engagement features at one explicit
observation timestamp:

```bash
featureforge compute-features \
  --input output/source_data \
  --output output/single_snapshot \
  --observation-time 2026-03-12T00:00:00+00:00 \
  --window-days 7
```

### Run a Canonical Feature Backfill

Run a point-in-time user and content feature backfill over an inclusive date
range. Use `output/offline_store` for the local Feast-serving workflow:

```bash
featureforge backfill \
  --input output/source_data \
  --output output/offline_store \
  --start-date 2026-03-10 \
  --end-date 2026-03-12 \
  --window-days 7 \
  --engine pandas
```

The Spark engine is also supported:

```bash
featureforge backfill \
  --input output/source_data \
  --output output/offline_store \
  --start-date 2026-03-10 \
  --end-date 2026-03-12 \
  --window-days 7 \
  --engine spark
```

Backfills overwrite the same canonical feature partitions when invoked with the
same input data, date range, lookback window, engine, and code. This provides
the local V1 idempotency behavior.

The backfill library may write to isolated paths for unit tests and local
experiments. Materialization and the local Feast FileSources use only the
canonical `output/offline_store/` contract.

### Validate Persisted Offline Features

Persisted feature-store validation is a library-level concern. It verifies
partitioned offline feature data before the FeatureForge materialization
workflow invokes Feast.

The application materialization boundary always validates:

```text
output/offline_store/
```

Do not introduce an independent materialization source path unless Feast
FileSources are configured from exactly the same resolved location.

### Run Feast Materialization

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

The materialization commands deliberately do not accept an
`--offline-store-dir` flag. This prevents a split-brain condition where the
quality gate validates one directory while Feast reads another.

The workflow is:

```text
output/offline_store/
        ↓
persisted offline feature-quality validation
        ↓
passed
        ↓
Feast materialization
        ↓
Redis online store
```

When validation fails:

```text
output/offline_store/
        ↓
persisted offline feature-quality validation
        ↓
failed checks
        ↓
materialization blocked
        ↓
blocked materialization manifest
        ↓
no Feast write is invoked
```

### Materialization Manifests

Completed and blocked materialization attempts write JSON manifests under:

```text
output/materialization_manifests/
```

A manifest records:

- Run type
- Status: `completed` or `blocked`
- Full or incremental mode
- Feast repository path
- Canonical offline-store path
- Requested time range
- Execution timestamps
- Persisted-feature quality report
- Failed check names for blocked runs

Inspect the latest manifests during debugging:

```bash
ls -lt output/materialization_manifests/
```

### Run Historical Retrieval

Run the Feast historical retrieval demo:

```bash
python feature_repo/historical_retrieval_demo.py
```

This produces:

```text
data/historical_features.parquet
```

It contains point-in-time-correct training features joined with observation
labels.

### Train Baseline Model

Train the sklearn baseline model:

```bash
python scripts/train_baseline.py
```

This produces:

```text
output/models/baseline_logreg_pipeline.joblib
output/models/baseline_logreg_metrics.json
```

### Run Diagnostic Analysis

Analyze model performance and feature distributions:

```bash
python scripts/diagnose_baseline.py
```

### Run Online Feature Lookup

Run the online feature lookup demo for a specific user:

```bash
python feature_repo/online_lookup_demo.py --user-id user_000290
```

This retrieves current materialized feature values through Feast.

### Run Personalization Ranking

Run the deterministic content-ranking demo:

```bash
python feature_repo/personalization_demo.py \
  --user-id user_000290 \
  --top-k 5
```

This retrieves online user engagement features, evaluates candidate content,
and returns a deterministic ranked list.

### Run the End-to-End Platform Demo

Start Redis and run the complete local workflow:

```bash
docker compose up -d
make demo
```

The demo executes:

```text
generate
  → canonical backfill into output/offline_store
  → persisted offline feature-quality validation
  → Feast full materialization into Redis
  → user online-feature lookup
  → deterministic content-candidate ranking
  → offline/online serving parity checks
```

Customize the demo:

```bash
make demo USER_ID=user_000290 TOP_K=5
make demo OUTPUT_DIR=output_demo
```

`OUTPUT_DIR` controls generated source data and run artifacts. It does not
replace the canonical local Feast source:

```text
output/offline_store/
```

### Run the PySpark Parity Suite

`src/featureforge/spark_features.py` computes the same two feature views as
`features.py` using PySpark instead of Pandas. Before changing either
implementation, run both parity suites:

```bash
pytest tests/unit/test_spark_features.py -v
pytest tests/unit/test_spark_parquet_features.py -v
```

If you modify feature logic in `features.py`, the equivalent change must also
be made in `spark_features.py`, and both parity suites must still pass. A
change that passes `test_features.py` but breaks Spark parity is not complete.

When working with timestamps inside `spark_features.py`, never pass a Python
`datetime` directly into a Spark `TimestampType` column and never rely on
`spark.sql.session.timeZone` alone to guarantee UTC correctness. Convert to UTC
epoch microseconds with `_to_epoch_micros` before values enter Spark, and
convert back with `_from_epoch_micros` only after `collect()`.

The project previously encountered a real one-hour timezone bug by skipping
this pattern. See [ARCHITECTURE.md](ARCHITECTURE.md#pandas-and-pyspark-parity)
for the design rationale.

## Validation Before a Commit

Run all checks before opening a pull request or creating a commit:

```bash
ruff format --check .
ruff check .
pytest -v
```

You may also use existing Make targets:

```bash
make validate
make lint
make test
make docker-config
```

For focused validation of the canonical-store and materialization contract:

```bash
pytest tests/unit/test_feature_quality.py -v
pytest tests/unit/test_materialization.py -v
pytest tests/integration/test_online_serving.py -v
```

All relevant checks should pass before a pull request is opened.

Example current validation outcome:

```text
ruff format --check .
All files already formatted

ruff check .
All checks passed!

pytest tests/unit/ -v
122 passed

pytest tests/integration/ -v
7 passed
```

## Testing Guidelines

Every behavior change should include an appropriate test.

Use `tests/unit/` for isolated contracts and domain logic:

- Pydantic validation
- Configuration validation
- Synthetic-data generation in independent and behavioral modes
- Feature calculations
- Date-range behavior
- Partition-path behavior
- Generation, backfill, and materialization manifest content
- Backfill idempotency at the function level
- Pandas-vs-PySpark feature parity
- Persisted offline feature-quality validation
- Feast materialization timestamp contracts
- Canonical materialization source enforcement
- Blocked materialization behavior and manifest content
- Completed materialization behavior and manifest content
- Full and incremental materialization gate behavior
- Online feature lookup behavior
- Missing online entities
- Online ranking behavior and stable tie-breaking

Use `tests/integration/` for executable multi-component paths:

- CLI argument parsing and execution
- Source-Parquet read/write flow
- CLI-to-backfill-to-partitioned-Parquet flow
- CLI-to-manifest flow
- End-to-end idempotency behavior
- Generate → backfill → retrieval → training flow
- Generate → canonical backfill → materialize → online lookup → ranking flow
- Redis-backed online serving behavior
- Offline/online feature parity against the latest canonical partition

Do not remove a temporal, quality, idempotency, canonical-source, or parity
test merely to make a failing suite pass. Understand and fix the underlying
contract violation.

## Development Principles

- Keep changes small and focused.
- Prefer explicit, readable code over clever abstractions.
- Keep business feature logic independent from CLI and filesystem code.
- Preserve event-time correctness.
- Make time windows explicit and tested.
- Keep generation and transformations deterministic.
- Prefer idempotent writes for backfills and materialization.
- Keep the local canonical offline-store contract consistent.
- Validate persisted offline partitions before online materialization.
- Do not allow validation and Feast materialization to use unrelated paths.
- Add tests for new behavior and failure modes.
- Keep the Pandas reference and PySpark engine provably equivalent, not merely
  similar.
- Never trust implicit timezone handling across a process or JVM boundary;
  encode time as UTC epoch integers at those boundaries instead.
- Update documentation and ADRs when architecture or behavior changes.
- Keep data contracts versioned and reviewable.
- Never commit credentials, private data, or generated local artifacts.
- Behavioral mode must produce genuine predictive signal without label leakage.
- Historical retrieval must enforce point-in-time correctness.
- Training pipelines must persist preprocessing with models.
- Materialization timestamps must be explicit and timezone-aware.
- Materialization must validate the canonical Feast source before Redis writes.
- Online feature lookup and ranking must use only materialized Feast values.
- Online values must remain parity-checkable against canonical offline
  snapshots.
- Ranking tie-breakers must be stable and deterministic.

## Generated Data

Generated outputs are intentionally ignored by Git:

```text
output/
*.parquet
data/
```

Do not commit:

- Generated Parquet datasets
- Generated run manifests under `output/`
- Virtual environments
- Cache directories
- Credentials
- `.env` files
- Local editor settings unless the change is intentionally project-wide
- Trained model artifacts that can be regenerated

Synthetic source data can be regenerated from YAML configuration. Backfill
outputs can be regenerated from source data and explicit date parameters.
Training datasets and models can be regenerated from the retrieval and training
scripts.

## Commit Messages

Use short, imperative Conventional Commit-style messages:

```text
feat: add idempotent partitioned feature backfills
feat: add point-in-time content popularity features
feat: add PySpark parity layer for point-in-time features
feat: add Feast integration with historical retrieval
feat: add ML training pipeline with time-based evaluation
feat: add behavioral mode with persistent activity weights
feat: add Feast full and incremental materialization
feat: add online feature lookup and deterministic ranking
feat: add end-to-end platform demo with make demo
feat: add pre-materialization feature quality gate
fix: prevent future events from entering feature windows
fix: convert Spark timestamps to UTC epoch micros to avoid timezone drift
fix: align E2E serving flow with canonical offline store
fix: enforce canonical offline store for materialization
test: add backfill manifest coverage
test: add Pandas-vs-PySpark feature parity suite
test: add historical retrieval point-in-time tests
test: add materialization timestamp contract tests
test: add offline feature quality gate coverage
test: add online lookup and ranking behavior tests
docs: document offline feature partition layout
docs: add ADR for offline-online feature store split
docs: define canonical offline store contract
docs: document canonical offline store quality gate
chore: ignore generated pipeline outputs
chore: ignore trained model artifacts
```

A good commit represents one coherent change. Avoid mixing unrelated refactors,
generated data, formatting-only changes, and functional changes in one commit.

## Pull Requests

A pull request should explain:

1. What changed.
2. Why the change was needed.
3. How it was tested.
4. Which data, feature, temporal, idempotency, quality, or parity contract is
   affected.
5. Whether the canonical offline-store contract is affected.
6. Whether an architecture decision changed.
7. Whether documentation was updated.
8. Any backward-compatibility or migration concern.

For changes affecting feature computation, include:

- The entity key.
- The feature-window semantics.
- Behavior for missing activity.
- Validation rules.
- Expected partitioning behavior.
- Tests for time-boundary cases where applicable.
- Confirmation that Pandas and PySpark outputs still match, where both engines
  implement the affected feature.

For changes affecting the ML pipeline, include:

- Historical retrieval correctness verification.
- Time-based split behavior and confirmation of no temporal leakage.
- Metric changes and interpretation.
- Reproducibility verification: same seed produces the same results.
- Artifact-persistence behavior.

For changes affecting Feast serving or materialization, include:

- Materialization timestamp contracts.
- Full versus incremental materialization behavior.
- Canonical offline-store source behavior.
- Persisted offline feature-quality-gate behavior.
- Blocked and completed manifest behavior.
- Online lookup behavior for present and missing entities.
- Serving parity behavior, if relevant.
- Ranking score computation and tie-breaking behavior, if relevant.

## Data and Privacy

FeatureForge must use synthetic or publicly distributable data only.

Never commit:

- Credentials
- Private customer data
- Access tokens
- API keys
- Local environment files
- Production identifiers
- Personally identifiable information

## Code of Conduct

Contributors should communicate respectfully, review changes constructively, and
prioritize correctness over speed. The project values clear ownership,
documented trade-offs, and reliable engineering practices.

## Documentation Updates

When adding significant features or changing architecture:

1. Update `README.md` with new commands, contracts, or flows.
2. Update `ARCHITECTURE.md` with new components, data flows, or guarantees.
3. Add or update an ADR in `docs/adr/` when a durable architectural decision
   changes.
4. Update inline code documentation where behavior changes.
5. Add or update docstrings for public functions and classes.
6. Verify all code examples in documentation still execute correctly.
7. Confirm no documentation implies a quality gate, Feast source, or
   materialization source path different from the canonical contract.

## Debugging Tips

### Temporal Correctness Issues

If you suspect temporal leakage:

1. Check event-window boundaries in feature calculations.
2. Verify label windows use `event_time`, not `ingested_at`.
3. Confirm historical retrieval uses point-in-time joins.
4. Inspect train/validation/test split timestamps for overlap.

### PySpark Parity Failures

If parity tests fail:

1. Check timestamp handling; it must use UTC epoch microseconds.
2. Verify window-filter boundaries are identical.
3. Confirm aggregation logic matches Pandas exactly.
4. Check for timezone assumptions in Spark configuration.
5. Run both engines on a minimal test dataset and compare field by field.

### Offline Feature Quality Failures

If materialization is blocked:

1. Inspect the blocked materialization manifest in
   `output/materialization_manifests/`.
2. Review `failed_checks` and the embedded quality-report payload.
3. Confirm the canonical path is `output/offline_store/`.
4. Inspect the affected Hive partitions and their `features.parquet` files.
5. Fix the backfill, serialization, schema, or feature-value issue.
6. Rerun the canonical backfill and materialization.
7. Do not bypass the quality gate or introduce an alternate source path merely
   to proceed.

### Feast Serving Issues

If materialization or online lookup fails:

1. Verify `start_time` and `end_time` are timezone-aware UTC datetimes.
2. Confirm `start_time < end_time` for full materialization.
3. Confirm canonical offline partitions exist for the requested range.
4. Run persisted-feature validation through the materialization workflow.
5. Verify Redis is running and accessible.
6. Inspect the latest completed or blocked materialization manifest.
7. Confirm the user or content ID exists in the relevant source data.
8. For parity failures, compare Redis values with the latest canonical offline
   partition rather than an arbitrary local output directory.

### ML Pipeline Issues

If training metrics look suspicious:

1. Check for temporal leakage in train/validation/test splits.
2. Verify point-in-time correctness in historical retrieval.
3. Inspect feature distributions for train-versus-test drift.
4. Confirm missing-value handling is deterministic.
5. Check that `window_days`, a constant feature, is excluded.

## Getting Help

For questions about:

- Feature computation semantics: see `ARCHITECTURE.md` → Feature Contracts
- Temporal correctness: see `ARCHITECTURE.md` → Temporal Correctness
- PySpark parity: see `ARCHITECTURE.md` → Pandas and PySpark Parity
- Canonical offline-store ownership: see `ARCHITECTURE.md` → Offline Storage
  and Backfills
- Offline feature-quality validation: see `ARCHITECTURE.md` → Offline Feature
  Quality Gate
- Feast integration: see `feature_repo/` module docstrings
- Materialization: see `ARCHITECTURE.md` → Materialization
- Online serving: see `ARCHITECTURE.md` → Online Serving and Ranking
- ML pipeline: see `scripts/` module docstrings and `ARCHITECTURE.md` → ML
  Training
- Architecture decisions: see `docs/adr/`