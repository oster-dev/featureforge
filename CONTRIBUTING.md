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


### Run Historical Retrieval


Run the Feast historical retrieval demo:


```bash
python feature_repo/historical_retrieval_demo.py
```


This produces:


```text
data/historical_features.parquet
```


Contains point-in-time-correct training features joined with observation labels.


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


Expected output:


```text
ruff format --check .
35 files already formatted

ruff check .
All checks passed!

pytest -v
98 passed in ~12s
```


## Testing Guidelines


Every behavior change should include an appropriate test.


Use `tests/unit/` for isolated contracts and domain logic:


- Pydantic validation
- configuration validation
- synthetic-data generation (independent and behavioral modes)
- feature calculations
- date-range behavior
- partition-path behavior
- manifest content
- idempotency at the backfill-function level
- Pandas-vs-PySpark parity for feature calculations
- Feast entity and feature view definitions
- historical retrieval point-in-time correctness


Use `tests/integration/` for executable multi-component paths:


- CLI argument parsing and execution
- source-Parquet read/write flow
- CLI-to-backfill-to-partitioned-Parquet flow
- CLI-to-manifest flow
- end-to-end idempotency behavior
- generate → backfill → retrieval → training flow


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
- Behavioral mode must produce genuine predictive signal without label leakage.
- Historical retrieval must enforce point-in-time correctness.
- Training pipelines must persist preprocessing with models.


## Generated Data


Generated outputs are intentionally ignored by Git:


```text
output/
*.parquet
data/
```


Do not commit:


- generated Parquet datasets
- generated run manifests under `output/`
- virtual environments
- cache directories
- credentials
- `.env` files
- local editor settings unless the change is intentionally project-wide
- trained model artifacts (can be regenerated)


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
fix: prevent future events from entering feature windows
fix: convert Spark timestamps to UTC epoch micros to avoid timezone drift
test: add backfill manifest coverage
test: add Pandas-vs-PySpark feature parity suite
test: add historical retrieval point-in-time tests
docs: document offline feature partition layout
docs: update README with ML pipeline instructions
chore: ignore generated pipeline outputs
chore: ignore trained model artifacts
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


For changes affecting the ML pipeline, include:


- historical retrieval correctness verification,
- time-based split behavior (no temporal leakage),
- metric changes and interpretation,
- reproducibility verification (same seed → same results),
- artifact persistence behavior.


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


## Documentation Updates


When adding significant features or changing architecture:


1. Update `README.md` with new commands or flows
2. Update `ARCHITECTURE.md` with new components or data flows
3. Update inline code documentation where behavior changes
4. Add or update docstrings for public functions and classes
5. Verify all code examples in documentation still execute correctly


## Debugging Tips


### Temporal Correctness Issues


If you suspect temporal leakage:


1. Check event window boundaries in feature calculations
2. Verify label windows use `event_time`, not `ingested_at`
3. Confirm historical retrieval uses point-in-time joins
4. Inspect train/val/test split timestamps for overlap


### PySpark Parity Failures


If parity tests fail:


1. Check timestamp handling (must use epoch microseconds)
2. Verify window filter boundaries are identical
3. Confirm aggregation logic matches Pandas exactly
4. Check for timezone assumptions in Spark configuration
5. Run both engines on a minimal test dataset and compare field-by-field


### ML Pipeline Issues


If training metrics look suspicious:


1. Check for temporal leakage in train/val/test splits
2. Verify point-in-time correctness in historical retrieval
3. Inspect feature distributions for train vs test drift
4. Confirm missing-value handling is deterministic
5. Check that `window_days` (constant feature) is excluded


## Getting Help


For questions about:


- Feature computation semantics: see `ARCHITECTURE.md` → Feature Contracts
- Temporal correctness: see `ARCHITECTURE.md` → Temporal Correctness
- PySpark parity: see `ARCHITECTURE.md` → PySpark Parity Layer
- Feast integration: see `feature_repo/` module docstrings
- ML pipeline: see `scripts/` module docstrings and `ARCHITECTURE.md` → ML Training Pipeline