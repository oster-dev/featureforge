# Contributing to FeatureForge

Thank you for contributing to FeatureForge.

FeatureForge is a production-inspired project for Data Infrastructure, Feature
Infrastructure, and ML Platform Engineering. The project prioritizes:

- reproducibility;
- event-time correctness;
- point-in-time feature semantics;
- canonical data ownership;
- executable data contracts;
- correctness and freshness gates;
- visible operational behavior;
- offline/online serving parity;
- deterministic workflows;
- documented production architecture.

Before contributing, read:

- [README.md](README.md)
- [ARCHITECTURE.md](ARCHITECTURE.md)
- [AWS Production Profile](docs/aws-production-profile.md)
- [ADR-001: Offline/Online Feature Store Split](docs/adr/ADR-001-offline-online-feature-store-split.md)
- [ADR-002: Canonical Offline Store Contract](docs/adr/ADR-002-canonical-offline-store-contract.md)
- [ADR-003: Feature Freshness SLOs and Fail-Safe Serving](docs/adr/ADR-003-freshness-slos-and-fail-safe-serving.md)
- [ADR-004: GitHub Actions CI for Reproducible Validation](docs/adr/ADR-004-github-actions-ci.md)
- [ADR-005: AWS S3 Offline-Store Production Profile](docs/adr/ADR-005-aws-s3-offline-store-profile.md)

## Development Setup

### Prerequisites

- Python 3.11, 3.12, or 3.13
- Docker Desktop for Redis and Feast serving workflows
- GNU Make
- Git

### Create a Virtual Environment

```bash
python3 -m venv .venv
source .venv/bin/activate
```

### Install Dependencies

Install the package with development dependencies:

```bash
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
```

Install PySpark for the parity-tested feature-computation engine:

```bash
python -m pip install pyspark
```

Install Feast for historical retrieval and online feature serving:

```bash
python -m pip install feast
```

Verify the CLI and package import:

```bash
python -c "import featureforge; print(featureforge.__file__)"
featureforge --help
```

Expected commands:

```text
generate
compute-features
backfill
check-freshness
materialize
materialize-incremental
```

## Repository Layout

```text
featureforge/
├── configs/              Synthetic-data configuration
├── data/                 Generated and training artifacts
├── docs/
│   ├── adr/              Architecture decisions
│   ├── runbooks/         Operational recovery procedures
│   └── aws-production-profile.md
├── feature_repo/         Feast entities, sources, views, services, demos
├── scripts/              Training and diagnostic workflows
├── src/featureforge/     Application and platform implementation
├── tests/
│   ├── failure_simulations/
│   ├── integration/
│   └── unit/
├── ARCHITECTURE.md
├── CONTRIBUTING.md
├── Makefile
├── README.md
├── docker-compose.yml
└── pyproject.toml
```

Generated data and run artifacts are intentionally recreated locally and should
not be committed unless a contribution explicitly requires a fixture.

## Local Infrastructure

FeatureForge uses Redis through Docker Compose for online-serving workflows.

Start Redis:

```bash
make docker-up
```

Verify the service:

```bash
docker exec featureforge-redis redis-cli ping
```

Expected output:

```text
PONG
```

Stop Redis:

```bash
make docker-down
```

Apply Feast definitions after Redis is running:

```bash
cd feature_repo
feast apply
cd ..
```

The baseline CI workflow does not provision Redis, Docker Compose, Feast, or
materialized online features. Those are local or infrastructure-enabled
integration prerequisites.

## AWS Production Profile

FeatureForge currently runs as a local reference platform. Local development,
the test suite, and baseline GitHub Actions CI do not require AWS credentials,
AWS resources, or cloud deployment.

The documented AWS production profile defines how the local platform can evolve
without changing its core feature contracts:

```text
Local V1
  output/offline_store/
  Redis via Docker Compose
  Feast FileSource on local Parquet

AWS profile
  s3://featureforge-<environment>/offline-store/
  Hive-partitioned Parquet
  SSE-KMS encryption
  environment-scoped IAM roles
  DynamoDB online-store profile
  Feast FileSource on S3
```

The AWS profile preserves the canonical-source invariant:

```text
Backfill writer
    = Correctness-gate input
    = Freshness-check input
    = Feast FileSource
    = Materialization source
    = Serving parity-test source
```

The following AWS architecture decisions are documented but are not yet
implemented or provisioned:

- One S3 bucket per environment: `dev`, `staging`, and `prod`.
- One customer-managed SSE-KMS key per environment.
- Hive-style feature partitions using `observation_date=YYYY-MM-DD`.
- Bucket versioning and lifecycle transitions for raw, source, feature,
  manifest, training, and model artifacts.
- `featureforge-backfill-writer` for source and offline-store writes.
- `featureforge-materialization-writer` for validated offline-store reads,
  materialization manifests, and DynamoDB writes.
- `featureforge-serving-reader` for read-only online feature serving.
- DynamoDB as the managed AWS online-store profile.

Do not introduce AWS SDK calls, AWS credentials, S3 paths, DynamoDB resources,
or deployment steps into local workflows unless the corresponding infrastructure
design, tests, cost boundaries, IAM policies, and documentation are added in
the same change.

See [AWS Production Profile](docs/aws-production-profile.md) and
[ADR-005: AWS S3 Offline-Store Production Profile](docs/adr/ADR-005-aws-s3-offline-store-profile.md).

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

### Compute a Single Feature Snapshot

Compute one user engagement snapshot at an explicit observation timestamp:

```bash
featureforge compute-features \
  --input output/source_data \
  --output output/single_snapshot \
  --observation-time 2026-03-12T00:00:00+00:00 \
  --window-days 7
```

### Run a Feature Backfill

Backfill an inclusive date range into the canonical local offline store:

```bash
featureforge backfill \
  --input output/source_data \
  --output output/offline_store \
  --start-date 2026-03-10 \
  --end-date 2026-03-12 \
  --window-days 7 \
  --engine pandas
```

The deterministic output layout is:

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
the same input data, date range, window, and code. This is the current V1
idempotency behavior.

The backfill runner supports:

- `pandas` as the readable correctness reference;
- `spark` as the parity-tested scalable engine.

Use Spark explicitly:

```bash
featureforge backfill \
  --input output/source_data \
  --output output/offline_store \
  --start-date 2026-03-10 \
  --end-date 2026-03-12 \
  --window-days 7 \
  --engine spark
```

### Check Offline Feature Freshness

Feature correctness and freshness are separate platform concerns.

Correctness asks:

```text
Are persisted feature values structurally and semantically valid?
```

Freshness asks:

```text
Is the newest persisted feature snapshot recent enough for the intended
serving workflow?
```

Freshness is based on the newest Hive-style partition under the canonical
offline store:

```text
output/offline_store/<feature-view>/observation_date=YYYY-MM-DD/
```

It uses business observation time, not filesystem modification time.

Run the default local freshness check:

```bash
make check-freshness
```

Run the CLI with an explicit UTC reference time and maximum lag:

```bash
featureforge check-freshness \
  --reference-time 2026-03-25T00:00:00+00:00 \
  --max-lag-hours 24
```

The command:

- checks `output/offline_store/`;
- evaluates every required feature view independently;
- reports the latest partition and calculated lag;
- returns exit code `0` when all checks pass;
- returns a non-zero exit code when a required view is stale or missing.

Stable failed-check names include:

```text
user_engagement_features.freshness
content_popularity_features.freshness
```

Do not add a global `datetime.now(UTC)` freshness requirement to all historical
materialization paths. Historical re-materialization can be valid when the
persisted feature data is correct for the requested interval, even if it is not
fresh for a present-time serving workflow.

### Run Feast Materialization

Full materialization requires explicit timezone-aware UTC timestamps:

```bash
featureforge materialize \
  --repo feature_repo \
  --start-time 2026-03-20T00:00:00+00:00 \
  --end-time 2026-03-25T00:00:00+00:00 \
  --manifest-output output
```

Incremental materialization uses Feast's stored watermark:

```bash
featureforge materialize-incremental \
  --repo feature_repo \
  --end-time 2026-03-25T00:00:00+00:00 \
  --manifest-output output
```

Materialization always validates the canonical offline store:

```text
output/offline_store/
```

The CLI intentionally does not accept `--offline-store-dir`. This prevents a
caller from validating a different path than the one configured in Feast
`FileSource` definitions.

The persisted-feature correctness gate is mandatory before every Feast write.
If correctness validation fails:

- Feast is not invoked;
- a blocked materialization manifest is written;
- `MaterializationBlockedError` is raised;
- the CLI exits with a non-zero status.

Completed and blocked manifests are written under:

```text
output/materialization_manifests/
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

The historical retrieval path performs point-in-time-correct feature joins with
the observation labels.

### Train the Baseline Model

Train the sklearn baseline model:

```bash
python scripts/train_baseline.py
```

This produces:

```text
output/models/baseline_logreg_pipeline.joblib
output/models/baseline_logreg_metrics.json
```

Run diagnostic analysis:

```bash
python scripts/diagnose_baseline.py
```

### Run Online Feature Lookup

Run the online feature lookup demo for a specific user:

```bash
python feature_repo/online_lookup_demo.py --user-id user_000290
```

### Run Personalization Ranking

Run the deterministic content-ranking demo:

```bash
python feature_repo/personalization_demo.py \
  --user-id user_000290 \
  --top-k 5
```

### Run the End-to-End Demo

Start Redis and run the complete local workflow:

```bash
docker compose up -d
make demo
```

This executes:

```text
generate
  → canonical backfill into output/offline_store
  → persisted-feature correctness validation
  → freshness check for the serving workflow
  → Feast materialization into Redis
  → online feature lookup
  → deterministic content ranking
  → offline/online parity checks
```

Customize the demo:

```bash
make demo USER_ID=user_000290 TOP_K=5
make demo OUTPUT_DIR=output_demo
```

`OUTPUT_DIR` contains run-scoped artifacts. It does not replace the canonical
Feast source path at `output/offline_store/`.

## Pandas and PySpark Parity

`src/featureforge/spark_features.py` computes the same two feature views as
`features.py` using PySpark.

Before changing either implementation, run:

```bash
pytest tests/unit/test_spark_features.py -v
pytest tests/unit/test_spark_parquet_features.py -v
```

If you modify feature logic in `features.py`, make the equivalent change in
`spark_features.py`, then ensure the parity suite still passes. A change that
passes the Pandas tests but breaks Spark parity is incomplete.

When working with timestamps in `spark_features.py`:

- never pass a Python `datetime` directly into a Spark `TimestampType` column;
- never rely on `spark.sql.session.timeZone` alone to guarantee UTC behavior;
- convert values to UTC epoch microseconds before Spark processing;
- convert them back only after collection.

## Development Rules

### Preserve the Canonical Source Contract

For the local Feast workflow:

```text
backfill output
    = correctness-gate input
    = freshness-check input
    = Feast FileSource input
    = materialization source
    = serving parity-test source
```

All of these use:

```text
output/offline_store/
```

For the documented AWS production profile, the same invariant uses:

```text
s3://featureforge-<environment>/offline-store/
```

Do not:

- introduce a second local source path for Feast;
- add an arbitrary offline-store override to materialization;
- validate one feature store while Feast reads another;
- move the Feast `FileSource` path without updating the canonical contract,
  architecture documentation, ADRs, and parity tests;
- introduce environment-specific S3 paths that allow one component to validate
  a different URI than the URI used by Feast or materialization.

### Preserve Correctness and Freshness Semantics

Do not conflate correctness with freshness.

```text
Correctness: Are persisted feature values valid?
Freshness:   Are persisted feature snapshots current enough for the intended
             serving workflow?
```

Correctness remains mandatory before every Feast materialization. Freshness
remains an explicit serving-safety check based on business-time
`observation_date` partitions and an explicit UTC reference time.

Do not use filesystem modification time as the primary freshness signal.

### Preserve Event-Time Semantics

Features and labels use event time, not ingestion time.

Feature window:

```text
(observation_time - window_days, observation_time]
```

Label window:

```text
observation_time < event_time <= label_window_end
```

Do not alter these boundaries without:

1. updating feature implementations;
2. updating Spark parity logic;
3. updating unit tests;
4. updating architecture documentation;
5. documenting the decision in an ADR if it changes the platform contract.

### Preserve Idempotency

Backfill output paths must remain deterministic:

```text
<offline-store-root>/<feature-view>/observation_date=YYYY-MM-DD/features.parquet
```

Do not replace deterministic overwrite behavior with append-only or randomly
named partition files unless you also introduce versioning, lineage, retention,
and reader-selection semantics.

For S3, preserve recovery from idempotent overwrites through bucket versioning.
Do not treat object versioning as permission to bypass correctness validation.

### Preserve Failure Visibility

Do not make failures look like successful runs. Failure behavior should provide
one or more of:

- a clear exception;
- a non-zero CLI exit code;
- a blocked materialization manifest;
- a stable failed check name;
- a reproducible failure-simulation test;
- a runbook with recovery and verification steps.

Maintain controlled reliability tests under:

```text
tests/failure_simulations/
```

Maintain operator procedures under:

```text
docs/runbooks/
```

## Testing Guidelines

Every behavior change should include an appropriate test.

### Unit Tests

Use `tests/unit/` for isolated contracts and domain logic:

- Pydantic validation;
- configuration validation;
- synthetic-data generation;
- feature calculations;
- date-range behavior;
- partition paths;
- manifest content;
- backfill idempotency;
- Pandas/PySpark parity;
- Feast entity and feature-view definitions;
- historical retrieval point-in-time correctness;
- persisted offline-feature correctness;
- freshness validation;
- materialization timestamp contracts;
- canonical materialization-source enforcement;
- deterministic materialization manifests;
- blocked materialization manifests;
- online lookup behavior;
- missing online entities;
- ranking and stable tie-breaking.

### Integration Tests

Use `tests/integration/` for executable multi-component paths:

- CLI argument parsing and execution;
- source-Parquet read/write flow;
- CLI-to-backfill-to-partitioned-Parquet flow;
- CLI-to-manifest flow;
- end-to-end idempotency;
- generation, backfill, retrieval, and training;
- generation, backfill, materialization, online lookup, and ranking;
- offline/online serving parity.

### Failure Simulations

Use `tests/failure_simulations/` for controlled reliability scenarios:

- stale offline features fail freshness validation;
- fresh offline features pass freshness validation;
- invalid backfill parameters fail clearly;
- empty event streams remain valid and produce zero-count features;
- missing Feast repositories fail clearly for full materialization;
- missing Feast repositories fail clearly for incremental materialization.

Do not remove a temporal, correctness, freshness, idempotency, parity, or
failure-simulation test merely to make a failing suite pass. Understand and fix
the underlying contract violation.

## Continuous Integration

GitHub Actions runs baseline validation on every push to `main` and every pull
request.

The clean Ubuntu runner uses Python 3.13 and validates:

- editable installation from the `src/` layout;
- `import featureforge` after installation;
- Ruff formatting;
- Ruff linting;
- the full pytest suite.

The local equivalent is:

```bash
python -m pip install --upgrade pip
python -m pip install -e ".[dev]"
python -c "import featureforge; print(featureforge.__file__)"
ruff format --check .
ruff check .
pytest -v
```

The current GitHub Actions baseline result is:

```text
141 passed, 5 skipped, 0 failed
```

The local full integration environment result is:

```text
146 passed, 0 skipped, 0 failed
```

The five skipped tests are in `tests/integration/test_online_serving.py` and
require Redis, applied Feast definitions, canonical offline snapshots, and
materialized online values.

The baseline workflow intentionally does not provision this infrastructure.
Do not remove the documented skip guard to force an artificial green result.
Use a separate infrastructure-enabled integration workflow when automated
online-serving validation is introduced.

The CI architecture decision is documented in
[ADR-004: GitHub Actions CI for Reproducible Validation](docs/adr/ADR-004-github-actions-ci.md).

## Validation Before a Commit

Run all relevant checks before opening a pull request or creating a commit:

```bash
ruff format --check .
ruff check .
pytest -v
```

You may also use the existing Make targets:

```bash
make validate
make lint
make test
make docker-config
make check-freshness
```

For the current baseline repository state, expect:

```text
Local:  146 passed, 0 skipped, 0 failed
CI:     141 passed, 5 skipped, 0 failed
```

Run focused checks after changing related code:

```bash
pytest tests/unit/test_feature_quality.py -v
pytest tests/unit/test_materialization.py -v
pytest tests/unit/test_cli.py -v
pytest tests/failure_simulations/ -v
make check-freshness
```

All relevant checks should pass before a pull request is opened.

## Troubleshooting

### Offline Feature Freshness Failures

If freshness validation fails:

1. Run `make check-freshness` or invoke `featureforge check-freshness` with an explicit reference time.
2. Identify the failed feature view from the stable check name.
3. Inspect the latest `observation_date=YYYY-MM-DD` partition under `output/offline_store/`.
4. Compare the latest observation date with the configured reference time and maximum lag.
5. Inspect the relevant backfill manifest under `output/offline_store/manifests/`.
6. Verify source-data availability and the intended backfill range.
7. Repair or rerun the canonical backfill if the latest partition is genuinely stale.
8. Re-run freshness validation.
9. Re-run materialization only if the current serving workflow requires refreshed online values.
10. Verify online lookup or offline/online parity.

Do not use filesystem modification time as evidence of feature freshness. Do not
bypass freshness by changing the canonical Feast source path.

For the detailed operational procedure, see
[`docs/runbooks/stale-features.md`](docs/runbooks/stale-features.md).

### Feast or Online-Serving Issues

If online lookup or materialization fails:

1. Confirm Docker is running.
2. Confirm Redis is healthy:

   ```bash
   docker exec featureforge-redis redis-cli ping
   ```

3. Confirm the Feast repository has been applied:

   ```bash
   cd feature_repo
   feast apply
   cd ..
   ```

4. Confirm canonical offline feature partitions exist:

   ```bash
   find output/offline_store -name features.parquet
   ```

5. Confirm correctness validation passes before materialization.
6. Run `make check-freshness` if the serving workflow requires current values.
7. Run materialization again with explicit timezone-aware UTC timestamps.
8. Inspect completed or blocked materialization manifests.

For the detailed procedure, see
[`docs/runbooks/failed-materialization.md`](docs/runbooks/failed-materialization.md).

### Failed Backfills

If a backfill fails:

1. Inspect the error for invalid input parameters.
2. Verify `--window-days` is positive.
3. Verify `--start-date` is not after `--end-date`.
4. Verify all source Parquet files exist:
   - `users.parquet`
   - `content.parquet`
   - `events.parquet`
   - `labels.parquet`
5. Check that the source data can be read by the active Python environment.
6. Re-run with an explicit and valid date range.
7. Confirm a backfill manifest appears under `output/offline_store/manifests/`.

For the detailed procedure, see
[`docs/runbooks/failed-backfill.md`](docs/runbooks/failed-backfill.md).

## Documentation Expectations

Update documentation whenever a change affects:

- data contracts;
- feature definitions;
- point-in-time or event-time semantics;
- canonical storage paths;
- Feast source ownership;
- quality or freshness gate behavior;
- materialization behavior;
- manifest schema;
- test or operational workflows;
- failure recovery expectations;
- project setup commands;
- CI validation behavior;
- AWS environment, storage, encryption, IAM, lifecycle, or online-store profile;
- production deployment, cost, resilience, or cloud-operational behavior.

Use an ADR when a change establishes a durable architecture, ownership,
reliability, security, or operational decision.

Use a runbook when a change introduces a recognizable operational incident,
diagnosis flow, recovery procedure, or prevention policy.

## Commit Guidance

Prefer small, focused commits. Examples:

```text
feat: add offline feature freshness checks
test: add controlled materialization failure simulation
docs: add freshness and failure-handling runbooks
docs: add ADR for freshness SLOs and fail-safe serving
docs: document CI validation and local serving prerequisites
docs: define AWS S3 and DynamoDB production profile
docs: add ADR for AWS offline-store ownership and IAM boundaries
fix: preserve UTC timestamp semantics in Spark features
refactor: isolate canonical offline store configuration
```

Commit messages should describe the platform behavior or contract being changed,
not only the file names that changed.

Before committing:

```bash
git status
git diff --check
git diff
```

After staging:

```bash
git add <files>
git diff --cached --check
git diff --cached
```

## Pull Request Guidance

A pull request should describe:

- the problem being solved;
- the data, temporal, correctness, freshness, serving, or cloud contract
  affected;
- the implementation approach;
- tests run;
- operational or migration implications;
- documentation or ADR updates;
- intentionally deferred follow-up work.

For changes affecting the canonical offline source, feature semantics,
correctness gates, freshness SLOs, materialization, manifests, serving behavior,
AWS storage, IAM, encryption, or cloud operating profile, explain why the
change preserves or intentionally changes the existing platform contract.

A useful pull request structure is:

```text
## Problem

## Change

## Contract impact

## Validation

## Operational impact

## Documentation and ADRs

## Follow-up work
```

## Architecture Decisions

Important durable decisions are documented as ADRs:

- [ADR-001: Offline/Online Feature Store Split](docs/adr/ADR-001-offline-online-feature-store-split.md)
- [ADR-002: Canonical Offline Store Contract](docs/adr/ADR-002-canonical-offline-store-contract.md)
- [ADR-003: Feature Freshness SLOs and Fail-Safe Serving](docs/adr/ADR-003-freshness-slos-and-fail-safe-serving.md)
- [ADR-004: GitHub Actions CI for Reproducible Validation](docs/adr/ADR-004-github-actions-ci.md)
- [ADR-005: AWS S3 Offline-Store Production Profile](docs/adr/ADR-005-aws-s3-offline-store-profile.md)