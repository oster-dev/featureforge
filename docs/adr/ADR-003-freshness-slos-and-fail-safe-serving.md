# ADR-003: Feature Freshness SLOs and Fail-Safe Serving

- Status: Accepted
- Date: 2026-09-24

## Context

FeatureForge has two separate reliability questions for persisted offline
features:

```text
Correctness:
Are the persisted feature values structurally and semantically valid?

Freshness:
Is the newest persisted feature snapshot recent enough for the intended
serving workflow?
```

The canonical offline feature store is partitioned by business observation
date:

```text
output/offline_store/
├── user_engagement_features/
│   └── observation_date=YYYY-MM-DD/
│       └── features.parquet
└── content_popularity_features/
    └── observation_date=YYYY-MM-DD/
        └── features.parquet
```

FeatureForge already has a correctness gate before Feast materialization. That
gate protects the online store from invalid persisted feature values.

Correctness alone is not sufficient for serving. A structurally valid feature
partition can still be too old for a current serving workflow.

The freshness signal must represent feature business time. Filesystem
modification time is not sufficient because it represents storage activity, not
the observation date through which the feature values are valid. Files can be
copied, restored, rewritten, or touched without becoming semantically newer.

FeatureForge also needs operationally visible behavior when feature data is
stale, missing, or when a backfill or materialization fails. A failed workflow
must not look like a successful run, and stale or invalid data must not reach
serving silently.

## Decision

FeatureForge treats feature correctness, feature freshness, and failure
handling as explicit platform contracts.

### 1. Keep correctness and freshness separate

The persisted-feature correctness gate remains mandatory before every Feast
materialization.

It validates the canonical offline feature data before Feast writes values to
the online store.

If correctness validation fails:

- Feast materialization is not called.
- A blocked materialization manifest is written.
- The caller receives `MaterializationBlockedError`.
- The CLI exits with a non-zero status.

Correctness protects the validity of feature values.

Freshness is evaluated separately because it depends on the serving use case and
the selected reference time. Freshness protects the recency of feature values.

### 2. Define freshness from canonical observation partitions

For every required feature view, the freshness check:

1. Uses the canonical offline store.
2. Locates the feature-view directory.
3. Finds the newest partition matching:

   ```text
   observation_date=YYYY-MM-DD
   ```

4. Interprets that date as UTC midnight.
5. Compares it with an explicit timezone-aware UTC reference time.
6. Fails the feature view when the lag exceeds the configured maximum.

The V1 contract is:

```text
latest canonical observation partition
        must be within
configured maximum lag
        of
explicit UTC reference time
```

Filesystem modification time is not used as the primary freshness signal.

The local default maximum lag is 24 hours.

### 3. Expose freshness as an explicit command

Freshness validation is available through:

```bash
featureforge check-freshness \
  --reference-time 2026-03-25T00:00:00+00:00 \
  --max-lag-hours 24
```

The repository also provides:

```bash
make check-freshness
```

The command:

- checks `output/offline_store/`;
- evaluates each required feature view independently;
- reports missing feature-view directories;
- reports missing or stale partitions;
- emits stable feature-view check names;
- returns exit code `0` only when all checks pass;
- returns a non-zero exit code when a required view is stale or missing.

Stable check names include:

```text
user_engagement_features.freshness
content_popularity_features.freshness
```

### 4. Apply freshness to current serving workflows

Freshness is an explicit serving-safety check for workflows that require
current feature snapshots.

The intended flow is:

```text
canonical offline feature store
        ↓
correctness quality gate
        ↓
freshness SLO check
        ↓
Feast materialization
        ↓
Redis online store
        ↓
online lookup or ranking
```

Freshness is not treated as a blanket wall-clock restriction on every
historical materialization.

Historical materialization can be valid when the requested historical feature
values are correct for the requested interval, even if the newest canonical
partition is not fresh relative to the current time.

Therefore:

```text
Correctness:
mandatory before every Feast materialization

Freshness:
explicit for current serving, demo, and operational workflows
```

### 5. Fail visibly and safely

Failure handling is part of the platform contract.

The following conditions must remain visible failures:

- stale offline feature partitions;
- missing feature-view directories or partitions;
- invalid backfill parameters;
- reversed backfill date ranges;
- failed Feast repository resolution;
- failed full materialization;
- failed incremental materialization.

Failures must not create misleading successful-run evidence.

The platform exposes failure evidence through:

- clear exceptions;
- non-zero CLI exit codes;
- blocked or failed run manifests where applicable;
- stable quality and freshness check names;
- reproducible tests;
- operational runbooks.

## Consequences

### Positive consequences

- Invalid feature values are blocked before they reach Redis.
- Stale feature snapshots are detected using business-time partitions.
- Freshness checks are deterministic when store contents, reference time, and
  maximum lag are identical.
- Historical materialization remains possible without incorrectly applying a
  current-serving freshness policy.
- Operators receive stable failure names and non-zero exit codes.
- Backfill and materialization failures are visible rather than silently
  represented as successful runs.
- Failure recovery is documented in version-controlled runbooks.
- Freshness and correctness can evolve independently as platform contracts.

### Negative consequences

- V1 freshness is partition-granular and does not prove that every row inside
  the latest partition is complete.
- A daily partition is represented at UTC midnight, so the selected threshold
  must be interpreted together with the partitioning schedule.
- Operators must provide an explicit reference time when reproducibility is
  required.
- The current implementation does not yet provide scheduling, alert routing,
  ownership metadata, paging, or automated remediation.
- Local failure simulations do not replace production-scale testing of external
  infrastructure failures such as Redis outages or object-store failures.
- Current freshness coverage is limited to the required canonical feature views.

## Alternatives Considered

### Use filesystem modification time

Rejected.

Modification time describes when a file changed on disk. It does not describe
the business observation date represented by the feature values.

### Combine correctness and freshness into one gate

Rejected.

Correctness and freshness answer different questions and operate on different
time semantics. Combining them would either make historical materialization too
strict or make serving freshness ambiguous.

### Run a current-time freshness check before every materialization

Rejected.

A global `datetime.now(UTC)` requirement would incorrectly reject legitimate
historical re-materialization requests.

### Let downstream consumers decide whether data is stale

Rejected.

This would distribute platform reliability responsibility across consumers and
could allow stale features to reach serving silently.

### Fail without manifests, exit codes, or runbooks

Rejected.

An exception without durable evidence and recovery guidance is insufficient for
operational use. The platform must make failures diagnosable and repeatable.

## Operational Contract

### Correctness contract

Before every Feast materialization:

```text
canonical offline feature data
        ↓
correctness validation
        ↓
blocked manifest and non-zero exit on failure
        ↓
Feast write only after validation succeeds
        ↓
Redis online store
```

### Freshness contract

Before workflows that require current serving data:

```text
canonical offline feature data
        ↓
featureforge check-freshness
        ↓
one freshness check per required feature view
        ↓
non-zero exit if stale or missing
        ↓
materialization and serving workflow
```

### Stale-feature recovery

```text
1. Run `make check-freshness`.
2. Identify the failed feature-view check.
3. Inspect the latest `observation_date` partition.
4. Inspect the latest backfill manifest.
5. Verify source-data availability.
6. Repair or re-run the canonical backfill.
7. Re-run the freshness check.
8. Materialize features if the workflow requires refreshed online values.
9. Verify online lookup or offline/online parity.
```

### Failure-simulation coverage

The repository contains controlled tests for:

- stale and fresh feature partitions;
- invalid backfill windows;
- reversed backfill date ranges;
- empty event streams;
- missing Feast repositories for full materialization;
- missing Feast repositories for incremental materialization.

Operational procedures are documented in:

```text
docs/runbooks/stale-features.md
docs/runbooks/failed-backfill.md
docs/runbooks/failed-materialization.md
```

