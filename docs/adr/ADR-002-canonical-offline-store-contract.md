# ADR-002: Canonical Offline Store Contract

- **Status:** Accepted
- **Date:** 2026-09-23
- **Decision owners:** FeatureForge maintainers

## Context

FeatureForge computes date-partitioned feature datasets, validates persisted
offline features, retrieves historical training data, and materializes current
values to the online store.

The local architecture has several components that depend on the same feature
dataset:

```text
Synthetic source data
        ↓
Backfill
        ↓
Partitioned Parquet offline feature store
        ↓
Quality validation
        ↓
Feast FileSources
        ├── Historical retrieval
        └── Materialization
                ↓
            Redis online store
```

If backfill, quality validation, and Feast source definitions can point to
independently configured locations, FeatureForge could validate one dataset
while Feast reads or materializes another. That creates a split-brain
source-of-truth condition and invalidates the protection provided by the
pre-materialization quality gate.

## Decision

For local FeatureForge V1, the canonical offline feature store is:

```text
output/offline_store/
```

It uses this Hive-partitioned layout:

```text
output/offline_store/
├── user_engagement_features/
│   └── observation_date=YYYY-MM-DD/
│       └── features.parquet
└── content_popularity_features/
    └── observation_date=YYYY-MM-DD/
        └── features.parquet
```

The canonical location is the shared contract for:

| Component | Responsibility |
|---|---|
| Backfill | Writes idempotent, date-partitioned Parquet feature datasets |
| Quality gate | Validates every persisted feature-view partition before materialization |
| Feast FileSources | Reads feature partitions for historical retrieval and materialization |
| Serving parity tests | Compare Redis values with the latest canonical offline partition |
| Makefile demo flow | Orchestrates the local end-to-end lifecycle |

For V1, materialization must validate the same canonical source that Feast
reads. FeatureForge must not expose an independent materialization source-path
option unless the Feast FileSources are configured from the same resolved
storage location.

## Consequences

### Positive

- The quality gate validates the exact data that Feast can materialize.
- Backfill, validation, retrieval, materialization, and parity tests use one
  explicit source of truth.
- The local demo is reproducible and easy for a reviewer to understand.
- The layout maps directly to a future environment-owned object-store prefix.

### Trade-offs

- Local V1 cannot materialize arbitrary alternate directories without
  deliberately reconfiguring the Feast source definitions.
- Parallel isolated environments require separate repository configuration or
  a future shared environment-level storage configuration.
- The fixed local path is intentional V1 scope control, not a production
  storage strategy.

## Alternatives considered

### Independent CLI path flags

Rejected for V1 because a materialization command could validate a different
directory from the one configured in Feast FileSources.

### Implicit defaults without an explicit contract

Rejected because the relationship between the producer, validator, and Feast
consumer would remain undocumented and easy to break during future changes.

### Environment-specific configuration immediately

Deferred because local V1 has one supported environment. The production
configuration mechanism should be introduced together with the AWS storage
profile rather than partially simulated through disconnected local flags.

## Future evolution

The AWS production profile will replace the local directory with one
environment-owned object-store URI, for example:

```text
s3://featureforge-<environment>/offline-store/
```

The resolved URI must be consumed consistently by:

- the backfill writer;
- the quality gate;
- Feast FileSources;
- manifests and lineage metadata;
- freshness and parity checks.

The production implementation will use one shared configuration contract rather
than unrelated per-command path flags.

## Validation

This decision is supported by:

- idempotent backfill tests that write Hive-partitioned feature datasets;
- offline feature-quality validation tests;
- materialization tests proving invalid partitions block Feast calls;
- live online/offline parity integration tests against the canonical store;
- documented `make e2e` and `make demo` workflows.