# ADR-005: AWS S3 Offline-Store Production Profile

**Status:** Accepted
**Date:** 2026-09-25
**Author:** Nico Ostermann
**Deciders:** Nico Ostermann

---

## Context

FeatureForge's local V1 platform uses one fixed canonical offline-store path:

```text
output/offline_store/
```

This path is the shared contract for backfill writes, the persisted-feature
correctness gate, freshness checks, Feast `FileSource` definitions,
materialization, and offline/online parity tests (see ADR-002).

The project's long-term goal is a credible AWS production profile without
prematurely introducing cloud complexity, cost, or deployment risk before the
local vertical slice is correct, tested, and documented. FeatureForge must be
able to grow from the local environment into the cloud without a fundamental
rewrite.

This requires defining, in advance, how the canonical offline-store contract
generalizes from a local fixed filesystem path to a durable, multi-environment
AWS storage profile.

## Decision

FeatureForge replaces the local fixed path with one environment-owned S3
prefix per deployment environment:

```text
s3://featureforge-<environment>/offline-store/
```

Examples:

```text
s3://featureforge-dev/offline-store/
s3://featureforge-staging/offline-store/
s3://featureforge-prod/offline-store/
```

Each environment has its own bucket. Buckets are not shared across
environments. This avoids cross-environment data leakage and keeps IAM
policies environment-scoped.

### Bucket and Prefix Layout

```text
s3://featureforge-<environment>/
├── raw/
│   └── events/
│       └── ingestion_date=YYYY-MM-DD/
├── source/
│   ├── users/
│   ├── content/
│   ├── events/
│   └── labels/
├── offline-store/
│   ├── user_engagement_features/
│   │   └── observation_date=YYYY-MM-DD/
│   │       └── features.parquet
│   ├── content_popularity_features/
│   │   └── observation_date=YYYY-MM-DD/
│   │       └── features.parquet
│   └── manifests/
│       ├── backfill/
│       ├── correctness/
│       ├── freshness/
│       └── materialization/
├── training/
│   └── historical-features/
└── models/
    └── baseline/
```

The `offline-store/` prefix preserves the same Hive-style partitioning used
locally:

```text
<feature_view>/observation_date=YYYY-MM-DD/features.parquet
```

This means feature computation, correctness validation, and freshness logic
do not need to change their partitioning assumptions when moving from local
Parquet to S3 Parquet. Only the storage backend and URI resolution change.

### Canonical Source Contract Preserved

The same invariant established in ADR-002 continues to apply, generalized
to the resolved storage URI for the active environment:

```text
Backfill writer
    = Correctness-gate input
    = Freshness-check input
    = Feast FileSource
    = Materialization source
    = Serving parity-test source
```

For AWS, `<offline-store-uri>` resolves to:

```text
s3://featureforge-<environment>/offline-store/
```

No component may read a different offline-store URI than the one written by
the backfill process for that environment. This includes Feast `FileSource`
definitions, the correctness gate, freshness checks, and parity tests.

### Encryption

All objects under `s3://featureforge-<environment>/` use server-side
encryption:

- **SSE-KMS** with a customer-managed KMS key, one key per environment.
- Bucket policies deny unencrypted `PutObject` requests.
- The KMS key grants encrypt/decrypt permissions only to the specific IAM
  roles defined below, not to broad account-level access.

Using a customer-managed key per environment (rather than SSE-S3) allows
key rotation, access auditing via CloudTrail, and environment-level key
isolation without added operational complexity at this project's scale.

### Partitioning and Lifecycle

- Partitioning follows `observation_date=YYYY-MM-DD`, matching the local
  Hive-style layout and enabling partition pruning for Spark/Athena-style
  reads.
- Parquet remains the columnar format for both source and feature data.
- Lifecycle rules transition `raw/` and `source/` objects older than 90 days
  to S3 Infrequent Access, and objects older than 365 days to S3 Glacier
  Instant Retrieval.
- `offline-store/` feature partitions remain in S3 Standard for the SLO
  window relevant to freshness checks (see the freshness contract in
  ADR-003), then transition to Infrequent Access after 180 days, since older
  partitions are primarily needed for point-in-time historical retrieval
  rather than current serving.
- `manifests/` are retained indefinitely in S3 Standard-Infrequent Access,
  since they are small, append-heavy, and required for lineage and audit
  history.
- Versioning is enabled on all buckets to protect against accidental
  overwrite or deletion, consistent with the idempotent-overwrite backfill
  contract (ADR-002) while still allowing recovery of a previous object
  version if a backfill writes corrupted data.

### IAM Roles

Access is split into three least-privilege roles instead of one broad role:

| Role | Can | Cannot |
|---|---|---|
| `featureforge-backfill-writer` | Write to `raw/`, `source/`, `offline-store/`; write backfill and correctness manifests | Write to the online store; modify IAM; write materialization manifests |
| `featureforge-materialization-writer` | Read `offline-store/`; write materialization manifests; write to the AWS online-store profile | Modify `raw/` or `source/`; modify feature definitions |
| `featureforge-serving-reader` | Read from the AWS online-store profile | Write to S3; write to the online store; trigger materialization; modify Feast definitions |

This mirrors the platform's existing correctness principle: the component
that validates and promotes data is distinct from the component that
consumes it. A compromised or buggy serving reader cannot corrupt offline or
online feature data.

## Consequences

### Positive

- The local-to-cloud migration path requires no change to feature logic,
  partitioning scheme, or the canonical-source invariant — only the resolved
  storage URI changes.
- Environment isolation via separate buckets prevents dev/staging/prod data
  cross-contamination.
- Least-privilege IAM roles limit blast radius if a credential is
  compromised.
- Lifecycle rules keep long-term storage cost predictable without deleting
  data needed for point-in-time historical retrieval.
- SSE-KMS with environment-scoped keys provides auditable encryption without
  a shared cross-environment key.

### Negative

- Three IAM roles and per-environment KMS keys add operational setup
  compared to a single shared role and key.
- S3 introduces network latency and eventual consistency characteristics
  that do not exist with local Parquet, which must be accounted for in
  materialization and correctness-check timing.
- Cross-environment promotion (for example, promoting a validated dev
  feature definition to staging) requires an explicit, documented process
  rather than a shared filesystem.

### Mitigations

- Document the exact IAM policy JSON per role in
  `docs/aws-production-profile.md` before any real deployment.
- Treat this ADR as an architecture profile, not a deployment order. No AWS
  resources are created as a result of this ADR alone.
- Revisit S3 consistency and materialization timing assumptions explicitly
  when a real AWS deployment is implemented, with corresponding tests.

## Compliance

This decision aligns with the L5 roadmap's Phase 5 goal:

> Document the S3 bucket layout, Parquet partitioning, lifecycle/cost
> trade-offs, and encryption. Document least-privilege roles: job writer,
> materialization writer, and serving reader.

It extends, rather than replaces, the local canonical-source contract
established in ADR-002 and preserves the correctness/freshness separation
established in ADR-003.

## Notes

- No AWS resources are provisioned by this ADR. This is an architecture and
  documentation milestone (Day 15), not a deployment milestone.
- The corresponding online-store profile (DynamoDB) is documented separately
  in `docs/aws-production-profile.md`.
- Local development continues to use `output/offline_store/` and Redis via
  Docker Compose, unaffected by this decision.
