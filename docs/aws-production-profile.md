# FeatureForge AWS Production Profile

This document describes the AWS production architecture profile for
FeatureForge. It is an architecture and planning artifact, not a deployment
record.

No AWS resources are provisioned as a result of this document. It exists so a
reviewer can evaluate the credibility, cost-awareness, and security posture of
the intended production system without requiring an actual cloud deployment.

## Scope

This document covers:

- the S3-backed offline feature store;
- Parquet partitioning strategy;
- encryption and key management;
- least-privilege IAM roles;
- lifecycle and cost trade-offs;
- resilience characteristics;
- the DynamoDB online-store profile;
- how this profile relates to the current local reference platform;
- what is explicitly out of scope for this stage.

## Relationship to the Local Platform

FeatureForge's local V1 platform is fully implemented and tested. It uses:

```text
output/offline_store/   (local Parquet, Hive-partitioned)
Redis via Docker Compose (local online store)
Feast FileSource         (local file-based feature source)
```

The AWS profile described here generalizes the same contracts rather than
replacing them:

| Concern | Local V1 | AWS production profile |
|---|---|---|
| Offline feature storage | `output/offline_store/` | `s3://featureforge-<environment>/offline-store/` |
| Partitioning | `observation_date=YYYY-MM-DD` | Same partitioning scheme |
| Format | Parquet | Parquet |
| Online store | Redis via Docker Compose | DynamoDB |
| Feature definitions | Feast `FileSource` | Feast `FileSource` pointed at S3 |
| Correctness gate | Local validation before materialization | Same validation logic against the S3 source |
| Freshness check | Latest local `observation_date` partition vs. UTC reference | Same logic against the S3 source |
| Manifests | Local JSON files | Versioned S3 manifest objects |

The canonical-source invariant from
[ADR-002](adr/ADR-002-canonical-offline-store-contract.md) is preserved and
generalized in
[ADR-005](adr/ADR-005-aws-s3-offline-store-profile.md):

```text
Backfill writer
    = Correctness-gate input
    = Freshness-check input
    = Feast FileSource
    = Materialization source
    = Serving parity-test source
```

No component may read a different resolved offline-store URI than the one
written by the backfill process for that environment.

## Environments

FeatureForge production planning assumes three isolated environments:

```text
dev
staging
prod
```

Each environment has:

- its own S3 bucket;
- its own KMS key;
- its own IAM roles;
- its own DynamoDB tables.

Environments are never allowed to share a bucket, key, or table. This keeps
blast radius contained and makes IAM policy scoping straightforward: a role
in `dev` structurally cannot reach `prod` resources.

## S3 Bucket and Prefix Layout

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

### Design Rationale

- `raw/` holds unprocessed ingested events, partitioned by ingestion date.
  This preserves the original ingestion-time record independent of any later
  reprocessing.
- `source/` holds validated, typed source tables equivalent to the local
  `users.parquet`, `content.parquet`, `events.parquet`, and `labels.parquet`
  outputs.
- `offline-store/` is the canonical feature store, structurally identical to
  the local `output/offline_store/` layout.
- `manifests/` is split into subdirectories by manifest type (backfill,
  correctness, freshness, materialization) rather than one flat directory, to
  keep lineage queries and lifecycle rules type-specific.
- `training/` and `models/` are separated from the feature store because they
  have different retention and access patterns: training datasets are
  regenerated from feature data, and models are versioned artifacts consumed
  by serving or evaluation, not by Feast.

## Partitioning Strategy

Feature partitions use the same Hive-style partitioning as the local platform:

```text
<feature_view>/observation_date=YYYY-MM-DD/features.parquet
```

This enables:

- partition pruning for Spark and Athena-style scans, so a query for one
  observation date does not scan the entire feature history;
- direct reuse of the existing point-in-time feature-window logic without
  modification;
- straightforward correctness and freshness checks based on the newest
  `observation_date` partition per feature view, consistent with
  [ADR-003](adr/ADR-003-freshness-slos-and-fail-safe-serving.md).

Raw events are partitioned by `ingestion_date` rather than `observation_date`,
since raw data represents unprocessed ingestion history, not a validated
feature snapshot.

## Encryption

All S3 objects are encrypted using **SSE-KMS** with a customer-managed key,
one key per environment:

```text
alias/featureforge-dev-key
alias/featureforge-staging-key
alias/featureforge-prod-key
```

Rationale:

- Customer-managed keys allow key rotation and per-environment revocation
  without affecting other environments.
- Bucket policies deny any `PutObject` request that does not specify the
  environment's KMS key, preventing accidental unencrypted or
  wrong-key writes.
- CloudTrail logs all KMS `Encrypt`/`Decrypt` API calls, giving an audit
  trail of exactly which role accessed which data and when.
- SSE-S3 (AWS-managed keys) was considered and rejected because it does not
  support per-environment key isolation or fine-grained IAM-based key access
  control.

DynamoDB tables use AWS-managed encryption at rest by default, which is
sufficient given that IAM already scopes table access per environment and per
role.

## IAM Roles

Access follows least privilege through three distinct roles:

### `featureforge-backfill-writer`

- Can write to `raw/`, `source/`, and `offline-store/` in its environment's
  bucket.
- Can write backfill and correctness manifests.
- Cannot write to the DynamoDB online store.
- Cannot modify IAM policies or KMS key policies.

### `featureforge-materialization-writer`

- Can read `offline-store/` in its environment's bucket.
- Can write materialization manifests.
- Can write to the environment's DynamoDB online-store table.
- Cannot modify `raw/` or `source/`.
- Cannot modify Feast feature definitions.

### `featureforge-serving-reader`

- Can read from the environment's DynamoDB online-store table.
- Cannot write to S3.
- Cannot write to DynamoDB.
- Cannot trigger materialization.
- Cannot modify Feast feature definitions.

### Rationale

This split mirrors the platform's existing correctness principle: the
component that validates and promotes data is distinct from the component
that consumes it. A compromised or buggy serving-reader credential cannot
corrupt offline or online feature data, because it structurally has no write
permission anywhere in the pipeline.

Example policy shape for `featureforge-serving-reader` (illustrative, not a
deployed policy):

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:BatchGetItem",
        "dynamodb:Query"
      ],
      "Resource": "arn:aws:dynamodb:*:*:table/featureforge-<environment>-online-store"
    }
  ]
}
```

## Lifecycle and Cost Trade-offs

| Prefix | Storage class transition | Rationale |
|---|---|---|
| `raw/` | Standard → IA at 90 days → Glacier Instant Retrieval at 365 days | Raw events are rarely re-read after processing; retained for reprocessing and audit |
| `source/` | Standard → IA at 90 days → Glacier Instant Retrieval at 365 days | Same access pattern as raw data |
| `offline-store/` | Standard → IA at 180 days | Feature partitions remain in Standard through the freshness SLO window, then move to IA once they are primarily used for historical retrieval rather than current serving |
| `manifests/` | Standard-IA immediately | Small, append-heavy, rarely re-read individually, but must be retained indefinitely for lineage |
| `training/` | Standard → IA at 90 days | Regenerable from feature data; retained for reproducibility, not primary access |
| `models/` | Standard, versioned | Small artifacts; versioning is more important than storage-class optimization |

All buckets have **versioning enabled**. This protects against accidental
overwrite from a faulty backfill, consistent with the idempotent-overwrite
contract in ADR-002: if a backfill writes corrupted data to a partition path,
the previous good version remains recoverable.

Cost is controlled primarily through lifecycle transitions rather than
deletion, since point-in-time historical retrieval requires that old feature
partitions remain queryable, even if infrequently accessed.

## Resilience

- S3 provides built-in multi-AZ durability and availability within a region;
  no additional configuration is required for this property.
- Versioning provides protection against accidental overwrite or deletion.
- Cross-region replication is explicitly **not** included in this stage. It
  is a documented future consideration if FeatureForge needs multi-region
  disaster recovery, not a V1 requirement.
- DynamoDB is configured with on-demand capacity mode for the online-store
  profile, avoiding the operational overhead of provisioned-capacity
  planning at this project's scale, while still providing multi-AZ
  durability by default.

## Observability and Lineage

Every backfill, correctness check, freshness check, and materialization run
writes a manifest object under `offline-store/manifests/<type>/`, mirroring
the local JSON manifest contract. Each manifest records:

- run type and status;
- Git SHA of the code that produced the run;
- resolved input and output S3 URIs;
- start and completion timestamps;
- row or partition counts;
- duration;
- correctness-report payload, when applicable.

This preserves the same auditability guarantee as the local platform:
any feature value in the online store can be traced back to the backfill run,
correctness check, and materialization run that produced it.

## Feast and Online-Store Profile

Feast `FileSource` definitions point at the environment's S3 offline-store
prefix instead of a local path. Historical retrieval and materialization use
the same Feast APIs already implemented locally; only the resolved source URI
changes.

DynamoDB is the documented AWS online-store profile, replacing Redis for
production serving:

| Local | AWS |
|---|---|
| Redis via Docker Compose | DynamoDB, on-demand capacity |
| Manual local start/stop | Managed, always-available service |
| Single-node, no built-in HA | Multi-AZ durability by default |

Redis remains the local development and CI-adjacent online store. It is not
replaced or removed; it continues to provide fast, reproducible local
iteration, consistent with the project's local-first development philosophy.

## Non-Goals for This Stage

The following are explicitly out of scope for Day 15 and this document:

- Provisioning any real AWS resources (buckets, keys, roles, tables).
- Terraform or other infrastructure-as-code implementation.
- An actual AWS deployment or migration of the running platform.
- Cross-region replication or multi-region disaster recovery.
- Autoscaling policy tuning for DynamoDB beyond on-demand capacity mode.
- A CI/CD pipeline that deploys to AWS.

These remain future roadmap items once the local platform's correctness,
reliability, and documentation are complete and stable, consistent with the
project's principle of not adding cloud complexity before the local vertical
slice is correct, testable, and documented.


