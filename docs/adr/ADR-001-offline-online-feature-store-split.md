# ADR-001: Offline and Online Feature Store Split

- **Status:** Accepted
- **Date:** 2026-09-23
- **Decision owners:** FeatureForge maintainers

## Context

FeatureForge supports two different feature-consumption patterns:

1. Offline, point-in-time-correct feature retrieval for model training and
   evaluation.
2. Low-latency retrieval of the latest validated feature values for online
   personalization and ranking.

Using one storage layer for both patterns would create conflicting requirements:

- Historical training retrieval requires durable, date-partitioned data with
  event-time-aware joins.
- Online serving requires low-latency key-value access to current feature
  values.
- Training and serving must use versioned feature definitions to minimize
  training-serving skew.

## Decision

FeatureForge uses an explicit offline/online feature-store split:

```text
Raw behavioral data
        ↓
Event-time feature computation
        ↓
Partitioned Parquet offline feature store
        ↓
Feast feature definitions and services
        ├── Historical retrieval for training data
        └── Materialization of validated latest values
                ↓
            Redis online store
                ↓
            Online lookup and deterministic ranking demo
```

Feast is the contract and retrieval layer between offline and online feature
consumption. It owns feature entities, feature views, feature services,
historical retrieval, and materialization.

The local V1 implementation uses:

| Layer | Technology | Responsibility |
|---|---|---|
| Feature computation | Pandas reference implementation and PySpark parity path | Event-time feature computation and backfills |
| Offline store | Hive-partitioned Parquet | Reproducible historical feature data |
| Feature platform | Feast | Versioned feature definitions, retrieval, and materialization |
| Online store | Redis via Docker Compose | Low-latency local serving |
| Online consumer | FeatureForge serving module and demo scripts | Lookup and deterministic candidate ranking |

## Consequences

### Positive

- Historical training retrieval and online serving can use the same versioned
  feature definitions.
- The architecture makes training-serving skew visible and testable.
- Backfills are reproducible because historical feature values remain in the
  partitioned offline store.
- Redis remains a serving cache for current values rather than a historical
  system of record.
- The local stack is small enough to run reproducibly for reviewers.

### Trade-offs

- The local environment requires multiple components: Parquet, Feast, and Redis.
- Materialization introduces operational concerns such as quality gates,
  freshness, and failure handling.
- Redis is appropriate for local development, but not the committed AWS
  production serving profile.

## Alternatives considered

### Use only Parquet

Rejected because it does not support low-latency online feature lookup.

### Use only Redis

Rejected because Redis is not suited to durable, point-in-time historical
training retrieval or reproducible backfills.

### Implement custom feature retrieval and materialization

Rejected because Feast provides established abstractions for entities, feature
views, services, historical retrieval, and online materialization. The project
focuses on operating a feature platform correctly rather than recreating a
feature-store product.

## Future evolution

The local architecture remains the correctness-first reference environment.

The AWS production profile will use:

- S3 for durable partitioned Parquet offline storage;
- DynamoDB as the managed online-serving profile;
- environment-owned storage and service configuration;
- documented IAM, encryption, cost, scalability, and resilience trade-offs.

## Validation

This decision is supported by:

- point-in-time feature computation and future-leakage tests;
- Pandas/PySpark feature parity tests;
- idempotent backfill tests;
- Feast historical retrieval flow;
- online/offline parity integration tests;
- Redis-backed online lookup and ranking demo.