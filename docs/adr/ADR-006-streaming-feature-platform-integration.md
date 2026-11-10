# ADR-006: Streaming Feature Platform Integration

## Status

Accepted

## Date

2026-11-10

## Context

FeatureForge originally explored streaming concepts in a separate repository,
FeatureForge Stream. That repository established event contracts, event-time
semantics, a play-count reference implementation, structured routing, a guarded
Redis online store, Kafka-to-Redis materialization, and unit and integration
tests.

Maintaining streaming as a separate repository created an unnecessary boundary
between the streaming prototype and the broader FeatureForge ML platform. The
platform needs one coherent architecture for offline feature computation,
online serving, streaming ingestion, streaming feature computation, and online
materialization.

FeatureForge must now treat streaming as a first-class platform capability
rather than as an isolated experimental project.

## Decision

FeatureForge Stream is integrated into the FeatureForge repository as part of
the FeatureForge ML platform.

Streaming is no longer maintained as a separate repository. Its proven
contracts, reference logic, tests, runtime configuration, Flink jobs, and
Kafka-to-Redis materializer are part of FeatureForge.

The streaming architecture is:

```text
Raw interaction events
  -> Kafka: interaction-events.v1
  -> Flink validation and routing
  -> Kafka: interaction-events.validated.v1
  -> Flink event-time feature computation
  -> Kafka: features.user-play-count-1h.v1
  -> Kafka-to-Redis materializer
  -> Redis online store
  -> Online feature retrieval
```

The first streaming feature is `user_play_count_1h`.

## Streaming Contracts

Streaming event contracts are executable platform contracts.

`InteractionEventV1` validates:

- event identity;
- event and ingestion timestamps;
- user and content identity;
- event type;
- play duration;
- source;
- schema version.

The contract normalizes timestamps to UTC and rejects invalid timestamps,
ingestion before event time, invalid entity IDs, unsupported event types or
sources, unknown fields, and invalid play semantics.

The routing layer separates valid event JSON from structured validation
failures. Invalid input produces a rejection record with a stable schema and
reason code. It does not silently disappear or block subsequent valid events.

## Event-Time Semantics

Streaming features use event time, not ingestion time.

The streaming feature-time mapper preserves right-closed window membership:

```text
start < event_time <= end
```

An event exactly at a window boundary belongs to the preceding window.

For example, a play event at exactly `11:30:00` belongs to the one-hour window
ending at `12:29`, not the window ending at `12:30`.

This preserves consistency between the Pandas reference implementation, the
Flink streaming job, and the Redis online store.

## Flink Runtime

FeatureForge uses a local Docker Compose streaming stack consisting of:

- Apache Kafka;
- Flink JobManager;
- Flink TaskManager;
- Redis.

The Flink jobs are mounted into the Flink containers from the repository's
`jobs/` directory.

The validation job routes raw events into valid and rejected topics:

```text
interaction-events.v1
  -> interaction-events.validated.v1
  -> interaction-events.rejected.v1
```

The user play-count job consumes validated events and emits streaming features:

```text
interaction-events.validated.v1
  -> features.user-play-count-1h.v1
  -> interaction-events.user-play-count-1h.late.v1
```

The Flink jobs support bounded replay mode for deterministic integration
testing.

## Redis Online Store

Redis is the local streaming online store.

For `user_play_count_1h`, the store maintains the latest feature value for each
user using an explicit, feature-specific policy:

- A newer observation window replaces an older one.
- For the same observation window, the play count may only increase.
- Identical deliveries leave the stored value unchanged.
- A replayed older value cannot undo a later correction.
- Concurrent writers use bounded Redis `WATCH` retries.

The Redis key layout is:

```text
featureforge-stream:online:v1:user_play_count_1h:<user_id>
```

This policy is specific to append-only counts without retractions. It is not a
generic revision policy for arbitrary features and cannot repair an already
inflated count.

## Kafka-to-Redis Materialization

The Kafka-to-Redis materializer consumes the streaming feature topic and writes
validated feature records into Redis.

It uses explicit consumer-group offsets, synchronous offset commits, bounded or
continuous consumption, single-partition topic validation, Redis connectivity
validation, and guarded online-store writes.

The materializer writes before committing an offset. If a Redis write fails,
the corresponding Kafka offset is not committed, allowing a later run to retry
the record.

The materializer refuses an empty Kafka range rather than silently completing
without doing work.

## CI Boundaries

Streaming integration tests require infrastructure and must not run in the
baseline CI job.

The baseline job runs:

```bash
pytest -v -m "not integration"
```

This excludes tests marked with `@pytest.mark.integration`.

Streaming integration tests are executed in dedicated infrastructure-enabled
CI jobs:

| CI job | Infrastructure | Verification |
|---|---|---|
| Baseline lint and test | None | Unit and non-infrastructure tests |
| Redis and Feast serving integration | Redis | Redis store, Feast serving, offline/online parity |
| Flink Kafka-to-Redis integration | Kafka, Flink, Redis | Joined Kafka-to-Flink-to-Redis flow |

The Redis job explicitly runs the Redis online-store and concurrency tests and
validates that none are skipped.

The Flink job starts the complete streaming stack, waits for Kafka, JobManager,
and Redis health, and executes the joined Kafka-to-Redis test.

## Consequences

### Positive

- FeatureForge has one coherent platform boundary for offline and streaming
  feature workflows.
- Streaming contracts, reference semantics, and online-store policies live
  beside the platform code they support.
- The streaming path is covered by dedicated integration tests.
- Redis concurrency behavior is explicitly verified.
- The joined Kafka-to-Redis flow is verified end to end.
- CI separates infrastructure-free validation from infrastructure-dependent
  verification.
- Future streaming features can reuse the established contracts, runtime
  patterns, materializer, and test boundaries.

### Negative

- The repository now contains streaming runtime and infrastructure concerns in
  addition to offline feature-platform code.
- Local streaming verification requires Docker Compose and more moving services.
- CI requires an additional Flink Kafka-to-Redis integration job.
- The current streaming implementation is limited to one feature and a local
  Docker Compose runtime.
- Replay, late-event, checkpoint-recovery, and multi-partition behavior require
  additional dedicated verification.

## Alternatives Considered

### Keep FeatureForge Stream separate

This would preserve a smaller FeatureForge repository, but it would leave
streaming as a parallel prototype rather than part of the platform.

It would also make it harder to enforce shared contracts, test the joined
Kafka-to-Redis path, and evolve offline and streaming feature semantics
together.

### Put all integration tests in the baseline CI job

This would make the baseline job slower and dependent on Redis, Kafka, and
Flink. It would also cause infrastructure-dependent tests to fail when the
required services are not provisioned.

### Skip streaming tests when infrastructure is unavailable

This would keep the baseline suite green, but it would weaken the integration
guarantee. A skipped Redis or Flink test must not be treated as proof that the
streaming path works.

## References

- [ADR-001: Offline/Online Feature Store Split](ADR-001-offline-online-feature-store-split.md)
- [ADR-002: Canonical Offline Store Contract](ADR-002-canonical-offline-store-contract.md)
- [ADR-003: Feature Freshness SLOs and Fail-Safe Serving](ADR-003-freshness-slos-and-fail-safe-serving.md)
- [ADR-004: GitHub Actions CI for Reproducible Validation](ADR-004-github-actions-ci.md)
- [ADR-005: AWS S3 Offline-Store Production Profile](ADR-005-aws-s3-offline-store-profile.md)
- [Streaming Migration Provenance](../docs/streaming-migration.md)
