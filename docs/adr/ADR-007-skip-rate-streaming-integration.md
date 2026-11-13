# ADR-007: Integrate User Skip Rate 24h Streaming Feature

- Status: Accepted
- Date: 2026-11-11
- Updated: 2026-11-13 (see the update note below)
- Scope: Streaming feature computation, online materialization, and integration verification

> **Update 2026-11-13:** The Flink job described in this ADR,
> `jobs/user_skip_rate_24h.py`, was replaced by the generic spec-driven job
> `jobs/feature_from_spec.py` with `feature_repo/specs/user_skip_rate_24h.yaml`
> (ADR-008, WP1 stage 4). The behavior described below is unchanged: window,
> watermark, lateness, deduplication, conflict handling and output records are
> the same, and the same integration tests verify them on the generic job. The
> Redis update policy, the materializer and the guarded store described here
> are unchanged and still hand-written. The hand-written job is preserved in
> the tag `hand-written-jobs-final`. The verification results below record the
> state of 2026-11-11.

## Context

FeatureForge already provides a locally verified streaming path for
`user_play_count_1h`. That path includes Kafka validation and routing,
Flink event-time computation, Kafka feature output, guarded Redis
materialization, bounded replay idempotency, and explicit event-identity
conflict handling.

The platform also needs a second streaming feature that demonstrates a
different online-update semantic. A play count is append-only within one
window: its count can increase, but not decrease. A skip rate, however,
can move in either direction when additional plays or skips are accepted
for the same observation window.

The frozen `featureforge-stream` repository contains a proven reference
implementation for `user_skip_rate_24h` at tag `v0.1.0`, commit
`a681c281806af492a05b2b4a141f2683f752e1a9`. FeatureForge should selectively
integrate that capability rather than maintain a second feature platform.

## Decision

Integrate `user_skip_rate_24h` into FeatureForge as a separate streaming
feature with its own contract, deterministic reference, Flink job, guarded
Redis store, Kafka-to-Redis materializer, and integration tests.

The feature computes:

```text
skip_count / (play_count + skip_count)
```

over the right-closed event-time window:

```text
(observation_time - 24 hours, observation_time]
```

The implementation preserves the source repository's semantics while
adapting package imports, target paths, test selection, and documentation
to FeatureForge.

## Feature semantics

The deterministic Python reference accepts validated
`InteractionEventV1` records and calculates a user-level skip rate.

Logical event identity is `event_id`. Repeated identical logical events
contribute once. `ingested_at` is excluded from logical identity because
it is receipt metadata.

Identity validation covers the complete supplied input before filters are
applied. Therefore, a conflicting payload for another user, another event
type, or outside the requested window still raises
`EventIdentityConflictError`.

Only `play` events increment `play_count`. Only `skip` events increment
`skip_count`. `impression` and `complete` events contribute to neither
count.

A zero denominator returns `0.0`. The returned rate is a float between
`0.0` and `1.0`, not a percentage.

The reference reconstructs event-time values from supplied records. It
does not establish historical online availability, leakage-safe retrieval,
watermark admission, or allowed-lateness behavior.

## Flink computation

`jobs/user_skip_rate_24h.py` consumes validated interaction events from
Kafka and publishes feature updates to Kafka.

The job uses:

- right-closed event-time mapping;
- 24-hour sliding windows;
- a one-minute slide;
- a 10-minute bounded-out-of-orderness watermark strategy;
- 30 minutes of allowed lateness;
- a side output for events too late for all assigned windows;
- event-ID-based deduplication;
- explicit failure for conflicting logical payloads;
- a 48-hour processing-time deduplication-state TTL;
- checkpointing every 10 seconds;
- continuous and bounded execution modes;
- an optional fail-fast `--no-restart` mode.

The job emits independent `play_count` and `skip_count` values plus their
computed `skip_rate`. It does not collapse the two counts into a rate-only
representation.

The 48-hour deduplication TTL is longer than the play-count job's
24-hour TTL. This is a feature-specific retention decision, not unlimited
replay protection.

## Redis update policy

`src/featureforge/streaming/skip_rate_store.py` stores the latest accepted
observation window per user in a feature-specific Redis namespace.

The policy compares `(play_count, skip_count)` componentwise within the
same observation window. It does not compare skip rates by magnitude.

| Incoming feature | Decision |
|---|---|
| Newer observation time | `written` |
| Older observation time | `stale` |
| Same time, identical count vector | `unchanged` |
| Both counts at least as large, one larger | `written` |
| Both counts at most as large, one smaller | `stale` |
| One count increases and the other decreases | `SkipRateUpdateConflictError` |

A lower skip rate is not automatically stale. For example, changing
`(1, 1)` to `(2, 1)` lowers the rate from `0.5` to `1/3`, but is a valid
correction because the play count increases.

The store does not merge conflicting count vectors by taking componentwise
maxima. Such a merge could produce a result the Flink job never emitted.

A newer window with a zero count vector replaces an older nonzero window.
This is the current state of a rolling 24-hour window, not a retraction of
historical data.

The store uses Redis optimistic concurrency through `WATCH`, `MULTI`, and
`EXEC`. It retries after a `WatchError`, with a maximum of 16 attempts.

## Materialization

`src/featureforge/streaming/materialize_skip_rate.py` consumes skip-rate
feature records from Kafka and writes them through the guarded Redis store.

The materializer supports exactly one Kafka partition. It rejects empty
readable ranges and feature tombstones. It validates each feature payload
before writing it to Redis.

Redis processing precedes Kafka offset commit. A failed record is not
committed as successfully processed.

A Redis write and a Kafka offset commit are not one distributed
transaction. A failure between them can cause redelivery.

## CI integration

The Flink integration CI job now selects the following additional test
modules:

- `tests/integration/test_user_skip_rate_job.py`;
- `tests/integration/test_skip_rate_flink_to_redis.py`;
- `tests/integration/test_skip_rate_materialization.py`;
- `tests/integration/test_redis_skip_rate_store.py`.

The Redis store tests run in the Flink integration job because they verify
the feature-specific guarded write policy against live Redis. The existing
Redis and Feast serving job continues to cover the play-count Redis store,
concurrency behavior, and unified online access.

## Verification

The following local results were recorded on November 11, 2026:

| Test module | Result |
|---|---|
| `tests/unit/test_skip_rate_update_policy.py` | 81 passed |
| `tests/integration/test_redis_skip_rate_store.py` | 12 passed in 0.74s |
| `tests/integration/test_skip_rate_materialization.py` | 1 passed in 1.91s |
| `tests/integration/test_skip_rate_flink_to_redis.py` | 1 passed in 40.90s |
| `tests/integration/test_user_skip_rate_job.py` | 8 passed in 176.64s |

The combined Flink-to-Redis test verifies the path from actual Flink
feature output through Kafka and the real materializer into Redis.

It covers:

- late corrections in both rate directions;
- duplicate suppression;
- replay of retained feature output through a fresh materializer
  consumer group;
- Redis values, counts, rate, and window metadata.

The isolated Flink-job test verifies golden reference values, late
corrections, duplicate suppression, and too-late routing.

## Guarantee boundaries

These results establish the tested component and integration paths. They
do not establish:

- checkpoint or savepoint recovery;
- deduplication behavior after the 48-hour state TTL expires;
- sustained continuous-operation behavior or performance targets;
- multi-partition Kafka or Redis materialization;
- end-to-end exactly-once delivery;
- versioned streaming-history publication;
- skip-rate snapshot export or historical retrieval;
- production readiness.

The Flink Kafka sinks remain configured for `AT_LEAST_ONCE` delivery.
Passing replay, correction, duplicate, and guarded-write tests must not be
described as an end-to-end exactly-once guarantee.

## Consequences

FeatureForge now has two integrated streaming features with distinct
online-update semantics:

| Feature | Same-window policy |
|---|---|
| `user_play_count_1h` | Append-only count; count may only increase |
| `user_skip_rate_24h` | Componentwise count-vector ordering; rate may rise or fall |

The skip-rate integration demonstrates that FeatureForge can preserve
feature-specific update semantics instead of forcing every streaming
feature into one generic monotonic-count policy.

Future streaming features must declare their own update policy, deduplication
horizon, late-event behavior, and materialization boundary. A shared
infrastructure component must not silently impose play-count semantics on
another feature.