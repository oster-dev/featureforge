# ADR-010 Time and availability model for streaming history

- Status: Proposed. Decides the time model of WP2. Nothing is implemented, and no `published_at` is fabricated.
- Date: 2026-11-16
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-006, ADR-007, ADR-009

## Context

The blueprint (section 6) names four times: `event_time`, `observation_time`, `computed_at` and `published_at`. Only the first two exist in code. Reading the code shows what the history has to build on.

- `FeatureRecord` carries `observation_time` and `window_start_exclusive`. It has no spec version, no computed time and no publication time. The version appears only in the Redis key namespace `v{spec.version}:{spec.name}:{entity}`.
- `ingested_at` is receipt metadata. It is excluded from event identity (ADR-007, ADR-008) and dropped before the generic Flink job (`jobs/feature_from_spec.py`). The online path has no time for when the system saw something.
- The offline contract expects one row per entity and `observation_time` (`unique_entity_observation_time`, `single_observation_time`). A streaming window is published again when a late event raises its count. One entity and one `observation_time` can therefore carry several values. The decision function (ADR-009) accepts this as `written`, `unchanged` or `stale`.
- The blueprint fixes four rules. An ingestion timestamp is not an availability timestamp. A later backfill must not rewrite the recorded availability of an earlier online value. Published-as-of retrieval uses only versions with recorded publication evidence. The online store and the history are not assumed to update atomically.
- `STATUS.md` states the open question as `event_time` vs. `published_at`. That is the wrong pair. `event_time` is when the interaction happened and never an availability time. The open question is who records `published_at` and what happens when the two stores disagree.

Goal: a stream feature can be retrieved as it was visible to serving at a given time, without a value that serving did not have at that time.

## Decision

| Concern | Rule |
|---|---|
| Availability axis | `published_at` of the online sink. For retrieval it is the time the spec-driven Redis store accepted an update. The Iceberg commit time is not an availability time. Iceberg snapshots carry their own commit timestamp. |
| `computed_at` | Not part of the retrieval contract. It would be assigned inside Flink, and a replay under at-least-once delivery produces a different value for the same update. If it is added for lag diagnosis, it is excluded from update identity like `ingested_at`. |
| Who assigns `published_at` | The materializer reads the UTC wall clock after the store returns `written`. The Flink record and the Redis value format do not change. |
| `stale` | No evidence. The value was never visible. |
| `unchanged` | Evidence only if the history has none for that update. It does not extend availability, because it does not refresh `online.ttl`. |
| Update identity | Spec name, spec version, entity value, `observation_time` and the count vector. A redelivery has the same identity. A correction with a higher count has a new one. |
| Not part of identity | Kafka offsets (scoped to one topic partition, different on replay), `ingested_at`, `computed_at`, `published_at`. |
| Namespace | A new spec version opens a new namespace (blueprint section 7). |
| Failure order | Redis write, then evidence append, then Kafka offset commit. The evidence is appended idempotently by update identity, and the first row wins. |
| Failure after Redis | The replay appends the evidence with a later `published_at`. After a recovery, `published_at` is an upper bound of the real visibility, not an exact value. |
| Retrieval modes | Event-time reconstruction and published-as-of retrieval are separate calls, never substituted for each other. |
| Backfill | A backfill writes only to event-time reconstruction and never writes a `published_at`. |
| Published-as-of rule | A version is available at `T` if its `published_at` is at or before `T`, no later accepted version has a `published_at` at or before `T`, and `T` is before `published_at` plus `online.ttl`. Past that the value is missing, never zero. |

## Consequences

- Positive: the history can hold several versions per entity and `observation_time`, keyed by update identity and `published_at`.
- Positive: published-as-of retrieval never reports a value earlier than serving had it. The failure direction is late, not early.
- Positive: the Flink job, the feature record and the Redis value format stay unchanged.
- Negative: only the spec-driven store records evidence. The hand-written stores of `user_play_count_1h` and `user_skip_rate_24h` write without expiry and without evidence. They qualify after the thin wrappers (open item of WP1).
- Negative: `published_at` is one materializer clock reading. Clock skew between several materializer processes is not handled. Topics are single-partition today.
- Negative: after a crash between Redis and the history append, `published_at` is later than the real publication. Training sees such a value later than serving did, never earlier.
- Negative: the offline uniqueness checks (`unique_entity_observation_time`) do not apply to the streaming history. It needs its own contract.

## Alternatives considered

- `computed_at` as the retrieval axis: rejected. A computed update can be unreadable for some time, so training would see values that serving did not have.
- Evidence before the Redis write: rejected. A crash between the two steps would claim availability of a value that serving never had.
- Kafka offset as revision number: rejected. Offsets are scoped to a partition, and the blueprint allows a revision only where a producer can assign it consistently.
- Atomic write to Redis and history: not available. The stores do not share a transaction (`atomic: false`).

## Verification

Nothing here is implemented. WP2 has to add tests for the following.

- Idempotent evidence on redelivery.
- No evidence on `stale`.
- A crash between Redis and the history append: the replay yields a later `published_at`, never an earlier one.
- The TTL cutoff of the published-as-of rule.
- A backfill leaves recorded evidence unchanged.

## Open items

- The history table layout (WP2, next ADR).
- Who runs the evidence append: the materializer itself or a second consumer.
- Behaviour with several partitions and several materializer processes.
- Whether `computed_at` is added for lag diagnosis.