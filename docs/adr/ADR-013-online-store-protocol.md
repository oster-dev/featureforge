# ADR-013 Online store protocol and a Cassandra backend

- Status: Accepted. Decides the contract that every online store of a spec keeps and the Cassandra backend behind it. The protocol, the factory, the Redis store and the Cassandra store are implemented, and one contract suite runs for both. The materializer writes to the backend that the spec names. The service reads Cassandra through `CassandraServingReader` ([ADR-016](ADR-016-feature-serving.md)). Redis stays the reference implementation.
- Date: 2026-11-18
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-003, ADR-009, ADR-010

## Context

ADR-009 left a second online backend and the store protocol that goes with it out of scope. Reading `RedisSpecStore` shows what a second backend has to match.

- The store has two methods: `read(entity_value)` returns a validated `FeatureRecord` or `None`, and `write(record)` returns `written`, `unchanged` or `stale`.
- The decision is `decide_count_vector_update(stored, incoming)` over count vectors. The record is validated against the spec by `parse_feature_record` before the store is touched. Entity values and key prefixes are checked by `check_text`. None of this depends on Redis.
- Three things depend on Redis: how the stored value is read, how the guarded write is applied (`WATCH`/`MULTI`, at most 16 attempts) and how the expiry is set (`SET ... EX online.ttl`, only on `written`).
- ADR-010 needs the outcome of every write. `written` appends evidence to the history, `unchanged` appends it only if none exists, and `stale` appends nothing. A backend that cannot report the outcome cannot feed the history.
- The materializer commits Kafka offsets only after the write and the evidence append. A replay decides again, so a repeated write is safe as long as the decision is idempotent.
- `RedisSpecStore` rejects a spec whose `online.store` is not `redis`.
- Apache Cassandra offers conditional writes through lightweight transactions. They run Paxos, apply only within one partition and report `[applied]` with the current values when they fail. Time to live is set per column with `USING TTL`. These are properties from the documentation. Nothing here is measured.

Goal: the same spec runs on Redis or Cassandra, and one contract test suite is green for both.

## Decision

| Concern | Rule |
|---|---|
| Protocol | `OnlineStore` is a `typing.Protocol` with `read(entity_value)` returning a `FeatureRecord` or `None`, and `write(record)` returning a `WriteOutcome`. Backend details such as `key_for` stay outside it. |
| Shared logic | Record validation, `decide_count_vector_update`, `check_text` and the policy check are reused by every backend. A backend supplies only reading, the guarded write and the expiry. |
| Unknown entity | `read` returns `None`, never zero (ADR-003). |
| Invalid input | A record that does not match the spec raises before the store is touched. An unsupported update policy or a composite entity key raises `UnsupportedPolicyError` at construction. |
| Outcomes | `written`, `unchanged` and `stale` follow the decision function of ADR-009. An incomparable vector raises and leaves the stored value unchanged. |
| Concurrency | The write is guarded and retried at most 16 times. After that it raises. |
| Expiry | The expiry is `online.ttl`, runs in wall-clock time from the last accepted write and is set only on `written`. `unchanged` and `stale` do not extend it. An expired value reads as `None`. |
| Namespace | `v{spec.version}:{spec.name}` per spec. A new spec version opens a new namespace. |
| Backend choice | `online.store` in the spec accepts `redis` and `cassandra`. A factory returns the store for a spec. The Redis store keeps rejecting specs for another backend. |
| Cassandra table | One generic table, `online_features`, with the partition key `(entity_key, entity_value)`, the clustering column `feature` and the column `payload` (text). The partition holds all features of one entity, so a multi-feature lookup of one entity reads one partition. The entity key name is part of the partition, so a user and a content id with the same text cannot collide. |
| Payload | The JSON of the Redis value, unchanged. The wire format does not change. |
| First write | `INSERT ... IF NOT EXISTS USING TTL`. |
| Replacement | `UPDATE ... USING TTL ? SET payload = ? WHERE ... IF payload = ?`, with the value read at serial consistency. The condition compares the whole payload, like the watched key in Redis. |
| Contention | If `[applied]` is false, the store decides again from the returned current value. After 16 attempts it raises. |
| Unknown outcome | A timeout of the conditional write propagates. The materializer does not commit the offset, and the replay decides again. It sees `unchanged` if the write was applied and `written` if not. `published_at` stays an upper bound (ADR-010). |
| Consistency | Conditional writes use `LOCAL_SERIAL` for the Paxos phase and `LOCAL_QUORUM` for the commit. Serving reads use `LOCAL_QUORUM`. Both are constructor arguments and not part of the spec. A single local node cannot show any difference between the levels. |
| Schema | `ensure_schema` creates the keyspace and the table if absent. It never alters them. A table that does not match fails loudly, as the history table does (ADR-011). |
| Local profile | One Cassandra node in Docker Compose behind its own profile, bound to localhost. The version is pinned in the implementation. |

## Consequences

- Positive: a new backend implements three things and inherits the decision, the validation and the outcomes. Redis and Cassandra cannot drift apart in their rules.
- Positive: the outcome stays available for the evidence of ADR-010, because the decision runs in the store and not in the database.
- Positive: a partition per entity keeps the batch lookup of one entity to one read, which the serving service uses.
- Negative: every replacement costs a read at serial consistency and a Paxos round on the conditional write. The documentation describes several round trips per conditional write. The cost is measured in the serving load test and not assumed here.
- Negative: reads at `LOCAL_QUORUM` are not linearizable. Serving can briefly see the value from before a conditional write that is still running. The failure direction is old, never invented.
- Negative: the single local node proves the protocol logic. It proves nothing about replication, several datacenters or failure of a node.
- Negative: the payload is compared as a whole. A larger record makes the condition more expensive.
- Negative: the Paxos state is kept per partition. Two features of one entity contend with each other on writes, although they sit in different rows. The partition per entity favors the read of one entity over concurrent writes of its features.
- Neutral: a payload that returns to an earlier value between the read and the conditional write (A to B to A) is harmless, because the decision depends only on the stored value.

## Alternatives considered

- Last-write-wins with `USING TIMESTAMP` from the window time: rejected. It reports no outcome, so the history cannot tell `written` from `stale`. It also cannot compare a count vector within one window, so an incomparable correction would be lost without an error.
- Conditions on typed count columns, for example `IF c1 <= ? AND c2 <= ?`: rejected. The rule needs "newer window, or same window and componentwise greater", and the condition language of CQL has no OR. The decision function stays the single source of the rule.
- Cassandra counter columns: rejected. Counters are not idempotent, and a replay would count twice.
- One table per spec: rejected. A new spec would need DDL, which breaks "no platform code per feature".
- A new store-neutral exception for exhausted retries: deferred. Redis raises `WatchError` today, and its tests rely on that.

## Verification

One contract test module runs against both backends. Each backend provides a fixture with the store, a way to read the remaining expiry, the exception for exhausted retries, and a way to inject a concurrent write.

- An unknown entity reads `None`.
- The first write is `written`, and the record reads back equal.
- The decision table of ADR-009: newer window, older window, equal vector, greater vector, smaller vector, and an incomparable vector that raises and keeps the stored value.
- A newer window with a count of zero replaces an older value.
- An invalid record, an unsupported policy and a composite key fail before the store is touched.
- The remaining expiry after a write is positive and at most `online.ttl`. `unchanged` and `stale` do not extend it. An expired value reads as `None`.
- A concurrent newer write that lands during the retry is preserved.
- Exhausted retries raise after 16 attempts.
- A new spec version opens a new namespace.
- The stored payload of both backends is identical for the same record.
- `content_play_count_1h` flows from Kafka through the generic job and the materializer into Cassandra, read back through the store, with no new Python file.

## Open items

- The Python driver and its support for the Python version of the project.
- The Cassandra version and its Paxos implementation. The behavior of conditional writes differs between versions.
- The row expiry in detail: whether a row survives its payload. The expiry test has to prove it.
- A store-neutral exception for contention and unknown outcomes.
- Serving reads at `SERIAL`, and what they cost. The serving load test measures it.
- The read path of the service is decided in ADR-016 (one reader per backend) and implemented backend by backend.
- Several nodes and several datacenters.
- Creating the schema from a production service rather than the local profile.
