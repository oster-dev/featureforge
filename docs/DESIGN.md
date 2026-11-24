# FeatureForge design

Last updated 2026-11-24. Implemented behavior is described in [ARCHITECTURE.md](../ARCHITECTURE.md),
delivery status and every known limit in [STATUS.md](../STATUS.md), and single decisions in the
ADRs. This document explains how the parts fit together and why they were built this way.

## 1. Problem and scope

A feature value has to be the same whether a model trains on it or serves it. Training-serving skew
comes from a few places: two code paths that compute the "same" feature, different notions of time,
late and duplicate events, and values that serving held at a moment that a later recomputation does
not reproduce.

FeatureForge is a local reference implementation. It runs on Docker Compose with synthetic data and
makes no claim about production scale. Its value is in the explicit contracts and the evidence behind
them. These rules hold throughout:

- Event time, never ingestion time.
- Explicit time boundaries.
- Executable data contracts.
- Deterministic output paths.
- Fail before bad data reaches consumers.
- Commit offsets only after successful writes.
- Store policies are declared in the feature spec and each one is tested.
- Every guarantee links to a test, a measurement or a drill report. What is not proven is listed in
  [STATUS.md](../STATUS.md).

## 2. One spec drives everything

A feature is a YAML spec ([ADR-008](adr/ADR-008-feature-spec-design.md)). It declares the entity key,
the source, aggregations, derived values, the window, allowed lateness, deduplication, the online
policy, the offline target and a freshness SLO. Expressions use a small side-effect-free subset and
are evaluated without `eval`.

The spec model rejects inconsistent specs before anything runs:

- a slide that does not divide the window,
- a dedupe TTL shorter than window plus allowed lateness,
- an online TTL shorter than the window,
- monotone update policies with non-count aggregations,
- expressions outside the safe subset and fields that are not in the event contract.

The language is bounded on purpose. It is not a general SQL or streaming compiler. The Flink job and
the Spark backfill support count aggregations and one entity key today, and other operators fail with
an explicit error instead of being computed wrongly.

The same semantics exist in several implementations: an exact **spec reference**, the generic Flink
job, the Spark backfill and the spec-driven store. The reference is the oracle. The others are
compared with it on hand-calculated golden fixtures and on randomized inputs at window boundaries. A
parity test needs an answer that does not come from the code under test, and this is where it comes
from.

## 3. Time model

Details: [ADR-010](adr/ADR-010-time-and-availability-model.md).

**Event time.** Windows are right-closed, `(t - size, t]`. A late event corrects a window that was
already emitted, and an event that is too late for every window goes to a late topic. Event identity
is `event_id`. `ingested_at` is never part of the identity, and identities are checked across the whole
input before any filtering. A missing value is never a zero: `avg`, `min` and `max` over no events are
missing.

**Availability.** Event time says what a value should be. It does not say when serving could first
return it. For that, every accepted online update gets a `published_at`, read from the UTC clock by the
materializer after the store reports `written`. The order is fixed: Redis write, evidence append, Kafka
offset commit. After a crash between the first two steps, the replay appends the evidence with a later
`published_at`. So `published_at` is an upper bound of the real publication, never an earlier time.
`computed_at` was rejected as a retrieval axis, because a replay under at-least-once delivery would
produce a different value for the same update.

**Two retrieval modes that never substitute each other.**

- `event_time` reconstructs a value from the events over `(t - size, t]`, including events that arrived
  after `t`.
- `published_as_of` returns what serving held at `t`: the latest version with `published_at` at or
  before `t`, and missing once `t` reaches `published_at + online.ttl`. Never zero.

A dataset names its mode in the call and in its manifest.

## 4. Online correction and the write path

Details: [ADR-009](adr/ADR-009-spec-driven-online-stores.md).

The generic Flink job emits an updated count vector for a window whenever late events change it. The
store then has to decide what to do with an arriving vector. One decision function compares it with the
stored one under the update policy of the spec:

- `append_only`: exactly one count aggregation, ordered by window and count.
- `componentwise_monotonic`: several counts, compared per component. An incomparable vector raises and
  leaves the stored value unchanged.
- `window_replace` (non-count operators) is declared but no store implements it yet.

The outcome is `written`, `unchanged` or `stale`. Only `written` sets the Redis expiry `online.ttl`,
so a replayed or stale write cannot extend the life of a key. The write is guarded and retried at most
16 times. A concurrent newer write that lands during a retry is preserved.

The sinks are at-least-once. Correctness comes from deduplication in the job and from guarded, idempotent
writes, not from exactly-once delivery. Kafka, Redis and Iceberg do not share a transaction, and the
design never claims an atomic publication across them.

## 5. Store contract and serving

Details: [ADR-013](adr/ADR-013-online-store-protocol.md) and
[ADR-016](adr/ADR-016-feature-serving.md).

**One contract, two backends.** `OnlineStore` is a protocol with two methods: read an entity value, and
write a record with an outcome. Validation, the decision function and the outcomes are shared. A backend
supplies only the read, the guarded write and the expiry. One contract test module runs against both
backends, and the stored payload is identical.

| | Redis | Cassandra |
|---|---|---|
| Guarded write | `WATCH`/`MULTI`, at most 16 attempts | `INSERT ... IF NOT EXISTS`, `UPDATE ... IF payload = <value read>` (lightweight transaction), at most 16 attempts |
| Expiry | `SET ... EX online.ttl` | `USING TTL` |
| Consistency | single node | `LOCAL_SERIAL` for the Paxos phase, `LOCAL_QUORUM` for commit and reads |
| Layout | one key per spec, version and entity | one table, partition `(entity_key, entity_value)`, clustering column `feature` |
| Read in the service | one `MGET` per request | one partition read per entity, run concurrently |

A replacement in Cassandra costs a read at serial consistency and a Paxos round. The write path is not
measured yet.

**The serving service.** A FastAPI process reads through a `ServingReader`, chosen by `online.store`.
One request names several entities and features (at most 100 and 50) and gets one entry per pair. Each
entry has a status:

- `ok`: present and fresh.
- `missing`: no key, unknown entity or expired key.
- `stale`: present but older than the freshness SLO, or from the future.
- `backend_error`: a timeout, a connection error or a record that fails validation.

A value is never replaced by a silent zero. A reader raises when its backend fails and never returns a
partial result, so the service marks every entry of that request. The per-feature fallback policy is
`omit`, `default_value` or `fail`, and `fail` answers 503. There are no retries in the request path,
because a retry hides tail latency inside the budget. The budget is 50 ms per request, enforced as a
socket timeout. The response states that it is not an atomic snapshot.

**Measured numbers** (one laptop, one service process, details and limits in
[docs/benchmarks.md](benchmarks.md)):

| Serving backend | Highest measured rate with p99 at most 25 ms | p99 at that rate | Saturation of one process | CPU per request |
|---|---|---|---|---|
| Redis 7.4 | 3,000 req/s | 6.8 to 7.7 ms | about 4,000 req/s | 0.28 to 0.33 ms |
| Cassandra 5.0, one node | 1,500 req/s | 16 to 23 ms | about 2,100 req/s | 0.46 to 0.50 ms |

At saturation the limit is the service process (0.98 cores with Cassandra), not the database. Why
Cassandra needs about 1.5 times the CPU per request is not profiled: it could be the partition reads,
the driver or the conversion of rows.

## 6. History and the event fact store

Details: [ADR-011](adr/ADR-011-history-table-layout.md) and
[ADR-015](adr/ADR-015-iceberg-event-fact-store.md).

**Publication history.** Every spec has one Iceberg table with the evidence of its accepted updates:
`update_id` (SHA-256 of the identity fields), the entity, the observation time, the counts and
`published_at`, partitioned by the day of `published_at`. The append is idempotent: the first row of an
update identity wins. PyIceberg writes, Spark reads, through a SQLite or a PostgreSQL catalog. The check
and the append are not atomic, so there is one appender per table. A schema derived from the spec that
differs from the table fails loudly.

**Event fact store.** Validated events are kept as immutable facts in `facts.interaction_events`,
partitioned by the day of `event_time` and with `ingested_at`. A separate writer reads the raw topic in
its own consumer group, before the Flink job drops `ingested_at`. It validates, appends and only then
commits the offset. A repeated `event_id` is dropped, and the same id with another payload fails and
writes nothing. A dataset built from the table records the snapshot id in its manifest, so the same
snapshot gives the same rows.

The writer and the Flink job read the topic independently. An event can be online before it is in the
table, or the other way round, so a dataset from a snapshot can miss an event that serving used. The
writer lag is not observable yet.

## 7. Training datasets

Details: [ADR-012](adr/ADR-012-label-specs.md) and
[ADR-014](adr/ADR-014-training-datasets.md).

A label is a spec of its own, because a feature window ends at the observation time and a label window
starts after it. The label window is `(t, t + horizon]`. A window that is not complete by `data_end` is
excluded and counted, and a label is never null.

The leakage rule holds in both modes: features use only information up to `t` and the label only events
strictly after `t`. An event at `t` belongs to the features. In `published_as_of` mode, an observation
before the latest history start has no evidence either way, so it is excluded and counted, not filled
with null.

The dataset is Parquet partitioned by the UTC day of the observation, written by Spark, never
overwritten, with a `manifest.json` written last. The manifest holds the mode, the spec versions,
`data_end`, the counts, every partition with its rows and, where it applies, the snapshot ids and the
history start. A directory without a manifest is not a dataset. The Arrow loader reads datasets about
twice as fast as `pandas.read_parquet` and adds six to nine times less peak memory (see ADR-014 for the
setup).

## 8. Operations

- **Metrics.** The service exports a lookup duration histogram, the entries by status and the requests
  by outcome, and Prometheus and a provisioned Grafana dashboard run in the Compose stack
  ([docs/observability.md](observability.md)). The histogram measures the time inside the lookup, not the
  HTTP layer.
- **Benchmarks.** One command runs the benchmark against a backend (`make bench-record`) with safety
  checks: a clean tree and only the services of that backend. A compare script reduces repeats to medians
  and marks a change only when it is at least 10 percent and the ranges of the repeats do not overlap.
  The threshold comes from the difference between two runs of the same code.
- **CI.** A baseline job (Ruff, mypy, tests without infrastructure), a compatibility job for Python 3.11
  and 3.12, a serving integration job (Redis, Feast, history and datasets), a Cassandra job, a smoke run
  of the benchmark against Redis, and a streaming integration job (Kafka, Flink, Redis). A nightly run,
  concurrency and timeouts. Each Flink test module starts from a fresh TaskManager.
- **Runbooks** for stale partitions, invalid windows and a missing Feast repository are in
  [docs/runbooks](runbooks).

There are no alert rules yet, no recovery drills beyond a TaskManager crash, no soak run and no table
maintenance for the Iceberg tables.

## 9. Known limits

The full list is in [STATUS.md](../STATUS.md). The ones that shape the design:

- Recovery is shown for a TaskManager crash and a planned stop with a savepoint. JobManager failure is
  not proven, and checkpoints live in JobManager memory.
- Topics have one partition. Scaling over partitions is not demonstrated.
- The engines support count aggregations and one entity key. `published_as_of` needs one entity key.
- There is no store for `window_replace`, so non-count features cannot be served online yet.
- The event table has one writer and no compaction or retention. Its append slows down with the number of
  data files (about 4 ms per file).
- Serving is measured on one machine and one process. Single runs show short bursts of `backend_error`
  with both backends; the cause is open. A longer warm-up does not explain them
  ([docs/benchmarks.md](benchmarks.md)).
- The Feast-based batch path still exists next to the spec path and is not yet defined as specs.

## 10. Comparison with Feast, Chronon and Axion

This comparison uses the public documentation of the other systems (links below). FeatureForge has not
been benchmarked against any of them, and it is a much smaller system.

| | Feast | Chronon | FeatureForge |
|---|---|---|---|
| Definition | Feature views over a data source | `GroupBy` and `Join` in Python, with windows and aggregations | A YAML spec with a bounded language |
| Online and offline | Offline store and online store, linked by materialization | Same declarative definition for batch training data and real-time serving | One spec drives Flink, Spark, store, history and datasets, compared with an exact reference |
| Point in time | `get_historical_features` joins the latest value at or before the row's event timestamp | `TEMPORAL` accuracy: computed in real time for serving and point-in-time correct for backfills; `SNAPSHOT` is midnight accuracy | Two modes: `event_time` reconstruction and `published_as_of`, which uses what serving held at the time |
| Streaming | Push API writes into the online (and offline) store | Streaming updates to the online key-value store | Flink job, guarded materializer, publication history |
| Online store | The online store holds only the latest values | Key-value store | Redis or Cassandra behind one contract and a contract test suite |

What is different on purpose: FeatureForge records when serving published a value, so a training
dataset can state what serving held and not only what event time implies. It also keeps raw facts in
Iceberg, so datasets are rebuilt from a pinned snapshot. This is the pattern of the Axion fact store at
Netflix (facts in Iceberg, shared feature encoders for online and offline, temporally accurate
regeneration), as an inspiration and not a copy. The encoder here is the spec engine, whose references,
Flink job and Spark backfill agree on golden fixtures.

What is smaller: the engines support count and ratio features only, there is no sawtooth window
(ADR-008 names it as the alternative if state growth becomes the bottleneck), and everything runs locally
on synthetic data.

### References

- [Feast: feature retrieval](https://docs.feast.dev/getting-started/concepts/feature-retrieval) and
  [overview](https://docs.feast.dev/getting-started/components/overview)
- [Chronon: GroupBy](https://chronon.ai/authoring_features/GroupBy.html) and
  [Airbnb Engineering: Chronon](https://airbnb.tech/opensource/chronon/)
- [Netflix: Evolution of ML Fact Store](https://netflixtechblog.com/evolution-of-ml-fact-store-5941d3231762)
