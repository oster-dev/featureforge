# ADR-011 History table layout and evidence append for streaming features

- Status: Accepted. Decides the layout of the streaming history and who appends it. The table, the update identity and the idempotent append are implemented. The materializer does not call the append yet.
- Date: 2026-11-16
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-009, ADR-010

## Context

ADR-010 decides the time model and leaves four points open: the layout of the history table, who runs the evidence append, the behaviour with several partitions and materializer processes, and whether `computed_at` is added.

- Every spec already declares its offline target, for example `offline: {store: iceberg, table: features.user_skip_rate_24h}`. No code reads it yet.
- The spec-driven materializer (`src/featureforge/streaming/materialize_spec.py`) applies the guarded store write and learns the outcome `written`, `unchanged` or `stale`. It commits Kafka offsets in batches after Redis succeeded.
- Local feasibility was checked on 2026-11-16 outside the repository. PyIceberg 0.12.0 wrote a table through its SQL catalog on a SQLite file, and Spark 3.5 read the same table through the Iceberg JDBC catalog (`iceberg-spark-runtime-3.5_2.12` 1.12.0, `sqlite-jdbc` 3.46.1.3). This is a feasibility check, not a test in the repository.

Goal: record publication evidence for every accepted online update so that ADR-010 can be answered from a table that Spark and PyIceberg both read.

## Decision

| Concern | Rule |
|---|---|
| Who appends | The spec-driven materializer, in the process that applies the store write. The order is the one of ADR-010: Redis write, evidence append, Kafka offset commit. A second consumer is rejected, because it sees the topic but not the store outcome. |
| Which outcomes | `written` appends evidence. `unchanged` appends evidence only if the history has none for that update identity. `stale` appends nothing (ADR-010). |
| `published_at` | Read from the UTC wall clock right after the store returns, per record, and kept with the buffered row. The append happens once per batch before the offset commit, so batching does not blur the time. |
| Catalog, local profile | PyIceberg SQL catalog on a SQLite file with a local filesystem warehouse. Spark reads it through the Iceberg JDBC catalog. This profile has one writer process per catalog. A production catalog (REST or Glue) is a separate decision. |
| Table | One table per spec. The name is `offline.table` of the spec, split at the first dot into namespace and table. The materializer creates the table from the spec on first use and never by hand. |
| Columns | `update_id` (string), `spec_version` (int), the entity column named by the spec entity key (string), `observation_time` and `window_start_exclusive` (timestamp with time zone, UTC), one long per count aggregation named as in the spec, one nullable double per derived value named as in the spec, and `published_at` (timestamp with time zone, UTC). |
| Not stored | `computed_at`, Kafka offsets and `ingested_at`. They are not part of update identity (ADR-010). |
| `update_id` | Lowercase hex SHA-256 of the canonical JSON of the identity fields of ADR-010: spec name, spec version, entity value, `observation_time` as UTC ISO string with microseconds, and the counts in spec order. |
| Idempotent append | Before an append, rows whose `update_id` already exists in the table, or twice in the batch, are dropped. The first row wins. The check and the append are not atomic and are safe only with one appender per table. |
| Partitioning | `day(published_at)`. Published-as-of queries filter on `published_at`. Entity-based partitions are rejected. |
| Schema check | If the schema derived from the spec differs from the existing table, the materializer fails with a clear error. It never alters a schema silently. |
| Spec versions | A new spec version writes to the same table with its own `spec_version` value. A spec change that alters the column set needs a new table name. |
| Backfill | A backfill never writes to this table (ADR-010). |

## Consequences

- Positive: a new spec yields its history table without platform code, as soon as the append exists. Spark and PyIceberg read the same table.
- Positive: `published_at` stays exact per record, while the table receives one append per batch.
- Negative: the SQLite catalog allows one writer. Several materializer processes need a catalog with optimistic commits, which is a later decision.
- Negative: check-then-append is not atomic. A second appender can create a duplicate update identity.
- Negative: after a crash between Redis and the append, the replay yields `unchanged` for records that Redis already holds, and `published_at` becomes an upper bound (ADR-010).
- Negative: small batches create many small files. Compaction and retention are not decided here.

## Alternatives considered

- A second consumer that appends from the feature topic: rejected. It cannot tell `written` from `stale` and has no reading of the store clock.
- One generic table with a map of counts: rejected. It loses types, makes SQL harder and prevents per-spec evolution.
- The REST catalog with an object store first: deferred. It needs two more services, and the local profile does not need them.
- The Iceberg commit time as `published_at`: rejected by ADR-010.
- Iceberg MERGE for first-row-wins: not chosen. It would put Spark into the materializer path and is not verified here.

## Verification

The table, the update identity and the idempotent append are implemented and tested in `tests/unit/test_history.py` and `tests/integration/test_history_spark_read.py`. The evidence append in the materializer is not implemented yet. The tests of ADR-010 apply, plus the following.

- PyIceberg writes a spec-derived table and Spark reads the same rows (the feasibility check as a test).
- `update_id` is stable across a replay and differs for a higher count.
- A duplicate update identity, in the table or in one batch, adds no row.
- A schema derived from a changed spec fails loudly against the existing table.
- `published_at` of a replayed `unchanged` record is later than the original, never earlier.

## Open items

- Several appender processes and several partitions.
- Compaction and retention of the history table.
- The published-as-of read function and its CLI.
- Whether `computed_at` is added for lag diagnosis (ADR-010).
