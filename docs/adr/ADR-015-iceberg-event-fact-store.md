# ADR-015 Iceberg event fact store and snapshot-pinned event-time datasets

- Status: Proposed. Decides what the event fact store holds, who writes it and how datasets read it. Nothing is implemented.
- Date: 2026-11-19
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-010, ADR-011, ADR-014

## Context

- The `event_time` mode of a training dataset reads events from a JSONL file. The manifest does not say which events were read. ADR-014 lists a content hash of the events as an open item.
- ADR-010 treats `ingested_at` as receipt metadata. It is excluded from event identity (ADR-007, ADR-008) and dropped before the generic Flink job, so the online path has no time for when the system saw something.
- ADR-014 names the cost: `event_time` includes events that arrived after `t` but happened before it, which serving did not have.
- `InteractionEventV1` carries `ingested_at`. The event frame of `backfill_spec` already holds it. `route_event_json` validates a payload and builds a rejection record with the raw payload.
- `history.py` has the Iceberg infrastructure for one writer process: the SQL catalog, the schema check and the idempotent append.
- The pattern is the fact store of Axion at Netflix: facts are stored in Iceberg and features are recomputed from them with the same encoders as online. FeatureForge is inspired by it, not equal to it. The encoder here is the spec engine, whose references, Flink job and Spark backfill agree on golden fixtures.

Goal: keep the validated events as immutable facts in Iceberg, so that datasets are reproducible from a pinned snapshot, the system can state what it had received, and every later offline computation reads one source.

## Decision

| Concern | Rule |
|---|---|
| What the store holds | Validated interaction events as immutable facts. Rejected events are not facts. |
| Table | `facts.interaction_events`, one table per event contract, in the catalog of ADR-011. The catalog is still named `history`, because the name is part of every catalog row. Renaming it needs a migration. |
| Columns | The fields of `InteractionEventV1`, including `ingested_at`. The model decides, the table derives its schema from it. Timestamps are with time zone, UTC. |
| Who writes | An event-log writer. It reads the raw topic, validates every payload with `route_event_json` and appends the valid events. It reads before the Flink job, because the job drops `ingested_at`. The Flink job, the materializer, the dataset commands and the backfill never write facts. |
| Rejections | They keep their current path. A table for rejected events is a separate decision. |
| Identity | `event_id`. |
| Duplicates | A row whose `event_id` exists is dropped. The first row wins and keeps the earliest `ingested_at`. If another payload field differs from the stored row, `ingested_at` excluded, the writer fails with the identity conflict of ADR-007 and ADR-008 and writes nothing. The check and the append are not atomic, so one writer per table is the rule, as in ADR-011. |
| Partitioning | `day(event_time)`. |
| Schema check | If the schema derived from the model differs from the existing table, the writer fails. It never alters a schema silently. |
| Dataset read | `build-training-dataset` in mode `event_time` reads either a JSONL file or the table at a pinned snapshot (`--events-root`, optional `--events-catalog-url`). The pin is the current snapshot when the command starts. |
| Reproducibility | A dataset from the table lists the snapshot id in its manifest under `events`. The same snapshot, spec, observations and `data_end` give the same rows. This replaces the content hash of ADR-014. |
| Receipt time | The table keeps `ingested_at` so that a later decision can add a mode that uses only events received by `t`. This ADR does not add that mode. |

## Consequences

- Positive: both modes of ADR-014 then read from pinned Iceberg snapshots. The manifest states the provenance of the events, not only of the history.
- Positive: `ingested_at` becomes available offline for the first time.
- Negative: the writer and the Flink job read the topic independently. An event can be in Redis before it is in the table, and the other way round. An offline recomputation at a snapshot can miss an event that online already used. The writer lag must be observable, and the parity holds up to the events the snapshot contains.
- Negative: the table deduplicates by `event_id` without expiry. The online path deduplicates within the TTL of the spec (48 h for `user_skip_rate_24h`). A duplicate older than that is counted online and dropped in the table.
- Negative: `ingested_at` is not what serving knew. Serving applies watermarks and allowed lateness. A mode that filters by receipt time approximates serving, and only `published_as_of` is exact (ADR-010).
- Negative: a second process to run. The SQLite catalog allows one writer, and small batches create many small files.
- Negative: the catalog name `history` is misleading once it holds facts.

## Alternatives considered

| Alternative | Why not |
|---|---|
| A table of backfill values per spec | It is a cache keyed by arbitrary observation times with little reuse. It is mutable, so a dataset from it needs its own pinning. It stores a result and not a fact. A materialized backfill can come later as its own decision. |
| A content hash of the JSONL file | It proves the identity of an input file, but it keeps no `ingested_at` and gives no shared source for later computations. |
| The Flink job writes the facts through an Iceberg sink | It couples ingestion to the job, the job has no `ingested_at`, and the sink is at-least-once. |
| Store the raw payload JSON | It loses the schema and the partition pruning. |
| Spark Structured Streaming as the writer | It is heavier to run, and PyIceberg already writes the history. |
| Rejected events in the same table with a status column | It mixes facts with diagnostics. |

## Verification

Planned, nothing exists yet.

- Round trip: events through the writer into the table, Spark reads the same events including `ingested_at`.
- The same batch twice leaves the rows unchanged and keeps the first `ingested_at`.
- The same `event_id` with another payload fails and writes nothing.
- A schema that differs fails. A rejected event is not in the table.
- A dataset from the table equals the dataset from the same events as JSONL.
- An append after the pin does not change the dataset, and the manifest names the snapshot id.
- Mutation: dropping the duplicate check fails the idempotency test.
- A bounded run through the writer and one through Flink and Redis agree on the events they saw.

## Open items

- A mode that uses only events received by `t`.
- `backfill-spec` reading the table.
- A table for rejected events.
- Several writers and several partitions, compaction and retention.
- A production catalog and a neutral catalog name.
- Event contract versions and schema evolution.
- The materialized backfill table, with its own name rule, as a later ADR.
