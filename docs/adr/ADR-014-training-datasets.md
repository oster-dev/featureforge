# ADR-014 Training datasets from features and labels

- Status: Accepted. Decides how a training dataset joins feature values and a label, in two modes. Nothing is implemented yet. The implementation follows in two changes, the event-time mode first.
- Date: 2026-11-18
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-008, ADR-010, ADR-011, ADR-012
- Related: ADR-013 decides the online store protocol and is independent of this decision.

## Context

The pieces of a training dataset exist. Reading the code shows what the join has to respect.

- `backfill_spec` evaluates a feature spec at explicit observation times over `(t - size, t]` and keeps a missing derived value null. It supports count aggregations only.
- `backfill_labels` evaluates a label spec over `(t, t + horizon]`, excludes and counts observations with `t + horizon > data_end`, and never returns a null value (ADR-012).
- `published_as_of(history, entities, as_of)` returns, for one spec and one time, what serving held for each entity: `present`, `not_published` or `expired`. It scans the history table on every call. A dataset needs the answer for many entity and time pairs.
- A history table has one entity column. Evidence exists only from the time the materializer started to write it (ADR-010 consequences). Before that time the history says nothing, which is not the same as "not published".
- ADR-010 separates event-time reconstruction from published-as-of retrieval and forbids substituting one for the other. A backfill never writes to the history (ADR-011).
- ADR-012 leaves the training dataset API and its two modes open.

Goal: for the same observation, features that contain only information up to the observation time and a label that contains only events after it, with the mode of the features written down.

## Decision

| Concern | Rule |
|---|---|
| Entry point | One function and one CLI command take feature specs, one label spec, an observation file, the events, `data_end`, an output directory and a `mode`. `mode` has no default. |
| Modes | `event_time`: feature values come from `backfill_spec` over events. `published_as_of`: feature values come from the history, as serving held them at the observation time. Neither replaces the other (ADR-010). The manifest names the mode. |
| Observations | Pairs of entity and observation time, with the format and the checks of `backfill-spec`. Observation times are timezone-aware. Invalid input fails before anything is written. |
| Entity | All features and the label name the same `keys`. The `published_as_of` mode needs one key, because the history has one entity column. Anything else fails before any data is read. |
| Rows | One row per observation that has a label. Columns are the keys, `observation_time`, the feature columns and the label columns. |
| Column names | `<spec_name>__<column>` for features, `label__<label_name>` for the label value and `label__<label_name>__observable_at` for its observability time. A bare name is never used, so a second feature cannot collide silently. |
| Features, `event_time` | Window `(t - size, t]` over events, as in the spec reference. A count over no events is 0 and a derived value without a denominator is null, as in `backfill_spec`. Events that arrived after `t` but happened before it are included. |
| Features, `published_as_of` | The version available at `t` by the rule of ADR-010. A feature that is `not_published` or `expired` at `t` has null counts and null derived values, never zero, and carries its status in `<spec_name>__status`. |
| History start | In `published_as_of` mode, each history table has a `history_start`, its earliest `published_at`. An observation with `t < history_start` for any feature has no evidence either way. It is excluded and counted, not filled with null. |
| Label | Always from events through `backfill_labels` over `(t, t + horizon]`. A label is never read from the history, because a label is not published. |
| Incomplete labels | The row is excluded and counted in `excluded_incomplete` (ADR-012). A row with a null label is never written. |
| Leakage rule | Features use only information up to `t`: events at or before `t` in `event_time` mode, versions with `published_at` at or before `t` in `published_as_of` mode. The label uses only events strictly after `t`. An event at `t` belongs to the features and never to the label. |
| Output | Parquet written by Spark, partitioned by the UTC day of the observation time. It is never one Pandas frame. Existing output is never overwritten. |
| Manifest | A JSON file next to the data with the mode, the feature and label specs with their versions, `data_end`, the counts (observations, rows, `excluded_incomplete`, `excluded_before_history`), every partition with its row count and the Spark version. In `published_as_of` mode it also records the Iceberg snapshot id and the `history_start` of each feature. |
| Reproducibility | `published_as_of` reads pinned snapshot ids. The same snapshots, the same events and the same `data_end` give the same rows. The `event_time` mode depends only on the events and `data_end`. |
| Published-as-of join | One scan per history table, never one per pair. Spark joins each entity and time to the latest version with `published_at` at or before `t`, with the ranking of `published_as_of`, then applies the TTL cutoff. The single-time function stays the oracle. |
| Operators | The dataset supports what the Spark backfill supports. Other operators fail loudly. |

## Consequences

- Positive: a dataset row never carries a feature value from after `t` and never a label from at or before `t`.
- Positive: the mode is explicit in the call and in the manifest, so a dataset cannot pass for the other kind.
- Positive: running both modes over the same observations shows how far reconstruction differs from what serving held. The difference comes from late events and from the expiry of `online.ttl`.
- Negative: the `event_time` mode includes events that arrived after `t`, which serving did not have. A model trained on it sees values that serving could not produce at that time. This is why `mode` is required.
- Negative: after a crash between Redis and the history append, `published_at` is later than the real publication (ADR-010). Training sees such a value later than serving did, never earlier.
- Negative: the `published_as_of` mode covers only the time since the history started. Earlier observations are excluded and counted.
- Negative: the history supports one entity key, so the `published_as_of` mode cannot join features with composite keys.
- Negative: the spec-qualified column names are long.
- Negative: `history_start` marks only the beginning. A later gap, for example a materializer run without a history root, reads as `not_published` although serving held a value. The rule cannot detect it.

## Alternatives considered

- Joining in Pandas: rejected. The output must scale beyond one frame, and the join is the part that needs a parity oracle.
- A row with a null label for an incomplete window: rejected by ADR-012. It invites training on rows without a label.
- A default mode: rejected. A silent default hides the choice between reconstruction and what serving held.
- Bare column names: rejected. Two features with a count of the same name would collide, or one would be renamed silently.
- One call of `published_as_of` per pair: rejected. Every call scans the table, which does not scale to a dataset.
- Null feature values before `history_start`: rejected. They would look like "not published" and hide that no evidence exists.
- Reading labels from the history: rejected. A label is computed from events after the observation and is never published.

## Verification

Planned, and written before the implementation.

- A hand-calculated fixture with events at `t`, just after `t`, at `t + horizon` and just after it. The event at `t` appears in the features and not in the label.
- In `published_as_of` mode, a version published exactly at `t` is visible, and one published just after `t` is not. The TTL cutoff is exact.
- The Spark join of the history returns the same values as `published_as_of` on randomized histories and observation times, including the boundaries.
- A `not_published` or `expired` feature gives null counts and a status, never zero.
- An observation before `history_start` is excluded and counted.
- An incomplete label window excludes the row and is counted.
- The mode is required, composite keys in `published_as_of` mode fail, and existing output is not overwritten.
- The manifest lists every partition, and the row counts add up to the rows written.
- A dataset run never writes to the history (ADR-011).
- The planned parity test between online and offline: a bounded replay writes Redis and the history, and the Redis value at the stop time equals the `published_as_of` value at that time.

## Open items

- Composite entity keys in the history.
- A content hash of the events in the manifest.
- Operators beyond count in the Spark backfill, and with them in the dataset.
- The Arrow loader for training and its throughput numbers.
- Several appender processes and their effect on `history_start`.
- Detecting gaps inside the history, not only its start.
