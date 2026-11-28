# ADR-017 Operators beyond count in the engines and the stores

- Status: Accepted
- Date: 2026-11-26
- Scope: Flink job, Spark backfill, spec model and the online store policies
- Refines: ADR-008 (update policy rules), ADR-009

## Context

The spec model and the reference support `count`, `sum`, `avg`, `min`, `max`, `last_k` and
`approx_distinct` (ADR-008). The generic Flink job and the Spark backfill support `count` only, and
the model rejects the monotone update policies for every other operator, because ADR-008 says that
only counts are monotone under added events.

Event identity conflicts fail the job, so events are only ever added to a window and never taken
away. This ADR decides how each further operator behaves under late events, which state it needs
and which store policy it uses.

## Decision

1. **Aggregators.** The Flink plan holds a list of aggregations with an operator and a field, not a
   list of counters. Each operator defines its zero value, its add step and its merge step. An
   operator that is not delivered yet keeps failing with `UnsupportedSpecError` in Flink and with
   `NotImplementedError` in Spark. It is never computed approximately.
2. **Monotone policies.** Added events can only grow `count`, `sum` over a non-negative field and
   `max`, and can only lower `min`. These may use `componentwise_monotonic`, with one direction per
   component. The model checks that a summed field cannot be negative in the event contract.
3. **avg.** `avg` is expressed as a derived value over a `sum` and a `count`, like the skip rate.
   It is never stored as a component.
4. **window_replace for last_k and approx_distinct.** Their value is not monotone. A record carries
   an events-seen count. A newer window replaces an older one. In the same window a higher count
   replaces, an equal count must be identical, and a lower count is stale. This replaces the
   "identical or conflict" rule of ADR-008 for this policy, which would reject a legitimate late
   correction.
5. **approx_distinct.** The reference stays exact. The engines use a sketch, and the accepted error
   bound is chosen in the pull request that adds the operator.
6. **Order of delivery.** `sum` and `avg`, then `min` and `max`, then `last_k`, then
   `approx_distinct`. Each step brings a hand-calculated golden fixture, parity with the reference
   at every window boundary in Flink and in Spark, and a store contract test on Redis and on
   Cassandra.

## Alternatives considered

- All non-count operators on `window_replace`: simpler, but it still needs the events-seen count
  and it throws away the ordering that `sum`, `min` and `max` can keep.
- No late corrections for non-count operators: hides data, rejected.

## Consequences

- Practitioners get `sum` and `avg` features from a YAML spec alone.
- The model rule of ADR-008 loosens, and the count-vector decision function becomes typed with a
  direction per component.
- `last_k` and `approx_distinct` need the new `window_replace` store policy first.
- Composite keys are a separate step and are not decided here.

## Answers to the first questions

- The summed field is guaranteed non-negative. `play_seconds` is a strict integer with `ge=0` in
  the event contract, so `sum` over it is a non-negative integer and monotone under added events.
- A derived expression may return a missing value for an empty window. The spec model accepts
  `play_seconds_sum / play_count if play_count > 0 else None`, and the reference returns `None` for
  an empty window and `90.0` for the fixture (`tests/unit/test_spec_sum_avg_golden.py`).
- The feature record parser and both stores accept a missing derived value only where the
  spec expression gives none for the components of the record. The record carries `null`,
  the history stores `null`, and a serving entry stays `ok` with a `null` value. A missing
  value is never replaced by zero or by a fallback default (ADR-003).
- A missing extreme is the state before the first matching event. In the decision it is behind
  every value in both directions, so a first event is a correction and a vector with a missing
  extreme against a stored value is stale. A minimum is more advanced when it is smaller.

## Open questions

- Float components (min and max over a non-integer field, a sum over a float field). The loader
  rejects them under `componentwise_monotonic` until a decision exists.

## Delivery

- `sum` and `avg` (as a derived value over a `sum` and a `count`): the Flink job, the Spark
  backfill, the spec model with `componentwise_monotonic`, the record parser, the Redis and the
  Cassandra store, the history table and the serving entry. A summed field must be a non-negative
  integer in the event contract, the loader rejects any other field under this policy.
- `min` and `max`: the Spark backfill, the Flink job and the online path with a direction per
  component, a missing extreme as `null`, the history table and the version ranking of
  published-as-of. A minimum or maximum field must be an integer in the event contract.
- Not delivered: `last_k`, `approx_distinct` and the
  `window_replace` store policy.

## Addendum: the shape of last_k and the window_replace store (2026-11-28)

This answers the open question on last_k and fixes what step 4 delivers.

7. **Value of last_k.** The value is a JSON array of integers: the column values of the matching
   events, oldest first, ordered by (event_time, event_id) like in the reference, at most k long.
   A window with fewer matching events gives a shorter array, a window without any gives an empty
   array. An empty array is a value, not a missing value. Like min and max, last_k takes an
   integer field only.
8. **k is bounded.** The model rejects k above 100. Every event enters every window it falls into
   (1,440 windows for a 24 h window with a 1 min slide, ADR-008), and each window of each key
   holds up to k values per aggregation, so k bounds the state.
9. **events_seen.** For a spec with the policy window_replace, the Flink job adds one integer
   field, events_seen, to every record: the number of events in the window after deduplication,
   counted for every event whatever the filters say. Events are only added to a window, so it only
   grows. The name is reserved, an aggregation or a derived value cannot use it, and records of
   the other policies do not have it.
10. **The store decision.** A newer window replaces an older one. In the same window a record with
    a higher events_seen is written, a record with the same events_seen and the same values is
    unchanged, one with the same events_seen and other values raises UpdateConflictError, and a
    lower events_seen is stale.
11. **History and ranking.** The history stores last_k as a list column and events_seen as an
    integer column. The update identity contains events_seen and the values, and events_seen ranks
    versions with the same publication time and window.
12. **Order of the pull requests.** The Spark backfill computes last_k, the Flink job computes
    last_k and emits events_seen, then the online path (parser, store, history, serving) follows.
    approx_distinct comes after, with its own decision on the error bound.

Consequence for the Flink job: the accumulator of a spec with last_k cannot stay a tuple of
integers, because it holds the buffer of the last k events with their time and identity.
