# ADR-017 Operators beyond count in the engines and the stores

- Status: Proposed
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

## Open questions

- Is the summed field guaranteed non-negative by the event contract?
- Can a derived expression return a missing value for an empty window?
- Integer and float components in one vector.
- `last_k`: order by (event_time, event_id), and the largest allowed k.
