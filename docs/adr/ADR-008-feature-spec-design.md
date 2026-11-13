# ADR-008: Declarative feature specification

Status: Accepted · Date: 2026-11-12 · Updated: 2026-11-13 (migration completed)

## Context

FeatureForge has two hand-written streaming features. Each feature needs its own
Flink job, reference implementation and Redis store class. Adding a feature means
writing and testing roughly the same code again. Practitioners should define and
test features without writing platform code.

## Decision

Features are defined in a **YAML specification**, validated by a Pydantic model
before anything runs. The spec is data, not code. A Python DSL was rejected
because it cannot be validated without executing it, cannot be statically
catalogued, and invites arbitrary logic that the engine cannot test.

A spec declares: identity (name, version, owner), entity key(s), source,
aggregations, derived values, window, lateness, deduplication, online policy,
offline target and freshness SLO.

### Semantics fixed by the spec

| Concern | Rule |
|---|---|
| Window boundary | Right-closed: `(t - size, t]`. Only `right_closed` is accepted. |
| Event time | The `event_time` field of the event contract. It is not configurable in this version. |
| Event identity | `dedupe.key`. `ingested_at` is never part of identity. Identities are validated across the whole input before any filtering. |
| Deduplication horizon | `dedupe.ttl >= window.size + lateness.allowed`, otherwise dedupe state is evicted while an event can still change a window. |
| Online horizon | `online.ttl >= window.size`. Declarative until stores are spec-driven. |
| Update policy | `append_only`: exactly one count aggregation. `componentwise_monotonic`: several count aggregations compared per component. Both require count-only aggregations, because only counts are monotone under added events. `window_replace`: a newer window replaces an older one, a re-emission within the same window is accepted only if identical, otherwise it is a conflict. No store implements `window_replace` yet. |
| Missing values | `avg`, `min` and `max` over no matching events are missing, never zero. A derived value with a missing input is missing. `count` and `sum` over no events are `0`. |
| Derived values | Pure expressions over aggregation names. No function calls, no attribute access. |
| Filters | Pure boolean expressions over event fields. Same safe subset. Fields must exist in the event contract. |

### Bounded scope

Not a SQL compiler. Supported operators: `count`, `sum`, `avg`, `min`, `max`,
`last_k`, `approx_distinct`, plus `ratio` through `derived`. Composite entity keys
are supported. Operators beyond this set require a new ADR.

### Reference as oracle

The spec-driven Python reference is exact and deliberately simple. The Flink job
and the Spark backfill must produce the same values. Where an operator is
approximate in an engine (`approx_distinct`), the reference stays exact and the
accepted error bound is documented with the engine. The generic Flink job
supports count aggregations and one entity key. Other operators and composite
keys are rejected with a clear error, not computed approximately.

## Comparison

| | Chronon | Feast stream views | FeatureForge spec |
|---|---|---|---|
| Unit | `GroupBy` + `Join` | `StreamFeatureView` | Spec (one feature group) |
| Windows | Sawtooth (sliding + hopping) | Time-window aggregations | Explicit sliding, `slide` declared |
| Update and dedupe policy | Engine-defined | Engine-defined | **Declared in the spec and tested** |
| Backfill timeline | `Join` left side | Not part of the view | Defined in WP2 |

Sources:
[Chronon GroupBy](https://chronon.ai/authoring_features/GroupBy.html),
[Feast stream feature view](https://docs.feast.dev/getting-started/concepts/stream-feature-view).

## Consequences

- Positive: one source of truth for stream job, backfill, reference and catalog;
  guarantees are explicit and testable.
- Negative: a 24 h window with a 1 min slide assigns each event to 1,440 windows.
  State growth must be measured (WP5, WP9). Sawtooth windows are the known
  alternative if this becomes the bottleneck.
- Negative: aggregations other than `count` need `window_replace`, which offers no
  online corrections within a window. A store for it must be built and tested
  before such a feature can be served online.
- Negative: `online.ttl` is not enforced yet. The current Redis stores write
  without expiry.

## Migration

`user_play_count_1h` and `user_skip_rate_24h` run on the generic job
`jobs/feature_from_spec.py`. The hand-written Flink jobs were deleted after
parity was proven: every integration test of both features ran against both
jobs and passed, and the golden fixtures matched the spec reference and the
Python references. The last version of the hand-written jobs is the tag
`hand-written-jobs-final`.

The Redis store policies, the materializers and Spark features are still
hand-written per feature. Spec-driven stores are the remaining part of the
migration and need their own decision, because `online.ttl` enforcement and the
`window_replace` policy change store behavior. Provenance of the migration
stages: [streaming-migration.md](../streaming-migration.md).