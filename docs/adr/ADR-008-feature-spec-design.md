# ADR-008: Declarative feature specification

Status: Proposed · Date: 2026-11-12

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
| Event identity | `dedupe.key`. `ingested_at` is never part of identity. |
| Deduplication horizon | `dedupe.ttl >= window.size + lateness.allowed`, otherwise dedupe state is evicted while an event can still change a window. |
| Online horizon | `online.ttl >= window.size`. |
| Update policy | `append_only`: exactly one aggregation. `componentwise_monotonic`: several aggregations compared per component. Both require count-only aggregations, because only counts are monotone under added events. |
| Derived values | Pure expressions over aggregation names. No function calls, no attribute access. |
| Filters | Pure boolean expressions over event fields. Same safe subset. |

### Bounded scope

Not a SQL compiler. Supported operators: `count`, `sum`, `avg`, `min`, `max`,
`last_k`, `approx_distinct`, plus `ratio` through `derived`. Composite entity keys
are supported. Operators beyond this set require a new ADR.

## Comparison

| | Chronon | Feast stream views | FeatureForge spec |
|---|---|---|---|
| Unit | `GroupBy` + `Join` | `StreamFeatureView` | Spec (one feature group) |
| Windows | Sawtooth (sliding + hopping) | Time-window aggregations | Explicit sliding, `slide` declared |
| Update and dedupe policy | Engine-defined | Engine-defined | **Declared in the spec and tested** |
| Backfill timeline | `Join` left side | Not part of the view | Defined in WP2 |

## Consequences

- Positive: one source of truth for stream job, backfill, reference and catalog;
  guarantees are explicit and testable.
- Negative: a 24 h window with a 1 min slide assigns each event to 1,440 windows.
  State growth must be measured (WP5, WP9). Sawtooth windows are the known
  alternative if this becomes the bottleneck.
- Negative: aggregations other than `count` cannot use monotone update policies.
  They need an explicit policy before they can be served online.

## Migration

`user_play_count_1h` and `user_skip_rate_24h` move to specs. The hand-written jobs
are deleted only when parity tests against the golden fixtures are green.