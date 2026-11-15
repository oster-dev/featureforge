# ADR-009 Spec-driven online stores and materializer

- Status: Accepted. Implemented and proven end to end for a feature without own code. Open: the alias and exception subclass of decision 1 and the thin wrappers of decision 10.
- Date: 2026-11-15
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-006, ADR-007, ADR-008

## Context

Both streaming features run on one generic Flink job (ADR-008). Everything after Kafka is still hand-written per feature: two Redis stores (`online_store.py`, `skip_rate_store.py`), two materializers (`materialize.py`, `materialize_skip_rate.py`) and two Pydantic payload models. Reading the code shows how little differs.

- The materializers differ only in docstring, default topic, default key prefix, store class, feature model and Kafka client id.
- The stores share `key_for`, `_decode`, `read` and a `WATCH`/`MULTI` loop with at most 16 retries. Only the write decision differs: inline for the play count, `decide_skip_rate_update` for the skip rate.
- `WriteResult` and `UpdateDecision` are the same three values: `written`, `unchanged`, `stale`. An incomparable count vector raises `SkipRateUpdateConflictError`.
- Validation differs. The skip-rate store rejects non-strings and whitespace in the key prefix and the user id. The play-count store only rejects blank values.
- The spec validates `online.ttl >= window.size`, but no store sets a Redis expiry. This is a known gap in STATUS.
- `content_play_count_1h` (PR #32) runs on the generic job from YAML only, but cannot reach Redis without a new store, materializer and model.

Goal: a spec the platform has never seen reaches Redis without a new Python file.

## Decision

1. One outcome type: `WriteOutcome = Literal["written", "unchanged", "stale"]`. `WriteResult` and `UpdateDecision` stay as aliases. Incomparable vectors keep raising an exception, `UpdateConflictError`. `SkipRateUpdateConflictError` becomes a subclass so existing handlers and tests keep working.
2. One decision function, `decide_count_vector_update(stored, incoming)`, over the count components of the spec. A newer observation window is `written`, an older one is `stale`. In the same window, equal vectors are `unchanged`, a vector with all components greater or equal is `written`, all less or equal is `stale`, and anything else raises. A single component is exactly the play-count rule. Derived values are never ordered, because a falling rate can be a valid correction.
3. `append_only` and `componentwise_monotonic` both map to this function. The spec already limits both to count-only aggregations, and `append_only` to exactly one. `window_replace` is not implemented here. The store constructor raises `UnsupportedPolicyError` for it, because it needs its own decision.
4. One store, `RedisSpecStore(client, spec, *, key_prefix=None)`, with `read`, `write` and `key_for`. It keeps the same `WATCH`/`MULTI` loop, the same 16 retries and the same error behavior. Records are a frozen dataclass, `FeatureRecord`, produced by `parse_feature_record(payload, spec)`. The parser checks the feature name, the entity key, a window length equal to `spec.window.size`, and counts that are non-negative integers (booleans rejected). It recomputes derived values from the spec expressions and compares them, which keeps the existing skip-rate guarantee. No dynamic Pydantic models.
5. The wire format does not change. The JSON of both features stays as the Flink job emits it today: base fields, entity key, counts, derived values. A parity test proves it.
6. Key schema: `featureforge-stream:online:v{spec.version}:{spec.name}:{entity_value}`. For both existing specs (version 1) this equals the current keys, so no migration is needed. A new spec version opens a new namespace. Composite entity keys are rejected, as in the Flink plan.
7. Validation uses the stricter rule of both stores for the key prefix and the entity value: a non-empty string without whitespace. Play-count entity values with inner whitespace were accepted before and are rejected now. This is a behavior change and gets a test. Event ids already match `^\S+$` (`EntityId` in the event contract), so no value produced by the pipeline is affected. Only direct callers of the old play-count store can notice it.
8. TTL is enforced: `SET key payload EX online.ttl` inside the same `MULTI`. The TTL runs in wall-clock time from the last accepted write, not in event time. `unchanged` and `stale` writes do not extend it. Behavior change: keys that never expired now expire after 2 h (play count) and 25 h (skip rate). An expired key reads as missing, never as zero (ADR-003).
9. One materializer, `materialize_spec(arguments, spec)`, with `python -m featureforge.streaming.materialize_spec --spec PATH`. The default topic is `features.{name with _ replaced by -}.v{spec.version}`, which equals the current defaults. `--topic` overrides it. Behavior is unchanged: single-partition topics only, empty ranges refused, tombstones rejected, offsets committed only after a successful write.
10. `materialize.py` and `materialize_skip_rate.py` stay as entry points with their current arguments and call the generic code. `RedisUserPlayCountStore` and `RedisUserSkipRateStore` become thin wrappers with the same constructors, methods and typed models, so the unified client and all existing tests stay unchanged.

## Out of scope

- `window_replace` and composite entity keys.
- A second online backend (Cassandra) and the store protocol that goes with it.
- Spark backfill from the spec and Iceberg history.
- Skip-rate reads in the unified online client (separate PR).

## Consequences

Positive:
- A new count or ratio feature reaches Redis from YAML alone.
- Concurrency, TTL and validation live in one place.
- Two near-identical 200-line materializers and two stores shrink to one implementation.
- The store contract can be tested once per spec and later reused for a second backend.

Negative:
- Two behavior changes: TTL expiry and stricter validation for play-count entity values.
- The wrappers add indirection until they are removed.
- Dict-based records are less typed than the hand-written Pydantic models.
- Derived-value checks depend on the spec reference evaluator.

Mitigations:
- Parity tests between the old wrappers and the generic store.
- Golden payload fixtures for both features.
- A TTL test against a real Redis.
- Both behavior changes are listed in STATUS.

## Verification plan

- Unit: the decision table of both existing test files, plus the case "newer window with zero replaces an older value".
- Parity: the same write sequence through the old wrappers and `RedisSpecStore` yields identical outcomes and identical stored JSON.
- Integration with real Redis: the existing correction, concurrency (`WATCH` conflict) and replay tests run against `RedisSpecStore` for both specs.
- TTL: after a write the remaining TTL is positive and at most `online.ttl`. An `unchanged` or `stale` write does not extend it.
- Proof: `content_play_count_1h` flows from Kafka through the generic Flink job and `materialize_spec` into Redis and is read back through `RedisSpecStore`, with no new Python file.
- Not verified by this ADR: multi-partition Kafka, JobManager failure, performance.

## Open questions

- Resolved: `EntityId` is `^\S+$`, so whitespace cannot occur in event-derived ids. Decision 7 only affects direct callers of the old play-count store.
- Resolved: the Flink plan takes `spec.keys[0]` as the entity key and `feature_record` emits it under that name, so the content record carries `content_id`. The proof test still asserts it.
- Whether `unchanged` deliveries should refresh the TTL. Decided no for now, to be revisited with freshness measurements.
