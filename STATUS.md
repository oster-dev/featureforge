# FeatureForge Status

Last updated: 2026-11-13 · CI: see the
[Actions page](https://github.com/oster-dev/featureforge/actions)

This file is the single place for what is **delivered**, **in progress**, and
**deliberately out of scope**. Other documents link here instead of repeating
status. Claims in the README link to the test or document that proves them.

Related: [README](README.md) · [Architecture](ARCHITECTURE.md) ·
[Platform blueprint](docs/PLATFORM_BLUEPRINT.md) ·
[Streaming provenance](docs/streaming-migration.md) ·
[ADR-008](docs/adr/ADR-008-feature-spec-design.md)

---

## Delivered

| Area | What exists | Evidence |
|---|---|---|
| Offline path | Deterministic source data, canonical Parquet backfill, persisted-feature correctness gate, freshness check | [demo.md](docs/demo.md), ADR-002, ADR-003 |
| Online serving | Feast materialization into Redis, online lookup, deterministic ranking | `tests/integration/test_online_serving.py` |
| Streaming features | `user_play_count_1h` and `user_skip_rate_24h`: Kafka, Flink, guarded Redis writes | `tests/integration/` (see below) |
| Validation and routing | Rejected events go to a separate topic, valid follow-up events keep flowing | `test_streaming_validation_routing.py` |
| Late data | Late correction of an emitted window, too-late routing, both skip-rate correction directions | `test_user_play_count_late_events.py`, `test_user_skip_rate_job.py` |
| Replay and conflicts | Replay idempotency, event-identity conflict detection | `test_user_play_count_replay_idempotency.py` |
| Unified read access | Batch and streaming play count through one client, explicit missing, freshness and non-atomic metadata | `src/featureforge/online.py`, `test_unified_online.py` |
| Skip-rate golden fixture | Deterministic event file with hand-calculated expected values: both window edges, a duplicate delivery, ignored event types, a second user, expiry to a zero denominator. Matched by the hand-written reference, by the spec reference including a sweep over every event boundary, and by the generic Flink job. | `fixtures/streaming/user_skip_rate_24h_v1.jsonl`, `tests/unit/test_streaming_user_skip_rate_golden.py`, `tests/unit/test_spec_golden_parity.py`, `tests/integration/test_feature_from_spec_job.py` |
| Feature spec model (WP1, stage 1) | Validated declarative spec: windows, lateness, dedupe, update policies, safe expressions. The loader rejects fields that are not in the event contract. Both existing features expressed as specs. | `src/featureforge/spec/`, `feature_repo/specs/`, `tests/unit/test_spec_model.py` |
| Spec reference (WP1, stage 2) | Exact event-time reference evaluated directly from a spec: all operators, composite keys, missing-value rules, event-field check. Matches both hand-written references on randomized inputs at window boundaries and the play-count golden fixture. | `src/featureforge/spec/reference.py`, `tests/unit/test_spec_reference.py` |
| Generic Flink job (WP1, stages 3 and 4a/4b) | A plan derived from a spec (window, lateness, dedupe horizon, counters with filters, derived value) drives one generic job. Supports count aggregations and one entity key, other operators are rejected with a clear error. For `user_skip_rate_24h` (two counters and a ratio) and `user_play_count_1h` (a single counter, no derived value) on time-ordered input, every emitted window matches the spec reference and the hand-written reference, hand-calculated windows are present (five for skip rate, seven for play count), no window is emitted twice, each record has exactly the expected fields, and nothing reaches the late topic. The eight skip-rate job scenarios (late corrections in both directions with duplicates, too-late routing, both window edges, zero denominator) run against the hand-written and the generic job. | `src/featureforge/spec/flink_plan.py`, `jobs/feature_from_spec.py`, `fixtures/streaming/user_play_count_1h_v1.jsonl`, `tests/unit/test_spec_flink_plan.py`, `tests/integration/test_feature_from_spec_job.py`, `tests/integration/test_user_skip_rate_job.py` |
| CI | Baseline, Python 3.11/3.12 compatibility job, serving integration, streaming Flink integration with a fresh TaskManager per module, nightly run, concurrency and timeouts, Dependabot for Actions | `.github/workflows/ci.yml`, `scripts/ci/run_flink_integration.sh` |
| Documentation | Architecture, blueprint, demo guide with batch, streaming and feature-spec walkthroughs, ADRs, AWS production profile (design only) | `docs/` |

## In progress

| Item | State |
|---|---|
| WP1: declarative spec and generic engine | Stages 1 to 3 delivered for both features. Stage 4 in progress: golden fixtures and the skip-rate scenarios run on the generic job. Stages 5 and 6 below. |
| Streaming history and published-as-of retrieval (WP2) | Not started. Needs the time-model decision (`event_time` vs. `published_at`). |

### WP1 stages

| Stage | Content | State |
|---|---|---|
| 1 | ADR-008, spec model, validation, two specs | **Done** |
| 2 | Generic reference from a spec, parity with the hand-written references and the play-count golden fixture | **Done** |
| 3 | Generic Flink job (`count`, `ratio`): golden fixtures, spec-reference parity, job plan from a spec, generic job verified against the fixtures of both features | **Done** |
| 4 | 4a done: the skip-rate job scenarios (late corrections, too-late routing, window edges, zero denominator) run on the hand-written and the generic job. 4b done: the play-count spec (single counter) reproduces its golden fixture on the generic job. Open: late events for the play count, replay and the Redis path on the generic job. Then migrate both features, delete the hand-written jobs when parity is green, spec-driven store policies | In progress |
| 5 | Further operators in Flink, composite keys, Spark backfill | Planned |
| 6 | Two new features by YAML only | Planned |

## Next

Order follows the [readiness plan](docs/PLATFORM_BLUEPRINT.md):

1. **WP1:** stages 4 to 6 above.
2. **WP2:** Iceberg offline store, stream history, point-in-time training datasets, parity test.
3. **WP3:** second online backend (Cassandra) behind one contract suite.
4. **WP4:** feature-serving service and load test.
5. **WP5:** observability, recovery drills, soak run.

Later: catalog and discovery, embeddings, notebooks, profiling, end-to-end ML
workflow (Metaflow, MLflow, registry, rollback).

## Known gaps

| Gap | Impact |
|---|---|
| No checkpoint or savepoint restore test | Checkpointing exists in code. Recovery is **not** proven. |
| No dedupe test after state TTL expiry (24 h, 48 h) | No claim of unlimited replay protection. |
| Single-partition Kafka topics only | Partition scaling is not demonstrated. |
| Flink Kafka sinks are `AT_LEAST_ONCE` | No exactly-once claim. |
| TaskManager failure in long CI runs is isolated, not explained | A fresh TaskManager per module isolates it. The metaspace is logged per module to test a class-loading-leak hypothesis. Root cause open. |
| The generic job is verified for the golden fixtures and the skip-rate scenarios only | Replay, the Redis path, recovery and the late-event scenarios for the play count are not re-proven for the generic job. |
| Only the generic job tests use a spec-driven job. Production jobs, stores and Spark features are still hand-written | The hand-written jobs stay in use until stage 4 is complete. |
| The Flink image installs `pydantic` and `PyYAML` explicitly | Any new runtime dependency of the job path must be added to `docker/flink/Dockerfile`. The image build checks the project imports. |
| `online.ttl` in the specs is declarative only | The current Redis stores write keys without expiry. Enforcement arrives when stores become spec-driven. |
| `window_replace` update policy has no store | Non-count operators cannot be served online yet. |
| `approx_distinct` is exact in the reference | The accepted error bound of an engine implementation is not defined yet. |
| Unified client does not read `user_skip_rate_24h` | Skip rate is verified through its own pipeline tests. |
| No latency or throughput numbers | No benchmark exists yet. |
| Older joined and replay tests lack topic cleanup and subprocess timeouts | Tracked in [streaming provenance](docs/streaming-migration.md#open-test-maintenance-items). |

## Deliberately out of scope

- **AWS deployment.** Only the [production profile](docs/aws-production-profile.md) as design documentation.
- **Kubernetes.**
- **A universal SQL-to-Flink compiler.** The spec language stays bounded (ADR-008).
- **Further hand-written single features** without the spec engine.
- **Production-scale claims.** Local soak and load tests are production-inspired, not production experience.
- **Cross-region disaster recovery.**

## How to read the evidence

A guarantee counts as delivered only when a test, benchmark or drill report
exists and is linked. Mechanisms present in job code (watermarks, state TTL,
checkpointing) are not treated as proof of recovery behavior.