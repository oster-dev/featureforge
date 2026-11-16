# FeatureForge Status

Last updated: 2026-11-16 · CI: see the
[Actions page](https://github.com/oster-dev/featureforge/actions)

This file is the single place for what is **delivered**, **in progress**, and
**deliberately out of scope**. Other documents link here instead of repeating
status. Claims in the README link to the test or document that proves them.

Related: [README](README.md) · [Architecture](ARCHITECTURE.md) ·
[Platform blueprint](docs/PLATFORM_BLUEPRINT.md) ·
[Streaming provenance](docs/streaming-migration.md) ·
[ADR-008](docs/adr/ADR-008-feature-spec-design.md) ·
[ADR-009](docs/adr/ADR-009-spec-driven-online-stores.md) ·
[ADR-010](docs/adr/ADR-010-time-and-availability-model.md)

---

## Delivered

| Area | What exists | Evidence |
|---|---|---|
| Offline path | Deterministic source data, canonical Parquet backfill, persisted-feature correctness gate, freshness check | [demo.md](docs/demo.md), ADR-002, ADR-003 |
| Online serving | Feast materialization into Redis, online lookup, deterministic ranking | `tests/integration/test_online_serving.py` |
| Streaming features | `user_play_count_1h` and `user_skip_rate_24h`: Kafka, one generic spec-driven Flink job, guarded Redis writes. The joined validation, play-count and Redis flow runs on it. | `tests/integration/` (see below), `test_user_play_count_kafka_to_redis.py` |
| Validation and routing | Rejected events go to a separate topic, valid follow-up events keep flowing | `test_streaming_validation_routing.py` |
| Late data | Late correction of an emitted window and too-late routing for the play count, both skip-rate correction directions | `test_user_play_count_late_events.py`, `test_user_skip_rate_job.py` |
| Replay and conflicts | Play count: replay idempotency including the Kafka-to-Redis materializer, and event-identity conflict detection that fails the job. Skip rate: corrections through the materializer, a replay that preserves the Redis value, and the same event-identity conflict failure. | `test_user_play_count_replay_idempotency.py`, `test_skip_rate_flink_to_redis.py`, `test_user_skip_rate_event_identity.py` |
| Savepoint resume | The generic job stops with a savepoint and a new job resumes from it. A redelivered event does not count again, a correction lands in the restored window, and the late topic stays empty. The test checks that the new job really restored the savepoint. Shown for the play-count spec. It covers a planned stop, not a crash. | `tests/integration/test_generic_job_recovery.py`, `docker-compose.yml` (shared savepoint volume) |
| Crash recovery | The TaskManager is killed while the generic job runs. The same job restarts on its own and restores a checkpoint that contains the earlier events and is not a savepoint. A redelivered event does not count again, a late correction lands in the restored window, and the late topic stays empty. Shown for the play-count spec. The JobManager keeps running and holds the checkpoints in memory. | `tests/integration/test_generic_job_checkpoint_recovery.py` |
| Unified read access | Batch features, streaming play count and skip rate through one client, explicit missing, freshness and non-atomic metadata | `src/featureforge/online.py`, `test_unified_online.py` |
| Skip-rate golden fixture | Deterministic event file with hand-calculated expected values: both window edges, a duplicate delivery, ignored event types, a second user, expiry to a zero denominator. Matched by the Python reference, by the spec reference including a sweep over every event boundary, and by the generic Flink job. | `fixtures/streaming/user_skip_rate_24h_v1.jsonl`, `tests/unit/test_streaming_user_skip_rate_golden.py`, `tests/unit/test_spec_golden_parity.py`, `tests/integration/test_feature_from_spec_job.py` |
| Feature spec model (WP1, stage 1) | Validated declarative spec: windows, lateness, dedupe, update policies, safe expressions. The loader rejects fields that are not in the event contract. Both existing features expressed as specs. | `src/featureforge/spec/`, `feature_repo/specs/`, `tests/unit/test_spec_model.py` |
| Spec validation command (WP1) | `featureforge.cli validate-spec` checks spec files against the spec model and the event contract. Without arguments it checks every spec in `feature_repo/specs`. Exit code 0 means valid, 1 means a spec is invalid and each violated field is listed, 2 means a file is missing or no spec was found. | `src/featureforge/cli.py`, `tests/unit/test_cli_validate_spec.py` |
| Spec test command (WP1) | `featureforge.cli test-spec` evaluates a spec on a JSONL event file for one entity and one observation time with the spec reference, prints every output and checks expected values with `--expect`. Exit code 0 means ok, 1 means invalid input or a failed expectation, 2 means a file is missing. It runs the reference, not the Flink job, and needs no Docker. | `src/featureforge/cli.py`, `tests/unit/test_cli_test_spec.py` |
| Spec-driven online path (WP1, stages 4k, 4l and 6b) | A count or ratio spec with one entity key reaches Redis without a new Python file. `parse_feature_record` validates the Flink record against the spec, including derived values. `RedisSpecStore` writes it with window ordering and componentwise count ordering for `append_only` and `componentwise_monotonic`, the same `WATCH`/`MULTI` retries as the former hand-written stores, and enforces `online.ttl` as a Redis expiry that only a written update refreshes. `materialize_spec` moves a feature topic into Redis. `content_play_count_1h` runs from events through the validation job, the generic Flink job and this path, with hand-calculated windows, dedupe of a redelivered event, an idempotent rerun and a replay that cannot undo the stored state. Since stage 4l the stores and materializers of the two original features are thin wrappers over this path: they keep their typed models, constructors and command lines, and their keys now expire with `online.ttl`. | `src/featureforge/streaming/contracts/count_vector.py`, `src/featureforge/streaming/contracts/feature_record.py`, `src/featureforge/streaming/spec_store.py`, `src/featureforge/streaming/materialize_spec.py`, `src/featureforge/streaming/online_store.py`, `src/featureforge/streaming/skip_rate_store.py`, `src/featureforge/streaming/materialize.py`, `src/featureforge/streaming/materialize_skip_rate.py`, `tests/unit/test_count_vector_update.py`, `tests/unit/test_feature_record.py`, `tests/unit/test_spec_store.py`, `tests/unit/test_materialize_spec.py`, `tests/unit/test_materialize_play_count.py`, `tests/unit/test_materialize_skip_rate.py`, `tests/integration/test_spec_store_redis.py`, `tests/integration/test_redis_skip_rate_store.py`, `tests/integration/test_streaming_redis_online_store.py`, `tests/integration/test_materialize_spec_kafka_to_redis.py`, `tests/integration/test_content_play_count_end_to_end.py` |
| Spec reference (WP1, stage 2) | Exact event-time reference evaluated directly from a spec: all operators, composite keys, missing-value rules, event-field check. Matches both Python references on randomized inputs at window boundaries and the play-count golden fixture. | `src/featureforge/spec/reference.py`, `tests/unit/test_spec_reference.py` |
| Generic Flink job (WP1, stages 3 and 4a to 4g) | A plan derived from a spec (window, lateness, dedupe horizon, counters with filters, derived value) drives one generic job, the only implementation of both streaming features. Supports count aggregations and one entity key, other operators are rejected with a clear error. On the golden fixtures of `user_skip_rate_24h` (two counters and a ratio) and `user_play_count_1h` (a single counter), every emitted window matches the spec reference and the Python reference, hand-calculated windows are present (five for skip rate, seven for play count), no window is emitted twice, each record has exactly the expected fields, and nothing reaches the late topic. The integration tests cover the eight skip-rate job scenarios (late corrections in both directions with duplicates, too-late routing, both window edges, zero denominator), the play-count late-event scenarios, play-count replay idempotency with the Redis materializer, the event-identity conflict failure, the skip-rate Flink-to-Redis flow and the joined play-count flow. The hand-written jobs were deleted after all of these passed on the generic job. The last version is the tag `hand-written-jobs-final`. | `src/featureforge/spec/flink_plan.py`, `jobs/feature_from_spec.py`, `fixtures/streaming/user_play_count_1h_v1.jsonl`, `tests/unit/test_spec_flink_plan.py`, `tests/integration/test_feature_from_spec_job.py`, `tests/integration/test_user_skip_rate_job.py`, `tests/integration/test_user_play_count_late_events.py`, `tests/integration/test_user_play_count_replay_idempotency.py`, `tests/integration/test_skip_rate_flink_to_redis.py`, `tests/integration/test_user_play_count_kafka_to_redis.py` |
| CI | Baseline, Python 3.11/3.12 compatibility job, serving integration, streaming Flink integration with a fresh TaskManager per module, nightly run, concurrency and timeouts, Dependabot for Actions | `.github/workflows/ci.yml`, `scripts/ci/run_flink_integration.sh` |
| Documentation | Architecture, blueprint, demo guide with batch, streaming and feature-spec walkthroughs, ADRs, AWS production profile (design only) | `docs/` |

## In progress

| Item | State |
|---|---|
| WP1: declarative spec and generic engine | Stages 1 to 3 delivered for both features. Stage 4 delivered except JobManager failure: every integration test runs on the generic job, the hand-written jobs are deleted, and the stores and materializers of the two original features are thin wrappers over the spec-driven code. Stages 5 and 6 below. |
| Streaming history and published-as-of retrieval (WP2) | In progress: the history table and an idempotent append exist ([ADR-011](docs/adr/ADR-011-history-table-layout.md)), the materializer does not write evidence yet. The time model is decided in [ADR-010](docs/adr/ADR-010-time-and-availability-model.md) (Accepted): availability is `published_at` of the online sink, assigned by the materializer after a `written` result. No `published_at` exists in code yet. |

### WP1 stages

| Stage | Content | State |
|---|---|---|
| 1 | ADR-008, spec model, validation, two specs | **Done** |
| 2 | Generic reference from a spec, parity with the Python references and the play-count golden fixture | **Done** |
| 3 | Generic Flink job (`count`, `ratio`): golden fixtures, spec-reference parity, job plan from a spec, generic job verified against the fixtures of both features | **Done** |
| 4 | 4a to 4f done: the skip-rate scenarios, the play-count golden fixture, the late-event scenarios, replay and identity conflict, the skip-rate Flink-to-Redis flow and the joined play-count flow all ran on both the hand-written and the generic job. 4g done: the hand-written jobs are deleted, the generic job is the only implementation. 4h done: stop with a savepoint and resume on the generic job. 4i done: recovery from a TaskManager crash with checkpoint state. 4j done: the dedupe state expires after its TTL, shown with a shortened TTL for the play-count spec. 4k done: spec-driven online path with one decision function, one store with `online.ttl` expiry and one materializer, proven by stage 6b. 4l done: the stores and materializers of the two original features are thin wrappers over the spec-driven code (ADR-009, decisions 1 and 10), and their keys expire with `online.ttl`. Open: JobManager failure | In progress |
| 5 | Further operators in Flink, composite keys, Spark backfill | Planned |
| 6 | Two new features by YAML only. 6a done: `content_play_count_1h` (entity `content_id`) runs on the generic job from a spec alone and matches hand-calculated windows. 6b done: `content_play_count_1h` reaches Redis from events through the generic Flink job, `materialize_spec` and `RedisSpecStore` without a new Python file. Open: `user_content_affinity_7d` with a composite key | In progress |

## Next

Order follows the [readiness plan](docs/PLATFORM_BLUEPRINT.md):

1. **WP1:** stages 4 to 6 above.
2. **WP2:** Iceberg offline store, stream history with publication evidence ([ADR-010](docs/adr/ADR-010-time-and-availability-model.md)), point-in-time training datasets, parity test.
3. **WP3:** second online backend (Cassandra) behind one contract suite.
4. **WP4:** feature-serving service and load test.
5. **WP5:** observability, recovery drills, soak run.

Later: catalog and discovery, embeddings, notebooks, profiling, end-to-end ML
workflow (Metaflow, MLflow, registry, rollback).

## Known gaps

| Gap | Impact |
|---|---|
| Recovery is tested for a TaskManager crash only | JobManager failure is **not** proven, and the checkpoints live in JobManager memory, so losing it loses them. Checkpoints on a shared volume are not tested. Both recovery tests cover the play-count spec only. |
| Dedupe expiry is shown with a shortened TTL (30 s) for the play-count spec, not at 24 h or 48 h | The production values are configuration. A redelivery after expiry is counted again, so replay protection is bounded by the TTL. |
| Single-partition Kafka topics only | Partition scaling is not demonstrated. |
| Flink Kafka sinks are `AT_LEAST_ONCE` | No exactly-once claim. |
| TaskManager failure in long CI runs is isolated, not explained | A fresh TaskManager per module isolates it. The metaspace is logged per module to test a class-loading-leak hypothesis. Root cause open. |
| Spark features are hand-written | They belong to WP2. |
| The spec-driven store is proven end to end from events to Redis by its own test for `content_play_count_1h` only | `user_play_count_1h` and `user_skip_rate_24h` run on the same code through their wrappers and are covered by their joined tests and Redis integration tests, which include a real concurrent writer (`test_real_watch_conflict_rechecks_same_window_counts`, `test_streaming_redis_online_store_concurrency.py`). The `content_play_count_1h` path has no separate concurrent-writer test, the code is the same. |
| The wrappers locate their spec relative to the source tree | `RedisUserPlayCountStore`, `RedisUserSkipRateStore` and the two materializer entry points load `feature_repo/specs/*.yaml` from the repository checkout. An installed package without the repository raises `FileNotFoundError`. |
| The Flink image installs `pydantic` and `PyYAML` explicitly | Any new runtime dependency of the job path must be added to `docker/flink/Dockerfile`. The image build checks the project imports. |
| `online.ttl` expires keys in wall-clock time from the last accepted write | All three streaming features expire (2 h for the play counts, 25 h for the skip rate). A replayed historical value lives for `online.ttl` from the time of the write. Keys written before stage 4l have no expiry until their next written update. |
| Published-as-of retrieval is decided but not implemented | ADR-010 and ADR-011 are Accepted. The history table and an idempotent append exist, but the materializer does not record publication evidence yet and there is no published-as-of read. All three streaming features run through the spec-driven store, so the evidence can be added in one place. |
| `window_replace` update policy has no store | Non-count operators cannot be served online yet. |
| `approx_distinct` is exact in the reference | The accepted error bound of an engine implementation is not defined yet. |
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
checkpointing) are not treated as proven recovery behavior unless a test
exercises them. The dedupe state TTL is the example: the configured Flink state
TTL did not expire the state until a test showed it.