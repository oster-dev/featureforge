# FeatureForge Status

Last updated: 2026-11-12 · Baseline: `1281d2c` on `main` · CI: see the
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
| Feature spec model (WP1, stage 1) | Validated declarative spec: windows, lateness, dedupe, update policy, safe expressions. Both existing features expressed as specs. | `src/featureforge/spec/`, `feature_repo/specs/`, `tests/unit/test_spec_model.py` |
| CI | Baseline, Python 3.11/3.12 compatibility, serving integration, streaming Flink integration with a fresh TaskManager per module, nightly run, concurrency and timeouts, Dependabot for Actions | `.github/workflows/ci.yml`, `scripts/ci/run_flink_integration.sh` |
| Documentation | Architecture, blueprint, demo guide, ADRs, AWS production profile (design only) | `docs/` |

## In progress

| Item | State |
|---|---|
| WP1: declarative spec and generic engine | Stage 1 delivered (spec model and validation). Next stages below. |
| Streaming history and published-as-of retrieval (WP2) | Not started. Needs the time-model decision (`event_time` vs. `published_at`). |

### WP1 stages

| Stage | Content | State |
|---|---|---|
| 1 | ADR-008, spec model, validation, two specs | **Done** |
| 2 | Generic Pandas reference from a spec, parity with the hand-written skip-rate reference on the golden fixtures | Next |
| 3 | Generic Flink job (`count`, `ratio`) matching the hand-written jobs | Planned |
| 4 | Migrate both features, delete the hand-written jobs when parity is green | Planned |
| 5 | Further operators, composite keys, Spark backfill | Planned |
| 6 | Two new features by YAML only | Planned |

## Next

Order follows the [readiness plan](docs/PLATFORM_BLUEPRINT.md):

1. **WP1:** stages 2 to 6 above.
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
| Specs are not used by any runtime component yet | Jobs, stores and references are still hand-written. The spec is validated, not executed. |
| Spec filters are not checked against the event contract's field names | A misspelled field passes validation. |
| `online.ttl` of `user_play_count_1h` in its spec is a placeholder | Not read from the existing store. |
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