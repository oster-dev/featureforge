# Streaming integration provenance

FeatureForge incorporates streaming functionality into the same platform
as its offline feature engineering and online serving paths. The source
repository records the origin of the imported components; it is not a
separate flagship platform maintained alongside FeatureForge.

Source repository: oster-dev/featureforge-stream
Source tag: v0.1.0
Source commit: a681c281806af492a05b2b4a141f2683f752e1a9

The sections below describe successive import stages and subsequent
development inside FeatureForge. Exclusions listed for an earlier stage
apply to that stage only, not to the current repository.

## Initial import: contracts, feature time, and reference computation

Imported event validation, right-closed timestamp mapping, the play-count
reference, their unit tests, and the golden play-count fixture.

Adaptations:

- package imports;
- test filenames;
- fixture location.

Computation and validation semantics were preserved during the import.

Kafka jobs, Redis materializers, and runtime configuration were not part
of this initial stage.

## Second import: routing, play-count store, and materializer

Imported structured event routing, the guarded Redis play-count store,
the Kafka-to-Redis materializer, and their unit tests from the same source
commit.

Adaptations:

- package imports;
- test filenames;
- fixture location;
- a new `streaming` optional dependency group.

Redis key names and store semantics were preserved during the import.

Flink jobs, container configuration, and integration tests were not part
of this stage.

## Third import: real Redis store and WATCH retry verification

Imported live Redis integration tests from the same source commit.

The tests cover:

- feature write and read behavior;
- unknown-user lookup;
- replacement by a newer window, including a zero count;
- rejection of older-window updates;
- unchanged results for identical deliveries;
- increasing corrections within the same window;
- protection against replay undoing a previously accepted correction;
- a controlled Redis WATCH conflict and the store's retry behavior.

Adaptations:

- package imports;
- target paths;
- the `integration` pytest marker.

Redis key names, assertions, and store semantics were preserved during
the import.

Flink jobs, container configuration, and joined Kafka-to-Redis
verification were not part of this stage.

## Fourth import: Flink runtime and joined Kafka-to-Redis verification

Imported the Flink runtime configuration, validation and user play-count
jobs, and a joined Kafka-to-Redis integration test from the same source
commit.

Adaptations:

- package imports;
- Kafka topic names;
- the joined test's right-closed window assertion.

The Flink image installs the project without transitive dependencies to
avoid conflicting with the base image's Apache Beam installation.
Pydantic is installed explicitly for the streaming contracts.

The joined test verifies the following bounded execution path:

1. Publish a valid play event to Kafka.
2. Validate the event through the Flink validation job.
3. Compute `user_play_count_1h` through the Flink feature job.
4. Materialize the resulting Kafka feature records into Redis.
5. Read the stored feature and assert its entity, count, and window
   metadata.

This established the initial end-to-end proof for one play event. It did
not establish checkpoint recovery, continuous-operation guarantees, or
all late-event behaviors.

## Subsequent development: bounded replay and identity-conflict coverage

After the imports, FeatureForge added stricter replay and conflict
verification in PR #6.

The work was integrated into `main` at commit `fdbbfe8`:

`test: verify streaming replay idempotency and identity conflicts`

Changed files:

- `jobs/user_play_count_1h.py`;
- `tests/integration/test_user_play_count_replay_idempotency.py`;
- `.github/workflows/ci.yml`.

This was development within FeatureForge, not another unchanged import
from the source repository.

### Bounded replay idempotency

The replay integration test publishes repeated deliveries of the same
logical play event with different `ingested_at` timestamps.

The test verifies that:

1. Validation and the initial bounded Flink computation complete
   successfully.
2. The initial materialization reports exactly 60 written window updates.
3. The final stored feature has a play count of one and the expected
   right-closed window metadata.
4. Another delivery is published and the input is reprocessed through
   fresh bounded validation and feature-job executions.
5. The materializer resumes with its existing consumer group and reports
   exactly 59 stale updates and one unchanged update.
6. Both the decoded feature and the serialized Redis value remain
   identical to their values before replay.

The replay executions reconstruct results from Kafka input. They do not
restore the previous Flink job's state from a checkpoint or savepoint.

### Event-identity conflict handling

The conflict integration test publishes two events with the same
`event_id` but different `content_id` values.

It requires:

- a nonzero exit code from the feature-job subprocess;
- `EventIdentityConflictError` in the captured output;
- the conflict message identifying the expected event ID.

A subprocess timeout is a test failure, not an accepted conflict result.

### Optional fail-fast execution

The play-count job now accepts `--no-restart`.

The conflict test uses this option to disable automatic job restarts for
its diagnostic execution. Invocations without the option preserve the
existing restart configuration.

This option does not change feature computation or Redis write policies.

### CI integration

The Flink integration CI job explicitly selects both:

- `tests/integration/test_user_play_count_kafka_to_redis.py`;
- `tests/integration/test_user_play_count_replay_idempotency.py`.

The Redis store and concurrency integration tests run in the separate
Redis and Feast serving integration CI job.

## Current streaming implementation

The current repository contains the following streaming components.

### Validation and routing

`jobs/validate_and_route_events.py` supports continuous execution and
bounded replay.

It validates incoming records and routes them to separate validated and
rejected Kafka outputs.

### Event-time play-count computation

`jobs/user_play_count_1h.py` implements:

- reference-compatible right-closed event-time mapping;
- one-hour sliding windows with a one-minute slide;
- a ten-minute bounded-out-of-orderness watermark strategy;
- thirty minutes of allowed lateness;
- a side output and Kafka sink for events too late for all assigned
  windows;
- deduplication keyed by event ID;
- explicit failure for conflicting logical payloads;
- a twenty-four-hour deduplication-state TTL;
- checkpointing at ten-second intervals;
- continuous execution and bounded replay.

The logical event identity excludes `ingested_at`. Identical duplicate
deliveries do not emit another event or refresh the deduplication
retention period.

### Kafka-to-Redis materialization

`src/featureforge/streaming/materialize.py` consumes feature records and
writes them through the guarded Redis store.

The test suite covers materializer behavior including write-before-commit
ordering, failed writes, invalid records, bounded consumption, committed
offset handling, and store-result accounting.

Unit-test coverage of these behaviors must not be confused with a
distributed crash-recovery proof.

### Guarded online storage

`src/featureforge/streaming/online_store.py` provides the play-count
feature representation and guarded Redis writes.

The verified write policy:

- accepts newer-window values, including zero;
- rejects older-window values;
- leaves identical values unchanged;
- accepts increasing corrections within the same window;
- rejects decreasing values within the same window;
- preserves a competing newer value in the tested WATCH-conflict case.

These same-window correction rules apply to the append-only play-count
contract. They are not a general policy for arbitrary mutable features.

## Verification recorded on November 11, 2026

The local audit was performed on `main` at commit `fdbbfe8`.

### Static and environment checks

The audit confirmed:

- Python 3.13.9;
- Java 17;
- no broken requirements reported by `python -m pip check`;
- package import from the local source tree;
- Ruff formatting checks passing;
- Ruff lint checks passing;
- no whitespace errors reported by `git diff --check`.

### Serving-environment preparation

The first baseline run found four online-serving failures because the
tested known entities returned no online feature values.

After applying the Feast definitions and materializing the existing
offline snapshots, all five online-serving integration tests passed.

The preparation used:

- start time: `2026-03-10T00:00:00+00:00`;
- exclusive end time: `2026-03-26T00:00:00+00:00`.

The latest offline snapshots were dated March 25, 2026, and contained
500 user rows and 250 content rows.

No application-code change was required to obtain the passing serving
result. This demonstrates that the prepared serving environment works;
it does not establish why the earlier online values were missing.

### Streaming integration results

All 13 selected Redis and Flink streaming integration test cases passed
in a dedicated local run.

The cases cover:

- Redis store behavior;
- the controlled WATCH-conflict retry;
- the joined Kafka-to-Flink-to-Redis path;
- bounded replay idempotency;
- explicit event-identity conflict failure.

### Complete test-suite result

After serving preparation, the complete test suite passed in one
combined execution:

`272 passed in 40.35s`

There were no reported failures, skips, or deselected tests in that run.

After the run:

- Flink reported no running jobs;
- the Git working tree was clean;
- local `main` was synchronized with `origin/main`.

These results describe the tested local state and the assertions in the
current suite. They are not a blanket production-readiness claim.

## Verification boundaries

The following distinctions remain important.

### Implemented mechanisms versus runtime proofs

Watermarks, allowed lateness, too-late routing, state TTL, and
checkpointing are present in the job code.

The current three Flink integration test cases do not independently
establish:

- rejected-record delivery through the real Kafka validation path;
- a late event causing a correction after controlled watermark
  advancement;
- delivery to the too-late Kafka topic;
- restoration from a completed checkpoint or savepoint;
- deduplication behavior after state TTL expiry;
- sustained continuous-operation behavior.

Reference and Redis tests provide narrower proofs for their respective
components. They do not replace these distributed runtime scenarios.

### Delivery guarantees

The Flink Kafka sinks are configured with `AT_LEAST_ONCE`.

Passing replay and guarded-write tests must not be described as an
end-to-end exactly-once guarantee.

### Recovery and replay

Fresh bounded reprocessing is not checkpoint or savepoint recovery.

Enabling checkpointing does not by itself demonstrate successful state
restoration or durable recovery across container replacement.

### Zero values

The Redis integration suite verifies that a newer feature window can
replace a previous count with zero.

It does not establish that the Flink window job emits a zero for an
otherwise inactive user merely because time advances.

### Feature scope

The imported and integrated Flink feature job computes
`user_play_count_1h`.

Support for other interaction event types in the event contract does not
mean that separate streaming features for those event types have been
implemented.

### Test-environment boundaries

The full local suite requires prepared infrastructure and, for the
serving tests, applied Feast definitions and materialized offline data.

At the audited commit, `test_online_serving.py` is not marked with
`integration`. It can therefore run under `-m "not integration"` when its
local availability check passes.

That check verifies offline-file availability and Redis connectivity;
it does not establish that the expected online values are materialized.

## Known test-maintenance follow-ups

The audit identified the following maintenance items:

- add subprocess timeouts to the original joined Kafka-to-Redis test;
- clean up the Redis key created by that original test;
- define cleanup for dynamically created integration-test Kafka topics;
- use separate rejected and too-late topics in the original joined test
  when expanding its error-path coverage;
- align the online-serving tests with the intended integration-test
  marker boundary.

These are distinct from missing feature implementations and should not
be presented as failures of the passing test cases.

## Current milestone statement

FeatureForge has an integrated and locally verified play-count streaming
path with event validation, event-time window computation, bounded replay
idempotency, explicit identity-conflict detection, and guarded Redis
materialization.

The complete existing test suite passed after preparing the local
serving environment.

Broader continuous-operation, late-event runtime, TTL-expiry, and
checkpoint-recovery guarantees require their own explicit verification.