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
not establish checkpoint recovery, sustained continuous operation, or
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

The play-count job accepts `--no-restart`.

The conflict test uses this option to disable automatic job restarts for
its diagnostic execution. Invocations without the option preserve the
existing restart configuration.

This option does not change feature computation or Redis write policies.

### CI integration at this stage

The Flink integration CI job was extended to select both:

- `tests/integration/test_user_play_count_kafka_to_redis.py`;
- `tests/integration/test_user_play_count_replay_idempotency.py`.

The Redis store and concurrency integration tests run in the separate
Redis and Feast serving integration CI job.

## Subsequent development: unified online feature access

PR #7 introduced a common online read interface for existing Feast batch
features and the Redis-backed `user_play_count_1h` streaming feature.

The work was integrated into `main` at commit `c8df9a8`:

`feat: add unified batch and streaming online access (#7)`

The implementation adds:

- `src/featureforge/online.py`;
- `tests/unit/test_online.py`;
- `tests/unit/test_online_cli.py`;
- `tests/integration/test_unified_online.py`.

The change also updates CI selection and this provenance document.

### Common Python API

`OnlineFeatureClient` reads:

- user engagement batch features through Feast;
- content popularity batch features through Feast;
- `user_play_count_1h` through the existing streaming Redis store.

The existing serving functions, deterministic ranking, and Redis write
policies remain unchanged.

Each returned feature includes:

- feature name and entity ID;
- backend;
- value;
- availability;
- observation time when available;
- age relative to the request's reference time;
- freshness classification;
- schema version when available;
- error type when applicable.

Availability and freshness are separate dimensions.

A valid zero remains present. A missing value is not replaced with zero.
A backend error is not reported as a missing feature.

Combined reads explicitly report `atomic: false`.

### Batch observation timestamps

The Feast adapter requests event timestamps from the online response
using `to_dict(include_event_timestamps=True)`.

Compatibility was checked locally with Feast 0.66.0. The observed
feature timestamp fields contain epoch seconds.

The adapter reads these timestamps from the online response. It does not
infer online timestamps from the latest offline partition.

Entity-key timestamp fields are not used as feature observation times.

A feature event timestamp is not evidence of its publication time or of
its availability to an earlier prediction.

### Freshness policy

The caller supplies:

- a reference time;
- a maximum batch-feature age;
- a maximum streaming-feature age.

The interface distinguishes:

- `fresh`;
- `stale`;
- `unknown`;
- `future`.

A feature with no usable timestamp is not automatically classified as
fresh.

Freshness evaluated against a historical demo reference time is not a
claim that the feature is fresh relative to the current wall clock.

### JSON CLI

The common read interface is available through:

`python -m featureforge.online`

The CLI accepts entity IDs, backend configuration, an optional reference
time, and explicit maximum-age parameters.

It emits JSON and uses the following exit behavior:

- successful reads, including explicit missing values: exit code 0;
- responses containing feature errors: exit code 1;
- invalid CLI requests: exit code 2.

This module entry point supplements the existing `featureforge`
commands; it does not replace them.

### Unified integration-test boundary

The unified integration test reads prepared Feast values and a streaming
value written under an isolated test prefix.

It verifies:

- combined batch and streaming access;
- preservation of a valid streaming zero;
- returned observation timestamps;
- freshness against the explicit test reference;
- missing results for an unknown user;
- non-atomic response metadata.

The test derives its reference from the prepared offline snapshots and
checks that the online batch timestamps match those snapshots.

The directly prepared streaming value tests the read interface.
Kafka/Flink production of streaming features remains covered by the
separate pipeline integration tests.

### CI integration

The unified integration test runs in the Redis and Feast serving job
after source generation, backfill, Feast application, and materialization.

The JUnit validation checks that the selected Redis and unified test
modules are represented without skips, failures, or errors.

The CI materialization end time was extended to March 13, 2026, so that
the March 12 snapshot lies within the declared exclusive end boundary.

## Subsequent development: routing and continuous late-event verification

The branch `test/streaming-routing-and-late-events` adds two dedicated
integration tests:

- `tests/integration/test_streaming_validation_routing.py`;
- `tests/integration/test_user_play_count_late_events.py`.

Both tests have passed individually and as part of the complete local
test suite.

At the time of this update, the expanded GitHub CI result and merge for
this branch have not yet been recorded.

No application-job change was required for these local results.

### Rejected-event routing

The routing test publishes, in order:

1. Invalid JSON.
2. A valid play event.
3. A contract-invalid event with an unsupported event type.
4. Another valid play event.

It runs the real validation job in bounded mode and reads the completed
Kafka output ranges.

The assertions require:

- exactly two validated records;
- exactly two rejected records;
- the expected valid event identities;
- rejection output matching the executable routing contract;
- both `INVALID_JSON` and `EVENT_CONTRACT_VIOLATION` reasons.

This verifies that invalid records are routed through the real Flink and
Kafka path without preventing valid follow-up records from being
processed.

The test uses isolated topics and requests their deletion during cleanup.

Exact record counts describe this failure-free bounded test execution.
They do not establish exactly-once delivery across recovery.

### Continuous late correction

The late-event test submits the existing play-count job to the Flink
cluster in detached continuous mode with automatic restart disabled.

It then:

1. Publishes a play event at 11:30.
2. Publishes a progress event at 12:11.
3. Waits for the tested user's window ending at 12:00 to be emitted with
   a count of one.
4. Publishes a distinct event ID for another play at 11:30.
5. Waits for the same 12:00 window to be emitted with a count of two.

Waiting for the initial output before publishing the correction
distinguishes this scenario from loading all events before a bounded job
finishes.

The correction is a new logical event, not a duplicate delivery of the
original event.

### Too-late routing

After verifying the correction, the same test:

1. Publishes a further progress event at 14:00.
2. Waits for the earlier progress user's window ending at 13:10 to be
   emitted.
3. Publishes another distinct event at 11:30.
4. Waits for that event's identity in the late Kafka topic.

The progress-window output demonstrates sufficient watermark
advancement to exceed all windows assigned to the tested 11:30 event and
their configured allowed-lateness retention.

The late record must contain:

- error schema version `v1`;
- reason `TOO_LATE_FOR_ALL_ASSIGNED_WINDOWS`;
- feature name `user_play_count_1h`;
- the original event payload.

The test waits for observable outputs rather than assuming watermark
progress after a fixed sleep.

### Cleanup and scope

The continuous test resolves its submitted Flink job ID and cancels that
specific job during cleanup.

It closes its consumers and requests deletion of its isolated topics.

This test verifies Flink's feature and late-event Kafka outputs. It does
not run those corrections through the Redis materializer.

The Redis correction policy remains separately covered by real Redis
integration tests.

### CI selection in this branch

The workflow change adds both new test modules to the existing Flink
integration step alongside the joined pipeline and replay tests.

The selected modules are:

- `tests/integration/test_user_play_count_kafka_to_redis.py`;
- `tests/integration/test_user_play_count_replay_idempotency.py`;
- `tests/integration/test_streaming_validation_routing.py`;
- `tests/integration/test_user_play_count_late_events.py`.

The expanded CI selection must pass in GitHub Actions before this branch
is treated as CI-verified.

## Current streaming implementation

The current implementation consists of the following components.
The routing and late-event test additions described above remain
branch-local until merged.

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

The logical event identity excludes `ingested_at`.

Identical duplicate deliveries do not emit another event or refresh the
deduplication retention period.

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

The write result `stale` describes rejection under this ordering policy.
It is distinct from the read interface's freshness classification.

### Consumer access

`src/featureforge/online.py` provides common online access to the
supported batch feature groups and the streaming play count.

It preserves backend-specific storage and publication mechanisms.
It does not write into Feast's internal Redis encoding.

## Verification recorded on November 11, 2026

The results below record separate executions and repository states.
Later results do not retroactively change earlier audit records.

### Initial audit at `fdbbfe8`

The initial local audit was performed on `main` at commit `fdbbfe8`.

#### Static and environment checks

The audit confirmed:

- Python 3.13.9;
- Java 17;
- no broken requirements reported by `python -m pip check`;
- package import from the local source tree;
- Ruff formatting checks passing;
- Ruff lint checks passing;
- no whitespace errors reported by `git diff --check`.

#### Serving-environment preparation

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
result.

This demonstrates that the prepared serving environment works. It does
not establish why the earlier online values were missing.

#### Streaming integration results

All 13 selected Redis and Flink streaming integration test cases passed
in a dedicated local run.

The cases cover:

- Redis store behavior;
- the controlled WATCH-conflict retry;
- the joined Kafka-to-Flink-to-Redis path;
- bounded replay idempotency;
- explicit event-identity conflict failure.

#### Complete test-suite result

After serving preparation, the complete test suite passed in one
combined execution:

`272 passed in 40.35s`

There were no reported failures, skips, or deselected tests in that run.

After the run:

- Flink reported no running jobs;
- the Git working tree was clean;
- local `main` was synchronized with `origin/main`.

### Unified online access verification

Before PR #7 was merged, the expanded complete local suite passed:

`297 passed in 41.01s`

This included:

- the previous 272 test cases;
- 19 unified online API unit-test cases;
- five CLI test cases;
- one unified Feast/Redis integration test.

A manual module-CLI call also returned the prepared batch values and
their online observation timestamps.

The streaming value under the standard prefix was explicitly reported
as missing. The isolated integration-test value did not populate that
standard prefix.

After the merge, local `main` was synchronized at `c8df9a8` with a clean
working tree.

### Routing and late-event branch verification

The routing test passed individually:

`1 passed in 3.93s`

The continuous late-event test passed individually:

`1 passed in 15.33s`

After the targeted continuous test, Flink reported no running jobs.

The complete local suite, including both new integration tests, then
passed:

`299 passed in 61.08s`

There were no reported failures, skips, or deselected tests in that run.

The same complete verification block also confirmed:

- Ruff formatting: 88 files already formatted;
- Ruff lint: all checks passed;
- no whitespace errors reported by `git diff --check`.

The 299 test cases comprise the previous 297 cases plus:

- one real Kafka/Flink validation-routing test;
- one continuous Flink test covering both late correction and too-late
  routing.

The expanded GitHub CI result and merge commit for this branch have not
yet been recorded.

These results describe the tested states and assertions. They are not a
blanket production-readiness claim.

## Verification boundaries

The following distinctions remain important.

### Implemented mechanisms versus runtime proofs

Watermarks, allowed lateness, too-late routing, state TTL, and
checkpointing are present in the job code.

The branch-local integration tests now provide successful local evidence
for:

- rejected-record delivery through the real Kafka validation path;
- continued processing of valid follow-up events;
- correction of an already emitted window in a continuous job;
- delivery to the too-late Kafka topic after observed watermark-driven
  progress.

They do not establish:

- restoration from a completed checkpoint or savepoint;
- deduplication behavior after state TTL expiry;
- sustained continuous-operation behavior or performance targets;
- behavior across multiple Kafka partitions.

Reference and Redis tests provide additional, narrower proofs for their
respective components.

### Delivery guarantees

The Flink Kafka sinks are configured with `AT_LEAST_ONCE`.

Passing replay, routing, correction, and guarded-write tests must not be
described as an end-to-end exactly-once guarantee.

### Recovery and replay

Fresh bounded reprocessing is not checkpoint or savepoint recovery.

Enabling checkpointing does not by itself demonstrate successful state
restoration or durable recovery across container replacement.

Cancelling the continuous test job during cleanup is not a recovery
test.

### Deduplication horizon

Deduplication uses a finite twenty-four-hour state TTL.

The existing tests do not establish behavior after that state expires.
They must not be presented as proof of unlimited replay protection.

### Zero values and inactivity

The Redis integration suite verifies that a newer feature window can
replace a previous count with zero.

The unified read tests verify that a stored zero remains a present value.

Neither establishes that the Flink window job emits a zero for an
otherwise inactive user merely because time advances.

### Feature scope

The imported and integrated Flink feature job computes
`user_play_count_1h`.

Support for other interaction event types in the event contract does not
mean that separate streaming features for those event types have been
implemented.

`user_skip_rate_24h` remains a separate integration item in the platform
blueprint.

### Historical publication

The verified Kafka-to-Redis and unified online paths do not establish a
versioned streaming-history publication or historical retrieval contract.

An online event timestamp is not proof that a value was published before
a historical prediction.

The interface does not fabricate `published_at` values or claim
published-as-of retrieval.

### Test-environment boundaries

The full local suite requires prepared infrastructure and, for the
serving tests, applied Feast definitions and materialized offline data.

At the audited baseline, `test_online_serving.py` is not marked with
`integration`. It can therefore run under `-m "not integration"` when its
local availability check passes.

That check verifies offline-file availability and Redis connectivity.
It does not establish that the expected online values are materialized.

The unified and newly added routing and late-event integration tests are
marked with `integration`.

### Read-interface boundaries

Combined online reads are not atomic cross-store snapshots.

Per-feature observation times and freshness are returned where supported.
Publication time and a universal definition version are not invented
when unavailable.

A missing numeric value is not silently replaced with zero in the
unified interface.

Existing legacy serving and ranking functions retain their previous
behavior.

## Known test-maintenance follow-ups

The audit identified the following maintenance items:

- add subprocess timeouts to the original joined Kafka-to-Redis test;
- clean up the Redis key created by that original test;
- add topic cleanup to the older joined and replay integration tests;
- use separate rejected and too-late topics in the original joined test
  when expanding its error-path coverage;
- align the existing online-serving tests with the intended
  integration-test marker boundary.

The new routing and continuous late-event tests already request cleanup
of their isolated topics.

The continuous late-event test also cancels its own resolved job ID.

These maintenance items are distinct from missing feature
implementations and should not be presented as failures of the passing
test cases.

## Current milestone statement

FeatureForge has an integrated and locally verified play-count streaming
path with event validation, event-time window computation, bounded replay
idempotency, explicit identity-conflict detection, and guarded Redis
materialization.

Unified batch and streaming online access is integrated on `main`
through PR #7.

Additional branch-local tests have successfully verified rejected-event
routing, valid follow-up processing, continuous late-window correction,
and too-late Kafka routing.

The complete local suite, including the routing and continuous
late-event tests, passed with 299 tests in 61.08 seconds.

The expanded GitHub CI result and merge for this branch remain to be
recorded.

The remaining platform work includes skip-rate integration, versioned
streaming history, recovery evidence, explicit TTL boundaries, and a
reproducible completion demonstration.

These remaining items must be completed or explicitly scoped before
declaring the broader blueprint-defined streaming phase finished.