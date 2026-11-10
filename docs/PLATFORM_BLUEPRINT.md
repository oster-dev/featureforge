# FeatureForge Platform Blueprint

Status: Proposed implementation baseline
Date: 2026-11-10
Existing release baseline: v0.1.0

## 1. Product

FeatureForge is evolving into a production-shaped ML feature platform:

Batch and streaming feature computation, versioned historical storage,
low-latency online serving, and an end-to-end ML workflow.

The primary user is an ML practitioner who needs to discover, select,
validate, and retrieve features for training and inference without
implementing infrastructure-specific access logic.

This document describes the target system. It does not declare its
unimplemented capabilities complete.

## 2. Repository decision

FeatureForge is the active platform repository.

FeatureForge Stream v0.1.0 is a frozen reference implementation.
Selected capabilities will be integrated incrementally, preserving
their implementation provenance, tests, and guarantee boundaries.

The existing FeatureForge v0.1.0 release remains unchanged.

The planned ML-workflow phase extends the training and serving foundation
already present in FeatureForge. It does not create a separate platform.

One repository does not imply one process, container, Python environment,
or deployment lifecycle.

## 3. Existing foundation

FeatureForge v0.1.0 documents:

- Deterministic synthetic source data and observation labels.
- Event-time and ingestion-time modeling.
- Pandas and PySpark feature computation with parity tests.
- User engagement and content popularity batch features.
- Canonical partitioned offline feature storage.
- Backfills, validation gates, freshness checks, and run manifests.
- Feast historical retrieval and Redis materialization.
- Time-based ML evaluation and persisted model artifacts.
- Online lookup and deterministic ranking.
- Baseline and Redis/Feast serving CI.
- A local demo and documented AWS operating profile.

These capabilities are retained unless a tested replacement is introduced.

Existing event-time correctness must not be presented as proof that
every historical feature value was available to the online system at
the corresponding prediction time.

## 4. Initial use case

Extend the existing user-content prediction workflow with recent
streaming activity.

The initial integrated feature set combines:

- Existing seven-day user engagement features.
- Existing seven-day content popularity features.
- user_play_count_1h.
- user_skip_rate_24h.

Do not add more features before this vertical slice works.

Before dataset integration, audit and document the existing label:

- What prediction is made.
- The prediction timestamp.
- The outcome horizon.
- When the label becomes observable.
- How incomplete outcomes are excluded.
- Which source event types are actually generated.

Do not fabricate skip or complete events from unsupported source fields.
The current streaming replay adapter supports play and impression.
Meaningful skip-rate training requires an explicitly versioned source
extension or a compatible declared dataset.

Synthetic-data results demonstrate engineering behavior, not real-world
recommendation quality.

## 5. Component ownership

### Contracts and catalog

Own feature identity, entities, schema, definition versions, window
semantics, quality rules, freshness rules, and feature-set membership.

Start with a small typed catalog and explicit implementation references.
Do not build a universal feature compiler.

A shared definition does not mean Spark and Flink automatically execute
one implementation. Matching semantics require independent tests.

### Batch computation

Retain existing Pandas references, PySpark implementations, backfills,
and canonical offline publication.

Backfills produce explicitly identified runs. A later backfill must not
rewrite the recorded availability of an earlier online value.

### Streaming computation

Integrate event validation, Flink jobs, late routing, finite-horizon
deduplication, and feature output contracts.

Preserve existing feature-specific correction policies initially.
Do not present append-only count ordering as general revision support.

### Storage and publication

Keep existing batch tables and introduce separately versioned streaming
history under a documented platform storage contract.

Current bounded play-count snapshots are reference artifacts, not an
already implemented continuous historical sink.

Online stores hold current accepted values.
Historical storage retains the versions needed for declared retrieval.

Both are projections of published feature updates; they are not assumed
to become visible atomically.

### Access

Provide one consumer-facing interface for:

- Feature discovery and metadata.
- Historical training retrieval.
- Online feature retrieval.
- Explicit freshness and missing-value status.

Use Feast for supported batch operations.
Use an adapter for streaming reads where its semantics require one.

Do not write directly into Feast's internal Redis encoding.

### ML workflow

Extend the existing training foundation to consume a declared mixed
batch/stream feature set.

Record dataset identity, feature versions, label definition, split
boundaries, code revision, model artifact, and evaluation metrics.

The inference example must load the trained model and use the same
declared input schema. Existing deterministic ranking remains a separate
baseline, not proof of model-backed inference.

## 6. Time and availability contract

Keep these concepts distinct:

- event_time: when an interaction occurred.
- observation_time: the feature window endpoint.
- computed_at: when a computation produced an update.
- published_at: when a particular sink accepted an update.
- definition_version: the meaning and schema of the feature.
- run_id or generation_id: the computation that owns the update.
- update identity: identifies delivery or replay of a specific update.

A revision number is added only if a producer can assign it consistently.
Kafka offsets are scoped to a topic partition, not universal revisions.

An ingestion timestamp is not an online availability timestamp.
A historical export timestamp is not proof of earlier online visibility.

Historical retrieval modes must be explicit:

1. Event-time reconstruction:
   Reconstruct values from a declared source dataset.

2. Published-as-of retrieval:
   Select only feature versions with recorded publication evidence
   before the prediction timestamp.

These modes must never be silently interchanged.

Exact reconstruction of what a prediction consumed is proved through
logged feature vectors and their metadata, not inferred from eventual
agreement between stores.

## 7. Online correction contract

Preserve the tested policies:

- Older observation windows do not replace newer ones.
- Same-window play counts can advance under append-only assumptions.
- Same-window skip-rate updates compare underlying counts.
- A decreasing rate can be a valid correction.
- Incomparable count vectors fail explicitly.
- Independent computations use separate generations or namespaces.

The observed Feast 0.66.0 default Redis path skips equal-timestamp writes.
Therefore, unchanged Feast push is not assumed to support provisional
same-window corrections.

Do not disable deduplication globally or modify observation timestamps
to force updates through.

Any future replacement must pass the existing correction, concurrency,
and replay tests before adoption.

## 8. Historical publication and recovery

The first historical streaming integration must declare:

- Update identity and ordering scope.
- Durable source retention requirements.
- Snapshot or commit boundaries.
- Publication failure behavior.
- Replay and deduplication behavior.
- Which timestamp is supported by publication evidence.

Do not claim atomic Kafka/Redis/historical publication.

If a sink succeeds but recording its publication fails, the discrepancy
must be observable and reconciled. Its earlier availability must not be
invented retrospectively.

Prediction logs capture the feature values actually consumed.

A completed historical dataset is immutable and referenced by a manifest.
An incomplete output directory is not an accepted dataset.

## 9. Consumer experience

Initial interfaces are a Python API and CLI, not a new network service.

Training requests provide entity keys and prediction timestamps.
Results include selected feature versions and dataset provenance.

Online responses include values, observation times, freshness status,
and the identifiers needed for diagnosis.

Missing, stale, and backend-error states remain distinct.
Do not silently substitute zero for missing features.

Mixed batch/stream reads are not atomic cross-store snapshots.
The consumer receives per-feature timestamps and an explicit policy for
allowed staleness and time differences.

Training output is partitioned and batch-readable.
Do not require loading the entire dataset into one Pandas frame.

One notebook demonstrates feature discovery, dataset construction,
evaluation, and online lookup.

## 10. Runtime compatibility

The existing batch CI uses Python 3.13.
The demonstrated Flink runtime uses Python 3.11 and Flink 1.20.3.

Preserve separate runtime environments until compatibility is tested.
Do not install the full platform dependency set into every container.

Pin tested runtime combinations during integration.
A broad package version range is not compatibility evidence.

## 11. Operational evidence

Measure rather than assume:

- Input rate and consumer lag.
- Invalid, duplicate, and too-late events.
- Feature update latency and observation age.
- Redis write outcomes and conflicts.
- Historical publication failures.
- Checkpoint and recovery behavior.
- Online lookup p50, p95, and p99 latency.
- Historical dataset read throughput.

Observation age, pipeline lag, and publication latency are different
metrics and must be reported separately.

Watermark-dependent output does not guarantee wall-clock refresh after
input becomes idle. Stale values must become visibly stale or expire
under an explicit serving policy.

Benchmarks record hardware, versions, workload, concurrency, errors,
and resource usage. Initial measurements establish targets; they do
not retroactively prove an SLO.

## 12. Acceptance demonstration

The integrated demonstration must:

1. Start from a documented clean environment.
2. Generate a versioned, compatible event dataset.
3. Build and validate the existing batch feature store.
4. Run validation and a selected streaming feature job.
5. Publish and retrieve streaming online values.
6. Demonstrate a valid late correction and duplicate handling.
7. Persist accepted streaming history with explicit time semantics.
8. Build an immutable mixed-feature training dataset.
9. Train and evaluate a baseline with time-based splits.
10. Load the model and predict using online platform features.
11. Log the consumed feature vector and metadata.
12. Produce a machine-readable evidence report.

A failure demonstration separately verifies a declared recovery boundary
and reconciles the resulting state.

The happy-path demo is not an end-to-end exactly-once proof.

## 13. Migration sequence

Stage A: Baseline and compatibility
- Preserve the released batch behavior.
- Inventory implementations and dependency constraints.
- Record source commits for imported streaming components.

Stage B: First streaming vertical slice
- Integrate play-count contracts, reference, job, store, and tests.
- Verify validation-to-online behavior inside FeatureForge.
- Add a common online read interface.

Stage C: Corrections and historical publication
- Integrate skip-rate contracts and tests.
- Define and test streaming history publication and retrieval.
- Verify late corrections, replay, and availability boundaries.

Stage D: Existing ML workflow extension
- Audit labels and source coverage.
- Build mixed-feature datasets.
- Compare batch-only and batch-plus-stream models.
- Add model-backed online inference.

Stage E: Operational and application release
- Add metrics, recovery evidence, and benchmarks.
- Provide a clean-clone demo and practitioner notebook.
- Publish architecture, limitations, and a reproducible release.

Later bounded extensions may include a cloud staging deployment,
Metaflow orchestration, MLflow tracking, or embedding retrieval.
They do not block the first integrated vertical slice.
