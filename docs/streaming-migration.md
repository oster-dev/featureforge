# Streaming integration provenance

Source repository: oster-dev/featureforge-stream
Source tag: v0.1.0
Source commit: a681c281806af492a05b2b4a141f2683f752e1a9

## Initial import

Imported event validation, right-closed timestamp mapping, the play-count
reference, their unit tests, and the golden play-count fixture.

Adaptations: package imports, test filenames, and fixture location.
Computation and validation semantics are unchanged.

Kafka jobs, Redis materializers, and runtime configuration are not part
of this initial import.

## Second import: routing, play-count store, and materializer

Imported structured event routing, the guarded Redis play-count store, the
Kafka-to-Redis materializer, and their unit tests from the same source commit.

Adaptations: package imports, test filenames, fixture location, and a new
`streaming` optional dependency group.

Redis key names and store semantics are unchanged.

Flink jobs, container configuration, and integration tests are not part of
this import.

## Third import: real Redis store and WATCH retry

Imported the two live Redis integration tests from the same source commit:

- append-only play-count write and read behavior;
- concurrent WATCH conflict and retry behavior.

Adaptations: package imports, target paths, and the `integration`
pytest marker.

Redis key names, assertions, and store semantics are unchanged.

Flink jobs, container configuration, and joined Kafka-to-Redis
verification remain outside this import.
