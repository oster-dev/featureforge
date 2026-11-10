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
