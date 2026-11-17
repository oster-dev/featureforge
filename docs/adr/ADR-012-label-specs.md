# ADR-012 Label specs for training data

- Status: Accepted. Decides what a label is, how its window and its observability work and which events it may use. Nothing is implemented yet, the implementation follows in separate changes.
- Date: 2026-11-17
- Author: Nico Ostermann
- Deciders: Nico Ostermann
- Extends: ADR-008, ADR-010

## Context

The readiness plan asks practitioners to define features and labels in the same declarative way. ADR-008 covers features only and has no section on labels. The code holds two facts that shape this decision.

- The batch generator `generate_observation_labels` builds `is_active_next_7d` from real events in the window after the observation time. The window is `(t, t + horizon]`, and the latest observation time is `end_time - horizon`, so no label is built from an incomplete window. A user counts as active when an event of that user falls into the window, whatever its type.
- The streaming event contract `InteractionEventV1` allows `impression`, `play`, `skip` and `complete`. The batch generator knows `impression`, `click`, `play`, `watch`, `like` and `search`. `synthetic_data.py` produces neither `skip` nor `complete`, and `complete` appears in `src` only in the contract. Skip and complete events exist only in hand-written fixtures.

## Decision

| Concern | Rule |
|---|---|
| Files and model | Label specs are YAML files in `feature_repo/labels/`, validated by their own model `LabelSpec`. They are not feature specs: `validate-spec` on `feature_repo/specs` never sees them. |
| Fields | `name`, `version`, `owner`, `description`, `entity`, `source`, `outcome`, `horizon` and `dedupe`. |
| Outcome | `outcome.op` is `exists` (boolean) or `count` (integer). `outcome.filter` is optional and uses the restricted expression subset of ADR-008 over the event fields. Without a filter every event counts. |
| Window | `(t, t + horizon]`, right closed. An event at `t` belongs to the features and never to the label. |
| Observability | The label of observation time `t` is known from `t + horizon` (`label_observable_at`). The caller passes `data_end`, the time up to which the input is complete. A row with `t + horizon > data_end` is excluded and counted in the result. It is never evaluated as false or zero. |
| False and unknown | `exists` is false for a complete window without an outcome event. An incomplete window has no value. |
| Identity | As for features (ADR-008): identities are checked over the whole input before any filtering, a conflict raises `EventIdentityConflictError`, and `ingested_at` is not part of the identity. |
| Source rule | A label may use only event types that its source really produces. A test generates the simulated source and asserts that every event type of the contract occurs in it, so contract and source cannot drift apart. |
| Training join | Features use events up to and including `t`, the label uses events strictly after `t`. The training dataset API joins both on entity and observation time and is decided in its own change. |

Two examples, both over the streaming contract:

```yaml
name: completed_within_7d
version: 1
owner: personalization-team
description: Whether the user completed a play in the 7 days after the observation time
entity: user_id
source: interaction-events.validated.v1
outcome: {op: exists, filter: "event_type == 'complete'"}
horizon: 7d
dedupe: {key: event_id}
```

```yaml
name: active_within_7d
version: 1
owner: personalization-team
description: Whether the user had any event in the 7 days after the observation time
entity: user_id
source: interaction-events.validated.v1
outcome: {op: exists}
horizon: 7d
dedupe: {key: event_id}
```

## Consequences

- Positive: labels follow the same pattern as features: one declarative file, one exact reference as oracle and a Spark evaluation that must match it.
- Positive: the leakage rule is explicit. Features end at `t` inclusive, labels start strictly after `t`, and an incomplete window never becomes a negative example.
- Negative: `completed_within_7d` cannot ship before a source produces `complete` events. A streaming event simulator with a documented behaviour model is built first.
- Negative: `is_active_next_7d` of the batch generator stays a separate definition on the batch contract until the batch path moves to specs.
- The online store protocol of WP3, planned as ADR-011 in the readiness plan, becomes ADR-013.

## Alternatives considered

- A label as a feature spec with a future window: rejected. Feature windows end at the observation time, and reusing them for the future mixes two semantics and invites leakage.
- Labels computed in training code: rejected. There is no shared definition, and no test can check it against an oracle.
- A left-closed window `[t, t + horizon]`: rejected. An event at `t` is already part of the features at `t`.
- An incomplete window evaluated as negative: rejected. It biases the label towards the end of the data.

## Verification

Planned, and written before the implementation:

- A hand-calculated fixture with events at `t`, just after `t`, at `t + horizon` and just after it. It pins the window and shows that an event at `t` is excluded.
- The exact reference and the Spark evaluation return the same values on that fixture and on randomized inputs at the boundaries.
- An incomplete window is excluded and counted, and an identity conflict fails as it does for features.
- A label that names an unknown field or an event type outside the contract is rejected by validation.
- The simulated source produces every event type of the contract.

## Open items

- The training dataset API and its two modes.
- Outcomes over sequences of events, for example the next play after an impression.
- Tuning the behaviour model of the simulator. The first version is implemented: a play ends as `skip` below a fraction of the content duration, as `complete` at the full duration, and otherwise without an end event, which is unknown and not negative.
- Labels on the batch event contract.
