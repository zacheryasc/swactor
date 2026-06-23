# StageController Contract

This document defines the behavioral contract for a provisioned stage's
`StageController`. The controller is control-path only: it observes setup,
worker, edge, and object events, and issues worker commands.

## Provisioning

- A stage starts unprovisioned.
- It accepts `ProvisionStage` only from the authorized orchestrator.
- It validates `run_id`, `stage_index`, stage count, layer range, and edge
  provisions before setup.
- Invalid provisioning emits `StageFault`.
- A stage does not rewire inbound or outbound edges.

## Preparation

- The controller configures the local worker role path.
- It starts assigned weight download, load, or bind work.
- It establishes the inbound receive edge.
- It establishes the outbound send edge.
- It reports `StageReady` only after worker readiness, weight readiness, inbound
  edge readiness, and outbound edge readiness.

## Execution Admission

- A ready stage executes only after inbound `ObjectLoaded`.
- The inbound object sequence must equal the next expected sequence.
- The stage issues exactly one `ExecuteStep` for each accepted inbound object.
- The output binding uses the same sequence as the input object.
- The MVP allows one active `ExecuteStep` per stage.

## Sequence Safety

- Sequence `0` is accepted as prefill.
- Decode sequences are strictly increasing after sequence `0`.
- Duplicate sequence faults the stage.
- Skipped sequence faults the stage.
- Out-of-order sequence faults the stage.

## Completion

- `StepCompleted` returns the stage to ready-for-next-object state.
- The controller releases per-step input handles according to policy.
- The stage does not report compute completion before the worker reports
  `StepCompleted`.

## Fault Behavior

- Worker crash faults the stage.
- `StepFailed` faults the stage.
- `ObjectFailed` faults the stage.
- Output edge fault faults the stage.
- Sequence violation faults the stage.
- After fault, the stage rejects new run work until stopped.

## Stopping

- On `StopRun`, the controller stops local edges.
- It releases per-run device objects.
- It stops or resets worker role state according to local policy.
- It emits `StageStopped` once teardown reaches the local terminal state.

## Test Direction

Tests should drive the controller with public setup and worker/edge events.
Successful tests should assert `StageReady` ordering, one `ExecuteStep` per
accepted object, and same-sequence output binding. Fault tests should inject
invalid sequence, setup failure, worker crash, and step failure and assert
`StageFault` followed by rejection of new work.
