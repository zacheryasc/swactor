# Orchestrator Run FSM Contract

This document defines the behavioral contract for the orchestrator run FSM. It
covers how a valid `RunPlan` becomes provisioning, readiness, prompt injection,
execution, terminal outcome, and teardown.

## Planning And Provisioning

- The orchestrator plans only after `PoolReady`.
- It provisions only from a valid `RunPlan`.
- It sends exactly one `ProvisionStage` to every planned stage.
- It does not provision unknown stages.
- It does not provision nodes outside the committed plan.
- It creates local token-in and token-out endpoints according to the plan.

## Readiness Barrier

- The orchestrator does not inject the prompt before every planned stage reports
  `StageReady`.
- The orchestrator does not inject the prompt before local token endpoints are
  ready.
- Duplicate `StageReady` does not advance readiness twice.
- `StageReady` from an unknown stage rejects or faults.
- `StageReady` for a different run rejects or faults.

## Execution Drive

- Prompt injection is the start signal.
- There is no separate broadcast start.
- The orchestrator injects sequence `0` first.
- It injects sequence `k + 1` only after consuming token sequence `k`.
- It stops injecting after EOS.
- It stops injecting after `max_tokens`.

## Fault Behavior

- `StageFault` before readiness faults the run.
- `StageFault` during execution faults the run.
- Membership loss for a required node faults the run.
- Endpoint fault faults the run.
- Timeout faults the run.
- The first run-level failure reason is retained.
- Later failure reasons do not replace the recorded terminal reason.

## Terminal Outcome

- Each run records exactly one terminal outcome.
- `Completed` and `Faulted` are mutually exclusive.
- Operator stop before completion records the operator-stopped outcome.
- After terminal outcome begins, no new prompt or token work is accepted.
- Teardown is required after success, fault, and operator stop.

## Teardown

- The orchestrator sends `StopRun` to every provisioned stage.
- It tears down local token endpoints.
- It waits for `StageStopped` from every provisioned stage or teardown timeout.
- It emits `run_torn_down` exactly once.
- `run_torn_down` is emitted only after teardown has reached its terminal state.

## Test Direction

Tests should drive the orchestrator with public events and observe emitted
commands, lifecycle events, and terminal outcome. Successful tests should prove
the readiness barrier, sequence injection rule, and teardown after completion.
Fault tests should inject one fault source at a time and assert exactly one
run-level terminal outcome and teardown.
