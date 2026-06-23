# Orchestrator Token Endpoint Contract

This document defines the behavioral contract for orchestrator-owned token
edges. The orchestrator is a data-plane participant for token-in and token-out,
but it does not run model compute.

## Endpoint Formation

- The orchestrator creates the token-in producer from the committed plan.
- The orchestrator creates the token-out consumer from the committed plan.
- Token endpoints use the same edge semantics as stage endpoints.
- The orchestrator endpoint has a stable `node_id`.
- Co-location with a GPU node does not change token edge semantics.

## Prompt Injection

- Prompt injection happens only after the global readiness barrier.
- Prompt injection writes token object sequence `0`.
- Prompt injection is the only run start signal.
- The token-in object conforms to the token `ObjectSpec`.

## Token Consumption

- Token-out consumption observes token objects in sequence order.
- The orchestrator consumes token sequence `k` before deciding whether to inject
  sequence `k + 1`.
- EOS stops further injection.
- `max_tokens` stops further injection.
- Out-of-order token output faults the run.

## Fault Behavior

- Token-in endpoint fault faults the run.
- Token-out endpoint fault faults the run.
- Malformed token object faults the run.
- Token sequence violation faults the run.
- Token endpoint teardown failure contributes to run teardown failure.

## Test Direction

Tests should drive the endpoint through prompt injection and token-return
traces. Successful tests should assert sequence `0` first and `k + 1` only
after token `k`. Fault tests should inject endpoint fault, malformed token, and
out-of-order token output and assert run fault.
