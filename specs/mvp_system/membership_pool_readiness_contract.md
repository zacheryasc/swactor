# Membership And Pool Readiness Contract

This document defines the behavioral contract for the orchestrator's membership
gate. SWIM is an input to pool readiness; it is not a distributed graph
agreement protocol.

## Pool Readiness Formation

- The orchestrator emits `PoolReady` only for the intended candidate pool.
- `PoolReady` requires every candidate node to be known to the orchestrator.
- `PoolReady` requires every candidate node to be live in the SWIM view.
- `PoolReady` requires every candidate node to have emitted `NodeAvailable`.
- `PoolReady` requires every candidate node to have data-plane identity material.
- `PoolReady` requires no candidate node to be suspect or faulted.
- `PoolReady` requires the pool view to remain stable for the configured
  convergence window.

## Planning Gate

- Run planning starts only after `PoolReady`.
- If pool readiness is lost before a `RunPlan` is committed, the orchestrator
  waits or aborts according to local policy.
- Nodes do not need to agree on graph state before planning.
- Nodes do not compute placement from SWIM state.

## Loss After Provisioning

- Membership loss for a required node after provisioning begins faults the run.
- The MVP does not re-place an active run after membership loss.
- A suspect or faulted required node is treated as unavailable for the active
  run.

## Authority

- SWIM reports membership and liveness facts.
- The orchestrator decides pool readiness.
- The orchestrator owns the candidate pool definition.
- SWIM does not assign stages, edges, layers, or object specs.

## Test Direction

Tests should feed deterministic membership and node-availability observations
into the orchestrator readiness gate. Successful tests should assert that
`PoolReady` appears only after all required facts and the convergence window.
Failure tests should remove or suspect one candidate node and assert no
`PoolReady`, or a run fault if provisioning has already begun.
