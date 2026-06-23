# Node Boot Lifecycle Contract

This document defines the behavioral contract for node boot. It covers the
observable transition from container start to `NodeAvailable`, and the failure
cases that keep a node out of the candidate run pool.

## Boot Formation

- A node boot attempt starts when the node process is launched in the intended
  pool.
- A boot attempt emits either `NodeAvailable` or a typed node boot fault.
- It never emits `NodeAvailable` before required local resources are ready.
- It never allows run provisioning to race ahead of node availability.

## Required Readiness Facts

- The Rust node process is alive.
- The swactor runtime can receive control messages.
- The node has a stable `node_id` known to the orchestrator.
- The arena is created and mapped in the node process.
- The GPU worker is ready, or the implementation has an explicit deferred
  worker-start policy with the same run-level readiness guarantee.
- The iroh endpoint is initialized and bound to the node identity.
- The SWIM participant has joined or is joining the intended pool.
- The node can accept run provisioning.

## Non-Readiness Facts

- `NodeAvailable` does not mean weights are present.
- `NodeAvailable` does not mean a role is configured.
- `NodeAvailable` does not mean run edges are established.
- `NodeAvailable` does not mean the node has been selected by a `RunPlan`.

## Failure Behavior

- Arena construction failure rejects node availability.
- Worker startup failure rejects node availability unless worker startup is
  explicitly deferred.
- Transport endpoint failure rejects node availability.
- Missing or invalid node identity rejects node availability.
- A node that is boot-faulted is not eligible for run planning.

## Authority

- The node owns local boot resources.
- The orchestrator owns whether a booted node is part of the intended candidate
  pool.
- A node does not self-assign stages, edges, or layer ranges during boot.

## Test Direction

Tests should drive the boot component through public resource outcomes and
observe emitted lifecycle events. Successful tests should assert that
`NodeAvailable` appears only after the required readiness facts. Failure tests
should inject one failed resource at a time and assert a typed boot fault with
no `NodeAvailable`.
