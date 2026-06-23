# Resource Inventory Contract

This document defines the behavioral contract for the MVP resource inventory
used by planning. The inventory is an orchestrator-owned input to `RunPlan`
formation, not a distributed negotiation protocol.

## Inventory Formation

- The orchestrator owns the intended node pool.
- The orchestrator owns the resource inventory used for placement.
- Each inventory entry is tied to a known `node_id`.
- Inventory facts are available before planning starts.
- A candidate node may report boot health and readiness, but does not negotiate
  graph placement after boot.

## Planning Input

- The planner receives a candidate pool and placement input from the
  orchestrator.
- The planner rejects placement that names nodes outside the candidate pool.
- The planner rejects missing or duplicate stage assignments.
- The planner does not mutate the candidate pool.
- The planner does not derive hidden nodes outside the inventory.

## Authority

- Resource inventory determines what the planner is allowed to place onto.
- `RunPlan` determines what actually gets placed.
- Nodes do not advertise new placement facts during run planning.
- Stages do not reinterpret inventory after provisioning.

## Test Direction

Tests should treat inventory as public planner input. Successful tests should
assert that every planned stage node comes from the inventory. Rejection tests
should name unknown nodes, duplicate stage assignments, or incomplete placement
facts and assert typed planner rejection.
