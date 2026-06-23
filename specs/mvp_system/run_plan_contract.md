# RunPlan Contract

This document defines the behavioral contract for MVP `RunPlan` formation and
projection. It is concerned with externally observable planner behavior: the
planner inputs, the emitted `RunPlan`, derived provisioning messages, and typed
rejections.

The planner is the first contract target because the orchestrator is the run
authority. Every downstream component receives its stage, layer, edge, and
object obligations from the committed plan.

## RunPlan Formation

- Given model facts, runtime config, intended node pool, stage count, and
  placement input, the planner either emits exactly one `RunPlan` or a typed
  rejection.
- It never emits a partial plan.
- It never mutates the candidate pool or derives hidden topology outside the
  plan.

## Layer Assignment

- Stage ranges are contiguous.
- Stage ranges do not overlap.
- The union covers the intended GGUF block range.
- Every stage has exactly one non-empty assigned range unless the spec
  explicitly allows empty ranges.
- Stage indices are `0..stage_count-1`.

## Edge Assignment

- Every edge id is unique within the run.
- Every edge has exactly one producer and one consumer.
- Token-in edge is `orchestrator -> stage 0`.
- Activation edge `i` is `stage i -> stage i + 1`.
- Token-out edge is `stage N-1 -> orchestrator`.
- Stages do not derive edge ids from names, hashes, layer ranges, or peers.

## Provisioning Projection

- For each `StagePlan`, deriving `ProvisionStage` is deterministic.
- A stage receives only its own layer range.
- A stage receives exactly one inbound and one outbound edge provision.
- Outbound provision contains the consumer `node_id`.
- Inbound provision does not require producer actor address.
- No provision message contains remote actor addresses for data flow.

## Object/Ring Spec Consistency

- Every edge has an `ObjectSpec` and `RingSpec`.
- Activation `max_extent` follows the model facts:
  `max_seq_len * hidden_dim * dtype_width_bytes`.
- Token edges use token object specs, activation edges use activation object
  specs.
- Specs are copied consistently into stage provisions.
- Invalid extent, alignment, dtype width, or unsupported shape/layout rejects
  the plan.

## Authority / Rejection

- Unknown node id rejects.
- Duplicate stage assignment rejects.
- Missing stage rejects.
- Invalid stage count rejects.
- Edge endpoint mismatch rejects.
- Model facts inconsistent with requested stage layout reject.
- The planner is the only source of stage, layer, edge, and object-spec
  assignment.

## Test Direction

The tests for this contract should assert guarantees over planner inputs,
planner outputs, derived provisioning messages, and typed rejections. They
should not assert planner internals, placement heuristics, helper APIs,
allocation strategy, or private data structures.

Successful-plan tests should inspect the returned `RunPlan` and derived
`ProvisionStage` values. Rejection tests should feed contradictory inputs and
assert that no plan is emitted.
