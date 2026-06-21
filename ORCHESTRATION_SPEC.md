# GGUF Pipeline Orchestration - MVP Specification

**Status:** draft buildout specification.

**Relationship to other documents.** `DESIGN_DIRECTIVES.md` remains steering
context. `RING_BACKPRESSURE_SPEC.md` defines edge establishment, object records,
rings, pumps, and teardown. `GPU_WORKER_INTERFACE_SPEC.md` defines worker
startup, shard/weight binding, and `ExecuteStep`. This document defines the
missing layer above them: how the orchestrator plans and stages one linear GGUF
pipeline inference run.

This document is intentionally not a general graph specification.

---

## 1. Scope

The MVP workload is pipeline-parallel inference from a GGUF model:

```text
orchestrator --tokens--> stage 0 --activations--> stage 1 --activations-->
... --activations--> stage N-1 --tokens--> orchestrator
```

The orchestrator is a control participant and token endpoint. It does not run GPU
compute. GPU compute happens only inside provisioned stages.

This spec covers:

- building a linear stage plan from a GGUF model and a provisioned GPU pool
- assigning GGUF shard/layer ranges to stages
- assigning run-scoped edge ids
- provisioning each stage with the facts it needs
- defining the readiness barrier before prompt injection
- defining the orchestrator and stage FSMs
- defining security and correctness guarantees for the MVP behavior

This spec does not cover:

- arbitrary graph execution
- automatic placement optimization
- batching, speculative decoding, or continuous serving
- failure recovery by re-placement
- detailed ring, pump, or worker internals already specified elsewhere
- behavioral test contracts; those come after the full system shape is drafted

---

## 2. Core Responsibilities

The orchestrator owns:

- run ids
- stage count and stage order
- GGUF shard/layer assignment
- edge id assignment
- stage provisioning
- the global readiness barrier
- prompt injection
- final token consumption
- EOS and `max_tokens` stop policy
- run-level fault and teardown

Each stage owns:

- loading its assigned GGUF shard/layer range
- configuring its local GPU worker
- establishing its local edge ends
- converting loaded inbound objects into local `ExecuteStep` calls
- producing the next object on its outbound edge
- reporting readiness and faults to the orchestrator

The ring and worker specs own the byte movement and GPU worker command details.
This spec only decides which stages and edges exist and what sequence of control
events makes the run progress.

---

## 3. Run Plan

The orchestrator builds one `RunPlan` before provisioning:

```rust
struct RunPlan {
    run_id: RunId,
    model: GgufModelPlan,
    stages: Vec<StagePlan>,
    edges: Vec<EdgePlan>,
    max_tokens: u32,
}

struct GgufModelPlan {
    model_id: String,
    gguf_source: GgufSource,
    num_layers: u32,
    hidden_dim: u32,
    dtype_family: DTypeFamily,
    dtype_width_bytes: u32,
    max_seq_len: u32,
    eos_token_id: u32,
}
```

`gguf_source` may identify a whole GGUF file, a pre-split shard collection, or a
cache key. The orchestration contract is the assigned layer range. Whether the
node reads only part of a whole GGUF file or receives a physically pre-split
artifact is a local loading detail.

Each stage receives a contiguous layer range:

```rust
struct StagePlan {
    run_id: RunId,
    stage_index: u32,
    stage_count: u32,
    node_id: NodeId,
    gguf_source: GgufSource,
    layer_start: u32,
    layer_end_exclusive: u32,
    inbound_edge: EdgeId,
    outbound_edge: EdgeId,
}
```

For stage `0`, `inbound_edge` is the token edge from the orchestrator. For the
last stage, `outbound_edge` is the token edge back to the orchestrator. Interior
edges carry activations.

Each edge has exactly one producer and one consumer:

```rust
struct EdgePlan {
    run_id: RunId,
    edge_id: EdgeId,
    kind: EdgeKind,
    producer: EdgeEndpoint,
    consumer: EdgeEndpoint,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
}

enum EdgeKind {
    TokenIn,
    Activation,
    TokenOut,
}

enum EdgeEndpoint {
    Orchestrator { node_id: NodeId },
    Stage { node_id: NodeId, stage_index: u32 },
}
```

The orchestrator assigns all `edge_id`s. Stage code never derives edge ids from
names, layer ranges, peer ids, or hashes.

---

## 4. Stage Provisioning Message

The orchestrator sends one provision message to each stage node:

```rust
ProvisionStage {
    run_id: RunId,
    stage_index: u32,
    stage_count: u32,
    gguf_source: GgufSource,
    layer_start: u32,
    layer_end_exclusive: u32,
    inbound: InboundEdgeProvision,
    outbound: OutboundEdgeProvision,
    model: StageModelFacts,
}

struct InboundEdgeProvision {
    edge_id: EdgeId,
    kind: EdgeKind,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
}

struct OutboundEdgeProvision {
    edge_id: EdgeId,
    kind: EdgeKind,
    consumer_node_id: NodeId,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
}

struct StageModelFacts {
    model_id: String,
    hidden_dim: u32,
    dtype_family: DTypeFamily,
    dtype_width_bytes: u32,
    max_seq_len: u32,
}
```

The inbound edge is established locally as a receive edge. The outbound edge is
established locally as a send edge to `consumer_node_id`. For the last stage,
`consumer_node_id` is the orchestrator node.

The node-local stage controller translates this provision message into the lower
level operations:

```text
configure worker for assigned stage
load/bind assigned GGUF shard or layer range
provision receive edge for inbound.edge_id
provision send edge for outbound.edge_id
report StageReady when all required local work is complete
```

---

## 5. Orchestrator FSM

The orchestrator has one run-level FSM:

```text
Planning
  build RunPlan
  validate layer ranges and edge ids
  -> Provisioning

Provisioning
  send ProvisionStage to every stage node
  create local token producer for token-in edge
  create local token consumer for token-out edge
  -> WaitingReady

WaitingReady
  on StageReady for every stage and local token endpoints ready
    -> Running
  on StageFault or timeout
    -> Faulted

Running
  inject prompt token object on token-in edge, sequence 0
  consume token objects from token-out edge in sequence order
  after token sequence k:
    if EOS or max_tokens reached -> Completed
    else inject token object sequence k + 1 on token-in edge
  on StageFault, edge fault, local token endpoint fault, or timeout
    -> Faulted

Completed
  stop injecting tokens
  finalize output text
  -> TearingDown

Faulted
  stop injecting tokens
  record one run-level failure reason
  -> TearingDown

TearingDown
  send StopRun to all provisioned stages
  tear down local token endpoints
  wait for StageStopped from every stage or timeout
  -> Done

Done
  terminal
```

There is no broadcast start message. The start signal is the first token object
written by the orchestrator after the readiness barrier.

---

## 6. Stage FSM

Each provisioned stage has one node-local stage controller. It is control-path
only: it watches local worker and edge events and issues worker commands. It does
not move payload bytes.

```text
Unprovisioned
  on ProvisionStage from authorized orchestrator
    validate run_id and stage assignment
    -> Preparing

Preparing
  configure local worker for assigned GGUF range
  start GGUF shard/range load and bind
  establish inbound receive edge
  establish outbound send edge
  when worker configured, shard bound, and both edge ends ready
    emit StageReady
    -> Ready
  on any required setup failure
    emit StageFault
    -> Faulted

Ready
  on inbound ObjectLoaded(sequence = s)
    if s is the next expected sequence
      issue ExecuteStep for sequence s
      -> Executing
    else
      emit StageFault(sequence violation)
      -> Faulted
  on StopRun
    -> Stopping

Executing
  worker runs exactly one step for the loaded inbound object
  worker writes the output object to outbound edge with the same sequence
  on StepCompleted
    release any per-step input handle that is no longer needed
    -> Ready
  on StepFailed or output fault
    emit StageFault
    -> Faulted

Faulted
  reject new run work
  wait for StopRun
  -> Stopping

Stopping
  stop local edges
  release per-run device objects
  stop or reset worker according to local policy
  emit StageStopped
  -> Stopped

Stopped
  terminal for this run
```

The controller is the component that decides when `ExecuteStep` is called. The
orchestrator does not issue per-stage execute commands during the run. Once the
prompt object is injected, stage execution is driven by object arrival and local
readiness.

---

## 7. Execution Semantics

Sequence `0` is prefill.

```text
orchestrator writes prompt token object sequence 0
stage 0 executes prefill over prompt tokens
stage 0 writes activation sequence 0
each interior stage executes prefill over activation sequence 0
last stage executes prefill and writes token sequence 0
orchestrator consumes token sequence 0
```

Decode sequences are `1..`:

```text
orchestrator writes one-token object sequence k
stage 0 executes decode for sequence k
each downstream stage executes decode for sequence k
last stage writes token sequence k
orchestrator consumes token sequence k
```

The orchestrator sends sequence `k + 1` only after consuming token sequence `k`
and deciding the run should continue.

For every stage:

- the inbound object sequence is the output object sequence
- one active `ExecuteStep` per stage is allowed in the MVP
- a stage cannot execute before its assigned GGUF shard/range is loaded and bound
- a stage cannot execute before its inbound object is loaded
- a stage cannot produce to an edge that is not ready

The last stage samples or otherwise produces token ids as part of its GPU worker
step. The orchestrator consumes those token ids, accumulates output, applies EOS
and `max_tokens`, and writes the next token object only when continuing.

---

## 8. Control Messages And Events

These are schematic message shapes, not final Rust APIs.

Orchestrator to stage:

```rust
ProvisionStage { ... }

StopRun {
    run_id: RunId,
    reason: StopReason,
}
```

Stage to orchestrator:

```rust
StageReady {
    run_id: RunId,
    stage_index: u32,
    node_id: NodeId,
}

StageFault {
    run_id: RunId,
    stage_index: u32,
    node_id: NodeId,
    reason: StageFaultReason,
}

StageStopped {
    run_id: RunId,
    stage_index: u32,
    node_id: NodeId,
}
```

Optional setup progress events may exist for diagnostics, but `StageReady`,
`StageFault`, and `StageStopped` are the only required run-level events in this
draft.

---

## 9. Object Specs

Token edges carry token objects. The prompt token object may contain multiple
token ids for prefill. Decode token objects contain one token id.

Activation edges carry activation objects with runtime extent bounded by model
shape:

```text
max_extent = max_seq_len * hidden_dim * dtype_width_bytes
```

The object record and ring behavior are defined by `RING_BACKPRESSURE_SPEC.md`.
This orchestration spec only requires that all stage plans for a run agree on the
model facts used to build those object specs.

---

## 10. Security Model

Nodes are trusted. The system does not attempt trustless verification,
adversarial tensor validation, Sybil defense, or incentive enforcement.

The orchestrator is the authority for run topology. A stage accepts run
provisioning only from the authorized orchestrator for its node.

Stages reject:

- unknown `run_id`
- stale `run_id`
- duplicate provisioning for an already-active run unless explicitly stopped
- edge ids not present in the provision message
- peer rewiring requests from another stage

`edge_id`s are run-scoped capabilities for wiring and demux. They are not a
cryptographic trust boundary between trusted nodes, but a stage must still reject
objects and stream setup that do not match its active run plan.

The orchestrator may tear down a run at any time. Stages must treat `StopRun` for
their active `run_id` as authoritative.

---

## 11. Correctness Guarantees

Layer assignment:

- stage layer ranges are contiguous
- stage layer ranges do not overlap
- the union of stage layer ranges covers the intended GGUF block range
- every stage has exactly one assigned range

Edge assignment:

- every `edge_id` is unique within a run
- every edge has exactly one producer and one consumer
- token-in is produced by the orchestrator and consumed by stage `0`
- token-out is produced by stage `N - 1` and consumed by the orchestrator
- activation edge `i` is produced by stage `i` and consumed by stage `i + 1`

Readiness:

- the orchestrator does not inject prompt tokens before every stage reports
  `StageReady`
- a stage does not report `StageReady` before its worker, shard/range binding,
  inbound edge, and outbound edge are ready

Execution:

- prefill is sequence `0`
- decode sequences are strictly increasing
- a stage executes sequence `s` only after loading inbound object sequence `s`
- a stage output uses the same sequence as its input
- the orchestrator injects sequence `s + 1` only after consuming token sequence
  `s`

Termination:

- each run has one terminal outcome: completed, faulted, or torn down
- after a run faults, the orchestrator stops injecting new token objects
- teardown is sent to every stage that was provisioned for the run

---

## 12. Deferred

- placement optimization
- physical GGUF shard format
- multiple concurrent runs on one stage chain
- batching and speculative decoding
- direct stage-to-stage token feedback that bypasses the orchestrator
- warm reuse policy across prompts
- re-placement after node failure
- behavioral test matrix and observability schema
