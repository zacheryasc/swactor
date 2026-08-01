# Myelin System Specification
 ***STALE! FOR HISTORICAL REFERENCE ONLY*** 

**Status:** draft consolidated system specification.

This document is the single Myelin reference for the swactor GGUF pipeline system.
It folds the system behavior previously split across the orchestration,
ring/backpressure, and GPU-worker drafts into one end-to-end specification.

The focused root specs may remain as design history while this document is being
stabilized. For the Myelin drafting process, this document is the place where the
complete system, its components, guarantees, and systemic behavior are described
together.

---

## 1. Purpose

The Myelin is a trusted distributed ML runtime for one linear GGUF inference
pipeline over a rented pool of GPU nodes.

The first deployment shape is:

```text
operator provisions rented GPU nodes
  -> each node runs a barebones Docker image
  -> the image contains swactor runtime + tinygrad/CUDA worker
  -> nodes join membership and reach SWIM convergence
  -> orchestrator assigns pipeline roles and edges
  -> nodes download/load assigned GGUF weights
  -> nodes provision arena-backed data edges
  -> nodes report readiness
  -> orchestrator injects a "hello world" prompt
  -> pipeline returns tokens
  -> run completes or faults and tears down
```

The document is intentionally about the first complete feature, not a broad
graph engine. Later features can generalize from this system after the Myelin
behavior is stable.

---

## 2. Scope

In scope:

- trusted node pool boot and readiness
- SWIM membership convergence as the planning gate
- orchestrator authority, planning, staging, faults, and teardown
- one linear GGUF pipeline
- stage/layer assignment and edge id assignment
- stage-local weight download, load, and bind readiness
- arena-backed rings and iroh/QUIC edge transport
- Rust node process to Python/tinygrad worker process control
- GPU worker object loading, step execution, and object production
- prompt injection, prefill, decode, token return
- completed, faulted, and operator-stopped terminal outcomes
- observability events needed for behavioral contracts and tests

Out of scope:

- arbitrary graph IR
- automatic placement optimization
- batching, speculative decoding, continuous serving
- re-placement after churn or node failure
- trustless verification, incentives, Sybil defense, adversarial tensor checks
- production artifact layout and cache eviction policy
- VastAI account automation, bidding, billing, or provider-specific control
  machinery
- high-performance host pinning policy beyond the safety contract

---

## 3. Design Commitments

Nodes are trusted. Payload content is trusted after structural checks. The
system does not defend against malicious tensor values or adversarial peers.

The orchestrator is the run authority. It owns placement, run planning,
provisioning, prompt injection, terminal outcome, and teardown.

swactor owns the control plane. Actors establish, observe, and tear down
components. Actors do not move tensor payload bytes.

Payload bytes move through shared-memory rings and iroh/QUIC streams. The hot
path is ring cursors, wake hints, and byte-pump tasks, not actor mailboxes.

The GPU worker is a supervised process, not a separate swactor runtime. It maps
the shared arena, uses a native ring helper, owns device allocations, and runs
tinygrad role code when explicitly commanded.

Compute is explicit. `ObjectLoaded` means data is ready on device.
`ExecuteStep` is the local compute admission command. Loading an object does not
automatically run tinygrad.

Backpressure is absence of writable ring space plus QUIC flow control. There is
no actor-level credit protocol, RTS/CTS exchange, or per-range acknowledgement.

One persistent iroh uni-stream carries all objects for one edge. The stream
starts with an `edge_id` preamble and then object records.

The Myelin permits transfer and device upload/download to overlap with network
streaming. It does not permit compute to observe a logical object until the
whole object has loaded.

---

## 4. Trust And Authority Model

The orchestrator owns:

- intended node pool
- resource inventory used by placement
- run ids
- model and runtime configuration
- tokenizer/vocabulary facts
- stage count and stage order
- stage-to-node assignment
- layer range assignment
- edge id assignment
- object specs and ring specs
- weight source assignment
- global readiness barrier
- prompt tokenization and prompt injection
- output token consumption
- EOS and `max_tokens` stop policy
- run-level fault and teardown decisions

Each node owns:

- stable node identity for the run
- node process lifecycle
- GPU worker process lifecycle
- local arena and arena leases
- local iroh endpoint and byte pumps
- SWIM participant
- local stage controller
- local edge establishment records
- assigned weight download/load work
- local worker/device resources

Nodes do not rewire the graph. A node accepts run provisioning only from the
authorized orchestrator and rejects edge data or provisioning that does not match
its active run plan.

Other stages cannot redirect a node to a new peer, edge id, or layer range.

Resource inventory is known to the orchestrator before placement in the Myelin. A
node may report boot health and readiness, but there is no distributed
capability negotiation protocol after boot.

---

## 5. System Topology

Each GPU node runs one Rust node process and one Python/tinygrad GPU worker
process:

```text
GPU node container
------------------
Rust node process
  swactor runtime
  SWIM participant
  iroh endpoint and driver
  ArenaManager
  EdgeEstablisher
  Tx/Rx edge actors
  GpuWorkerCtl
  StageController

Python/tinygrad worker process
  mapped shared arena
  native ring helper
  device bridge
  role code
  device allocations and KV/state
```

The orchestrator may run in its own container or on one of the provisioned GPU
machines. It is a control participant and token endpoint. It does not run model
compute for the Myelin.

When the orchestrator participates in token edges over the data plane, it has a
stable `node_id`, an iroh endpoint, and local token edge endpoints like any
other producer or consumer node. Co-locating the orchestrator with a GPU node is
an implementation choice, not a change to edge semantics.

The system has two network-facing planes:

- membership/control observation: SWIM provides node liveness/membership state
  used by the orchestrator as the pool readiness gate
- data movement: iroh/QUIC carries persistent uni-streams for data edges,
  addressed by `(node_id, edge_id)`

Provisioning, readiness, faults, and teardown are swactor messages. Tensor bytes
do not travel in actor messages.

---

## 6. Identifiers

Identifier types are schematic. Concrete Rust APIs may wrap these as newtypes.

```rust
struct RunId(u64);
struct NodeId(u64);
struct RoleId(u64);
struct StageIndex(u32);
struct EdgeId(u64);
struct RingId(u64);
struct ObjectId(u64);
struct Sequence(u64);
struct StepId(u64);
struct WorkerGeneration(u64);
```

`EdgeId` is unique within a run and assigned only by the orchestrator.

`Sequence` names the inference-step index carried on object records. It is not a
token id, token-array position, byte offset, stream packet number, or transport
ordering primitive.

`RingId` is unique for the node lifetime. Arena ranges may be reused after
quiescence, but ring ids are not reused.

`DeviceObjectHandle` is opaque to the Rust node process:

```rust
struct DeviceObjectHandle {
    worker_generation: u64,
    id: u64,
}
```

A device handle is valid only in the worker generation that created it. Worker
restart invalidates all prior handles, roles, rings, and in-flight steps.

---

## 7. System Lifecycle

The complete Myelin lifecycle is:

```text
Deployment
  -> NodeBoot
  -> MembershipConvergence
  -> RunPlanning
  -> StageProvisioning
  -> WeightLoading
  -> EdgeProvisioning
  -> GlobalReadiness
  -> PromptInjection
  -> PipelineExecution
  -> CompletionOrFault
  -> Teardown
  -> Done
```

Phase ownership:

- node boot is local to each node
- membership convergence is observed by the orchestrator
- run planning is owned by the orchestrator
- local provisioning is owned by each node
- global readiness is owned by the orchestrator
- execution progress is driven by object arrival at stages
- terminal outcome and teardown are owned by the orchestrator

There is no broadcast start message. The first prompt object written by the
orchestrator after the readiness barrier starts execution.

---

## 8. Node Boot

At container start, each node runs:

```text
ContainerStarting
  -> NodeProcessStarting
  -> ArenaReady
  -> WorkerReady
  -> TransportReady
  -> MembershipJoining
  -> NodeAvailable
```

`NodeAvailable` means:

- Rust node process is alive
- swactor runtime can receive control messages
- node has a stable `node_id` known to the orchestrator
- arena is created and mapped in the node process
- GPU worker has emitted `WorkerReady`
- iroh endpoint is initialized and associated with the node identity
- SWIM participant has joined or is joining the intended pool
- node can accept run provisioning

`NodeAvailable` does not mean weights are present, a role is configured, or any
run edge is established.

If implementation defers worker startup until run provisioning, the same
run-level gate must still hold: a stage cannot report `StageReady` before its
worker is ready and assigned weights are usable.

Boot failure before `ArenaReady` or `WorkerReady` means the node is unavailable.
No run edge should be provisioned to that node.

---

## 9. Membership And Pool Readiness

The orchestrator starts run planning only after the intended candidate pool is
ready.

For the Myelin, `PoolReady` means:

- every node in the intended candidate pool is known to the orchestrator
- every candidate node is live in the SWIM membership view
- every candidate node has reported `NodeAvailable`
- every candidate node has data-plane identity/endpoint material the
  orchestrator can provision into edges
- no candidate node is currently suspect or faulted in the orchestrator view
- the pool view has remained stable for the configured convergence window

The convergence window is an implementation parameter.

Nodes do not compute placement and do not need to agree on graph state. SWIM is a
membership/liveness input to the orchestrator, not a distributed graph protocol.

If pool readiness is lost before a `RunPlan` is committed, the orchestrator keeps
waiting or aborts before provisioning according to local policy.

If pool readiness is lost after provisioning begins, the run faults. The Myelin does
not re-place an active run after a node disappears.

---

## 10. GGUF Pipeline Workload

The Myelin workload is one linear pipeline:

```text
orchestrator --tokens--> stage 0 --activations--> stage 1 --activations-->
... --activations--> stage N-1 --tokens--> orchestrator
```

The orchestrator is a token endpoint and control participant. It does not run GPU
compute.

Each stage owns a contiguous GGUF layer range. A stage receives typed input
objects, executes its assigned layer range, and writes typed output objects.

Stage `0` consumes token objects from the orchestrator and produces activation
objects. Interior stages consume and produce activation objects. The last stage
consumes activation objects and produces token objects for the orchestrator.

The last stage samples or otherwise produces token ids as part of its worker
step if sampling is delegated to the stage. If sampling is not delegated, the
worker output object must contain enough logits/token data for the orchestrator
to apply the configured policy. The `RunPlan` must state which policy is used.

---

## 11. Run Plan

The orchestrator builds exactly one `RunPlan` before provisioning:

```rust
struct RunPlan {
    run_id: RunId,
    model: GgufModelPlan,
    runtime: RuntimePlan,
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
    tokenizer: TokenizerSource,
}

struct RuntimePlan {
    prompt: PromptSource,
    sampling: SamplingPolicy,
    token_output_policy: TokenOutputPolicy,
}
```

`gguf_source` may identify a whole GGUF file, a pre-split shard collection, a
cache key, a local path, or an HTTP/object-store artifact. The system contract is
the assigned layer range and readiness after that range is usable by the worker.

Each stage receives one contiguous layer range:

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

Layer assignment guarantees:

- stage layer ranges are contiguous
- stage layer ranges do not overlap
- the union of stage layer ranges covers the intended GGUF block range
- every stage has exactly one assigned range

Edge assignment guarantees:

- every edge id is unique within the run
- token-in is produced by the orchestrator and consumed by stage `0`
- token-out is produced by stage `N - 1` and consumed by the orchestrator
- activation edge `i` is produced by stage `i` and consumed by stage `i + 1`
- stages never derive edge ids from names, layer ranges, peer ids, or hashes

---

## 12. Provisioning Messages

The orchestrator sends one stage provision message to each stage node:

```rust
struct ProvisionStage {
    run_id: RunId,
    stage_index: u32,
    stage_count: u32,
    gguf_source: GgufSource,
    layer_start: u32,
    layer_end_exclusive: u32,
    inbound: InboundEdgeProvision,
    outbound: OutboundEdgeProvision,
    model: StageModelFacts,
    runtime: StageRuntimeFacts,
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

struct StageRuntimeFacts {
    role_id: RoleId,
    input_port: PortId,
    output_port: PortId,
    sampling: Option<SamplingPolicy>,
}
```

The inbound edge is established locally as a receive edge. The outbound edge is
established locally as a send edge to `consumer_node_id`.

For the last stage, `consumer_node_id` is the orchestrator node.

Provisioning fan-out is parallel. There is no Tx-to-Rx actor handshake and no
peer-to-peer endpoint exchange.

---

## 13. Orchestrator FSM

The orchestrator subsystem owns planning, validation, and control. The
run-level FSM described here receives an already-valid `RunPlan`; plan
validation failures are planner errors and are not FSM states.

```text
Planning outside this FSM
  build RunPlan
  validate layer ranges, edge ids, object specs, node ids
  emit PlanAvailable(valid RunPlan)

Provisioning
  after PoolReady and PlanAvailable(valid RunPlan)
  send ProvisionStage to every stage node
  create local token-in producer
  create local token-out consumer
  -> WaitingReady

WaitingReady
  on StageReady for every stage and local token endpoints ready
    -> Running
  on StageFault, endpoint fault, membership loss, or timeout
    -> Faulted

Running
  inject input object for inference step 0
  consume output objects from token-out edge in inference-step order
  after output object for inference step k:
    if run policy stops -> Completed
    else inject next input object for inference step k + 1
  on StageFault, edge fault, endpoint fault, membership loss, or timeout
    -> Faulted

Completed
  stop injecting objects
  finalize the run output defined by the run contract
  -> TearingDown

Faulted
  stop injecting objects
  record one run-level failure reason
  -> TearingDown

OperatorStopped
  stop injecting objects
  record operator stop
  -> TearingDown

TearingDown
  send StopRun to all provisioned stages
  tear down local token endpoints
  wait for StageStopped from every stage or teardown timeout
  -> Done

Done
  terminal
```

The orchestrator does not issue per-stage `ExecuteStep` during a run. After
prompt injection, stage execution is driven by object arrival and local stage
readiness.

---

## 14. StageController FSM

Each provisioned stage has one node-local StageController. It is control-path
only: it watches worker and edge events, issues worker commands, and reports
run-level events. It does not move payload bytes.

```text
Unprovisioned
  on ProvisionStage from authorized orchestrator
    validate run_id and stage assignment
    -> Preparing

Preparing
  configure local worker role path
  start GGUF shard/range load and bind
  establish inbound receive edge
  establish outbound send edge
  when worker configured, weights bound, and both edge ends ready
    emit StageReady
    -> Ready
  on required setup failure
    emit StageFault
    -> Faulted

Ready
  on inbound ObjectLoaded(sequence = s)
    if s is next expected sequence and local state is ready
      issue ExecuteStep for sequence s
      -> Executing
    else
      emit StageFault(sequence violation)
      -> Faulted
  on StopRun
    -> Stopping

Executing
  worker runs exactly one step for the loaded input object
  worker writes output object to outbound edge with same sequence
  on StepCompleted
    release per-step input handles according to policy
    -> Ready
  on StepFailed, ObjectFailed, output fault, or worker crash
    emit StageFault
    -> Faulted

Faulted
  reject new run work
  wait for StopRun
  -> Stopping

Stopping
  stop local edges
  release per-run device objects
  stop or reset worker role state according to local policy
  emit StageStopped
  -> Stopped

Stopped
  terminal for this run
```

The Myelin allows one active `ExecuteStep` per stage.

---

## 15. Weight Lifecycle

Weights are stage-local persistent state for the run.

The orchestrator sends `ProvisionStage` to every runtime-ready pipeline stage
without waiting for another stage's weights. Each provision carries enough
weight-source planning information for that stage's assigned layer range: the
source identity, selected artifact ranges or shard identifiers, required
metadata, and cache key. The node materializes or locates the stage-local weight
artifact from the assigned source; stages do not stream weights to one another.

The StageController starts weight work from the assigned weight source and layer
range. A stage may acquire:

- only the artifact ranges required for its stage-local shard
- one or more physical shards containing its range
- a whole artifact and load only its range when no shard plan is available
- a cached artifact that already exists on the node

The system-visible contract is `WeightsReady` before `StageReady`.

`WeightsReady` means:

- assigned artifact bytes are locally available or already cached
- assigned layer range has been validated against the run plan
- GPU worker has loaded or bound the range needed for execution
- failures in download, parse, device allocation, or binding have surfaced as
  stage faults

Warm model reuse across runs is deferred.

---

## 16. Execution Semantics

Inference step `0` is the initial/prefill step:

```text
orchestrator writes input object for inference step 0
stage 0 executes over the input object for inference step 0
stage 0 writes output object for inference step 0
each interior stage executes over its input object for inference step 0
last stage writes output object for inference step 0
orchestrator consumes output object for inference step 0
```

Continuation steps are `1..`:

```text
orchestrator writes input object for inference step k
stage 0 executes for inference step k
each downstream stage executes for inference step k
last stage writes output object for inference step k
orchestrator consumes output object for inference step k
```

Payload bytes are opaque to this protocol. A run plan's `ObjectSpec`, runtime
policy, and role contract define what an input or output object means and how
the orchestrator decides whether to continue.

For every stage:

- inbound object sequence equals outbound object sequence for the same
  inference step
- a stage cannot execute before weights are loaded and bound
- a stage cannot execute before inbound object is loaded
- a stage cannot produce to an edge that is not ready
- `ObjectLoaded` is data readiness, not compute completion
- `ObjectProduced` is output object committed to egress ring
- `StepCompleted` is the compute transaction terminal success event

The orchestrator writes inference step `k + 1` only after consuming and accepting
the output object for inference step `k` and deciding the run should continue.

---

## 17. Object Specs And Object Records

Objects are logical byte payloads on an edge. Their payload meaning is defined
by the run plan's `ObjectSpec` and role contract; transport, edge, and lifecycle
components treat payload bytes as opaque.
Object specs are role-known validation contracts:

```rust
struct ObjectSpec {
    kind: ObjectKind,
    max_extent: u64,
    dtype_family: DTypeFamily,
    dtype_width_bytes: u32,
    shape: ShapeRule,
    layout: LayoutRule,
    alignment: u32,
    sequence_policy: SequencePolicy,
}
```

The stream carries object records:

```text
ObjectHeader
payload bytes, exactly header.extent bytes
```

The Myelin header is fixed-size:

```rust
struct ObjectHeader {
    magic: u32,
    version: u16,
    header_len: u16,
    object_id: u64,
    sequence: u64,
    extent: u64,
    flags: u32,
    reserved: u32,
}
```

The header supplies runtime facts: object id, inference-step sequence, extent,
and flags. `ObjectSpec` supplies dtype, shape/layout family, max extent,
alignment, and sequence policy.

Activation maximum extent:

```text
max_extent = max_seq_len * hidden_dim * dtype_width_bytes
```

`extent` may vary by inference step and object contract. It must satisfy
`extent <= ObjectSpec.max_extent`.

The worker rejects a record before exposing a device object if:

- magic or version is unsupported
- header length is malformed
- `extent > ObjectSpec.max_extent`
- extent violates alignment/layout rules
- sequence violates the edge inference-step ordering policy
- ring or stream closes before `extent` bytes arrive

Payload content itself is trusted. The worker does not inspect tensor values.

---

## 18. Ring Specs And Layouts

`RingSpec` is the requested operating shape for a ring:

```rust
struct RingSpec {
    data_capacity: u64,
    alignment: u32,
    direction: RingDirection,
    host_pinning: HostPinning,
    wake_coalescing: WakeCoalescing,
}

enum RingDirection {
    Ingress,
    Egress,
}

enum HostPinning {
    Pageable,
    PinnedRequired,
}

enum WakeCoalescing {
    PendingBit,
    ReadySet,
}
```

`RingLayout` is the arena-relative layout minted by ArenaManager after a lease:

```rust
struct RingLayout {
    ring_id: RingId,
    arena_offset: u64,
    total_len: u64,
    header_offset: u64,
    data_offset: u64,
    data_capacity: u64,
    alignment: u32,
}
```

`RingLayout` never contains process-local pointers. Each process derives local
pointers from its own arena mapping base plus arena offsets.

The ring header and data region must be aligned for cross-process atomics and
backend copy requirements. Host pinning is optional for the Myelin, but if a range
is pinned, it cannot be unpinned or returned to the free list until ring
quiescence.

---

## 19. Arena Model

Each node owns one sparse host-memory arena.

The arena is a `memfd`, created by the Rust node process, truncated to a large
sparse ceiling, and mapped once in the Rust process. The GPU worker maps the same
fd once. Neither process remaps or resizes the arena during node lifetime.

The arena is address space. It does not define flow control and does not define
object ownership.

The ArenaManager owns:

- arena fd
- node process mapping base pointer
- reservation ceiling
- arena free list
- pending lease queue
- table of live ring leases

The ArenaManager does not own:

- worker process
- QUIC stream
- pump task
- object parser
- payload byte

The ArenaManager only mints stable offsets and releases ranges after proof of
quiescence.

ArenaManager FSM:

```text
Booting
  on ConstructArena{ceiling}
    -> Ready if memfd, truncate, mmap succeed
    -> Failed if any boot resource fails

Ready
  on LeaseRing{request_id, requester, edge_id, direction, ring_spec}
    -> RingLeased if a range fits
    -> queue request if satisfiable but no current range fits
    -> RingLeaseRejected if request can never fit in the ceiling

Ready
  on CancelLease{request_id}
    remove queued request if not yet leased

Ready
  on ReleaseRing{ring_id, proof}
    return range to free list
    retry queued leases serially

Ready
  on Shutdown
    -> ShuttingDown

ShuttingDown
  reject new leases
```

Temporary arena pressure is represented by a queued lease request. Permanent
impossibility is explicit through `RingLeaseRejected`.

Two live leases cannot overlap because lease/release operations pass through one
ArenaManager mailbox.

---

## 20. Ring Contract

A ring is a bounded single-producer/single-consumer byte stream backed by an
arena lease.

Rings define flow control for every payload-moving boundary:

- QUIC ingress stream -> host ingress ring -> worker -> GPU memory
- GPU memory -> worker -> host egress ring -> QUIC egress stream
- disk reader -> host ring -> worker/GPU memory, if used

Each ring has exactly one producer and one consumer.

Ingress:

```text
producer = recv-pump
consumer = GPU worker
```

Egress:

```text
producer = GPU worker
consumer = send-pump
```

Fan-in and fan-out are not ring features. They are represented by multiple rings
or by a higher-level component that owns one side of a ring.

The shared arena ABI is not a Rust collection. It stores offsets, cursors, state
bits, wake bits, and payload bytes. It never stores process-local pointers.

Ring header:

```rust
#[repr(C, align(64))]
struct RingHeader {
    magic: u32,
    version: u16,
    header_len: u16,
    ring_id: u64,
    capacity: u64,
    commit: AtomicU64,
    consume: AtomicU64,
    state: AtomicU32,
    wake: AtomicU32,
}
```

`commit` is the first byte after the committed readable prefix. Bytes with
logical positions `< commit` are valid for the consumer.

`consume` is the first byte not yet released by the consumer. Bytes with logical
positions `< consume` are free for producer reuse.

The producer keeps a local `write` cursor. `write` is the first byte after the
producer's reserved or in-progress write prefix.

Readable interval:

```text
consume .. commit
```

Reserved but not readable interval:

```text
commit .. write
```

Physical index:

```text
physical_index = cursor % capacity
```

The ring is empty when `consume == commit`. The ring is full when:

```text
write - consume == capacity
```

Cursor values are monotonic logical byte positions.

---

## 21. Ring Producer, Consumer, And Wake Rules

Producer rule:

- acquire `consume`
- compute free space as `capacity - (write - consume)`
- reserve a contiguous physical span by advancing local `write`
- write bytes into that span
- publish new `commit` with release ordering after bytes are valid
- send or coalesce `RingReadable { ring_id }`

Consumer rule:

- acquire `commit`
- read bytes in `consume .. commit`
- greedily drain all bytes it can use
- publish new `consume` with release ordering after bytes are safe to release
- send or coalesce `RingWritable { ring_id }` if producer may be unblocked

Wake hints are edge-trigger hints, not ownership:

```rust
RingReadable { ring_id: RingId }
RingWritable { ring_id: RingId }
```

The receiver of a wake must reload cursors from shared memory. Wake hints carry
no byte ranges, byte counts, host pointers, or free-space counts.

Duplicate wake hints may be coalesced only while a pending bit, ready-set entry,
or equivalent durable scheduler state still makes the ring discoverable. Losing
the only transition from empty to readable or full to writable is a liveness bug.

If asynchronous DMA reads host ring memory, the consumer cannot advance
`consume` until the DMA no longer depends on those bytes.

Safety:

- consumer cannot read unwritten bytes because producer publishes `commit` only
  after writing
- producer cannot overwrite unread bytes because free space is computed from
  consumer-owned `consume`
- wraparound cannot alias stale bytes because ownership uses monotonic logical
  cursors
- stale wake/control events cannot alias replacement rings because `RingId` is
  unique for node lifetime

---

## 22. Native Ring Helper

Both Rust hot-path code and the Python worker access process-crossing rings
through the same native implementation.

Python does not implement shared atomics, wrap arithmetic, span calculation, or
cursor publication directly.

The helper exposes operations equivalent to:

```text
ring_open(arena_base, RingLayout) -> RingHandle
ring_close(handle)
ring_readable_span(handle) -> ptr, len
ring_advance_consume(handle, len)
ring_writable_span(handle) -> ptr, len
ring_advance_commit(handle, len)
ring_state(handle) -> state
```

Returned pointers are process-local addresses derived from the caller's mapped
arena base plus arena offsets.

Ring cursors must use atomic acquire/release semantics across the process
boundary.

---

## 23. Edge Stream Protocol

Each edge uses one persistent iroh/QUIC uni-stream from producer node to
consumer node.

Wire shape:

```text
[edge_id preamble]
[object record]
[object record]
...
```

The receiver's edge-demux reader consumes the fixed-width `edge_id` preamble and
hands the stream to the driver rendezvous. After the preamble, the recv-pump is
byte-blind. It copies stream bytes into the ingress ring and advances `commit`.
The worker parses object records from the ring.

The stream is persistent for the edge. It is not opened per object.

QUIC owns reliability and stream ordering. The Myelin does not add app-level
fragment hashes, striping, resume, or per-object streams.

---

## 24. Driver And Transport

The driver is the node's swactor-to-iroh boundary. It owns:

- one iroh endpoint
- connection cache
- edge ALPN
- edge demux
- receive stream rendezvous
- send/recv pump tasks

Example edge ALPN:

```text
swactor/edge/1
```

Connections are cached per `(peer_node_id, ALPN)`. All edges between the same
node pair and ALPN reuse the same connection. Each edge has one persistent
uni-stream within that connection.

Driver inbound messages:

```rust
EstablishSend {
    edge_id: EdgeId,
    rx_node_id: NodeId,
    ring_id: RingId,
    ring_layout: RingLayout,
    tx_addr: ActorAddress,
}

EstablishRecv {
    edge_id: EdgeId,
    ring_id: RingId,
    ring_layout: RingLayout,
    rx_addr: ActorAddress,
}

StreamArrived {
    edge_id: EdgeId,
    stream: RecvStream,
}

StopEdge {
    edge_id: EdgeId,
}
```

Driver outbound events:

```rust
DriverEdgeReady { edge_id: EdgeId }
StreamClosed { edge_id: EdgeId }
StreamFault { edge_id: EdgeId, reason: StreamFaultReason }
PumpStopped { edge_id: EdgeId, ring_id: RingId }
```

No per-object, per-range, or fragment messages exist in the driver hot path.

---

## 25. Receive Demux And Pump FSMs

A recv-pump needs two resources:

- local receive establishment state, including ingress ring
- arriving QUIC stream

They may arrive in either order. The driver stores both halves:

```text
recv_specs: HashMap<EdgeId, RecvSpec>
pending_streams: HashMap<EdgeId, RecvStream>
```

On `EstablishRecv`, if a pending stream exists, the driver spawns the recv-pump.
Otherwise it stores the spec.

On `StreamArrived`, if a recv spec exists, the driver spawns the recv-pump.
Otherwise it stores the stream.

This removes the need for an inter-end readiness handshake. If a stream arrives
before local receive establishment, it waits in `pending_streams`; because no
recv-pump reads from it, QUIC flow control eventually stalls the sender.

Recv-pump FSM:

```text
WaitingForSpecAndStream
  -> Streaming when ring and stream are both present

Streaming
  read free ring span
  read QUIC bytes into that span
  advance commit
  send/coalesce RingReadable{ring_id} to worker
  repeat

Backpressured
  entered when no ring free space exists
  wait for RingWritable{ring_id}
  return to Streaming

Closed
  entered on stream EOF or edge teardown

Faulted
  entered on read error, protocol edge failure, or ring fault
```

The recv-pump does not parse `ObjectHeader` and does not know object boundaries.
EOF alignment is classified by the worker parser, not by the pump.

Send-pump FSM:

```text
WaitingForConnection
  ensure or await cached edge-ALPN connection

WaitingForBytes
  wait for RingReadable{ring_id}

OpenStream
  open one uni-stream
  write edge_id preamble
  -> Streaming

Streaming
  acquire commit
  write committed egress bytes to QUIC
  advance consume after bytes are accepted by write_all
  send/coalesce RingWritable{ring_id}
  repeat

Backpressured
  write_all is pending because network/QUIC flow control is slow
  keep ownership of unread ring bytes until write completes

Closed/Faulted
  emit coarse driver event
```

---

## 26. Edge Establishment

Establishment is local actor setup plus transport rendezvous. Remote edge ends
do not exchange actor messages.

For each edge, the orchestrator provisions the producer and consumer:

```rust
struct ProvisionTx {
    edge_id: EdgeId,
    rx_node_id: NodeId,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
}

struct ProvisionRx {
    edge_id: EdgeId,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
}
```

The producer needs consumer `node_id`, not consumer actor address. The consumer
needs shared `edge_id`, not producer actor address.

Each node has one EdgeEstablisher actor. It owns per-edge records:

```rust
struct EdgeRecord {
    edge_id: EdgeId,
    direction: RingDirection,
    state: EdgeProvisionState,
    lease_request_id: Option<LeaseRequestId>,
    ring_id: Option<RingId>,
    local_edge_actor: ActorAddress,
}
```

Record FSM:

```text
New
  on ProvisionTx/ProvisionRx
    spawn local Tx/Rx edge actor
    create LeaseRequestId
    send LeaseRing to ArenaManager
    -> WaitingForLease

WaitingForLease
  on RingLeased matching request_id
    record ring_id
    send InstallRing to GpuWorkerCtl or token endpoint
    -> WaitingForWorkerRing
  on RingLeaseRejected matching request_id
    notify local edge actor failure
    -> Failed
  on StopEdge
    send CancelLease
    notify local edge actor stopped
    -> Stopped

WaitingForWorkerRing
  on RingInstalled
    send EstablishSend/EstablishRecv to Driver
    -> WaitingForDriver
  on RingFault or StopEdge
    -> Stopping

WaitingForDriver
  on DriverEdgeReady
    notify local Tx/Rx actor Ready
    -> Ready
  on StreamFault/RingFault/StopEdge
    -> Stopping

Ready
  hot path runs without EdgeEstablisher
  coarse object/fault events may pass through

Stopping
  stop pump if one exists
  uninstall worker ring if installed
  wait for quiescence proofs
  release ring if leased
  -> Stopped
```

Stale events for stopped records are ignored except that an unused fresh lease
granted after cancellation must be released without installing worker or pump
state.

---

## 27. Tx And Rx Edge Actors

Tx and Rx actors are role-facing lifecycle gates.

Tx actor state:

```text
Provisioning -> Ready -> Producing -> Stopping -> Stopped
                         -> Faulted
```

Rx actor state:

```text
Provisioning -> Ready -> LoadingObject -> ObjectReady -> Stopping -> Stopped
                         -> Faulted
```

They receive coarse events:

```rust
EdgeReady { edge_id: EdgeId }
ObjectLoaded { edge_id: EdgeId, object_id: ObjectId, device_handle: DeviceObjectHandle }
ObjectProduced { edge_id: EdgeId, object_id: ObjectId }
ObjectFailed { edge_id: EdgeId, object_id: Option<ObjectId>, reason: ObjectFailure }
StreamFault { edge_id: EdgeId, reason: StreamFaultReason }
StopEdge { edge_id: EdgeId }
```

They do not receive:

- bytes
- host pointers
- per-range readiness
- consumed ranges
- free-space events
- flow-control credits

---

## 28. GPU Worker Process Boundary

Each node has one `GpuWorkerCtl` actor. It owns:

- worker process spawn and termination through `swactor_process`
- worker generation numbering
- arena fd inheritance or passing setup
- process actor address and upstream `ProcessOutput` handling
- table of installed rings for current worker generation
- routing worker events to EdgeEstablisher, Driver, Tx/Rx actors, and role layer
- crash detection and crash fanout
- restart policy

`GpuWorkerCtl` does not own:

- arena leases
- QUIC streams
- ring payload bytes
- device allocations
- tinygrad execution
- graph placement

The worker process owns:

- mapped arena view
- worker-side ring handles
- per-ring parser/producer state
- device allocations
- device object handles
- tinygrad role state
- KV cache and worker-internal state
- host-to-device and device-to-host copy scheduling
- copy completion tracking

The public Rust-side interface is a normal actor message enum:

```rust
enum GpuWorkerCtlMsg {
    StartWorker,
    InstallRing(InstallRing),
    UninstallRing(UninstallRing),
    RingReadable { ring_id: RingId },
    RingWritable { ring_id: RingId },
    ConfigureRole(ConfigureRole),
    ExecuteStep(ExecuteStep),
    ReleaseDeviceObject { device_handle: DeviceObjectHandle },
    ShutdownWorker(ShutdownWorker),
    Process(ProcessOutput),
    WorkerAdapter(WorkerAdapterEvent),
}
```

`Process(ProcessOutput)` is delivered by the configured upstream process owner;
there is no process-local notification subscription bridge. If the
production worker still uses a subprocess stdin/stdout protocol, `GpuWorkerCtl`
talks to a separate worker I/O adapter. That adapter owns the child stdio handles
and is outside managed-process core.

The optional worker I/O adapter may use newline-delimited JSON for worker
commands/events. This is an adapter-local protocol, not part of
`crates/process` and not a second distributed protocol.

Adapter rules:

- one command/event JSON object per line;
- adapter-owned stdout may carry worker events;
- adapter-owned stderr may carry logs and diagnostics;
- payload bytes are forbidden in JSON;
- invalid JSON or unknown event shape is a worker/adapter fault.

Worker environment:

```text
SWACTOR_ARENA_FD      memfd for the shared arena
SWACTOR_ARENA_BYTES   arena reservation ceiling
```

---

## 29. GPU Worker Commands

`InitializeWorker`:

```rust
struct InitializeWorker {
    worker_generation: WorkerGeneration,
    arena_ceiling: u64,
    required_ring_helper_abi: u16,
    backend: JsonValue,
}
```

Maps the arena, initializes native helper and backend, then emits `WorkerReady`
or `WorkerFatal`.

`ConfigureRole` is optional:

```rust
struct ConfigureRole {
    role_id: RoleId,
    config: JsonValue,
}
```

`config` is low-frequency app metadata and must not carry payload bytes.

`InstallRing`:

```rust
struct InstallRing {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
    direction: RingDirection,
    layout: RingLayout,
    object_spec: ObjectSpec,
}
```

`edge_id` and `port_id` are required so worker events can be reported in
graph-facing terms.

`UninstallRing`:

```rust
struct UninstallRing {
    ring_id: RingId,
    reason: UninstallReason,
}
```

The worker removes ring state, waits for copy lifetimes to end, closes the
helper handle, and emits `RingQuiesced`.

Wake hints:

```rust
RingReadable { ring_id: RingId }
RingWritable { ring_id: RingId }
```

`ExecuteStep`:

```rust
struct ExecuteStep {
    role_id: RoleId,
    step_id: StepId,
    inputs: Vec<InputBinding>,
    outputs: Vec<OutputBinding>,
    runtime: JsonValue,
    release_inputs_after: bool,
}

struct InputBinding {
    port_id: PortId,
    object_id: ObjectId,
    sequence: Sequence,
    device_handle: DeviceObjectHandle,
}

struct OutputBinding {
    port_id: PortId,
    ring_id: RingId,
    object_id: ObjectId,
    sequence: Sequence,
    extent: u64,
    flags: u32,
}
```

Execution requirements:

1. Validate role is available.
2. Validate input handles belong to current worker generation.
3. Wrap inputs as tinygrad-compatible views.
4. Run role code.
5. Validate returned outputs against declared output bindings.
6. Write each output object to the named egress ring.
7. Emit `ObjectProduced` after each full output object is committed.
8. Emit `StepCompleted` after all declared outputs are produced and role state
   updates are complete.

`ReleaseDeviceObject` frees a device object after no compute or copy event still
references it.

`ShutdownWorker`:

```rust
struct ShutdownWorker {
    mode: ShutdownMode,
}

enum ShutdownMode {
    Graceful,
    AbortInFlight,
}
```

Deferred worker commands:

- generic command accepted/rejected acks
- `BindDeviceObject` / `UnbindDeviceObject`
- `CancelStep`
- `AbortObject`
- `Ping`
- role module/factory provisioning

---

## 30. GPU Worker Events

Worker lifecycle:

```rust
WorkerReady {
    pid: u32,
    worker_generation: WorkerGeneration,
    ring_helper_abi: u16,
    backend: JsonValue,
}

WorkerFatal {
    reason: WorkerFatalReason,
}

WorkerStopped {
    reason: WorkerStoppedReason,
}
```

Role events, only if `ConfigureRole` is used:

```rust
RoleConfigured { role_id: RoleId }
RoleFailed { role_id: RoleId, reason: RoleFailure }
```

Ring events:

```rust
RingInstalled { ring_id: RingId, edge_id: EdgeId, port_id: PortId }
RingFault { ring_id: RingId, edge_id: EdgeId, port_id: PortId, reason: RingFaultReason }
RingQuiesced { ring_id: RingId }
```

Object events:

```rust
ObjectLoaded {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
    object_id: ObjectId,
    sequence: Sequence,
    extent: u64,
    device_handle: DeviceObjectHandle,
}

ObjectProduced {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
    object_id: ObjectId,
    sequence: Sequence,
    extent: u64,
}

ObjectFailed {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
    object_id: Option<ObjectId>,
    sequence: Option<Sequence>,
    reason: ObjectFailure,
}
```

Step events:

```rust
StepCompleted { role_id: RoleId, step_id: StepId }
StepFailed { role_id: RoleId, step_id: StepId, reason: StepFailure }
```

Device lifetime:

```rust
DeviceObjectReleased { device_handle: DeviceObjectHandle }
ReleaseFailed { device_handle: DeviceObjectHandle, reason: ReleaseFailure }
```

Wake hints emitted by worker:

- ingress rings: `RingWritable` after advancing `consume`
- egress rings: `RingReadable` after advancing `commit`

`GpuWorkerCtl` may synthesize `WorkerCrashed` and `RingFault` after process exit,
process error, or worker I/O adapter control-stream failure.

---

## 31. GpuWorkerCtl And Worker FSMs

`GpuWorkerCtl` FSM:

```text
NotStarted
  on StartWorker -> Spawning

Spawning
  spawn ProcessActor with ProcessSpec and upstream = GpuWorkerCtl/process owner
  wait for ProcessOutput::Started
  if subprocess worker protocol is enabled:
    start/connect worker I/O adapter
    send InitializeWorker through worker I/O adapter
  -> Initializing

Initializing
  on WorkerReady -> Running
  on WorkerFatal/process exit/timeout -> Failed

Running
  on actor command -> validate state, write worker command, update tables
  on worker event -> route event
  on ShutdownWorker -> Stopping
  on process exit/error -> Crashed

Stopping
  send ShutdownWorker if process alive
  wait for WorkerStopped and process exit
  on timeout -> Killing

Killing
  close/kill ProcessActor according to policy
  handle terminal ProcessOutput
  mark installed rings faulted
  -> Stopped

Crashed
  mark current-generation device handles invalid
  mark installed rings faulted
  ask driver to stop pumps for installed rings
  wait for teardown to release rings
  -> Failed or Restarting

Restarting
  increment worker_generation
  -> Spawning
```

Worker process FSM:

```text
Booting
  read environment
  wait for InitializeWorker on stdin

Initializing
  map arena
  initialize native helper
  initialize backend/tinygrad
  emit WorkerReady
  -> Running

Running
  on InstallRing -> install ring state
  on RingReadable/RingWritable -> reload cursors and advance ring FSMs
  on ExecuteStep -> run explicit role step subject to backpressure
  on ReleaseDeviceObject -> release handle when safe
  on ShutdownWorker -> Draining

Draining
  reject new work
  finish or abort in-flight work according to mode
  quiesce rings
  emit WorkerStopped
  exit

Fatal
  emit WorkerFatal if possible
  exit non-zero
```

The worker greedily makes progress after relevant commands or wake hints: drain
readable ingress prefixes, advance egress output if writable space exists,
observe copy completions, and emit resulting events.

---

## 32. Worker Ring FSMs

Ingress ring FSM:

```text
Uninstalled
  on InstallRing(direction = Ingress) -> NeedHeader

NeedHeader
  on committed bytes < header_len -> wait
  on complete header -> validate
    valid -> allocate device object -> NeedPayload
    invalid -> Faulted

NeedPayload
  copy committed payload prefixes to device allocation
  advance consume only after copied bytes are safe to release
  if copied == extent -> ObjectComplete
  on close before copied == extent -> Faulted

ObjectComplete
  wait for copy completion
  emit ObjectLoaded
  -> NeedHeader

Faulted
  emit ObjectFailed or RingFault
  stop consuming until UninstallRing

Uninstalling
  stop consuming
  wait for copy lifetimes to end
  emit RingQuiesced
  -> Uninstalled
```

Egress ring FSM:

```text
Uninstalled
  on InstallRing(direction = Egress) -> WaitingForOutput

WaitingForOutput
  on ExecuteStep output binding naming this ring -> NeedWritableForHeader

NeedWritableForHeader
  wait for writable span
  write ObjectHeader
  advance commit
  emit RingReadable
  -> NeedWritableForPayload

NeedWritableForPayload
  copy device payload prefixes into writable ring spans
  advance commit only after host bytes are valid
  emit/coalesce RingReadable
  if produced == extent -> ObjectProduced
  -> WaitingForOutput

Faulted
  emit RingFault
  stop producing until UninstallRing

Uninstalling
  stop accepting outputs
  wait for copy lifetimes to end
  emit RingQuiesced
  -> Uninstalled
```

The egress producer may block an `ExecuteStep` while waiting for egress ring
space. This is the intended backpressure path.

---

## 33. Device Bridge

The worker must provide a backend-specific device bridge:

```text
alloc_device(ObjectSpec, extent) -> DeviceAllocation
free_device(DeviceAllocation)
host_to_device(arena_ptr, len, DeviceAllocation, device_offset) -> CopyEvent
device_to_host(DeviceAllocation, device_offset, arena_ptr, len) -> CopyEvent
copy_event_complete(CopyEvent) -> bool
wrap_for_tinygrad(DeviceAllocation, TensorViewSpec) -> tinygrad object
```

The canonical contract supports range copies so objects may be larger than a
ring. Ingress and egress can then stream prefixes through bounded host rings
while the logical object lives in device memory.

If an implementation uses a constrained complete-object fallback, that mode must
be explicit and must preserve system-visible object, sequence, readiness, and
backpressure semantics. It must also constrain object/ring sizing accordingly.

For synchronous copies:

```text
copy returns -> safe to advance relevant cursor
```

For asynchronous copies:

```text
start copy -> record CopyEvent and host/device ranges
event complete -> advance cursor or release device object
```

---

## 34. Ingress And Egress Flow

Ingress path:

```text
remote worker/GPU -> remote egress ring -> QUIC -> local ingress ring
  -> local worker -> local GPU memory -> ObjectLoaded
```

Receiving sequence:

1. Driver demux reads stream `edge_id` preamble.
2. Driver pairs stream with local ingress ring.
3. Recv-pump waits for free ring space.
4. Recv-pump reads QUIC bytes into free ring span.
5. Recv-pump advances `commit`.
6. Recv-pump sends/coalesces `RingReadable`.
7. Worker wakes and reads `consume..commit`.
8. Worker parses object headers when enough committed bytes exist.
9. Worker allocates device object after header validation.
10. Worker copies committed payload prefixes into device memory.
11. Worker advances `consume` after copies are safe to release.
12. Recv-pump sees free space and continues.
13. Worker emits `ObjectLoaded` after full object and copy completion.

Egress path:

```text
local GPU memory -> local worker -> local egress ring -> QUIC
  -> remote ingress ring -> remote worker/GPU
```

Sending sequence:

1. Worker receives or creates compute result in GPU memory.
2. Worker creates `ObjectHeader` according to `ObjectSpec`.
3. Worker waits for free egress ring space.
4. Worker writes and commits header bytes.
5. Worker copies payload bytes from device into writable ring spans.
6. Worker advances `commit` as host bytes become valid.
7. Worker sends/coalesces `RingReadable`.
8. Send-pump opens persistent uni-stream on first bytes.
9. Send-pump writes `edge_id` preamble once.
10. Send-pump writes committed bytes to QUIC.
11. Send-pump advances `consume` after `write_all` accepts bytes.
12. Send-pump sends/coalesces `RingWritable`.

An object may be larger than the ring. The ring is a transfer window, not object
storage. Compute visibility still begins only at `ObjectLoaded`.

---

## 35. Backpressure

No credits or per-range acknowledgements exist.

Worker slow on ingress:

```text
worker slow
  -> ingress consume does not advance
  -> recv-pump sees no free ingress space
  -> recv-pump stops reading QUIC
  -> QUIC stream flow control stalls remote sender
  -> sender send-pump stops draining egress ring
  -> sender worker stalls on egress free space
```

Network slow on egress:

```text
network slow
  -> send-pump write_all remains pending
  -> egress consume does not advance
  -> worker sees no free egress space
  -> worker stops producing more bytes into egress ring
```

Arena pressure:

```text
arena temporarily exhausted
  -> ArenaManager queues LeaseRing
  -> EdgeEstablisher record remains WaitingForLease
  -> no worker ring is installed
  -> no pump is spawned
  -> no hot-path state exists for that edge
```

No data is dropped because each layer stops before overwriting unread bytes.

---

## 36. Fault Semantics

A run has one execution terminal outcome:

- completed
- faulted
- operator-stopped before completion

Run fault sources include:

- node unavailable before or during run
- membership loss for a required node
- provisioning rejection
- arena boot failure
- oversized ring request
- temporary pressure timeout
- weight download/load/bind failure
- edge establishment failure
- malformed object header
- EOF mid-object
- stream fault
- pump failure
- ring fault
- worker fatal error
- worker crash
- device OOM
- device copy failure
- inference-step ordering violation
- step failure

Teardown failures are recorded in teardown outcome/status. They do not rewrite
a completed inference into a faulted run outcome.

Myelin recovery policy is fail-stop at the run level. The orchestrator records the
first run-level failure reason, stops injecting objects, and begins teardown. It
does not re-place the run.

After a stage faults, it rejects new run work until stopped.

---

## 37. Error Taxonomy

Ring faults:

```rust
enum RingFaultReason {
    UnsupportedRingVersion,
    RingLayoutInvalid,
    RingStateInvalid,
    DeviceOutOfMemory,
    DeviceCopyFailed,
    SequenceViolation,
    HeaderMalformed,
    WorkerProcessExited,
    WorkerShuttingDown,
    WorkerInternal,
}
```

Object failures:

```rust
enum ObjectFailure {
    HeaderMalformed,
    UnsupportedObjectVersion,
    ExtentExceedsMax,
    ExtentAlignmentInvalid,
    SequenceViolation,
    DeviceAllocationFailed,
    DeviceCopyFailed,
    EofMidObject,
}
```

Step failures:

```rust
enum StepFailure {
    RoleUnavailable,
    InvalidInputHandle,
    InvalidOutputRing,
    TinygradError,
    DeviceOutOfMemory,
    OutputExtentInvalid,
    OutputCopyFailed,
    WorkerProcessExited,
    WorkerShuttingDown,
}
```

Worker fatal reasons:

```rust
enum WorkerFatalReason {
    ArenaMapFailed,
    RingHelperAbiMismatch,
    BackendInitFailed,
    MalformedControlMessage,
    UnhandledException,
}
```

Other control reasons:

```rust
enum UninstallReason {
    EdgeStopped,
    RingFaulted,
    WorkerShutdown,
}

enum WorkerStoppedReason {
    Graceful,
    AbortInFlight,
    Fatal,
}
```

---

## 38. Teardown

Teardown starts after completion, fault, or operator stop.

Teardown has its own outcome, separate from the run execution outcome:

```text
RunOutcome = Completed | Faulted | OperatorStopped
TeardownOutcome = Clean | Faulted | TimedOut
```

Orchestrator teardown:

1. Stop injecting objects.
2. Send `StopRun` to every provisioned stage.
3. Stop local token endpoints.
4. Wait for `StageStopped` from every stage or timeout.
5. Record run teardown outcome/status.

Stage teardown:

1. Stop accepting new run work.
2. Stop local edges.
3. Stop driver pumps.
4. Uninstall worker rings.
5. Release arena leases after quiescence proof.
6. Release per-run device objects.
7. Stop or reset worker role state according to local policy.
8. Emit `StageStopped`.

Edge teardown FSM:

```text
Ready
  on StopEdge or fault -> StoppingPump

StoppingPump
  driver stops recv/send pump
  driver emits PumpStopped
  -> StoppingWorkerRing

StoppingWorkerRing
  GpuWorkerCtl sends UninstallRing
  worker removes ring state
  worker waits for in-flight copies or process death
  worker emits RingQuiesced
  -> ReleasingArena

ReleasingArena
  EdgeEstablisher sends ReleaseRing{proof}
  ArenaManager returns lease to free list
  -> Stopped
```

Arena release is safe only after:

- driver pump stopped
- worker ring quiesced or worker process reaped
- copy lifetimes ended or owning process is gone

Teardown is required after success and fault.

---

## 39. System Guarantees

Authority:

- orchestrator is the only component that assigns topology, stages, layer
  ranges, edge ids, and object specs
- nodes reject provisioning and edge data that do not match active run plan

Readiness:

- orchestrator does not inject prompt before global readiness barrier
- a stage does not report `StageReady` before worker, weights, inbound edge,
  outbound edge, and StageController are ready

Weight use:

- a stage cannot execute before assigned weights are loaded and bound
- weight loading failures become stage faults

Edge identity:

- every edge id is unique within run
- every edge has one producer and one consumer
- data plane is addressed by `(node_id, edge_id)`
- remote actor addresses are not needed for data flow

Inference-step ordering:

- prefill is inference step `0`
- continuation steps are strictly increasing
- a stage executes step `s` only after loading inbound object sequence `s`
- stage output object uses sequence `s`
- orchestrator injects `s + 1` only after consuming and accepting output object
  sequence `s`
- workers do not invent graph-visible object ids or sequence numbers

Payload isolation:

- payload bytes never travel in actor messages, process control messages, logs,
  or JSON command/event lines
- actors carry identities, lifecycle events, wake hints, and opaque handles

Compute visibility:

- compute receives only complete logical objects
- `ObjectLoaded` is emitted only after valid header, exact extent copy, copy
  completion, and device handle creation

Backpressure:

- slow worker, slow network, or downstream stall propagates by ring and QUIC
  flow control
- unread bytes are not overwritten

Terminal outcome:

- each run records exactly one terminal outcome
- after terminal outcome begins, no new run work is accepted except teardown

Quiescence:

- arena memory cannot be reused under a live pump, worker ring, or copy
  operation
- release requires teardown proof

Worker restart:

- restart creates a new generation
- old device handles, roles, rings, and steps are invalid
- roles and rings must be reinstalled

---

## 40. Observability Surface

The system must emit stable lifecycle events for behavioral contracts and tests.
Event transport and storage are implementation details.

Required event identities:

- `run_id`
- `node_id`
- `stage_index`
- `edge_id`
- `ring_id`
- `object_id`
- `sequence`
- `step_id`
- `worker_generation`

Required lifecycle events:

```text
node_started
node_available
node_faulted
pool_ready
run_planned
stage_provision_started
weights_download_started
weights_downloaded
weights_loaded
edge_provision_started
edge_ready
stage_ready
readiness_barrier_passed
prompt_injected
object_loaded
execute_step_started
object_produced
step_completed
token_received
run_completed
run_faulted
stop_run_sent
stage_stopped
run_torn_down
```

Fault events must include a stable reason enum and the component that detected
the fault. Tests and operators should not need to scrape free-form logs to
determine lifecycle progress.

---

## 41. Behavioral Contract Inventory

The next specification layer should extract API surfaces and behavioral
contracts for these components and required behaviors:

- node boot lifecycle
- membership/SWIM convergence
- resource inventory and run planner
- orchestrator run FSM
- StageController
- stage-local weight loading path
- orchestrator token endpoint behavior
- ArenaManager
- shared ring helper/ABI
- EdgeEstablisher
- Tx and Rx edge actors
- iroh driver and recv/send pumps
- GpuWorkerCtl
- GPU worker process adapter
- GPU worker ingress parser
- GPU worker egress producer
- device bridge
- observability/event surface

Required test layers:

1. Component behavioral contract tests.
2. Local end-to-end mock tests without real networking.
3. Local Docker cluster tests with real ports and real networking.

VastAI smoke/integration validation comes after local Docker cluster behavior is
stable.

---

## 42. Canonical Myelin Scenario

The first end-to-end scenario is one "hello world" prompt over a rented GPU
pool:

1. Operator provisions `N` GPU nodes with the barebones swactor + tinygrad/CUDA
   image.
2. Every node starts the Rust node process and GPU worker process.
3. Nodes join SWIM membership.
4. Orchestrator observes `PoolReady`.
5. Orchestrator builds a linear GGUF `RunPlan`.
6. Orchestrator provisions every stage.
7. Each stage downloads and loads assigned weights.
8. Each stage provisions inbound and outbound edges.
9. Every stage reports `StageReady`.
10. Orchestrator observes the global readiness barrier.
11. Orchestrator injects the initial input object for inference step `0`.
12. Stages execute the initial step and return output object sequence `0`.
13. Orchestrator continues until the run policy stops or `max_tokens` is reached.
14. Orchestrator records `run_completed`.
15. Orchestrator tears down edges, worker run state, and token endpoints.
16. Orchestrator records `run_torn_down`.

Success requires one terminal run outcome and every arena range leased by the run
to be quiesced or released by teardown.

---

## 43. Deferred

- automatic placement optimization
- arbitrary graph execution
- multi-input joins and fan-out beyond explicit role-layer handling
- multiple concurrent runs on one stage chain
- batching, speculative decoding, continuous serving
- warm model/weight reuse across prompts
- production artifact layout and weight cache eviction
- re-placement after node failure
- trustless verification or adversarial payload defense
- host pinning and asynchronous DMA performance policy
- removing backend copy limitations
- VastAI-specific provisioning automation
