# GPU Worker Interface and Control Specification

**Status:** draft design specification for review.

**Relationship to other documents.** This document extends
`RING_BACKPRESSURE_SPEC.md`. The ring spec remains the authority for shared
memory rings, arena leases, QUIC pumps, ring cursor safety, object records, and
backpressure. `DESIGN_DIRECTIVES.md` remains steering context. This document
defines the process-facing control surface: how the Rust node process starts,
drives, supervises, and tears down the Python/tinygrad GPU worker.

The design is intentionally a swactor-integrated worker, not a custom runtime.
The Rust-side interface is a small actor message FSM. The process boundary uses
the existing `swactor_process` actor pattern: a domain actor owns a
`ProcessActor`, writes process input through `ProcessCommand::WriteStdin`, and
receives lifecycle/output through `ProcessNotification`.

Current code is pattern guidance for this document, especially the
`InferenceActor` and `StageActor` process supervision shape. Current payload
movement code is not otherwise authoritative.

---

## 1. Scope

This document specifies:

- the node process to GPU worker process boundary
- the swactor messages used to drive the worker
- worker boot, ring installation, execution, teardown, and crash handling
- GPU worker responsibilities and non-responsibilities
- device object lifetime
- how tinygrad execution is driven by the control plane
- correctness and safety invariants

This document does not specify:

- placement policy
- model graph partitioning
- role provisioning beyond the minimal control hook needed by execution
- QUIC stream implementation details
- actor runtime internals
- trustless verification
- high-performance host pinning policy beyond the required safety contract

---

## 2. Design Commitments

The GPU worker is driven by the node control plane. It is not an autonomous graph
scheduler.

The Rust-side worker controller is a swactor actor. Other actors send it typed
messages; it handles those messages serially according to its current state.

The worker process does not run a separate swactor runtime. It is treated as a
process-backed mailbox endpoint owned by the controller actor.

The worker never receives payload bytes in actor/control messages. Payload bytes
move through shared-memory rings only.

Actors and the worker controller exchange coarse lifecycle events, terminal
events, and wake hints. They do not exchange byte ranges, free byte counts, host
pointers, tensor payloads, or fragments.

The worker owns GPU-visible state. The node process owns arena allocation,
transport, edge lifecycle, process supervision, and graph ordering.

The role layer drives compute explicitly with `ExecuteStep`. Loading an ingress
object into GPU memory does not automatically run tinygrad.

---

## 3. Process Topology

Each node has:

```text
node process (Rust)                         GPU worker process
-------------------                         ------------------
swactor runtime                             Python control loop
GpuWorkerCtl actor                          native ring helper
swactor_process::ProcessActor               tinygrad role code
ArenaManager                                device bridge
Driver and QUIC pumps

shared memfd arena mapped once by both processes
ProcessActor-owned stdin/stdout/stderr pipes for process control and diagnostics
```

`GpuWorkerCtl` is the Rust-side owner of the GPU worker process. It is the only
domain actor that sends worker commands or interprets worker events.

Driver pumps and edge actors do not talk to the worker process directly. They
send worker-bound messages to `GpuWorkerCtl`. `GpuWorkerCtl` serializes those
commands onto the process actor.

The process actor remains the only code that owns OS pipes, child process
lifecycle, and process notifications. This matches the existing
`InferenceActor`/`StageActor` pattern:

```text
domain actors
  -> GpuWorkerCtlMsg
  -> GpuWorkerCtl actor
  -> ProcessCommand::WriteStdin { data }
  -> ProcessActor
  -> worker stdin

worker stdout/stderr
  -> ProcessNotification::Output
  -> ProcessBridge
  -> GpuWorkerCtlMsg::Process
  -> GpuWorkerCtl actor
  -> domain events
```

The worker process maps the arena once at startup. The worker uses the native
ring helper for all cross-process ring cursor operations. Python code does not
implement shared atomics, wrap arithmetic, cursor publication, or ring span
calculation.

---

## 4. Components and Ownership

### 4.1 GpuWorkerCtl

`GpuWorkerCtl` owns:

- worker process spawn and termination through `swactor_process`
- worker generation numbering
- arena fd inheritance or fd passing setup
- the process actor address and process notification bridge
- the Rust-side table of installed rings for the current worker generation
- routing worker events to EdgeEstablisher, Driver, Tx/Rx actors, and the role
  layer
- crash detection and crash fanout
- restart policy

`GpuWorkerCtl` does not own:

- arena leases
- QUIC streams
- ring payload bytes
- device allocations
- tinygrad execution
- graph placement

### 4.2 GPU Worker Process

The worker process owns:

- its mapped view of the arena
- worker-side ring handles
- per-ring parser and producer state
- device allocations
- device object handles
- tinygrad role state
- worker-internal state such as KV cache
- host-to-device and device-to-host copy scheduling
- copy completion tracking

The worker process does not own:

- arena allocation or lease reuse
- edge establishment
- peer routing
- QUIC stream creation
- actor addresses
- graph-level ordering outside the received `ExecuteStep` commands

### 4.3 Native Ring Helper

The native ring helper is linked or loaded by the worker process and used by Rust
hot-path code. It exposes the operations needed to:

- open the shared arena and open/close rings by layout
- read committed spans
- advance consume after bytes are safe to release
- read writable spans
- advance commit after bytes are valid
- inspect ring state for fault/debug handling

The helper may also expose wake pending-bit helpers, but wake ownership remains
outside payload ownership. Ring cursors are the authority.

### 4.4 Device Bridge

The worker must have a backend-specific device bridge capable of range copies:

```text
alloc_device(ObjectSpec, extent) -> DeviceAllocation
free_device(DeviceAllocation)
host_to_device(arena_ptr, len, DeviceAllocation, device_offset) -> CopyEvent
device_to_host(DeviceAllocation, device_offset, arena_ptr, len) -> CopyEvent
copy_event_complete(CopyEvent) -> bool
wrap_for_tinygrad(DeviceAllocation, TensorViewSpec) -> tinygrad object
```

A high-level API that can only copy a complete host buffer into a complete tensor
is insufficient. Objects may be larger than a ring, so ingress and egress require
partial range copies.

For the MVP, copies may be synchronous. If copies are asynchronous, the worker
must not advance a ring cursor past bytes still used by DMA.

---

## 5. Actor and Process Message Boundary

The public Rust-side interface is a normal swactor actor message enum.

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
    Process(ProcessNotification),
}
```

`Process(ProcessNotification)` is delivered by a small `ProcessBridge` actor,
exactly like the existing process-backed inference and pipeline actors.

There is no custom length-prefixed frame, no control envelope, no protocol
version field on every message, and no generic command id/reply id layer.
Correlation uses the domain identifiers already present in the command:
`ring_id`, `edge_id`, `port_id`, `object_id`, `sequence`, `step_id`, and
`device_handle`.

### 5.1 Process Adapter Encoding

The process adapter may encode worker commands and events as one JSON object per
line on stdin/stdout. This is an implementation adapter for a Python subprocess,
not a separate protocol layer.

Rules:

- one serialized command or event per line
- stdout is reserved for worker events
- stderr is reserved for logs and diagnostics
- payload bytes are forbidden in command/event JSON
- invalid JSON or unknown event shape is a worker/process fault from
  `GpuWorkerCtl`'s perspective

This keeps the Python worker easy to inspect while preserving the actual swactor
boundary: actors exchange typed Rust messages with `GpuWorkerCtl`, and
`GpuWorkerCtl` uses `ProcessActor` as the subprocess mailbox adapter.

### 5.2 Process Environment

At spawn, the node makes the arena fd available to the worker and passes the fd
number through environment:

```text
SWACTOR_ARENA_FD      memfd for the shared arena
SWACTOR_ARENA_BYTES   arena reservation ceiling
```

The worker reads commands from stdin and writes events to stdout. It may write
logs to stderr. It must not write logs to stdout.

---

## 6. Identifiers and Minimal Data Types

```rust
struct WorkerGeneration(u64);
struct RoleId(u64);
struct PortId(u64);
struct RingId(u64);
struct EdgeId(u64);
struct ObjectId(u64);
struct Sequence(u64);
struct StepId(u64);
```

`RingId` is unique for the node lifetime, as specified by the ring spec.

`JsonValue` means a `serde_json::Value`-style opaque configuration value used
only for low-frequency app metadata. It must not carry payload bytes.

`DeviceObjectHandle` is opaque to the node process:

```rust
struct DeviceObjectHandle {
    worker_generation: u64,
    id: u64,
}
```

A device handle is valid only inside the worker generation that created it. A
worker restart invalidates every previous handle, even if a later worker maps the
same arena.

### 6.1 Ring Layout and Object Spec

`RingLayout`, `ObjectSpec`, and `ObjectHeader` are defined by
`RING_BACKPRESSURE_SPEC.md`. This document only relies on these facts:

- `RingLayout` contains arena-relative offsets and ring capacity; it never
  contains process-local pointers
- `ObjectHeader` supplies runtime facts: `object_id`, `sequence`, `extent`, and
  flags
- `ObjectSpec` supplies the role-known validation contract: object kind, max
  extent, dtype/shape/layout, alignment, and sequence policy

### 6.2 Role Configuration

Role provisioning is intentionally not designed here. The current worker may be
single-role and may choose its role through `ProcessSpec` args/env at spawn time.

`ConfigureRole` exists only as a low-frequency control hook for deployments that
need runtime role configuration before execution:

```rust
struct ConfigureRole {
    role_id: RoleId,
    config: JsonValue,
}
```

`config` is opaque to this spec. It must not carry payload bytes. Detailed module
loading, factory selection, versioning, and persistent binding APIs are deferred
until role provisioning is actually implemented.

---

## 7. Commands Sent To The Worker

These are the worker commands that solve current control problems. Anything not
listed here is deferred until a concrete caller needs it.

### 7.1 InitializeWorker

Sent once after the process starts.

```rust
struct InitializeWorker {
    worker_generation: WorkerGeneration,
    arena_ceiling: u64,
    required_ring_helper_abi: u16,
    backend: JsonValue,
}
```

The worker maps the arena fd, initializes the native helper and backend, and
emits `WorkerReady` or `WorkerFatal`.

### 7.2 ConfigureRole

Optional. Sent only when the app needs runtime role configuration.

```rust
struct ConfigureRole {
    role_id: RoleId,
    config: JsonValue,
}
```

The worker records enough role state to execute later `ExecuteStep` commands. It
does not run compute.

### 7.3 InstallRing

```rust
struct InstallRing {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
    direction: RingDirection,
    layout: RingLayout,
    object_spec: ObjectSpec,
}

enum RingDirection {
    Ingress,
    Egress,
}
```

`edge_id` and `port_id` are required. The worker must be able to report
`ObjectLoaded`, `ObjectProduced`, and `RingFault` in role/edge terms without
guessing from `ring_id`.

For ingress, the worker creates a parser state machine in `NeedHeader`.

For egress, the worker creates a producer state machine waiting for an
`ExecuteStep` output that names the ring.

Terminal events:

```rust
RingInstalled { ring_id, edge_id, port_id }
RingFault { ring_id, edge_id, port_id, reason }
```

### 7.4 UninstallRing

```rust
struct UninstallRing {
    ring_id: RingId,
    reason: UninstallReason,
}
```

The worker removes the ring from active state, stops parsing or producing on the
ring, waits for copy lifetimes to end, and closes the native helper handle.

Terminal event:

```rust
RingQuiesced { ring_id }
```

### 7.5 RingReadable

```rust
RingReadable { ring_id: RingId }
```

Wake hint to the worker. For ingress rings, it tells the worker that committed
bytes may be available. The worker must reload ring cursors from shared memory.

Duplicate hints may be coalesced.

### 7.6 RingWritable

```rust
RingWritable { ring_id: RingId }
```

Wake hint to the worker. For egress rings, it tells the worker that free space
may be available. The worker must reload ring cursors from shared memory.

Duplicate hints may be coalesced.

### 7.7 ExecuteStep

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

The role layer assigns output `object_id` and `sequence`. The worker does not
invent graph-visible ordering.

Execution requirements:

1. Validate the role is available.
2. Validate input handles belong to the current worker generation.
3. Wrap input handles as tinygrad-compatible views.
4. Run the role code.
5. Validate returned outputs against the declared output bindings.
6. Write each output object to the named egress ring.
7. Emit `ObjectProduced` after each full output object has been committed.
8. Emit `StepCompleted` after all declared outputs are produced and role state
   updates are complete.

Terminal events:

```rust
StepCompleted { role_id, step_id }
StepFailed { role_id, step_id, reason }
```

If `release_inputs_after` is true, the worker releases each input device object
after the step reaches a terminal event and no backend work still references the
object.

There is no separate step FSM requirement. The step behavior follows from serial
message handling, the per-ring FSMs, and backpressure from writable ring space.

### 7.8 ReleaseDeviceObject

```rust
ReleaseDeviceObject {
    device_handle: DeviceObjectHandle,
}
```

The worker frees the device allocation after no tinygrad computation or copy
event still references it.

Terminal events:

```rust
DeviceObjectReleased { device_handle }
ReleaseFailed { device_handle, reason }
```

### 7.9 ShutdownWorker

```rust
struct ShutdownWorker {
    mode: ShutdownMode,
}

enum ShutdownMode {
    Graceful,
    AbortInFlight,
}
```

For `Graceful`, the worker rejects new work, finishes accepted operations if
possible, quiesces rings, releases device objects, emits `WorkerStopped`, and
exits.

For `AbortInFlight`, the worker stops accepting new commands, faults in-flight
work, quiesces rings as far as possible, emits `WorkerStopped`, and exits.

The timeout policy belongs to `GpuWorkerCtl` and `ProcessActor`, not to the
worker command schema.

### 7.10 Deferred Commands

These controls are intentionally not part of the MVP:

- generic command accepted/rejected acknowledgements
- `BindDeviceObject` / `UnbindDeviceObject`
- `CancelStep`
- `AbortObject`
- `Ping`
- role module/factory provisioning

They can be added when a concrete caller needs them. Until then, the existing
domain terminal events carry enough state to route success and failure.

---

## 8. Events Sent By The Worker

### 8.1 Worker Lifecycle Events

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

`WorkerReady` means the arena is mapped, the native helper ABI is compatible, and
the backend can accept ring installation and execution commands. It does not mean
any role or ring is installed.

### 8.2 Role Events

Only needed if `ConfigureRole` is used:

```rust
RoleConfigured { role_id: RoleId }
RoleFailed { role_id: RoleId, reason: RoleFailure }
```

### 8.3 Ring Events

```rust
RingInstalled {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
}

RingFault {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
    reason: RingFaultReason,
}

RingQuiesced {
    ring_id: RingId,
}
```

### 8.4 Object Events

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

`ObjectLoaded` means a full ingress object has been copied into device memory and
all copy events for that object have completed.

`ObjectProduced` means the full egress object header and payload have been
committed to the egress ring. It does not mean the remote node received it.

### 8.5 Step Events

```rust
StepCompleted {
    role_id: RoleId,
    step_id: StepId,
}

StepFailed {
    role_id: RoleId,
    step_id: StepId,
    reason: StepFailure,
}
```

### 8.6 Device Lifetime Events

```rust
DeviceObjectReleased {
    device_handle: DeviceObjectHandle,
}

ReleaseFailed {
    device_handle: DeviceObjectHandle,
    reason: ReleaseFailure,
}
```

### 8.7 Wake Hints Sent By The Worker

```rust
RingWritable { ring_id: RingId }
RingReadable { ring_id: RingId }
```

For ingress rings, the worker sends `RingWritable` after advancing `consume`.

For egress rings, the worker sends `RingReadable` after advancing `commit`.

Wake hints carry no byte counts, ranges, or ownership.

### 8.8 GpuWorkerCtl Synthetic Events

Some events are generated by `GpuWorkerCtl`, not read from the worker:

```rust
WorkerCrashed {
    worker_generation: WorkerGeneration,
    status: ExitStatus,
}

RingFault {
    ring_id: RingId,
    edge_id: EdgeId,
    port_id: PortId,
    reason: RingFaultReason,
}
```

`ExitStatus` is `swactor_process::ExitStatus`.

`GpuWorkerCtl` synthesizes these events after process exit, process error, or
stdout control-stream failure. The worker cannot emit them because it is already
gone or no longer trustworthy. For process exit, the synthetic ring fault reason
is `WorkerProcessExited`.

---

## 9. GpuWorkerCtl Actor FSM

`GpuWorkerCtl` is a normal swactor actor. Its FSM is advanced by incoming
`GpuWorkerCtlMsg` values and by process notifications delivered through
`ProcessBridge`.

```text
NotStarted
  on StartWorker -> Spawning

Spawning
  spawn ProcessActor with ProcessSpec
  spawn ProcessBridge
  subscribe bridge to ProcessActor
  wait for ProcessNotification::Started
  send InitializeWorker through ProcessCommand::WriteStdin
  -> Initializing

Initializing
  on WorkerReady -> Running
  on WorkerFatal/process exit/timeout -> Failed

Running
  on actor command -> validate state, write worker command, update local tables
  on worker event -> route event to local actors and driver
  on ShutdownWorker -> Stopping
  on process exit/error -> Crashed

Stopping
  send ShutdownWorker if process is still alive
  wait for WorkerStopped and process exit
  on timeout -> Killing

Killing
  send ProcessCommand::Close or stop/kill ProcessActor according to ProcessSpec
  reap process through ProcessActor notification
  mark installed rings faulted
  -> Stopped

Crashed
  mark current-generation device handles invalid
  mark installed rings faulted
  ask driver to stop pumps for all installed rings
  wait for teardown to release rings
  -> Failed or Restarting

Restarting
  increment worker_generation
  -> Spawning
```

`GpuWorkerCtl` must not claim a ring is quiesced merely because the worker exited.
Quiescence for arena reuse still requires the edge teardown proof from the ring
spec: pump stopped, worker process reaped or ring quiesced, and copy lifetimes
ended or the process that owns them is gone.

---

## 10. Worker Process FSM

The worker process has one state owner and handles process commands serially. It
may maintain multiple internal FSMs, especially one per installed ring, but there
is no separate worker scheduler runtime requirement.

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
  on RingReadable/RingWritable -> reload cursors and advance affected ring FSMs
  on ExecuteStep -> run explicit role step subject to ring backpressure
  on ReleaseDeviceObject -> release handle when safe
  on ShutdownWorker -> Draining

Draining
  reject new work
  finish or abort in-flight work according to shutdown mode
  quiesce rings
  emit WorkerStopped
  exit

Fatal
  emit WorkerFatal if possible
  exit non-zero
```

The worker should greedily make progress after any relevant command or wake hint:
drain readable ingress prefixes, advance egress output if writable space exists,
observe copy completions, and emit resulting events. The concrete internal data
structures used to remember ready rings or pending copies are implementation
details, provided duplicate wake hints cannot lose liveness.

---

## 11. Per-Ring Worker State

### 11.1 Ingress Ring FSM

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

### 11.2 Egress Ring FSM

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
  stop accepting outputs for this ring
  wait for copy lifetimes to end
  emit RingQuiesced
  -> Uninstalled
```

The egress producer may block an `ExecuteStep` while waiting for ring space. That
is the intended backpressure path.

---

## 12. Role and Step State

Role state is intentionally minimal in this spec.

```text
Unavailable
  on WorkerReady with fixed boot role -> Ready
  on ConfigureRole -> Configuring

Configuring
  load/record app-defined role config
  on success -> Ready
  on failure -> Failed

Ready
  on ExecuteStep -> run one explicit step
  on Shutdown -> Stopping

Stopping
  reject new steps
  release role-local resources
```

The MVP role concurrency policy is one active `ExecuteStep` per role. A later
worker may support multiple active steps only if the role declares that its state
is reentrant or the steps are explicitly independent.

No separate step FSM is specified. The step either reaches `StepCompleted` or
`StepFailed`, and any partial egress object faults its ring.

---

## 13. Message Flows

### 13.1 Boot

```text
GpuWorkerCtl -> swactor_process: spawn ProcessActor
ProcessActor -> OS: spawn worker with arena fd/env
ProcessActor -> GpuWorkerCtl: ProcessNotification::Started
GpuWorkerCtl -> ProcessActor: WriteStdin(InitializeWorker JSON line)
worker: map arena once
worker: initialize native helper and backend
worker -> stdout: WorkerReady JSON line
ProcessActor -> GpuWorkerCtl: ProcessNotification::Output(stdout)
GpuWorkerCtl: parse WorkerReady; node may now install rings and execute steps
```

Failure before `WorkerReady` is a boot failure. No rings are installed, so no
arena lease can be corrupted by the worker.

### 13.2 Install Receive Edge

```text
EdgeEstablisher receives RingLeased
EdgeEstablisher -> GpuWorkerCtl: InstallRing(direction = Ingress, edge_id, port_id)
GpuWorkerCtl -> worker: InstallRing
worker: open native ring handle and create ingress parser
worker -> GpuWorkerCtl: RingInstalled
GpuWorkerCtl -> EdgeEstablisher: RingInstalled
EdgeEstablisher -> Driver: EstablishRecv
Driver: pair recv spec with stream when available
Driver: spawn recv-pump
```

The driver does not start a recv-pump until the worker has installed the ring.

### 13.3 Ingress Object Load

```text
recv-pump reads QUIC bytes into free ingress ring span
recv-pump advances commit
recv-pump -> GpuWorkerCtl: RingReadable(ring_id)
GpuWorkerCtl -> worker: RingReadable(ring_id)

worker reloads commit/consume
worker parses ObjectHeader
worker validates extent and sequence
worker allocates device object
worker copies committed payload prefixes to device
worker advances consume after copies are safe
worker -> GpuWorkerCtl: RingWritable(ring_id)
GpuWorkerCtl -> recv-pump: RingWritable(ring_id)

when full object is copied and copy events complete:
worker -> GpuWorkerCtl: ObjectLoaded(..., device_handle)
GpuWorkerCtl routes ObjectLoaded to the role/Rx layer
```

No actor receives byte ranges. The ring cursors are the only byte ownership
state.

### 13.4 Execute Step and Produce Output

```text
role layer has all required input device handles
role layer chooses output object_id and sequence
role layer -> GpuWorkerCtl: ExecuteStep(inputs, outputs)
GpuWorkerCtl -> worker: ExecuteStep

worker validates handles
worker calls tinygrad role code
worker writes output ObjectHeader to egress ring
worker copies output payload to egress ring as space becomes available
worker advances commit after host bytes are valid
worker -> GpuWorkerCtl: RingReadable(egress_ring)
GpuWorkerCtl -> send-pump: RingReadable

send-pump writes committed bytes to QUIC
send-pump advances consume after write_all accepts bytes
send-pump -> GpuWorkerCtl: RingWritable(egress_ring)
GpuWorkerCtl -> worker: RingWritable

worker -> GpuWorkerCtl: ObjectProduced
worker -> GpuWorkerCtl: StepCompleted
```

`StepCompleted` follows `ObjectProduced` for all declared outputs. For a step
with no outputs, `StepCompleted` follows successful compute and state update.

### 13.5 Release Device Object

```text
role layer decides object is no longer needed
role layer -> GpuWorkerCtl: ReleaseDeviceObject(device_handle)
GpuWorkerCtl -> worker: ReleaseDeviceObject
worker waits until no copy or compute references the object
worker frees device allocation
worker -> GpuWorkerCtl: DeviceObjectReleased
```

The node process never dereferences a device handle. It can only pass it back to
the worker.

### 13.6 Stop Edge

```text
EdgeEstablisher -> Driver: StopEdge
Driver stops pump
Driver -> EdgeEstablisher: PumpStopped

EdgeEstablisher -> GpuWorkerCtl: UninstallRing
GpuWorkerCtl -> worker: UninstallRing
worker removes ring from active state
worker waits for copy lifetimes to end
worker -> GpuWorkerCtl: RingQuiesced
GpuWorkerCtl -> EdgeEstablisher: RingQuiesced

EdgeEstablisher -> ArenaManager: ReleaseRing(proof)
```

The arena lease is released only after both the driver pump and worker ring are
quiesced, or after the worker process is dead and reaped.

### 13.7 Worker Crash

```text
worker process exits or process actor reports error
GpuWorkerCtl marks all current-generation device handles invalid
GpuWorkerCtl emits WorkerCrashed to role layer
GpuWorkerCtl synthesizes RingFault for every installed ring
GpuWorkerCtl asks Driver to stop pumps for those rings
EdgeEstablisher waits for PumpStopped
worker side is considered gone only after process reap
EdgeEstablisher releases arena rings after teardown proof
```

The arena survives because it is owned by the node process. Device allocations do
not survive because they were owned by the worker process/backend context.

Restart creates a new worker generation. Rings and roles must be reinstalled.

---

## 14. Error Taxonomy

### 14.1 RingFaultReason

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

### 14.2 ObjectFailure

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

### 14.3 StepFailure

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

### 14.4 WorkerFatalReason

```rust
enum WorkerFatalReason {
    ArenaMapFailed,
    RingHelperAbiMismatch,
    BackendInitFailed,
    MalformedControlMessage,
    UnhandledException,
}
```

### 14.5 Role and Release Failures

```rust
enum RoleFailure {
    InvalidRoleConfig,
    BackendUnsupported,
    WorkerRejectedRole,
}

enum ReleaseFailure {
    InvalidDeviceHandle,
    ObjectInUse,
    BackendFreeFailed,
}
```

### 14.6 Control Reasons

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

MVP policy: device OOM during ingress object allocation faults the ring. The
worker does not silently skip a payload and continue to later objects. Replanning
or retry is a higher-level policy.

---

## 15. Correctness and Safety Invariants

**Control messages carry no payload bytes.** Payload movement occurs only through
arena-backed rings. This keeps actors and process stdio out of the byte hot path.

**The worker cannot access an unleased arena range.** `InstallRing` is sent only
after ArenaManager emits `RingLeased`; the message contains the only usable ring
layout.

**The driver cannot pump into a ring the worker has not installed.**
EdgeEstablisher sends `EstablishRecv` or `EstablishSend` only after
`RingInstalled`.

**Python does not own ring atomics.** All cursor loads, cursor stores, span
calculation, and wrap arithmetic use the native ring helper.

**Ingress compute visibility starts at `ObjectLoaded`.** The worker emits
`ObjectLoaded` only after header validation, exact extent copy, copy completion,
and device handle creation. The role layer cannot pass a partial object to
`ExecuteStep` because no handle exists before then.

**The recv-pump cannot overwrite host bytes still needed by DMA.** The worker
advances ingress `consume` only after the corresponding host bytes are no longer
read by synchronous copy or asynchronous DMA.

**The send-pump cannot read unwritten egress bytes.** The worker advances egress
`commit` only after header or payload bytes are valid in host memory.

**Backpressure is absence of writable ring space.** If the worker is slow,
ingress `consume` does not advance and the recv-pump stops reading QUIC. If the
network is slow, egress `consume` does not advance and the worker stops producing
more bytes into the egress ring.

**Wake hints are not ownership.** `RingReadable` and `RingWritable` only cause
the receiver to reload cursors. Duplicate hints may be coalesced only while the
ring remains discoverable.

**Device handles are generation-scoped.** A handle from an old worker generation
is rejected by the new worker. This prevents stale handles from aliasing new
device allocations after restart.

**Object id and sequence ownership is explicit.** Ingress object id and sequence
come from the received `ObjectHeader` and are validated against `ObjectSpec`.
Egress object id and sequence are assigned by the role/control layer in
`ExecuteStep`; the worker serializes them but does not invent graph-visible
ordering.

**Role execution is explicit.** Loading an object does not run compute. The role
layer must issue `ExecuteStep` with concrete input handles and output bindings.

**Partial egress objects fault the ring.** If the worker commits an output header
but cannot commit the full declared payload, the egress ring is faulted. The
receiver must not interpret later bytes as the next object.

**A released arena range has no live users.** `ReleaseRing` is sent only after
the driver pump is stopped and the worker has emitted `RingQuiesced`, or after
the worker process is reaped. Copy lifetimes must have ended or the process that
owned them must be gone.

**Worker restart does not preserve GPU state.** Roles, rings, steps, and device
handles are all current-generation state. Restart requires reconfiguration.

---

## 16. Implementation Requirements

### 16.1 Actor Integration

The Rust-side implementation must follow the existing process-actor idiom:

- `GpuWorkerCtl` is an `ActorInterface` implementation with one incoming enum
- it spawns a `ProcessActor` through `swactor_process`
- it spawns a private bridge actor for `ProcessNotification`
- it sends worker input with `ProcessCommand::WriteStdin`
- it closes the worker with `ProcessCommand::Close` and normal actor teardown
- it never exposes process stdin/stdout handles to driver pumps or edge actors

### 16.2 Copy Safety

For synchronous copies:

```text
copy returns -> safe to advance relevant cursor
```

For asynchronous copies:

```text
start copy -> record CopyEvent and host/device ranges
event complete -> advance relevant cursor or release device object
```

The worker must track host ranges for ring cursor release and device ranges for
device object lifetime.

### 16.3 Output Commit Discipline

For egress:

1. The worker must know the output extent before writing the object header.
2. The worker writes and commits the header.
3. The worker copies payload prefixes into writable spans.
4. The worker commits each prefix only after host bytes are valid.
5. The worker emits `ObjectProduced` only after all `extent` bytes are committed.

If the worker cannot determine output extent before serialization, it must first
materialize the output into a device object with known byte extent. The wire
header cannot contain a placeholder extent.

### 16.4 Logs and Diagnostics

Worker events go to stdout as process-adapter messages. Diagnostic logs go to
stderr.

`GpuWorkerCtl` may keep a stderr tail for crash diagnostics. This is diagnostic
only and does not change safety rules.

---

## 17. Resolutions To Known Design Issues

### 17.1 No Custom Control Frames

Decision: remove length-prefixed frames, generic envelopes, per-message protocol
versions, command ids, and reply ids. Rust code uses normal swactor messages.
The Python subprocess adapter may use newline-delimited JSON because the
codebase already uses `ProcessActor` stdin/stdout for process-backed workers.

Reason: this worker is integrated into swactor. A second local control protocol
duplicates actor semantics without solving a current problem.

### 17.2 Ring Install Needs Edge and Port Identity

Decision: `InstallRing` includes `edge_id` and `port_id`. The worker reports all
object and ring events with `ring_id`, `edge_id`, and `port_id`.

Reason: `ring_id` is a node-local transport identifier. The role and edge layers
need graph-facing identity without maintaining an ambiguous reverse lookup in
the worker.

### 17.3 Device Handle Lifetime

Decision: device handles are opaque, generation-scoped, and explicitly released
with `ReleaseDeviceObject`.

Reason: this prevents stale handle aliasing after restart and gives the role
layer explicit control over activation and cache lifetimes.

### 17.4 Compute Driving API

Decision: compute is driven by `ExecuteStep`; `ConfigureRole` is optional and
opaque until role provisioning exists. `ObjectLoaded` is a data-ready event, not
an execution trigger.

Reason: the control plane owns ordering and correlation.

### 17.5 Object Id And Sequence Ownership

Decision: ingress validates object id and sequence from the stream header.
Egress object id and sequence are assigned by the role/control layer and supplied
in `ExecuteStep.OutputBinding`.

Reason: the worker should not invent graph-visible ordering. The control plane
already knows the expected ordering and correlation.

### 17.6 Weight Loading

Decision: weight/persistent-state specifics are deferred with role provisioning.
For now, weights can be loaded as ordinary ingress objects and passed to
`ExecuteStep`, or a fixed-role worker can load them during its own initialization
from app-specific config.

Reason: this avoids designing binding APIs before the role provisioning path
exists.

### 17.7 Multi-Port And Fan-In

Decision: rings bind to ports, and `ExecuteStep` lists all input handles and
output rings explicitly. Fan-in is represented at the role layer by waiting for
multiple `ObjectLoaded` events before issuing one `ExecuteStep`.

Reason: the ring remains SPSC and byte-level simple. Join semantics stay out of
the ring and pump layers.

### 17.8 GPU OOM And Allocation Pressure

Decision: ingress allocation failure faults the ring in the MVP. Step-time OOM
fails the step. If an egress object was partially committed, the egress ring is
faulted.

Reason: skipping a payload or continuing after a partial object requires more
protocol machinery and can violate stream alignment. Replanning or retry belongs
above the worker boundary.

### 17.9 Error Separation

Decision: use separate failure classes for worker fatal errors, ring faults,
object failures, release failures, and step failures.

Reason: recovery action differs. A malformed object is not the same as a process
crash, and a tinygrad exception is not the same as a corrupt ring cursor.

### 17.10 tinygrad Range-Copy Gap

Decision: the worker implementation must provide a `DeviceBridge` with range
copy support. If tinygrad does not expose the necessary primitive directly, the
bridge must use a native backend helper or tinygrad raw-buffer API. The ring
protocol must not be weakened to require `extent <= ring_capacity`.

Reason: the ring spec explicitly allows objects larger than rings. Partial
prefix copy is required for liveness and backpressure.

### 17.11 Restart Semantics

Decision: restart creates a new worker generation and invalidates all device
handles, role instances, ring installs, and in-flight steps. The node may reuse
the same arena only after normal ring teardown proves quiescence.

Reason: GPU backend state is process-local. Treating restart as transparent
would risk stale handles, leaked DMA, and mismatched role state.
