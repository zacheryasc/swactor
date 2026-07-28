# Data Plane Actor Architecture Specification

**Status:** implemented architecture contract for the `data-plane` crate.

This document describes the behavioral boundary between reusable data-plane actors
and MVP-specific orchestration/runtime code. It is intentionally architectural: it
names responsibilities, actor roles, message families, and ownership boundaries
without prescribing file layout or migration steps.

---

## 1. Purpose

`data-plane` owns the behavior required to move model objects between stages and
between a node process and its local GPU worker process.

The crate defines the actor protocol for:

- provisioning logical data edges;
- distinguishing network/wire edges from node-local IPC rings;
- leasing, installing, readying, faulting, stopping, quiescing, and releasing
  local rings;
- binding logical edges to transport endpoints and local worker rings;
- parsing, validating, sequencing, loading, producing, and reporting objects;
- gating readiness and object visibility on the correct lifecycle transitions;
- translating low-level arena, transport, and worker observations into coarse
  data-plane outcomes.

`mvp-system` uses `data-plane` as a reusable actor subsystem. It provides run
intent, concrete runtime actor addresses, and MVP-specific report sinks. It does
not own the fine-grained data-plane state machine.

---

## 2. Core boundary

Swactor actors carry control, lifecycle, and identity messages. Payload bytes do
not move through actor mailboxes.

Payload bytes move through:

- arena-backed shared-memory rings for node-local IPC;
- transport byte streams for node-to-node data edges;
- GPU-worker-owned device allocations for compute-ready objects.

The data-plane actors decide when these byte paths are established, readable,
writable, faulted, stopped, and safe to release. Runtime adapter actors execute
concrete effects and report observations back.

---

## 3. Edge and ring model

### 3.1 Wire edge

A wire edge is a logical run-plan connection between a producer endpoint and a
consumer endpoint.

It carries:

- run identity;
- edge identity;
- producer and consumer node identity;
- edge kind, such as token input, activation, or token output;
- object contract;
- transport contract;
- optional remote endpoint and remote actor identity.

A wire edge answers: "which logical data stream connects these stage endpoints?"

It does not answer: "which local arena offset or worker-process ring is being
used on this node?"

### 3.2 Local IPC ring

A local IPC ring is a node-local buffer used by a Rust node process and its owned
GPU worker process.

It carries:

- ring identity allocated by the local arena manager;
- arena layout and capacity;
- worker-process generation;
- local role port, such as input or output;
- direction relative to the worker process;
- object contract installed into the worker;
- quiescence and release state.

A local IPC ring answers: "how does this node exchange bytes with its local
worker process for a specific edge endpoint?"

It does not answer: "which remote node or distributed route owns the other side
of the logical edge?"

### 3.3 Binding

A wire edge endpoint may bind to zero or more local resources depending on its
role:

```text
inbound wire edge endpoint
  -> recv transport endpoint
  -> local worker ingress ring
  -> device object handle

outbound wire edge endpoint
  -> local worker egress ring
  -> send transport endpoint

local-only edge endpoint
  -> local producer/consumer binding
  -> optional worker ring
```

The binding is owned by data-plane state. MVP code may observe the binding only
through coarse reports such as edge ready, object loaded, object produced, edge
faulted, and edge stopped.

---

## 4. Actor topology

The target topology is actor-oriented.

### 4.1 Data-plane node actor

One data-plane node actor owns the data-plane state for one local node within one
active run.

It owns:

- local node identity;
- run-scoped edge table;
- mapping from wire edge endpoints to local rings;
- mapping from ring ids to edge endpoints;
- object sequence state;
- device-handle visibility state;
- data-plane child actor addresses;
- MVP report sink addresses.

It receives provisioning intent from MVP code and observations from runtime
adapters. It emits actor messages to arena, worker, transport, and MVP report
sinks.

### 4.2 Wire edge actor

A wire edge actor owns the lifecycle of one logical edge endpoint on the local
node.

It owns:

- provisioning state;
- transport establishment state;
- send/receive pump readiness;
- stream faults;
- logical edge readiness;
- stop and fault propagation for that edge endpoint.

It does not own worker-process state or arena layout details except through a
binding supplied by the data-plane node actor or ring actor.

### 4.3 Local worker ring actor

A local worker ring actor owns one local IPC ring lifecycle.

It owns:

- arena lease request and result;
- worker ring installation;
- worker ring readable/writable notifications;
- ring fault and quiescence observations;
- release proof collection;
- arena lease release.

It does not own remote endpoint routing. It can be bound to a wire edge endpoint
by edge id, but the ring lifecycle remains local.

### 4.4 GPU worker control adapter

The GPU worker control adapter is the actor-facing boundary to the owned Python
worker process.

It owns or fronts:

- worker process generation;
- command serialization to the worker;
- stdout/stderr event parsing;
- device handle generation checks;
- worker stop/crash/restart observations.

The data plane treats this as an actor endpoint. Worker-process JSON and Python
helper details are not exposed to MVP stage logic.

### 4.5 Transport adapter actors

Transport adapter actors own concrete wire byte movement.

They own or front:

- accepted edge streams;
- outbound edge streams;
- edge preamble validation;
- byte read/write readiness;
- transport-specific stream faults;
- pump stop observations.

The data plane treats transport events as observations on a wire edge. Transport
actors do not decide stage readiness or object admission.

### 4.6 MVP report sink

The MVP report sink receives coarse data-plane outcomes and maps them to
MVP-specific control messages.

Examples:

- inbound edge ready;
- outbound edge ready;
- object loaded for stage execution;
- object produced for downstream transport;
- edge faulted;
- local edges stopped.

The sink does not inspect ring cursors, arena leases, worker generations, or
transport pump internals.

---

## 5. Actor API surface

Concrete Rust names are schematic. The contract is the message shape and
ownership boundary.

### 5.1 Provisioning input

MVP sends provisioning intent to the data-plane node actor:

```text
ProvisionDataPlaneRun {
    run_id,
    local_node_id,
    arena_actor,
    worker_actor,
    transport_actor,
    report_sink,
}

ProvisionWireEdgeEndpoint {
    run_id,
    edge_id,
    direction,
    edge_kind,
    local_role_port,
    local_node_id,
    peer_node_id,
    peer_endpoint,
    object_spec,
    transport_spec,
    local_ring_spec,
}
```

`direction` is relative to the local node's stage role: inbound means the local
stage consumes objects from the edge; outbound means the local stage produces
objects to the edge.

Provisioning is declarative. MVP describes the intended edge endpoint and the
actors available to execute effects. It does not prescribe lease/install/driver
ordering.

### 5.2 Runtime observations

Runtime adapters report observations back to data-plane actors:

```text
ArenaRingLeased
ArenaRingLeaseRejected
ArenaRingReleased
ArenaRingReleaseRejected

WorkerReady
WorkerRingInstalled
WorkerRingFaulted
WorkerRingQuiesced
WorkerRingReadable
WorkerRingWritable
WorkerObjectLoaded
WorkerObjectProduced
WorkerObjectFailed
WorkerStopped
WorkerFaulted

TransportEdgeReady
TransportBytesReceived
TransportBytesSent
TransportStreamClosed
TransportStreamFaulted
TransportPumpStopped
```

Observations are facts, not commands. The data plane decides the next state and
any follow-up messages.

### 5.3 Data-plane effects

The data plane sends effect requests to runtime adapter actors:

```text
LeaseArenaRing
CancelArenaRingLease
ReleaseArenaRingLease

InstallWorkerRing
UninstallWorkerRing
NotifyWorkerRingReadable
NotifyWorkerRingWritable
LoadObjectFromWorkerRing
ExecuteWorkerStep
ReleaseWorkerDeviceObject

EstablishWireSend
EstablishWireRecv
WriteWireObject
StopWirePump
```

Effects are actor messages. The receiving adapter owns the concrete mechanism:
memfd/mmap, JSON stdin/stdout, process supervision, iroh streams, or test doubles.

### 5.4 Data-plane reports

The data plane reports only stable semantic outcomes to MVP:

```text
InboundEdgeReady { edge_id }
OutboundEdgeReady { edge_id }
ObjectLoaded { edge_id, object_id, sequence, device_handle }
ObjectProduced { edge_id, object_id, sequence, extent }
EdgeFaulted { edge_id, reason }
EdgeStopped { edge_id }
LocalEdgesStopped { run_id }
WorkerDataPlaneFaulted { reason }
```

Reports are the only data-plane messages MVP stage/orchestrator actors should
need for normal stage progression.

---

## 6. Behavior owned by data-plane

### 6.1 Edge establishment

For each provisioned edge endpoint, data-plane actors own the establishment
sequence.

Inbound endpoint:

```text
provision endpoint
  -> lease local ingress ring
  -> install ring into worker input port
  -> establish receive transport if the edge is remote
  -> report inbound edge ready
```

Outbound endpoint:

```text
provision endpoint
  -> lease local egress ring when worker output is required
  -> install ring into worker output port
  -> establish send transport if the edge is remote
  -> report outbound edge ready
```

Readiness is reported only after every required local and wire resource for that
endpoint is ready. A local-only endpoint may omit transport establishment. A
wire-only endpoint may omit worker-ring establishment when it terminates outside
the local GPU worker.

### 6.2 Object ingress

For inbound data, data-plane actors own object admission.

The data plane:

- associates incoming bytes with the correct wire edge and stream;
- validates object framing and object spec constraints;
- preserves sequence ordering required by the edge contract;
- writes or exposes the object through the local ingress ring;
- asks the worker to load the object to device;
- waits for a valid worker object-loaded observation;
- reports object loaded to MVP only after the device handle is current and the
  logical object is complete.

MVP does not parse object headers, track ingress buffers, reload cursors, or gate
object-loaded visibility.

### 6.3 Object egress

For outbound data, data-plane actors own object production and forwarding.

The data plane:

- receives compute/output observations from the worker;
- binds produced objects to the correct outbound edge and sequence;
- validates object extent and object contract;
- publishes readable/writable state to the worker and transport actors;
- forwards complete object records on the wire when the edge is remote;
- reports object produced or step-visible outcomes to MVP at semantic
  boundaries, not cursor boundaries.

MVP does not decide when a local output ring is readable, when a transport stream
should consume it, or when a produced object is safe to expose downstream.

### 6.4 Faults

The data plane owns data movement fault classification and propagation.

Fault sources include:

- arena lease rejection or release rejection;
- worker ring installation failure;
- worker ring fault;
- malformed object framing;
- sequence violation;
- worker object load/produce failure;
- transport stream read/write/protocol failure;
- pump stop before quiescence;
- stale worker generation or device handle.

A fault on one edge endpoint must not silently corrupt another endpoint. The data
plane maps local faults to edge-scoped or worker-scoped reports, starts the
required stop/quiescence path, and emits the appropriate MVP report.

### 6.5 Stop, quiescence, and release

The data plane owns teardown ordering for local resources.

For a bound edge/ring pair, stop requires:

```text
stop transport pump if present
  -> uninstall or quiesce worker ring if installed
  -> prove no local reader/writer still uses the ring
  -> release arena lease
  -> report edge stopped
```

Arena release must be gated by quiescence proof. MVP may request run or edge
stop, but it does not supply low-level release proof or decide when a ring is
safe to release.

---

## 7. MVP-system utilization

`mvp-system` remains responsible for MVP orchestration and stage semantics.

It owns:

- run planning and edge assignment;
- stage provisioning authority;
- membership/readiness gates outside the data plane;
- weight loading and role configuration intent;
- stage controller behavior;
- prompt injection and token consumption;
- mapping data-plane reports to MVP lifecycle messages;
- selecting concrete runtime adapters for arena, worker process, and transport.

For data movement, MVP code acts as a client:

1. Spawn or obtain actor addresses for the data-plane node actor and required
   runtime adapters.
2. Send run and edge provisioning intent to the data-plane node actor.
3. Forward runtime observations from concrete adapter actors when those adapters
   are MVP-owned.
4. Receive coarse data-plane reports.
5. Translate those reports into stage-controller or orchestrator messages.

MVP must not rely on private edge states such as waiting-for-lease,
waiting-for-worker-ring, waiting-for-driver, pump-stopped, ring-quiesced, or
release-ready. Those are data-plane implementation states.

---

## 8. Required invariants

- `EdgeId` names a logical run-plan edge, not a local ring allocation.
- `RingId` names a local arena-backed IPC ring, not a distributed edge.
- A ring may be bound to an edge endpoint, but the identifiers are not
  interchangeable.
- Actor messages carry control and identities, not tensor payload bytes.
- Object-loaded reports are emitted only for complete, validated objects with
  current-generation device handles.
- Edge-ready reports are emitted only after required wire and local IPC resources
  are ready.
- Arena lease release is gated by local quiescence proof.
- Worker process generation is part of every device-handle validity decision.
- Transport faults and worker faults are classified by data-plane before they
  become MVP reports.
- MVP stage logic observes semantic outcomes, never ring cursor mechanics.

---

## 9. Non-goals

`data-plane` does not own:

- global run planning;
- placement optimization;
- model layer assignment;
- weight download or weight loading semantics;
- prompt tokenization or output token policy;
- membership convergence;
- provider provisioning;
- concrete iroh endpoint construction;
- concrete Python helper implementation.

The crate defines reusable actor protocols and data-movement behavior. Concrete
runtime adapters may live beside MVP code, inside reusable support crates, or in
tests, as long as they satisfy the actor contracts above.
