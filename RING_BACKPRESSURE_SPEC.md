# Ring-Backpressured Data Movement - Canonical Specification

**Status:** design specification. This document supersedes the previous root
workflow drafts for edge establishment, blob streaming, driver streams, stream
transport, and arena management:

- `EDGE_ESTABLISHMENT.md`
- `BLOB_STREAMING.md`
- `STREAM_TRANSPORT.md`
- `DRIVER_STREAMS.md`
- `ARENA_MANAGEMENT.md`

`DESIGN_DIRECTIVES.md` remains steering context. This file is the buildout
specification.

**Scope.** Node-local host memory, process-crossing rings, worker loading,
edge establishment, persistent QUIC streams, actor/control messages, safety
contracts, backpressure, and teardown. This covers activations, weights, and
other large objects moving between disk, network, host memory, and GPU memory.

**Out of scope.** Compute overlap with partial tensors, placement policy,
churn placement, trustless verification, object-store semantics, and high-level
model graph scheduling. This spec permits transfer and VRAM upload to overlap
with network streaming. It deliberately does not permit matmuls or other compute
to observe an object until the whole logical object is loaded.

---

## 1. Shape

A node is an async backpressure machine built from one stable host-memory arena
and many bounded byte rings.

The arena is address space. It is one large sparse `memfd`, created by the Rust
node process, mapped once by the Rust node process, and mapped once by the GPU
worker process. It does not define flow control and it does not define object
ownership.

Rings define flow control. Every payload-moving boundary is represented as a
bounded single-producer/single-consumer byte ring backed by a lease inside the
arena:

- QUIC ingress stream -> host ingress ring -> worker -> GPU memory
- GPU memory -> worker -> host egress ring -> QUIC egress stream
- disk reader -> host ring -> worker/GPU memory
- future GPU download/upload and network paths follow the same ring contract

Actors establish, supervise, and tear down rings. Actors do not move payload
bytes, do not relay per-range readiness, and do not track per-byte ownership.
Once a ring is active, the hot path is shared ring metadata plus coalesced wake
hints:

```text
producer writes bytes into ring
producer advances commit cursor
producer sends/coalesces a readable wake hint

consumer wakes
consumer drains every committed byte it can use
consumer advances consume cursor
consumer sends/coalesces a writable wake hint if space was released
```

The wake hint is a signal that ring state may have changed. It carries a
`ring_id` and a reason such as readable or writable, not byte counts or ranges.
The producer may send it as soon as bytes are committed; it does not wait for the
ring to fill. Duplicate hints may be coalesced through a ready set or pending bit,
but a ring that has become readable or writable must remain discoverable until
the other side reloads the cursors. The cursors are the state.

The worker can load ingress into VRAM while the stream is still arriving. It does
that by greedily consuming the committed prefix of the ingress byte ring and
copying those bytes into the correct offsets of a device allocation. The object
becomes compute-visible only after all expected bytes have been copied and the
worker has observed object completion.

---

## 2. Vocabulary

**node process** - The Rust process hosting the swactor runtime, actors, driver,
arena manager, iroh endpoint, pump tasks, and worker supervisor.

**GPU worker** - A separate OS process, normally Python plus tinygrad and a small
native ring helper. It maps the arena, receives coalesced ring wake hints, parses
object streams, copies payload bytes into or out of GPU memory, and emits coarse
events.

**arena** - One sparse `memfd` reservation per node. The node process and worker
process map the same bytes. Arena offsets are stable for the node lifetime.

**arena lease** - A non-overlapping byte range in the arena assigned to one ring.
The ArenaManager mints leases and releases them after quiescence.

**ring** - A bounded single-producer/single-consumer byte stream backed by an
arena lease. A ring has shared metadata, data bytes, one producer, one consumer,
and coalesced wake hints.

**edge** - A one-way typed conduit from one role to another role. An edge has one
producer node, one consumer node, one `edge_id`, and one persistent QUIC
uni-stream once data begins flowing.

**`edge_id`** - A run-global edge identifier assigned by the orchestrator. It is
the control-plane name of the edge and the fixed-width stream preamble used by
the receiver's driver demux.

**object** - One logical payload on an edge, such as an activation tensor, a
weight tensor, a token batch, or a model shard. Objects are sent as object
records inside the edge byte stream.

**extent** - The actual byte length of an object. It is a runtime fact and may be
smaller than the edge's maximum object capacity.

**object spec** - The role-known contract for objects on an edge: maximum extent,
dtype family, shape/layout rules, object kind, and any alignment requirements.
The worker uses this spec to turn raw bytes into a correctly shaped device
allocation.

**ring spec** - The size and operating parameters of a ring: data capacity,
alignment, optional host-pinning requirement, wake coalescing mode, and whether
the ring is ingress or egress.

**pump** - A driver-owned async task. A recv-pump copies QUIC bytes into an
ingress ring. A send-pump copies egress ring bytes onto QUIC. Pumps do not parse
payload objects after the `edge_id` preamble.

**Tx/Rx edge actors** - Small swactor actors representing the local edge end.
They hold edge identity and lifecycle state, receive coarse completion/fault
events, and integrate with the role layer. They do not receive per-byte or
per-range messages.

---

## 3. Process Topology

Each node has two payload-relevant OS processes:

```text
node process (Rust)                         GPU worker process
-------------------                         ------------------
swactor runtime                             tinygrad / CUDA
ArenaManager actor                          ring parser
EdgeEstablisher actor                       device allocator
Tx/Rx edge actors                           host-to-device / device-to-host copies
Driver mailbox
iroh endpoint
recv/send pumps

          shared memfd arena, mapped by both processes
          actor/control messages for install, wake hints, and coarse events
```

Host-to-device means copying from arena-backed host memory into GPU memory.
Device-to-host means copying from GPU memory into arena-backed host memory.

The node process creates the `memfd` without `CLOEXEC` before spawning the worker,
or otherwise passes the fd explicitly during worker startup. The worker maps the
same reservation once. Neither process remaps the arena during node lifetime.

For a process-crossing ring:

- `RingReadable { ring_id }` wakes the consumer after the producer commits bytes.
- `RingWritable { ring_id }` wakes the producer after the consumer releases bytes.

For ingress, the producer is the Rust recv-pump and the consumer is the worker.
For egress, the producer is the worker and the consumer is the Rust send-pump.

The worker control pipe carries lifecycle messages, wake hints, and coarse events
such as `InstallRing`, `RingInstalled`, `ObjectLoaded`, and `RingFault`. It does
not carry payload bytes, byte counts, or per-range ownership.

---

## 4. Arena Manager

The ArenaManager is the single per-node authority for arena layout.

### 4.1 Responsibilities

The ArenaManager owns:

- the arena `memfd`
- the node process mapping base pointer
- the reservation ceiling
- the arena free-list
- the pending lease queue
- the table of live ring leases

The ArenaManager does not own:

- any worker process
- any QUIC stream
- any pump task
- any object parser
- any payload byte

It never reads or writes payload bytes. It only mints stable offsets.

### 4.2 Boot

At node boot:

1. The ArenaManager creates the `memfd`.
2. It truncates it to a generous sparse ceiling.
3. It maps the whole reservation once in the node process.
4. It exposes the base pointer to the driver and ring constructors.
5. It makes the fd available to the worker process at spawn.

The reservation costs virtual address space. Physical pages are backed lazily by
the kernel when touched. The mapping is not moved or resized. Any offset minted
by the ArenaManager remains meaningful until the node shuts down.

### 4.3 FSM

```text
Booting
  on ConstructArena{ceiling}
    -> Ready if memfd, truncate, and mmap succeed
    -> Failed if any boot resource cannot be created

Ready
  on LeaseRing{request_id, requester, edge_id, direction, ring_spec}
    -> lease immediately and emit RingLeased if a range fits
    -> enqueue request if the request is satisfiable but no current range fits
    -> emit RingLeaseRejected if the ring_spec can never fit in the ceiling

Ready
  on CancelLease{request_id}
    -> remove queued request if it has not been leased yet

Ready
  on ReleaseRing{ring_id, proof}
    -> return range to free-list
    -> retry queued leases serially

Ready
  on Shutdown
    -> ShuttingDown

ShuttingDown
  no new leases are accepted
```

### 4.4 Messages

Inbound:

```rust
ConstructArena {
    ceiling: u64,
}

LeaseRing {
    request_id: LeaseRequestId,
    requester: ActorAddress,
    edge_id: EdgeId,
    direction: RingDirection,
    ring_spec: RingSpec,
}

CancelLease {
    request_id: LeaseRequestId,
}

ReleaseRing {
    ring_id: RingId,
    proof: QuiescenceProof,
}

Shutdown
```

Outbound:

```rust
ArenaReady {
    base_ptr: NonNull<u8>,
    ceiling: u64,
}

RingLeased {
    request_id: LeaseRequestId,
    requester: ActorAddress,
    edge_id: EdgeId,
    ring_id: RingId,
    direction: RingDirection,
    arena_offset: u64,
    layout: RingLayout,
}

RingLeaseRejected {
    request_id: LeaseRequestId,
    requester: ActorAddress,
    edge_id: EdgeId,
    reason: LeaseRejectReason,
}
```

There is no temporary allocation-failure message. Temporary pressure is encoded
by absence of `RingLeased`: the request waits in the lease queue. Permanent
impossibility is explicit because no future release can make an oversized ring
fit. A queued request can be cancelled by `request_id` if the edge establishment
record stops before the lease is granted.

### 4.5 Correctness

Two live leases cannot overlap because all lease and release operations pass
through one ArenaManager mailbox. A handler mutates the free-list to completion
before the next handler runs. The allocator either removes one complete range
from the free-list and records it in the live table, or it leaves the free-list
unchanged and queues/rejects the request. There is no state in which a partial
lease is visible downstream.

A pump or worker cannot observe an unleased range because `RingLeased` is the
only message that contains a usable ring offset. Edge establishment does not
install a worker ring or spawn a pump until that message exists.

A range cannot be reused under a live pump or worker because `ReleaseRing` is a
proof, not a request. The EdgeEstablisher emits it only after the driver has
stopped the pump, the worker has uninstalled the ring, and in-flight DMA for the
ring has completed or been abandoned with the worker process dead. The
ArenaManager does not infer quiescence; it relies on the upstream teardown FSM
to earn the proof.

Queued lease requests cannot corrupt establishment because the requester receives
nothing while queued. No ring offset exists, so no driver or worker hot-path state
can be created for that ring. If a stop races with a grant, the
EdgeEstablisher accepts `RingLeased` only when the matching edge record is still
waiting on the same `request_id`; otherwise it releases the unused lease without
installing a worker ring or spawning a pump.

---

## 5. Ring Contract

A ring is a bounded SPSC byte stream in shared memory. It is the universal
payload handoff primitive.

The process-crossing ring is a fixed shared-memory ABI, not a Rust collection
placed inside the arena. In-process queues such as `crossbeam_queue::ArrayQueue`
may be used for local actor channels, ready sets, or wake scheduling, but the
arena ABI stores only offsets, cursors, state bits, and payload bytes. This keeps
the mapped bytes valid even when the node process and worker process map the
same `memfd` at different virtual addresses.

### 5.1 Single Producer, Single Consumer

Each ring has exactly one producer and one consumer.

Ingress:

```text
producer = recv-pump
consumer = worker
```

Egress:

```text
producer = worker
consumer = send-pump
```

Disk or future GPU rings follow the same rule. Fan-in or fan-out is represented
by multiple rings or by a higher-level mux/demux component that itself owns one
side of a ring. A ring never has multiple hot-path producers or consumers.

### 5.2 Shared-Arena ABI

The ring header lives in shared memory and is aligned for cross-process atomic
operations.

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

`ring_id` is unique for the node lifetime. Arena ranges may be reused after
quiescence, but ring identifiers are not reused. Stale control or wake events
therefore cannot alias a later ring that happens to occupy the same arena range.

`commit` is the first byte after the committed readable prefix. Bytes with
logical positions `< commit` are valid for the consumer to read.

`consume` is the first byte not yet released by the consumer. Bytes with logical
positions `< consume` are free for the producer to reuse.

The producer also keeps a local `write` cursor. `write` is the first byte after
the producer's reserved or in-progress write prefix. It is not shared with the
consumer because bytes in `commit..write` are not readable yet.

The readable interval is:

```text
consume .. commit
```

The producer-owned reserved interval is:

```text
commit .. write
```

Cursor values are monotonic logical byte positions. The physical byte index is:

```text
physical_index = cursor % capacity
```

The ring is empty when `consume == commit`. The ring is full when:

```text
write - consume == capacity
```

At producer startup for a fresh ring, `write == commit == consume`. After a
producer reserves a span for an async read or copy, `write` may be greater than
`commit`. The consumer still cannot read the reserved bytes because `commit` has
not advanced.

The `state` word records coarse lifecycle state such as active, closing, closed,
or faulted. The `wake` word records coalescing bits such as readable-pending and
writable-pending. Wake bits are hints for scheduling; `commit` and `consume` are
the ownership authority.

### 5.3 Native Helper Surface

Both Rust hot-path code and the Python worker access process-crossing rings
through the same native implementation. Python does not implement shared atomics,
wrap arithmetic, or cursor publication directly.

The helper exposes operations equivalent to:

```text
ring_readable_span(handle) -> ptr, len
ring_advance_consume(handle, len)
ring_writable_span(handle) -> ptr, len
ring_advance_commit(handle, len)
ring_state(handle) -> state
```

Returned pointers are process-local addresses derived from the caller's mapped
arena base plus arena offsets. The shared header never stores process-local
pointers.

### 5.4 Producer Rule

The producer may write only into free space:

```text
free = capacity - (write - consume)
```

Before writing, the producer acquires `consume`. It computes free space against
its local `write` cursor, reserves a contiguous physical span by advancing
`write`, and then writes into that reserved span. If the ring wraps, the producer
reserves at most to the end of the physical buffer, commits that prefix after it
is written, and then reserves the wrapped span.

After bytes are written, the producer stores the new `commit` with release
ordering and sends or coalesces a readable wake hint.

The producer may advance `commit` after any successful network read, disk read,
or GPU copy. It does not wait for a complete tensor, complete object, complete
range, or complete frame before committing newly valid bytes.

### 5.5 Consumer Rule

The consumer acquires `commit` and may read any byte in:

```text
consume .. commit
```

The consumer greedily drains all bytes it can make progress on. For the worker,
"make progress" means:

- parse complete control headers when enough bytes are available
- copy available payload bytes into the correct device allocation
- stop only when the ring is empty, the next header is incomplete, the target
  device allocation cannot currently accept more bytes, or the ring is faulted

After the consumer no longer needs a prefix of bytes, it stores the new `consume`
with release ordering and sends or coalesces a writable wake hint if the release
may unblock the producer.

If the consumer uses asynchronous DMA from host memory, it cannot release bytes
until the DMA no longer reads those bytes. The MVP uses synchronous copy or
event-tracked asynchronous copy that advances `consume` only after the copy event
completes.

### 5.6 Wakeups

Wakeups are edge-trigger hints, not state.

The producer sends `RingReadable { ring_id }` after advancing `commit` when the
consumer may be asleep. The consumer must still load `commit` from the ring
header, because several commits may have coalesced into one wakeup.

The consumer sends `RingWritable { ring_id }` after advancing `consume` when the
producer may be blocked on free space. The producer must still load `consume`
from the ring header, because several releases may have coalesced into one
wakeup.

Wake coalescing must preserve liveness. Dropping duplicate wake hints is allowed
only while a pending bit, ready-set entry, or equivalent durable scheduler state
still makes the ring discoverable. Losing the only transition from empty to
readable, or from full to writable, is a liveness bug even though it does not
corrupt memory. The cursors remain the source of truth for ownership.

### 5.7 Safety

The consumer cannot read unwritten bytes because the producer publishes
`commit` only after the bytes have been written, and the consumer reads `commit`
with acquire ordering before reading the bytes.

The producer cannot overwrite unread bytes because it computes free space from
the consumer-owned `consume` cursor and the producer-local `write` cursor. Bytes
at or after `consume` remain unavailable for reuse until the consumer advances
`consume`; bytes in `commit..write` are also unavailable because the producer has
reserved them but not yet published them.

Two consumers cannot both consume the same byte because a ring has one consumer
and only that consumer writes `consume`. Any design needing two consumers must
split the stream into two rings or insert an explicit fan-out component.

Wraparound cannot cause stale bytes to be mistaken for new bytes because
ownership is determined by monotonic logical cursors, not by physical indices.
The same physical index can be reused only after `consume` has advanced past the
previous logical byte range that occupied it.

Cursor overflow cannot occur during a node lifetime if the implementation treats
the `u64` logical cursor space as a runtime ceiling and tears the ring down before
approaching wrap. A ring carrying 1 TB/s would take centuries to exhaust `u64`
byte positions, so this is an operationally unreachable limit for the intended
workloads.

---

## 6. Object Stream Protocol

The ring carries a byte stream. The stream contains small object headers and raw
payload bytes. Headers define how the worker maps subsequent payload bytes into
VRAM. Payload bytes are not actor messages.

### 6.1 Edge Stream

Each edge uses one persistent QUIC uni-stream from producer node to consumer
node.

Wire shape:

```text
[edge_id preamble]
[object record]
[object record]
...
```

The receiver's edge-demux reader consumes the fixed-width `edge_id` preamble and
hands the stream to the driver rendezvous. After that preamble, the recv-pump is
byte-blind. It copies stream bytes into the ingress ring and advances `commit`.
The worker parses object records from the ring.

### 6.2 Object Record

An object record is:

```text
ObjectHeader
payload bytes, exactly header.extent bytes
```

The header is fixed-size in the MVP so the worker can parse it without heap
allocation:

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

The edge's `ObjectSpec` supplies dtype, shape family, max extent, layout, and
alignment. The header supplies runtime facts: which object this is, its sequence
on the edge, and its actual extent.

`extent` may be larger than the ingress ring capacity. That is normal. The ring
is a transfer window, not the object storage location.

### 6.3 Parsing Partial Records

Headers and payloads may arrive partially. The worker parser therefore has two
states per ingress ring:

```text
NeedHeader
NeedPayload{object_id, remaining, device_offset}
```

In `NeedHeader`, the worker waits until at least `header_len` committed bytes are
available. It may copy those header bytes into a small parser scratch buffer and
release them. Header bytes are control, not payload.

In `NeedPayload`, the worker copies any committed payload prefix into the target
device allocation. It does not wait for the full payload. If 60 percent of a
large object has arrived and the worker is free, it can copy that 60 percent to
VRAM, release the ring space, and let the recv-pump continue reading the
remaining 40 percent.

An object is complete when the worker has copied exactly `extent` payload bytes
for that object and any required copy-completion event has fired.

### 6.4 Validation

The worker rejects a record before allocating or exposing a device object if:

- `magic` or `version` is not supported
- `extent > ObjectSpec.max_extent`
- `extent` violates the edge's alignment/layout rules
- the sequence violates the edge ordering policy
- the ring closes before `extent` bytes arrive

These checks prevent malformed control bytes from becoming a visible tensor. The
payload content itself is trusted. The worker does not inspect tensor values.

---

## 7. Worker Design

The worker is the consumer of ingress rings and the producer of egress rings. It
is responsible for translating raw stream bytes into GPU-resident objects.

### 7.1 Worker Control Surface

The control pipe carries lifecycle messages:

```rust
InstallRing {
    ring_id: RingId,
    direction: RingDirection,
    layout: RingLayout,
    object_spec: ObjectSpec,
}

UninstallRing {
    ring_id: RingId,
}

AbortObject {
    ring_id: RingId,
    object_id: u64,
    reason: AbortReason,
}

RingReadable {
    ring_id: RingId,
}

RingWritable {
    ring_id: RingId,
}

Shutdown
```

Worker outbound control events and wake hints:

```rust
RingInstalled {
    ring_id: RingId,
}

ObjectLoaded {
    ring_id: RingId,
    edge_id: EdgeId,
    object_id: u64,
    device_handle: DeviceObjectHandle,
}

ObjectProduced {
    ring_id: RingId,
    edge_id: EdgeId,
    object_id: u64,
}

ObjectFailed {
    ring_id: RingId,
    edge_id: EdgeId,
    object_id: u64,
    reason: ObjectFailure,
}

RingFault {
    ring_id: RingId,
    reason: RingFaultReason,
}

RingQuiesced {
    ring_id: RingId,
}

RingReadable {
    ring_id: RingId,
}

RingWritable {
    ring_id: RingId,
}
```

There is no `ReadyRange`, `ConsumedRange`, `LandedRange`, or `FreeBytes` control
message. Wake hints name a ring, not a byte range. Per-range messages would put
the actor/control path back into the hot loop.

### 7.2 Ingress Worker FSM

Each ingress ring has an independent worker-side FSM:

```text
Uninstalled
  on InstallRing -> NeedHeader

NeedHeader
  on committed bytes < header_len -> wait
  on complete header -> validate and allocate device object -> NeedPayload
  on invalid header -> Faulted

NeedPayload
  on committed payload bytes -> copy greedy prefix to device
  on copied bytes == extent -> ObjectComplete
  on stream/ring abort -> Faulted

ObjectComplete
  wait for outstanding copy completion
  emit ObjectLoaded
  -> NeedHeader

Faulted
  emit RingFault or ObjectFailed
  stop consuming until control resolves or uninstalls the ring
```

The worker greedily drains. Wake delivery maintains a ready set, typically backed
by an in-process `ArrayQueue<RingId>` plus per-ring pending bits, so the worker
drains rings that were reported readable instead of scanning every installed
ring on each wake. It continues until no ready ring can make progress.

### 7.3 Device Assembly

On a valid `ObjectHeader`, the worker creates an assembly record:

```rust
struct ObjectAssembly {
    object_id: u64,
    sequence: u64,
    extent: u64,
    object_spec: ObjectSpec,
    device_allocation: DeviceAllocation,
    bytes_copied: u64,
    copies_in_flight: CopyTracker,
}
```

`ObjectSpec` tells the worker how to interpret the raw bytes:

- dtype width and dtype family
- shape rule for turning `extent` into rows/tokens/elements
- memory layout expected by the compute role
- alignment requirements
- maximum extent

For activations, the shape family is known at provisioning and `extent` selects
the runtime row/token count. For weights, the extent and shape may be fixed by
the role or model shard. The worker does not deserialize through Python objects;
it creates or reserves a device buffer whose byte layout matches the role's
expected tensor layout.

The required primitive is a range copy into a device allocation:

```text
arena[ring_span] -> device_allocation[object_offset .. object_offset + len]
```

The worker may implement this through a lower-level tinygrad device buffer API,
a native CUDA helper, or another backend-specific range-copy primitive. A
high-level API that only supports "copy this entire host buffer into this entire
tensor" is insufficient for ingress streaming, because the object may be larger
than the host ring and the worker must copy partial committed prefixes.

### 7.4 Host Memory and DMA Safety

If the worker uses synchronous host-to-device copies, it may advance `consume`
immediately after the copy call returns.

If it uses asynchronous copies, it may advance `consume` for a byte range only
after the copy no longer depends on that host memory. With pinned host memory,
that means recording the CUDA/event backend completion and releasing the range
after the event fires. Without this rule, the recv-pump could overwrite a ring
span still being read by DMA. The overwrite cannot occur when `consume` is held
back until copy completion, because producer free space is computed from
`consume`.

### 7.5 Compute Visibility

The role/compute layer receives an object only after `ObjectLoaded`.

`ObjectLoaded` is emitted after:

1. a valid object header was parsed
2. exactly `extent` payload bytes were copied into the device allocation
3. all copy events for those bytes completed
4. the resulting device allocation was associated with the role's expected dtype,
   shape, and layout

Because no compute-visible handle exists before `ObjectLoaded`, compute cannot
observe a partially loaded object.

---

## 8. Driver and Transport

The driver is the node's swactor-to-iroh boundary. It owns the endpoint,
connection cache, edge demux, and pump tasks.

### 8.1 Connection Model

The node uses one iroh endpoint. Edge streams use a dedicated ALPN, for example:

```text
swactor/edge/1
```

Connections are cached per `(peer_node_id, ALPN)`. A QUIC connection is the
transport object that performs a real handshake. A uni-stream is opened
unilaterally by the sender and is cheap relative to the connection.

All edges between the same node pair and ALPN reuse the same connection. Each
edge has one persistent uni-stream within that connection.

### 8.2 Driver Mailbox

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

Driver outbound actor events:

```rust
DriverEdgeReady {
    edge_id: EdgeId,
}

StreamClosed {
    edge_id: EdgeId,
}

StreamFault {
    edge_id: EdgeId,
    reason: StreamFaultReason,
}

PumpStopped {
    edge_id: EdgeId,
    ring_id: RingId,
}
```

There are no per-object, per-range, or per-buffer-fragment driver mailbox messages in the
hot path.

### 8.3 Receive Demux Rendezvous

A recv-pump needs two resources:

1. local receive establishment state, including the ingress ring
2. the arriving QUIC stream

They can arrive in either order. The driver stores both halves:

```text
recv_specs: HashMap<EdgeId, RecvSpec>
pending_streams: HashMap<EdgeId, RecvStream>
```

On `EstablishRecv`, if a pending stream exists, the driver spawns the recv-pump.
Otherwise it stores the spec.

On `StreamArrived`, if a recv spec exists, the driver spawns the recv-pump.
Otherwise it stores the stream.

The orchestrator therefore does not need an inter-end readiness handshake. If a
stream arrives before the receiver is locally established, it waits in
`pending_streams`. Because no recv-pump reads from it yet, QUIC flow control
eventually stalls the sender instead of dropping bytes.

### 8.4 Recv-Pump FSM

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

The recv-pump is byte-blind after stream demux. It does not parse `ObjectHeader`
and it does not know where object boundaries are. Its only correctness
responsibility is to copy bytes into free ring space and publish `commit` after
those bytes are valid.

Because the recv-pump is byte-blind, it does not classify EOF as object-aligned
or mid-object. It reports stream closure. The worker parser classifies the close
against its per-ring parser state: EOF with no partial record is a clean stream
close; EOF while a header or payload is incomplete is an object failure.

If the worker is slow, `consume` stops advancing. The recv-pump computes no free
space, stops reading QUIC, and waits. Since the recv-pump stops reading the
stream, QUIC's stream flow control stalls the remote sender. No actor credit
protocol is needed.

### 8.5 Send-Pump FSM

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

If the network is slow, the send-pump stops advancing `consume`. The worker then
runs out of free egress ring space and stalls before producing more outbound
bytes. This is egress backpressure.

---

## 9. Edge Establishment

Establishment is local actor setup plus transport rendezvous. The two remote edge
ends do not exchange actor messages with each other.

### 9.1 Orchestrator

The orchestrator owns graph placement. For each edge, it sends local provision
messages to the producer node and consumer node:

```rust
ProvisionTx {
    edge_id: EdgeId,
    rx_node_id: NodeId,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
}

ProvisionRx {
    edge_id: EdgeId,
    object_spec: ObjectSpec,
    ring_spec: RingSpec,
}
```

The producer needs the consumer's `node_id`, not the consumer's actor address.
The consumer needs the shared `edge_id`, not the producer's actor address.

### 9.2 EdgeEstablisher Records

Each node has one EdgeEstablisher actor. The actor itself stays live and able to
receive new messages. It does not enter `WaitingForLease` globally. Instead, it
owns a table of per-edge establishment records:

```rust
edge_records: HashMap<EdgeId, EdgeRecord>
ring_to_edge: HashMap<RingId, EdgeId>

struct EdgeRecord {
    edge_id: EdgeId,
    direction: RingDirection,
    state: EdgeProvisionState,
    lease_request_id: Option<LeaseRequestId>,
    ring_id: Option<RingId>,
    local_edge_actor: ActorAddress,
}
```

The record FSM is:

```text
New
  on ProvisionTx/ProvisionRx
    spawn local Tx/Rx edge actor
    create LeaseRequestId
    send LeaseRing{request_id, ...} to ArenaManager
    -> WaitingForLease

WaitingForLease
  on RingLeased matching request_id
    record ring_id
    send InstallRing to WorkerCtl
    -> WaitingForWorkerRing
  on RingLeaseRejected matching request_id
    notify local edge actor failure
    -> Failed
  on StopEdge
    send CancelLease{request_id}
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
    notify local Tx/Rx edge actor Ready
    -> Ready
  on StreamFault/RingFault/StopEdge
    -> Stopping

Ready
  hot path runs without this actor
  coarse ObjectLoaded/ObjectProduced/StreamFault events may pass through

Stopping
  stop pump if one exists
  uninstall worker ring if installed
  wait for quiescence proofs
  release ring if leased
  -> Stopped
```

An edge cannot become `Ready` without a ring lease because the record transition
out of `WaitingForLease` requires a `RingLeased` carrying the same
`LeaseRequestId`.

An edge cannot spawn a pump for an unmapped worker ring because
`EstablishSend/Recv` is sent only after the same record observes `RingInstalled`.

If a stale `RingLeased`, `RingInstalled`, `RingFault`, or `PumpStopped` arrives
for a record that has already stopped or for a ring id no longer present in
`ring_to_edge`, the EdgeEstablisher ignores it except for releasing an unused
fresh lease that was granted after cancellation raced with allocation.

### 9.3 Tx and Rx Edge Actors

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

They receive:

```rust
EdgeReady { edge_id }
ObjectLoaded { edge_id, object_id, device_handle }
ObjectProduced { edge_id, object_id }
ObjectFailed { edge_id, object_id, reason }
StreamFault { edge_id, reason }
StopEdge { edge_id }
```

They do not receive:

- bytes
- host pointers to payload
- per-range readiness
- per-range consumed events
- free-space events

This keeps actor execution deterministic and bounded by coarse workflow events,
while ring cursors handle the high-frequency byte path.

### 9.4 Race Freedom

A sender may open its stream before the receiver has completed local
establishment. This does not lose bytes because the receiving driver demux stores
the stream by `edge_id` until `EstablishRecv` supplies a ring. The stream is not
read until the recv-pump exists. Unread QUIC streams apply transport flow control
to the sender.

A receiver may establish before the sender opens its stream. This does not need a
remote ack because the receive spec waits in the driver demux table. When the
stream arrives, `edge_id` pairs the two halves.

The two sides do not need each other's actor addresses because data-plane routing
is `(node_id, edge_id)`: the sender dials the consumer node and writes `edge_id`
as the stream preamble; the receiver demuxes by `edge_id`.

---

## 10. Ingress Flow

Ingress is the path:

```text
remote worker/disk/GPU -> remote egress ring -> QUIC -> local ingress ring
    -> local worker -> local GPU memory -> ObjectLoaded
```

### 10.1 End-to-End Sequence

On the receiving node:

1. The driver demux reads the stream's `edge_id` preamble.
2. The driver rendezvous pairs the stream with the local ingress ring.
3. The recv-pump waits for free ring space.
4. The recv-pump reads QUIC bytes directly into the ring's free span.
5. The recv-pump advances `commit` after each successful read.
6. The recv-pump sends or coalesces `RingReadable { ring_id }`.
7. The worker wakes and reads `consume..commit`.
8. The worker parses object headers as soon as enough committed bytes exist.
9. The worker allocates the target device object after validating the header.
10. The worker copies every committed payload prefix it can into VRAM.
11. The worker advances `consume` after copies are safe to release.
12. The recv-pump sees free space and continues reading.
13. When all bytes for an object are copied, the worker emits `ObjectLoaded`.

### 10.2 Partial Ring Fill

If an object is larger than the ingress ring, the ring may fill with only a
prefix of the object. This is normal.

Example:

```text
object extent = 10 GiB
ingress ring = 512 MiB

recv-pump fills 512 MiB and stalls
worker receives a readable wake and copies committed bytes to VRAM
worker advances consume
recv-pump resumes and reads the next bytes
```

The pipeline remains live because the worker consumes the committed prefix, not
completed ranges. The object is not compute-visible until the worker has copied
all 10 GiB and emitted `ObjectLoaded`.

### 10.3 Why No Per-Range Actor Messages Are Needed

The worker does not need `Rx` to tell it that bytes landed. `RingReadable` and
the ring cursors already provide that information at the process boundary. The
worker does not need `Rx` to return buffer space. Advancing `consume` releases
bytes to the producer; a coalesced `RingWritable` wake only tells the producer to
reload the cursor.

Removing per-range actor messages is correct because actor state is not the
authority for ring ownership. The producer and consumer cursors are the
authority. An actor message would be a slower duplicate of state the worker can
read directly.

---

## 11. Egress Flow

Egress is the path:

```text
local GPU memory -> local worker -> local egress ring -> QUIC
    -> remote ingress ring -> remote worker/GPU
```

### 11.1 Sequence

1. The worker receives or creates a compute result in GPU memory.
2. It creates an `ObjectHeader` according to the edge's `ObjectSpec`.
3. It waits for free egress ring space.
4. It writes header bytes into the egress ring and advances `commit`.
5. It copies payload bytes from the device object into free egress ring spans.
6. It advances `commit` as host bytes become valid.
7. It sends or coalesces `RingReadable { ring_id }`.
8. The send-pump wakes and opens the persistent edge uni-stream on first bytes.
9. The send-pump writes the `edge_id` preamble once.
10. It writes committed egress bytes to QUIC.
11. It advances `consume` after bytes have been accepted by `write_all`.
12. It sends or coalesces `RingWritable { ring_id }`, allowing the worker to
    produce more bytes.

### 11.2 Egress Backpressure

If QUIC or the remote receiver is slow, `write_all` stops completing. The
send-pump cannot advance `consume`. The egress ring fills. The worker eventually
finds no free space and stops copying more bytes out of GPU memory.

This cannot overwrite egress bytes because the worker computes free space from
the send-pump-owned `consume` cursor. Until the send-pump advances `consume`,
those bytes remain owned by the send-pump.

---

## 12. Backpressure Model

There are no credits, no RTS/CTS, and no per-range acknowledgements.

Backpressure is always absence of writable ring space.

### 12.1 Worker Slow on Ingress

```text
worker slow
  -> consume does not advance
  -> recv-pump sees no free ingress space
  -> recv-pump stops reading QUIC
  -> QUIC flow control stalls sender
  -> sender send-pump stops draining its egress ring
  -> sender worker eventually stalls on egress free space
```

No data is dropped because each layer stops before overwriting unread bytes.

### 12.2 Network Slow on Egress

```text
network slow
  -> send-pump write_all remains pending
  -> egress consume does not advance
  -> worker sees no free egress space
  -> worker stops copying more outbound bytes
```

No actor credit protocol is needed because the bounded ring and QUIC flow control
already encode the pressure.

### 12.3 Arena Pressure

```text
arena temporarily exhausted
  -> ArenaManager queues LeaseRing
  -> the EdgeEstablisher edge record remains WaitingForLease
  -> no worker ring is installed
  -> no pump is spawned
  -> no hot-path state exists for that edge
```

When a ring is released, the ArenaManager retries queued leases. Allocation
pressure is establishment backpressure.

---

## 13. Teardown and Churn

Teardown must prove quiescence before releasing an arena lease.

### 13.1 Teardown FSM

For one edge:

```text
Ready
  on StopEdge or fault
    -> StoppingPump

StoppingPump
  driver stops recv/send pump
  driver emits PumpStopped
  -> StoppingWorkerRing

StoppingWorkerRing
  WorkerCtl sends UninstallRing
  worker removes the ring from ready queues
  worker waits for in-flight copies or abandons them by process death
  worker emits RingQuiesced
  -> ReleasingArena

ReleasingArena
  EdgeEstablisher sends ReleaseRing{proof}
  ArenaManager returns lease to free-list
  -> Stopped
```

### 13.2 Why Release Is Safe

The ArenaManager can safely reuse the range after `ReleaseRing` because the proof
requires both hot-path owners to be gone:

- the driver pump has stopped, so no Rust task will write/read the ring bytes
- the worker has uninstalled the ring or died, so no worker loop will read/write
  the ring bytes
- copy events have completed or the process owning them is gone, so no DMA will
  read/write the ring bytes

Since every live user of the lease is stopped before the release message is sent,
the next lease cannot alias a live user.

### 13.3 Churn

Churn replaces edges. It does not mutate a live edge into a different peer.

A replacement edge gets a new `edge_id`, a new establishment sequence, and either
a fresh ring lease or a reused lease after the old edge proves quiescence. This
keeps old streams, old ring cursors, and old object sequence numbers from merging
with replacement state.

---

## 14. Failure Handling

### 14.1 Boot Failure

If `memfd_create`, `ftruncate`, or `mmap` fails, the node does not enter steady
state. No arena offsets have been minted, no rings exist, and no payload state can
be corrupted.

### 14.2 Oversized Ring Request

If a `RingSpec` can never fit in the arena ceiling, the ArenaManager emits
`RingLeaseRejected`. The edge fails before worker install or pump spawn. Since no
ring offset is emitted, no hot-path state can reference invalid memory.

### 14.3 Temporary Arena Exhaustion

Temporary exhaustion queues the lease. The edge waits before establishment. This
does not lose data because no stream pump has been created for the edge. If a
remote stream arrives early, the receiver's driver holds it in `pending_streams`
and QUIC flow control stalls the sender until local establishment catches up.

### 14.4 Malformed Object Header

The worker faults the object if header validation fails. It does not allocate a
compute-visible object. Since compute receives only `ObjectLoaded`, malformed
objects cannot be consumed by the role layer.

### 14.5 EOF Mid-Object

If the stream closes before `extent` bytes are copied, the worker emits
`ObjectFailed`. The driver emits `StreamClosed` for transport EOF or
`StreamFault` for a read error; it does not decide whether the close was
object-aligned. The partially allocated device object is discarded. It is not
exposed because completion requires exactly `extent` copied bytes and copy
completion.

### 14.6 Worker Crash

If the worker process exits, WorkerCtl marks installed rings faulted and asks the
driver to stop their pumps. The ArenaManager does not release leases until the
worker process is reaped and the driver reports `PumpStopped`. The arena itself
survives worker restart because it is owned by the node process.

### 14.7 Pump Failure

If a pump fails, the driver emits `StreamFault` and stops touching the ring. The
worker is told to uninstall the matching ring. The arena lease is released only
after both sides quiesce.

---

## 15. Implementation Requirements

### 15.1 Shared Atomics

Ring cursors must be accessed with atomic acquire/release semantics across the
process boundary. The Python worker must use the native ring helper for ring
header access, span calculation, cursor publication, and wake coalescing. Python
may own parser and tinygrad logic, but it does not directly implement
cross-process atomic cursor operations.

### 15.2 Alignment

Ring headers must be aligned for atomic cursor operations. Ring data must be
aligned to the largest required host-copy and device-copy alignment for the
backend. Alignment is a performance and DMA requirement; non-overlap correctness
still comes from ArenaManager leases.

### 15.3 Host Pinning

The MVP may use pageable host memory and synchronous copies. Host pinning is the
path to efficient asynchronous DMA. If host memory is pinned, it must be pinned
per ring lease and unpinned only after ring quiescence. A pinned range cannot be
returned to the free-list while a backend may still DMA from it.

### 15.4 Object Layout

The object byte layout must be fixed by `ObjectSpec`. The worker may parse
headers and validate sizes, but it does not reinterpret or transform payload
values. Payload bytes are copied into device memory in the layout the compute
role expects.

### 15.5 Persistent Streams

Each edge uses one persistent stream, not one stream per object. The `edge_id`
preamble is written once. Object records follow back-to-back. This avoids
per-object stream allocation and preserves the ring's natural pipeline.

---

## 16. Invariants and Why They Hold

**Arena offsets remain valid.** The arena mapping is created once and never moved
or resized. Offsets are relative to that mapping and remain meaningful until node
shutdown.

**Live rings do not overlap.** The ArenaManager serializes all lease/release
operations through one mailbox and records every live lease. A range is removed
from the free-list before `RingLeased` is emitted and returned only after
`ReleaseRing`.

**Stale ring events cannot alias replacement rings.** `ring_id` values are unique
for the node lifetime. Churn may reuse arena ranges after quiescence, but it does
not reuse the identifier that control messages and wake hints carry.

**The worker cannot read bytes before the recv-pump writes them.** The recv-pump
writes bytes first, then advances `commit` with release ordering. The worker
loads `commit` with acquire ordering and never reads beyond it.

**The recv-pump cannot overwrite bytes still needed by the worker.** Free space is
computed from the worker-owned `consume` cursor and the producer-local `write`
cursor. The worker advances `consume` only after it has parsed/copied the bytes
and, for DMA, after the copy no longer depends on that host memory. Until then,
the producer's free-space calculation cannot include that physical range.

**Wakeups do not own data.** Wakeups carry no ownership information. The ring
cursors are durable shared state. A consumer that wakes late still sees all bytes
in `consume..commit`. Liveness requires the wake implementation to preserve a
pending ring in a ready set, pending bit, or equivalent durable scheduler state
until the other side reloads the relevant cursor.

**Partial objects cannot reach compute.** The only compute-visible event is
`ObjectLoaded`, and the worker emits it only after the full declared extent has
been copied into device memory and copy completion is known.

**A stream can arrive before local receive establishment without dropping bytes.**
The driver demux stores the stream by `edge_id`. It does not read the stream
payload until a recv-pump exists. QUIC flow control stalls the sender if buffers
fill while the stream is pending.

**Actor scheduling cannot corrupt payload state.** Actors do not own payload
bytes or per-byte cursors. The hot-path state is in ring atomics owned by exactly
one producer and one consumer.

**A released arena range has no live users.** `ReleaseRing` is sent only after the
driver pump has stopped, the worker ring has quiesced, and copy lifetimes have
ended. The ArenaManager reuses ranges only after that proof.

---
