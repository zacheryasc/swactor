# Design Directives


Captured design directives for the swactor data-movement / coordination-plane work.
These are the constraints and decisions as directed — not proposals. Resume from here.

## Problem & domain
- Building a **library for neural nets over heterogeneous, WAN-connected, churning hardware**.
- The thing being designed: how nodes **coordinate transfer/streaming of large blobs** — weights, activations, data — as a **reusable primitive**.
- **Nodes are trusted.** All trustless/P2P-style concerns (incentives, adversarial verification, Sybil, DHT discovery) are irrelevant.
- Focus is **ML workloads**, not general P2P.

## Scope discipline
- **Control plane first.** Performance (pre-alloc, buffering, chunked streams) is for later — *we don't know our bottlenecks yet*.
- The problem is **much smaller than what an object store solves** — don't over-engineer toward Ray/Plasma scope.
- Want **a solid set of abstractions that make designing distributed graphs over a WAN easy.**

## The library goal (ought) & lifecycle
- (For directional purposes only, we are only building one piece currently)
- Input: **a workload graph + a pool of hardware.** The library **distributes the work according to the graph across the hardware.**
- The meta lifecycle:
  1. **define graph and pool in top-level code**
  2. graph is **broken up per optimal placement** onto the extant resource pool
  3. nodes **establish edges/networking**
  4. nodes **fetch the weights/data they need**
  5. **data flows end-to-end**
  6. **teardown**, etc.

## Layers
- **Total compute graph** (e.g. inference over the transformer weights in a GGUF).
- **Roles created strategically** from **model shape + available hardware**.
- **Distribution of the model.**
- **Piping the data around.**

## Roles, graph, edges
- **Shape is owned by the graph, decided before roles are assigned.**
- A **role = a portion of the compute graph**: tensors flow in through the network boundary, get processed, new tensors flow back out.
- The graph is **known ahead of time** (pipelined inference). Nodes know ahead of time: they'll **send** activations, the **role they send to**, and that they'll **receive**.
- Each node knows: its **incoming tensors, outgoing tensors, and its own held weights.**
- The control plane specifies **size + shape + abstracted role endpoints** ("I am X sending Y to Z; I am Z receiving Y from X").
- **Graph-specifying code references a role abstractly** — not how the data reaches it or is received.
- Assume a **placement algorithm exists** (hand-defined at first, real later) — assignment/boundaries are already decided.

## swactor's mandate & boundaries
- **swactor is the runtime: the coordination plane for data movement and resource provisioning.**
- swactor actors are **strongly-typed FSMs passing messages**, to allow strict design of what the data looks like.
- The **CP is owned by an orchestrator** that: **runs and observes SWIM**, **handles resource provisioning**, and **stages execution on the nodes**.
- swactor actor paradigm = the **CP/dataflow layer**; **tinygrad = GPU execution.**
- **swactor does NOT handle the stream. iroh handles the streaming** — forget the nuts and bolts of the bytes on the wire.
- swactor is **not** doing flow control / credit / RTS-CTS — that solves a different problem, because **everything but the transport route is known ahead of time.**

## What's known vs. unknown
- **Known ahead of time:** the blob's **shape/dtype family** and its **`capacity`**
  (the per-chunk byte ceiling, `max_seq_len × hidden × dtype`), plus the abstracted
  endpoint (role).
- **Unknown at runtime:** the **transport** — who role X physically is, how to reach
  it — *and* each chunk's actual **`extent`** (e.g. the prompt length: prefill
  carries many rows, decode one), a per-step runtime fact `≤ capacity`.
- Runtime job: **resolve the transport problem** and bind the abstract role to a concrete route.

## swactor's concrete job (the gate)
- Assume over-the-wire streams are solved: **bytes travel fast and safe and arrive in a known buffer location.**
- **Receive side:** (zero-)deserialize the already-buffered bytes; **signal that we have the tensors** so they can be piped to the GPU.
- **Send side:** bytes are cooked from the GPU; swactor **populates a send buffer** and **resolves the sink.**
- What swactor tells the process (tinygrad) is **an actual location of the bytes on-device.**
- swactor **handles the stages** of this handoff, **not the actual movement.**
- **swactor actors are simple gates.**
- → Concretely realized in **The process-facing layer (stream & sink)** below.

## The process-facing layer (stream & sink)

Framing: we are designing the **typed send/receive infrastructure**, not the process.
The process view is the motivating lens — start from what the local compute sees, then
build the CP that drives it.

- **The role is a function:** typed chunks come in, get computed, typed chunks go out,
  through ports the process exposes. For the CP these ports are an **arbitrary address +
  handler space**; the first iteration uses one inbound and one outbound, but nothing
  assumes that count. Inbound and outbound ports are **independent** — not a coupled 1:1
  pass-through; the source and the sink in a single process are unrelated.
- **The process is driven, not autonomous.** It waits for the CP to stage a chunk,
  computes, hands the result back. (We lean toward a blocking pull as the natural shape
  for a single straight-line GPU worker, but that's a hedge, not a committed API — the
  load-bearing point is that the CP drives.)
- **A chunk is a typed tensor, not bytes** — fully parsed, sanitized, shaped, i.e.
  everything except the handoff to tinygrad and the GPU copy, which is the one step the
  process performs. Shape/dtype are already determined inside the process by its role;
  they aren't designed here, and we stay abstract about the type machinery for now.
- **The control plane handles ordering** and correlation between inbound and outbound
  chunks. The process tags and tracks nothing.
- **The sink carries its destination.** The process knows the next address and binds it
  to the sink — the sink, as a type/struct, has the *next address* built in. **Resolving
  that address, serializing the chunk, and streaming it out is the CP's job.** This is
  where we reach for **swactor's distributed address space**: the CP outside the process
  receives and parses typed chunks, feeds them into the local process (tinygrad + GPU),
  then on the way out resolves the sink's address and streams the outbound chunk.
- **Statefulness** (KV cache, position) is worker-internal and out of scope for this
  layer.
- **Handle/payload representation and zero-copy staging are deferred** — performance
  lives there, behind an unchanged process-facing surface.

This is an **abstract in-process surface** — a good vantage to design from, not a locked
API:
- an inbound typed-chunk source the process is driven from,
- an outbound typed sink the process pushes to, carrying its next address,
- both shaped by the role, both independent.

The CP's mandate from here: **receive + parse typed chunks into a process, and resolve +
serialize + stream typed chunks out of it.**

## End-to-end single pass (abstract)

Assumes SWIM converged and a start signal received. The pass is one repeating **edge**
(role A → role B) plus two ends; the **orchestrator is just another participant** (sink →
role0, source ← roleN), and **tokens are typed chunks** like activations.

- **Egress (role A, CP):** take the typed chunk from the sink; serialize → wire bytes;
  resolve the sink's abstract next-address → concrete route. Serialize and resolve are
  independent operations.
- **Transport:** stream bytes A → B (iroh; not swactor's concern).
- **Ingress (role B, CP):** reassemble → deserialize + validate against the role-known
  spec → stage → drive the local process.
- **Ends:** orch → role0 ships *tokens*; roleN → orch ships a *token*. Not special cases
  — edges whose endpoint is the orch and whose chunk type is tokens.

The open seam, designed next: **addressing** — binding the sink's abstract next-address to
a concrete remote endpoint.

## The edge: addressing, signals, and the byte boundary

How an edge is established and how chunks flow across it. Steady state assumes SWIM
converged and edges established.

**Addressing — orchestrator-direct, no inter-end handshake**

- An edge is a **pair of stream actors** (distinct from the process-driving actor): a
  `Tx` (send) and an `Rx` (receive). **Neither end knows the other's actor address.**
  The data plane is addressed by **`(node_id, edge_id)`**: the Tx sends to the
  consumer's node by its `node_id`, and the `edge_id` at the head of the stream
  demuxes it to the right `Rx`.
- **There is no inter-end negotiation** — the two ends never exchange a message; each
  is handed everything it needs at provisioning (the orchestrator owns placement).
  This still avoids per-chunk negotiation and the RTS-CTS/credit flow-control the
  directives rule out ("everything but the route is known ahead of time") — it just
  avoids the per-*edge* handshake too.
- The data endpoint is **handed down as the stable `node_id`** (the orchestrator knows
  it from placement); iroh resolves the live path from it — so there's no stale
  mapping to rot. (Churn is deferred; we design the happy case where both ends are
  resolvable.)
- **Byte-level backpressure is pushed into the streaming logic**, not the actor layer.
  Actors hold the edge; the streaming layer owns moving the bytes.

**The buffer-ownership baton & signals**

- The sink buffer is owned by either the process or the actor system at any instant;
  signals are the handoffs.
- Egress: process **`done`** (buffer filled) → egress actor hands `(buffer, endpoint)` to
  streaming → streaming **`released`** returns the slot to the alloc pool.
- Ingress: streaming **`landed`** → ingress actor inspects → **`ready`** drives the
  process.
- **`done` is non-blocking.** The process never blocks after signalling it produced a
  chunk. It blocks only on **alloc** (acquiring a send slot) and on **recv** (a chunk
  arriving). With a single buffer, chunk *k+1*'s alloc blocks until chunk *k*'s
  `released`. (Double-buffering deferred.)

**The byte boundary — egress trusts, ingress verifies**

- **Egress:** no actor-level parsing. The buffer is correct by construction, the endpoint
  is bound from setup, and the receiver knows how to decode (type known a priori). The
  egress actor hands the buffer straight to the streaming layer. **swactor touches zero
  bytes on egress.**
- **Ingress:** swactor enters the byte path only to **read/check, never to transform**.
  The **`Rx` and its edge service double as the inspector** — no new actor.
  - Behind the **stream abstraction (data-plane integrity):** the framed `[extent]`
    prefix is read and its `extent` bytes arrive complete (`extent ≤ capacity`).
    Size/length lives here; a short or torn frame means no `landed`.
  - The **ingress actor (control-plane gate):** given a complete chunk, clears it to
    drive the process (belongs to this edge, expected in sequence), then flags `ready`.
    The designated home for any sanity/terms check; thin in the happy path, but where
    checking lives so the process is never handed an unvetted chunk.
- Net swactor byte-contract, both sides: **never transforms payload bytes; reads them
  only to inspect, and only on ingress.**

**Open (not yet decided):** the depth of the ingress check — pure terms/sequence gate vs.
cracking the payload for a content-level (shape/dtype) sanity check before `ready`.

---

## Ingress check depth — decided (resolves "Open" above)

**Optimistic ingress: a chunk is accepted on its framed length alone.** If the
`[extent]` prefix reads cleanly, `extent ≤ capacity`, and that many bytes arrive
complete, they go to the process as-is — no peeking inside, no
deserialize-to-validate, no shape/dtype content check. Correct-by-construction
egress + a clean framed read on ingress is the entire gate. Nodes are trusted;
content trust is total.

## The transport, minimal

- Bytes move over **iroh** (already fixed). One **ordered, reliable stream per
  edge** is the whole mechanism. Striping, chunk hashing/verification, and resume
  are **deferred**.
- We are **not** building on the existing `crates/datastore/src/streams/` module —
  treated as not-ready; design fresh.

## The transfer actors — a reusable primitive

- An edge's ends are **two actor types: `Tx` (send) and `Rx` (receive)**, one per
  edge-end. They are a **general blob-moving primitive** — no notion of role,
  compute, or the graph.
- Each is **pre-told the blob's `capacity`** (a `BlobSpec`; the per-chunk byte
  ceiling — `dtype`/`shape` live in the role layer above, not the transport) and owns
  **zero-copy (de)serialization**: Tx views the producer's buffer as bytes (no
  transform); Rx views landed bytes back as a typed value. Rx **pre-allocates its
  landing buffer** from `capacity` at setup; each chunk's actual `extent ≤ capacity`
  varies per step and rides the wire as a length-prefix.

## Edge establishment — orchestrator-direct (contract #1)

- **No derived/hashed addresses, no gossip discovery, no polling.** The
  **orchestrator owns placement and wires edges directly** — it hands each end
  everything it needs. Addresses stay runtime-assigned (random); identities are
  *handed over*, never computed or discovered.
- **The data plane is addressed by `(node_id, edge_id)`.** The orchestrator hands the
  `Tx` its consumer's stable `node_id` (known from placement) in the provision
  message; iroh resolves the live path from it, so no static transport mapping can
  rot. There is no peer-to-peer endpoint exchange.
- **Per-node `Provisioner`** spawns the local `Tx`/`Rx`. The node's **edge service**
  (the ALPN-aware `IrohDriver`) demuxes incoming streams to the right `Rx` by the
  run-global **`edge_id`** at the head of the stream — not a separate `Listener`
  actor; the demux is a tokio task on the existing endpoint.
- **No inter-end handshake; race-free by a single barrier.** The two ends never
  exchange a message — each is fully equipped at provisioning, which fans out **in
  parallel** (no Tx-before-Rx ordering). Race-freedom is one barrier: a node acks
  `Provisioned` only after its `Rx` ends have **registered** their landing, and the
  orchestrator injects the prompt only after **every** node has acked — so no stream
  can arrive before its `Rx` is registered, with no per-edge ordering.
- **The establishment "exchange" is just the `edge_id` stream preamble (Tx→Rx).** No
  `EndpointOffer`, no `Ready`, no `EdgeReady`, no `tx_addr` relay. READY is a local
  terminal state: `Tx` is ready on spawn; `Rx` is ready once it has pre-allocated and
  registered its landing buffer.
- **Kickoff, not broadcast.** The orchestrator is just another participant; once all
  nodes are `Provisioned` it injects the driving prompt into role0 on its own
  outbound edge. Every other node derives its own state from arriving data.
- **Asymmetry:** `Tx` never needs `Rx`'s actor address — and now neither end needs
  the other's; they are coupled only by the shared `edge_id`.

## Still deferred (unchanged stance)

Churn/failure policy, teardown, the fan-in **join**, and the per-chunk zero-copy
baton (contract #4) remain out of scope. (The **start signal** is no longer here —
it's decided: there is none; see "Kickoff, not broadcast" above.)

---

# Blob streaming & host allocation

Detailing blob streaming + host allocation (contract #4). Full flow in
`BLOB_STREAMING.md` (draft).

## The host substrate — one sparse arena per node
- Blob bytes live on the host in a single `memfd` arena, mapped by **both** the
  node process and the Python GPU worker.
- The arena is reserved **big and sparse** (lazy tmpfs backing) and mapped **once**;
  the mapping is never moved. Edges are **regions sub-allocated** from it and
  returned on teardown, so topology is **dynamic without touching the fd**.

## The slot handoff
- Each edge owns a **ring of N `capacity`-sized slots** (default 2). A slot is owned
  at any instant by exactly one of {iroh, GPU worker}; ownership passes by **signal,
  two per direction**, over the existing stdin/stdout pipe. Actors never touch a
  payload byte; the node process is the sole authority on slot state.

## The GPU boundary
- tinygrad reads/writes slots **in place** via a `memoryview` (`copyin`/`copyout`);
  a blob **never enters Python's heap**. The host↔device DMA is the worker's only
  copy.

## Streaming
- **One long-lived iroh uni-stream per edge**; `edge_id` preamble once; then
  length-prefixed chunks `[extent: u32][extent bytes]`, so the **prefix is the
  frame** (each chunk's `extent ≤ capacity` varies per step; the slot is sized once
  to `capacity`). Refines establishment's per-call `open_uni` into a persistent
  stream, and its single landing buffer into the ring.

## Deferred to their own passes
- Host-pinning + removing tinygrad's CUDA `copyin` bounce (perf).

---

# Activation stream transport — the iroh ↔ swactor boundary

How activation tensors actually cross a READY edge, and how the swactor actors,
the iroh driver, and the GPU worker are wired to move them. Full spec in
`STREAM_TRANSPORT.md`. Scoped to activations (not gossip, not weights).

## The decision in one line
- **One persistent uni-stream per edge; the bytes ride it in place, in the arena;
  swactor passes only slot indices, never bytes.**

## Stream shape — persistent, length-framed
- **One long-lived uni-stream per edge**, not a stream per tensor. `edge_id`
  preamble once; then back-to-back length-prefixed tensors. Because each tensor's
  size varies per step (prefill many rows, decode one), each rides a fixed-width
  `u32` `extent` prefix — `[extent][extent bytes]`, `extent ≤ capacity` — and **the
  prefix *is* the frame**; the slot is sized once to `capacity`. Stream-per-message
  was rejected: it pays a task spawn + alloc + a `max_concurrent_uni_streams` slot
  per tensor and buys nothing.
- **QUIC owns reliability.** No app-level fragmentation (the ring already pipelines
  a whole-tensor object) and no striping (one connection over one path shares a
  single congestion window — striping needs multipath we don't have).

## Zero-copy — bytes never enter an actor
- Bytes live in the **shared arena** from the worker's `copyout` to the far
  worker's `copyin`, moved **in place**: the wire `read_exact`/`write_all`s arena
  slots directly. Huge tensors are never copied into a `Vec` or an actor message.
- **swactor moves slot indices (`usize`), not bytes.** The only payload-byte
  touchers are the **GPU worker** and a per-edge **byte-pump task**.

## Roles — tokio stays behind the driver wall
- **Driver** owns the endpoint, the connection cache, the `edge_id` demux, and
  **spawns/owns the byte-pump tasks** — the one place tokio lives, async byte
  readers and writers.
- **Reads are tokio-native** — `read_exact`/`write_all` only advance when polled on
  the runtime — so a byte-pump *task* is unavoidable while iroh is the transport.

## MVP
- **MVP = one dedicated byte-pump task per edge-end**, driver-owned. The
  **actor↔driver contract is slot-indices-in, slot-indices-up**, so the pump
  *mechanism* is a driver-internal detail. Toward removing tokio, it can later
  collapse to one-task-per-connection or to the node loop polling the stream
  futures — a driver refactor that **touches no actor**.
- **No single-threaded-tick assumption:** all cross-thread traffic is `deliver_raw`
  + the slot channels + actor isolation, so the design survives a multi-threaded
  runtime.

## Deferred
- The swactor ↔ GPU-worker pipe **mechanism** (async-Python rework) — its
  `ready/consumed/filled/drained` signals are fixed here, the transport is not.

---

# Remaining orchestration decisions before implementation

The first concrete workload is **sharded inference of large models across
prosumer GPUs**. Do not prematurely generalize this into a broad graph IR. The
next spec layer should describe only the workload/role shape needed for that use
case: model partitioning, role boundaries, edge object specs, weight/shard
ownership, and the runtime sequence for prefill/decode.

The **orchestrator remains the authority** for resource provisioning, placement,
and run staging. Nodes do not need to advertise capabilities after boot as part
of this design; the orchestrator provisions the pool and already knows the
resource inventory it is placing onto.

The missing spec surface is therefore:
- how the orchestrator decides roles and placement from model shape plus
  provisioned hardware,
- what a role provisioning message contains,
- how model weights/shards are assigned, fetched, loaded, and declared ready,
- what execution semantics the first inference path guarantees,
- and what behavioral contracts are required for reliable tests.

Observability should be added when the descriptive specs are converted into
behavioral contracts for testing. The goal is not just prose architecture, but
testable run behavior: provisioned, loaded, ready, object produced/consumed,
completed, faulted, and torn down.
