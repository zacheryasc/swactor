# Distributed Actor Runtime: Implementation Plan

## Overview

Two-layer distributed system:

1. **SWIM/Lifeguard membership** — node discovery, failure detection, membership gossip.
2. **Kademlia-style actor directory** — decentralized `actor_id → node` lookup with signed entries, quorum reads, Byzantine-tolerant up to `n ≤ f < 2f + 1`.

Key properties: 256-bit actor IDs, fixed placement (no migration), forwarding on cache miss is acceptable, node count << actor count, actor count unbounded.

```
┌──────────────────────────────────────────────────┐
│  Node                                            │
│                                                  │
│  Local Registry ─ LRU Cache ─ Directory Shard    │
│                                                  │
│  ──── Kademlia Routing Table (256 k-buckets) ──  │
│                                                  │
│  ──── SWIM Membership Layer ───────────────────  │
│                                                  │
│  ──── Transport (pluggable) ───────────────────  │
└──────────────────────────────────────────────────┘
```

### Core Invariants

- `node_id` = ed25519 public key (identity + signing key in one).
- Directory entries are signed by the spawning node. Replication factor `r = 2f+1`, quorum reads require `f+1` agreement.
- SWIM membership is a per-node CRDT: higher generation wins, within a generation `dead > suspect > alive`.

---

## Existing Infrastructure (what we're building on)

### Already implemented

- **`ActorAddress([u8; 32])`** — 256-bit actor identity, random generation, serde support. Lives in `src/actor.rs`.
- **Transport layer** (`src/transport.rs`, feature-gated `transport`):
  - `Transport` trait — `fn send(&self, envelope: WireEnvelope) -> Result<(), Error>`
  - `Codec<M>` trait — user-provided encode/decode per message type
  - `NetworkMessage` trait — marker with `type_tag()` for wire routing
  - `WireEnvelope { dest: ActorAddress, type_tag: String, payload: Vec<u8> }`
  - `CodecRegistry` — type-erased encoder/decoder dispatch (TypeId → encode, type_tag → decode)
  - `TransportRouter` — address→transport mapping (`RwLock<HashMap<ActorAddress, Arc<dyn Transport>>>`)
  - `InMemoryTransport` — in-process transport via mpsc
  - `send_via_transport()` — crate-internal helper wiring codec+router
  - TCP transport example with length-prefix framing in `examples/tcp_ping_pong.rs`
- **Delivery integration** (`src/delivery.rs`):
  - `TickContext::route_nonlocal()` — tries inbox registry → transport router → error
  - Message routing already falls through to transport when address is not local
- **Gossip crate** (`crates/swactor-gossip/`):
  - LWW key-value gossip (NOT SWIM membership — different protocol)
  - Full-push gossip (sends entire state each round)
  - Simulation harness with topologies: Ring, Star, FullMesh, Chain, Partitioned
  - Event tracing, snapshots, property-based tests
  - Gossip + runtime dashboards (`crates/gossip-dashboard/`, `crates/runtime-dashboard/`)

### What still needs building

- `NodeId` type (ed25519 public key) — distinct from `ActorAddress`
- ed25519 crypto primitives (keypair gen, sign, verify)
- `DirectoryEntry`, `NodeRecord` types
- SWIM membership protocol (probes, failure detection, dissemination)
- Piggyback field on `WireEnvelope` for SWIM dissemination
- Kademlia routing table and lookup
- Actor directory (STORE / FIND_VALUE with quorum)
- Node-level integration type
- Connection pooling and bidirectional TCP listener

---

## Workflow

Each chunk: **Think** (understand constraints), **Plan** (design interfaces), **Act** (implement and test).

After each chunk: `git add -A && git commit -m "<chunk summary>"`.

---

### Chunk 0: Core Types and Crypto

Define `NodeId` (ed25519 public key wrapper), `Keypair`, `Signature`, `DirectoryEntry`, `NodeRecord`. Leverage existing `ActorAddress` as-is for actor identity. Add `ed25519-dalek` dependency. Implement sign/verify. Unit test serialization round-trips and signature correctness.

**New crate**: `crates/swactor-distribution/` — keeps distribution concerns out of the core runtime.

**Types to define**:
- `NodeId([u8; 32])` — ed25519 public key, XOR distance for Kademlia
- `Keypair` — ed25519 signing key + public key
- `Signature([u8; 64])` — ed25519 signature
- `NodeRecord { node_id, addr: SocketAddr, generation: u64 }` — SWIM membership record
- `DirectoryEntry { actor_addr: ActorAddress, node_id: NodeId, generation: u64, signature: Signature }` — signed actor→node binding
- `MemberState { Alive, Suspect, Dead }` — SWIM state enum

**Files**: `crates/swactor-distribution/src/{lib.rs, types.rs, crypto.rs}`

```bash
git add -A && git commit -m "chunk-0: distribution crate, core types, crypto primitives"
```

---

### Chunk 1: Transport Extensions

Extend the existing transport layer for distribution needs. The `Transport` trait, `Codec`, `WireEnvelope`, `CodecRegistry`, and `TransportRouter` already exist — this chunk adds what's missing for node-to-node communication.

**Changes**:
- Add optional `piggyback: Vec<u8>` field to `WireEnvelope` for SWIM dissemination (backward-compatible: empty vec = no piggyback)
- Promote the TCP transport from the example into a reusable `TcpTransport` in the distribution crate, with connection pooling (`HashMap<SocketAddr, TcpStream>`) and a listening accept loop
- Add `request()` to `Transport` trait (send + await response) — needed for SWIM probes and Kademlia lookups
- Register distribution message codecs (`Ping`, `PingReq`, `Ack`, `FindNode`, `Store`, `FindValue`) in a `DistributionCodecRegistry`

**Files**: `crates/swactor-distribution/src/{transport.rs, codec.rs}`, modifications to `src/transport.rs` (piggyback field)

```bash
git add -A && git commit -m "chunk-1: transport extensions for distribution"
```

---

### Chunk 2: SWIM Probes

Implement the SWIM probe cycle as a state machine in the distribution crate. This is pure protocol logic, testable without networking.

**Components**:
- `SwimProbe` state machine: periodic random-order pinging, `PingReq` indirect probes on timeout
- `MemberList` — the membership CRDT: `HashMap<NodeId, (MemberState, incarnation: u64)>`
- State transitions: `Alive → Suspect → Dead`, with incarnation-based refutation (suspected node bumps incarnation to refute)
- `SwimConfig` — probe interval, probe timeout, suspicion timeout

**Key design**: The probe logic is a pure function `(current_state, event) → (new_state, actions)` where actions are messages to send. This makes it testable without real networking — reuse the simulation pattern from `swactor-gossip`.

**Files**: `crates/swactor-distribution/src/{swim/mod.rs, swim/probe.rs, swim/member_list.rs}`

```bash
git add -A && git commit -m "chunk-2: SWIM probe cycle and failure detection"
```

---

### Chunk 3: SWIM Dissemination

Membership changes piggyback on existing protocol messages — no separate gossip channel. This builds on the `piggyback` field added in Chunk 1.

**Components**:
- Dissemination queue: list of `(MembershipUpdate, transmit_count)` entries
- Infection-style counting: each update transmitted `Λ * log(n)` times before eviction
- Priority ordering: `dead > suspect > alive` (most urgent first)
- Piggyback packing: serialize top-N updates into the piggyback field of outgoing messages
- Piggyback unpacking: on receive, extract and apply membership updates before processing the primary message

**Reuse**: The `swactor-gossip` simulation harness (topologies, tracing) can validate dissemination convergence. Consider adapting the property tests.

**Files**: `crates/swactor-distribution/src/swim/dissemination.rs`

```bash
git add -A && git commit -m "chunk-3: SWIM piggybacked dissemination"
```

---

### Chunk 4: SWIM Join Protocol

Implement seed-node bootstrap and dynamic cluster formation.

**Components**:
- `JoinRequest` / `JoinResponse` messages
- New node contacts seed(s), receives current member list, is announced via dissemination
- Solo-node case: first node starts with empty member list, becomes its own seed
- `SwimNode` — the integrated SWIM actor: probe timer + dissemination + join/leave

**Files**: `crates/swactor-distribution/src/swim/join.rs`, update `swim/mod.rs`

```bash
git add -A && git commit -m "chunk-4: join protocol and seed node bootstrap"
```

---

### Chunk 5: Kademlia Routing Table

Pure data structure, no network calls. Implement as a standalone module.

**Components**:
- 256-entry k-bucket array indexed by `XOR(self_id, target_id).leading_zeros()`
- XOR distance metric on `NodeId` (256-bit)
- Per-bucket LRU eviction: prefer long-lived nodes, new nodes wait in replacement cache
- `closest(target: NodeId, count: usize) -> Vec<NodeId>` — k-closest query
- `insert(node_id)` / `remove(node_id)` with LRU maintenance

**Files**: `crates/swactor-distribution/src/kademlia/routing_table.rs`

```bash
git add -A && git commit -m "chunk-5: kademlia k-bucket routing table"
```

---

### Chunk 6: Kademlia Node Lookup

Iterative `FIND_NODE` using the `Transport::request()` method from Chunk 1.

**Components**:
- `NodeLookup` — async iterative walker: start from α closest local contacts, query in parallel, incorporate responses, converge on k-closest
- `FindNodeRequest { target: NodeId }` / `FindNodeResponse { closest: Vec<(NodeId, SocketAddr)> }` messages
- Lookup termination: all k-closest nodes queried, or max rounds exceeded

**Files**: `crates/swactor-distribution/src/kademlia/lookup.rs`

```bash
git add -A && git commit -m "chunk-6: iterative FIND_NODE lookup"
```

---

### Chunk 7: Actor Directory (STORE / FIND_VALUE)

The largest chunk. Signed directory entries with quorum reads.

**Components**:
- `DirectoryShard` — local storage of `HashMap<ActorAddress, Vec<DirectoryEntry>>`
- **STORE**: sign a `DirectoryEntry`, use FIND_NODE to locate the `r` closest nodes to the `ActorAddress`, store on all of them
- **FIND_VALUE**: quorum read — query `r` nodes, require `f+1` agreement on the same `(node_id, generation)`, verify signatures, highest-generation-wins conflict resolution
- Fallback: if quorum not met from initial `r` nodes, iterative walk to find more replicas

**Files**: `crates/swactor-distribution/src/kademlia/directory.rs`

```bash
git add -A && git commit -m "chunk-7: signed directory STORE and quorum FIND_VALUE"
```

---

### Chunk 8: Cache and Message Routing

Wire the directory into the existing routing pipeline in `src/delivery.rs`.

**Components**:
- LRU cache: `ActorAddress → NodeId` with bounded capacity and TTL
- Extended routing pipeline: local `AddressMap` → LRU cache hit → Kademlia FIND_VALUE → `Transport::send()`
- Redirect/forward on receiving side: if a message arrives for a non-local actor, look up the correct node and forward
- Cache invalidation: on delivery failure (transport error), evict the stale entry and re-resolve

**Integration point**: `TickContext::route_nonlocal()` currently tries inbox → transport. This chunk extends it to: inbox → cache → directory resolve → transport.

**Files**: `crates/swactor-distribution/src/cache.rs`, modifications to `src/delivery.rs`

```bash
git add -A && git commit -m "chunk-8: LRU cache and message routing pipeline"
```

---

### Chunk 9: Directory Republish and Repair

React to SWIM death notifications to maintain directory consistency.

**Components**:
- Wire SWIM `Dead` events into directory layer: when a node dies, identify affected directory entries and replicate to replacement nodes
- Periodic republish: spawning nodes re-STORE their entries on a timer to heal accumulated churn
- TTL-based expiration: entries whose host node is confirmed dead are expired after a grace period

**Files**: `crates/swactor-distribution/src/kademlia/repair.rs`

```bash
git add -A && git commit -m "chunk-9: directory republish and churn repair"
```

---

### Chunk 10: Node Integration

Compose SWIM + Kademlia + Transport + Cache into a single `DistributedNode` type.

**Components**:
- `DistributedNode` — public API: `start(config)`, `stop()`, `spawn(actor)`, `send(addr, msg)`, `members() -> Vec<NodeRecord>`
- Wraps a `Runtime` + `SwimNode` + `RoutingTable` + `DirectoryShard` + `LruCache`
- Startup sequence: generate keypair → bind transport → join cluster (SWIM) → populate routing table → ready
- Shutdown sequence: leave cluster (SWIM disseminate Dead for self) → drain in-flight messages → close transport
- End-to-end test: multi-node cluster, spawn actors, send cross-node messages, kill nodes, verify fault tolerance

**Files**: `crates/swactor-distribution/src/node.rs`, `crates/swactor-distribution/tests/integration.rs`

```bash
git add -A && git commit -m "chunk-10: node integration and public API"
```

---

### Chunk 11: Hardening (Lifeguard)

Add Lifeguard protocol extensions for production resilience.

**Components**:
- **Local Health Multiplier (LHM)**: degraded nodes (high nack rate, slow acks) increase their own probe interval to reduce false accusations
- **Dynamic suspect timeout**: scaled by `log(n)` where n = cluster size
- **Protocol period scaling**: under load, probe intervals stretch rather than dropping probes
- Stress tests: simulated partitions, asymmetric failures, high churn — reuse the `swactor-gossip` simulation harness patterns

**Files**: `crates/swactor-distribution/src/swim/lifeguard.rs`, stress test binaries

```bash
git add -A && git commit -m "chunk-11: lifeguard hardening and stress tests"
```

---

## Dependency Graph

```
[0] ─→ [1] ─→ [2] ─→ [3] ─→ [4] ─┐
              │                     │
              └─→ [5] ─→ [6] ─→ [7] ─┐
                                      ├─→ [8] ─→ [10] ─→ [11]
                                      │     │
                                      │    [9] ┘
                                      │
                             [4] ─────┘
```

Chunks 2-4 (SWIM) and 5-7 (Kademlia) can be developed in parallel off the transport extensions. Chunk 10 merges them. Chunk 11 is a hardening pass.

---

## Crate Layout

```
crates/swactor-distribution/
├── Cargo.toml           # deps: swactor, ed25519-dalek, serde
├── src/
│   ├── lib.rs
│   ├── types.rs         # NodeId, Keypair, Signature, NodeRecord, DirectoryEntry, MemberState
│   ├── crypto.rs        # sign, verify, keypair generation
│   ├── transport.rs     # TcpTransport (pooled), DistributionCodecRegistry
│   ├── codec.rs         # Codecs for all distribution messages
│   ├── cache.rs         # LRU actor location cache
│   ├── node.rs          # DistributedNode public API
│   ├── swim/
│   │   ├── mod.rs       # SwimNode actor
│   │   ├── probe.rs     # Probe cycle state machine
│   │   ├── member_list.rs  # Membership CRDT
│   │   ├── dissemination.rs  # Piggybacked gossip queue
│   │   ├── join.rs      # Seed-node bootstrap
│   │   └── lifeguard.rs # LHM, dynamic timeouts
│   └── kademlia/
│       ├── mod.rs
│       ├── routing_table.rs  # k-bucket array
│       ├── lookup.rs    # Iterative FIND_NODE
│       ├── directory.rs # STORE / FIND_VALUE with quorum
│       └── repair.rs    # Republish and churn healing
└── tests/
    └── integration.rs   # End-to-end multi-node tests
```
