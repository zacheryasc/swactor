# Distribution Layer — Development History

> Covers all work after the TUI / agent-interface / stats-hook milestone.
> ~99 files changed · 8,152 insertions · 1,694 deletions
>
> *Note: this work was squash-merged into master as a single commit.
> The phases below reflect the logical development order on the feature branch.*

---

## Table of Contents

1. [Overview & Motivation](#1-overview--motivation)
2. [What Was Built](#2-what-was-built)
3. [Development Phases](#3-development-phases)
4. [Distribution Crate — Architecture](#4-distribution-crate--architecture)
5. [Distribution Crate — SWIM Implementation](#5-distribution-crate--swim-implementation)
6. [Distribution Crate — Kademlia Implementation](#6-distribution-crate--kademlia-implementation)
7. [Distribution Crate — Integration Layer (DistributedNode)](#7-distribution-crate--integration-layer-distributednode)
8. [Distribution Crate — Supporting Modules](#8-distribution-crate--supporting-modules)
9. [Simulation Framework](#9-simulation-framework)
10. [Dashboard Integration](#10-dashboard-integration)
11. [Crate Renames & Workspace Cleanup](#11-crate-renames--workspace-cleanup)
12. [Design Decisions & Tradeoffs](#12-design-decisions--tradeoffs)
13. [What's Unclear / Indeterminate](#13-whats-unclear--indeterminate)
14. [Known Gaps & Future Improvements](#14-known-gaps--future-improvements)
15. [Test Coverage Summary](#15-test-coverage-summary)

---

## 1. Overview & Motivation

Before this work, swactor was a single-node actor runtime with basic transport. Actors could be spawned, messaged, and monitored — but only within one process. The goal of the distribution layer is **cluster membership + distributed actor location** without depending on external coordination services (etcd, Consul, ZooKeeper).

Two classic distributed systems protocols were chosen:

- **SWIM** (Scalable Weakly-consistent Infection-style Membership) — for cluster membership and failure detection. Each node probes peers in constant-overhead rounds, piggybacking membership updates on protocol messages. Failures are detected within O(log n) protocol periods with tunable false-positive rates.

- **Kademlia** — for the actor directory (which node owns which actor). A DHT with XOR-distance routing, providing O(log n) lookup without a central registry. Entries are cryptographically signed (ed25519) so nodes can't forge actor locations.

The two protocols are independent: SWIM manages liveness ("who is in the cluster?"), Kademlia manages location ("where is actor X?"). A thin integration layer (`DistributedNode`) wires membership events into routing table updates and triggers repair/replication when nodes die.

---

## 2. What Was Built

| Component | Location | Source LOC | Test LOC | Files |
|-----------|----------|-----------|----------|-------|
| Distribution crate | `crates/distribution/` | ~3,045 | ~2,613 | 23 source + 13 test |
| Simulation crate | `crates/simulation/` | ~5,118 | (included) | 21 |
| Dashboard integration | `crates/runtime-dashboard/` | ~1,350 | — | 8 changed + 2 new |
| Crate renames | workspace-wide | — | — | 34 files touched |

**Distribution crate** (`crates/distribution/`): Full SWIM membership protocol (probe cycle, CRDT member list, piggybacked dissemination, Lifeguard extensions) + full Kademlia DHT (256-bucket routing table, iterative lookup, signed directory, repair/republish) + TCP transport with connection pooling + JSON codec + LRU location cache. 133 behavioral tests.

**Simulation crate** (`crates/simulation/`): Protocol-agnostic simulation harness with two implementations — a distribution simulation (SWIM cluster formation, actor resolution, fault injection) and a gossip simulation (migrated from the former `swactor-gossip` crate, feature-gated). 47 tests including 36 gossip property tests and 6 distribution scenario tests.

**Dashboard integration**: Distribution monitoring page (747-line HTML/JS with force-directed graph, Barnes-Hut quadtree layout, ego-centric node selection, 9 stat cards, real-time SSE updates). Trait-based provider decoupling. 554-line demo example with 9-node churn simulation.

**Crate renames**: `swactor-python` → `python`, `swactor-dp-mnist` → `dp-mnist`, `swactor-wasm` → `wasm`, `swactor-gossip` → absorbed into `crates/simulation/src/gossip/` (feature-gated), `gossip-dashboard` → `simulation`.

**Totals**: ~99 files changed, 8,152 insertions, 1,694 deletions.

---

## 3. Development Phases

The implementation plan (`distribution_plan.md`) defined 12 chunks (0–11). They were developed in 5 logical phases on the feature branch (squash-merged to master as one commit):

### Phase 1 — Distribution crate + simulation + crate renames
The bulk of the work (~6,900 lines).

Delivered plan chunks 0–11:
- Created `crates/distribution/` with the complete SWIM + Kademlia implementation (types, crypto, codec, transport, cache, messages, snapshot, node integration)
- Created `crates/simulation/` with distribution simulation harness + migrated gossip simulation
- Renamed crates: `swactor-python` → `python`, `swactor-wasm` → `wasm`, `swactor-gossip` absorbed into simulation
- 13 test files with 133 distribution tests + 47 simulation tests

### Phase 2 — Housekeeping: gossip feature-gate, renames, example fixes

- Feature-gated gossip module behind `gossip` feature in simulation crate
- Renamed `swactor-dp-mnist` → `dp-mnist`
- Renamed `gossip-dashboard` → `simulation-dashboard`
- Fixed examples broken by crate renames

### Phase 3 — Distribution snapshot accessors for dashboard consumption

- Added public accessors on `DistributedNode`: `entries()`, `all_nodes()`, `bucket_sizes()`, `recent_probe_targets()`
- Created `DistributionNodeSnapshot` — serializable point-in-time state for monitoring
- Added `snapshot()` method on `DistributedNode`

### Phase 4 — Distribution monitoring page in runtime-dashboard

- Added `distribution_collector.rs` — `DistributionStatsProvider` trait + generic `DistributionCollector<T>`
- Added `distribution_html.rs` — 747-line self-contained HTML/CSS/JS dashboard page
- Wired SSE `/events` stream to include `distribution` event type
- Feature-gated with `distribution` feature (default on)

### Phase 5 — Distribution dashboard demo example

- Added `dashboard_demo.rs` (later expanded to 554 lines) — 9-node cluster with runtime + distribution + dashboard
- Introduced `SnapshotProvider` decoupling pattern

---

## 4. Distribution Crate — Architecture

### Module Layout

```
crates/distribution/src/
├── lib.rs              (10 lines)  — module exports
├── types.rs            (178)       — NodeId, MemberState, NodeRecord, DirectoryEntry
├── crypto.rs           (84)        — ed25519 keypair, signing, verification
├── codec.rs            (52)        — JSON codec registry for 10 message types
├── transport.rs        (248)       — TCP with connection pooling, length-prefix framing
├── cache.rs            (96)        — LRU location cache (ActorAddress → NodeId)
├── messages.rs         (155)       — 10 protocol message types
├── snapshot.rs         (156)       — DistributionNodeSnapshot for monitoring
├── node.rs             (297)       — DistributedNode (top-level integration)
├── swim/
│   ├── mod.rs          (5)
│   ├── probe.rs        (357)       — Probe cycle FSM
│   ├── member_list.rs  (161)       — CRDT membership map
│   ├── dissemination.rs(134)       — Piggybacked update queue
│   ├── node.rs         (329)       — SwimNode (composition layer)
│   └── lifeguard.rs    (132)       — Health-aware timeout scaling
└── kademlia/
    ├── mod.rs          (4)
    ├── routing_table.rs(200)       — 256 k-buckets, LRU eviction
    ├── directory.rs    (157)       — Signed actor location storage
    ├── lookup.rs       (193)       — Iterative FIND_NODE state machine
    └── repair.rs       (97)        — Re-replication + periodic republish
```

### Dependency Structure

```
                    types.rs ◄─── crypto.rs
                       ▲
          ┌────────────┼────────────┐
          │            │            │
     messages.rs    codec.rs   transport.rs
          ▲            ▲
          │            │
    ┌─────┴─────┐     │
    │           │     │
swim/         kademlia/
    │           │
    └─────┬─────┘
          │
       node.rs  ◄─── cache.rs
          │
       snapshot.rs
```

SWIM and Kademlia are **independent** of each other. `node.rs` (DistributedNode) is the sole integration point where membership events from SWIM drive routing table updates in Kademlia.

### Core Design Pattern: Pure State Machines

Both protocols follow `(state, event) → (state, Vec<Action>)`. The state machine processes an input event, mutates internal state, and returns a list of actions the caller must execute (send messages, update timers, etc.). The state machine never performs I/O — the caller is responsible for dispatch.

This pattern makes every component independently testable without a runtime, networking, or timers.

---

## 5. Distribution Crate — SWIM Implementation

### 5.1 Probe Cycle — `swim/probe.rs` (357 lines)

The probe cycle is a three-phase finite state machine:

```
           ┌────────────────────────────────────┐
           │                                    │
           ▼                                    │
        Idle ──[tick]──► WaitingDirectAck ──────┤
                           │                    │
                        [timeout]               │
                           │                [ack received]
                           ▼                    │
                   WaitingIndirectAck ──────────┘
                           │
                        [timeout]
                           │
                           ▼
                     Suspect target
```

**Configuration** (`SwimConfig`):
- `probe_interval: u64` — ticks between probe cycles (default: 10)
- `probe_timeout: u64` — ticks to wait for direct ack (default: 3)
- `indirect_probes: usize` — number of relay nodes for indirect probing (default: 3)
- `suspicion_timeout: u64` — ticks before declaring suspected node dead (default: 30)

**Target selection**: Round-robin through members with XOR-based shuffle. When the probe index wraps, the member order is reshuffled. This ensures every member is probed before any is probed twice, while avoiding predictable patterns. The 16 most recent probe targets are tracked in a `VecDeque` for dashboard display.

**Suspicion timers**: Stored as `Vec<SuspicionTimer>` — `(NodeId, started_at)` tuples. Each tick, timers are checked; expired ones emit `DeclareDead` actions. If an ack arrives for a suspected node, the timer is cancelled.

**Inputs** (`SwimEvent`): `Tick`, `AckReceived { from, sequence }`, `IndirectAckReceived { target, sequence }`

**Outputs** (`SwimAction`): `SendPing`, `SendPingReq`, `Suspect`, `DeclareDead`, `Refute`

The probe logic is a pure function — `step(event, members) → Vec<SwimAction>` — with no I/O, no timers, no concurrency. The caller (SwimNode) translates actions into real network messages.

### 5.2 Member List — `swim/member_list.rs` (161 lines)

The member list is a CRDT with merge semantics based on incarnation numbers:

```
MemberList
  self_id: NodeId
  self_incarnation: u64
  members: HashMap<NodeId, MemberEntry>   // excludes self
```

**Merge rule** (in `apply()`):
1. Higher incarnation number always wins — replace entry regardless of state
2. Same incarnation, higher state priority wins — `Dead (2) > Suspect (1) > Alive (0)`
3. Lower incarnation number is ignored

This ensures convergence: all nodes eventually agree on the highest-incarnation state for each member.

**Incarnation refutation**: When a node receives a `Suspect` about itself, it increments `self_incarnation` and broadcasts `Alive` with the new incarnation. Since higher incarnation always wins, this overrides the suspicion at all nodes.

**Key methods**: `apply()` (merge), `suspect()` (Alive → Suspect), `declare_dead()` (any → Dead), `refute()` (bump self incarnation), `alive_members()`, `snapshot()` (for join responses).

### 5.3 Dissemination — `swim/dissemination.rs` (134 lines)

Membership updates are piggybacked on all SWIM protocol messages (pings, acks, ping-reqs) using infection-style counting.

**Transmit budget**: Each update is transmitted `Λ × ⌈log₂(n)⌉` times, where `Λ` (lambda) defaults to 3 and `n` is the cluster size. For a 10-node cluster, each update rides ~12 messages before expiring.

**Priority ordering**: When selecting which updates to piggyback (up to 8 per message), `Dead` updates are sent first, then `Suspect`, then `Alive`. This ensures failure information propagates fastest.

**Deduplication**: If a newer update for the same node arrives (higher incarnation, or same incarnation with higher-priority state), the old entry is replaced. This prevents stale information from consuming transmit budget.

**Wire format**: Updates are serialized to JSON bytes via `pack_piggyback()` and deserialized via `unpack_piggyback()`. The piggyback field is a `Vec<u8>` on every SWIM message.

### 5.4 SwimNode — `swim/node.rs` (329 lines)

SwimNode composes the probe cycle, dissemination queue, and member list into a unified interface.

**`NodeAction` enum** (6 variants):
- `SendPing { to, to_addr, sequence, piggyback }` — direct probe with gossip payload
- `SendPingReq { relay, relay_addr, target, target_addr, sequence, piggyback }` — indirect probe
- `SendAck { to, to_addr, sequence, piggyback }` — probe response
- `SendJoinRequest { to_addr }` — cluster bootstrap
- `SendJoinResponse { to, to_addr, members }` — membership snapshot for joiner
- `MembershipChanged { node_id, state, incarnation }` — notification hook for Kademlia wiring

**`MembershipChanged`** is the key integration point: DistributedNode listens for this action and translates it into routing table inserts/removes, cache invalidations, and repair queue entries.

**Join protocol** (one RTT):
1. Joiner calls `join([seed_addrs])` → emits `SendJoinRequest` to each seed
2. Seed receives `JoinRequest` → adds joiner to member list → enqueues for dissemination → responds with `SendJoinResponse` containing current member snapshot
3. Joiner receives `JoinResponse` → applies all members → cluster membership bootstrapped

**Graceful leave**: `leave()` enqueues a self-death update for dissemination. Other nodes receive the death notification through normal gossip and remove the departing node.

### 5.5 Lifeguard — `swim/lifeguard.rs` (132 lines)

Lifeguard implements three mechanisms from the Lifeguard paper (Hashicorp, 2018) to reduce false-positive failure detections under load:

**1. Local Health Multiplier (LHM)**: Tracks a health score (0 = healthy, up to `max_health_score` = 8). Each nack increments the score; each ack decrements it. The score translates to a multiplier (`1 + score`) that stretches probe intervals and timeouts. A degraded node probes less aggressively, giving itself more time to respond to others.

**2. Dynamic Suspect Timeout**: Scales with cluster size:
```
timeout = clamp(base × ⌈log₂(n + 1)⌉ × multiplier, min, max)
```
Default range: 15–120 ticks. Larger clusters get longer timeouts to accommodate higher message volumes.

**3. Protocol Period Scaling**: Probe interval and timeout are both multiplied by the health multiplier. Under load, the protocol slows down rather than dropping probes — this prevents cascading false suspicions.

**Status**: Lifeguard is fully implemented as pure computation but **not yet wired into `SwimProbe`**. The interface for feeding ack/nack events and reading dynamic timeouts is designed, but the connection point is missing. See [Section 13](#13-whats-unclear--indeterminate) for details.

---

## 6. Distribution Crate — Kademlia Implementation

### 6.1 Routing Table — `kademlia/routing_table.rs` (200 lines)

The routing table stores known nodes indexed by XOR distance from self.

```
RoutingTable
  self_id: NodeId
  buckets: Vec<KBucket>   // 256 buckets, one per bit of distance
  k: usize                // bucket capacity (default: 20)
```

**Bucket selection**: For a given `node_id`, compute `xor_leading_zeros(self_id, node_id)`. This gives an index 0–255 (clamped at 255). Bucket 0 contains nodes with the most-significant-bit different from self (farthest); bucket 255 would contain nodes with all bits matching (closest, essentially self).

**Insertion logic**:
- Node already in bucket → move to back (most-recent, LRU update)
- Bucket has space → add to back
- Bucket full → add to replacement cache (not main list)

**Replacement cache**: Each bucket maintains a secondary `VecDeque` of replacement candidates. When a node is removed from the main list (e.g., declared dead), the first replacement is promoted. This implements Kademlia's longevity bias — long-lived nodes are preferred because they're statistically more likely to remain alive.

**`closest(target, count)`**: Collects all nodes from all buckets, sorts by XOR distance to target, returns the `count` nearest. Used for lookup initialization and FIND_NODE responses.

**Design choice**: Static 256 buckets regardless of cluster size. Most high-index buckets are empty for small clusters, but the memory overhead is negligible (256 empty `VecDeque`s). This avoids the complexity of S/Kademlia's dynamic bucket splitting while maintaining correctness.

### 6.2 Directory — `kademlia/directory.rs` (157 lines)

The directory stores actor-to-node mappings with cryptographic signatures.

```
DirectoryShard
  entries: HashMap<ActorAddress, Vec<DirectoryEntry>>
```

**Multi-entry model**: Multiple nodes can claim the same actor address (e.g., during migration or replication). Each entry is signed by the claiming node's ed25519 key.

**Store logic** (`store(entry) → bool`):
1. Verify signature — reject if invalid
2. If node_id not already stored for this actor → push entry
3. If node_id already stored → replace only if `generation > existing.generation`

**Quorum resolution** (`resolve_quorum_entries(entries, quorum)`):
1. Group entries by `(node_id, generation)` pair
2. Verify all signatures
3. The group with the highest generation that has ≥ quorum entries wins
4. Returns `Resolved(entry)`, `NoQuorum(entries)`, or `NotFound`

**Death cleanup** (`remove_by_node(node_id)`): Removes all entries held by a dead node and returns them for re-replication via the repair queue.

### 6.3 Iterative Lookup — `kademlia/lookup.rs` (193 lines)

The lookup state machine implements Kademlia's iterative FIND_NODE algorithm:

```
NodeLookup
  target: NodeId
  known: HashMap<NodeId, (SocketAddr, [u8; 32])>   // distance cached
  queried: HashSet<NodeId>
  pending: HashSet<NodeId>
  round: usize
  done: bool
```

**Constants**: `ALPHA = 3` (concurrency), `K = 20` (replication), `MAX_ROUNDS = 20`.

**Algorithm**:
1. **Start**: Initialize `known` with k closest nodes from local routing table. Query the α closest.
2. **Each response**: Add newly discovered nodes to `known`. When all pending queries return, start next round.
3. **Next round**: Sort `known` by XOR distance. Pick up to α unqueried nodes from the k closest. Query them.
4. **Termination**: All k closest have been queried, OR no new nodes discovered in a round, OR MAX_ROUNDS exceeded.

**Outputs** (`LookupAction`): `Query { node_id, addr }` or `Done { closest: Vec<(NodeId, SocketAddr)> }`.

**Design choice**: The lookup is agnostic to FIND_VALUE vs. FIND_NODE — it always returns the k closest nodes. The caller interprets the result and issues the appropriate FIND_VALUE RPCs if looking for an actor. This keeps the state machine simple but means there's no early-termination optimization when the value is found during lookup (see [Section 13](#13-whats-unclear--indeterminate)).

### 6.4 Repair & Republish — `kademlia/repair.rs` (97 lines)

Two mechanisms maintain directory consistency under churn:

**`RepairQueue`** (reactive — on node death):
```
RepairQueue
  pending: HashMap<ActorAddress, DirectoryEntry>
```
When a node is declared dead, `on_node_death(dead_node, shard)` extracts all directory entries the dead node held and queues them for re-STORE on the next-closest node. The caller calls `drain()` to get entries and issue STORE RPCs.

**`RepublishTracker`** (proactive — periodic):
```
RepublishTracker
  local_actors: HashMap<ActorAddress, u64>   // generation
  interval: u64
  next_republish: u64
```
Tracks locally-spawned actors. On each `tick()`, if the republish interval has elapsed, returns all local actors for re-STORE. This counters topology drift: as nodes join and leave, the "r-closest" nodes for an actor change, and periodic republish keeps entries on the currently-closest nodes.

Both are **pull-based** — they return data for the caller to act on, rather than performing I/O themselves. This matches the overall "caller drives" philosophy.

---

## 7. Distribution Crate — Integration Layer (DistributedNode)

### Composition

```
DistributedNode
  ├── keypair: Keypair                    — identity (ed25519)
  ├── swim: SwimNode                      — membership & failure detection
  │     ├── members: MemberList           — CRDT member map
  │     ├── probe: SwimProbe              — probe cycle FSM
  │     └── dissemination: DisseminationQueue
  ├── routing_table: RoutingTable         — 256 k-buckets
  ├── directory: DirectoryShard           — actor → node mappings
  ├── cache: LocationCache                — LRU (ActorAddress → NodeId)
  ├── repair_queue: RepairQueue           — re-replication queue
  ├── republish: RepublishTracker         — periodic re-STORE
  └── tick_count: u64
```

### Actor Resolution Flow

```
resolve_actor(actor_addr)
  │
  ├─► Check LRU cache ──► HIT ──► Cached(NodeId)
  │
  ├─► Check local directory shard ──► HIT ──► Cached(NodeId) + update cache
  │
  └─► Get closest nodes from routing table
        └─► NeedsLookup { closest_nodes }
              (caller drives iterative Kademlia lookup)
```

Three-tier resolution: O(1) cache lookup → O(1) local directory → O(log n) Kademlia lookup. The `NeedsLookup` result contains the closest known nodes; the caller must drive the `NodeLookup` state machine and issue FIND_VALUE RPCs.

### Membership Change Cascade

When SWIM emits `MembershipChanged`, DistributedNode reacts based on the new state:

**`Alive` (new node joined)**:
1. Insert into routing table (appropriate k-bucket)

**`Dead` (node failed or left)**:
1. Remove from routing table
2. Invalidate all cache entries pointing to the dead node (`cache.invalidate_node()`)
3. Extract dead node's directory entries → populate repair queue
4. Repair queue entries available on next `drain()`

### tick() as the Driver

`tick()` is the main entry point. It:
1. Calls `swim.tick()` → gets `Vec<NodeAction>`
2. Processes `MembershipChanged` actions (routing table / cache / repair)
3. Checks `republish.tick()` → adds re-STORE actions if interval elapsed
4. Increments tick counter
5. Returns combined `Vec<NodeAction>` for caller to dispatch

The caller runs a loop: `tick()` → dispatch actions (send messages via transport) → handle incoming messages → `tick()` → ...

### snapshot() for Monitoring

`snapshot()` returns a `DistributionNodeSnapshot` — a serializable point-in-time view of the entire node state. Used by the dashboard for live monitoring without blocking the tick loop.

---

## 8. Distribution Crate — Supporting Modules

### 8.1 types.rs (178 lines)

Core data types shared across all modules:

- **`NodeId([u8; 32])`** — ed25519 public key, doubling as Kademlia key. `xor_distance()` computes bitwise XOR for routing. `xor_leading_zeros()` counts leading zero bits (0–256) to determine k-bucket index.

- **`Signature([u8; 64])`** — ed25519 signature. Serde-serializable.

- **`MemberState`** — `Alive | Suspect | Dead`. Implements `Ord` via `priority()` (0, 1, 2) for CRDT merge: Dead > Suspect > Alive at same incarnation.

- **`NodeRecord`** — Wire-format membership entry: `{ node_id, addr, state, incarnation }`. Used in join responses and membership snapshots.

- **`DirectoryEntry`** — Signed actor→node binding: `{ actor_addr, node_id, generation, signature }`. `payload()` extracts the signable portion (excludes signature field) for verification.

### 8.2 crypto.rs (84 lines)

Thin wrapper around `ed25519_dalek`:

- **`Keypair`** — wraps `ed25519_dalek::SigningKey`. `generate()` creates a random keypair. `node_id()` returns the public key as `NodeId`. Identity = public key (self-certifying, no CA needed).

- **`sign_directory_entry(actor_addr, generation)`** — creates and signs a `DirectoryEntry` in one call.

- **`verify_directory_entry(entry)`** — reconstructs the payload, verifies the ed25519 signature against `entry.node_id`.

### 8.3 codec.rs (52 lines)

- **`impl_json_codec!`** macro — generates `JsonCodec<T>` implementing the `Codec<T>` trait for any serde type.

- **`distribution_codec_registry()`** — returns a `CodecRegistry` with all 10 message types registered. JSON format chosen for debuggability; acknowledged as production debt (see [Section 12](#12-design-decisions--tradeoffs)).

### 8.4 cache.rs (96 lines)

Simple LRU cache for actor location:

```
LocationCache
  entries: HashMap<ActorAddress, CacheEntry>
  capacity: usize
  counter: u64   // monotonic ordering
```

- `get()` returns `Option<NodeId>` and updates LRU ordering
- `peek()` returns `Option<NodeId>` without updating order
- `insert()` adds entry, evicts least-recently-used if at capacity
- `invalidate_node(node_id)` bulk-removes all entries for a dead node

### 8.5 transport.rs (248 lines)

TCP transport with connection pooling and length-prefix framing:

**Wire format**:
```
[4 bytes: frame_len (big-endian u32)]
[32 bytes: destination ActorAddress]
[4 bytes: type_tag_len (big-endian u32)]
[N bytes: type_tag (UTF-8 string)]
[remaining: JSON payload]
```

- **`TcpTransport`** — connection pool (`HashMap<SocketAddr, TcpStream>`). `send_to()` reuses or creates connections. `set_nodelay(true)` for low latency.

- **`TcpAcceptor`** — non-blocking TCP listener. `try_recv()` accepts pending connections and reads framed messages.

### 8.6 messages.rs (155 lines)

All 10 protocol message types, each implementing `NetworkMessage`:

**SWIM messages**: `Ping`, `Ack`, `PingReq`, `JoinRequest`, `JoinResponse`, `MembershipUpdate`

**Kademlia messages**: `FindNodeRequest`, `FindNodeResponse`, `StoreRequest`, `FindValueRequest`, `FindValueResponse` (enum: `Found(DirectoryEntry)` | `Closer(Vec<(NodeId, SocketAddr)>)`)

### 8.7 snapshot.rs (156 lines)

`DistributionNodeSnapshot` — serializable monitoring state:
- SWIM: member list with `alive_count`, `suspect_count`, `dead_count`
- Kademlia: `routing_table_size`, `routing_buckets` (index → count), `routing_neighbors`
- Cache: `cache_size`, `cache_entries` (actor → node mappings)
- Directory: `directory_entry_count`
- Repair: `repair_queue_size`
- Monitoring: `recent_probe_targets` (hex NodeId strings)

---

## 9. Simulation Framework

The simulation crate (`crates/simulation/`) provides a protocol-agnostic testing harness with two protocol implementations.

### Architecture

```
crates/simulation/src/
├── lib.rs                    — module exports
├── config.rs       (15)     — SimConfig
├── topology.rs     (81)     — Topology enum + edge computation
├── trace.rs        (24)     — generic Event<K>, SimulationTrace<K, S>
├── properties.rs   (48)     — PropertyResult, std_dev, coeff_of_variation, chi_squared_uniform
├── distribution/
│   ├── mod.rs      (3)
│   ├── sim.rs      (473)   — DistributionSimConfig, run_simulation()
│   ├── trace.rs    (27)    — DistributionEventKind, DistributionSnapshot
│   └── properties.rs (183) — 4 property checks + DistributionMetrics
└── gossip/                  — feature-gated ("gossip")
    ├── mod.rs      (8)
    ├── protocol.rs (341)   — GossipActor, LWW key-value store
    ├── sim.rs      (337)   — GossipSimConfig, ST + MT harnesses
    ├── trace.rs    (101)   — GossipEvent, GossipEventKind, NodeSnapshot
    ├── properties.rs (857) — 25 property checks, GossipMetrics
    ├── report.rs   (550)   — HTML trace report with SVG visualizations
    └── property_report.rs (786) — HTML property verification report
```

### Shared Infrastructure

**`Topology` enum**: `Ring`, `Star`, `FullMesh`, `Chain`, `Partitioned`. Each variant computes edges via `edges(num_nodes)`. `Partitioned` splits nodes into two halves; `heal_edges()` reconnects them.

**`SimulationTrace<K, S>`**: Generic trace type parameterized over event kind `K` and snapshot type `S`. Stores node names, topology edges, events, and per-round snapshots. Reused by both distribution and gossip simulations.

**Property utilities**: `PropertyResult` struct (name, category, passed, expected, actual, description). Helper functions: `std_dev()`, `coeff_of_variation()`, `chi_squared_uniform()`.

### Distribution Simulation (`src/distribution/`)

**Harness** (`run_simulation()` — 473 lines):
1. Initialize N nodes with sequential addresses (`127.0.0.1:10001+i`)
2. Form cluster: `nodes[1..]` join via seed (node 0)
3. Tick-settle: 10 rounds for SWIM convergence
4. Register actors per node + propagate directory entries via STORE
5. Main loop per round:
   - Apply kill/revive schedule (fault injection)
   - Tick all nodes + deliver actions (`tick_all_and_deliver` + `deliver_actions_tagged`)
   - Resolve random actors from random nodes
   - Take snapshots (member count, routing table size, directory entries, cache size, repair queue)
6. Return `SimulationTrace<DistributionEventKind, DistributionSnapshot>`

**Event kinds**: `Joined`, `MembershipChanged`, `ActorRegistered`, `ActorStored`, `ActorResolved`, `ActorResolveFailed`, `NodeKilled`, `NodeRevived`, `PingSent`, `AckReceived`.

**Properties** (4 checks):
1. `check_join_convergence()` — cluster membership converges within bound
2. `check_membership_accuracy()` — fraction of alive nodes with correct membership view
3. `check_actor_resolution()` — actor resolution success rate ≥ minimum threshold
4. `check_failure_detection()` — killed nodes detected (member count reduced after death)

### Gossip Simulation (`src/gossip/`)

Migrated from the former `swactor-gossip` crate, feature-gated with `gossip`.

**Protocol**: LWW (Last-Writer-Wins) key-value gossip. Each round, a node picks a random peer and pushes its entire state. The receiver merges by version number (higher wins). Simple but well-understood — serves as a baseline for property testing.

**Dual harnesses**: Single-threaded (deterministic, tick-driven) and multi-threaded (non-deterministic, sleep-based). Both produce the same trace format.

**Properties** (25 checks across 8 categories):

| Category | Checks | Examples |
|----------|--------|---------|
| Reliability | 3 | Delivery ratio, atomic delivery, LWW single-value |
| Latency | 5 | Convergence bound, last-node latency, S-curve shape, zero residue, partition heals |
| Message complexity | 4 | Total push count, redundancy ratio, one-push-per-node-per-round, linear scaling |
| Bandwidth/Load | 3 | Hub hotspot detection, load balance CV, amplification factor |
| Convergence | 5 | Monotonic curve, entropy at convergence, entropy decreasing, partition no-converge, partial before heal |
| Consistency | 2 | No stale reads, state size stabilizes |
| Scalability | 2 | Sublinear round scaling, no push without peers |
| Peer selection | 1 | Chi-squared uniformity |

**Reporting**: Self-contained HTML reports with embedded CSS/SVG: trace reports (topology visualization, propagation heatmap, convergence curve, message flow timeline) and property reports (executive summary, per-section results, scalability plots, thread comparison).

### Gossip as Testbed for Distribution

The gossip property framework (45 tests across 11 test scenarios) served as the testbed for building the simulation infrastructure. The distribution simulation reuses the same `Topology`, `SimulationTrace`, and `PropertyResult` types but currently has only 6 scenario tests vs. gossip's 45. Expanding distribution property coverage using the gossip framework's patterns is a known future improvement.

---

## 10. Dashboard Integration

### distribution_collector.rs (36 lines)

Decoupled provider pattern:

```rust
pub trait DistributionStatsProvider: Send + Sync {
    fn snapshot(&self) -> Option<DistributionNodeSnapshot>;
}
```

Generic wrapper `DistributionCollector<T>` holds `Arc<Mutex<T>>` and implements the trait. The dashboard never touches `DistributedNode` directly — it polls the trait for a serializable snapshot.

### distribution_html.rs (747 lines)

Self-contained HTML/CSS/JS served at `/distribution`. Key features:

- **Force-directed graph** with Barnes-Hut quadtree optimization (O(N log N)). Nodes represent cluster members; edges show Kademlia routing relationships.
- **Ego-centric click mode**: Click any node to focus — highlights its routing neighbors with dashed edges, fades other nodes to 15% opacity. Self node shown with 1.5x radius and white outline.
- **Color-coded node states**: Alive (green #4caf50), Suspect (orange #ff9800), Dead (red #f44336), Self (indigo #6366f1).
- **9 stat cards**: Members, Alive, Suspect, Dead, LRU Cache size, Routing Table size, Directory entries, Repair Queue size, Recent probes.
- **Members table**: State, node ID (truncated), address, incarnation.
- **LRU cache table**: Actor address → residing node ID (top 200 entries).
- **Routing bucket histogram**: Distribution of entries across k-buckets.
- **Recent probes list**: Most recent SWIM probe targets.
- **Real-time SSE updates**: Subscribes to `/events` stream, updates every ~200ms.
- **Dark theme**: GitHub-style (#0f1117 background).

### server.rs — SSE Route Additions

Feature-gated with `#[cfg(feature = "distribution")]`:

- Route: `"/distribution"` serves the HTML template
- SSE stream: `/events` includes `"distribution"` event type with serialized `DistributionNodeSnapshot` JSON
- Polling: Same 200ms interval as runtime stats
- Gracefully handles no provider attached (skips poll)

### lib.rs — Feature Gate

```rust
#[cfg(feature = "distribution")]
pub mod distribution_collector;

// In DashboardHandle:
pub fn set_distribution(&self, provider: Arc<dyn DistributionStatsProvider>) { ... }
```

Feature `distribution` is **default-on** in the dashboard crate's `Cargo.toml`.

### dashboard_demo.rs (554 lines)

Full demo composing runtime + distribution + dashboard:

- **9-node cluster** with SWIM probing (probe_interval=5, probe_timeout=2)
- **Actor workload**: 16 ping-pong actors + counter actors (growing to 500)
- **Churn cycle** (every 400 rounds starting at round 200):
  - Kill peer 8 (simulated crash)
  - Revive peer 8 after 150 rounds
  - Peer 7 graceful leave at round 200
  - Peer 7 rejoin at round 350

**SnapshotProvider decoupling pattern**: Instead of giving the SSE thread direct access to `DistributedNode` via `Arc<Mutex<>>`, the demo holds a cached `Option<DistributionNodeSnapshot>` behind `Arc<Mutex<>>`. The main loop updates the snapshot every tick; the SSE thread reads it. This means the SSE thread never contends for the node lock — snapshots can be up to 200ms stale (one tick round), which is acceptable for monitoring.

---

## 11. Crate Renames & Workspace Cleanup

| Before | After | Rationale |
|--------|-------|-----------|
| `swactor-python` | `python` | Reduce crate name pollution; the workspace context makes the parent clear |
| `swactor-dp-mnist` | `dp-mnist` | Same |
| `swactor-wasm` | `wasm` | Same |
| `swactor-gossip` | absorbed into `crates/simulation/src/gossip/` | Gossip is a simulation protocol, not production code. Feature-gated with `gossip` |
| `gossip-dashboard` | `simulation` | Shared simulation report infrastructure used by both gossip and distribution sims |

The gossip protocol was originally in its own crate with a separate dashboard. Since it's primarily useful for property testing (not production membership), it was consolidated into the simulation crate behind a feature gate. This avoids maintaining a separate crate for what is essentially test infrastructure, while keeping it available for comparison benchmarking.

---

## 12. Design Decisions & Tradeoffs

### 12.1 Pure State Machines vs. Async Actors

**Choice**: `(state, event) → (state, Vec<Action>)` pattern throughout.

**Why not make SwimNode an Actor?** It was tempting — swactor is an actor runtime, after all. But embedding SWIM inside the actor system creates a circular dependency: the membership layer would depend on the runtime it's trying to distribute.

**Pros**: Every component is testable without a runtime, networking, or timers. Tests are deterministic — feed events, assert actions. The caller controls the execution model (single-threaded tick loop, dedicated thread, or integrated into worker threads).

**Cons**: The caller must implement the dispatch loop (tick → send actions → receive messages → tick). This is boilerplate but keeps the library pure.

**Alternative considered**: Embed a `Runtime` inside `DistributedNode` for a self-driving tick loop. Rejected because it couples the distribution layer to a specific runtime configuration and makes testing non-deterministic.

### 12.2 SWIM over Full-State Gossip

**Choice**: SWIM for membership, not the existing `swactor-gossip` LWW protocol.

**Why**: SWIM has O(1) message overhead per probe round (ping one node, piggyback updates). Full-push gossip is O(state_size) per round per node. For membership (where the state is the member list), SWIM also provides built-in failure detection — the probe cycle itself is the detector.

**Tradeoff**: SWIM is more complex to implement correctly (probe phases, suspicion timers, incarnation numbers). The gossip protocol is simpler but doesn't detect failures — it only propagates state.

**The gossip crate remains** (in simulation/) for key-value use cases and as a property testing baseline.

### 12.3 Kademlia over Consistent Hashing

**Choice**: Kademlia DHT for actor location directory.

**Why**: Kademlia provides iterative lookup without a central hash ring. No single point of failure. Logarithmic lookup (O(log n) hops). The XOR distance metric is symmetric and satisfies the triangle inequality, enabling efficient routing.

**Tradeoff**: More complex than consistent hashing with virtual nodes. Requires active maintenance (bucket refresh, entry republish, repair on death). A hash ring is simpler and sufficient for static clusters but requires ring rebalancing on every membership change.

### 12.4 ed25519 for Identity + Signing

**Choice**: `NodeId = ed25519 public key`. Identity is the key.

**Why**: Self-certifying identity. No certificate authority needed. A node proves its identity by signing messages with its private key. Directory entries are signed, preventing forgery — node A can't claim to host an actor that lives on node B.

**Tradeoff**: 32-byte NodeIds (larger than 16-byte UUIDs). No key rotation without changing identity. An alternative is separate identity and signing keys (more flexible key management, but more complex).

### 12.5 JSON Wire Format

**Choice**: All protocol messages serialized as JSON.

**Why**: Debuggable. `tcpdump` or wireshark can read messages directly. During development, this saved significant debugging time — you can print a SWIM Ping and see exactly what's in it.

**Tradeoff**: 2-3x larger than bincode, slower parsing. Not suitable for production at scale. `codec.rs` acknowledges this debt — the codec registry abstraction exists specifically to make swapping to bincode/msgpack a one-line change.

### 12.6 TCP over UDP for SWIM

**Choice**: TCP transport for all SWIM messages (including probes).

**Why**: Connection pooling amortizes TCP handshake cost. No message size limits or fragmentation needed. The existing swactor transport infrastructure was TCP-based, so reuse was natural.

**Tradeoff**: Most SWIM implementations (Hashicorp Memberlist, SWIM paper) use UDP for probes because it's lower overhead per message and avoids TCP head-of-line blocking. TCP adds ~40 bytes of header overhead per message and can stall if a connection is congested. For large clusters, UDP with application-level retries would be more appropriate.

### 12.7 In-Process Simulation over Network Simulation

**Choice**: Simulation uses direct method calls (`node.handle_ping(...)`) instead of real networking.

**Why**: Deterministic execution (single-threaded mode). Fast — no syscalls, no port allocation, no TCP handshakes. No port conflicts in CI. A 100-round simulation of 20 nodes completes in milliseconds.

**Tradeoff**: Doesn't test real network failures (packet loss, reordering, delayed delivery, TCP RST). The gap between "works in simulation" and "works on a real network" is where subtle bugs hide. Adding a probabilistic drop/delay/reorder layer to the simulation is a known future improvement.

### 12.8 Pull-Based Repair vs. Automatic re-STORE

**Choice**: `RepairQueue` returns entries to the caller rather than automatically issuing STORE RPCs.

**Why**: Keeps the library pure — `tick()` never performs I/O. The caller decides when and how to re-STORE. This matches the overall "caller drives" philosophy: state machines produce actions, callers execute them.

**Tradeoff**: Easy for a caller to forget to drain the repair queue. Requires vigilance in the dispatch loop. An alternative is `tick()` returning `StoreRequest` actions alongside `SendPing`/`SendAck` — which would make repair automatic while staying pure. This is a likely future change.

### 12.9 Static 256 k-Buckets vs. Dynamic Splitting

**Choice**: Fixed 256 k-buckets, one per bit of the 256-bit key space.

**Why**: Simpler implementation. Predictable memory (256 buckets × k entries max). No splitting/merging logic. For clusters up to ~1000 nodes, most buckets are empty or sparse, but the overhead is negligible.

**Tradeoff**: S/Kademlia's dynamic splitting is more space-efficient for large clusters and provides better load balancing across buckets. For clusters > 10,000 nodes, the static approach wastes memory on empty high-index buckets. Not a concern at current scale.

### 12.10 Snapshot Provider Decoupling

**Choice**: Dashboard demo uses `SnapshotProvider` with cached `Option<DistributionNodeSnapshot>` instead of `Arc<Mutex<DistributedNode>>`.

**Why**: The SSE thread (HTTP server) must not block on the node's tick loop. With `Arc<Mutex<DistributedNode>>`, the SSE thread would contend for the lock every 200ms, potentially stalling ticks. The snapshot pattern means the SSE thread reads a pre-computed snapshot — zero contention.

**Tradeoff**: Snapshot can be up to 200ms stale (one tick round). For monitoring purposes this is acceptable. For operational tooling (e.g., "is this node alive RIGHT NOW?"), direct access might be needed.

---

## 13. What's Unclear / Indeterminate

### 13.1 Lifeguard Wiring

`lifeguard.rs` is fully implemented and tested (16 tests) but not integrated into `SwimProbe`. The `HealthMultiplier` computes dynamic timeouts and interval scaling, but there's no call site in `probe.rs` that reads these values.

**Open question**: Should Lifeguard modify `SwimConfig` dynamically (mutate the config struct each tick), or should `SwimProbe` query a `HealthMultiplier` reference each time it needs a timeout? The first approach is simpler but means config values are no longer stable; the second requires threading a reference through probe methods.

### 13.2 FIND_VALUE vs. FIND_NODE

The `NodeLookup` state machine is generic — it finds the k closest nodes to a target. There's no dedicated `ValueLookup` that terminates early when a directory entry is found mid-lookup.

`ResolveResult::NeedsLookup` returns closest nodes from the routing table, but the caller has no state machine to drive the actual FIND_VALUE queries. The gap between "I know who to ask" and "I got the answer" is unimplemented.

**Impact**: Actor resolution currently works only for locally-cached or locally-stored entries. Cross-node resolution requires the caller to manually drive the lookup, which no code currently does.

### 13.3 Replication Factor

`distribution_plan.md` specifies `r = 2f+1` quorum replication, but the implementation doesn't enforce a replication factor. `register_actor()` stores locally and returns the `DirectoryEntry` — the caller is responsible for issuing STORE to the r-closest nodes. No mechanism tracks whether r copies exist.

### 13.4 TTL / Expiration

`DirectoryShard::remove_where()` exists but is never called. Directory entries have no timestamp or TTL field. Without TTL, orphaned entries from permanently dead nodes accumulate indefinitely. `RepairQueue` handles known deaths but not silent disappearances (nodes that crash without being detected, or entries for actors that were unregistered but not cleaned up).

### 13.5 Real Network Integration

The transport layer (`TcpTransport`, `TcpAcceptor`) is implemented and tested, but `DistributedNode` never uses it directly. All integration tests and simulations use in-process method calls. The actual wiring of `node.tick() → transport.send()` for each `NodeAction` is missing.

### 13.6 Multi-Threaded Tick

`DistributedNode` is `!Send + !Sync` (contains mutable references and non-atomic state). Running it in a multi-threaded context requires wrapping in `Arc<Mutex<>>` (as the dashboard demo does). It's unclear whether the tick loop should be:
- A dedicated thread (simple, but adds latency for actor resolution queries)
- Integrated into the runtime's worker threads (low latency, but requires `Send + Sync` or a message-passing interface)
- An actor within the swactor runtime (elegant, but circular dependency concerns from [12.1](#121-pure-state-machines-vs-async-actors))

---

## 14. Known Gaps & Future Improvements

Listed with rough effort estimates. Not prioritized.

| Gap | Effort | Impact | Notes |
|-----|--------|--------|-------|
| Wire Lifeguard into SwimProbe | Small | High | Reduces false positives under load. Feed ack/nack events to `HealthMultiplier`, read dynamic timeouts in probe cycle |
| FIND_VALUE lookup state machine | Medium | High | Clone `NodeLookup`, add early termination when value found. Bridge the `NeedsLookup` → actual resolution gap |
| Automatic STORE replication after `register_actor` | Medium | High | `tick()` emits `StoreRequest` actions to r-closest nodes after registration |
| Republish automation in tick() | Small | Medium | `tick()` emits re-STORE actions when `RepublishTracker` fires. Currently returns data but no one acts on it |
| TTL/expiration for directory entries | Small | Medium | Add timestamp field to `DirectoryEntry` + periodic `remove_where(expired)` in tick |
| UDP transport for SWIM probes | Medium | Medium | New transport impl with message fragmentation. Lower per-message overhead, avoids TCP HOL blocking |
| Push-pull anti-entropy | Medium | Medium | Periodically exchange full member lists for partition recovery. Supplements SWIM's piggybacked dissemination |
| Bucket refresh for Kademlia | Small | Low | Periodic FIND_NODE for random IDs in sparse buckets. Keeps routing table fresh |
| Network failure injection in simulation | Medium | High | Probabilistic drop/delay/reorder layer. Bridges the gap between in-process and real-network testing |
| Distribution property tests | Medium | Medium | Apply gossip's 25-property framework to SWIM convergence and actor resolution. Currently 6 tests vs gossip's 47 |
| Distribution HTML reports in simulation | Medium | Low | Visualization for SWIM probe cycles, membership evolution, resolution success rates |
| Bincode/msgpack wire format | Small | Medium | Swap codec, benchmark. Infrastructure exists (`CodecRegistry` abstraction) |
| S/Kademlia security extensions | Large | Low (for now) | Node ID certification, disjoint lookup paths, bucket verification. Needed for adversarial environments |
| Multi-DC support | Large | Low (for now) | RTT-aware timeouts, zone-aware routing, cross-DC replication strategies |

---

## 15. Test Coverage Summary

### Distribution Crate — 133 Tests

| File | Module | Tests | Focus |
|------|--------|-------|-------|
| `types_and_crypto.rs` | Core | 19 | NodeId XOR distance, MemberState ordering, DirectoryEntry signing, Keypair generation |
| `transport_and_codec.rs` | Core | 6 | TCP framing, wire format round-trip, codec registry |
| `cache.rs` | Core | 7 | LRU eviction, capacity enforcement, bulk invalidation by node |
| `swim_probe.rs` | SWIM | 13 | Probe cycle phases, suspicion timers, indirect probe relay, timeout transitions |
| `swim_node.rs` | SWIM | 11 | Join protocol, leave, ping/ack handling, incarnation refutation, piggyback |
| `swim_dissemination.rs` | SWIM | 11 | Transmit budget, priority ordering, deduplication, piggyback pack/unpack |
| `lifeguard.rs` | SWIM | 16 | Health scoring, ack/nack tracking, dynamic timeout scaling, multiplier bounds |
| `kademlia_routing.rs` | Kademlia | 14 | Bucket insertion, LRU eviction, replacement promotion, closest-k query |
| `kademlia_lookup.rs` | Kademlia | 7 | Iterative convergence, round termination, α-concurrency, failure handling |
| `kademlia_directory.rs` | Kademlia | 12 | Store with signature verification, generation ordering, quorum resolution |
| `repair.rs` | Kademlia | 6 | Death-triggered re-replication, periodic republish scheduling |
| `node_integration.rs` | Integration | 11 | 3-node cluster: join handshake, membership convergence, actor registration + resolution, leave + death cascade |

### Simulation Crate — 47 Tests

| File | Tests | Focus |
|------|-------|-------|
| `gossip_properties.rs` | 36 | 25 property checks across 11 topology/config scenarios (ring, star, mesh, chain, partitioned, scaled) |
| `gossip_convergence.rs` | 5 | Behavioral convergence: chain propagation, higher-version-wins, disjoint merge, mutual gossip, concurrent updates |
| `distribution_sim.rs` | 6 | Cluster convergence, node death detection, rejoin recovery, actor resolution, full-mesh properties |

### Testing Philosophy

All tests follow behavioral Given/When/Then style — not structural (no insert-then-lookup). Tests encode decisions the system made, not just echo what the code does. The `deliver_actions()` helper in integration tests simulates network rounds by routing `NodeAction` outputs to the appropriate handler methods on peer nodes, enabling multi-node scenarios without real networking.

**Asymmetry note**: The distribution simulation has 6 tests vs. gossip's 47. This reflects development sequencing — the gossip framework was built first as a testbed, and applying its full property suite to distribution is a known future improvement.

### Total: 180 tests across both crates.
