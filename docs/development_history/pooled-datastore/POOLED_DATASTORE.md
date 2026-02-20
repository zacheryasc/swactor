# Pooled Datastore & Sim-Cluster — Development History

> Adds a gossip-converged pooled storage protocol, a generic gossip channel
> abstraction, a pool dashboard page, shared pool types, iroh connection
> hardening, and a Docker-free multi-process sim-cluster test harness.
>
> ~18 new/modified files · ~2,400 insertions
>
> *Branch: `pooled-datastore`*

---

## Table of Contents

1. [Overview & Motivation](#1-overview--motivation)
2. [What Was Built](#2-what-was-built)
3. [Pooled Storage Protocol](#3-pooled-storage-protocol)
4. [Generic Gossip Channel Abstraction](#4-generic-gossip-channel-abstraction)
5. [Pool Disseminator](#5-pool-disseminator)
6. [Pool Coordinator Actor](#6-pool-coordinator-actor)
7. [Shared Pool Types](#7-shared-pool-types)
8. [Dashboard Pool Page](#8-dashboard-pool-page)
9. [Iroh Connection Hardening](#9-iroh-connection-hardening)
10. [Sim-Cluster Test Harness](#10-sim-cluster-test-harness)
11. [Design Decisions & Tradeoffs](#11-design-decisions--tradeoffs)
12. [Test Coverage](#12-test-coverage)
13. [Known Gaps & Future Work](#13-known-gaps--future-work)

---

## 1. Overview & Motivation

The existing datastore provides content-addressed storage on individual nodes,
but there is no mechanism for multiple nodes to form a shared storage pool —
knowing who has what content, how much capacity each node offers, or where to
place new data.

This branch introduces a **pooled datastore protocol** that layers on top of
the existing content-addressed datastore. Multiple nodes join a named pool,
gossip their membership/capacity/content-locations via SWIM piggyback, and
converge on a shared view of the pool's state. This enables:

- **Content location**: find which node(s) hold a given content hash without
  fan-out queries.
- **Capacity-aware placement**: route new writes to the node with the most free
  space.
- **Pool ACL**: optional allow-list to restrict which nodes can join a pool.
- **Live observability**: a new dashboard page shows pool membership, capacity
  bars, content location map, and ACL state in real time via SSE.

Separately, the branch also introduces:

- A **generic gossip channel abstraction** (`GossipChannel` trait +
  `DisseminationBuffer<T>`) that replaces the 4 duplicated dissemination
  patterns in the distribution crate.
- A **sim-cluster test harness** (`cargo xtask test sim-cluster` / `cargo
  xtask sim-cluster`) that spawns real multi-process clusters with a local iroh
  relay — no Docker required.
- **Iroh connection hardening**: relay URL resolution cascade and connect
  timeouts to prevent indefinite hangs during peer connection.

---

## 2. What Was Built

| Component | Crate / Location | Lines |
|-----------|-----------------|-------|
| Pool types (PoolId, entries, config) | `crates/shared-types/src/pool.rs` | ~170 |
| Gossip channel trait + DisseminationBuffer | `crates/distribution/src/gossip_channel.rs` | ~270 |
| Pool disseminator (CRDT state + gossip) | `crates/datastore/src/pool/disseminator.rs` | ~720 |
| Pool coordinator actor | `crates/datastore/src/pool/coordinator.rs` | ~275 |
| Pool messages | `crates/datastore/src/pool/messages.rs` | ~80 |
| Pool dashboard HTML/JS | `crates/dashboard/src/pool_html.rs` | ~390 |
| Sim-cluster harness | `xtask/src/sim_cluster.rs` | ~700 |
| Pool integration tests | `crates/datastore/tests/pool_tests.rs` | ~340 |
| Docker compose (dev cluster) | `tests/docker/docker-compose.dev-cluster.yml` | ~75 |

Modified files:

| File | Change |
|------|--------|
| `crates/distribution/src/iroh_driver.rs` | Relay URL cascade + connect timeout |
| `crates/swactor-node/src/main.rs` | Seed node relay URL hints for iroh |
| `crates/datastore/tests/dashboard_integration_test.rs` | Start HTTP standalone |
| `xtask/src/main.rs` | `sim-cluster` subcommand + test group |
| `xtask/Cargo.toml` | reqwest, tokio, iroh-relay deps |

---

## 3. Pooled Storage Protocol

The pool protocol is a set of four CRDT entry types that converge via gossip:

```
┌────────────────────────────────────────────────────┐
│                   Pool State (per node)             │
├──────────────┬─────────────┬───────────┬───────────┤
│  Membership  │  Capacity   │  Content  │    ACL    │
│              │             │  Location │           │
│ node→state   │ node→bytes  │ (hash,    │ node→     │
│ (Active/Left)│ (total/used)│  node)→   │  grant/   │
│              │             │  tombstone│  revoke   │
├──────────────┴─────────────┴───────────┴───────────┤
│             Higher generation always wins           │
│          (last-writer-wins register per key)        │
└────────────────────────────────────────────────────┘
```

**Convergence rule**: For each entry type, the key is derived from the entry
(e.g. `node_id` for membership, `(content_hash, node_id)` for content
locations). When two entries share a key, the one with the higher `generation`
wins. This makes all merges commutative, associative, and idempotent — a CRDT.

**Deletion**: Content locations and ACL entries use tombstones (`tombstone:
true` / `revoked: true`) with a generation bump. Tombstones are garbage
collected after a configurable TTL.

**Dissemination**: All entries go through a shared `DisseminationBuffer<PoolEntry>`
which transmits each entry `Λ * ceil(log₂(n))` times before eviction, matching
the standard SWIM protocol budget.

---

## 4. Generic Gossip Channel Abstraction

**File**: `crates/distribution/src/gossip_channel.rs`

Before this branch, SWIM piggyback dissemination was hardcoded for membership
updates, directory entries, and dead-letter notifications — each with its own
copy of the `Λ * ceil(log₂(n))` budget logic.

The new abstraction provides:

- **`GossipChannel` trait**: A topic-tagged channel that produces/consumes
  `Vec<u8>` entries for piggyback. Methods: `topic_tag()`,
  `take_pending_bytes()`, `apply_incoming_bytes()`, `re_disseminate_all()`,
  `on_node_death()`, `gc_tick()`.

- **`DisseminationBuffer<T>`**: A generic budget-limited queue. Entries are
  enqueued with a transmit budget of `Λ * ceil(log₂(n))` and evicted after
  exhaustion. Supports `enqueue`, `enqueue_or_replace` (idempotent upsert),
  `take`, `re_enqueue_all`, and `retain`.

- **Serialization helpers**: `serialize_each()` and `deserialize_each()` for
  converting between typed entries and `Vec<u8>`.

The pool disseminator is the first consumer, plugging into the distribution
layer via `SharedPoolChannel` which wraps `Arc<Mutex<PoolDisseminator>>`.

---

## 5. Pool Disseminator

**File**: `crates/datastore/src/pool/disseminator.rs`

The core state machine. Manages four `HashMap` tables (membership, capacity,
content locations, ACL) and a single `DisseminationBuffer<PoolEntry>`.

Key methods:

- **Lifecycle**: `join()`, `leave()` — announce membership state changes.
- **Storage**: `announce_content()`, `remove_content()`, `announce_capacity()`.
- **ACL**: `grant_access()`, `revoke_access()`, `is_node_authorized()`.
- **Queries**: `active_members()`, `member_count()`, `content_count()`,
  `locate_content()`, `node_with_most_free_space()`, `pool_capacity_summary()`.
- **Dashboard**: `snapshot_json()` — full JSON snapshot for SSE.
- **Internal**: `merge_entry()` applies the higher-generation-wins rule.
  `take_pending_inner()` / `apply_incoming_inner()` drive gossip exchange.

`SharedPoolChannel` wraps this in an `Arc<Mutex<>>` and implements
`GossipChannel`, bridging ownership between the `PoolCoordinator` actor
(lifecycle/queries) and `DistributedNode` (gossip transport).

---

## 6. Pool Coordinator Actor

**File**: `crates/datastore/src/pool/coordinator.rs`

An actor implementing `ActorInterface` for `PoolCoordinatorMsg`. It acts as a
placement-aware CRUD facade:

- **PoolPut/Get/Delete/List**: Delegates to the co-located `DatastoreNode`
  actor. Future: redirect to best node based on capacity.
- **PoolStatus**: Queries the disseminator and returns a JSON status snapshot.
- **JoinPool/LeavePool**: Checks ACL authorization, then calls the
  disseminator.
- **GrantPoolAccess/RevokePoolAccess**: Manages the allow-list.
- **PoolTick**: Periodic capacity re-announcement (every 100 ticks).

---

## 7. Shared Pool Types

**File**: `crates/shared-types/src/pool.rs`

Types live in `shared-types` to avoid circular dependencies between
`distribution` and `datastore`:

- **`PoolId`**: `blake3(name_bytes)` — 32-byte deterministic pool identifier.
  Supports hex encoding/decoding and truncated display.
- **`PoolMemberEntry`**: Node membership with `Active`/`Left` state.
- **`PoolCapacityEntry`**: Storage capacity announcement (total/used bytes).
- **`ContentLocationEntry`**: Where a content hash is stored, with tombstone
  support.
- **`PoolACLEntry`**: Authorization grant/revoke with `granted_by` provenance.
- **`PoolEntry`**: Tagged enum wrapping all four entry types for gossip
  serialization.
- **`PoolConfig`**: Pool configuration (name, capacity, TTL, GC interval, Λ).

---

## 8. Dashboard Pool Page

**File**: `crates/dashboard/src/pool_html.rs`

A new `/pool` page in the dashboard with:

- **Summary cards**: pool name, member count, content count, total/used
  capacity.
- **Capacity bars**: per-node usage with color thresholds (green < 70%, orange
  < 90%, red >= 90%).
- **Members table**: node ID (truncated with tooltip), state, total/used/free.
- **Content location map**: content hash → replica count → node list.
- **ACL panel**: open mode indicator or allow-list table.
- **Join/Leave buttons**: POST to `/api/pool/join` and `/api/pool/leave`.
- **Live updates**: SSE `pool` events drive real-time state refresh.

---

## 9. Iroh Connection Hardening

**File**: `crates/distribution/src/iroh_driver.rs`

Two problems fixed:

1. **Relay URL resolution cascade**: When connecting to a peer, the driver now
   tries three sources in order: (a) explicit relay URL cache from prior
   connections, (b) SWIM metadata gossip (via `node.relay_url()`), (c) the
   local node's own home relay. Previously only the explicit cache was checked,
   causing connections to fail when the cache was empty.

2. **Connect timeout**: All `endpoint.connect()` calls now have a 2-second
   `tokio::time::timeout` wrapper. Previously, connections could hang
   indefinitely if a peer was unreachable.

**File**: `crates/swactor-node/src/main.rs`

Seed node addresses now include relay URLs so iroh can locate the seed through
the relay server, rather than relying solely on direct addressing.

---

## 10. Sim-Cluster Test Harness

**File**: `xtask/src/sim_cluster.rs`

A new test stage and development tool that spawns real multi-process swactor
clusters without Docker:

### Test mode: `cargo xtask test sim-cluster`

Runs 4 scenarios sequentially, each with a fresh 5-node cluster:

| # | Scenario | Validates |
|---|----------|-----------|
| 1 | Cluster convergence | All 5 nodes see >= 4 alive peers, routing table >= 4 |
| 2 | Node death detection | Kill node 2, survivors detect alive drop, dead count >= 1 |
| 3 | Killed node rejoins | Kill node 2, restart it, rejoined node sees alive >= 1 |
| 4 | Actors resolvable | Each node has >= 2 directory entries, total >= 10 |

### Interactive mode: `cargo xtask sim-cluster --nodes N`

Spawns a persistent cluster for development. Prints dashboard URLs and blocks
until Ctrl-C.

### Infrastructure

- **Local relay server**: Embedded `iroh-relay` server on an ephemeral port.
  Nodes connect through the relay rather than requiring direct connectivity.
- **RAII lifecycle**: `SimCluster` owns child processes and SIGTERM's them on
  drop. `RelayServer` owns its tokio runtime.
- **Config generation**: Each node gets a `node.toml` with dashboard port,
  actor count, relay host/port, and optional seed node ID.
- **Seed key discovery**: Polls the seed node's key file on disk to extract the
  public key before spawning joiner nodes.
- **HTTP observation**: Polls `/api/distribution` on each node's dashboard.
  Uses `serde_json::Value` to avoid compile-time coupling to protocol types.
- **Node lifecycle**: `kill_node()` sends SIGTERM, `restart_node()` re-spawns
  with the same config (non-seed nodes get the seed's public key).

### Dev cluster compose

**File**: `tests/docker/docker-compose.dev-cluster.yml`

A 3-node Docker Compose file for development with pool configuration
(`--pool-name dev-pool --pool-capacity 104857600`). Uses a bridge network with
static IPs.

---

## 11. Design Decisions & Tradeoffs

**Higher-generation-wins CRDT over vector clocks**: Pool entries use a simple
monotonic generation counter per entry key. This is sufficient because each
entry has a single writer (the node that owns it). Vector clocks would add
complexity without benefit since there are no concurrent writers for the same
key.

**Tombstones with TTL over immediate deletion**: Content locations and ACL
revocations use tombstones that propagate via gossip before being GC'd. Without
tombstones, a deleted entry could be re-introduced by a node that hasn't yet
received the deletion.

**Shared `Arc<Mutex<>>` over message-passing for disseminator**: The pool
disseminator needs to be accessed by both the coordinator actor (for
lifecycle/queries) and the distribution layer (for gossip). Rather than adding
an actor-to-actor message protocol, the disseminator is wrapped in
`Arc<Mutex<PoolDisseminator>>`. The lock is held only briefly for individual
operations.

**Sim-cluster over Docker for testing**: Docker adds build time, image
management, and network configuration complexity. The sim-cluster spawns bare
processes on localhost, uses an embedded iroh relay, and tears down in
milliseconds. Scenarios that previously required Docker Compose now run with
`cargo xtask test sim-cluster`.

**HTTP polling over direct protocol observation**: The sim-cluster observes
node state via HTTP (`/api/distribution`) rather than linking against protocol
types. This makes the test harness resilient to protocol changes and mirrors
how an operator would observe a real cluster.

**Pool types in `shared-types`**: Pool entry types live in `shared-types`
rather than `datastore` to avoid a circular dependency — `distribution` needs
to know about pool entries for gossip serialization, and `datastore` depends on
`distribution`.

---

## 12. Test Coverage

### Unit tests (disseminator internals)

In `crates/datastore/src/pool/disseminator.rs`:

- `join_and_query_members` — join lifecycle
- `leave_removes_from_active` — leave lifecycle
- `announce_and_locate_content` — content announcement + query
- `remove_content_tombstones` — tombstone semantics
- `capacity_summary` — capacity aggregation
- `acl_grant_and_check` / `acl_revoke` / `empty_acl_means_open` — ACL logic
- `higher_generation_wins_merge` — CRDT merge rule
- `two_disseminators_converge_via_gossip_exchange` — two-node gossip
- `three_node_convergence_loop` — multi-round gossip convergence

### Unit tests (gossip channel)

In `crates/distribution/src/gossip_channel.rs`:

- `budget_math_*` (4 tests) — transmit budget calculation
- `enqueue_take_evicts_after_budget` — budget exhaustion
- `enqueue_or_replace_*` (2 tests) — idempotent upsert
- `re_enqueue_all_refreshes_budgets` — anti-entropy
- `retain_removes_non_matching` — predicate-based eviction
- `serialize_deserialize_roundtrip` — wire format

### Unit tests (shared types)

In `crates/shared-types/src/pool.rs`:

- `pool_id_from_name_is_deterministic` / `pool_id_different_names_differ`
- `pool_id_hex_roundtrip`
- `pool_entry_serde_roundtrip`
- `higher_generation_wins_for_membership`
- `content_location_tombstone_semantics`

### Integration tests (pool protocol)

In `crates/datastore/tests/pool_tests.rs`:

- `two_nodes_converge_on_membership` — two-node gossip convergence
- `content_location_propagates_via_gossip` — cross-node content discovery
- `leave_propagates_via_gossip` — membership leave propagation
- `content_deletion_propagates` — tombstone propagation
- `acl_grant_propagates` — ACL gossip
- `capacity_propagates_and_summarizes` — capacity gossip + aggregation
- `placement_query_picks_node_with_most_space` — capacity-aware placement
- `five_node_pool_converges` — 5-node full convergence
- `shared_pool_channel_topic_tag` — GossipChannel interface
- `gossip_channel_bytes_roundtrip` — wire format through GossipChannel

### Sim-cluster scenarios (multi-process)

In `xtask/src/sim_cluster.rs`:

- Cluster convergence (5 nodes)
- Node death detection (kill + observe)
- Killed node rejoins (kill + restart + observe)
- Actors resolvable (directory entry propagation)

---

## 13. Known Gaps & Future Work

- **Remote content fetch**: `PoolGet` currently only checks the local
  datastore. It should use `locate_content()` to fetch from the node that
  actually has the content.
- **Capacity-aware placement**: `PoolPut` delegates to the local datastore.
  It should use `node_with_most_free_space()` to route writes to the best node.
- **Actual usage tracking**: `PoolTick` re-announces capacity with `used: 0`.
  It should query the `BlobStore` for actual disk usage.
- **GossipChannel integration**: The `GossipChannel` trait and
  `SharedPoolChannel` are built but not yet wired into `DistributedNode`'s
  piggyback system. The existing hardcoded dissemination channels need to be
  migrated to the new trait.
- **Dashboard SSE integration**: The pool dashboard HTML is built, but the
  server-side SSE event source for `pool` events needs to be wired to the
  `PoolDisseminator::snapshot_json()` method.
- **Sim-cluster namespace isolation**: The harness uses high ephemeral ports
  for isolation. Full Linux network namespace isolation (as designed in
  `TEST_ISOLATION.md`) is a future enhancement.
- **Sim-cluster in CI**: The sim-cluster test group is opt-in and excluded
  from `essential`/`all`. Once proven stable, it should be added to CI.
