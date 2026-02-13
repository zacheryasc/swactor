# DistributedNode

`DistributedNode` is the top-level integration type that composes SWIM
membership, Kademlia routing, the actor directory, location cache, and
repair infrastructure into a single public API.

## Internal Composition

```
┌─ DistributedNode ────────────────────────────────────────────────────────┐
│                                                                          │
│  keypair: Keypair             ← ed25519 identity + signing               │
│                                                                          │
│  ┌── SWIM ─────────────────┐  ┌── Kademlia ────────────────────────────┐ │
│  │  SwimNode                │  │  RoutingTable  (256 k-buckets, K=20)  │ │
│  │    ├ MemberList (CRDT)   │  │  DirectoryShard  (local entries)      │ │
│  │    ├ SwimProbe (FSM)     │  │  RepairQueue  (death-triggered)       │ │
│  │    └ DisseminationQueue  │  │  RepublishTracker  (periodic)         │ │
│  └──────────────────────────┘  └────────────────────────────────────────┘ │
│                                                                          │
│  cache: LocationCache         ← LRU: ActorAddress → NodeId              │
│  tick_count: u64              ← monotonic clock                          │
│                                                                          │
└──────────────────────────────────────────────────────────────────────────┘
```

## Node Lifecycle

```
  new(config)
      │
      v
  Generate Keypair ──► NodeId = public key bytes
      │
      v
  Initialize subsystems (SwimNode, RoutingTable, DirectoryShard, ...)
      │
      v
  join(seeds) ──► SendJoinRequest to each seed
      │
      v
  handle_join_response(members) ──► populate MemberList + RoutingTable
      │
      v
  ┌─────────────┐
  │  tick() loop │ ──► drives SWIM probes, processes membership changes,
  │  (caller)    │     runs periodic republish, drains repair queue
  └─────────────┘
      │
      v
  leave() ──► disseminate Dead for self, graceful shutdown
```

See [distribution_minor_flows.svg](../diagrams/distribution_minor_flows.svg) for the
join handshake, dissemination piggybacking, and membership change cascade.

## Actor Registration

`register_actor(addr, generation)`:
1. Sign a `DirectoryEntry` with the node's keypair.
2. Store in local `DirectoryShard`.
3. Insert into `LocationCache` (self is the host).
4. Register with `RepublishTracker` for periodic re-STORE.
5. Return the signed entry — caller STOREs to `r`-closest nodes.

See [actor_resolution.svg](../diagrams/actor_resolution.svg) for the full datapath.

## Actor Resolution

`resolve_actor(addr)` implements a 3-tier lookup chain:
1. **LRU cache** — instant, O(1).
2. **Local directory shard** — checks entries this node holds.
3. **Kademlia lookup** — returns `NeedsLookup` with closest known nodes;
   caller drives iterative FIND_VALUE + quorum read.

On delivery failure, `invalidate_cache(addr)` evicts stale entries.

## Membership Change Cascade

When SWIM detects a state change, `handle_membership_change(node_id, state)`
propagates effects through all subsystems:

| State | Actions |
|-------|---------|
| **Alive** | Insert/update in `RoutingTable` |
| **Suspect** | Keep in `RoutingTable` (could downprioritize) |
| **Dead** | Remove from `RoutingTable`, invalidate cache entries for that node, queue affected directory entries in `RepairQueue` |

This cascade ensures that a single SWIM death detection triggers routing
table cleanup, cache invalidation, and directory repair in one tick.

## NodeDriver — TCP Network Bridge

`NodeDriver` (`driver.rs`) bridges the pure state machine API with real TCP
networking. It owns a `DistributedNode` plus a `TcpTransport` (connection
pool) and `TcpAcceptor` (non-blocking listener).

```
┌─ NodeDriver ─────────────────────────────────────────────────┐
│                                                               │
│  node: DistributedNode     ← pure state machine              │
│  transport: TcpTransport   ← connection pool for outgoing    │
│  acceptor: TcpAcceptor     ← non-blocking listener           │
│  streams: Vec<TcpStream>   ← accepted connections            │
│                                                               │
│  tick()  ──► node.tick() → map NodeAction → TCP send          │
│  recv()  ──► acceptor.try_recv() → dispatch → handler calls   │
│  join()  ──► node.join() → send JoinRequest via TCP           │
│                                                               │
└───────────────────────────────────────────────────────────────┘
```

The caller runs a loop: `recv()` → `tick()` → sleep. The driver handles
all TCP I/O internally — the caller never touches sockets directly.

See [DOCKER_REALIZATION.md](../development_history/DOCKER_REALIZATION.md)
for implementation details of the driver, the node binary (`crates/node/`),
and the Docker cluster integration tests.

## Dashboard REST API

The runtime dashboard exposes `/api/distribution` (feature-gated with
`distribution`) which returns the `DistributionNodeSnapshot` as JSON.
This supplements the SSE stream (`/events`) with a synchronous polling
endpoint used by integration tests.

## Where Things Live

| Type | File | Role |
|------|------|------|
| `DistributedNode` | `node.rs` | Top-level integration facade |
| `DistributedNodeConfig` | `node.rs` | Node configuration |
| `ResolveResult` | `node.rs` | 3-tier resolution outcomes |
| `NodeDriver` | `driver.rs` | TCP ↔ NodeAction bridge |
| `LocationCache` | `cache.rs` | LRU actor→node cache |
| `Keypair` | `crypto.rs` | Ed25519 keypair + signing |
| `NodeId` | `types.rs` | 32-byte node identity |
| `MemberState` | `types.rs` | Alive / Suspect / Dead |
| `DirectoryEntry` | `types.rs` | Signed actor→node binding |
| `NodeRecord` | `types.rs` | Wire-format membership record |
