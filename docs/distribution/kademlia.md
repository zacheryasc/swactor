# Kademlia DHT

The Kademlia layer provides a distributed actor directory. Actors are mapped
to nodes using XOR-distance routing over a 256-bit keyspace, with iterative
lookups, quorum reads, and automatic repair on node failure.

## Routing Table

256 k-buckets indexed by `XOR(self_id, target).leading_zeros()`. Each bucket
holds up to `K` nodes (default 20) in LRU order — most-recently-seen at tail.

```
┌─ RoutingTable ──────────────────────────────────────────────────────────┐
│                                                                         │
│  self_id: NodeId                                                        │
│  buckets: [KBucket; 256]                                                │
│                                                                         │
│  Bucket[i] holds nodes where XOR distance has exactly i leading zeros.  │
│  Bucket 0 = farthest half of keyspace.                                  │
│  Bucket 255 = nearest neighbor.                                         │
│                                                                         │
│  Each bucket:                                                           │
│    nodes:        VecDeque<NodeEntry>   (LRU, capacity K)                │
│    replacements: VecDeque<NodeEntry>   (overflow cache)                 │
│                                                                         │
│  On insert into a full bucket:                                          │
│    → new node goes to replacement cache                                 │
│    → only promoted when an existing node is evicted                     │
│    → prefers long-lived nodes (Kademlia stability heuristic)            │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

The `closest(target, count)` method scans all buckets, sorts by XOR distance,
and returns the `count` nearest entries. This is used both for iterative
lookups and for selecting STORE targets.

## Iterative Lookup

See [actor_resolution.svg](../diagrams/actor_resolution.svg) for the registration and
resolution datapaths.

The `NodeLookup` state machine drives iterative `FIND_NODE`:

1. Seed with the `K` closest nodes from the local routing table.
2. Query `alpha` (default 3) closest unqueried nodes in parallel.
3. Responses bring new closer nodes — incorporate and repeat.
4. Terminate when all `K` closest nodes have been queried, or max rounds (20).

```
  Start(target)
      │
      v
  seed = routing_table.closest(target, K)
      │
      v
  ┌───────────────────┐
  │  next_round()     │◄──────────────────┐
  │  pick α closest   │                   │
  │  unqueried nodes  │                   │
  └────────┬──────────┘                   │
           │                              │
           v                              │
  ┌───────────────────┐      ┌────────────┴───────────┐
  │ Query(node, addr) │─────►│ handle_response(from,  │
  │ × α in parallel   │      │   closer_nodes)        │
  └───────────────────┘      │ incorporate new nodes   │
                             └────────────────────────┘
           │
      all K closest queried
      or max_rounds exceeded
           │
           v
  Done { closest: Vec<(NodeId, SocketAddr)> }
```

The lookup is pure — it produces `LookupAction`s and the caller translates
them into network I/O.

## Directory Shards & Quorum

Each node holds a `DirectoryShard` — a local map from `ActorAddress` to
`Vec<DirectoryEntry>`. Entries are signed by the spawning node's keypair:

```rust
pub struct DirectoryEntry {
    pub actor_addr: ActorAddress,
    pub node_id: NodeId,          // who spawned the actor
    pub generation: u64,          // bumped on re-registration
    pub signature: Signature,     // ed25519 over (actor_addr, node_id, generation)
}
```

STORE replicates entries to the `r` closest nodes in XOR space.
FIND_VALUE does quorum reads with signature verification:

- Entries are grouped by `(node_id, generation)`.
- Groups meeting quorum (`f + 1` agreement) are candidates.
- Highest generation among quorum groups wins.
- Invalid signatures are silently rejected.

Resolution returns `QuorumResult::Resolved(entry)`,
`QuorumResult::NoQuorum(all)`, or `QuorumResult::NotFound`.

## Repair & Republish

Two mechanisms maintain directory integrity under churn:

**RepairQueue** — On node death, `on_node_death(dead_id, shard)` extracts
all entries authored by the dead node and queues them for re-STORE to the
new `r`-closest nodes.

**RepublishTracker** — Each node periodically re-STOREs its locally-spawned
actor entries at a configurable interval. This counteracts topology drift:
as nodes join and leave, the "r-closest" set shifts, and republishing ensures
entries migrate to the current closest nodes.

See `crates/distribution/DESIGN_NOTES.md` for the full rationale behind
re-replication and republishing.

## Where Things Live

| Type | File | Role |
|------|------|------|
| `RoutingTable` | `kademlia/routing_table.rs` | 256 k-buckets, XOR routing |
| `KBucket` | `kademlia/routing_table.rs` | LRU bucket + replacement cache |
| `NodeEntry` | `kademlia/routing_table.rs` | Node in routing table |
| `NodeLookup` | `kademlia/lookup.rs` | Iterative FIND_NODE state machine |
| `LookupAction` | `kademlia/lookup.rs` | Lookup I/O actions |
| `DirectoryShard` | `kademlia/directory.rs` | Local directory storage |
| `QuorumResult` | `kademlia/directory.rs` | Quorum read outcomes |
| `RepairQueue` | `kademlia/repair.rs` | Death-triggered re-replication |
| `RepublishTracker` | `kademlia/repair.rs` | Periodic re-STORE |
| `NodeId` | `types.rs` | 256-bit identity / XOR key |
| `DirectoryEntry` | `types.rs` | Signed actor→node binding |
| `Signature` | `types.rs` | Ed25519 signature (64 bytes) |
