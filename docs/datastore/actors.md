# Datastore Actor Reference

## Overview

The datastore is built from four actors within the swactor runtime. `DatastoreNode` is the public facade — all external requests (HTTP API, network protocol) enter through it and are routed to two long-lived worker actors: `BlobStoreActor` (content-addressed chunk/manifest I/O) and `MetadataActor` (object index, DHT replication, GC). A fourth actor, `TransferActor`, is spawned ephemerally for each remote download and self-terminates on completion or failure.

```
                        ┌─────────────────────────┐
                        │     store_node (main)    │
                        │  spawns all 3 long-lived │
                        │  actors, drives ticks    │
                        └────┬──────┬──────┬───────┘
                             │      │      │
                    spawn    │      │      │   spawn
              ┌──────────────┘      │      └──────────────┐
              ▼                     │ spawn                ▼
  ┌───────────────────┐             │          ┌───────────────────┐
  │  BlobStoreActor   │             │          │  MetadataActor    │
  │  (chunks, manifests)│           │          │  (index, DHT, GC) │
  └─────────▲─────────┘            │          └──▲────────┬───────┘
            │                       ▼             │        │
            │            ┌───────────────────┐    │        │
            │            │  DatastoreNode    │────┘        │
            │            │  (facade/router)  │─────────────┘
            └────────────│                   │
                         └────────┬──────────┘
                                  │ spawns (per download)
                                  ▼
                         ┌───────────────────┐
                         │  TransferActor    │
                         │  (ephemeral)      │
                         └───────────────────┘

  Arrows: ──▶ sends messages to
```

## Actors

### DatastoreNode

| | |
|---|---|
| **Role** | Top-level coordinator/facade. Accepts user-facing commands and incoming network protocol messages, delegates all work to `BlobStoreActor` and `MetadataActor`. |
| **Source** | `crates/datastore/src/actors/datastore_node.rs` |
| **Spawned by** | `store_node` binary (`crates/datastore/src/bin/store_node.rs:188`) |
| **Lifecycle** | Long-lived — runs for the lifetime of the process |

**Inbound messages** (`DatastoreNodeMsg` — 11 variants):

User-facing commands:
- `Put { data, name, tags, reply_to }` — chunk a blob, write chunks + manifest to `BlobStoreActor`, register in `MetadataActor`
- `Get { content_hash, reply_to }` — retrieve object metadata + manifest via `MetadataActor`
- `Delete { content_hash, reply_to }` — remove object via `MetadataActor`
- `List { name_filter, all, reply_to }` — list objects (local or swarm-wide) via `MetadataActor`
- `Status { reply_to }` — return this node's `NodeId`
- `ReadChunk { hash, reply_to }` — read a single chunk via `BlobStoreActor`

Protocol routing (incoming network messages):
- `IncomingGetChunk` — forwards to `BlobStoreActor::ReadChunk`
- `IncomingGetManifest` — forwards to `BlobStoreActor::ReadManifest`
- `IncomingStoreObject` — forwards to `MetadataActor::HandleStoreObject`
- `IncomingFindObject` — forwards to `MetadataActor::HandleFindObject`
- `IncomingListObjects` — forwards to `MetadataActor::ListLocal`

**Key outbound messages:**
- `BlobStoreMsg::WriteChunk`, `WriteManifest`, `ReadChunk`, `ReadManifest` — to `BlobStoreActor`
- `MetadataMsg::PutObject`, `GetObject`, `DeleteObject`, `ListLocal`, `ListSwarm`, `HandleStoreObject`, `HandleFindObject` — to `MetadataActor`
- `DatastoreResponse::NodeStatus` — directly to caller for `Status`

---

### BlobStoreActor

| | |
|---|---|
| **Role** | Content-addressed storage for chunks and manifests. All I/O goes through a pluggable `StorageBackend` (filesystem or in-memory). |
| **Source** | `crates/datastore/src/actors/blob_store.rs` |
| **Spawned by** | `store_node` binary (`store_node.rs:178`) |
| **Lifecycle** | Long-lived — runs for the lifetime of the process |

**Inbound messages** (`BlobStoreMsg` — 8 variants):

Chunk operations:
- `WriteChunk { hash, data, reply_to }` — persist a chunk, reply `ChunkStored`
- `ReadChunk { hash, reply_to }` — read a chunk, reply `ChunkOk` or `NotFound`
- `DeleteChunk { hash }` — remove a chunk (fire-and-forget)
- `HasChunk { hash, reply_to }` — existence check, reply `Bool`
- `ListChunks { reply_to }` — list all chunk hashes, reply `ChunkList`
- `GcUnreferenced { referenced }` — delete chunks not in the referenced set (fire-and-forget)

Manifest operations:
- `WriteManifest { manifest, reply_to }` — persist a manifest, reply `ManifestStored`
- `ReadManifest { hash, reply_to }` — read a manifest, reply `ManifestOk` or `NotFound`

**Key outbound messages:**
- `DatastoreResponse` variants (`ChunkStored`, `ChunkOk`, `ManifestStored`, `ManifestOk`, `NotFound`, `Error`, `Bool`, `ChunkList`) — always back to the `reply_to` address

---

### MetadataActor

| | |
|---|---|
| **Role** | Object metadata index. Maintains a `HashMap<ContentHash, ObjectEntry>` and a manifest cache. Handles DHT-style find/store operations, epidemic dissemination of entries to peers, and periodic garbage collection. |
| **Source** | `crates/datastore/src/actors/metadata.rs` |
| **Spawned by** | `store_node` binary (`store_node.rs:185`) |
| **Lifecycle** | Long-lived — runs for the lifetime of the process |

**Inbound messages** (`MetadataMsg` — 11 variants):

Object operations:
- `PutObject { entry, manifest, reply_to }` — store metadata + manifest locally, enqueue for dissemination, reply `PutOk`
- `GetObject { content_hash, reply_to }` — local lookup, reply `GetOk` or `NotFound`
- `DeleteObject { content_hash, reply_to }` — remove from local index, reply `DeleteOk` or `NotFound`
- `ListLocal { name_filter, reply_to }` — list local entries with optional name filter, reply `ListOk`
- `ListSwarm { name_filter, reply_to }` — swarm-wide list (currently delegates to `ListLocal`)

DHT protocol:
- `HandleFindObject { from, content_hash, reply_to }` — answer an incoming FIND_VALUE from a peer
- `HandleStoreObject { entry, manifest }` — accept an incoming STORE from a peer (fire-and-forget)

Peer management:
- `SetPeers { peers }` — update the list of peer `MetadataActor` addresses for dissemination

Periodic ticks (driven by the `store_node` main loop):
- `DisseminateTick` — send pending entries to all known peers
- `GcTick` — collect referenced chunks from all manifests, send `BlobStoreMsg::GcUnreferenced` to `BlobStoreActor`

**Key outbound messages:**
- `DatastoreResponse` variants (`PutOk`, `GetOk`, `DeleteOk`, `ListOk`, `NotFound`, `Error`) — to caller
- `MetadataMsg::HandleStoreObject` — to peer `MetadataActor` addresses during dissemination
- `BlobStoreMsg::GcUnreferenced` — to local `BlobStoreActor` during GC

---

### TransferActor

| | |
|---|---|
| **Role** | Manages a single object download from a remote node. Tracks pending/received chunks, forwards received data to the local `BlobStoreActor`, and reports completion or failure to the original requester. |
| **Source** | `crates/datastore/src/actors/transfer.rs` |
| **Spawned by** | `DatastoreNode` (one per remote download) |
| **Lifecycle** | Ephemeral — self-terminates via `ctx.stop_self()` on completion, failure, or cancel |

**Inbound messages** (`TransferMsg` — 4 variants):

- `StartDownload { manifest, source_node, reply_to }` — initialize the download with a manifest and source
- `ChunkReceived { hash, data }` — a chunk arrived from the remote node
- `ChunkFailed { hash, reason }` — a chunk fetch failed (retries up to `max_retries`, then fails the whole transfer)
- `Cancel` — abort the transfer immediately

**Key outbound messages:**
- `BlobStoreMsg::WriteChunk` — to local `BlobStoreActor` for each received chunk
- `DatastoreResponse::TransferComplete` — to `reply_to` when all chunks received
- `DatastoreResponse::TransferFailed` — to `reply_to` when retries are exhausted

---

## Message Reference

All message types are defined in `crates/datastore/src/messages.rs`.

### Intra-node actor messages

| Enum | Variants | Handled by |
|------|----------|------------|
| `DatastoreNodeMsg` | 11 (6 user-facing + 5 protocol routing) | `DatastoreNode` |
| `BlobStoreMsg` | 8 (5 chunk ops + 1 GC + 2 manifest ops) | `BlobStoreActor` |
| `MetadataMsg` | 11 (5 object ops + 2 DHT + 1 peer mgmt + 2 ticks) | `MetadataActor` |
| `TransferMsg` | 4 (start + chunk received + chunk failed + cancel) | `TransferActor` |

### Shared response enum

`DatastoreResponse` — 15 variants used as the return type for all four actors:

| Variant | Meaning |
|---------|---------|
| `PutOk { content_hash }` | Object stored successfully |
| `GetOk { entry, manifest }` | Object found |
| `DeleteOk { content_hash }` | Object deleted |
| `ListOk { entries }` | List result |
| `ChunkOk { hash, data }` | Chunk data retrieved |
| `ChunkStored { hash }` | Chunk written to storage |
| `ManifestStored { hash }` | Manifest written to storage |
| `ManifestOk { manifest }` | Manifest retrieved |
| `TransferComplete { content_hash }` | All chunks downloaded |
| `TransferFailed { reason }` | Transfer failed |
| `NodeStatus { node_id }` | Node identity |
| `NotFound` | Resource not found |
| `Error { reason }` | Generic error |
| `Bool(bool)` | Boolean result (e.g. `HasChunk`) |
| `ChunkList { hashes }` | List of chunk hashes |

### Inter-node wire messages (NetworkMessage)

| Struct | Direction | Purpose |
|--------|-----------|---------|
| `GetChunkRequest` | requester → holder | Fetch a chunk by hash |
| `GetChunkResponse` | holder → requester | Return chunk data (or `None`) |
| `StoreObjectRequest` | origin → DHT peer | Kademlia STORE for object metadata |
| `FindObjectRequest` | requester → DHT peer | Kademlia FIND_VALUE for object metadata |
| `FindObjectResponse` | DHT peer → requester | Return `Found(entry)` or `Closer(nodes)` |
| `GetManifestRequest` | requester → holder | Fetch a manifest by content hash |
| `GetManifestResponse` | holder → requester | Return manifest (or `None`) |
| `ListObjectsRequest` | requester → peer | List objects with optional name filter |
| `ListObjectsResponse` | peer → requester | Return matching entries |

Wire messages are distinguished from intra-node messages by implementing the `NetworkMessage` trait with a stable `type_tag()` string. They are serialized with serde for transport over iroh/QUIC.

---

## Key Flows

### Put (store a blob)

```
Client → DatastoreNode::Put
  → chunk_blob() splits data into chunks
  → BlobStoreActor::WriteChunk  (for each chunk, fire-and-forget)
  → BlobStoreActor::WriteManifest
  → MetadataActor::PutObject
    → stores entry + manifest locally
    → enqueues for dissemination
    → replies DatastoreResponse::PutOk
```

### Get (retrieve metadata)

```
Client → DatastoreNode::Get
  → MetadataActor::GetObject
    → local index lookup
    → replies DatastoreResponse::GetOk  (or NotFound)
```

### Data (reassemble from chunks)

See [streaming.md](streaming.md) for the full transfer protocol. In summary:

```
API server → DatastoreNode::Get → MetadataActor (local miss)
  → iterate peers:
    → FindObjectRequest  (wire) → peer MetadataActor
    → GetManifestRequest (wire) → peer BlobStoreActor
    → GetChunkRequest    (wire) → peer BlobStoreActor  (per chunk)
    → BlobStoreActor::WriteChunk (store locally)
  → BlobStoreActor::WriteManifest
  → MetadataActor::PutObject
  → reassemble_blob() → verify blake3 → respond
```

### Dissemination (epidemic replication)

```
store_node main loop (every disseminate_interval ticks)
  → MetadataActor::DisseminateTick
    → take_pending() selects entries with remaining budget
    → for each peer: MetadataActor::HandleStoreObject
      → peer inserts if absent, re-enqueues for further dissemination
```

Budget per entry = `Λ * ceil(log2(cluster_size))` (SWIM-style, Λ=3).

### GC (garbage collection)

```
store_node main loop (every gc_interval ticks)
  → MetadataActor::GcTick
    → scans all manifests → builds referenced chunk set
    → BlobStoreActor::GcUnreferenced { referenced }
      → deletes any chunk not in the referenced set
```

---

## Related

- [streaming.md](streaming.md) — chunking, transfer protocol, reassembly, and progress tracking
