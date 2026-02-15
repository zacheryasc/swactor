# Swactor Datastore Protocol Specification

**Version:** 0.2.0 (MVP)
**Status:** Draft

## 1. Overview

The Swactor Datastore is a distributed personal file/blob storage protocol for small trusted clusters (laptop, phone, browser). It provides content-hash-first addressing with immutable content-addressed objects, replicated via a Kademlia-based metadata DHT.

### Design Principles

- **Content-hash-first addressing** — every object is identified by `blake3(blob_bytes)`. This is the primary key for all operations.
- **Immutable content-addressed objects** — content hashes are unique identifiers. There are no write conflicts by construction.
- **Names are metadata** — optional flat strings attached to objects, not keys. Multiple objects can share a name; distinguished by content hash.
- **Separation of data and metadata** — chunks are large opaque blobs; metadata is small, gossiped, and queryable.
- **Crash-safe** — fsync before acknowledge on all writes.
- **Actor-based** — three actor types coordinate via message passing within the swactor runtime.
- **Transport-agnostic** — protocol messages defined as `NetworkMessage` types; MVP uses iroh (QUIC + NAT hole-punch + encryption).
- **Pluggable storage** — `StorageBackend` trait abstracts I/O for filesystem (MVP), IndexedDB (browser), etc.

## 2. Terminology

| Term | Definition |
|------|-----------|
| **Object** | Content-addressed blob identified by `blake3(blob_bytes)`. May carry an optional human-readable name as metadata. |
| **Blob** | The raw byte content of an object. |
| **Chunk** | A fixed-size (1 MB default) slice of a blob, identified by its blake3 content hash. |
| **Manifest** | An ordered list of `ChunkRef`s describing how to reassemble an object from chunks. Stored under the object's content hash. |
| **ContentHash** | 32-byte blake3 digest. Primary identifier for blobs and DHT key. |
| **ObjectEntry** | Metadata record: content hash, optional name, owner node, tags. |
| **DHT overlay** | A Kademlia distributed hash table for object metadata, separate from the actor directory DHT. Keys are `blake3(blob_bytes)`. |
| **StorageBackend** | Trait abstracting chunk and manifest I/O for pluggable backends (filesystem, IndexedDB, etc.). |
| **Node** | A device running the swactor runtime with a datastore actor set (BlobStoreActor + MetadataActor). |

## 3. Data Model

### 3.1 ContentHash

```
ContentHash = blake3(data)[0..32]    // 32 bytes
```

- **Hashing algorithm:** blake3 — 2-3x faster than sha256, tree-hashable (parallel hashing of large chunks), same 32-byte output. Supports streaming hashing for large blobs via `blake3::Hasher`.
- **Display:** first 8 bytes as hex + ellipsis (e.g. `a1b2c3d4e5f6a7b8…`).
- **XOR distance:** bitwise XOR of the 32-byte arrays, used for Kademlia routing in the metadata DHT.

### 3.2 ObjectEntry

```
ObjectEntry {
    content_hash:  ContentHash,              // blake3(entire_blob) — primary identifier
    name:          Option<String>,           // Optional flat string, not a path
    node_id:       NodeId,                   // Node that stores the object
    tags:          BTreeMap<String, String>,  // User-defined key-value tags
    size_bytes:    u64,                      // Total object size
    created_at:    u64,                      // Wall-clock creation time (informational)
}
```

No conflict resolution is needed — content hashes are unique identifiers. Storing the same blob twice is a no-op (same content hash). Different blobs always have different content hashes.

### 3.3 ObjectManifest

```
ObjectManifest {
    content_hash:  ContentHash,         // blake3(entire_blob) — NOT the hash of this manifest
    chunks:        Vec<ChunkRef>,       // Ordered list of chunks
    total_size:    u64,                 // Total object size in bytes
    chunk_size:    u32,                 // Fixed chunk size used (e.g. 1MB)
    content_type:  Option<String>,      // MIME type
}

ChunkRef {
    hash:   ContentHash,                // Content hash of chunk data
    offset: u64,                        // Byte offset in original object
    size:   u32,                        // Actual size (last chunk may be smaller)
}
```

The `content_hash` field is `blake3(entire_blob)`, computed via a streaming hasher alongside chunking. The manifest is stored and looked up using this content hash as the key.

### 3.4 Storage Backend

The `StorageBackend` trait abstracts all chunk and manifest I/O:

```rust
pub trait StorageBackend: Send {
    fn write_chunk(&mut self, hash: &ContentHash, data: &[u8]) -> Result<(), io::Error>;
    fn read_chunk(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>, io::Error>;
    fn delete_chunk(&mut self, hash: &ContentHash) -> Result<(), io::Error>;
    fn has_chunk(&self, hash: &ContentHash) -> bool;
    fn list_chunks(&self) -> Vec<ContentHash>;
    fn write_manifest(&mut self, manifest: &ObjectManifest) -> Result<(), io::Error>;
    fn read_manifest(&self, content_hash: &ContentHash) -> Result<Option<ObjectManifest>, io::Error>;
    fn delete_manifest(&mut self, content_hash: &ContentHash) -> Result<(), io::Error>;
}
```

#### MVP: FilesystemBackend

Two-level directory sharding to avoid huge directories:

```
{storage_path}/
├── chunks/
│   └── {hex[0..2]}/
│       └── {hex[2..4]}/
│           └── {full_hex_hash}        # Raw chunk bytes
└── manifests/
    └── {hex[0..2]}/
        └── {hex[2..4]}/
            └── {full_hex_hash}        # JSON-serialized ObjectManifest
```

Example: chunk with hash `abcdef12...` is stored at `chunks/ab/cd/abcdef12...`.

All writes are fsynced before acknowledging.

## 4. Content Addressing

### 4.1 Chunking Algorithm

Fixed-size chunking (MVP):

1. Read the input file in `chunk_size` byte blocks (default: 1,048,576 = 1 MB).
2. For each block, compute `ContentHash::of(block)`.
3. Store each chunk via the `StorageBackend`.
4. Build a `Vec<ChunkRef>` with sequential offsets.
5. Compute `content_hash = blake3(entire_blob)` using a streaming hasher fed alongside chunking.
6. Create the `ObjectManifest` with this `content_hash` and store it via the `StorageBackend` keyed by `content_hash`.

The last chunk may be smaller than `chunk_size`.

### 4.2 Reassembly

1. Read the `ObjectManifest` (by its `content_hash`).
2. For each `ChunkRef` in order, read the chunk by `hash`.
3. Concatenate all chunk data to reconstruct the original blob.
4. Verify: `blake3(reassembled) == content_hash` (optional integrity check).

## 5. Metadata DHT

> **Status:** Types and routing table logic exist in the `distribution` crate. `MetadataActor` has a dissemination queue (`enqueue`/`take_pending`) and peer-to-peer replication via `SetPeers` + `DisseminateTick`. Verified in local multi-node simulation. Full Kademlia iterative lookup (FIND_VALUE with α-parallel queries) is not yet implemented — dissemination is epidemic/gossip-style.

### 5.1 Overlay Design

The metadata DHT is a **separate Kademlia overlay** from the actor directory. It stores `ObjectEntry` records keyed by `blake3(blob_bytes)` — the content hash of the entire blob.

This separation ensures:
- Object metadata routing doesn't interfere with actor discovery.
- Different replication factors can be used (objects may be stored on fewer nodes).
- The DHT can be independently tuned for the metadata workload.

### 5.2 Key Mapping

```
DHT key = blake3(blob_bytes) = entry.content_hash
```

### 5.3 Store Flow

When storing object metadata:
1. Use `key = entry.content_hash`.
2. Find the `k` closest nodes to `key` in the metadata DHT routing table.
3. Send `StoreObjectRequest { entry }` to each of the `k` closest nodes.

### 5.4 Lookup Flow

When looking up object metadata:
1. Send `FindObjectRequest { content_hash }` to the `α` closest known nodes.
2. Each node responds with either `Found(ObjectEntry)` or `Closer(Vec<(NodeId, SocketAddr)>)`.
3. Continue querying closer nodes until convergence.

No merge step is needed — content hashes are unique identifiers.

## 6. Protocol Flows

### 6.1 PUT — Store an Object

```
User                    MetadataActor           BlobStoreActor
 │                           │                        │
 │─── PutObject ────────────>│                        │
 │                           │                        │
 │                           │  (chunk the file,      │
 │                           │   stream blake3 hash)  │
 │                           │                        │
 │                           │─── WriteChunk ────────>│
 │                           │<── ChunkStored ────────│  (repeat for each chunk)
 │                           │                        │
 │                           │─── WriteManifest ─────>│
 │                           │<── ManifestStored ─────│
 │                           │                        │
 │                           │  (create ObjectEntry,  │
 │                           │   store in local index,│
 │                           │   enqueue for DHT      │
 │                           │   dissemination)       │
 │                           │                        │
 │<── PutOk {content_hash} ─│                        │
```

### 6.2 GET — Retrieve an Object (Local)

```
User                    MetadataActor           BlobStoreActor
 │                           │                        │
 │─── GetObject ────────────>│                        │
 │   {content_hash}          │                        │
 │                           │  (lookup content_hash  │
 │                           │   in local index)      │
 │                           │                        │
 │<── GetOk { entry,        │                        │
 │          manifest } ──────│                        │
 │                                                    │
 │  (for each chunk in manifest)                      │
 │───────────── ReadChunk ───────────────────────────>│
 │<────────────── ChunkOk ───────────────────────────│
 │                                                    │
 │  (reassemble chunks into original file)            │
```

### 6.3 GET — Retrieve an Object (Remote)

> **Status:** `TransferActor` state machine is functional and stores received chunks to the local `BlobStoreActor`. Chunks must be fed externally (via `ChunkReceived` messages). Automatic chunk pulling from remote nodes is not yet implemented — the test harness or a future network adapter plays the "pull" role. Verified in multi-node simulation.

```
User        MetadataActor       TransferActor       Remote BlobStore
 │               │                    │                     │
 │─ GetObject ──>│                    │                     │
 │ {content_hash}│                    │                     │
 │               │  (not in local     │                     │
 │               │   index; DHT       │                     │
 │               │   lookup)          │                     │
 │               │                    │                     │
 │               │─ StartDownload ───>│                     │
 │               │                    │                     │
 │               │                    │── GetChunkRequest ─>│
 │               │                    │<─ GetChunkResponse ─│
 │               │                    │                     │
 │               │                    │  (repeat for each   │
 │               │                    │   chunk)            │
 │               │                    │                     │
 │               │<─ TransferComplete │                     │
 │               │                    │                     │
 │<── GetOk ────│                    │  (stops self)       │
```

### 6.4 DELETE — Remove an Object

```
User                    MetadataActor
 │                           │
 │─── DeleteObject ─────────>│
 │   {content_hash}          │
 │                           │
 │                           │  (remove from local   )
 │                           │  (index, best-effort   )
 │                           │  (notify DHT peers     )
 │                           │
 │<── DeleteOk              │
 │   {content_hash} ────────│
```

Chunk data is **not** immediately deleted. Unreferenced chunks are cleaned up during GC sweeps (see Section 10).

### 6.5 LIST — List Objects (Local)

```
User                    MetadataActor
 │                           │
 │─── ListLocal ────────────>│
 │   {name_filter}           │
 │                           │
 │                           │  (filter local index   )
 │                           │  (by name substring    )
 │                           │
 │<── ListOk { entries } ───│
```

### 6.6 LIST — List Objects (Swarm-Wide)

> **Status:** `ListSwarm` currently delegates to `ListLocal` (returns local entries only). Fan-out to peer MetadataActors is not yet wired. Swarm-wide listing is verified in simulation by querying each node and merging results in the test harness.

```
User        MetadataActor           Remote MetadataActors
 │               │                        │
 │─ ListSwarm ──>│                        │
 │ {name_filter} │                        │
 │               │── ListObjectsRequest ─>│  (fan-out to all known
 │               │<─ ListObjectsResponse ─│   alive nodes)
 │               │                        │
 │               │  (merge all results,   │
 │               │   deduplicate by       │
 │               │   content hash)        │
 │               │                        │
 │<── ListOk ───│                        │
```

## 7. Actor Architecture

> **Status:** All three actor types are fully implemented and tested. `DatastoreNode` coordinator routes commands to internal actors. 83+ tests across 8 test files verify single-node operations. Multi-node dissemination and cross-node transfers verified in simulation.

### 7.1 BlobStoreActor

**Responsibility:** Chunk and manifest I/O via `StorageBackend` trait.

- **State:** `Box<dyn StorageBackend>`
- **Lifecycle:** Long-lived, one per node.
- **Guarantees:** Delegates to backend; filesystem backend fsyncs before acknowledging.

**Message types:** `BlobStoreMsg` (see `messages.rs`)

### 7.2 MetadataActor

**Responsibility:** Object metadata index, DHT routing.

- **State:** Local object index (`HashMap<ContentHash, ObjectEntry>`), manifest cache, dissemination queue.
- **Lifecycle:** Long-lived, one per node.
- **Coordinates with:** BlobStoreActor (for manifest storage), remote MetadataActors (DHT operations).

**Message types:** `MetadataMsg` (see `messages.rs`)

### 7.3 TransferActor

**Responsibility:** Downloading an object (all its chunks) from a remote node.

- **State:** Manifest, pending/received chunk sets, retry counts.
- **Lifecycle:** Ephemeral — spawned per download, self-terminates on completion/failure/cancel.
- **Coordinates with:** Remote BlobStoreActor (chunk requests), local BlobStoreActor (chunk storage).

**Message types:** `TransferMsg` (see `messages.rs`)

## 8. Wire Protocol

> **Status:** All message types are defined with `NetworkMessage` implementations and stable type tags. Serialization is JSON (serde). No transport integration yet — messages are passed directly via actor addresses in simulation.

### 8.1 Message Types

All inter-node messages implement `NetworkMessage` with a stable `type_tag()`:

| Message | type_tag | Direction |
|---------|----------|-----------|
| `GetChunkRequest` | `swactor_datastore::GetChunkRequest` | requester → holder |
| `GetChunkResponse` | `swactor_datastore::GetChunkResponse` | holder → requester |
| `StoreObjectRequest` | `swactor_datastore::StoreObjectRequest` | writer → DHT nodes |
| `FindObjectRequest` | `swactor_datastore::FindObjectRequest` | reader → DHT nodes |
| `FindObjectResponse` | `swactor_datastore::FindObjectResponse` | DHT node → reader |
| `GetManifestRequest` | `swactor_datastore::GetManifestRequest` | requester → holder |
| `GetManifestResponse` | `swactor_datastore::GetManifestResponse` | holder → requester |
| `ListObjectsRequest` | `swactor_datastore::ListObjectsRequest` | requester → remote node |
| `ListObjectsResponse` | `swactor_datastore::ListObjectsResponse` | remote node → requester |

`FindObjectRequest` contains a `content_hash` field (the `blake3(blob_bytes)` key).

### 8.2 Serialization

MVP: serde JSON for all messages. Binary format (bincode or msgpack) planned for later to reduce overhead, especially for `GetChunkResponse` which carries large payloads.

### 8.3 Framing

Messages are framed over iroh QUIC streams:
- Each request/response pair uses a single bidirectional stream.
- Message format: `[4-byte length (big-endian)][JSON payload]`.

## 9. Naming

Names are **optional flat strings** — human-readable labels attached to objects as metadata.

- Names are not keys. The content hash is the only primary identifier.
- Multiple objects can share the same name. They are distinguished by content hash.
- Names are simple strings (e.g. `"vacation.jpg"`, `"backup-2024-01"`). No path hierarchy, no separators enforced.
- No conflict resolution is needed — different content always produces different content hashes.

## 10. Garbage Collection

> **Status:** Fully implemented. `MetadataActor::gc_tick()` builds a referenced chunk set from all local manifests and sends `GcUnreferenced` to `BlobStoreActor`. Verified with 6 GC-specific tests including deduplication safety, interval gating, and empty-store edge case.

### 10.1 Entry Removal

Deleting an object:
1. Remove the `ObjectEntry` from the local index.
2. Best-effort notify DHT peers to remove their replicas.
3. Remove the local manifest.

### 10.2 Chunk Reference Counting

Unreferenced chunk cleanup:

1. Build a referenced set: union of all chunk hashes from all local manifest entries.
2. Send `BlobStoreMsg::GcUnreferenced { referenced }` to the BlobStoreActor.
3. BlobStoreActor diffs its chunk list against the referenced set and deletes unreferenced chunks.

**Safety:** A chunk may be referenced by multiple objects (deduplication). Only delete when zero references remain.

### 10.3 GC Schedule

- `MetadataActor` runs `gc_tick()` every tick. Actual GC sweep happens every `gc_interval` ticks (default: 1000).
- Chunk GC is triggered less frequently (order of minutes) to avoid overhead.

## 11. Failure Modes

### 11.1 Node Offline

- **Metadata persists** in the DHT (replicated to k-closest nodes). Lookups succeed as long as any replica is alive.
- **Chunk fetches fail** if the only copy is on the offline node. The TransferActor retries once, then reports failure.
- **Recovery:** When the node comes back, its metadata is re-disseminated (anti-entropy).

### 11.2 Transfer Interrupted

- **Partial state:** Some chunks may be written to the local BlobStoreActor before the transfer fails.
- **Cleanup:** Partially downloaded chunks are not harmful — they're content-addressed and may be useful for future downloads. Unreferenced chunks are cleaned up by GC.
- **Retry:** The user can retry the GET, and only missing chunks need to be fetched (future optimization).

### 11.3 DHT Inconsistency

- **Stale metadata:** A node may serve an outdated ObjectEntry. Anti-entropy dissemination ensures replicas converge.
- Content addressing eliminates write conflicts — storing the same content hash twice is idempotent.

### 11.4 Disk Full

- `StorageBackend::write_chunk` fails with an I/O error, which is propagated back to the requester as `DatastoreResponse::Error`.
- No partial writes — fsync ensures atomicity (filesystem backend).

## 12. CLI Interface

> **Status:** Command types defined in `src/cli.rs`. Parser, dispatcher, and `[[bin]]` target not yet implemented. Planned for a follow-up session.

```
swactor-store put <local-path> [--name <label>] [--tag key=value...]
    Store a local file as a distributed object.
    Returns the content hash of the stored object.
    --name sets an optional human-readable label.

swactor-store get <content-hash>[@<node>] [--output <local-path>]
    Retrieve an object by content hash. Fetches from the specified node or discovers via DHT.
    --output defaults to the object's name (if set) in the current directory.

swactor-store delete <content-hash>
    Remove an object from the local index and notify DHT peers.

swactor-store list [--name <substring>] [--node <node-name>] [--all]
    List objects. --name filters by name substring. --all queries all nodes (swarm-wide). Default is local.

swactor-store status
    Show node info: identity, chunk count, storage usage.
```

## 13. Browser API

> **Status:** Not started. Separate milestone.

WASM-exposed functions for browser integration:

```
list_objects(name_filter: Option<String>) -> Vec<ObjectEntry>
    List objects visible to this node, optionally filtered by name.

get_object(content_hash: ContentHash) -> Result<Vec<u8>, Error>
    Download and reassemble an object by content hash.

put_object(data: Vec<u8>, name: Option<String>) -> Result<ContentHash, Error>
    Chunk, store, and register an object. Returns the content hash.

delete_object(content_hash: ContentHash) -> Result<(), Error>
    Remove an object from the local index.

get_node_status() -> NodeStatus
    Node identity, chunk count, connected peers.
```

These map directly to the MetadataActor message types. The WASM runtime handles serialization across the JS/Rust boundary.
