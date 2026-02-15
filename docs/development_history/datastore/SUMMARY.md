# swactor-datastore: Development History & Status

## Overview

`swactor-datastore` is a distributed personal file/blob storage protocol for small trusted clusters (laptop, phone, browser). It provides content-hash-first addressing with immutable content-addressed objects, replicated via epidemic metadata dissemination across peers.

The implementation is organized as a single Rust crate (`crates/datastore/`) built on the `swactor` actor runtime. It was developed in 7 ordered modules (local single-node operations) followed by a multi-node simulation phase.

**Current state: 93 tests across 9 test files, all passing. Zero warnings.**

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   DatastoreNode                      │  Coordinator/facade
│            (single entry point for callers)           │
├────────────────────┬────────────────────────────────┤
│   MetadataActor    │         BlobStoreActor          │  Long-lived, one per node
│  (object index,    │     (chunk & manifest I/O       │
│   dissemination,   │      via StorageBackend)        │
│   GC orchestration)│                                 │
├────────────────────┴────────────────────────────────┤
│              TransferActor (ephemeral)                │  One per download
│         (chunk tracking, retry, self-termination)     │
├─────────────────────────────────────────────────────┤
│               StorageBackend trait                    │  Pluggable I/O
│    FilesystemBackend  │  InMemoryBackend             │
├─────────────────────────────────────────────────────┤
│            Chunking Engine (pure functions)           │  No I/O, deterministic
│     chunk_blob · reassemble_blob · verify_integrity  │
└─────────────────────────────────────────────────────┘
```

### Core Design Principles

- **Content-hash-first addressing** -- every object identified by `blake3(blob_bytes)`.
- **Immutable content-addressed objects** -- no write conflicts by construction.
- **Names are metadata** -- optional flat strings, not keys.
- **Separation of data and metadata** -- chunks are large opaque blobs; metadata is small and gossiped.
- **Actor-based** -- three actor types coordinate via message passing.
- **Transport-agnostic** -- protocol messages defined as `NetworkMessage` types.
- **Pluggable storage** -- `StorageBackend` trait abstracts I/O.

---

## Module-by-Module Development History

### Module 1: Chunking Engine

**What:** Pure functions for content-addressed blob chunking and reassembly. `chunk_blob()`, `reassemble_blob()`, `verify_integrity()`, plus `ContentHash`, `ObjectManifest`, `ChunkRef` types.

**Key decisions:**
- Fixed-size chunking over content-defined chunking (simpler, deterministic; CDC dedup unnecessary for small clusters)
- BLAKE3 for all hashing (3-7 GB/s, 32-byte output matching `NodeId`)
- Whole-blob hash as content hash rather than Merkle root of chunk hashes
- Empty blob produces a valid zero-chunk manifest

**Tests:** 13 (10 scenario + 3 proptest). Round-trips, edge cases, integrity verification, determinism.

**Files:** `src/chunking.rs`, `src/types.rs`

---

### Module 2: Storage Backend

**What:** `StorageBackend` trait with two implementations: `FilesystemBackend` (2-level hex-sharded dirs, fsync-on-write) and `InMemoryBackend` (HashMap-based for tests/WASM).

**Key decisions:**
- 2-level hex sharding (65,536 possible directories) to avoid hot directories
- In-memory chunk index for O(1) `has_chunk` lookups, populated via scan-on-init
- JSON manifest serialization for debuggability
- `Send` but not `Sync` on the trait (single-actor ownership)
- Idempotent writes and deletes

**Tests:** 12 (9 parameterized across both backends + 2 FS-only + 1 proptest).

**Files:** `src/storage/mod.rs`, `src/storage/in_memory.rs`

---

### Module 3: BlobStoreActor

**What:** Message-driven actor wrapping `Box<dyn StorageBackend>` for chunk/manifest CRUD plus garbage collection. Also introduced the shared test harness (`tests/common/mod.rs`).

**Key decisions:**
- Thin delegation -- actor adds no logic beyond message dispatch
- Explicit `reply_to` pattern (tell, not ask) for response routing
- Fire-and-forget deletes and GC (no reply needed)
- `Box<dyn StorageBackend>` for dynamic dispatch (one backend per instance)
- Single-threaded tick-based testing for determinism

**Tests:** 7 scenario tests through the swactor runtime.

**Established patterns:** `reply_to` pattern, shared test harness with `test_runtime()`, `tick_n()`, `tick_until_recv()`, `DatastoreHarness`.

**Files:** `src/actors/blob_store.rs`, `tests/blob_store_tests.rs`, `tests/common/mod.rs`

---

### Module 4: MetadataActor

**What:** Object metadata index (`HashMap<ContentHash, ObjectEntry>`), manifest cache, SWIM-inspired gossip dissemination queue. Handles local CRUD, DHT protocol messages (`HandleFindObject`, `HandleStoreObject`), and GC tick orchestration.

**Key decisions:**
- Node ID stamping on `PutObject` (prevents spoofing; remote entries retain original owner)
- Idempotent DHT store (insert-if-absent semantics)
- Synthetic empty manifest for `HandleFindObject` when manifest is missing
- Separate `GetObject` (local, error on missing manifest) vs `HandleFindObject` (DHT, synthesizes empty manifest)
- SWIM-style dissemination with budget `Lambda * ceil(log2(n))`, Lambda=3

**Tests:** 10 scenario tests covering CRUD, DHT operations, idempotency, lifecycle.

**Files:** `src/actors/metadata.rs`, `tests/metadata_tests.rs`

---

### Module 5: TransferActor

**What:** Ephemeral per-download actor. Tracks pending/received chunks, forwards received chunks to BlobStoreActor, implements per-chunk retry, self-terminates on completion/failure/cancel.

**Key decisions:**
- Ephemeral actor pattern (one per download, isolates transfer state)
- Passive design -- chunks driven externally via `ChunkReceived`/`ChunkFailed` (decoupled from networking)
- Whole-transfer failure on any chunk exhausting retries
- `max_retries` defaults to 1 (first failure allows retry, second aborts)
- Fire-and-forget chunk persistence (ChunkStored reply silently dropped)

**Tests:** 10 scenario tests covering state machine transitions, retry logic, cancellation, data recovery.

**Files:** `src/actors/transfer.rs`, `tests/transfer_tests.rs`

---

### Module 6: DatastoreNode Coordinator

**What:** Facade actor encapsulating BlobStoreActor + MetadataActor behind a single address. Routes 6 user-facing commands and 5 network protocol variants.

**Key decisions:**
- Pass-through `reply_to` pattern (responses go directly to caller, coordinator never intercepts)
- Inline chunking in `handle_put` (synchronous, no async coordination)
- Fire-and-forget chunk writes (same pattern as TransferActor)
- Immutable state after construction
- `Put` uses `data: Vec<u8>` not `PathBuf` (testable, WASM-compatible)

**Tests:** 12 scenario tests through `NodeHarness`.

**Files:** `src/actors/datastore_node.rs`, `tests/datastore_node_tests.rs`

---

### Module 7: Garbage Collection

**What:** Completed `MetadataActor::gc_tick()` to build a referenced chunk set from all manifests and send `GcUnreferenced` to BlobStoreActor for orphan cleanup.

**Key decisions:**
- `blob_store_addr = None` guard for backward compatibility (GC no-ops when not wired)
- Fire-and-forget `GcUnreferenced` (no reply needed)
- `spawn_metadata_with_config()` helper to wire blob_store_addr before spawning
- Mark-and-sweep: union of all chunk hashes from all manifests = referenced set

**Tests:** 6 scenario tests covering cleanup, preservation, deduplication safety, interval gating, empty-store edge case.

**Files:** `src/actors/metadata.rs` (delta), `tests/gc_tests.rs`, `tests/common/mod.rs` (GcHarness)

---

### Multi-Node Simulation (Phases 3-4)

**What:** Wired up MetadataActor for peer-to-peer metadata dissemination. Added `SetPeers` and `DisseminateTick` messages. Created `MultiNodeHarness` for simulating clusters on a single Runtime. Extended `HandleStoreObject` to carry manifests alongside entries for full metadata replication.

**Key decisions:**
- Single Runtime for simulation -- all nodes' actors share one Runtime; actor addresses are globally unique so cross-node messaging "just works" via `ctx.send()`
- MetadataActor owns peer relationships (simpler than routing through DatastoreNode)
- Manifest dissemination alongside entry dissemination (peers receive both)
- TransferActor stays passive -- tests feed chunks from remote BlobStoreActor (test harness plays the "network adapter" role)
- No automatic remote GET orchestration yet -- DatastoreNode remains a stateless router

**New messages:**
- `MetadataMsg::SetPeers { peers: Vec<ActorAddress> }`
- `MetadataMsg::DisseminateTick`
- `MetadataMsg::HandleStoreObject` extended with `manifest: Option<ObjectManifest>`

**Tests:** 10 new scenario tests in `tests/multi_node_tests.rs`:

| # | Test | Verifies |
|---|------|----------|
| 1 | `metadata_replicates_to_peer_after_dissemination` | Put on 0, disseminate, node 1 finds it |
| 2 | `metadata_replicates_to_all_peers_in_3_node_cluster` | Full cluster replication |
| 3 | `dissemination_budget_expires_after_enough_rounds` | Budget exhaustion, fresh entries still work |
| 4 | `delete_on_origin_does_not_propagate_to_peers` | Delete is local only |
| 5 | `duplicate_put_via_dissemination_is_idempotent` | No duplicate entries on peer |
| 6 | `find_object_on_peer_after_dissemination` | HandleFindObject succeeds on peer |
| 7 | `chunk_transfer_from_remote_blob_store` | TransferActor pulls chunks cross-node |
| 8 | `full_remote_get_scenario` | End-to-end: put on 0, disseminate, transfer 0->1, reassemble matches |
| 9 | `list_across_all_nodes_finds_objects_from_any_node` | Simulated ListSwarm fan-out |
| 10 | `gc_on_one_node_does_not_affect_other_nodes` | GC isolation between nodes |

**Files:** `src/messages.rs`, `src/actors/metadata.rs`, `tests/common/mod.rs` (MultiNodeHarness), `tests/multi_node_tests.rs`

---

## Code Quality Pass

Alongside the multi-node work, a cleanup pass was performed:

- **PROTOCOL.md** -- Added honest `> Status:` annotations to sections 5 (Metadata DHT), 6.3 (Remote GET), 6.6 (ListSwarm), 7 (Actor Architecture), 8 (Wire Protocol), 10 (GC), 12 (CLI), and 13 (Browser API)
- **metadata.rs** -- Updated stale `ListSwarm` "MVP" comment
- **transfer.rs** -- Updated architecture comments describing simulation-ready passive design
- **tests/common/mod.rs** -- Removed 3 unused imports (`ActorInterface`, `Ctx`, `DatastoreNodeMsg`)

---

## Test Summary

| Test File | Count | What |
|-----------|-------|------|
| `chunking_tests.rs` | 13 | Pure function round-trips, edge cases, proptests |
| `storage_tests.rs` | 12 | Backend CRUD, parameterized across FS + InMemory, proptests |
| `blob_store_tests.rs` | 7 | Actor-level chunk/manifest CRUD, GC |
| `metadata_tests.rs` | 10 | Object index, DHT protocol, lifecycle |
| `transfer_tests.rs` | 10 | Download state machine, retry, cancel |
| `datastore_node_tests.rs` | 12 | Coordinator routing, network protocol |
| `datastore_tests.rs` | 13 | Content-addressing properties, proptests |
| `gc_tests.rs` | 6 | Mark-and-sweep GC, dedup safety |
| `multi_node_tests.rs` | 10 | Dissemination, cross-node transfer, GC isolation |
| **Total** | **93** | |

**Testing philosophy:** Scenario/story tests first, property-based tests for invariants, contract tests for serialization. No white-box/structural tests. Low coupling to internals -- tests should survive a refactor.

---

## Future Work

### Near-Term (Next Sessions)

**CLI Implementation (Phase 2)**
- Parser and dispatcher using `clap`
- `[[bin]]` target in Cargo.toml
- `ContentHash::from_hex()` for CLI input
- Commands: `put <path>`, `fetch <hash>` (metadata only), `get <hash> --output <path>` (full download), `delete <hash>`, `list`, `status`
- Single-node only (no networking); spawns its own actor set
- Follow `crates/node/src/main.rs` pattern

**ListSwarm Fan-Out**
- Currently delegates to `ListLocal`. Wire MetadataActor to query all peers and merge/deduplicate results by content hash.

**Automatic Remote GET Orchestration**
- Currently, remote GET requires manual orchestration (test harness or external driver reads chunks from remote BlobStore and feeds them to TransferActor).
- DatastoreNode needs to become stateful: detect local miss, query peers via `HandleFindObject`, spawn TransferActor, coordinate chunk pulling from the remote BlobStoreActor.
- This is the largest remaining architectural change for local functionality.

### Medium-Term

**Transport Integration (iroh/QUIC)**
- Wire `NetworkMessage` types to actual network transport.
- DatastoreNode gains peer management (`AddPeer`/`RemovePeer`) at the node level.
- Replace simulation-only direct actor addressing with network-routed messages.
- Framing: `[4-byte length (big-endian)][JSON payload]` over QUIC streams.

**Anti-Entropy / Repair**
- When a node comes back online, re-disseminate its metadata to peers.
- Periodic full-index comparison between peers to detect and repair drift.

**Active Chunk Pulling in TransferActor**
- `StartDownload` sends `GetChunkRequest` to the source node for each chunk.
- Currently passive (chunks fed externally); make it drive its own downloads.

**Parallel Chunk Fetching**
- TransferActor currently fetches sequentially. Add configurable concurrency (`max_concurrent_transfers` in config already exists).

### Longer-Term

**Browser API (WASM)**
- Expose `list_objects`, `get_object`, `put_object`, `delete_object`, `get_node_status` via WASM bindings.
- Use `InMemoryBackend` (or IndexedDB backend) in the browser.
- Coordinate with `crates/wasm/` for the in-browser swactor runtime.

**Binary Wire Format**
- Replace JSON serialization with bincode or msgpack for `GetChunkResponse` and other payload-heavy messages.

**Content-Defined Chunking (CDC)**
- Replace fixed-size chunking with FastCDC or similar for better cross-object deduplication.
- Transparent to the rest of the system -- only `chunk_blob()` changes; manifest format is the same.

**Streaming / Large File Support**
- Current `Put` takes `data: Vec<u8>` (entire blob in memory). For large files, add streaming chunking that reads from a `Read` source.

**Delete Propagation**
- Currently, delete is local only (by design). Add optional "tombstone dissemination" to remove entries from peers.

**Replication Factor Control**
- Currently, dissemination is epidemic (all peers get everything). Add configurable k-closest replication for the metadata DHT.

**IndexedDB Backend**
- Implement `StorageBackend` for browser IndexedDB for persistent storage in web contexts.

---

## Key Files

| File | Purpose |
|------|---------|
| `src/types.rs` | Core types: `ContentHash`, `ObjectEntry`, `ObjectManifest`, `ChunkRef`, `DatastoreConfig` |
| `src/messages.rs` | All inter-node and intra-node message types |
| `src/chunking.rs` | Pure chunking/reassembly functions |
| `src/storage/mod.rs` | `StorageBackend` trait + `FilesystemBackend` |
| `src/storage/in_memory.rs` | `InMemoryBackend` |
| `src/actors/blob_store.rs` | Chunk/manifest I/O actor |
| `src/actors/metadata.rs` | Object index, dissemination, GC orchestration |
| `src/actors/transfer.rs` | Ephemeral download actor |
| `src/actors/datastore_node.rs` | Coordinator/facade |
| `src/cli.rs` | CLI command type definitions (types only, no implementation) |
| `PROTOCOL.md` | Protocol specification with status annotations |
| `PROTOCOL_IMPLEMENTATION_PLAN.md` | Original 7-module implementation plan |
| `tests/common/mod.rs` | Shared test harness: DatastoreHarness, GcHarness, MultiNodeHarness, NodeHarness |
