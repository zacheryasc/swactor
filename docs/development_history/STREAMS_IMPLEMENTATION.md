# Swactor Streams -- Implementation Status

## What Was Built

Stages 1-4 are implemented. Stages 1-3 built the stream primitive in `crates/streams/` (the `swactor-streams` crate). Stage 4 connected streams to the datastore so blob transfers use QUIC streams instead of sequential actor-message round-trips. All 42 tests pass (37 streams + 5 blob_transfer).

### Stage 1: Types, Wire Format, and Buffer Pool

Pure Rust -- no tokio, no iroh, no network. Compiles and tests in isolation.

#### `src/types.rs`

Core domain types for the stream system.

- **`StreamId([u8; 16])`** -- 16-byte random identifier. `Copy`, `Hash`, `Eq`, `Serialize`/`Deserialize`. Custom `Debug` (4-byte hex prefix) and `Display` (8-byte hex prefix) following the codebase's ID conventions. Not an `ActorAddress` -- streams are not actors and don't pollute the address space.
- **`StreamMode`** -- enum with `BlobTransfer` variant. Extensible for future modes (continuous streams, datagrams).
- **`StreamConfig`** -- negotiation parameters: `stripe_count` (default 4), `frame_size` (default 256KB), `metadata` (opaque bytes for application-level negotiation payloads like ContentHash).
- **`StreamError`** -- error enum covering `Closed`, `BrokenPipe`, `Disconnected`, `BufferExhausted`, `InvalidHeader`, and `ChunkVerificationFailed` (with expected/actual hashes for diagnostics).
- **`ResumeToken`** -- checkpoint for resuming interrupted transfers, carrying stream identity and progress counters.

#### `src/wire.rs`

Binary wire format for stream headers and data frames. Pure functions, no I/O.

- **Constants**: `MAGIC: [0x53, 0x57]` ("SW"), `VERSION: 0x01`, `ALPN: b"swactor/stream/1"`.
- **`StreamHeader`** -- the negotiation header sent at connection establishment. Wire layout: `[2B magic][1B version][16B stream_id][1B mode][1B stripe_count][4B frame_size][4B metadata_len][N metadata]`.
- **`encode_header` / `decode_header`** -- round-trippable serialization with validation (magic, version, mode, truncation checks).
- **Data frame format**: `[4B payload_len (big-endian)][payload]`. Deliberately minimal -- no per-frame type tags or checksums (QUIC provides TLS integrity). A zero-length payload signals end-of-stripe.
- **`encode_data_frame` / `decode_data_frame` / `encode_end_of_stripe`** -- frame-level codec.

#### `src/buffer.rs`

Pre-allocated buffer pool for zero-allocation data transfer.

- **`FrameBuf`** -- a `Box<[u8]>` with read/write cursors. `write(&[u8]) -> usize` fills from the write cursor, `read(&mut [u8]) -> usize` drains from the read cursor. `reset()` zeroes only the cursors (not the data) for fast recycling. `load(&[u8])` replaces content directly.
- **`BufferPool`** -- a fixed-size pool backed by `crossbeam::ArrayQueue<FrameBuf>` (lock-free MPMC). `checkout() -> Option<FrameBuf>` and `checkin(buf)` enable concurrent use between actor threads and tokio tasks without locks. `Clone` shares the underlying `Arc` so send/recv sides reference the same pool.

### Stage 2: StreamHandle, Channels, and Data Plane

Introduces tokio channels and async tasks but NOT iroh. Data-plane tasks are generic over `AsyncRead`/`AsyncWrite`, fully testable with `tokio::io::DuplexStream`.

#### `src/channel.rs`

Typed channel messages that move `FrameBuf`s by ownership (zero-copy handoff).

- **`SendCommand`** -- `Data(FrameBuf)`, `Flush`, `Close`. Actor -> send task.
- **`SendEvent`** -- `WriteReady`, `Error(StreamError)`, `Closed`. Send task -> actor.
- **`RecvCommand`** -- `Consumed(FrameBuf)`, `Close`. Actor -> recv task.
- **`RecvEvent`** -- `Data(FrameBuf)`, `Error(StreamError)`, `Closed`. Recv task -> actor.

#### `src/notify.rs`

Notification coalescing to prevent flooding actor mailboxes.

- **`NotifyFlag`** -- `AtomicU8` bitflags (`DATA_READY`, `WRITE_READY`, `CLOSED`, `ERROR`). `set(kind) -> bool` returns true only if the bit was previously clear, signaling a new notification should be injected. `clear(kind)` is called by the actor after handling.
- **`StreamEvent`** / **`StreamEventKind`** -- the lightweight sentinel message injected into actor mailboxes. Carries `stream_id` and `kind` (DataReady, WriteReady, Closed, Error).
- **`NotifySink`** -- held by data-plane tasks. Combines the shared `NotifyFlag` with an inject closure. Convenience methods: `data_ready()`, `write_ready()`, `closed()`, `error()`.

#### `src/handle.rs`

The actor-facing API for reading and writing stream data.

- **`SendHalf`** -- owns `mpsc::Sender<SendCommand>`, `mpsc::Receiver<SendEvent>`, a `BufferPool` clone, and an active `FrameBuf`. `try_write(&[u8]) -> Result<usize>` fills the active buffer and sends full buffers via `try_send` (non-blocking). Returns 0 on backpressure. `flush()` sends partial buffers. `close()` flushes remaining data and sends the Close command.
- **`RecvHalf`** -- owns `mpsc::Receiver<RecvEvent>`, `mpsc::Sender<RecvCommand>`, a `BufferPool` clone, and an active `FrameBuf`. `try_read(&mut [u8]) -> Result<usize>` drains the active buffer then pulls new buffers from the channel. Returns 0 when no data is available. `has_data()` peeks without consuming.
- **`StreamHandle`** -- combines `SendHalf` and `RecvHalf`. `Send` but not `Clone` (the mpsc receivers are not cloneable).
- **`create_stream_handle(stream_id, config, pool_size, channel_capacity)`** -- factory that returns `(StreamHandle, DataPlaneEndpoints)`. The handle goes to the actor; the endpoints go to the data-plane tasks.

#### `src/data_plane.rs`

Async tasks that bridge `StreamHandle` channels to actual byte streams.

- **`send_stripe_task<W: AsyncWrite>`** -- reads `SendCommand`s from the channel, wire-encodes them as data frames, writes to the transport, returns consumed buffers to the pool, and optionally notifies the actor via `NotifySink`.
- **`recv_stripe_task<R: AsyncRead>`** -- reads wire-encoded frames from the transport, loads payloads into `FrameBuf`s from the pool, sends `RecvEvent::Data` to the actor channel. Handles end-of-stripe sentinel and connection closure.
- **`spawn_send_stripes` / `spawn_recv_stripes`** -- spawn a set of stripe tasks from a writer/reader factory. The recv spawner merges all stripe outputs into a single `mpsc::Receiver<RecvEvent>`.

Generic over `AsyncRead + AsyncWrite + Send + Unpin + 'static`, so tests use `tokio::io::DuplexStream` with no network stack.

### Stage 3: QUIC Integration and StreamManager Actor

Connects the data-plane tasks to real QUIC streams via iroh. Introduces the `StreamManager` system actor with full open/accept/reject lifecycle. Modifies `IrohDriver` for generic ALPN routing and bootstraps the StreamManager in `swactor-node`.

#### `src/messages.rs`

Protocol types for the stream control plane.

- **`OneShot<T>`** -- Clone-friendly wrapper for non-Clone data (`StreamHandle`, `Connection`). Uses `Arc<Mutex<Option<T>>>` internally. First `.take()` extracts the value; subsequent calls (including from clones) return `None`. This allows non-Clone payloads inside Clone message enums required by the actor system's `Message` trait.
- **`StreamManagerMsg`** -- 8-variant enum for messages sent TO the StreamManager actor:
  - `Open { target_node, mode, config, reply_to }` -- Request a new stream to a remote node.
  - `Accept { stream_id, reply_to }` -- Accept an offered incoming stream.
  - `Reject { stream_id }` -- Reject an offered incoming stream.
  - `Listen { mode, listener }` -- Register as a stream listener for a given mode.
  - `Close { stream_id }` -- Close a stream.
  - `IncomingConnection { node_id, stream_id, mode, config, conn }` -- Internal: from accept bridge to StreamManager.
  - `OpenCompleted { stream_id, reply_to, result }` -- Internal: async open task completed.
  - `AcceptCompleted { stream_id, reply_to, result }` -- Internal: async accept task completed.
- **`StreamNotification`** -- 4-variant enum for notifications sent FROM StreamManager TO user actors:
  - `StreamReady { stream_id, handle }` -- Stream is ready for use (open or accept completed).
  - `StreamOffer { stream_id, mode, metadata, from_node }` -- A remote node is offering a stream.
  - `StreamClosed { stream_id, reason }` -- A stream was closed.
  - `StreamFailed { stream_id, error }` -- A stream open/accept failed.

#### `src/connection.rs`

Async connection cache for stream QUIC connections, separate from SWIM connections.

- **`StreamConnectionCache`** -- `HashMap<[u8; 32], Connection>` with health-check-on-access. `get_or_connect()` checks `conn.close_reason().is_none()` before reuse and falls back to connecting via `endpoint.connect(key, ALPN)`. `prune_closed()` for bulk cleanup. Uses the stream ALPN (`swactor/stream/1`).

#### `src/manager.rs`

The core StreamManager system actor.

- **`StreamManager`** -- implements `ActorInterface<Incoming = StreamManagerMsg>`. Manages active streams, pending incoming offers, listener registrations, and a connection cache. Holds an `Endpoint`, `tokio::runtime::Handle`, and `Arc<Runtime>` for spawning async tasks and sending messages back to itself.
- **`STREAM_MANAGER_NAME`** -- well-known name `"StreamManager"` for the name registry.
- **Open flow**: Generates `StreamId`, spawns a tokio task that connects, sends header on a control bi-stream, waits for a 1-byte accept/reject response, then creates `StreamHandle` + data-plane tasks, and sends `OpenCompleted` back to the StreamManager. StreamManager then delivers `StreamNotification::StreamReady` to the requesting actor.
- **Incoming flow**: Accept bridge reads header, sends `IncomingConnection` to StreamManager. StreamManager stores as pending, notifies matching listeners with `StreamOffer`.
- **Accept flow**: Takes pending connection, spawns tokio task that sends accept byte, creates `StreamHandle` + data-plane tasks, sends `AcceptCompleted` back. StreamManager delivers `StreamReady` to accepting actor.
- **Reject flow**: Sends reject byte on a uni-stream, drops the connection.
- **Close flow**: Removes stream state; data-plane tasks terminate when channels drop.
- **`handle_down`**: Cleans up streams owned by dead actors and removes dead listeners.
- **Data-plane spawning**: For each stream direction, a single tokio task opens N uni-streams and round-robins data frames across them. Recv tasks accept incoming uni-streams and dispatch each to a `recv_stripe_task`.

#### `src/accept.rs`

Bridge between incoming QUIC connections and the StreamManager actor.

- **`spawn_accept_bridge`** -- spawns a tokio task that reads from a channel of `(node_id, Connection)` pairs, accepting the control bi-stream, reading the stream header via `read_to_end` + `decode_header`, and forwarding `StreamManagerMsg::IncomingConnection` to the StreamManager via `runtime.send_to()`.
- **`handle_incoming`** -- public async function for per-connection header processing. Can also be called directly from the main loop (used by `swactor-node`).

#### Modified: `crates/distribution/src/iroh_driver.rs`

Generic ALPN support to route stream connections separately from SWIM.

- **`IrohDriverConfig`**: Added `additional_alpns: Vec<Vec<u8>>` field. All existing call sites updated with `additional_alpns: vec![]`.
- **Endpoint creation**: ALPNs now include both SWIM and any additional ALPNs (`vec![ALPN.to_vec()] + additional_alpns`).
- **Accept loop**: After accepting a connection, checks `conn.alpn()`. SWIM ALPN routes to `accepted_conns` (existing behavior). All other ALPNs route to `other_accepted_conns` (new buffer).
- **New field**: `other_accepted_conns: Arc<Mutex<Vec<(NodeId, Connection)>>>`.
- **New methods**: `endpoint() -> &Endpoint` (for outbound stream connections), `drain_other_connections() -> Vec<(NodeId, Connection)>` (polled from main loop).

#### Modified: `crates/streams/src/types.rs`

- Added `Hash` derive to `StreamMode` (needed as `HashMap` key in listeners registry).

#### Modified: `crates/streams/src/lib.rs`

- Added module declarations and re-exports for `accept`, `connection`, `manager`, `messages`.
- Re-exports: `StreamConnectionCache`, `StreamManager`, `STREAM_MANAGER_NAME`, `OneShot`, `StreamManagerMsg`, `StreamNotification`.

#### Modified: `crates/streams/Cargo.toml`

- Added `swactor-std` dependency (for `CtxMonitoring`, `RuntimeNaming`).
- Added `io-util` feature to `tokio` (for `AsyncWriteExt::flush`).

#### Modified: `crates/swactor-node/src/main.rs`

Bootstrap integration in `run_iroh()`.

- Passes `swactor_streams::ALPN.to_vec()` in `IrohDriverConfig::additional_alpns`.
- After driver creation, spawns `StreamManager::new(endpoint, tokio_handle, runtime)` as a named actor under `"StreamManager"`.
- In the main loop, drains `driver.drain_other_connections()` and spawns `handle_incoming` tasks for each, forwarding to the StreamManager.

#### Modified: `crates/swactor-node/Cargo.toml`

- Added `swactor-streams` dependency.

#### Modified: `crates/distribution/tests/common/iroh.rs`, `crates/dashboard/src/bin/swactor-node.rs`

- Updated all `IrohDriverConfig` construction sites with `additional_alpns: vec![]`.

### Stage 4: Datastore Stream Integration

Connects the stream system to the datastore so blob transfers flow over QUIC streams instead of sequential per-chunk actor-message round-trips. A 1GB blob with 1MB chunks that previously required 1,024 round-trips now flows in a single burst.

#### `crates/datastore/src/blob_transfer.rs` (NEW)

Async functions for sending/receiving blobs over StreamHandle. Runs inside tokio tasks, NOT actor handlers.

- **`BlobTransferError`** -- enum: `IncompleteTransfer(String)`, `ChunkVerificationFailed { expected, actual }`, `InvalidManifest(String)`, `Storage(String)`.
- **`ReceivedBlob`** -- `{ manifest: ObjectManifest, chunks: Vec<(ContentHash, Vec<u8>)> }`.
- **`send_blob(send, manifest, read_chunk)`** -- generic over an async callback `F: Fn(ContentHash) -> Future<Output = Result<Vec<u8>>>`. Writes `[4B manifest_json_len][manifest JSON]` preamble, then for each chunk in the manifest calls `read_chunk(hash)` and writes the raw bytes. Chunks are NOT preloaded -- the callback reads one at a time.
- **`recv_blob(recv)`** -- reads manifest preamble, deserializes JSON, then reads + blake3-verifies each chunk against the manifest's `ChunkRef` entries. Returns `ReceivedBlob`.
- **`poll_inbox(inbox, timeout)`** -- async version of `bridge.rs:poll_response`. Yields (`tokio::task::yield_now`) instead of `thread::sleep`, polling the swactor `Inbox` until a message arrives or timeout.
- **Internal helpers**: `write_all` (loops `try_write` + `yield_now`), `read_exact` (loops `try_read` + `yield_now`).

Wire format:
```
[4B manifest_json_length (u32 BE)]
[N bytes manifest JSON]
[chunk_0 raw bytes]  <- size from manifest.chunks[0].size
[chunk_1 raw bytes]
...
```

#### `crates/datastore/src/actors/stream_listener.rs` (NEW)

Listens for incoming BlobTransfer stream offers and routes them to DatastoreNode.

- **`StreamListener`** -- `Incoming = StreamNotification`. State: `datastore_node: ActorAddress`, `stream_manager: Option<ActorAddress>`.
- `on_start`: looks up `"StreamManager"` via `ctx.where_is()`, sends `StreamManagerMsg::Listen { mode: BlobTransfer }`.
- `handle(StreamOffer)`: extracts 32-byte ContentHash from `metadata`, sends `DatastoreNodeMsg::HandleStreamOffer` to DatastoreNode. Rejects if metadata != 32 bytes.

#### `crates/datastore/src/actors/stream_downloader.rs` (NEW)

Opens a stream to a remote node and downloads a blob.

- **`StreamDownloader`** -- `Incoming = StreamNotification`. Constructor takes: `content_hash`, `source_node`, `datastore_node`, `blob_store`, `reply_to`, `stream_manager`, `tokio_handle`, `runtime`.
- `on_start`: sends `StreamManagerMsg::Open { target_node, mode: BlobTransfer, config.metadata: content_hash.0.to_vec() }`.
- `handle(StreamReady)`: takes handle via `OneShot::take()`, spawns tokio task:
  - Calls `recv_blob(&mut recv_half)`.
  - Writes each chunk to BlobStore via `runtime.send_to(blob_store, WriteChunk)` (fire-and-forget).
  - Writes manifest via `runtime.send_to(blob_store, WriteManifest)` (fire-and-forget).
  - Sends `DatastoreNodeMsg::StreamDownloadComplete` to DatastoreNode.
  - On error: sends `DatastoreNodeMsg::StreamDownloadFailed`.
  - Actor calls `ctx.stop_self()` after spawning the task.
- `handle(StreamFailed)`: sends `StreamDownloadFailed`, stops self.

#### `crates/datastore/src/actors/stream_server.rs` (NEW)

Serves a blob to a requesting node over a stream, reading chunks on-demand.

- **`StreamServer`** -- `Incoming = StreamNotification`. Constructor takes: `stream_id`, `content_hash`, `blob_store`, `stream_manager`, `tokio_handle`, `runtime`.
- `on_start`: sends `StreamManagerMsg::Accept { stream_id }`.
- `handle(StreamReady)`: takes handle, spawns tokio task:
  - Reads manifest from BlobStore via `runtime.new_inbox()` + `poll_inbox` (async Inbox polling).
  - Calls `send_blob(&mut send_half, &manifest, |chunk_hash| { ... })` with a callback that reads each chunk on-demand from BlobStore via a fresh Inbox.
  - At most one chunk is in memory at a time. Chunks flow directly from BlobStore to stream.
  - Actor calls `ctx.stop_self()`.
- `handle(StreamFailed)`: stops self.

#### Modified: `crates/datastore/src/messages.rs`

Added 5 new variants to `DatastoreNodeMsg`:

- `DownloadViaStream { content_hash, source_node, reply_to }` -- triggers a stream download.
- `HandleStreamOffer { stream_id, content_hash, from_node, stream_manager }` -- routes incoming stream offers.
- `StreamDownloadComplete { content_hash, manifest, reply_to }` -- download succeeded; persist metadata.
- `StreamDownloadFailed { content_hash, reason, reply_to }` -- download failed; notify caller.
- `ConfigureStreams { stream_manager, tokio_handle, runtime }` -- late-binding stream support.

Changed from `#[derive(Debug, Clone)]` to `#[derive(Clone)]` with manual `Debug` impl (because `Arc<Runtime>` doesn't implement `Debug`).

#### Modified: `crates/datastore/src/actors/datastore_node.rs`

Added stream support fields and handlers to the coordinator actor.

- **New fields**: `runtime: Option<Arc<Runtime>>`, `tokio_handle: Option<tokio::runtime::Handle>`, `stream_manager: Option<ActorAddress>` -- all initialized to `None`.
- **`handle_configure_streams`**: stores runtime/tokio_handle/stream_manager.
- **`handle_download_via_stream`**: spawns `StreamDownloader`. Returns `TransferFailed` if streams not configured.
- **`handle_stream_offer`**: spawns `StreamServer`.
- **`handle_stream_download_complete`**: creates `ObjectEntry`, sends `MetadataMsg::PutObject` to metadata actor with the original `reply_to` for direct response routing.
- **`handle_stream_download_failed`**: sends `DatastoreResponse::TransferFailed` to `reply_to`.

#### Modified: `crates/datastore/src/actors/mod.rs`

Added module declarations for `stream_downloader`, `stream_listener`, `stream_server`.

#### Modified: `crates/datastore/src/lib.rs`

Added `pub mod blob_transfer`.

#### Modified: `crates/datastore/src/bridge.rs`

- Added `datastore_addr: ActorAddress` field to `DatastoreGroup` (stored during `spawn()`).
- Added `configure_streams(&self, stream_manager, tokio_handle)` method: sends `ConfigureStreams` to DatastoreNode, spawns and registers `StreamListener` under `"StreamListener"`.

#### Modified: `crates/datastore/Cargo.toml`

- Added `swactor-streams = { path = "../streams" }` and `tokio = { version = "1", features = ["sync", "rt", "time"] }` dependencies.
- Added dev-dependencies for testing: `swactor-streams`, `tokio` with `rt-multi-thread`, `macros`, `io-util`.

#### Modified: `crates/swactor-node/src/main.rs`

After StreamManager registration, wires stream support into the datastore:
```rust
if let Some(group) = ds_group {
    group.configure_streams(stream_mgr_addr, driver.tokio_handle());
}
```

## Test Coverage

42 tests across all modules:

| Category | Tests | What they verify |
|----------|-------|------------------|
| `types` | 4 | StreamId uniqueness, Debug/Display formatting, StreamConfig defaults |
| `wire` | 8 | Header round-trip (basic + property-based), bad magic/version/truncation rejection, data frame round-trip (basic + property-based), end-of-stripe sentinel |
| `buffer` | 7 | FrameBuf write/read/reset/load, BufferPool checkout/checkin/exhaustion/recycling/sharing |
| `notify` | 4 | Set returns true first time / false on duplicate, clear re-enables, independent flags, read shows all bits |
| `data_plane` | 9 | Single-stripe end-to-end transfer, multi-chunk ordered delivery (20 chunks), 4-stripe round-robin (100 chunks), graceful close, notification coalescing, backpressure detection |
| `messages` | 5 | OneShot take-once semantics, clone sharing, debug format, StreamManagerMsg is Message, StreamNotification is Message |
| `blob_transfer` | 5 | Small blob round-trip (single chunk), multi-chunk round-trip (4MB / 256KB chunks / 16 chunks), corrupted chunk detection (blake3 verification), truncated stream detection, property-based arbitrary blob round-trips |

Property-based tests (via `proptest`) cover:
- Arbitrary stream headers (random IDs, stripe counts 1-16, frame sizes 1KB-1MB, metadata 0-256 bytes)
- Arbitrary data frame payloads (0-256KB)
- Arbitrary blob transfers (random data 1-64KB, chunk sizes 256B-8KB)

## Dependency Footprint

### `swactor-streams` crate

- `swactor` (core actor types, with `serde` feature)
- `swactor-std` (for `CtxMonitoring`, `RuntimeNaming`)
- `shared-types` (ContentHash)
- `distribution` (NodeId, iroh re-exports)
- `crossbeam-queue` (lock-free buffer pool -- already a workspace dep)
- `tokio` (mpsc channels, async I/O traits, io-util)
- `iroh` (QUIC transport, connections, endpoints)
- `blake3`, `serde`, `getrandom`

Dev dependencies: `proptest`, `tokio` (with rt-multi-thread, macros, test-util, io-util).

### `swactor-datastore` crate (Stage 4 additions)

- `swactor-streams` (stream primitives, messages, types)
- `tokio` (sync, rt, time -- for spawning async blob transfer tasks and `poll_inbox`)

Dev dependencies: `swactor-streams`, `tokio` (with rt-multi-thread, macros, io-util).

## Next Steps

### Remaining MVP Work

These items complete the minimum viable stream-based blob transfer:

1. **Two-node integration test** -- full open/accept/data-transfer/close cycle with real iroh endpoints and two `DatastoreGroup` instances. Verifies StreamListener receives offers, StreamServer serves blobs, StreamDownloader receives and persists them. This is the critical end-to-end validation that all the pieces work together over real QUIC.

2. **CtxStreams extension trait** (`crates/streams/src/ctx_ext.rs`) -- convenience methods on `Ctx`: `stream_open()`, `stream_listen()`, `stream_accept()`, `stream_reject()`, `stream_close()`. Looks up `"StreamManager"` via `where_is()` and wraps the message construction. Reduces boilerplate for any actor wanting to use streams.

3. **Resume tokens** -- checkpoint emission every N chunks or N bytes during `send_blob`/`recv_blob`. Stored in `ResumeToken` (already defined in `types.rs`). On reconnect, receiver sends its token in `StreamConfig.metadata` and sender seeks to the right chunk offset.

### Post-MVP Phases

- **Dashboard stream metrics** -- expose active streams, bytes transferred, and transfer rates through the existing dashboard infrastructure.
- **Continuous Streams** -- `ContinuousStream` mode for unbounded data (ML gradient streams, data pipelines). Single bidirectional QUIC stream, variable-sized frames, ring buffer backpressure.
- **Unreliable Datagrams** -- QUIC datagram-based mode for latency-sensitive data (voice/video, game state). Sequence-based dropping, jitter buffer.
- **Priority and QoS** -- per-stream priority, write scheduling across concurrent streams, QUIC stream priority hints.
- **Parallel Unordered Transfer** -- independent per-chunk QUIC streams for workloads where any chunk is consumable independently (distributed ML gradient exchange).
