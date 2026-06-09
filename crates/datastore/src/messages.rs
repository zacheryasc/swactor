//! Protocol messages for the distributed datastore.
//!
//! Split into two categories:
//! - **Inter-node messages** — travel over the wire (iroh/QUIC) between nodes.
//!   Each implements `NetworkMessage` with a stable `type_tag()`.
//! - **Intra-node messages** — actor-to-actor within a single node.
//!   Plain enums routed through the local actor system.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_transport::NetworkMessage;

use swactor_transport::NodeId;
use crate::streams::types::StreamId;

use crate::auth::{AccessRequestInfo, AuthorizedKeyInfo, DeniedReason, SignedRequest};
use crate::types::{ContentHash, ObjectEntry, ObjectManifest};

// ═══════════════════════════════════════════════════════════════════════════
// Inter-node messages (wire protocol over iroh)
// ═══════════════════════════════════════════════════════════════════════════

// ─── Chunk transfer ─────────────────────────────────────────────────────────

/// Request a chunk by its content hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetChunkRequest {
    pub from: NodeId,
    pub hash: ContentHash,
}

impl NetworkMessage for GetChunkRequest {
    fn type_tag() -> &'static str {
        "swactor_datastore::GetChunkRequest"
    }
}

/// Response to a chunk request. `data` is `None` if the chunk is not found.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetChunkResponse {
    pub hash: ContentHash,
    pub data: Option<Vec<u8>>,
}

impl NetworkMessage for GetChunkResponse {
    fn type_tag() -> &'static str {
        "swactor_datastore::GetChunkResponse"
    }
}

// ─── Object metadata (DHT operations) ───────────────────────────────────────

/// Store object metadata in the DHT (Kademlia STORE).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreObjectRequest {
    pub entry: ObjectEntry,
}

impl NetworkMessage for StoreObjectRequest {
    fn type_tag() -> &'static str {
        "swactor_datastore::StoreObjectRequest"
    }
}

/// Look up object metadata by content hash (Kademlia FIND_VALUE).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FindObjectRequest {
    pub from: NodeId,
    pub content_hash: ContentHash,
}

impl NetworkMessage for FindObjectRequest {
    fn type_tag() -> &'static str {
        "swactor_datastore::FindObjectRequest"
    }
}

/// Response to a FIND_VALUE for object metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FindObjectResponse {
    /// Found the object — here's the metadata entry.
    Found(ObjectEntry),
    /// Don't have it — here are closer nodes to ask.
    Closer(Vec<(NodeId, SocketAddr)>),
}

impl NetworkMessage for FindObjectResponse {
    fn type_tag() -> &'static str {
        "swactor_datastore::FindObjectResponse"
    }
}

/// Request an object manifest by its content hash.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetManifestRequest {
    pub from: NodeId,
    pub hash: ContentHash,
}

impl NetworkMessage for GetManifestRequest {
    fn type_tag() -> &'static str {
        "swactor_datastore::GetManifestRequest"
    }
}

/// Response to a manifest request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetManifestResponse {
    pub manifest: Option<ObjectManifest>,
}

impl NetworkMessage for GetManifestResponse {
    fn type_tag() -> &'static str {
        "swactor_datastore::GetManifestResponse"
    }
}

// ─── Listing ────────────────────────────────────────────────────────────────

/// List objects stored on a specific node, optionally filtered by name substring.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsRequest {
    pub from: NodeId,
    pub name_filter: Option<String>,
}

impl NetworkMessage for ListObjectsRequest {
    fn type_tag() -> &'static str {
        "swactor_datastore::ListObjectsRequest"
    }
}

/// Response to a list request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListObjectsResponse {
    pub entries: Vec<ObjectEntry>,
}

impl NetworkMessage for ListObjectsResponse {
    fn type_tag() -> &'static str {
        "swactor_datastore::ListObjectsResponse"
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Intra-node messages (actor-to-actor)
// ═══════════════════════════════════════════════════════════════════════════

// ─── BlobStoreMsg ───────────────────────────────────────────────────────────

/// Messages handled by the `BlobStoreActor`.
#[derive(Debug, Clone)]
pub enum BlobStoreMsg {
    /// Write a chunk to disk. The hash must match `ContentHash::of(data)`.
    WriteChunk {
        hash: ContentHash,
        data: Vec<u8>,
        reply_to: ActorAddress,
    },
    /// Read a chunk from disk by its content hash.
    ReadChunk {
        hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// Delete a chunk from disk.
    DeleteChunk { hash: ContentHash },
    /// Check whether a chunk exists locally.
    HasChunk {
        hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// List all chunk hashes stored locally.
    ListChunks { reply_to: ActorAddress },
    /// Garbage-collect chunks not in the referenced set.
    GcUnreferenced { referenced: HashSet<ContentHash> },
    /// Store a manifest to disk.
    WriteManifest {
        manifest: ObjectManifest,
        reply_to: ActorAddress,
    },
    /// Read a manifest from disk by its content hash.
    ReadManifest {
        hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// Write an entry to disk (fire-and-forget).
    WriteEntry { entry: ObjectEntry },
    /// Delete an entry from disk (fire-and-forget).
    DeleteEntry { hash: ContentHash },
    /// Load all persisted entries + their manifests at startup.
    LoadAll { reply_to: ActorAddress },
}

// ─── MetadataMsg ────────────────────────────────────────────────────────────

/// Messages handled by the `MetadataActor`.
#[derive(Debug, Clone)]
pub enum MetadataMsg {
    /// Store object metadata and manifest locally, then replicate to DHT.
    PutObject {
        entry: ObjectEntry,
        manifest: ObjectManifest,
        reply_to: ActorAddress,
    },
    /// Look up an object by content hash (local first, then DHT).
    GetObject {
        content_hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// Delete an object by content hash (remove from local index).
    DeleteObject {
        content_hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// List objects stored on this node, optionally filtered by name substring.
    ListLocal {
        name_filter: Option<String>,
        reply_to: ActorAddress,
    },
    /// Fan-out list to all known alive nodes, merge results.
    ListSwarm {
        name_filter: Option<String>,
        reply_to: ActorAddress,
    },
    /// Handle an incoming FIND_VALUE request from the DHT.
    HandleFindObject {
        from: NodeId,
        content_hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// Handle an incoming STORE request from the DHT.
    HandleStoreObject {
        entry: ObjectEntry,
        manifest: Option<ObjectManifest>,
    },
    /// Set the list of peer MetadataActor addresses for dissemination.
    SetPeers { peers: Vec<ActorAddress> },
    /// Trigger one round of epidemic dissemination to peers.
    DisseminateTick,
    /// Periodic garbage collection tick.
    GcTick,
    /// Bulk-load entries and manifests from storage at startup.
    BulkLoad {
        entries: Vec<(ObjectEntry, ObjectManifest)>,
    },
}

// ─── TransferMsg ────────────────────────────────────────────────────────────

/// Messages handled by the `TransferActor` (ephemeral, one per download).
#[derive(Debug, Clone)]
pub enum TransferMsg {
    /// Start downloading an object from a remote node.
    StartDownload {
        manifest: ObjectManifest,
        source_node: NodeId,
        reply_to: ActorAddress,
    },
    /// A chunk has been received from the remote node.
    ChunkReceived {
        hash: ContentHash,
        data: Vec<u8>,
    },
    /// A chunk fetch failed.
    ChunkFailed {
        hash: ContentHash,
        reason: String,
    },
    /// Cancel this transfer.
    Cancel,
}

// ─── DatastoreNodeMsg ───────────────────────────────────────────────────────

/// Messages handled by the `DatastoreNode` coordinator actor.
#[derive(Clone)]
pub enum DatastoreNodeMsg {
    // ── User-facing commands ────────────────────────────────────────────
    /// Store a blob with optional name and tags.
    Put {
        data: Vec<u8>,
        name: Option<String>,
        tags: BTreeMap<String, String>,
        reply_to: ActorAddress,
    },
    /// Retrieve object metadata and manifest by content hash.
    Get {
        content_hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// Delete an object by content hash.
    Delete {
        content_hash: ContentHash,
        reply_to: ActorAddress,
    },
    /// List stored objects with optional name filter.
    List {
        name_filter: Option<String>,
        all: bool,
        reply_to: ActorAddress,
    },
    /// Query node identity.
    Status { reply_to: ActorAddress },
    /// Read a single chunk by hash.
    ReadChunk {
        hash: ContentHash,
        reply_to: ActorAddress,
    },

    // ── Incoming network protocol ───────────────────────────────────────
    /// Route an incoming GetChunk request to BlobStoreActor.
    IncomingGetChunk {
        request: GetChunkRequest,
        reply_to: ActorAddress,
    },
    /// Route an incoming GetManifest request to BlobStoreActor.
    IncomingGetManifest {
        request: GetManifestRequest,
        reply_to: ActorAddress,
    },
    /// Route an incoming StoreObject request to MetadataActor.
    IncomingStoreObject { request: StoreObjectRequest },
    /// Route an incoming FindObject request to MetadataActor.
    IncomingFindObject {
        request: FindObjectRequest,
        reply_to: ActorAddress,
    },
    /// Route an incoming ListObjects request to MetadataActor.
    IncomingListObjects {
        request: ListObjectsRequest,
        reply_to: ActorAddress,
    },

    // ── Stream-based blob transfer ─────────────────────────────────────
    /// Download a blob via QUIC stream from a remote node.
    DownloadViaStream {
        content_hash: ContentHash,
        source_node: [u8; 32],
        reply_to: ActorAddress,
    },
    /// Handle an incoming stream offer (from StreamListener).
    HandleStreamOffer {
        stream_id: StreamId,
        content_hash: ContentHash,
        from_node: [u8; 32],
        stream_manager: ActorAddress,
        resume_from_chunk: u64,
    },
    /// A stream download completed successfully.
    StreamDownloadComplete {
        content_hash: ContentHash,
        manifest: ObjectManifest,
        reply_to: ActorAddress,
    },
    /// A stream download failed.
    StreamDownloadFailed {
        content_hash: ContentHash,
        reason: String,
        chunks_completed: u64,
        reply_to: ActorAddress,
    },
    /// Configure stream support (StreamManager address + tokio handle).
    ConfigureStreams {
        stream_manager: ActorAddress,
        tokio_handle: tokio::runtime::Handle,
        runtime: Arc<Runtime>,
    },
}

impl std::fmt::Debug for DatastoreNodeMsg {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Put { name, .. } => f.debug_struct("Put").field("name", name).finish_non_exhaustive(),
            Self::Get { content_hash, .. } => f.debug_struct("Get").field("content_hash", content_hash).finish_non_exhaustive(),
            Self::Delete { content_hash, .. } => f.debug_struct("Delete").field("content_hash", content_hash).finish_non_exhaustive(),
            Self::List { name_filter, all, .. } => f.debug_struct("List").field("name_filter", name_filter).field("all", all).finish_non_exhaustive(),
            Self::Status { .. } => write!(f, "Status"),
            Self::ReadChunk { hash, .. } => f.debug_struct("ReadChunk").field("hash", hash).finish_non_exhaustive(),
            Self::IncomingGetChunk { .. } => write!(f, "IncomingGetChunk"),
            Self::IncomingGetManifest { .. } => write!(f, "IncomingGetManifest"),
            Self::IncomingStoreObject { .. } => write!(f, "IncomingStoreObject"),
            Self::IncomingFindObject { .. } => write!(f, "IncomingFindObject"),
            Self::IncomingListObjects { .. } => write!(f, "IncomingListObjects"),
            Self::DownloadViaStream { content_hash, .. } => f.debug_struct("DownloadViaStream").field("content_hash", content_hash).finish_non_exhaustive(),
            Self::HandleStreamOffer { stream_id, content_hash, resume_from_chunk, .. } => f.debug_struct("HandleStreamOffer").field("stream_id", stream_id).field("content_hash", content_hash).field("resume_from_chunk", resume_from_chunk).finish_non_exhaustive(),
            Self::StreamDownloadComplete { content_hash, .. } => f.debug_struct("StreamDownloadComplete").field("content_hash", content_hash).finish_non_exhaustive(),
            Self::StreamDownloadFailed { content_hash, reason, chunks_completed, .. } => f.debug_struct("StreamDownloadFailed").field("content_hash", content_hash).field("reason", reason).field("chunks_completed", chunks_completed).finish_non_exhaustive(),
            Self::ConfigureStreams { stream_manager, .. } => f.debug_struct("ConfigureStreams").field("stream_manager", stream_manager).finish_non_exhaustive(),
        }
    }
}

// ─── DatastoreResponse ──────────────────────────────────────────────────────

/// Response messages sent back to the requester by datastore actors.
#[derive(Debug, Clone)]
pub enum DatastoreResponse {
    /// Object was stored successfully.
    PutOk {
        content_hash: ContentHash,
    },
    /// Object was found — here's the metadata and manifest.
    GetOk {
        entry: ObjectEntry,
        manifest: ObjectManifest,
    },
    /// Object was deleted.
    DeleteOk {
        content_hash: ContentHash,
    },
    /// List of matching objects.
    ListOk { entries: Vec<ObjectEntry> },
    /// Chunk data retrieved.
    ChunkOk {
        hash: ContentHash,
        data: Vec<u8>,
    },
    /// Chunk was stored successfully.
    ChunkStored { hash: ContentHash },
    /// Manifest stored successfully.
    ManifestStored { hash: ContentHash },
    /// Manifest retrieved.
    ManifestOk { manifest: ObjectManifest },
    /// Transfer completed — all chunks downloaded.
    TransferComplete { content_hash: ContentHash },
    /// Transfer failed.
    TransferFailed { reason: String },
    /// Node identity status response.
    NodeStatus { node_id: NodeId },
    /// Requested resource was not found.
    NotFound,
    /// An error occurred.
    Error { reason: String },
    /// Request was denied by the auth layer.
    Denied { reason: DeniedReason },
    /// Boolean response (e.g. HasChunk).
    Bool(bool),
    /// List of chunk hashes.
    ChunkList { hashes: Vec<ContentHash> },
    /// All persisted entries loaded at startup.
    LoadedAll {
        entries: Vec<(ObjectEntry, ObjectManifest)>,
    },
    /// List of pending access requests.
    AccessRequests {
        requests: Vec<AccessRequestInfo>,
    },
    /// List of authorized keys with labels.
    AuthorizedKeys {
        keys: Vec<AuthorizedKeyInfo>,
    },
}

// ─── GatewayMsg ────────────────────────────────────────────────────────────

/// Messages handled by the `GatewayActor` — the auth enforcement point.
#[derive(Debug, Clone)]
pub enum GatewayMsg {
    /// Auth Path 2: verify a signed request and dispatch if allowed.
    HandleSignedRequest {
        request: SignedRequest,
        reply_to: ActorAddress,
    },
    /// Auth Path 1: check whether a node is authorized for connection.
    CheckConnection {
        node_id: NodeId,
        reply_to: ActorAddress,
    },
    /// Owner-only: grant access to a key.
    Grant {
        requester: NodeId,
        key: NodeId,
        label: Option<String>,
        reply_to: ActorAddress,
    },
    /// Owner-only: revoke access from a key.
    Revoke {
        requester: NodeId,
        key: NodeId,
        reply_to: ActorAddress,
    },
    /// Auth-only check: verify a signed request without forwarding the action.
    Authorize {
        request: SignedRequest,
        reply_to: ActorAddress,
    },
    /// Verify signature only (no ACL check) — for access request submissions.
    VerifySignature {
        request: SignedRequest,
        reply_to: ActorAddress,
    },
    /// Submit an access request from a browser user.
    SubmitAccessRequest {
        key: NodeId,
        name: String,
        message: String,
        reply_to: ActorAddress,
    },
    /// List pending access requests (owner-only).
    ListAccessRequests {
        requester: NodeId,
        reply_to: ActorAddress,
    },
    /// Deny (dismiss) a pending access request (owner-only).
    DenyAccessRequest {
        requester: NodeId,
        key: NodeId,
        reply_to: ActorAddress,
    },
    /// List all authorized keys with labels (owner-only).
    ListAuthorizedKeys {
        requester: NodeId,
        reply_to: ActorAddress,
    },
    /// Periodic nonce garbage collection tick.
    NonceGcTick,
}

