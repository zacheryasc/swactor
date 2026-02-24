//! Core data types for the distributed datastore protocol.
//!
//! Content-hash-first addressing: every object is identified by
//! `blake3(blob_bytes)`. Names are optional metadata, not keys.

use std::collections::BTreeMap;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use swactor::transport::NodeId;

pub use crate::content_hash::ContentHash;

// ─── ObjectEntry ────────────────────────────────────────────────────────────

/// Metadata record for a stored object — content-addressed by `blake3(blob_bytes)`.
///
/// Names are optional flat strings, not hierarchical paths.
/// No LWW conflict resolution — content hashes are unique identifiers.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectEntry {
    /// Primary identifier: `blake3(entire_blob)`.
    pub content_hash: ContentHash,
    /// Optional human-readable name (flat string, not a path).
    pub name: Option<String>,
    /// Node that stores (or last wrote) this object.
    pub node_id: NodeId,
    /// User-defined key-value tags for filtering and search.
    pub tags: BTreeMap<String, String>,
    /// Total object size in bytes.
    pub size_bytes: u64,
    /// Wall-clock creation time (informational).
    pub created_at: u64,
}

// ─── ObjectManifest ─────────────────────────────────────────────────────────

/// Describes the chunked layout of a stored object.
///
/// Keyed by `content_hash = blake3(entire_blob)`, computed via a streaming
/// hasher alongside chunking. The manifest itself is stored under this key.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectManifest {
    /// Content hash of the entire blob: `blake3(all_bytes)`.
    /// This is the primary key for looking up the manifest.
    pub content_hash: ContentHash,
    /// Ordered list of chunk references.
    pub chunks: Vec<ChunkRef>,
    /// Total size of the original object in bytes.
    pub total_size: u64,
    /// Fixed chunk size used during chunking (e.g. 1MB).
    pub chunk_size: u32,
    /// MIME type of the object, if known.
    pub content_type: Option<String>,
}

/// A reference to a single chunk within an object manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChunkRef {
    /// Content hash of this chunk's data.
    pub hash: ContentHash,
    /// Byte offset of this chunk within the original object.
    pub offset: u64,
    /// Actual size of this chunk in bytes (last chunk may be smaller).
    pub size: u32,
}

// ─── DatastoreConfig ────────────────────────────────────────────────────────

/// Configuration for a datastore node.
#[derive(Debug, Clone)]
pub struct DatastoreConfig {
    /// Fixed chunk size in bytes. Default: 1,048,576 (1 MB).
    pub chunk_size: u32,
    /// Root directory for on-disk storage (chunks and manifests).
    pub storage_path: PathBuf,
    /// Ticks between GC sweeps.
    pub gc_interval: u64,
    /// Maximum number of simultaneous transfers.
    pub max_concurrent_transfers: usize,
}

impl Default for DatastoreConfig {
    fn default() -> Self {
        Self {
            chunk_size: 1_048_576,
            storage_path: PathBuf::from("datastore"),
            gc_interval: 1000,
            max_concurrent_transfers: 4,
        }
    }
}

// ─── Transfer state ─────────────────────────────────────────────────────────

/// Status of an in-progress transfer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransferStatus {
    /// Download is in progress.
    Downloading {
        /// Number of chunks received so far.
        chunks_received: usize,
        /// Total number of chunks in the manifest.
        chunks_total: usize,
    },
    /// Transfer completed successfully.
    Complete,
    /// Transfer failed.
    Failed { reason: String },
    /// Transfer was cancelled.
    Cancelled,
}
