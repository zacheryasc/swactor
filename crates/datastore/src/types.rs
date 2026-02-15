//! Core data types for the distributed datastore protocol.
//!
//! Content-hash-first addressing: every object is identified by
//! `blake3(blob_bytes)`. Names are optional metadata, not keys.

use std::collections::BTreeMap;
use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use distribution::types::NodeId;

// ─── ContentHash ────────────────────────────────────────────────────────────

/// A blake3 content hash (32 bytes).
///
/// The primary identifier for blobs and the DHT key. Mirrors the `NodeId`
/// pattern from `distribution::types` — XOR distance for DHT routing, compact
/// Debug/Display for logging.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    /// Compute the blake3 hash of the given data.
    pub fn of(data: &[u8]) -> Self {
        let hash = blake3::hash(data);
        ContentHash(*hash.as_bytes())
    }

    /// XOR distance between two content hashes (Kademlia metric).
    pub fn xor_distance(&self, other: &ContentHash) -> [u8; 32] {
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = self.0[i] ^ other.0[i];
        }
        out
    }

    /// Number of leading zero bits in the XOR distance to `other`.
    /// Returns 0..=256. Used to select the k-bucket index in the metadata DHT.
    pub fn xor_leading_zeros(&self, other: &ContentHash) -> u32 {
        let dist = self.xor_distance(other);
        let mut zeros = 0u32;
        for byte in dist {
            if byte == 0 {
                zeros += 8;
            } else {
                zeros += byte.leading_zeros();
                break;
            }
        }
        zeros
    }

    /// Parse a 64-character hex string into a ContentHash.
    /// Returns `None` if the string is not exactly 64 hex characters.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_digit(chunk[0])?;
            let lo = hex_digit(chunk[1])?;
            bytes[i] = (hi << 4) | lo;
        }
        Some(ContentHash(bytes))
    }

    /// Encode as lowercase hex string.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            use fmt::Write;
            write!(s, "{:02x}", b).unwrap();
        }
        s
    }

    /// The zero hash (all zeroes). Used as a sentinel.
    pub const ZERO: ContentHash = ContentHash([0u8; 32]);
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash(")?;
        for b in &self.0[..4] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026})")
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

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
