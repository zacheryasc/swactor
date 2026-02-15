//! Chunking engine — pure functions for splitting blobs into content-addressed
//! chunks and reassembling them.
//!
//! No I/O. All functions are deterministic and side-effect-free.

use crate::types::{ChunkRef, ContentHash, ObjectManifest};

/// Errors that can occur during chunk reassembly.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChunkingError {
    /// A chunk referenced by the manifest was not provided.
    MissingChunk { hash: ContentHash },
    /// The reassembled data size does not match the manifest's `total_size`.
    SizeMismatch { expected: u64, actual: u64 },
    /// The reassembled data's content hash does not match the manifest's `content_hash`.
    HashMismatch {
        expected: ContentHash,
        actual: ContentHash,
    },
}

impl std::fmt::Display for ChunkingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkingError::MissingChunk { hash } => write!(f, "missing chunk: {hash}"),
            ChunkingError::SizeMismatch { expected, actual } => {
                write!(f, "size mismatch: expected {expected}, got {actual}")
            }
            ChunkingError::HashMismatch { expected, actual } => {
                write!(f, "hash mismatch: expected {expected}, got {actual}")
            }
        }
    }
}

impl std::error::Error for ChunkingError {}

/// Split a blob into fixed-size chunks and produce a manifest.
///
/// Returns `(content_hash, manifest, chunks)` where:
/// - `content_hash` is `blake3(data)` — the whole-blob hash
/// - `manifest` describes the chunk layout
/// - `chunks` is a vec of `(chunk_hash, chunk_bytes)` pairs
///
/// `chunk_size` must be > 0.
pub fn chunk_blob(
    data: &[u8],
    chunk_size: u32,
) -> (ContentHash, ObjectManifest, Vec<(ContentHash, Vec<u8>)>) {
    assert!(chunk_size > 0, "chunk_size must be > 0");

    let content_hash = ContentHash::of(data);
    let mut chunks = Vec::new();
    let mut chunk_refs = Vec::new();
    let mut offset: u64 = 0;

    if data.is_empty() {
        let manifest = ObjectManifest {
            content_hash,
            chunks: chunk_refs,
            total_size: 0,
            chunk_size,
            content_type: None,
        };
        return (content_hash, manifest, chunks);
    }

    for chunk_data in data.chunks(chunk_size as usize) {
        let hash = ContentHash::of(chunk_data);
        chunk_refs.push(ChunkRef {
            hash,
            offset,
            size: chunk_data.len() as u32,
        });
        chunks.push((hash, chunk_data.to_vec()));
        offset += chunk_data.len() as u64;
    }

    let manifest = ObjectManifest {
        content_hash,
        chunks: chunk_refs,
        total_size: data.len() as u64,
        chunk_size,
        content_type: None,
    };

    (content_hash, manifest, chunks)
}

/// Reassemble a blob from its manifest and chunk data.
///
/// Chunks are looked up by hash from the provided slice. The manifest's
/// `chunks` field determines the ordering. Verifies total size and
/// content hash after reassembly.
pub fn reassemble_blob(
    manifest: &ObjectManifest,
    chunks: &[(ContentHash, Vec<u8>)],
) -> Result<Vec<u8>, ChunkingError> {
    let mut result = Vec::with_capacity(manifest.total_size as usize);

    for chunk_ref in &manifest.chunks {
        let chunk_data = chunks
            .iter()
            .find(|(h, _)| *h == chunk_ref.hash)
            .map(|(_, d)| d);

        match chunk_data {
            Some(data) => result.extend_from_slice(data),
            None => {
                return Err(ChunkingError::MissingChunk {
                    hash: chunk_ref.hash,
                })
            }
        }
    }

    if result.len() as u64 != manifest.total_size {
        return Err(ChunkingError::SizeMismatch {
            expected: manifest.total_size,
            actual: result.len() as u64,
        });
    }

    let actual_hash = ContentHash::of(&result);
    if actual_hash != manifest.content_hash {
        return Err(ChunkingError::HashMismatch {
            expected: manifest.content_hash,
            actual: actual_hash,
        });
    }

    Ok(result)
}

/// Verify that `data` hashes to `expected`.
pub fn verify_integrity(data: &[u8], expected: &ContentHash) -> bool {
    ContentHash::of(data) == *expected
}
