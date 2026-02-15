//! In-memory storage backend — useful for tests and browser/WASM targets.

use std::collections::HashMap;

use crate::types::{ContentHash, ObjectManifest};

use super::StorageBackend;

/// A purely in-memory storage backend.
///
/// All data lives in `HashMap`s. No persistence across restarts.
/// Useful for unit tests and browser/WASM environments.
pub struct InMemoryBackend {
    chunks: HashMap<ContentHash, Vec<u8>>,
    manifests: HashMap<ContentHash, ObjectManifest>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            chunks: HashMap::new(),
            manifests: HashMap::new(),
        }
    }
}

impl Default for InMemoryBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl StorageBackend for InMemoryBackend {
    fn write_chunk(&mut self, hash: &ContentHash, data: &[u8]) -> Result<(), std::io::Error> {
        self.chunks.insert(*hash, data.to_vec());
        Ok(())
    }

    fn read_chunk(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>, std::io::Error> {
        Ok(self.chunks.get(hash).cloned())
    }

    fn delete_chunk(&mut self, hash: &ContentHash) -> Result<(), std::io::Error> {
        self.chunks.remove(hash);
        Ok(())
    }

    fn has_chunk(&self, hash: &ContentHash) -> bool {
        self.chunks.contains_key(hash)
    }

    fn list_chunks(&self) -> Vec<ContentHash> {
        self.chunks.keys().copied().collect()
    }

    fn write_manifest(&mut self, manifest: &ObjectManifest) -> Result<(), std::io::Error> {
        self.manifests.insert(manifest.content_hash, manifest.clone());
        Ok(())
    }

    fn read_manifest(
        &self,
        content_hash: &ContentHash,
    ) -> Result<Option<ObjectManifest>, std::io::Error> {
        Ok(self.manifests.get(content_hash).cloned())
    }

    fn delete_manifest(&mut self, content_hash: &ContentHash) -> Result<(), std::io::Error> {
        self.manifests.remove(content_hash);
        Ok(())
    }
}
