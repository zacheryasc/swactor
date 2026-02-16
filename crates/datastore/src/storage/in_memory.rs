//! In-memory storage backend — useful for tests and browser/WASM targets.

use std::collections::HashMap;

use crate::types::{ContentHash, ObjectEntry, ObjectManifest};

use super::StorageBackend;

/// A purely in-memory storage backend.
///
/// All data lives in `HashMap`s. No persistence across restarts.
/// Useful for unit tests and browser/WASM environments.
pub struct InMemoryBackend {
    chunks: HashMap<ContentHash, Vec<u8>>,
    manifests: HashMap<ContentHash, ObjectManifest>,
    entries: HashMap<ContentHash, ObjectEntry>,
}

impl InMemoryBackend {
    pub fn new() -> Self {
        Self {
            chunks: HashMap::new(),
            manifests: HashMap::new(),
            entries: HashMap::new(),
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

    fn write_entry(&mut self, entry: &ObjectEntry) -> Result<(), std::io::Error> {
        self.entries.insert(entry.content_hash, entry.clone());
        Ok(())
    }

    fn read_entry(&self, hash: &ContentHash) -> Result<Option<ObjectEntry>, std::io::Error> {
        Ok(self.entries.get(hash).cloned())
    }

    fn delete_entry(&mut self, hash: &ContentHash) -> Result<(), std::io::Error> {
        self.entries.remove(hash);
        Ok(())
    }

    fn list_entries(&self) -> Result<Vec<ObjectEntry>, std::io::Error> {
        Ok(self.entries.values().cloned().collect())
    }
}
