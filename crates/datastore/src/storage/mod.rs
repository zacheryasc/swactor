//! StorageBackend trait and implementations.
//!
//! Abstracts chunk and manifest I/O so backends can be swapped
//! (filesystem for MVP, IndexedDB for browser, in-memory for tests/WASM).

pub mod in_memory;

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;

use crate::types::{ContentHash, ObjectEntry, ObjectManifest};

pub use in_memory::InMemoryBackend;

/// Pluggable storage backend for chunks and manifests.
pub trait StorageBackend: Send {
    fn write_chunk(&mut self, hash: &ContentHash, data: &[u8]) -> Result<(), std::io::Error>;
    fn read_chunk(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>, std::io::Error>;
    fn delete_chunk(&mut self, hash: &ContentHash) -> Result<(), std::io::Error>;
    fn has_chunk(&self, hash: &ContentHash) -> bool;
    fn list_chunks(&self) -> Vec<ContentHash>;
    fn write_manifest(&mut self, manifest: &ObjectManifest) -> Result<(), std::io::Error>;
    fn read_manifest(&self, content_hash: &ContentHash) -> Result<Option<ObjectManifest>, std::io::Error>;
    fn delete_manifest(&mut self, content_hash: &ContentHash) -> Result<(), std::io::Error>;
    fn write_entry(&mut self, entry: &ObjectEntry) -> Result<(), std::io::Error>;
    fn read_entry(&self, hash: &ContentHash) -> Result<Option<ObjectEntry>, std::io::Error>;
    fn delete_entry(&mut self, hash: &ContentHash) -> Result<(), std::io::Error>;
    fn list_entries(&self) -> Result<Vec<ObjectEntry>, std::io::Error>;
}

/// Filesystem-backed storage with 2-level directory sharding.
///
/// Layout:
/// ```text
/// {root}/
/// ├── chunks/{hex[0..2]}/{hex[2..4]}/{full_hex_hash}
/// ├── manifests/{hex[0..2]}/{hex[2..4]}/{full_hex_hash}
/// └── entries/{hex[0..2]}/{hex[2..4]}/{full_hex_hash}
/// ```
pub struct FilesystemBackend {
    root: PathBuf,
    chunk_index: HashSet<ContentHash>,
}

impl FilesystemBackend {
    pub fn new(root: PathBuf) -> Self {
        let mut backend = Self {
            root,
            chunk_index: HashSet::new(),
        };
        backend.scan_chunks();
        backend
    }

    fn chunk_path(&self, hash: &ContentHash) -> PathBuf {
        let hex = hash.to_hex();
        self.root
            .join("chunks")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
    }

    fn manifest_path(&self, hash: &ContentHash) -> PathBuf {
        let hex = hash.to_hex();
        self.root
            .join("manifests")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
    }

    fn entry_path(&self, hash: &ContentHash) -> PathBuf {
        let hex = hash.to_hex();
        self.root
            .join("entries")
            .join(&hex[..2])
            .join(&hex[2..4])
            .join(&hex)
    }

    fn scan_chunks(&mut self) {
        let chunks_dir = self.root.join("chunks");
        if !chunks_dir.exists() {
            return;
        }
        let Ok(level1) = fs::read_dir(&chunks_dir) else {
            return;
        };
        for d1 in level1.flatten() {
            let Ok(level2) = fs::read_dir(d1.path()) else {
                continue;
            };
            for d2 in level2.flatten() {
                let Ok(files) = fs::read_dir(d2.path()) else {
                    continue;
                };
                for file in files.flatten() {
                    if let Some(name) = file.file_name().to_str() {
                        if let Some(hash) = ContentHash::from_hex(name) {
                            self.chunk_index.insert(hash);
                        }
                    }
                }
            }
        }
    }

    fn write_and_sync(path: &PathBuf, data: &[u8]) -> std::io::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = fs::File::create(path)?;
        file.write_all(data)?;
        file.sync_all()?;
        Ok(())
    }
}

impl StorageBackend for FilesystemBackend {
    fn write_chunk(&mut self, hash: &ContentHash, data: &[u8]) -> Result<(), std::io::Error> {
        let path = self.chunk_path(hash);
        Self::write_and_sync(&path, data)?;
        self.chunk_index.insert(*hash);
        Ok(())
    }

    fn read_chunk(&self, hash: &ContentHash) -> Result<Option<Vec<u8>>, std::io::Error> {
        if !self.chunk_index.contains(hash) {
            return Ok(None);
        }
        let path = self.chunk_path(hash);
        match fs::read(&path) {
            Ok(data) => Ok(Some(data)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete_chunk(&mut self, hash: &ContentHash) -> Result<(), std::io::Error> {
        self.chunk_index.remove(hash);
        let path = self.chunk_path(hash);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn has_chunk(&self, hash: &ContentHash) -> bool {
        self.chunk_index.contains(hash)
    }

    fn list_chunks(&self) -> Vec<ContentHash> {
        self.chunk_index.iter().copied().collect()
    }

    fn write_manifest(&mut self, manifest: &ObjectManifest) -> Result<(), std::io::Error> {
        let path = self.manifest_path(&manifest.content_hash);
        let data = serde_json::to_vec(manifest)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Self::write_and_sync(&path, &data)
    }

    fn read_manifest(&self, content_hash: &ContentHash) -> Result<Option<ObjectManifest>, std::io::Error> {
        let path = self.manifest_path(content_hash);
        match fs::read(&path) {
            Ok(data) => {
                let manifest: ObjectManifest = serde_json::from_slice(&data)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(Some(manifest))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete_manifest(&mut self, content_hash: &ContentHash) -> Result<(), std::io::Error> {
        let path = self.manifest_path(content_hash);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn write_entry(&mut self, entry: &ObjectEntry) -> Result<(), std::io::Error> {
        let path = self.entry_path(&entry.content_hash);
        let data = serde_json::to_vec(entry)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        Self::write_and_sync(&path, &data)
    }

    fn read_entry(&self, hash: &ContentHash) -> Result<Option<ObjectEntry>, std::io::Error> {
        let path = self.entry_path(hash);
        match fs::read(&path) {
            Ok(data) => {
                let entry: ObjectEntry = serde_json::from_slice(&data)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(Some(entry))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn delete_entry(&mut self, hash: &ContentHash) -> Result<(), std::io::Error> {
        let path = self.entry_path(hash);
        match fs::remove_file(&path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn list_entries(&self) -> Result<Vec<ObjectEntry>, std::io::Error> {
        let entries_dir = self.root.join("entries");
        if !entries_dir.exists() {
            return Ok(Vec::new());
        }
        let mut entries = Vec::new();
        let level1 = fs::read_dir(&entries_dir)?;
        for d1 in level1.flatten() {
            let Ok(level2) = fs::read_dir(d1.path()) else {
                continue;
            };
            for d2 in level2.flatten() {
                let Ok(files) = fs::read_dir(d2.path()) else {
                    continue;
                };
                for file in files.flatten() {
                    let data = fs::read(file.path())?;
                    match serde_json::from_slice::<ObjectEntry>(&data) {
                        Ok(entry) => entries.push(entry),
                        Err(e) => {
                            eprintln!("warning: skipping corrupt entry file {}: {e}", file.path().display());
                        }
                    }
                }
            }
        }
        Ok(entries)
    }
}

