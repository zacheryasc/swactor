//! Tests for StorageBackend implementations — parameterized across backends.

use std::collections::HashSet;

use swactor_datastore::chunking::chunk_blob;
use swactor_datastore::storage::{FilesystemBackend, InMemoryBackend, StorageBackend};
use swactor_datastore::types::{ChunkRef, ContentHash, ObjectManifest};

// ═══════════════════════════════════════════════════════════════════════════
// Backend factory helpers
// ═══════════════════════════════════════════════════════════════════════════

fn run_with_both_backends(test: impl Fn(&mut dyn StorageBackend)) {
    // In-memory
    let mut mem = InMemoryBackend::new();
    test(&mut mem);

    // Filesystem
    let dir = tempfile::tempdir().unwrap();
    let mut fs = FilesystemBackend::new(dir.path().to_path_buf());
    test(&mut fs);
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: chunk storage contract
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn store_and_retrieve_a_single_chunk() {
    run_with_both_backends(|backend| {
        let data = b"hello chunk";
        let hash = ContentHash::of(data);

        backend.write_chunk(&hash, data).unwrap();
        let read_back = backend.read_chunk(&hash).unwrap();

        assert_eq!(read_back, Some(data.to_vec()));
    });
}

#[test]
fn chunk_not_found_returns_none() {
    run_with_both_backends(|backend| {
        let hash = ContentHash::of(b"nonexistent");
        assert_eq!(backend.read_chunk(&hash).unwrap(), None);
    });
}

#[test]
fn delete_chunk_makes_it_unretrievable() {
    run_with_both_backends(|backend| {
        let data = b"ephemeral";
        let hash = ContentHash::of(data);

        backend.write_chunk(&hash, data).unwrap();
        backend.delete_chunk(&hash).unwrap();

        assert_eq!(backend.read_chunk(&hash).unwrap(), None);
    });
}

#[test]
fn has_chunk_reflects_storage_state() {
    run_with_both_backends(|backend| {
        let data = b"existence check";
        let hash = ContentHash::of(data);

        assert!(!backend.has_chunk(&hash));
        backend.write_chunk(&hash, data).unwrap();
        assert!(backend.has_chunk(&hash));
        backend.delete_chunk(&hash).unwrap();
        assert!(!backend.has_chunk(&hash));
    });
}

#[test]
fn list_chunks_returns_all_stored_hashes() {
    run_with_both_backends(|backend| {
        let mut expected = HashSet::new();
        for i in 0u8..5 {
            let data = vec![i; 32];
            let hash = ContentHash::of(&data);
            backend.write_chunk(&hash, &data).unwrap();
            expected.insert(hash);
        }

        let listed: HashSet<ContentHash> = backend.list_chunks().into_iter().collect();
        assert_eq!(listed, expected);
    });
}

#[test]
fn store_and_retrieve_manifest() {
    run_with_both_backends(|backend| {
        let manifest = ObjectManifest {
            content_hash: ContentHash::of(b"my-blob"),
            chunks: vec![ChunkRef {
                hash: ContentHash::of(b"chunk-0"),
                offset: 0,
                size: 1024,
            }],
            total_size: 1024,
            chunk_size: 1024,
            content_type: Some("text/plain".to_string()),
        };

        backend.write_manifest(&manifest).unwrap();
        let read_back = backend.read_manifest(&manifest.content_hash).unwrap();

        assert_eq!(read_back, Some(manifest));
    });
}

#[test]
fn delete_manifest_removes_it() {
    run_with_both_backends(|backend| {
        let manifest = ObjectManifest {
            content_hash: ContentHash::of(b"deletable"),
            chunks: vec![],
            total_size: 0,
            chunk_size: 1024,
            content_type: None,
        };

        backend.write_manifest(&manifest).unwrap();
        backend.delete_manifest(&manifest.content_hash).unwrap();
        assert_eq!(backend.read_manifest(&manifest.content_hash).unwrap(), None);
    });
}

#[test]
fn overwriting_chunk_is_idempotent() {
    run_with_both_backends(|backend| {
        let data = b"idempotent write";
        let hash = ContentHash::of(data);

        backend.write_chunk(&hash, data).unwrap();
        backend.write_chunk(&hash, data).unwrap();

        assert_eq!(backend.read_chunk(&hash).unwrap(), Some(data.to_vec()));
        assert_eq!(backend.list_chunks().len(), 1);
    });
}

#[test]
fn chunked_blob_round_trips_through_storage() {
    run_with_both_backends(|backend| {
        let original: Vec<u8> = (0..500).map(|i| (i % 256) as u8).collect();
        let (_, manifest, chunks) = chunk_blob(&original, 128);

        // Store all chunks and manifest.
        for (hash, data) in &chunks {
            backend.write_chunk(hash, data).unwrap();
        }
        backend.write_manifest(&manifest).unwrap();

        // Read back and reassemble.
        let read_manifest = backend.read_manifest(&manifest.content_hash).unwrap().unwrap();
        let mut reassembled = Vec::new();
        for chunk_ref in &read_manifest.chunks {
            let data = backend.read_chunk(&chunk_ref.hash).unwrap().unwrap();
            reassembled.extend_from_slice(&data);
        }

        assert_eq!(reassembled, original);
    });
}

// ═══════════════════════════════════════════════════════════════════════════
// Filesystem-only scenarios
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn backend_rescans_chunks_on_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let data = b"persistent";
    let hash = ContentHash::of(data);

    // Write with first instance.
    {
        let mut backend = FilesystemBackend::new(path.clone());
        backend.write_chunk(&hash, data).unwrap();
    }

    // Reopen — should discover existing chunks.
    let backend = FilesystemBackend::new(path);
    assert!(backend.has_chunk(&hash));
    assert_eq!(backend.read_chunk(&hash).unwrap(), Some(data.to_vec()));
}

#[test]
fn sharding_creates_expected_directory_structure() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().to_path_buf();

    let data = b"shard-check";
    let hash = ContentHash::of(data);
    let hex = hash.to_hex();

    let mut backend = FilesystemBackend::new(path.clone());
    backend.write_chunk(&hash, data).unwrap();

    // Verify the 2-level sharded path exists.
    let expected_path = path
        .join("chunks")
        .join(&hex[..2])
        .join(&hex[2..4])
        .join(&hex);
    assert!(expected_path.exists(), "sharded chunk path should exist: {expected_path:?}");
}

// ═══════════════════════════════════════════════════════════════════════════
// Property-based tests
// ═══════════════════════════════════════════════════════════════════════════

mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn any_chunk_survives_write_read_round_trip(
            data in proptest::collection::vec(any::<u8>(), 1..4096),
        ) {
            // In-memory
            let mut mem = InMemoryBackend::new();
            let hash = ContentHash::of(&data);
            mem.write_chunk(&hash, &data).unwrap();
            prop_assert_eq!(mem.read_chunk(&hash).unwrap(), Some(data.clone()));

            // Filesystem
            let dir = tempfile::tempdir().unwrap();
            let mut fs = FilesystemBackend::new(dir.path().to_path_buf());
            fs.write_chunk(&hash, &data).unwrap();
            prop_assert_eq!(fs.read_chunk(&hash).unwrap(), Some(data));
        }
    }
}
