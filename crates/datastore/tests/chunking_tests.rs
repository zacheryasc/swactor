//! Tests for the chunking engine — pure function contracts.

use std::collections::HashSet;

use swactor_datastore::chunking::{chunk_blob, reassemble_blob, verify_integrity, ChunkingError};
use swactor_datastore::types::ContentHash;

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: basic chunking behaviour
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn small_file_fits_in_single_chunk() {
    let data = b"tiny";
    let (content_hash, manifest, chunks) = chunk_blob(data, 1024);

    assert_eq!(chunks.len(), 1);
    assert_eq!(manifest.chunks.len(), 1);
    assert_eq!(content_hash, ContentHash::of(data));
    // Single chunk's hash == hash of that chunk's data (which is the whole blob).
    assert_eq!(manifest.chunks[0].hash, ContentHash::of(data));
}

#[test]
fn multi_chunk_blob_reassembles_to_original() {
    let data: Vec<u8> = (0..300).map(|i| (i % 256) as u8).collect();
    let chunk_size = 64;

    let (_, manifest, chunks) = chunk_blob(&data, chunk_size);
    assert!(manifest.chunks.len() >= 3);

    let reassembled = reassemble_blob(&manifest, &chunks).unwrap();
    assert_eq!(reassembled, data);
}

#[test]
fn last_chunk_is_smaller_when_not_aligned() {
    let data = vec![0xAB; 100];
    let (_, manifest, chunks) = chunk_blob(&data, 64);

    assert_eq!(chunks.len(), 2);
    assert_eq!(manifest.chunks[0].size, 64);
    assert_eq!(manifest.chunks[1].size, 36);
    assert_eq!(manifest.chunks[0].offset, 0);
    assert_eq!(manifest.chunks[1].offset, 64);
}

#[test]
fn empty_blob_produces_empty_manifest() {
    let (content_hash, manifest, chunks) = chunk_blob(b"", 1024);

    assert!(chunks.is_empty());
    assert!(manifest.chunks.is_empty());
    assert_eq!(manifest.total_size, 0);
    assert_eq!(content_hash, ContentHash::of(b""));

    // Reassembly of empty manifest yields empty data.
    let reassembled = reassemble_blob(&manifest, &chunks).unwrap();
    assert!(reassembled.is_empty());
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: reassembly failure modes
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn reassembly_with_missing_chunk_fails() {
    // Use non-repeating data so each chunk has a unique hash.
    let data: Vec<u8> = (0..128).collect();
    let (_, manifest, mut chunks) = chunk_blob(&data, 64);
    // Remove the last chunk.
    chunks.pop();

    let err = reassemble_blob(&manifest, &chunks).unwrap_err();
    match err {
        ChunkingError::MissingChunk { hash } => {
            assert_eq!(hash, manifest.chunks.last().unwrap().hash);
        }
        other => panic!("expected MissingChunk, got: {other:?}"),
    }
}

#[test]
fn reassembly_detects_wrong_content_hash() {
    let data = vec![42; 128];
    let (_, mut manifest, chunks) = chunk_blob(&data, 64);

    // Corrupt the manifest's content hash.
    manifest.content_hash = ContentHash::of(b"wrong");

    let err = reassemble_blob(&manifest, &chunks).unwrap_err();
    assert!(matches!(err, ChunkingError::HashMismatch { .. }));
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: determinism and deduplication
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn identical_blobs_produce_identical_manifests() {
    let data = b"deterministic input";
    let (h1, m1, c1) = chunk_blob(data, 8);
    let (h2, m2, c2) = chunk_blob(data, 8);

    assert_eq!(h1, h2);
    assert_eq!(m1, m2);
    assert_eq!(c1.len(), c2.len());
    for (a, b) in c1.iter().zip(c2.iter()) {
        assert_eq!(a.0, b.0);
        assert_eq!(a.1, b.1);
    }
}

#[test]
fn deduplication_across_objects() {
    // Two blobs that share a common prefix produce the same chunk hash for that prefix.
    let shared_prefix = vec![0xBE; 64];
    let mut blob_a = shared_prefix.clone();
    blob_a.extend_from_slice(&[0xAA; 64]);
    let mut blob_b = shared_prefix.clone();
    blob_b.extend_from_slice(&[0xBB; 64]);

    let (_, _, chunks_a) = chunk_blob(&blob_a, 64);
    let (_, _, chunks_b) = chunk_blob(&blob_b, 64);

    // First chunk should be identical (shared prefix).
    assert_eq!(chunks_a[0].0, chunks_b[0].0);
    // Second chunk should differ.
    assert_ne!(chunks_a[1].0, chunks_b[1].0);

    // A HashSet of all chunk hashes should have 3 unique entries (shared + 2 distinct).
    let all_hashes: HashSet<_> = chunks_a
        .iter()
        .chain(chunks_b.iter())
        .map(|(h, _)| *h)
        .collect();
    assert_eq!(all_hashes.len(), 3);
}

#[test]
fn verify_integrity_passes_for_correct_data() {
    let data = b"check me";
    let hash = ContentHash::of(data);
    assert!(verify_integrity(data, &hash));
}

#[test]
fn verify_integrity_fails_for_wrong_data() {
    let hash = ContentHash::of(b"original");
    assert!(!verify_integrity(b"tampered", &hash));
}

// ═══════════════════════════════════════════════════════════════════════════
// Property-based tests
// ═══════════════════════════════════════════════════════════════════════════

mod proptests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn chunk_then_reassemble_is_identity(
            data in proptest::collection::vec(any::<u8>(), 0..8192),
            chunk_size in 1u32..=512,
        ) {
            let (_, manifest, chunks) = chunk_blob(&data, chunk_size);
            let reassembled = reassemble_blob(&manifest, &chunks).unwrap();
            prop_assert_eq!(data, reassembled);
        }
    }

    proptest! {
        #[test]
        fn content_hash_matches_blake3_of_whole_blob(
            data in proptest::collection::vec(any::<u8>(), 0..4096),
        ) {
            let (content_hash, manifest, _) = chunk_blob(&data, 256);
            let expected = ContentHash::of(&data);
            prop_assert_eq!(content_hash, expected);
            prop_assert_eq!(manifest.content_hash, expected);
        }
    }

    proptest! {
        #[test]
        fn chunk_offsets_are_contiguous(
            data in proptest::collection::vec(any::<u8>(), 1..4096),
            chunk_size in 1u32..=256,
        ) {
            let (_, manifest, _) = chunk_blob(&data, chunk_size);
            let mut expected_offset = 0u64;
            for chunk_ref in &manifest.chunks {
                prop_assert_eq!(chunk_ref.offset, expected_offset);
                expected_offset += chunk_ref.size as u64;
            }
            prop_assert_eq!(expected_offset, manifest.total_size);
        }
    }
}
