//! Integration-style tests for the swactor-datastore crate.
//!
//! Focuses on scenario/story tests and property-based tests that exercise
//! the protocol through its public types — low coupling to internals.

use std::collections::HashSet;

use swactor_datastore::types::{
    ChunkRef, ContentHash, DatastoreConfig, ObjectEntry, ObjectManifest,
};

use swactor_transport::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: Content-addressing round-trip
// ═══════════════════════════════════════════════════════════════════════════

/// Chunking a file, building a manifest, and reassembling produces the
/// original data — the core content-addressing contract.
#[test]
fn chunking_and_reassembly_preserves_data() {
    let original_data = b"Hello, distributed world! This is a test blob.";
    let chunk_size: u32 = 16; // Small chunks for testing.

    // Chunk the data.
    let mut chunks: Vec<(ContentHash, Vec<u8>)> = Vec::new();
    let mut chunk_refs: Vec<ChunkRef> = Vec::new();
    let mut offset: u64 = 0;

    for chunk_data in original_data.chunks(chunk_size as usize) {
        let hash = ContentHash::of(chunk_data);
        chunk_refs.push(ChunkRef {
            hash,
            offset,
            size: chunk_data.len() as u32,
        });
        chunks.push((hash, chunk_data.to_vec()));
        offset += chunk_data.len() as u64;
    }

    // content_hash = blake3(entire_blob)
    let content_hash = ContentHash::of(original_data);

    let manifest = ObjectManifest {
        content_hash,
        chunks: chunk_refs,
        total_size: original_data.len() as u64,
        chunk_size,
        content_type: Some("application/octet-stream".to_string()),
    };

    // Reassemble from chunks using manifest order.
    let mut reassembled = Vec::new();
    for chunk_ref in &manifest.chunks {
        let (_, data) = chunks
            .iter()
            .find(|(h, _)| *h == chunk_ref.hash)
            .expect("chunk not found");
        reassembled.extend_from_slice(data);
    }

    assert_eq!(reassembled.as_slice(), original_data.as_slice());
    assert_eq!(reassembled.len() as u64, manifest.total_size);

    // Verify content hash matches the reassembled data.
    assert_eq!(ContentHash::of(&reassembled), content_hash);
}

/// Identical data produces identical content hashes (deterministic).
#[test]
fn identical_data_produces_same_hash() {
    let data = b"same content";
    assert_eq!(ContentHash::of(data), ContentHash::of(data));
}

/// Different data produces different content hashes (collision resistance).
#[test]
fn different_data_produces_different_hashes() {
    assert_ne!(ContentHash::of(b"alpha"), ContentHash::of(b"beta"));
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: Manifest serialization round-trip
// ═══════════════════════════════════════════════════════════════════════════

/// A manifest can be serialized to JSON and deserialized back without data loss.
#[test]
fn manifest_survives_json_round_trip() {
    let manifest = ObjectManifest {
        content_hash: ContentHash::of(b"my-entire-blob"),
        chunks: vec![
            ChunkRef {
                hash: ContentHash::of(b"chunk-0"),
                offset: 0,
                size: 1_048_576,
            },
            ChunkRef {
                hash: ContentHash::of(b"chunk-1"),
                offset: 1_048_576,
                size: 524_288,
            },
        ],
        total_size: 1_572_864,
        chunk_size: 1_048_576,
        content_type: Some("image/jpeg".to_string()),
    };

    let json = serde_json::to_string(&manifest).unwrap();
    let deserialized: ObjectManifest = serde_json::from_str(&json).unwrap();

    assert_eq!(manifest, deserialized);
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: ContentHash hex round-trip
// ═══════════════════════════════════════════════════════════════════════════

/// to_hex → from_hex round-trips for any content hash.
#[test]
fn content_hash_hex_round_trip() {
    let data = b"round-trip through hex encoding";
    let hash = ContentHash::of(data);
    let hex = hash.to_hex();
    let recovered = ContentHash::from_hex(&hex).expect("valid hex should parse");
    assert_eq!(hash, recovered);
}

/// from_hex rejects strings that are not exactly 64 hex characters.
#[test]
fn from_hex_rejects_wrong_length() {
    assert!(ContentHash::from_hex("abcd").is_none());
    assert!(ContentHash::from_hex("").is_none());
    // 63 chars
    assert!(ContentHash::from_hex(
        &"a".repeat(63)
    ).is_none());
    // 65 chars
    assert!(ContentHash::from_hex(
        &"a".repeat(65)
    ).is_none());
}

/// from_hex rejects non-hex characters.
#[test]
fn from_hex_rejects_non_hex_chars() {
    // 'g' is not valid hex
    let bad = format!("{}g{}", "a".repeat(31), "a".repeat(32));
    assert_eq!(bad.len(), 64);
    assert!(ContentHash::from_hex(&bad).is_none());
}

/// from_hex accepts uppercase hex.
#[test]
fn from_hex_accepts_uppercase() {
    let hash = ContentHash::of(b"uppercase test");
    let hex_upper = hash.to_hex().to_uppercase();
    let recovered = ContentHash::from_hex(&hex_upper).expect("uppercase hex should parse");
    assert_eq!(hash, recovered);
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: ContentHash DHT properties
// ═══════════════════════════════════════════════════════════════════════════

/// XOR distance is symmetric — required for Kademlia correctness.
#[test]
fn xor_distance_is_symmetric() {
    let a = ContentHash::of(b"node-alpha");
    let b = ContentHash::of(b"node-beta");

    assert_eq!(a.xor_distance(&b), b.xor_distance(&a));
}

/// XOR distance to self is zero — a node is closest to itself.
#[test]
fn xor_distance_to_self_is_zero() {
    let a = ContentHash::of(b"self");
    assert_eq!(a.xor_distance(&a), [0u8; 32]);
    assert_eq!(a.xor_leading_zeros(&a), 256);
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: ObjectEntry serialization
// ═══════════════════════════════════════════════════════════════════════════

/// An ObjectEntry with tags survives JSON round-trip.
#[test]
fn object_entry_with_tags_survives_round_trip() {
    let mut tags = std::collections::BTreeMap::new();
    tags.insert("album".to_string(), "vacation-2024".to_string());
    tags.insert("device".to_string(), "phone".to_string());

    let entry = ObjectEntry {
        content_hash: ContentHash::of(b"beach-photo-bytes"),
        name: Some("beach.jpg".to_string()),
        node_id: NodeId([0x42; 32]),
        tags,
        size_bytes: 4_500_000,
        created_at: 1700000000,
    };

    let json = serde_json::to_string(&entry).unwrap();
    let deserialized: ObjectEntry = serde_json::from_str(&json).unwrap();

    assert_eq!(entry, deserialized);
}

/// An ObjectEntry without a name survives JSON round-trip.
#[test]
fn object_entry_without_name_survives_round_trip() {
    let entry = ObjectEntry {
        content_hash: ContentHash::of(b"anonymous-blob"),
        name: None,
        node_id: NodeId([0x01; 32]),
        tags: std::collections::BTreeMap::new(),
        size_bytes: 1024,
        created_at: 0,
    };

    let json = serde_json::to_string(&entry).unwrap();
    let deserialized: ObjectEntry = serde_json::from_str(&json).unwrap();

    assert_eq!(entry, deserialized);
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: DatastoreConfig defaults
// ═══════════════════════════════════════════════════════════════════════════

/// Default config uses 1MB chunks — the documented default for the protocol.
#[test]
fn default_config_uses_1mb_chunks() {
    let config = DatastoreConfig::default();
    assert_eq!(config.chunk_size, 1_048_576);
}

// ═══════════════════════════════════════════════════════════════════════════
// Property-based tests
// ═══════════════════════════════════════════════════════════════════════════

mod proptests {
    use super::*;
    use proptest::prelude::*;

    // ContentHash::of is a pure function — same input always gives same output.
    proptest! {
        #[test]
        fn content_hash_is_deterministic(data in proptest::collection::vec(any::<u8>(), 0..4096)) {
            prop_assert_eq!(ContentHash::of(&data), ContentHash::of(&data));
        }
    }

    // XOR distance is always symmetric.
    proptest! {
        #[test]
        fn xor_distance_symmetry(
            a_bytes in proptest::collection::vec(any::<u8>(), 32..=32),
            b_bytes in proptest::collection::vec(any::<u8>(), 32..=32),
        ) {
            let a = ContentHash(a_bytes.try_into().unwrap());
            let b = ContentHash(b_bytes.try_into().unwrap());
            prop_assert_eq!(a.xor_distance(&b), b.xor_distance(&a));
        }
    }

    // ContentHash::from_hex is the inverse of to_hex.
    proptest! {
        #[test]
        fn hex_round_trip(data in proptest::collection::vec(any::<u8>(), 1..1024)) {
            let hash = ContentHash::of(&data);
            let hex = hash.to_hex();
            let recovered = ContentHash::from_hex(&hex).unwrap();
            prop_assert_eq!(hash, recovered);
        }
    }

    // Chunking any data and reassembling preserves the original.
    proptest! {
        #[test]
        fn chunk_reassemble_identity(
            data in proptest::collection::vec(any::<u8>(), 1..8192),
            chunk_size in 1u32..=256,
        ) {
            let mut chunks: Vec<(ContentHash, Vec<u8>)> = Vec::new();
            for chunk_data in data.chunks(chunk_size as usize) {
                chunks.push((ContentHash::of(chunk_data), chunk_data.to_vec()));
            }

            let reassembled: Vec<u8> = chunks.iter().flat_map(|(_, d)| d.iter().copied()).collect();
            prop_assert_eq!(data, reassembled);
        }
    }

    // Content deduplication: chunks with identical data share the same hash,
    // so storing them once is correct.
    proptest! {
        #[test]
        fn duplicate_chunks_deduplicate(data in proptest::collection::vec(any::<u8>(), 1..512)) {
            let hash1 = ContentHash::of(&data);
            let hash2 = ContentHash::of(&data);
            prop_assert_eq!(hash1, hash2);

            // Storing both in a HashSet yields one entry.
            let mut set = HashSet::new();
            set.insert(hash1);
            set.insert(hash2);
            prop_assert_eq!(set.len(), 1);
        }
    }
}
