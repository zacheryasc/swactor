//! Scenario tests for the DatastoreNode coordinator actor.
//!
//! All tests route commands through a single DatastoreNode address.
//! Black-box only — no direct access to BlobStoreActor or MetadataActor.

mod common;

use std::collections::BTreeMap;

use common::{tick_n, tick_until_recv, NodeHarness};

use swactor_datastore::messages::DatastoreNodeMsg;
use swactor_datastore::types::ContentHash;
use swactor_datastore::messages::{DatastoreResponse, GetChunkRequest};
use swactor_datastore::{reassemble_blob, verify_integrity};

use distribution::types::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// Put & Retrieve
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn put_returns_content_hash() {
    let h = NodeHarness::new();
    let data = b"hello datastore";

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Put {
            data: data.to_vec(),
            name: None,
            tags: BTreeMap::new(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::PutOk { content_hash } => {
            assert_eq!(content_hash, ContentHash::of(data));
        }
        other => panic!("expected PutOk, got {other:?}"),
    }
}

#[test]
fn put_then_get_returns_entry_and_manifest() {
    let h = NodeHarness::new();
    let data = b"greeting.txt contents";

    // Put
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Put {
            data: data.to_vec(),
            name: Some("greeting.txt".to_string()),
            tags: BTreeMap::new(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let content_hash = match resp {
        DatastoreResponse::PutOk { content_hash } => content_hash,
        other => panic!("expected PutOk, got {other:?}"),
    };

    // Get
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::GetOk { entry, manifest } => {
            assert_eq!(entry.name.as_deref(), Some("greeting.txt"));
            assert_eq!(entry.content_hash, content_hash);
            assert_eq!(manifest.total_size, data.len() as u64);
            assert!(!manifest.chunks.is_empty());
        }
        other => panic!("expected GetOk, got {other:?}"),
    }
}

#[test]
fn put_and_read_chunks_recovers_original_data() {
    let h = NodeHarness::new();
    // 200 bytes with chunk_size=64 → 4 chunks (64+64+64+8)
    let data: Vec<u8> = (0..200).map(|i| (i % 251) as u8).collect();

    // Put
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Put {
            data: data.clone(),
            name: Some("multi-chunk".to_string()),
            tags: BTreeMap::new(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let content_hash = match resp {
        DatastoreResponse::PutOk { content_hash } => content_hash,
        other => panic!("expected PutOk, got {other:?}"),
    };

    // Get manifest
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let manifest = match resp {
        DatastoreResponse::GetOk { manifest, .. } => manifest,
        other => panic!("expected GetOk, got {other:?}"),
    };

    assert!(manifest.chunks.len() > 1, "expected multi-chunk manifest");

    // ReadChunk for each chunk
    let mut chunks = Vec::new();
    for chunk_ref in &manifest.chunks {
        h.rt.send_to(
            h.node,
            DatastoreNodeMsg::ReadChunk {
                hash: chunk_ref.hash,
                reply_to: h.reply_addr(),
            },
        )
        .unwrap();

        let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
        match resp {
            DatastoreResponse::ChunkOk { hash, data } => {
                chunks.push((hash, data));
            }
            other => panic!("expected ChunkOk, got {other:?}"),
        }
    }

    // Reassemble and verify
    let recovered = reassemble_blob(&manifest, &chunks).unwrap();
    assert!(verify_integrity(&recovered, &content_hash));
    assert_eq!(recovered, data);
}

#[test]
fn put_with_tags_preserves_metadata() {
    let h = NodeHarness::new();
    let data = b"tagged blob";

    let mut tags = BTreeMap::new();
    tags.insert("album".to_string(), "vacation".to_string());
    tags.insert("year".to_string(), "2024".to_string());

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Put {
            data: data.to_vec(),
            name: None,
            tags: tags.clone(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let content_hash = match resp {
        DatastoreResponse::PutOk { content_hash } => content_hash,
        other => panic!("expected PutOk, got {other:?}"),
    };

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::GetOk { entry, .. } => {
            assert_eq!(entry.tags, tags);
        }
        other => panic!("expected GetOk, got {other:?}"),
    }
}

#[test]
fn put_empty_blob_succeeds() {
    let h = NodeHarness::new();
    let data = b"";

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Put {
            data: data.to_vec(),
            name: None,
            tags: BTreeMap::new(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let content_hash = match resp {
        DatastoreResponse::PutOk { content_hash } => content_hash,
        other => panic!("expected PutOk, got {other:?}"),
    };

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::GetOk { manifest, .. } => {
            assert_eq!(manifest.chunks.len(), 0);
            assert_eq!(manifest.total_size, 0);
        }
        other => panic!("expected GetOk, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Get & Delete
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn get_nonexistent_returns_not_found() {
    let h = NodeHarness::new();
    let bogus_hash = ContentHash::of(b"never stored");

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Get {
            content_hash: bogus_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    assert!(
        matches!(resp, DatastoreResponse::NotFound),
        "expected NotFound, got {resp:?}"
    );
}

#[test]
fn delete_makes_object_unretrievable() {
    let h = NodeHarness::new();
    let data = b"ephemeral data";

    // Put
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Put {
            data: data.to_vec(),
            name: None,
            tags: BTreeMap::new(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let content_hash = match resp {
        DatastoreResponse::PutOk { content_hash } => content_hash,
        other => panic!("expected PutOk, got {other:?}"),
    };

    // Delete
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Delete {
            content_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    assert!(
        matches!(resp, DatastoreResponse::DeleteOk { .. }),
        "expected DeleteOk, got {resp:?}"
    );

    // Get should now return NotFound
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    assert!(
        matches!(resp, DatastoreResponse::NotFound),
        "expected NotFound after delete, got {resp:?}"
    );
}

#[test]
fn delete_nonexistent_returns_not_found() {
    let h = NodeHarness::new();
    let bogus_hash = ContentHash::of(b"never stored");

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Delete {
            content_hash: bogus_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    assert!(
        matches!(resp, DatastoreResponse::NotFound),
        "expected NotFound, got {resp:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// List
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn list_returns_all_stored_objects() {
    let h = NodeHarness::new();

    let blobs: Vec<&[u8]> = vec![b"blob-one", b"blob-two", b"blob-three"];
    let mut expected_hashes = Vec::new();

    for blob in &blobs {
        h.rt.send_to(
            h.node,
            DatastoreNodeMsg::Put {
                data: blob.to_vec(),
                name: None,
                tags: BTreeMap::new(),
                reply_to: h.reply_addr(),
            },
        )
        .unwrap();

        let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
        match resp {
            DatastoreResponse::PutOk { content_hash } => {
                expected_hashes.push(content_hash);
            }
            other => panic!("expected PutOk, got {other:?}"),
        }
    }

    // List all
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::List {
            name_filter: None,
            all: false,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => {
            assert_eq!(entries.len(), 3);
            let listed_hashes: Vec<_> = entries.iter().map(|e| e.content_hash).collect();
            for hash in &expected_hashes {
                assert!(
                    listed_hashes.contains(hash),
                    "expected hash {hash:?} in list"
                );
            }
        }
        other => panic!("expected ListOk, got {other:?}"),
    }
}

#[test]
fn list_with_name_filter_matches_correctly() {
    let h = NodeHarness::new();

    let named_blobs = vec![
        (b"alpha content" as &[u8], Some("alpha.txt")),
        (b"alphabet content", Some("alphabet.txt")),
        (b"unnamed content", None),
    ];

    for (data, name) in &named_blobs {
        h.rt.send_to(
            h.node,
            DatastoreNodeMsg::Put {
                data: data.to_vec(),
                name: name.map(|s| s.to_string()),
                tags: BTreeMap::new(),
                reply_to: h.reply_addr(),
            },
        )
        .unwrap();

        let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
        assert!(matches!(resp, DatastoreResponse::PutOk { .. }));
    }

    // Filter "alpha" → should match both alpha.txt and alphabet.txt
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::List {
            name_filter: Some("alpha".to_string()),
            all: false,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => {
            assert_eq!(entries.len(), 2, "expected 2 matches for 'alpha'");
        }
        other => panic!("expected ListOk, got {other:?}"),
    }

    // Filter "zzz" → should match nothing
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::List {
            name_filter: Some("zzz".to_string()),
            all: false,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => {
            assert_eq!(entries.len(), 0, "expected 0 matches for 'zzz'");
        }
        other => panic!("expected ListOk, got {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Status & Network Protocol
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn status_reports_node_identity() {
    let h = NodeHarness::new();

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Status {
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::NodeStatus { node_id } => {
            assert_eq!(node_id, h.node_id);
        }
        other => panic!("expected NodeStatus, got {other:?}"),
    }
}

#[test]
fn incoming_chunk_request_serves_stored_data() {
    let h = NodeHarness::new();
    let data = b"network accessible blob";

    // Put the blob so chunks are persisted
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Put {
            data: data.to_vec(),
            name: None,
            tags: BTreeMap::new(),
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let content_hash = match resp {
        DatastoreResponse::PutOk { content_hash } => content_hash,
        other => panic!("expected PutOk, got {other:?}"),
    };

    // Let chunk writes settle
    tick_n(&h.rt, 5);

    // Get the manifest to find chunk hashes
    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    let manifest = match resp {
        DatastoreResponse::GetOk { manifest, .. } => manifest,
        other => panic!("expected GetOk, got {other:?}"),
    };

    // Simulate an incoming network GetChunk request for the first chunk
    let chunk_hash = manifest.chunks[0].hash;
    let remote_node = NodeId([0xAA; 32]);

    h.rt.send_to(
        h.node,
        DatastoreNodeMsg::IncomingGetChunk {
            request: GetChunkRequest {
                from: remote_node,
                hash: chunk_hash,
            },
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&h.rt, &h.inbox, 20).unwrap();
    match resp {
        DatastoreResponse::ChunkOk { hash, data: chunk_data } => {
            assert_eq!(hash, chunk_hash);
            assert!(!chunk_data.is_empty());
        }
        other => panic!("expected ChunkOk, got {other:?}"),
    }
}
