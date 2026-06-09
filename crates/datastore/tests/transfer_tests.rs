//! Tests for TransferActor — exercised as a black box through the swactor runtime.

mod common;

use swactor_datastore::chunking::{chunk_blob, reassemble_blob, verify_integrity};
use swactor_datastore::messages::{BlobStoreMsg, DatastoreResponse, TransferMsg};
use swactor_datastore::types::{ChunkRef, ContentHash, ObjectManifest};

use swactor_transport::NodeId;

use common::{
    spawn_blob_store, spawn_transfer, test_runtime, tick_and_drain, tick_n, tick_until_recv,
};

// ─── Helpers ─────────────────────────────────────────────────────────────────

fn test_source_node() -> NodeId {
    NodeId([0xAA; 32])
}

/// Build a manifest with `n` distinct 16-byte chunks.
/// Returns the manifest and a vec of (hash, data) pairs for each chunk.
fn make_multi_chunk_manifest(n: usize) -> (ObjectManifest, Vec<(ContentHash, Vec<u8>)>) {
    let mut all_data = Vec::new();
    let mut chunks_data = Vec::new();
    let mut chunk_refs = Vec::new();

    for i in 0..n {
        let data = vec![i as u8; 16];
        let hash = ContentHash::of(&data);
        chunk_refs.push(ChunkRef {
            hash,
            offset: (i * 16) as u64,
            size: 16,
        });
        chunks_data.push((hash, data.clone()));
        all_data.extend_from_slice(&data);
    }

    let content_hash = ContentHash::of(&all_data);
    let manifest = ObjectManifest {
        content_hash,
        chunks: chunk_refs,
        total_size: all_data.len() as u64,
        chunk_size: 16,
        content_type: None,
    };

    (manifest, chunks_data)
}

// ═══════════════════════════════════════════════════════════════════════════
// Download Completion
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn single_chunk_download_completes() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(1);
    let expected_content_hash = manifest.content_hash;

    // Start the download.
    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // Deliver the single chunk.
    let (hash, data) = &chunks[0];
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash,
            data: data.clone(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferComplete { content_hash } => {
            assert_eq!(content_hash, expected_content_hash);
        }
        other => panic!("expected TransferComplete, got: {other:?}"),
    }
}

#[test]
fn multi_chunk_download_completes_after_all_chunks_arrive() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(3);
    let expected_content_hash = manifest.content_hash;

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // Deliver first two chunks — no TransferComplete yet.
    for (hash, data) in &chunks[..2] {
        rt.send_to(
            transfer,
            TransferMsg::ChunkReceived {
                hash: *hash,
                data: data.clone(),
            },
        )
        .unwrap();
    }
    let partial = tick_and_drain(&rt, &inbox, 10);
    assert!(
        partial.is_empty(),
        "expected no response after partial delivery, got: {partial:?}"
    );

    // Deliver the final chunk.
    let (hash, data) = &chunks[2];
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash,
            data: data.clone(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferComplete { content_hash } => {
            assert_eq!(content_hash, expected_content_hash);
        }
        other => panic!("expected TransferComplete, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Chunk Deduplication & Filtering
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn duplicate_chunk_is_silently_ignored() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(2);
    let expected_content_hash = manifest.content_hash;

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    let (hash_a, data_a) = &chunks[0];
    let (hash_b, data_b) = &chunks[1];

    // Send chunk A.
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash_a,
            data: data_a.clone(),
        },
    )
    .unwrap();
    tick_n(&rt, 3);

    // Send chunk A again (duplicate).
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash_a,
            data: data_a.clone(),
        },
    )
    .unwrap();
    let after_dup = tick_and_drain(&rt, &inbox, 5);
    assert!(
        after_dup.is_empty(),
        "duplicate chunk should not trigger early completion: {after_dup:?}"
    );

    // Send chunk B — now transfer completes.
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash_b,
            data: data_b.clone(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferComplete { content_hash } => {
            assert_eq!(content_hash, expected_content_hash);
        }
        other => panic!("expected TransferComplete, got: {other:?}"),
    }
}

#[test]
fn unexpected_chunk_hash_is_ignored() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(1);
    let expected_content_hash = manifest.content_hash;

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // Send a chunk with a bogus hash (not in manifest).
    let bogus_data = b"bogus-data-not-in-manifest".to_vec();
    let bogus_hash = ContentHash::of(&bogus_data);
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: bogus_hash,
            data: bogus_data,
        },
    )
    .unwrap();
    let after_bogus = tick_and_drain(&rt, &inbox, 5);
    assert!(
        after_bogus.is_empty(),
        "unexpected chunk should have no effect: {after_bogus:?}"
    );

    // Now send the correct chunk.
    let (hash, data) = &chunks[0];
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash,
            data: data.clone(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferComplete { content_hash } => {
            assert_eq!(content_hash, expected_content_hash);
        }
        other => panic!("expected TransferComplete, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Retry & Failure
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn first_chunk_failure_allows_retry_and_eventual_success() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(1);
    let expected_content_hash = manifest.content_hash;
    let (hash, data) = &chunks[0];

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // First failure — should be silently retried (no TransferFailed).
    rt.send_to(
        transfer,
        TransferMsg::ChunkFailed {
            hash: *hash,
            reason: "timeout".into(),
        },
    )
    .unwrap();
    let after_fail = tick_and_drain(&rt, &inbox, 5);
    assert!(
        after_fail.is_empty(),
        "first failure should not produce TransferFailed: {after_fail:?}"
    );

    // Retry succeeds.
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash,
            data: data.clone(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferComplete { content_hash } => {
            assert_eq!(content_hash, expected_content_hash);
        }
        other => panic!("expected TransferComplete after retry, got: {other:?}"),
    }
}

#[test]
fn second_chunk_failure_fails_entire_transfer() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(1);
    let (hash, _) = &chunks[0];

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // First failure — silent retry.
    rt.send_to(
        transfer,
        TransferMsg::ChunkFailed {
            hash: *hash,
            reason: "timeout".into(),
        },
    )
    .unwrap();
    tick_and_drain(&rt, &inbox, 5);

    // Second failure — exhausts max_retries=1 → TransferFailed.
    rt.send_to(
        transfer,
        TransferMsg::ChunkFailed {
            hash: *hash,
            reason: "connection lost".into(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferFailed { reason } => {
            assert_eq!(reason, "connection lost");
        }
        other => panic!("expected TransferFailed, got: {other:?}"),
    }
}

#[test]
fn partial_progress_lost_when_one_chunk_exhausts_retries() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(3);
    let (hash_a, data_a) = &chunks[0];
    let (hash_b, _) = &chunks[1];

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // Chunk A received successfully.
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash_a,
            data: data_a.clone(),
        },
    )
    .unwrap();
    tick_n(&rt, 3);

    // Chunk B fails twice → transfer fails despite chunk A being received.
    rt.send_to(
        transfer,
        TransferMsg::ChunkFailed {
            hash: *hash_b,
            reason: "fail-1".into(),
        },
    )
    .unwrap();
    tick_and_drain(&rt, &inbox, 5);

    rt.send_to(
        transfer,
        TransferMsg::ChunkFailed {
            hash: *hash_b,
            reason: "fail-2".into(),
        },
    )
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferFailed { reason } => {
            assert_eq!(reason, "fail-2");
        }
        other => panic!("expected TransferFailed, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Cancellation
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn cancel_stops_transfer_with_no_response() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(2);

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // Deliver one chunk (partial progress).
    let (hash_a, data_a) = &chunks[0];
    rt.send_to(
        transfer,
        TransferMsg::ChunkReceived {
            hash: *hash_a,
            data: data_a.clone(),
        },
    )
    .unwrap();
    tick_n(&rt, 3);

    // Cancel.
    rt.send_to(transfer, TransferMsg::Cancel).unwrap();

    // After cancel, no TransferComplete or TransferFailed should appear.
    let after_cancel = tick_and_drain(&rt, &inbox, 10);
    assert!(
        after_cancel.is_empty(),
        "cancel should produce no response: {after_cancel:?}"
    );
}

// ═══════════════════════════════════════════════════════════════════════════
// Persistence to BlobStoreActor
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn received_chunks_are_forwarded_to_blob_store() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    let (manifest, chunks) = make_multi_chunk_manifest(2);

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest,
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // Deliver both chunks.
    for (hash, data) in &chunks {
        rt.send_to(
            transfer,
            TransferMsg::ChunkReceived {
                hash: *hash,
                data: data.clone(),
            },
        )
        .unwrap();
    }

    // Wait for TransferComplete.
    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    assert!(
        matches!(resp, DatastoreResponse::TransferComplete { .. }),
        "expected TransferComplete, got: {resp:?}"
    );

    // Give extra ticks for blob store writes to settle.
    tick_n(&rt, 5);

    // Verify both chunks are readable from BlobStoreActor.
    for (hash, expected_data) in &chunks {
        rt.send_to(
            blob,
            BlobStoreMsg::ReadChunk {
                hash: *hash,
                reply_to: reply,
            },
        )
        .unwrap();
        let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ChunkOk { hash: h, data } => {
                assert_eq!(h, *hash);
                assert_eq!(data, *expected_data);
            }
            other => panic!("expected ChunkOk for {hash:?}, got: {other:?}"),
        }
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Full Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn download_and_reassemble_recovers_original_data() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let transfer = spawn_transfer(&rt, blob);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();
    tick_n(&rt, 2);

    // Use real chunking to produce a multi-chunk manifest.
    let original: Vec<u8> = (0..200).map(|i| (i % 256) as u8).collect();
    let (content_hash, manifest, chunks) = chunk_blob(&original, 64);

    rt.send_to(
        transfer,
        TransferMsg::StartDownload {
            manifest: manifest.clone(),
            source_node: test_source_node(),
            reply_to: reply,
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    // Deliver all chunks via TransferActor.
    for (hash, data) in &chunks {
        rt.send_to(
            transfer,
            TransferMsg::ChunkReceived {
                hash: *hash,
                data: data.clone(),
            },
        )
        .unwrap();
    }

    let resp = tick_until_recv(&rt, &inbox, 20).unwrap();
    match resp {
        DatastoreResponse::TransferComplete {
            content_hash: ch, ..
        } => {
            assert_eq!(ch, content_hash);
        }
        other => panic!("expected TransferComplete, got: {other:?}"),
    }

    // Give blob store time to persist.
    tick_n(&rt, 5);

    // Read all chunks from BlobStoreActor and reassemble.
    let mut chunk_pairs = Vec::new();
    for chunk_ref in &manifest.chunks {
        rt.send_to(
            blob,
            BlobStoreMsg::ReadChunk {
                hash: chunk_ref.hash,
                reply_to: reply,
            },
        )
        .unwrap();
        let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ChunkOk { hash, data } => {
                chunk_pairs.push((hash, data));
            }
            other => panic!("expected ChunkOk, got: {other:?}"),
        }
    }

    let reassembled = reassemble_blob(&manifest, &chunk_pairs).expect("reassembly should succeed");
    assert!(verify_integrity(&reassembled, &content_hash));
    assert_eq!(reassembled, original);
}
