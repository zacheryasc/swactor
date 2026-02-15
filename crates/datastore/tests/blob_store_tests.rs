//! Tests for BlobStoreActor — exercised as a black box through the swactor runtime.

mod common;

use std::collections::HashSet;

use swactor_datastore::chunking::chunk_blob;
use swactor_datastore::messages::{BlobStoreMsg, DatastoreResponse};
use swactor_datastore::types::{ChunkRef, ContentHash, ObjectManifest};

use common::{spawn_blob_store, test_runtime, tick_and_drain, tick_n, tick_until_recv};

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: single chunk CRUD through actor
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn write_chunk_and_read_it_back_through_actor() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let data = b"actor-chunk".to_vec();
    let hash = ContentHash::of(&data);

    // Write.
    rt.send_to(blob, BlobStoreMsg::WriteChunk { hash, data: data.clone(), reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::ChunkStored { hash: h } if h == hash));

    // Read.
    rt.send_to(blob, BlobStoreMsg::ReadChunk { hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ChunkOk { hash: h, data: d } => {
            assert_eq!(h, hash);
            assert_eq!(d, data);
        }
        other => panic!("expected ChunkOk, got: {other:?}"),
    }
}

#[test]
fn reading_absent_chunk_returns_not_found() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();

    rt.send_to(blob, BlobStoreMsg::ReadChunk {
        hash: ContentHash::of(b"ghost"),
        reply_to: *inbox.addr(),
    }).unwrap();

    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::NotFound));
}

#[test]
fn has_chunk_reports_presence_correctly() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let data = b"check-me";
    let hash = ContentHash::of(data);

    // Before write.
    rt.send_to(blob, BlobStoreMsg::HasChunk { hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::Bool(false)));

    // After write.
    rt.send_to(blob, BlobStoreMsg::WriteChunk { hash, data: data.to_vec(), reply_to: reply }).unwrap();
    tick_until_recv(&rt, &inbox, 10); // drain ChunkStored
    rt.send_to(blob, BlobStoreMsg::HasChunk { hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::Bool(true)));
}

#[test]
fn list_chunks_after_storing_several() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let mut expected = HashSet::new();
    for i in 0u8..3 {
        let data = vec![i; 16];
        let hash = ContentHash::of(&data);
        expected.insert(hash);
        rt.send_to(blob, BlobStoreMsg::WriteChunk { hash, data, reply_to: reply }).unwrap();
    }
    // Drain write responses.
    tick_and_drain(&rt, &inbox, 5);

    // List.
    rt.send_to(blob, BlobStoreMsg::ListChunks { reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ChunkList { hashes } => {
            let listed: HashSet<_> = hashes.into_iter().collect();
            assert_eq!(listed, expected);
        }
        other => panic!("expected ChunkList, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: GC unreferenced through actor
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn gc_unreferenced_removes_orphan_chunks() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    // Store 5 chunks.
    let mut all_hashes = Vec::new();
    for i in 0u8..5 {
        let data = vec![i; 32];
        let hash = ContentHash::of(&data);
        all_hashes.push(hash);
        rt.send_to(blob, BlobStoreMsg::WriteChunk { hash, data, reply_to: reply }).unwrap();
    }
    tick_and_drain(&rt, &inbox, 5);

    // Mark only the first 2 as referenced.
    let referenced: HashSet<_> = all_hashes[..2].iter().copied().collect();
    rt.send_to(blob, BlobStoreMsg::GcUnreferenced { referenced: referenced.clone() }).unwrap();
    tick_n(&rt, 3);

    // List remaining.
    rt.send_to(blob, BlobStoreMsg::ListChunks { reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ChunkList { hashes } => {
            let remaining: HashSet<_> = hashes.into_iter().collect();
            assert_eq!(remaining, referenced);
        }
        other => panic!("expected ChunkList, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: manifest CRUD through actor
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn write_and_read_manifest_through_actor() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let manifest = ObjectManifest {
        content_hash: ContentHash::of(b"test-blob"),
        chunks: vec![ChunkRef {
            hash: ContentHash::of(b"chunk-0"),
            offset: 0,
            size: 256,
        }],
        total_size: 256,
        chunk_size: 1024,
        content_type: None,
    };

    // Write.
    rt.send_to(blob, BlobStoreMsg::WriteManifest { manifest: manifest.clone(), reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::ManifestStored { .. }));

    // Read.
    rt.send_to(blob, BlobStoreMsg::ReadManifest { hash: manifest.content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ManifestOk { manifest: m } => assert_eq!(m, manifest),
        other => panic!("expected ManifestOk, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: full blob lifecycle through actor
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn full_blob_lifecycle_through_actor() {
    let rt = test_runtime();
    let blob = spawn_blob_store(&rt);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let original: Vec<u8> = (0..300).map(|i| (i % 256) as u8).collect();
    let (content_hash, manifest, chunks) = chunk_blob(&original, 128);

    // Store all chunks.
    for (hash, data) in &chunks {
        rt.send_to(blob, BlobStoreMsg::WriteChunk { hash: *hash, data: data.clone(), reply_to: reply }).unwrap();
    }
    tick_and_drain(&rt, &inbox, 5);

    // Store manifest.
    rt.send_to(blob, BlobStoreMsg::WriteManifest { manifest: manifest.clone(), reply_to: reply }).unwrap();
    tick_and_drain(&rt, &inbox, 3);

    // Read manifest back.
    rt.send_to(blob, BlobStoreMsg::ReadManifest { hash: content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    let read_manifest = match resp {
        DatastoreResponse::ManifestOk { manifest: m } => m,
        other => panic!("expected ManifestOk, got: {other:?}"),
    };

    // Read all chunks and reassemble.
    let mut reassembled = Vec::new();
    for chunk_ref in &read_manifest.chunks {
        rt.send_to(blob, BlobStoreMsg::ReadChunk { hash: chunk_ref.hash, reply_to: reply }).unwrap();
        let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
        match resp {
            DatastoreResponse::ChunkOk { data, .. } => reassembled.extend_from_slice(&data),
            other => panic!("expected ChunkOk, got: {other:?}"),
        }
    }

    assert_eq!(reassembled, original);
    assert!(swactor_datastore::verify_integrity(&reassembled, &content_hash));
}
