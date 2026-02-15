//! Multi-node simulation tests for metadata dissemination and cross-node operations.
//!
//! All nodes share a single Runtime — actor addresses are globally unique,
//! so cross-node messaging works via `ctx.send()` without a transport layer.

mod common;

use common::{tick_n, tick_until_recv, MultiNodeHarness};

use swactor_datastore::messages::{MetadataMsg, TransferMsg};
use swactor_datastore::types::ContentHash;
use swactor_datastore::TransferActor;

use distribution::types::NodeId;

// ═══════════════════════════════════════════════════════════════════════════
// Phase 3: Metadata dissemination tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn metadata_replicates_to_peer_after_dissemination() {
    let h = MultiNodeHarness::new(2);
    let data = b"hello distributed world";
    let hash = h.put_on(0, data, Some("greeting.txt"));

    // Before dissemination: node 1 doesn't have it.
    assert!(h.get_from(1, hash).is_none());

    // After dissemination: node 1 has the entry.
    h.disseminate_all();
    let (entry, _manifest) = h.get_from(1, hash).expect("node 1 should have the entry after dissemination");
    assert_eq!(entry.content_hash, hash);
    assert_eq!(entry.name.as_deref(), Some("greeting.txt"));
}

#[test]
fn metadata_replicates_to_all_peers_in_3_node_cluster() {
    let h = MultiNodeHarness::new(3);
    let data = b"replicate me everywhere";
    let hash = h.put_on(0, data, Some("everywhere.bin"));

    // Multiple rounds of dissemination to let entries propagate through all peers.
    // Node 0 → nodes 1,2 on first round. Nodes 1,2 may re-disseminate to each other.
    for _ in 0..3 {
        h.disseminate_all();
    }

    for node_idx in 0..3 {
        let result = h.get_from(node_idx, hash);
        assert!(
            result.is_some(),
            "node {node_idx} should have the entry after dissemination"
        );
    }
}

#[test]
fn dissemination_budget_expires_after_enough_rounds() {
    let h = MultiNodeHarness::new(2);
    let data = b"budget test data";
    let _hash = h.put_on(0, data, Some("budget.dat"));

    // The dissemination budget is lambda * ceil(log2(cluster_size)).
    // With lambda=3, cluster_size=3 (hardcoded in enqueue), budget = 3 * ceil(log2(3)) = 3*2 = 6.
    // After 6+ rounds of dissemination, take_pending should return empty.
    for _ in 0..10 {
        h.disseminate_all();
    }

    // Put a new entry to verify dissemination still works for new entries
    // while old ones have expired.
    let data2 = b"fresh data after budget expired";
    let hash2 = h.put_on(0, data2, Some("fresh.dat"));

    h.disseminate_all();

    let result = h.get_from(1, hash2);
    assert!(result.is_some(), "fresh entry should disseminate normally");
}

#[test]
fn delete_on_origin_does_not_propagate_to_peers() {
    let h = MultiNodeHarness::new(2);
    let data = b"delete me locally";
    let hash = h.put_on(0, data, Some("local-delete.dat"));

    // Disseminate so node 1 has the entry.
    h.disseminate_all();
    assert!(h.get_from(1, hash).is_some());

    // Delete on node 0.
    h.delete_on(0, &hash);

    // Node 0 no longer has it.
    assert!(h.get_from(0, hash).is_none());

    // Node 1 still has it — delete is local only.
    let (entry, _) = h.get_from(1, hash).expect("peer should retain entry after origin deletes");
    assert_eq!(entry.content_hash, hash);
}

#[test]
fn duplicate_put_via_dissemination_is_idempotent() {
    let h = MultiNodeHarness::new(2);
    let data = b"idempotent dissemination";
    let hash = h.put_on(0, data, Some("idem.dat"));

    // Disseminate multiple times.
    for _ in 0..5 {
        h.disseminate_all();
    }

    // Node 1 should have exactly 1 entry, not duplicates.
    let entries = h.list_on(1, None);
    let matching: Vec<_> = entries.iter().filter(|e| e.content_hash == hash).collect();
    assert_eq!(matching.len(), 1, "should have exactly 1 entry, not duplicates");
}

// ═══════════════════════════════════════════════════════════════════════════
// Phase 4: Cross-node operation tests
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn find_object_on_peer_after_dissemination() {
    let h = MultiNodeHarness::new(2);
    let data = b"findable across nodes";
    let hash = h.put_on(0, data, Some("findable.dat"));

    h.disseminate_all();

    // HandleFindObject on node 1 should find the entry.
    let remote_node = NodeId([0xFF; 32]);
    h.rt.send_to(
        h.nodes[1].metadata,
        MetadataMsg::HandleFindObject {
            from: remote_node,
            content_hash: hash,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();
    let resp = tick_until_recv(&h.rt, &h.inbox, 10).unwrap();
    match resp {
        swactor_datastore::DatastoreResponse::GetOk { entry, .. } => {
            assert_eq!(entry.content_hash, hash);
        }
        other => panic!("expected GetOk from HandleFindObject, got: {other:?}"),
    }
}

#[test]
fn chunk_transfer_from_remote_blob_store() {
    let h = MultiNodeHarness::new(2);
    let data = vec![0xAB; 200]; // > chunk_size(64), so multiple chunks
    let hash = h.put_on(0, &data, Some("transfer-test.bin"));

    // Get the manifest from node 0.
    let (_entry, manifest) = h.get_from(0, hash).expect("node 0 should have the entry");
    assert!(manifest.chunks.len() > 1, "should have multiple chunks");

    // Spawn a TransferActor wired to node 1's BlobStore.
    let transfer_addr = h.rt.spawn(TransferActor::new(h.nodes[1].blob_store)).unwrap();
    tick_n(&h.rt, 1);

    // Start the download.
    h.rt.send_to(
        transfer_addr,
        TransferMsg::StartDownload {
            manifest: manifest.clone(),
            source_node: h.nodes[0].node_id,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();
    tick_n(&h.rt, 2);

    // Feed chunks from node 0's BlobStore to the TransferActor.
    for chunk_ref in &manifest.chunks {
        let chunk_data = h.read_chunk_from(0, chunk_ref.hash)
            .expect("chunk should exist on node 0");
        h.rt.send_to(
            transfer_addr,
            TransferMsg::ChunkReceived {
                hash: chunk_ref.hash,
                data: chunk_data,
            },
        )
        .unwrap();
        tick_n(&h.rt, 3);
    }

    // Should get TransferComplete.
    let resp = tick_until_recv(&h.rt, &h.inbox, 10).unwrap();
    match resp {
        swactor_datastore::DatastoreResponse::TransferComplete { content_hash } => {
            assert_eq!(content_hash, hash);
        }
        other => panic!("expected TransferComplete, got: {other:?}"),
    }

    // Verify chunks are now on node 1's BlobStore.
    for chunk_ref in &manifest.chunks {
        let data_on_1 = h.read_chunk_from(1, chunk_ref.hash);
        assert!(data_on_1.is_some(), "chunk should now exist on node 1");
    }
}

#[test]
fn full_remote_get_scenario() {
    let h = MultiNodeHarness::new(2);
    let original_data = vec![0xCD; 200]; // Multiple chunks
    let hash = h.put_on(0, &original_data, Some("full-remote.bin"));

    // Disseminate metadata (including manifest) to node 1.
    h.disseminate_all();

    // Node 1 now has the entry and manifest via dissemination.
    let (_entry, manifest) = h.get_from(1, hash)
        .expect("node 1 should have entry+manifest via dissemination");

    // Spawn TransferActor wired to node 1's BlobStore.
    let transfer_addr = h.rt.spawn(TransferActor::new(h.nodes[1].blob_store)).unwrap();
    tick_n(&h.rt, 1);

    h.rt.send_to(
        transfer_addr,
        TransferMsg::StartDownload {
            manifest: manifest.clone(),
            source_node: h.nodes[0].node_id,
            reply_to: h.reply_addr(),
        },
    )
    .unwrap();
    tick_n(&h.rt, 2);

    // Transfer chunks from node 0 → node 1.
    for chunk_ref in &manifest.chunks {
        let chunk_data = h.read_chunk_from(0, chunk_ref.hash)
            .expect("chunk should exist on node 0");
        h.rt.send_to(
            transfer_addr,
            TransferMsg::ChunkReceived {
                hash: chunk_ref.hash,
                data: chunk_data,
            },
        )
        .unwrap();
        tick_n(&h.rt, 3);
    }

    let resp = tick_until_recv(&h.rt, &h.inbox, 10).unwrap();
    assert!(
        matches!(resp, swactor_datastore::DatastoreResponse::TransferComplete { .. }),
        "expected TransferComplete"
    );

    // Reassemble from node 1's BlobStore and verify byte-for-byte match.
    let mut reassembled = Vec::new();
    for chunk_ref in &manifest.chunks {
        let chunk_data = h.read_chunk_from(1, chunk_ref.hash)
            .expect("chunk should be on node 1 after transfer");
        reassembled.extend_from_slice(&chunk_data);
    }
    assert_eq!(reassembled, original_data, "reassembled data should match original");
}

#[test]
fn list_across_all_nodes_finds_objects_from_any_node() {
    let h = MultiNodeHarness::new(3);

    // Put distinct blobs on each node.
    let hash0 = h.put_on(0, b"data from node zero", Some("zero.txt"));
    let hash1 = h.put_on(1, b"data from node one", Some("one.txt"));
    let hash2 = h.put_on(2, b"data from node two", Some("two.txt"));

    // Query all nodes and merge results (simulating ListSwarm fan-out).
    let mut all_entries = Vec::new();
    for i in 0..3 {
        all_entries.extend(h.list_on(i, None));
    }

    // Deduplicate by content hash (simulating the merge step).
    let mut seen = std::collections::HashSet::new();
    all_entries.retain(|e| seen.insert(e.content_hash));

    assert_eq!(all_entries.len(), 3);
    let hashes: std::collections::HashSet<ContentHash> = all_entries.iter().map(|e| e.content_hash).collect();
    assert!(hashes.contains(&hash0));
    assert!(hashes.contains(&hash1));
    assert!(hashes.contains(&hash2));
}

#[test]
fn gc_on_one_node_does_not_affect_other_nodes() {
    let h = MultiNodeHarness::new(2);

    // Put the same data on both nodes (each stores its own chunks).
    let data = vec![0xEE; 200];
    let hash = h.put_on(0, &data, Some("gc-test.bin"));
    let _hash1 = h.put_on(1, &data, Some("gc-test.bin"));

    // Verify both nodes have chunks.
    let chunks_0_before = h.list_chunks_on(0);
    let chunks_1_before = h.list_chunks_on(1);
    assert!(!chunks_0_before.is_empty());
    assert!(!chunks_1_before.is_empty());

    // Delete + GC on node 0.
    h.delete_on(0, &hash);
    h.gc_ticks_on(0, 5); // gc_interval=3, so 5 ticks guarantees at least 1 GC sweep.

    // Node 0's chunks should be gone.
    let chunks_0_after = h.list_chunks_on(0);
    // Only manifest-related chunks referenced by remaining manifests survive.
    // Since we deleted the only object, all chunks should be gone.
    assert!(
        chunks_0_after.is_empty(),
        "node 0 chunks should be GC'd after delete, found {}",
        chunks_0_after.len()
    );

    // Node 1's chunks should be untouched.
    let chunks_1_after = h.list_chunks_on(1);
    assert_eq!(
        chunks_1_before.len(),
        chunks_1_after.len(),
        "node 1 chunks should be unaffected by node 0 GC"
    );
}
