//! Garbage collection scenario tests.
//!
//! Verifies the full GC flow: MetadataActor builds a referenced chunk set
//! from its manifests and sends GcUnreferenced to BlobStoreActor, which
//! deletes orphaned chunks.

mod common;

use common::GcHarness;

use swactor_datastore::chunking::chunk_blob;

#[test]
fn gc_cleans_up_chunks_after_object_deleted() {
    let h = GcHarness::new();

    // Store a 200-byte blob (produces 4 chunks at chunk_size=64).
    let data = vec![0xAB; 200];
    let hash = h.put_blob(&data, Some("doomed.bin"));

    let chunks_before = h.list_chunks();
    assert!(!chunks_before.is_empty(), "chunks should exist after put");

    // Delete the object from the metadata index.
    h.delete_blob(&hash);

    // Run enough ticks for GC to fire (gc_interval=3).
    h.gc_ticks(3);

    // All chunks should be gone — nothing references them anymore.
    let chunks_after = h.list_chunks();
    assert!(
        chunks_after.is_empty(),
        "expected all chunks removed after GC, found {}",
        chunks_after.len()
    );
}

#[test]
fn gc_preserves_chunks_still_referenced() {
    let h = GcHarness::new();

    // Store two distinct blobs.
    let data_a = vec![0x11; 200];
    let data_b = vec![0x22; 150];
    let hash_a = h.put_blob(&data_a, Some("keep.bin"));
    let _hash_b = h.put_blob(&data_b, Some("also-keep.bin"));

    let chunks_before = h.list_chunks();

    // Delete only blob A.
    h.delete_blob(&hash_a);

    // GC should clean up A's orphaned chunks but preserve B's.
    h.gc_ticks(3);

    // Blob B's chunks should all survive.
    let (_, manifest_b, _) = chunk_blob(&data_b, h.chunk_size);
    for chunk_ref in &manifest_b.chunks {
        assert!(
            h.has_chunk(&chunk_ref.hash),
            "blob B chunk {:?} should survive GC",
            chunk_ref.hash
        );
    }

    // Blob A's chunks should be gone (they don't overlap with B since data differs).
    let (_, manifest_a, _) = chunk_blob(&data_a, h.chunk_size);
    for chunk_ref in &manifest_a.chunks {
        assert!(
            !h.has_chunk(&chunk_ref.hash),
            "blob A chunk {:?} should be removed by GC",
            chunk_ref.hash
        );
    }

    // Total chunk count should have decreased.
    let chunks_after = h.list_chunks();
    assert!(
        chunks_after.len() < chunks_before.len(),
        "chunk count should decrease after GC removes orphans"
    );
}

#[test]
fn gc_handles_deduplication_correctly() {
    let h = GcHarness::new();

    // Two 128-byte blobs sharing the same 64-byte prefix (first chunk is identical).
    let mut data_x = vec![0xCC; 128];
    let mut data_y = vec![0xCC; 128];
    // The first 64 bytes are identical → same first chunk hash.
    // Differ in the second 64 bytes → different second chunk + different content hash.
    data_x[64..].fill(0xAA);
    data_y[64..].fill(0xBB);

    let hash_x = h.put_blob(&data_x, Some("x.bin"));
    let _hash_y = h.put_blob(&data_y, Some("y.bin"));

    // Verify the shared chunk exists.
    let (_, manifest_x, _) = chunk_blob(&data_x, h.chunk_size);
    let (_, manifest_y, _) = chunk_blob(&data_y, h.chunk_size);
    let shared_chunk = manifest_x.chunks[0].hash;
    assert_eq!(
        shared_chunk, manifest_y.chunks[0].hash,
        "first chunk should be identical (shared prefix)"
    );

    // Delete only X.
    h.delete_blob(&hash_x);
    h.gc_ticks(3);

    // Shared chunk should survive (Y still references it).
    assert!(
        h.has_chunk(&shared_chunk),
        "shared chunk should survive — still referenced by Y"
    );

    // X's unique second chunk should be gone.
    let x_unique = manifest_x.chunks[1].hash;
    assert!(
        !h.has_chunk(&x_unique),
        "X's unique chunk should be removed by GC"
    );

    // Y's unique second chunk should survive.
    let y_unique = manifest_y.chunks[1].hash;
    assert!(
        h.has_chunk(&y_unique),
        "Y's unique chunk should survive GC"
    );
}

#[test]
fn gc_is_no_op_when_nothing_deleted() {
    let h = GcHarness::new();

    let data = vec![0xFF; 200];
    h.put_blob(&data, Some("survivor.bin"));

    let chunks_before = h.list_chunks();

    // GC fires but nothing was deleted — all chunks should survive.
    h.gc_ticks(3);

    let chunks_after = h.list_chunks();
    assert_eq!(
        chunks_before.len(),
        chunks_after.len(),
        "GC without any deletes should preserve all chunks"
    );
}

#[test]
fn gc_runs_on_interval_not_every_tick() {
    let h = GcHarness::new();

    let data = vec![0xDD; 200];
    let hash = h.put_blob(&data, Some("interval-test.bin"));

    h.delete_blob(&hash);

    // Send gc_interval - 1 = 2 ticks. GC should NOT have fired yet.
    h.gc_ticks(2);

    let chunks_mid = h.list_chunks();
    assert!(
        !chunks_mid.is_empty(),
        "chunks should still exist before gc_interval is reached"
    );

    // One more tick reaches gc_interval=3. GC fires and cleans up.
    h.gc_ticks(1);

    let chunks_after = h.list_chunks();
    assert!(
        chunks_after.is_empty(),
        "chunks should be cleaned after gc_interval reached"
    );
}

#[test]
fn gc_with_empty_datastore_is_harmless() {
    let h = GcHarness::new();

    // GC on an empty store — should not panic.
    h.gc_ticks(3);

    let chunks = h.list_chunks();
    assert!(chunks.is_empty(), "empty store should remain empty after GC");
}
