//! Tests for MetadataActor — exercised as a black box through the swactor runtime.

mod common;

use std::collections::{BTreeMap, HashSet};

use swactor_datastore::messages::{DatastoreResponse, MetadataMsg};
use swactor_datastore::types::{ContentHash, ObjectEntry};

use swactor_transport::NodeId;

use common::{
    make_entry, make_manifest, spawn_metadata, test_node_id, test_runtime, tick_n, tick_until_recv,
};

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: Store & Retrieve
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn store_and_retrieve_object_with_manifest() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let data = b"hello metadata";
    let entry = make_entry(data, Some("greeting.txt"));
    let manifest = make_manifest(data);
    let content_hash = entry.content_hash;

    // Put.
    rt.send_to(meta, MetadataMsg::PutObject { entry, manifest: manifest.clone(), reply_to: reply })
        .unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::PutOk { content_hash: h } if h == content_hash));

    // Get.
    rt.send_to(meta, MetadataMsg::GetObject { content_hash, reply_to: reply })
        .unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::GetOk { entry: e, manifest: m } => {
            assert_eq!(e.content_hash, content_hash);
            assert_eq!(e.name, Some("greeting.txt".to_string()));
            assert_eq!(m, manifest);
        }
        other => panic!("expected GetOk, got: {other:?}"),
    }
}

#[test]
fn query_for_nonexistent_object_returns_not_found() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();

    rt.send_to(meta, MetadataMsg::GetObject {
        content_hash: ContentHash::of(b"does-not-exist"),
        reply_to: *inbox.addr(),
    })
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::NotFound));
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: Delete
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn delete_object_makes_it_unretrievable() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let data = b"ephemeral";
    let entry = make_entry(data, Some("temp.txt"));
    let manifest = make_manifest(data);
    let content_hash = entry.content_hash;

    // Put.
    rt.send_to(meta, MetadataMsg::PutObject { entry, manifest, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::PutOk { .. }));

    // Delete.
    rt.send_to(meta, MetadataMsg::DeleteObject { content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::DeleteOk { content_hash: h } if h == content_hash));

    // Get → NotFound.
    rt.send_to(meta, MetadataMsg::GetObject { content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::NotFound));
}

#[test]
fn delete_nonexistent_object_returns_not_found() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();

    rt.send_to(meta, MetadataMsg::DeleteObject {
        content_hash: ContentHash::of(b"ghost"),
        reply_to: *inbox.addr(),
    })
    .unwrap();

    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::NotFound));
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: List & Filter
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn list_local_returns_all_stored_objects() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let items: Vec<(&[u8], Option<&str>)> = vec![
        (b"aaa", Some("first.txt")),
        (b"bbb", Some("second.txt")),
        (b"ccc", None),
    ];

    let mut expected_hashes = HashSet::new();
    for (data, name) in &items {
        let entry = make_entry(*data, *name);
        let manifest = make_manifest(*data);
        expected_hashes.insert(entry.content_hash);
        rt.send_to(meta, MetadataMsg::PutObject { entry, manifest, reply_to: reply }).unwrap();
        tick_until_recv(&rt, &inbox, 10); // drain PutOk
    }

    // ListLocal with no filter.
    rt.send_to(meta, MetadataMsg::ListLocal { name_filter: None, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => {
            let got: HashSet<ContentHash> = entries.iter().map(|e| e.content_hash).collect();
            assert_eq!(got, expected_hashes);
        }
        other => panic!("expected ListOk, got: {other:?}"),
    }
}

#[test]
fn list_local_with_name_filter_excludes_non_matching_and_unnamed() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let items: Vec<(&[u8], Option<&str>)> = vec![
        (b"one", Some("alpha.txt")),
        (b"two", Some("alphabet.txt")),
        (b"three", None),
    ];

    for (data, name) in &items {
        let entry = make_entry(*data, *name);
        let manifest = make_manifest(*data);
        rt.send_to(meta, MetadataMsg::PutObject { entry, manifest, reply_to: reply }).unwrap();
        tick_until_recv(&rt, &inbox, 10);
    }

    // Filter "alpha" → both named entries match.
    rt.send_to(meta, MetadataMsg::ListLocal {
        name_filter: Some("alpha".to_string()),
        reply_to: reply,
    })
    .unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => assert_eq!(entries.len(), 2),
        other => panic!("expected ListOk, got: {other:?}"),
    }

    // Filter "zzz" → no matches.
    rt.send_to(meta, MetadataMsg::ListLocal {
        name_filter: Some("zzz".to_string()),
        reply_to: reply,
    })
    .unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => assert_eq!(entries.len(), 0),
        other => panic!("expected ListOk, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: DHT Protocol
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn dht_store_from_remote_makes_object_findable() {
    let rt = test_runtime();
    let node_id = test_node_id();
    let meta = spawn_metadata(&rt, node_id);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let remote_node = NodeId([0xAA; 32]);
    let data = b"remote-object";
    let entry = ObjectEntry {
        content_hash: ContentHash::of(data),
        name: Some("remote.dat".to_string()),
        node_id: remote_node,
        tags: BTreeMap::new(),
        size_bytes: data.len() as u64,
        created_at: 0,
    };
    let content_hash = entry.content_hash;

    // HandleStoreObject — fire-and-forget.
    rt.send_to(meta, MetadataMsg::HandleStoreObject { entry, manifest: None }).unwrap();
    tick_n(&rt, 3);

    // HandleFindObject → GetOk with synthetic empty manifest.
    rt.send_to(meta, MetadataMsg::HandleFindObject {
        from: remote_node,
        content_hash,
        reply_to: reply,
    })
    .unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::GetOk { entry: e, manifest: m } => {
            assert_eq!(e.content_hash, content_hash);
            assert!(m.chunks.is_empty());
            assert_eq!(m.chunk_size, 0);
        }
        other => panic!("expected GetOk, got: {other:?}"),
    }
}

#[test]
fn put_object_stamps_local_node_id_on_entry() {
    let rt = test_runtime();
    let local_node = test_node_id();
    let meta = spawn_metadata(&rt, local_node);
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let foreign_node = NodeId([0xFF; 32]);
    let data = b"stamped";
    let entry = ObjectEntry {
        content_hash: ContentHash::of(data),
        name: Some("stamped.txt".to_string()),
        node_id: foreign_node,
        tags: BTreeMap::new(),
        size_bytes: data.len() as u64,
        created_at: 0,
    };
    let manifest = make_manifest(data);
    let content_hash = entry.content_hash;

    // Put with foreign node_id.
    rt.send_to(meta, MetadataMsg::PutObject { entry, manifest, reply_to: reply }).unwrap();
    tick_until_recv(&rt, &inbox, 10); // drain PutOk

    // Get → entry should have local node_id stamped.
    rt.send_to(meta, MetadataMsg::GetObject { content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::GetOk { entry: e, .. } => {
            assert_eq!(e.node_id, local_node);
        }
        other => panic!("expected GetOk, got: {other:?}"),
    }
}

#[test]
fn dht_store_is_idempotent_for_existing_entries() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let data = b"duplicate";
    let entry = ObjectEntry {
        content_hash: ContentHash::of(data),
        name: Some("dup.dat".to_string()),
        node_id: NodeId([0xBB; 32]),
        tags: BTreeMap::new(),
        size_bytes: data.len() as u64,
        created_at: 0,
    };

    // Two HandleStoreObject with same content_hash.
    rt.send_to(meta, MetadataMsg::HandleStoreObject { entry: entry.clone(), manifest: None }).unwrap();
    tick_n(&rt, 3);
    rt.send_to(meta, MetadataMsg::HandleStoreObject { entry, manifest: None }).unwrap();
    tick_n(&rt, 3);

    // ListLocal → exactly 1 entry.
    rt.send_to(meta, MetadataMsg::ListLocal { name_filter: None, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => assert_eq!(entries.len(), 1),
        other => panic!("expected ListOk, got: {other:?}"),
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: Full Lifecycle
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn full_lifecycle_put_list_get_delete_verify_empty() {
    let rt = test_runtime();
    let meta = spawn_metadata(&rt, test_node_id());
    let inbox = rt.new_inbox::<DatastoreResponse>().unwrap();
    let reply = *inbox.addr();

    let data = b"lifecycle-test";
    let entry = make_entry(data, Some("lifecycle.bin"));
    let manifest = make_manifest(data);
    let content_hash = entry.content_hash;

    // 1. Put.
    rt.send_to(meta, MetadataMsg::PutObject { entry, manifest: manifest.clone(), reply_to: reply })
        .unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::PutOk { .. }));

    // 2. ListLocal → 1 entry.
    rt.send_to(meta, MetadataMsg::ListLocal { name_filter: None, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match &resp {
        DatastoreResponse::ListOk { entries } => assert_eq!(entries.len(), 1),
        other => panic!("expected ListOk, got: {other:?}"),
    }

    // 3. GetObject → verify entry + manifest.
    rt.send_to(meta, MetadataMsg::GetObject { content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::GetOk { entry: e, manifest: m } => {
            assert_eq!(e.content_hash, content_hash);
            assert_eq!(e.name, Some("lifecycle.bin".to_string()));
            assert_eq!(m, manifest);
        }
        other => panic!("expected GetOk, got: {other:?}"),
    }

    // 4. Delete → DeleteOk.
    rt.send_to(meta, MetadataMsg::DeleteObject { content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::DeleteOk { content_hash: h } if h == content_hash));

    // 5. ListLocal → 0 entries.
    rt.send_to(meta, MetadataMsg::ListLocal { name_filter: None, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    match resp {
        DatastoreResponse::ListOk { entries } => assert_eq!(entries.len(), 0),
        other => panic!("expected ListOk, got: {other:?}"),
    }

    // 6. GetObject → NotFound.
    rt.send_to(meta, MetadataMsg::GetObject { content_hash, reply_to: reply }).unwrap();
    let resp = tick_until_recv(&rt, &inbox, 10).unwrap();
    assert!(matches!(resp, DatastoreResponse::NotFound));
}
