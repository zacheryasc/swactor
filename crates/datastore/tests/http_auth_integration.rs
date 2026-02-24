//! Integration test: HTTP API endpoints gated behind auth.
//!
//! Spins up a full actor runtime with GatewayActor, starts the HTTP API server,
//! and uses ureq to prove that authorized requests succeed while unauthorized
//! ones get 403 and missing-auth requests get 401.
#![cfg(feature = "node")]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::Ordering;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use swactor_datastore::crypto::Keypair;
use swactor_datastore::content_hash::ContentHash;
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;
use swactor_datastore::actors::{BlobStoreActor, DatastoreNode, GatewayActor, MetadataActor};
use swactor_datastore::api::start_api_server;
use swactor_datastore::auth::{
    sign_request, AccessControlList, AuthzEngine, DatastoreAction, SignedRequestPayload,
};
use swactor_datastore::storage::InMemoryBackend;
use swactor_datastore::types::DatastoreConfig;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn random_nonce() -> [u8; 16] {
    let t = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let mut nonce = [0u8; 16];
    nonce.copy_from_slice(&t.to_le_bytes());
    nonce
}

fn sign_header(keypair: &Keypair, action: DatastoreAction) -> String {
    let payload = SignedRequestPayload {
        action,
        timestamp: now_secs(),
        nonce: random_nonce(),
    };
    let request = sign_request(keypair, payload);
    serde_json::to_string(&request).unwrap()
}

/// Find an available TCP port by binding to :0.
fn available_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

// ═══════════════════════════════════════════════════════════════════════════
// Scenario: Owner operates over HTTP; stranger is denied
// ═══════════════════════════════════════════════════════════════════════════

#[test]
fn http_auth_owner_allowed_stranger_denied() {
    let owner_kp = Keypair::generate();
    let stranger_kp = Keypair::generate();
    let owner_id = owner_kp.node_id();

    // ── Build runtime & actors ───────────────────────────────────────────
    let rt = Runtime::new(RuntimeConfig {
        num_threads: 2,
        max_actors: 256,
        channel_buffer_size: 1024,
        ..Default::default()
    });

    let blob_store_addr = rt
        .spawn(BlobStoreActor::new(Box::new(InMemoryBackend::new())))
        .unwrap();

    let mut metadata = MetadataActor::new(owner_id, &DatastoreConfig::default());
    metadata.set_blob_store(blob_store_addr);
    let metadata_addr = rt.spawn(metadata).unwrap();

    let config = DatastoreConfig {
        chunk_size: 1_048_576,
        ..Default::default()
    };
    let datastore_node = DatastoreNode::new(owner_id, blob_store_addr, metadata_addr, config);
    let datastore_addr = rt.spawn(datastore_node).unwrap();

    let acl = AccessControlList {
        owner: owner_id,
        authorized_keys: HashSet::new(),
        key_labels: HashMap::new(),
    };
    let engine = AuthzEngine::new(acl);
    let gateway_addr = rt
        .spawn(GatewayActor::new(engine, datastore_addr, None))
        .unwrap();

    let handle = rt.run().expect("failed to start runtime");

    // ── Start HTTP server ────────────────────────────────────────────────
    let port = available_port();
    let metrics = std::sync::Arc::new(swactor_datastore::metrics::DatastoreMetrics::new());
    let (shutdown, _peers) = start_api_server(
        handle.runtime.clone(),
        datastore_addr,
        metadata_addr,
        blob_store_addr,
        Some(gateway_addr),
        port,
        metrics,
    );

    // Give the HTTP server threads a moment to start accepting connections.
    std::thread::sleep(Duration::from_millis(100));

    let base = format!("http://127.0.0.1:{port}");

    // ── 1. Owner PUTs data ───────────────────────────────────────────────
    let test_data = b"hello from the integration test";
    let expected_hash = ContentHash::of(test_data);

    let put_header = sign_header(
        &owner_kp,
        DatastoreAction::Put {
            name: Some("test.txt".to_string()),
            content_hash: expected_hash,
            size_bytes: test_data.len() as u64,
            tags: BTreeMap::new(),
        },
    );

    let put_resp = ureq::post(&format!("{base}/api/put?name=test.txt"))
        .set("X-Signed-Request", &put_header)
        .send_bytes(test_data)
        .expect("PUT request failed");

    assert_eq!(put_resp.status(), 200);
    let put_body: serde_json::Value = put_resp.into_json().unwrap();
    let returned_hash = put_body["content_hash"].as_str().unwrap();
    assert_eq!(returned_hash, expected_hash.to_hex());

    // ── 2. Owner GETs it back ────────────────────────────────────────────
    let get_header = sign_header(
        &owner_kp,
        DatastoreAction::Get {
            content_hash: expected_hash,
        },
    );

    let get_resp = ureq::get(&format!("{base}/api/get?hash={}", expected_hash.to_hex()))
        .set("X-Signed-Request", &get_header)
        .call()
        .expect("GET request failed");

    assert_eq!(get_resp.status(), 200);
    let get_body: serde_json::Value = get_resp.into_json().unwrap();
    assert_eq!(
        get_body["entry"]["content_hash"].as_str().unwrap(),
        expected_hash.to_hex()
    );

    // ── 3. Owner LISTs ──────────────────────────────────────────────────
    let list_header = sign_header(
        &owner_kp,
        DatastoreAction::List { name_filter: None },
    );

    let list_resp = ureq::get(&format!("{base}/api/list"))
        .set("X-Signed-Request", &list_header)
        .call()
        .expect("LIST request failed");

    assert_eq!(list_resp.status(), 200);
    let list_body: serde_json::Value = list_resp.into_json().unwrap();
    let entries = list_body["entries"].as_array().unwrap();
    assert!(
        entries
            .iter()
            .any(|e| e["content_hash"].as_str() == Some(&expected_hash.to_hex())),
        "expected hash in list results"
    );

    // ── 4. Stranger tries GET → 403 ─────────────────────────────────────
    let stranger_header = sign_header(
        &stranger_kp,
        DatastoreAction::Get {
            content_hash: expected_hash,
        },
    );

    let stranger_resp = ureq::get(&format!(
        "{base}/api/get?hash={}",
        expected_hash.to_hex()
    ))
    .set("X-Signed-Request", &stranger_header)
    .call();

    match stranger_resp {
        Err(ureq::Error::Status(403, _)) => {} // expected
        Err(e) => panic!("expected 403, got error: {e}"),
        Ok(r) => panic!("expected 403, got {}", r.status()),
    }

    // ── 5. No auth header → 401 ─────────────────────────────────────────
    let no_auth_resp = ureq::get(&format!(
        "{base}/api/get?hash={}",
        expected_hash.to_hex()
    ))
    .call();

    match no_auth_resp {
        Err(ureq::Error::Status(401, _)) => {} // expected
        Err(e) => panic!("expected 401, got error: {e}"),
        Ok(r) => panic!("expected 401, got {}", r.status()),
    }

    // ── 6. Owner DELETEs ─────────────────────────────────────────────────
    let delete_header = sign_header(
        &owner_kp,
        DatastoreAction::Delete {
            content_hash: expected_hash,
        },
    );

    let delete_resp = ureq::post(&format!(
        "{base}/api/delete?hash={}",
        expected_hash.to_hex()
    ))
    .set("X-Signed-Request", &delete_header)
    .send_bytes(&[])
    .expect("DELETE request failed");

    assert_eq!(delete_resp.status(), 200);

    // ── Teardown ─────────────────────────────────────────────────────────
    shutdown.store(true, Ordering::Relaxed);
    handle.shutdown();
    handle.join();
}
