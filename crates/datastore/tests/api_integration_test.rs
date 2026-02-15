#![cfg(feature = "node")]

//! Integration test: spins up a real datastore node with HTTP API and exercises
//! the full CRUD lifecycle over HTTP.

use std::sync::Arc;
use std::thread;
use std::time::Duration;

use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;

use swactor_datastore::actors::{BlobStoreActor, DatastoreNode, MetadataActor};
use swactor_datastore::api::start_api_server;
use swactor_datastore::metrics::DatastoreMetrics;
use swactor_datastore::storage::InMemoryBackend;
use swactor_datastore::DatastoreConfig;

use distribution::types::NodeId;

fn find_free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// Full CRUD lifecycle over HTTP:
/// status → put → list → get metadata → get data → delete → list (empty) → get (404)
#[test]
fn http_crud_lifecycle() {
    let port = find_free_port();
    let base = format!("http://127.0.0.1:{port}");

    // Set up runtime with worker threads (needed for HTTP server)
    let collector = runtime_dashboard::collector::StatsCollector::new(2);
    let mut rt = Runtime::new(RuntimeConfig {
        num_threads: 2,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    });
    rt.set_stats_hook(collector);

    let node_id = NodeId([0xAA; 32]);

    let config = DatastoreConfig {
        chunk_size: 1_048_576,
        gc_interval: 1000,
        ..Default::default()
    };

    let backend: Box<dyn swactor_datastore::StorageBackend> = Box::new(InMemoryBackend::new());
    let blob_store_addr = rt.spawn(BlobStoreActor::new(backend)).unwrap();

    let mut metadata = MetadataActor::new(node_id, &config);
    metadata.set_blob_store(blob_store_addr);
    let metadata_addr = rt.spawn(metadata).unwrap();

    let datastore_node = DatastoreNode::new(node_id, blob_store_addr, metadata_addr, config);
    let datastore_addr = rt.spawn(datastore_node).unwrap();

    let handle = rt.run().expect("failed to start runtime");

    let metrics = Arc::new(DatastoreMetrics::new());
    let (api_shutdown, _peers) = start_api_server(
        handle.runtime.clone(),
        datastore_addr,
        metadata_addr,
        blob_store_addr,
        port,
        Arc::clone(&metrics),
    );

    // Give the HTTP server time to bind
    thread::sleep(Duration::from_millis(200));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_crud_scenario(&base, &metrics);
    }));

    // Cleanup
    api_shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    handle.shutdown();
    handle.join();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

fn run_crud_scenario(base: &str, metrics: &Arc<DatastoreMetrics>) {
    // 1. Status — should return a node_id
    let status: serde_json::Value = ureq::get(&format!("{base}/api/status"))
        .call()
        .expect("status request failed")
        .into_json()
        .unwrap();
    let node_id = status["node_id"].as_str().expect("node_id should be a string");
    assert_eq!(node_id.len(), 64, "node_id should be 64 hex chars");

    // 2. Put — upload some content
    let content = b"hello from the integration test!";
    let put_resp: serde_json::Value = ureq::post(&format!("{base}/api/put?name=greeting"))
        .send_bytes(content)
        .expect("put request failed")
        .into_json()
        .unwrap();
    let hash = put_resp["content_hash"]
        .as_str()
        .expect("put should return content_hash");
    assert_eq!(hash.len(), 64, "content_hash should be 64 hex chars");

    // 3. List — should contain exactly one entry matching our upload
    let list_resp: serde_json::Value = ureq::get(&format!("{base}/api/list"))
        .call()
        .expect("list request failed")
        .into_json()
        .unwrap();
    let entries = list_resp["entries"].as_array().expect("entries should be an array");
    assert_eq!(entries.len(), 1, "should have exactly 1 entry after put");
    assert_eq!(entries[0]["content_hash"].as_str().unwrap(), hash);
    assert_eq!(entries[0]["name"].as_str().unwrap(), "greeting");

    // 4. Get metadata — entry + manifest for the uploaded object
    let get_resp: serde_json::Value = ureq::get(&format!("{base}/api/get?hash={hash}"))
        .call()
        .expect("get request failed")
        .into_json()
        .unwrap();
    let entry = &get_resp["entry"];
    assert_eq!(entry["content_hash"].as_str().unwrap(), hash);
    assert_eq!(entry["name"].as_str().unwrap(), "greeting");
    assert_eq!(entry["size_bytes"].as_u64().unwrap(), content.len() as u64);
    let manifest = &get_resp["manifest"];
    let chunks = manifest["chunks"].as_array().expect("manifest should have chunks");
    assert!(!chunks.is_empty(), "manifest should have at least one chunk");

    // 5. Get data — download the raw bytes and verify content matches
    let data_resp = ureq::get(&format!("{base}/api/data?hash={hash}"))
        .call()
        .expect("data request failed");
    let mut downloaded = Vec::new();
    data_resp
        .into_reader()
        .read_to_end(&mut downloaded)
        .unwrap();
    assert_eq!(downloaded, content, "downloaded bytes should match uploaded content");

    // 6. Delete — remove the object
    let del_resp: serde_json::Value = ureq::post(&format!("{base}/api/delete?hash={hash}"))
        .call()
        .expect("delete request failed")
        .into_json()
        .unwrap();
    assert_eq!(del_resp["content_hash"].as_str().unwrap(), hash);

    // 7. List after delete — should be empty
    let list_resp2: serde_json::Value = ureq::get(&format!("{base}/api/list"))
        .call()
        .expect("list request failed")
        .into_json()
        .unwrap();
    let entries2 = list_resp2["entries"]
        .as_array()
        .expect("entries should be an array");
    assert!(entries2.is_empty(), "list should be empty after delete");

    // 8. Get after delete — should 404
    let get_err = ureq::get(&format!("{base}/api/get?hash={hash}")).call();
    match get_err {
        Err(ureq::Error::Status(404, _)) => {} // expected
        Err(e) => panic!("expected 404, got error: {e}"),
        Ok(_) => panic!("expected 404, got 200"),
    }

    // 9. Verify metrics snapshot reflects the full lifecycle
    let snap = metrics.snapshot();
    assert_eq!(snap.put_ops, 1, "one put recorded");
    // handle_get + handle_data = 2 get operations
    assert_eq!(snap.get_ops, 2, "metadata-get + data-get recorded");
    assert_eq!(snap.delete_ops, 1, "one delete recorded");
    assert!(snap.objects.is_empty(), "no objects after delete");
    assert!(
        snap.recent_events.len() >= 4,
        "at least 4 events (put + get + get + delete), got {}",
        snap.recent_events.len()
    );
}
