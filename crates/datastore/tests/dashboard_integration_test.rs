//! End-to-end integration test: spins up a datastore node with an HTTP API
//! **and** a runtime dashboard, performs CRUD over HTTP, then verifies:
//!
//! - `DatastoreMetrics::snapshot()` reflects the operations
//! - Dashboard `/datastore` serves HTML
//! - Dashboard `/api/datastore` returns a JSON snapshot matching the metrics

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

/// Scenario: datastore CRUD → dashboard reflects live operation metrics.
///
/// Story:
///   Two files are uploaded. One is fetched (metadata + data). The other
///   is deleted. Afterwards we check that the metrics snapshot, the
///   dashboard HTML page, and the dashboard JSON API all agree on what
///   happened.
#[test]
fn dashboard_reflects_datastore_operations() {
    let api_port = find_free_port();
    let dash_port = find_free_port();
    let api_base = format!("http://127.0.0.1:{api_port}");
    let dash_base = format!("http://127.0.0.1:{dash_port}");

    // ── Infrastructure: runtime + actors + dashboard + API ──────────────

    let dash = dashboard::start_dashboard(dashboard::DashboardConfig {
        port: dash_port,
        ..Default::default()
    });

    let collector = dashboard::collector::StatsCollector::new(2);
    let mut rt = Runtime::new(RuntimeConfig {
        num_threads: 2,
        max_actors: 1024,
        channel_buffer_size: 2000,
        ..Default::default()
    });
    rt.set_stats_hook(collector.clone());

    let node_id = NodeId([0xBB; 32]);
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
    let node_hex: String = node_id.0.iter().map(|b| format!("{b:02x}")).collect();
    metrics.set_node_id(node_hex);

    dash.set_runtime(handle.runtime.clone(), collector);
    dash.set_datastore(
        Arc::clone(&metrics)
            as Arc<dyn dashboard::datastore_collector::DatastoreStatsProvider>,
    );

    let (api_shutdown, _peers) = start_api_server(
        handle.runtime.clone(),
        datastore_addr,
        metadata_addr,
        blob_store_addr,
        None,
        api_port,
        Arc::clone(&metrics),
    );

    thread::sleep(Duration::from_millis(300));

    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        run_dashboard_scenario(&api_base, &dash_base, &metrics);
    }));

    // Cleanup
    api_shutdown.store(true, std::sync::atomic::Ordering::Relaxed);
    dash.shutdown();
    handle.shutdown();
    handle.join();

    if let Err(e) = result {
        std::panic::resume_unwind(e);
    }
}

fn run_dashboard_scenario(api: &str, dash: &str, metrics: &Arc<DatastoreMetrics>) {
    // ── 1. Upload two objects ───────────────────────────────────────────

    let put_a: serde_json::Value = ureq::post(&format!("{api}/api/put?name=alpha"))
        .send_bytes(b"payload-alpha")
        .expect("put A failed")
        .into_json()
        .unwrap();
    let hash_a = put_a["content_hash"].as_str().unwrap().to_string();

    let _put_b: serde_json::Value = ureq::post(&format!("{api}/api/put?name=bravo"))
        .send_bytes(b"payload-bravo")
        .expect("put B failed")
        .into_json()
        .unwrap();
    let hash_b = _put_b["content_hash"].as_str().unwrap().to_string();

    // ── 2. GET object A (metadata + raw data → 2 get ops) ──────────────

    let _: serde_json::Value = ureq::get(&format!("{api}/api/get?hash={hash_a}"))
        .call()
        .unwrap()
        .into_json()
        .unwrap();

    let data_resp = ureq::get(&format!("{api}/api/data?hash={hash_a}"))
        .call()
        .unwrap();
    let mut body = Vec::new();
    data_resp.into_reader().read_to_end(&mut body).unwrap();
    assert_eq!(body, b"payload-alpha", "downloaded data should match");

    // ── 3. DELETE object B ──────────────────────────────────────────────

    let _: serde_json::Value = ureq::post(&format!("{api}/api/delete?hash={hash_b}"))
        .call()
        .unwrap()
        .into_json()
        .unwrap();

    // ── 4. Assert: in-process metrics snapshot ──────────────────────────

    let snap = metrics.snapshot();

    assert_eq!(snap.put_ops, 2, "two puts recorded");
    assert_eq!(snap.get_ops, 2, "metadata-get + data-get recorded");
    assert_eq!(snap.delete_ops, 1, "one delete recorded");
    assert_eq!(snap.objects.len(), 1, "only alpha remains after deleting bravo");
    assert_eq!(snap.objects[0].hash, hash_a);
    assert!(
        snap.recent_events.len() >= 5,
        "at least 5 events (2 put + 2 get + 1 delete), got {}",
        snap.recent_events.len()
    );

    // ── 5. Assert: dashboard /datastore serves HTML ─────────────────────

    let page = ureq::get(&format!("{dash}/datastore")).call().unwrap();
    assert_eq!(page.status(), 200);
    assert!(
        page.header("Content-Type")
            .unwrap_or("")
            .contains("text/html"),
    );
    let html = page.into_string().unwrap();
    assert!(html.contains("Datastore"), "page should mention Datastore");

    // ── 6. Assert: /api/datastore JSON matches metrics ──────────────────

    let ds: serde_json::Value = ureq::get(&format!("{dash}/api/datastore"))
        .call()
        .unwrap()
        .into_json()
        .unwrap();

    assert_eq!(ds["put_ops"].as_u64().unwrap(), 2);
    assert_eq!(ds["get_ops"].as_u64().unwrap(), 2);
    assert_eq!(ds["delete_ops"].as_u64().unwrap(), 1);

    let objects = ds["objects"].as_array().expect("objects should be array");
    assert_eq!(objects.len(), 1);
    assert_eq!(objects[0]["hash"].as_str().unwrap(), hash_a);

    let events = ds["recent_events"]
        .as_array()
        .expect("recent_events should be array");
    assert!(events.len() >= 5);
}
