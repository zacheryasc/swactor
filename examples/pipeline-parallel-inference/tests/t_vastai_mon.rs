//! Integration test for the external vastai poller.
//!
//! Scenario: a contract is leased and the poller observes it through the
//! image-pull window (vast still reports `loading`). We assert the poller
//! actually fetches from the vast API and ships full instance observations into
//! a real collector under the synthetic `vastai-external` node — proving the
//! external view lands in the same per-run timeline the in-VM view will use,
//! without any swactor wiring.

use std::sync::Arc;
use std::time::Duration;

use distribution::diagnostics::collector::{CollectorState, bind, serve};
use pipeline_parallel_inference::vastai_mon::{VastaiPoller, VastaiPollerConfig};
use wiremock::matchers::{method, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

struct TempDir(std::path::PathBuf);
impl TempDir {
    fn new() -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "vastai-mon-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        TempDir(p)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn poller_observes_a_loading_contract_and_ships_to_collector() {
    // 1. A real collector to receive observations.
    let store = TempDir::new();
    let spool = TempDir::new();
    let state =
        Arc::new(CollectorState::new(store.path()).with_finalize_wait(Duration::from_millis(0)));
    let listener = bind("127.0.0.1:0".parse().unwrap()).await.expect("bind");
    let collector_addr = listener.local_addr().unwrap();
    let serve_state = Arc::clone(&state);
    let _server = tokio::spawn(async move {
        let _ = serve(listener, serve_state).await;
    });

    // 2. A mock vast.ai API: the instance is still pulling its image, so the
    //    live util fields are absent — exactly the window vast-only callers go
    //    blind on, and the reason the poller polls before `running`.
    let vast = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/v0/instances/9000/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "instances": {
                "id": 9000,
                "gpu_name": "RTX 4090",
                "actual_status": "loading",
                "intended_status": "running",
                "status_msg": "Pulling from registry",
                "disk_usage": 2.0,
                "dph_total": 0.4
            }
        })))
        .mount(&vast)
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    // 3. The poller, polling fast so the test is quick.
    let config = VastaiPollerConfig {
        collector_url: format!("http://{collector_addr}"),
        run_id: "vastai-mon-run".to_string(),
        api_key: "test-key".to_string(),
        base_url: vast.uri(),
        poll_interval: Duration::from_millis(100),
        spool_dir: spool.path().to_path_buf(),
    };
    let poller = VastaiPoller::spawn(config, Some(1)).expect("spawn poller");
    poller
        .tracker()
        .track(9000, Some(0), Some("pp-run-0".to_string()));

    // 4. Wait for a couple of polled observations to land at the collector.
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let n = state
            .run_stats("vastai-mon-run")
            .and_then(|s| s.nodes.get("vastai-external").map(|n| n.vastai_records))
            .unwrap_or(0);
        // >=2: at least the DeployStart/ContractLeased lifecycle plus one polled
        // instance observation.
        if n >= 2 {
            break;
        }
        if std::time::Instant::now() > deadline {
            panic!("poller shipped only {n} records before timeout");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    poller.shutdown().await;

    // 5. An instance observation landed on disk with the loading status — the
    //    image-pull window is captured.
    let node_dir = store.path().join("vastai-mon-run").join("vastai-external");
    let mut found_loading = false;
    for entry in std::fs::read_dir(&node_dir).expect("node dir") {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.starts_with("vastai_instance-") {
            let body: serde_json::Value =
                serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
            if body["body"]["actual_status"] == "loading" {
                found_loading = true;
                // The raw vast JSON is preserved verbatim for later mining.
                assert_eq!(body["body"]["raw"]["status_msg"], "Pulling from registry");
            }
        }
    }
    assert!(
        found_loading,
        "expected a vastai_instance record capturing the loading status"
    );
}
