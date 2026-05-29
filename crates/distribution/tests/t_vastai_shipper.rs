//! End-to-end test for the vastai monitoring shipper against a real collector.
//!
//! Scenario: a producer (the orchestrator's external poller, here) ships a few
//! vastai records to a running collector. We assert they actually land — on disk
//! under the synthetic node dir, and in the collector's per-run accounting as
//! `vastai_records` — proving the independent vastai layer rides the existing
//! collector transport without any swactor wiring.

#![cfg(feature = "collector")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use distribution::diagnostics::collector::{CollectorState, bind, serve};
use distribution::diagnostics::vastai::record::{
    HostSample, InstanceObservation, LifecycleEvent, LogBatch, LogLine, LogStream, Source,
    VastaiNodeRef,
};
use distribution::diagnostics::vastai::{
    LogForwarder, LogForwarderConfig, VastaiShipper, VastaiShipperConfig,
};

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "vastai-ship-{}-{}",
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
async fn shipper_delivers_each_body_kind_and_collector_buckets_them_as_vastai() {
    let store = TempDir::new();
    let spool = TempDir::new();
    let state = Arc::new(
        CollectorState::new(store.path()).with_finalize_wait(Duration::from_millis(0)),
    );
    let listener = bind("127.0.0.1:0".parse().unwrap()).await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let serve_state = Arc::clone(&state);
    let _server = tokio::spawn(async move {
        let _ = serve(listener, serve_state).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let run_id = "vastai-run-1";
    let config = VastaiShipperConfig::new(
        format!("http://{addr}"),
        run_id,
        "vastai-external",
        spool.path(),
    )
    .with_request_timeout(Duration::from_secs(2));
    let node = VastaiNodeRef {
        contract_id: Some(99),
        stage_index: Some(0),
        label: Some("pp-run-0".into()),
    };
    let shipper = VastaiShipper::spawn(config, node, Source::External).expect("spawn shipper");

    shipper.instance(InstanceObservation {
        id: 99,
        gpu_name: Some("RTX 4090".into()),
        actual_status: Some("loading".into()),
        raw: serde_json::json!({"future_field": 1}),
        ..Default::default()
    });
    shipper.sample(HostSample::default());
    shipper.logs(LogBatch {
        stream: LogStream::Stderr,
        lines: vec![LogLine {
            wall_ms: 1,
            line: 0,
            text: "worker started".into(),
        }],
    });
    shipper.lifecycle(LifecycleEvent::Teardown {
        contract_id: 99,
        reason: Some("done".into()),
    });

    // Wait for all four to be delivered (or fail the test).
    let handle = shipper.handle();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while handle.delivered_count() < 4 {
        if std::time::Instant::now() > deadline {
            panic!(
                "only {} of 4 records delivered before timeout",
                handle.delivered_count()
            );
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    handle.shutdown().await;

    // The collector accounts these under the synthetic node, all as vastai.
    let stats = state.run_stats(run_id).expect("run recorded");
    let node_stats = stats
        .nodes
        .get("vastai-external")
        .expect("external node dir recorded");
    assert_eq!(
        node_stats.vastai_records, 4,
        "all four bodies counted as vastai records"
    );
    // And they never masquerade as swactor records.
    assert_eq!(node_stats.event_batches, 0);
    assert_eq!(node_stats.snapshots, 0);
    assert!(!node_stats.boot_recorded);

    // On disk, each kind landed under the run's node dir with its own prefix.
    let node_dir = store.path().join(run_id).join("vastai-external");
    let names: Vec<String> = std::fs::read_dir(&node_dir)
        .expect("node dir exists")
        .filter_map(|e| e.ok())
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();
    for prefix in [
        "vastai_instance-",
        "vastai_sample-",
        "vastai_logs-",
        "vastai_lifecycle-",
    ] {
        assert!(
            names.iter().any(|n| n.starts_with(prefix)),
            "expected a file starting with {prefix}, got {names:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn log_forwarder_streams_lines_in_order_with_truncation() {
    let store = TempDir::new();
    let spool = TempDir::new();
    let state =
        Arc::new(CollectorState::new(store.path()).with_finalize_wait(Duration::from_millis(0)));
    let listener = bind("127.0.0.1:0".parse().unwrap()).await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let serve_state = Arc::clone(&state);
    let _server = tokio::spawn(async move {
        let _ = serve(listener, serve_state).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let run_id = "vastai-logs-run";
    let shipper = VastaiShipper::spawn(
        VastaiShipperConfig::new(format!("http://{addr}"), run_id, "vastai-stage-0", spool.path()),
        VastaiNodeRef {
            stage_index: Some(0),
            ..Default::default()
        },
        Source::InVm,
    )
    .expect("spawn shipper");

    // Small line cap so we can assert truncation behaviour.
    let cfg = LogForwarderConfig {
        max_line_bytes: 5,
        ..LogForwarderConfig::default()
    };
    let forwarder = LogForwarder::new(shipper.clone(), cfg);
    forwarder.push(LogStream::Stderr, "first line\n");
    forwarder.push(LogStream::Stderr, "second");
    forwarder.flush();

    let handle = shipper.handle();
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while handle.delivered_count() < 1 {
        if std::time::Instant::now() > deadline {
            panic!("no log batch delivered before timeout");
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    handle.shutdown().await;

    // The stderr batch landed with both lines, in order, each truncated to the
    // 5-byte cap and newline-stripped.
    let node_dir = store.path().join(run_id).join("vastai-stage-0");
    let mut checked = false;
    for entry in std::fs::read_dir(&node_dir).expect("node dir") {
        let entry = entry.unwrap();
        if entry.file_name().to_string_lossy().starts_with("vastai_logs-") {
            let body: serde_json::Value =
                serde_json::from_slice(&std::fs::read(entry.path()).unwrap()).unwrap();
            let lines = &body["body"]["lines"];
            assert_eq!(lines[0]["line"], 0);
            assert_eq!(lines[0]["text"], "first");
            assert_eq!(lines[1]["line"], 1);
            assert_eq!(lines[1]["text"], "secon");
            checked = true;
        }
    }
    assert!(checked, "expected a vastai_logs record on disk");
}
