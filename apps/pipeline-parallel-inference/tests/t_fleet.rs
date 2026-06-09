//! Scenario: the orchestrator-hosted fleet path over the swactor cluster transport.
//!
//! A stage's datastream frames, shipped as `DatastreamFrame` messages to the
//! orchestrator's `DatastreamSink` actor, must surface as a row in the Fleet
//! table. This is the producer→consumer mechanism the live dashboard relies on:
//! the stage runs a [`FleetEmitter`] over a `ClusterFrameSink`, the orchestrator
//! runs the [`DatastreamSink`] actor folding into a `FleetView`, and the `vastai`
//! plugin serves the resulting JSON. There is no dedicated channel — telemetry
//! rides the same transport as everything else. Exercised in-process (one
//! runtime) so it tests the real fold without standing up iroh.

use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use swactor::actor::ActorAddress;
use swactor::runtime::{Runtime, RuntimeConfig};

use dashboard::datastream_source::FleetView;
use datastream::catalog::RuntimeStats;
use datastream::DatastreamSink;
use pipeline_parallel_inference::fleet::FleetEmitter;

#[test]
fn stage_frames_over_cluster_transport_appear_in_the_fleet_table() {
    let rt = Arc::new(Runtime::new(RuntimeConfig::default()));

    // Consumer: the orchestrator's DatastreamSink actor folding each delivery
    // into a FleetView and caching the fleet JSON the dashboard serves.
    let cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let sink_addr = {
        let mut view = FleetView::new(None);
        let cache = Arc::clone(&cache);
        rt.spawn(DatastreamSink::new(move |stream, frame| {
            let update = view.ingest(&stream, &frame);
            *cache.lock().unwrap() = Some(update.fleet_json);
        }))
        .expect("spawn datastream-sink actor")
    };

    // Producer: one stage's emitter shipping over the cluster transport to the
    // resolved sink. The pre-filled slot stands in for SWIM name resolution.
    let node_hex = "ab".repeat(32); // 64 hex chars = a 32-byte node id
    let slot: Arc<OnceLock<ActorAddress>> = Arc::new(OnceLock::new());
    slot.set(sink_addr).expect("set sink slot");
    let mut emitter = FleetEmitter::new(
        Arc::clone(&rt),
        Arc::clone(&slot),
        &node_hex,
        1,
        "pp-stage-0",
        "127.0.0.1:5000",
    );

    // Tick the emitter (ships frames over the cluster transport) and the runtime
    // (delivers them to the sink actor) until the node row shows up. The expected
    // value — a row whose `id` is our node hex — is what we emit, never read back
    // from the consumer first.
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut found = None;
    while Instant::now() < deadline {
        emitter.tick(&[], RuntimeStats::default(), false, 0);
        rt.tick();
        let snapshot = cache.lock().unwrap().clone();
        if let Some(json) = snapshot {
            let v: serde_json::Value = serde_json::from_str(&json).expect("fleet json parses");
            let has_node = v["nodes"]
                .as_array()
                .map(|ns| ns.iter().any(|n| n["id"] == serde_json::json!(node_hex)))
                .unwrap_or(false);
            if has_node {
                found = Some(v);
                break;
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }

    let v = found.expect("the stage's emitted frames must surface as a Fleet-table row");
    assert!(
        v["node_count"].as_u64().unwrap_or(0) >= 1,
        "fleet table must report at least one live node, got {v}",
    );
}
