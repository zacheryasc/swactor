//! Story test for the bootstrap → joined fleet lifecycle.
//!
//! The orchestrator launches N nodes; each one is *seen loading* (its boot
//! output), then either joins (flips to live) or misses the deadline (flips to
//! failed, keeping its last logs as evidence). This drives that one story end to
//! end through the public [`FleetLifecycle`] API and asserts on the Fleet model
//! it renders — the contract the dashboard consumes — not on its internals.

use std::sync::{Arc, Mutex};

use dashboard::datastream_source::FleetView;
use pipeline_parallel_inference::fleet_lifecycle::FleetLifecycle;

/// Parse the rendered Fleet cache and return the row for `stage`.
fn row_for_stage(cache: &Arc<Mutex<Option<String>>>, stage: u64) -> serde_json::Value {
    let json = cache.lock().unwrap().clone().expect("lifecycle rendered a model");
    let model: serde_json::Value = serde_json::from_str(&json).unwrap();
    model["nodes"]
        .as_array()
        .unwrap()
        .iter()
        .find(|n| n["stage"].as_u64() == Some(stage))
        .cloned()
        .unwrap_or_else(|| panic!("no fleet row for stage {stage}"))
}

#[test]
fn a_node_loads_one_joins_the_rest_fail() {
    let fleet = Arc::new(Mutex::new(FleetView::new(None)));
    let cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let lifecycle = FleetLifecycle::new(Arc::clone(&fleet), Arc::clone(&cache), 3);

    // At launch every entry is bootstrapping and present in the model — the
    // operator sees three rows immediately, before anything has joined.
    let model: serde_json::Value =
        serde_json::from_str(&cache.lock().unwrap().clone().unwrap()).unwrap();
    assert_eq!(model["node_count"].as_u64(), Some(3));
    assert!(
        model["nodes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|n| n["state"] == "bootstrapping"),
        "every launched node starts bootstrapping: {model}"
    );

    // The nodes are seen loading — their boot output flows into the rows.
    lifecycle.record_bootstrap(0, false, "pulling image swactor-pp-gpu");
    lifecycle.record_bootstrap(1, false, "starting container");
    lifecycle.record_bootstrap(2, true, "warning: slow disk");

    // Stage 0 joins the cluster: the seam flips it to live.
    lifecycle.bind_live(0, "aabbccddeeff00112233445566778899");

    // The join deadline passes for the rest — they flip to failed but keep the
    // last thing they printed as the failure evidence.
    lifecycle.fail_remaining_bootstrapping();

    let s0 = row_for_stage(&cache, 0);
    let s1 = row_for_stage(&cache, 1);
    let s2 = row_for_stage(&cache, 2);

    assert_eq!(s0["state"], "live", "the joined node is live: {s0}");
    assert_eq!(s0["short"], "aabbccdd", "live row shows the node's short id");

    assert_eq!(s1["state"], "failed", "a node that never joined failed: {s1}");
    assert_eq!(
        s1["last_proc"], "starting container",
        "a failed node keeps its last boot line as evidence: {s1}"
    );
    assert_eq!(s2["state"], "failed");
    assert_eq!(s2["last_proc"], "warning: slow disk");
}

#[test]
fn lease_metadata_rides_the_row_without_node_plumbing() {
    let fleet = Arc::new(Mutex::new(FleetView::new(None)));
    let cache: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let lifecycle = FleetLifecycle::new(Arc::clone(&fleet), Arc::clone(&cache), 2);

    // Contract/cost is orchestrator-side knowledge from leasing — it attaches to
    // the row directly, with no telemetry shipped from the node.
    lifecycle.set_lease(
        1,
        pipeline_parallel_inference::fleet_lifecycle::LeaseMeta {
            contract_id: 987654,
            dph: 0.42,
            status: "leased".into(),
            region: "us-west".into(),
            gpu: "RTX4090".into(),
        },
    );

    let row = row_for_stage(&cache, 1);
    assert_eq!(row["contract"].as_u64(), Some(987654));
    assert_eq!(row["region"], "us-west");
    assert_eq!(row["gpu"], "RTX4090");
}
