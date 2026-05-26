//! Sim cross-pollination — spec §"Sim cross-pollination" in
//! `examples/pipeline-parallel-inference/N3_OBSERVABILITY_UPGRADE_SPEC.md`.
//!
//! A simulated node's snapshots and events must conform to the same
//! shape as a real node's: the bundle reader should not be able to
//! tell from data shape alone whether a given snapshot came from a
//! real deployment or the sim. We probe by deserialising the sim's
//! snapshot bytes through the production types directly — if they
//! round-trip cleanly, the shapes match.
//!
//! Covers:
//!   - F1: the sim's stage host can install a subprocess fake; its
//!         snapshot block satisfies production's `Tier3SubprocessState`,
//!         and the typed `SubprocessSpawned` event lands in the bundle's
//!         event stream wrapped in the existing `diag_event` envelope.
//!         The spec-named "stage's worker never came up" scenario
//!         (Spawned + no worker_ready) is verifiable in one path.
//!   - F2: the sim's stage host carries a tunnel-status field in its
//!         snapshot under the production-shape `Tier2RelaySession`,
//!         defaulting to `unknown / derived` so honesty-under-absence
//!         (§2) holds even with no scenario config.

use distribution::diagnostics::{Tier2RelaySession, Tier3SubprocessState};
use distribution::types::NodeId;
use serde_json::Value;

use simulation::host::{Action, Host};
use simulation::stage_host::{InferenceFakeSpec, StageHost, SubprocessFakeSpec};

#[test]
fn stage_host_snapshot_always_carries_tier2_relay_session_with_unknown_default() {
    let mut host = StageHost::new("stage-x", "name-x", "addr-x");
    let _ = host.tick(0);
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).expect("snapshot is JSON");
    let tier2 = parsed
        .get("tier2_relay_session")
        .cloned()
        .expect("snapshot must always carry tier2_relay_session for shape compatibility");
    let typed: Tier2RelaySession = serde_json::from_value(tier2)
        .expect("tier2_relay_session must round-trip through production's Tier2RelaySession");
    assert_eq!(
        typed.status, "unknown",
        "default tunnel status must be `unknown` under §2 honesty-under-absence",
    );
    assert_eq!(
        typed.status_source, "derived",
        "default status_source must be `derived` so readers know it's synthesized",
    );
}

#[test]
fn stage_host_relay_session_override_round_trips_through_production_type() {
    let mut host = StageHost::new("stage-r", "name-r", "addr-r");
    host.set_relay_session(Tier2RelaySession {
        relay_url: Some("https://relay.example/".into()),
        status: "connected".into(),
        status_source: "iroh".into(),
        status_changed_at_ms: Some(10),
        status_entered_at_ms: Some(10),
        last_send_at_ms: Some(20),
        last_recv_at_ms: Some(30),
        tx_bytes_total: Some(1024),
        rx_bytes_total: Some(2048),
    });
    let _ = host.tick(0);
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).unwrap();
    let typed: Tier2RelaySession = serde_json::from_value(parsed["tier2_relay_session"].clone())
        .expect("override round-trips through Tier2RelaySession");
    assert_eq!(typed.status, "connected");
    assert_eq!(typed.status_source, "iroh");
    assert_eq!(typed.tx_bytes_total, Some(1024));
}

#[test]
fn subprocess_fake_emits_typed_spawn_and_carries_production_shape_snapshot_block() {
    let mut host = StageHost::new("stage-fake", "name-f", "addr-f");
    host.set_subprocess_fake(SubprocessFakeSpec {
        label: "fake-worker".into(),
        pid: 31000,
        command: "/bin/synthetic --x".into(),
        never_ready: false,
        exit_after_ns: None,
        exit_code: None,
        exit_signal: None,
    });
    let actions = host.tick(0);
    let diag_events = collect_diag_events(&actions);
    let saw_spawned = diag_events.iter().any(|p| {
        p.get("type").and_then(|v| v.as_str()) == Some("SubprocessSpawned")
            && p.get("label").and_then(|v| v.as_str()) == Some("fake-worker")
            && p.get("pid").and_then(|v| v.as_u64()) == Some(31000)
    });
    assert!(
        saw_spawned,
        "SubprocessSpawned must reach the bundle's diag_event stream; got {diag_events:#?}",
    );
    // When never_ready is false, the worker_ready Custom companion
    // event fires so the bundle reader can distinguish "spawned and
    // running, ready" from the never-ready bucket.
    let saw_ready = diag_events.iter().any(|p| {
        p.get("type").and_then(|v| v.as_str()) == Some("Custom")
            && p.get("kind").and_then(|v| v.as_str()) == Some("worker_ready")
    });
    assert!(saw_ready, "worker_ready Custom companion must fire when never_ready=false");

    // Snapshot block is production-shape.
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).unwrap();
    let tier3: Tier3SubprocessState = serde_json::from_value(
        parsed["tier3_subprocess"].clone(),
    )
    .expect("tier3_subprocess must round-trip through Tier3SubprocessState");
    assert_eq!(tier3.subprocesses.len(), 1);
    let entry = &tier3.subprocesses[0];
    assert_eq!(entry.label, "fake-worker");
    assert_eq!(entry.pid, 31000);
    assert_eq!(entry.status, "running");
    assert_eq!(entry.cmdline.as_deref(), Some("/bin/synthetic --x"));
}

#[test]
fn never_ready_subprocess_fake_emits_spawned_without_worker_ready() {
    // The spec calls out the "stage's worker never came up" bucket
    // explicitly: a SubprocessSpawned with no following worker_ready
    // Custom event. The sim fake must be able to reproduce it so
    // scenarios can model that failure case.
    let mut host = StageHost::new("stage-stuck", "name-s", "addr-s");
    host.set_subprocess_fake(SubprocessFakeSpec {
        label: "stuck-worker".into(),
        pid: 31001,
        command: "/bin/python startup_hangs.py".into(),
        never_ready: true,
        exit_after_ns: None,
        exit_code: None,
        exit_signal: None,
    });
    let actions = host.tick(0);
    let diag_events = collect_diag_events(&actions);
    let saw_spawned = diag_events
        .iter()
        .any(|p| p.get("type").and_then(|v| v.as_str()) == Some("SubprocessSpawned"));
    let saw_ready = diag_events.iter().any(|p| {
        p.get("type").and_then(|v| v.as_str()) == Some("Custom")
            && p.get("kind").and_then(|v| v.as_str()) == Some("worker_ready")
    });
    assert!(saw_spawned, "SubprocessSpawned must still fire");
    assert!(
        !saw_ready,
        "never_ready=true suppresses worker_ready (spec §4 stuck-worker bucket)",
    );
}

#[test]
fn exit_after_ns_emits_typed_exited_with_correct_uptime() {
    let mut host = StageHost::new("stage-exit", "name-e", "addr-e");
    host.set_subprocess_fake(SubprocessFakeSpec {
        label: "ephemeral".into(),
        pid: 31002,
        command: "/bin/true".into(),
        never_ready: false,
        exit_after_ns: Some(5_000_000),
        exit_code: Some(0),
        exit_signal: None,
    });
    // First tick at t=0 spawns + emits worker_ready.
    let _ = host.tick(0);
    // Tick at t=6ms is past the 5ms exit_after_ns threshold —
    // SubprocessExited must fire with uptime_ms = 6.
    let actions = host.tick(6_000_000);
    let diag_events = collect_diag_events(&actions);
    let exit = diag_events
        .iter()
        .find(|p| p.get("type").and_then(|v| v.as_str()) == Some("SubprocessExited"))
        .expect("SubprocessExited must fire past exit_after_ns");
    assert_eq!(exit["pid"].as_u64(), Some(31002));
    assert_eq!(exit["exit_code"].as_i64(), Some(0));
    assert_eq!(exit["uptime_ms"].as_u64(), Some(6));

    // The post-exit snapshot must show status="exited" with the
    // exit code on the snapshot side too — spec cross-cutting §2
    // requires both channels for §4 subprocess facts.
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).unwrap();
    let tier3: Tier3SubprocessState =
        serde_json::from_value(parsed["tier3_subprocess"].clone()).unwrap();
    assert_eq!(tier3.subprocesses[0].status, "exited");
    assert_eq!(tier3.subprocesses[0].exit_code, Some(0));
}

// ──────────────────────────────────────────────────────────────────────
// Coverage 2.4 — inference response-leg send-outcome event
// ──────────────────────────────────────────────────────────────────────

#[test]
fn inference_fake_emits_typed_response_sent_with_timeout_outcome() {
    // Coverage 2.4 close-criterion shape: the last stage emits
    // exactly one `InferenceResponseSent` carrying target / request /
    // size / outcome when its outbound to the orchestrator fails.
    // The `1779733878` failure attribution — "last stage could not
    // deliver the response" — is now a single typed read, not a
    // triangulation against dial timeouts.
    let mut host = StageHost::new("stage-last", "pp-stage-last", "10.0.0.20:7700");
    let orch_node_id = NodeId([0xAB; 32]);
    host.set_inference_fake(InferenceFakeSpec {
        fire_at_ns: 5_000_000,
        target_peer_node_id: orch_node_id,
        request_id: "req-7f3c".into(),
        byte_size: 4_096,
        send_outcome: "timeout".into(),
    });
    // Drive into Running.
    let _ = host.tick(0);
    // Past the fire time: the event lands.
    let actions = host.tick(5_500_000);
    let diag_events = collect_diag_events(&actions);
    let sent = diag_events
        .iter()
        .find(|p| p.get("type").and_then(|v| v.as_str()) == Some("InferenceResponseSent"))
        .expect("InferenceResponseSent must fire past fire_at_ns");
    assert_eq!(sent["request_id"].as_str(), Some("req-7f3c"));
    assert_eq!(sent["byte_size"].as_u64(), Some(4_096));
    assert_eq!(sent["send_outcome"].as_str(), Some("timeout"));
    // target_peer round-trips through the production NodeId schema.
    let target: NodeId = serde_json::from_value(sent["target_peer"].clone())
        .expect("target_peer must deserialize as NodeId");
    assert_eq!(target, orch_node_id);
}

#[test]
fn inference_fake_fires_at_most_once_across_many_ticks() {
    // Spec §2.4 says "exactly one" event per response send. A stage
    // host that re-emitted on every tick past `fire_at_ns` would
    // produce double-counting in the bundle.
    let mut host = StageHost::new("stage-once", "pp-stage-once", "10.0.0.21:7700");
    host.set_inference_fake(InferenceFakeSpec {
        fire_at_ns: 1_000_000,
        target_peer_node_id: NodeId([0xCD; 32]),
        request_id: "req-dedupe".into(),
        byte_size: 128,
        send_outcome: "success".into(),
    });
    let _ = host.tick(0);
    let mut seen = 0usize;
    for t in [1_000_000u64, 2_000_000, 3_000_000, 10_000_000] {
        let actions = host.tick(t);
        for p in collect_diag_events(&actions) {
            if p.get("type").and_then(|v| v.as_str()) == Some("InferenceResponseSent") {
                seen += 1;
            }
        }
    }
    assert_eq!(
        seen, 1,
        "InferenceResponseSent must fire exactly once across many ticks past fire_at_ns",
    );
}

#[test]
fn inference_fake_unset_emits_no_response_event() {
    // Honesty-under-absence: a stage with no inference fake produces
    // no InferenceResponseSent. The bundle reader sees the absence
    // (the postproc renders the gap-2.4 absence-line); a silent
    // synthesized event would break the discriminator contract.
    let mut host = StageHost::new("stage-quiet", "pp-stage-quiet", "10.0.0.22:7700");
    let _ = host.tick(0);
    for t in [1_000_000u64, 5_000_000, 50_000_000] {
        let actions = host.tick(t);
        for p in collect_diag_events(&actions) {
            assert_ne!(
                p.get("type").and_then(|v| v.as_str()),
                Some("InferenceResponseSent"),
                "unsetting the inference fake must suppress InferenceResponseSent",
            );
        }
    }
}

// ─── helpers ──────────────────────────────────────────────────────────

fn collect_diag_events(actions: &[Action]) -> Vec<Value> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::RecordEvent { event, .. } => {
                let v: Value = serde_json::from_slice(event).ok()?;
                if v.get("kind").and_then(|x| x.as_str()) == Some("diag_event") {
                    v.get("payload").cloned()
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect()
}
