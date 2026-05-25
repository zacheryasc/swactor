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
use serde_json::Value;

use simulation::host::{Action, Host};
use simulation::stage_host::{StageHost, SubprocessFakeSpec};

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
