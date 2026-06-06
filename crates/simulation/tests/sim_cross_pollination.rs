//! Sim cross-pollination — spec §"Sim cross-pollination" in
//! `examples/pipeline-parallel-inference/N3_OBSERVABILITY_UPGRADE_SPEC.md`.
//!
//! A simulated node's snapshots and events must conform to the same
//! shape a bundle reader decodes for a real node: typed records on
//! named channels, the datastream idiom (`ChannelId` is open-string;
//! a reader dispatches on the channel name the way the dashboard's
//! `FleetView` dispatches on frame channels). We probe by
//! deserialising the sim's bytes through the typed records directly —
//! if they round-trip cleanly, the shapes match.
//!
//! Covers:
//!   - F1: the sim's stage host can install a subprocess fake; its
//!         snapshot block satisfies [`SubprocessState`], and the typed
//!         `spawned` lifecycle record lands in the bundle's event
//!         stream on the `subprocess.lifecycle` channel. The
//!         spec-named "stage's worker never came up" scenario
//!         (`spawned` + no `ready`) is verifiable in one path.
//!   - F2: the sim's stage host carries a tunnel-status field in its
//!         snapshot under the typed [`RelaySession`] shape, defaulting
//!         to `unknown / derived` so honesty-under-absence (§2) holds
//!         even with no scenario config.

use distribution::types::NodeId;
use serde_json::Value;

use simulation::host::{Action, Host};
use simulation::stage_host::{
    InferenceFakeSpec, InferenceResponse, RelaySession, StageHost, SubprocessFakeSpec,
    SubprocessLifecycle, SubprocessState, INFERENCE_RESPONSE_CHANNEL,
    SUBPROCESS_LIFECYCLE_CHANNEL,
};

#[test]
fn stage_host_snapshot_always_carries_relay_session_with_unknown_default() {
    let mut host = StageHost::new("stage-x", "name-x", "addr-x");
    let _ = host.tick(0);
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).expect("snapshot is JSON");
    let block = parsed
        .get("relay_session")
        .cloned()
        .expect("snapshot must always carry relay_session for shape compatibility");
    let typed: RelaySession = serde_json::from_value(block)
        .expect("relay_session must round-trip through the typed RelaySession record");
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
fn stage_host_relay_session_override_round_trips_through_typed_record() {
    let mut host = StageHost::new("stage-r", "name-r", "addr-r");
    host.set_relay_session(RelaySession {
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
    let typed: RelaySession = serde_json::from_value(parsed["relay_session"].clone())
        .expect("override round-trips through RelaySession");
    assert_eq!(typed.status, "connected");
    assert_eq!(typed.status_source, "iroh");
    assert_eq!(typed.tx_bytes_total, Some(1024));
}

#[test]
fn subprocess_fake_emits_typed_spawn_and_carries_typed_snapshot_block() {
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
    let lifecycle = collect_records::<SubprocessLifecycle>(&actions, SUBPROCESS_LIFECYCLE_CHANNEL);
    let saw_spawned = lifecycle
        .iter()
        .any(|r| r.phase == "spawned" && r.label == "fake-worker" && r.pid == 31000);
    assert!(
        saw_spawned,
        "spawned record must reach the bundle's subprocess.lifecycle channel; got {lifecycle:#?}",
    );
    // When never_ready is false, the `ready` companion record fires so
    // the bundle reader can distinguish "spawned and running, ready"
    // from the never-ready bucket.
    let saw_ready = lifecycle.iter().any(|r| r.phase == "ready");
    assert!(saw_ready, "ready companion record must fire when never_ready=false");

    // Snapshot block decodes through the typed record.
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).unwrap();
    let block: SubprocessState = serde_json::from_value(parsed["subprocess"].clone())
        .expect("subprocess must round-trip through SubprocessState");
    assert_eq!(block.subprocesses.len(), 1);
    let entry = &block.subprocesses[0];
    assert_eq!(entry.label, "fake-worker");
    assert_eq!(entry.pid, 31000);
    assert_eq!(entry.status, "running");
    assert_eq!(entry.cmdline.as_deref(), Some("/bin/synthetic --x"));
}

#[test]
fn never_ready_subprocess_fake_emits_spawned_without_ready() {
    // The spec calls out the "stage's worker never came up" bucket
    // explicitly: a `spawned` record with no following `ready` record.
    // The sim fake must be able to reproduce it so scenarios can model
    // that failure case.
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
    let lifecycle = collect_records::<SubprocessLifecycle>(&actions, SUBPROCESS_LIFECYCLE_CHANNEL);
    let saw_spawned = lifecycle.iter().any(|r| r.phase == "spawned");
    let saw_ready = lifecycle.iter().any(|r| r.phase == "ready");
    assert!(saw_spawned, "spawned record must still fire");
    assert!(
        !saw_ready,
        "never_ready=true suppresses the ready record (spec §4 stuck-worker bucket)",
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
    // First tick at t=0 spawns + emits the ready record.
    let _ = host.tick(0);
    // Tick at t=6ms is past the 5ms exit_after_ns threshold — the
    // `exited` record must fire with uptime_ms = 6.
    let actions = host.tick(6_000_000);
    let lifecycle = collect_records::<SubprocessLifecycle>(&actions, SUBPROCESS_LIFECYCLE_CHANNEL);
    let exit = lifecycle
        .iter()
        .find(|r| r.phase == "exited")
        .expect("exited record must fire past exit_after_ns");
    assert_eq!(exit.pid, 31002);
    assert_eq!(exit.exit_code, Some(0));
    assert_eq!(exit.uptime_ms, Some(6));

    // The post-exit snapshot must show status="exited" with the
    // exit code on the snapshot side too — spec cross-cutting §2
    // requires both channels for §4 subprocess facts.
    let snap = host.snapshot();
    let parsed: Value = serde_json::from_slice(&snap).unwrap();
    let block: SubprocessState =
        serde_json::from_value(parsed["subprocess"].clone()).unwrap();
    assert_eq!(block.subprocesses[0].status, "exited");
    assert_eq!(block.subprocesses[0].exit_code, Some(0));
}

// ──────────────────────────────────────────────────────────────────────
// Coverage 2.4 — inference response-leg send-outcome record
// ──────────────────────────────────────────────────────────────────────

#[test]
fn inference_fake_emits_typed_response_sent_with_timeout_outcome() {
    // Coverage 2.4 close-criterion shape: the last stage emits
    // exactly one `inference.response` record carrying target /
    // request / size / outcome when its outbound to the orchestrator
    // fails. The `1779733878` failure attribution — "last stage could
    // not deliver the response" — is a single typed read, not a
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
    // Past the fire time: the record lands.
    let actions = host.tick(5_500_000);
    let responses = collect_records::<InferenceResponse>(&actions, INFERENCE_RESPONSE_CHANNEL);
    let sent = responses
        .first()
        .expect("inference.response record must fire past fire_at_ns");
    assert_eq!(sent.request_id, "req-7f3c");
    assert_eq!(sent.byte_size, 4_096);
    assert_eq!(sent.send_outcome, "timeout");
    // target_peer round-trips through the production NodeId schema.
    assert_eq!(sent.target_peer, orch_node_id);
}

#[test]
fn inference_fake_fires_at_most_once_across_many_ticks() {
    // Spec §2.4 says "exactly one" record per response send. A stage
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
        seen += collect_records::<InferenceResponse>(&actions, INFERENCE_RESPONSE_CHANNEL).len();
    }
    assert_eq!(
        seen, 1,
        "inference.response must fire exactly once across many ticks past fire_at_ns",
    );
}

#[test]
fn inference_fake_unset_emits_no_response_record() {
    // Honesty-under-absence: a stage with no inference fake produces
    // no inference.response record. The bundle reader sees the absence;
    // a silent synthesized record would break the channel contract.
    let mut host = StageHost::new("stage-quiet", "pp-stage-quiet", "10.0.0.22:7700");
    let _ = host.tick(0);
    for t in [1_000_000u64, 5_000_000, 50_000_000] {
        let actions = host.tick(t);
        assert!(
            collect_records::<InferenceResponse>(&actions, INFERENCE_RESPONSE_CHANNEL).is_empty(),
            "unsetting the inference fake must suppress inference.response records",
        );
    }
}

// ─── helpers ──────────────────────────────────────────────────────────

/// Decode every `channel_record` event on `channel` through the typed
/// record `R` — the same "dispatch on channel, decode the payload"
/// move a datastream consumer makes.
fn collect_records<R: serde::de::DeserializeOwned>(actions: &[Action], channel: &str) -> Vec<R> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::RecordEvent { event, .. } => {
                let v: Value = serde_json::from_slice(event).ok()?;
                if v.get("kind").and_then(|x| x.as_str()) == Some("channel_record")
                    && v.get("channel").and_then(|x| x.as_str()) == Some(channel)
                {
                    serde_json::from_value(v.get("record")?.clone()).ok()
                } else {
                    None
                }
            }
            _ => None,
        })
        .collect()
}
