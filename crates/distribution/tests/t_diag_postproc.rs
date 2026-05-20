//! Gate for S11: post-processor binary + module.
//!
//! Drives the **whole renderer pipeline** against a deterministic
//! N=3 fixture bundle that we build at test start by feeding the
//! collector's storage layer the same shape of records a real run
//! would emit. The committed file `expected-summary.md` is the
//! golden for the renderer — if you change the renderer output,
//! update the golden; that's intentional.
//!
//! Coverage:
//!
//! 1. Fixture bundle is assembled successfully (tarball produced).
//! 2. `Bundle::parse_path` reads it back with the right shape:
//!    three nodes, identity blocks present, events sorted, etc.
//! 3. `Outputs::from_bundle` produces a non-empty `summary.md`, a
//!    reachability TSV with the matrix shape we expect, and a
//!    timeline TSV per ordered pair.
//! 4. `summary.md` matches the committed golden byte-for-byte. This
//!    is the **regression check**.
//! 5. The compiled `swactor-diag-postproc` binary runs against the
//!    bundle and writes the same files.
//! 6. `render_diff` produces a sensible message when two bundles
//!    differ on the first-Dead pair.

#![cfg(feature = "collector")]

use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use distribution::diagnostics::collector::{CollectorState, bundle::assemble};
use distribution::diagnostics::postproc::{Bundle, Outputs, render_diff};

const RUN_ID: &str = "fixture-run-1";
const ORCH_HEX: &str = "1010101010101010101010101010101010101010101010101010101010101010";
const STAGE0_HEX: &str = "2020202020202020202020202020202020202020202020202020202020202020";
const STAGE1_HEX: &str = "3030303030303030303030303030303030303030303030303030303030303030";

#[test]
fn parses_bundle_and_renders_outputs_matching_golden() {
    let env = TestEnv::new("parses");
    let bundle_path = build_fixture_bundle(&env, RUN_ID, FixtureKind::Standard);

    // 1. Parse.
    let bundle = Bundle::parse_path(&bundle_path).expect("parse bundle");
    assert_eq!(bundle.run_id, RUN_ID);
    assert_eq!(bundle.manifest.nodes.len(), 3);
    for label in ["orchestrator", "stage-0", "stage-1"] {
        let node = bundle
            .nodes
            .get(label)
            .unwrap_or_else(|| panic!("missing node {label}"));
        assert!(
            node.identity.is_some(),
            "node {label} missing identity (boot.json)"
        );
        assert!(
            !node.snapshots.is_empty(),
            "node {label} has no snapshots"
        );
        // orchestrator drives the timeline events; stages have some too.
        assert!(
            !node.events.is_empty(),
            "node {label} has no events"
        );
        // Events should be chronologically sorted.
        for w in node.events.windows(2) {
            assert!(
                (w[0].wall_ms, w[0].monotonic_seq) <= (w[1].wall_ms, w[1].monotonic_seq),
                "node {label} events not chronologically sorted"
            );
        }
    }

    // 2. Render every output.
    let outputs = Outputs::from_bundle(&bundle);

    // 3. Reachability TSV: header line + at least one data row per
    //    snapshot. Column count = 2 (time, observer) + N (one per
    //    peer label).
    let reach_lines: Vec<&str> = outputs.reachability_tsv.lines().collect();
    assert!(!reach_lines.is_empty(), "reachability tsv empty");
    let header = reach_lines[0];
    let header_cols: Vec<&str> = header.split('\t').collect();
    assert_eq!(header_cols[0], "time_ms");
    assert_eq!(header_cols[1], "observer");
    assert_eq!(header_cols.len(), 2 + 3, "expected 2 + N=3 columns");
    let total_snapshots: usize = bundle.nodes.values().map(|n| n.snapshots.len()).sum();
    assert_eq!(
        reach_lines.len() - 1,
        total_snapshots,
        "one data row per snapshot"
    );
    for row in &reach_lines[1..] {
        let cols: Vec<&str> = row.split('\t').collect();
        assert_eq!(cols.len(), header_cols.len(), "ragged row: {row}");
    }

    // 4. Timeline TSVs: one per ordered pair of labels, N*(N-1) = 6.
    assert_eq!(outputs.timelines.len(), 6);
    let names: Vec<&str> = outputs.timelines.iter().map(|(n, _)| n.as_str()).collect();
    for required in [
        "timeline-orchestrator-to-stage-1.tsv",
        "timeline-stage-1-to-orchestrator.tsv",
    ] {
        assert!(
            names.contains(&required),
            "missing timeline {required}; got {names:?}"
        );
    }
    // The orchestrator→stage-1 timeline must mention the
    // SwimTransition -> Dead row — that's the storyline.
    let orch_to_s1 = outputs
        .timelines
        .iter()
        .find(|(n, _)| n == "timeline-orchestrator-to-stage-1.tsv")
        .map(|(_, b)| b.as_str())
        .unwrap();
    assert!(
        orch_to_s1.contains("SwimTransition"),
        "orchestrator→stage-1 timeline missing SwimTransition rows: {orch_to_s1}"
    );
    assert!(
        orch_to_s1.contains("Dead"),
        "orchestrator→stage-1 timeline missing Dead transition"
    );

    // 5. summary.md regression check against committed golden.
    let golden_path = fixtures_dir().join("expected-summary.md");
    if std::env::var_os("UPDATE_SUMMARY_GOLDEN").is_some() {
        std::fs::create_dir_all(golden_path.parent().unwrap()).expect("mkdir fixtures");
        std::fs::write(&golden_path, &outputs.summary_md).expect("write golden");
    }
    let golden = std::fs::read_to_string(&golden_path)
        .unwrap_or_else(|_| panic!("missing fixture: {}", golden_path.display()));
    assert_eq!(
        outputs.summary_md, golden,
        "summary.md drifted from golden at {}. Re-run with UPDATE_SUMMARY_GOLDEN=1 if the change is intentional.",
        golden_path.display()
    );

    // 6. Outputs::write_all_to writes the expected file set.
    let out_dir = env.tmp_root.join("rendered");
    let written = outputs.write_all_to(&out_dir).expect("write outputs");
    assert!(out_dir.join("summary.md").exists());
    assert!(out_dir.join("reachability.tsv").exists());
    assert!(
        written
            .iter()
            .any(|p| p.ends_with("timeline-orchestrator-to-stage-1.tsv")),
        "did not write orchestrator→stage-1 timeline; got {written:?}"
    );
}

#[test]
fn binary_renders_bundle_to_default_output_dir() {
    let env = TestEnv::new("binary");
    let bundle_path = build_fixture_bundle(&env, RUN_ID, FixtureKind::Standard);

    let exe = env!("CARGO_BIN_EXE_swactor-diag-postproc");
    let status = Command::new(exe)
        .arg(&bundle_path)
        .status()
        .expect("spawn swactor-diag-postproc");
    assert!(status.success(), "postproc binary failed: {status:?}");

    // Default OUT_DIR is `<bundle stem>.out/` sibling.
    let out_dir = bundle_path
        .parent()
        .unwrap()
        .join(format!("{RUN_ID}.out"));
    let summary = out_dir.join("summary.md");
    let body = std::fs::read_to_string(&summary).expect("read produced summary.md");
    assert!(!body.is_empty());
    assert!(body.contains("# Diagnostics summary"));
    assert!(out_dir.join("reachability.tsv").exists());
    assert!(out_dir.join("timeline-orchestrator-to-stage-1.tsv").exists());
}

#[test]
fn diff_highlights_when_first_dead_pair_changes() {
    let env_a = TestEnv::new("diff-a");
    let env_b = TestEnv::new("diff-b");
    let bundle_a = build_fixture_bundle(&env_a, "run-a", FixtureKind::Standard);
    // The "AltVictim" fixture flips who goes Dead — stage-0 is the
    // first to die instead of stage-1. Diff should call that out.
    let bundle_b = build_fixture_bundle(&env_b, "run-b", FixtureKind::AltVictim);

    let a = Bundle::parse_path(&bundle_a).expect("parse a");
    let b = Bundle::parse_path(&bundle_b).expect("parse b");
    let diff = render_diff(&a, &b);
    assert!(
        diff.contains("first_dead pair changed"),
        "diff did not mention pair change:\n{diff}"
    );
    assert!(diff.contains("run-a"));
    assert!(diff.contains("run-b"));
}

// ---------- Fixture construction ----------

#[derive(Debug, Clone, Copy)]
enum FixtureKind {
    /// stage-1 is the first peer to go Dead (from orchestrator's view).
    Standard,
    /// stage-0 is the first peer to go Dead — used by the diff test.
    AltVictim,
}

fn build_fixture_bundle(env: &TestEnv, run_id: &str, kind: FixtureKind) -> PathBuf {
    let state = Arc::new(CollectorState::new(&env.collector_root));
    let dead_peer_hex = match kind {
        FixtureKind::Standard => STAGE1_HEX,
        FixtureKind::AltVictim => STAGE0_HEX,
    };

    // Boot records — collector treats these as the identity payload
    // for the per-node label.
    persist_boot(&state, run_id, ORCH_HEX, identity_value(ORCH_HEX, "orchestrator", None, None));
    persist_boot(
        &state,
        run_id,
        STAGE0_HEX,
        identity_value(STAGE0_HEX, "stage", Some(0), Some(2)),
    );
    persist_boot(
        &state,
        run_id,
        STAGE1_HEX,
        identity_value(STAGE1_HEX, "stage", Some(1), Some(2)),
    );

    // Orchestrator events.
    let orch_events = orchestrator_events(dead_peer_hex);
    persist_events(&state, run_id, ORCH_HEX, &orch_events);

    // stage-0 events.
    let stage0_events = stage_events(STAGE0_HEX, dead_peer_hex == STAGE0_HEX);
    persist_events(&state, run_id, STAGE0_HEX, &stage0_events);

    // stage-1 events.
    let stage1_events = stage_events(STAGE1_HEX, dead_peer_hex == STAGE1_HEX);
    persist_events(&state, run_id, STAGE1_HEX, &stage1_events);

    // Snapshots. Each node gets two: pre-incident, post-incident.
    persist_snapshot(
        &state,
        run_id,
        ORCH_HEX,
        orchestrator_snapshot(run_id, 1, 2500, dead_peer_hex, true),
    );
    persist_snapshot(
        &state,
        run_id,
        ORCH_HEX,
        orchestrator_snapshot(run_id, 2, 5200, dead_peer_hex, false),
    );
    persist_snapshot(
        &state,
        run_id,
        STAGE0_HEX,
        stage_snapshot(run_id, STAGE0_HEX, "stage", 0, 1, 2200),
    );
    persist_snapshot(
        &state,
        run_id,
        STAGE1_HEX,
        stage_snapshot(run_id, STAGE1_HEX, "stage", 1, 1, 2200),
    );

    // Finalize from orchestrator.
    persist_finalize(&state, run_id, ORCH_HEX, json!({"exit_reason": "ok"}));

    assemble(&state, run_id).expect("assemble bundle")
}

fn persist_boot(state: &CollectorState, run_id: &str, node_hex: &str, body: Value) {
    state
        .persist(
            run_id,
            node_hex,
            distribution::diagnostics::collector::RecordKind::Boot,
            0,
            &body,
        )
        .expect("persist boot");
}

fn persist_events(state: &CollectorState, run_id: &str, node_hex: &str, batch: &[Value]) {
    let body = Value::Array(batch.to_vec());
    state
        .persist(
            run_id,
            node_hex,
            distribution::diagnostics::collector::RecordKind::Events,
            0,
            &body,
        )
        .expect("persist events");
}

fn persist_snapshot(state: &CollectorState, run_id: &str, node_hex: &str, body: Value) {
    state
        .persist(
            run_id,
            node_hex,
            distribution::diagnostics::collector::RecordKind::Snapshot,
            0,
            &body,
        )
        .expect("persist snapshot");
}

fn persist_finalize(state: &CollectorState, run_id: &str, node_hex: &str, body: Value) {
    state
        .persist(
            run_id,
            node_hex,
            distribution::diagnostics::collector::RecordKind::Finalize,
            0,
            &body,
        )
        .expect("persist finalize");
}

fn identity_value(
    node_hex: &str,
    role: &str,
    stage_index: Option<u32>,
    stage_count: Option<u32>,
) -> Value {
    let short: String = node_hex.chars().take(8).collect();
    json!({
        "node_id_hex": node_hex,
        "node_id_short": short,
        "role": role,
        "stage_index": stage_index,
        "stage_count": stage_count,
        "run_id": RUN_ID,
        "vastai_contract_id": null,
        "host_ip_public": null,
        "host_country": null,
        "datacenter_id": null,
        "hostname": null,
        "container_id": null,
        "process_start_unix_ms": 0u64,
        "boot_sequence": 0u32,
        "binary_version": null,
        "git_sha": null,
        "iroh_version": null,
        "home_relay_url_at_boot": null,
    })
}

// Build a NodeId array as serde sees it (32 numeric bytes).
fn node_id_array(hex: &str) -> Value {
    let bytes = hex_decode(hex);
    Value::Array(bytes.iter().map(|b| Value::from(*b as u64)).collect())
}

fn hex_decode(hex: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks_exact(2) {
        let hi = nibble(chunk[0]);
        let lo = nibble(chunk[1]);
        out.push((hi << 4) | lo);
    }
    out
}

fn nibble(b: u8) -> u8 {
    match b {
        b'0'..=b'9' => b - b'0',
        b'a'..=b'f' => b - b'a' + 10,
        b'A'..=b'F' => b - b'A' + 10,
        _ => 0,
    }
}

fn orchestrator_events(dead_peer_hex: &str) -> Vec<Value> {
    let mut out = Vec::new();
    let mut seq: u64 = 0;
    // Dial + transition into Alive for both stages.
    out.push(event(&mut seq, ORCH_HEX, 1000, "DialStarted", json!({
        "peer": node_id_array(STAGE0_HEX),
        "attempt": 1u32,
        "timeout_ms": 500u64,
    })));
    out.push(event(&mut seq, ORCH_HEX, 1012, "DialOutcome", json!({
        "peer": node_id_array(STAGE0_HEX),
        "attempt": 1u32,
        "outcome": "Success",
        "duration_ms": 12u64,
    })));
    out.push(event(&mut seq, ORCH_HEX, 1015, "SwimTransition", json!({
        "peer": node_id_array(STAGE0_HEX),
        "from": "Unknown",
        "to": "Alive",
        "reason": "joined",
    })));
    out.push(event(&mut seq, ORCH_HEX, 1100, "DialStarted", json!({
        "peer": node_id_array(STAGE1_HEX),
        "attempt": 1u32,
        "timeout_ms": 500u64,
    })));
    out.push(event(&mut seq, ORCH_HEX, 1114, "DialOutcome", json!({
        "peer": node_id_array(STAGE1_HEX),
        "attempt": 1u32,
        "outcome": "Success",
        "duration_ms": 14u64,
    })));
    out.push(event(&mut seq, ORCH_HEX, 1120, "SwimTransition", json!({
        "peer": node_id_array(STAGE1_HEX),
        "from": "Unknown",
        "to": "Alive",
        "reason": "joined",
    })));

    // Ping-pong with both.
    out.push(event(&mut seq, ORCH_HEX, 2000, "MessageSent", json!({
        "peer": node_id_array(STAGE0_HEX),
        "kind": "ping", "size": 32u32,
    })));
    out.push(event(&mut seq, ORCH_HEX, 2030, "MessageReceived", json!({
        "peer": node_id_array(STAGE0_HEX),
        "kind": "pong", "size": 16u32,
    })));
    out.push(event(&mut seq, ORCH_HEX, 2050, "MessageSent", json!({
        "peer": node_id_array(STAGE1_HEX),
        "kind": "ping", "size": 32u32,
    })));
    out.push(event(&mut seq, ORCH_HEX, 2080, "MessageReceived", json!({
        "peer": node_id_array(STAGE1_HEX),
        "kind": "pong", "size": 16u32,
    })));

    // The dying peer goes through Suspect → Dead. Reuse the dial /
    // cache events to support the storyline.
    out.push(event(&mut seq, ORCH_HEX, 3000, "DialStarted", json!({
        "peer": node_id_array(dead_peer_hex),
        "attempt": 2u32,
        "timeout_ms": 1000u64,
    })));
    out.push(event(&mut seq, ORCH_HEX, 4000, "DialOutcome", json!({
        "peer": node_id_array(dead_peer_hex),
        "attempt": 2u32,
        "outcome": "Timeout",
        "duration_ms": 1000u64,
    })));
    out.push(event(&mut seq, ORCH_HEX, 4001, "ConnectionCacheInvalidated", json!({
        "peer": node_id_array(dead_peer_hex),
        "generation": 1u64,
        "reason": "dial-timeout",
    })));
    out.push(event(&mut seq, ORCH_HEX, 4100, "SwimTransition", json!({
        "peer": node_id_array(dead_peer_hex),
        "from": "Alive",
        "to": "Suspect",
        "reason": "missed-acks",
    })));
    out.push(event(&mut seq, ORCH_HEX, 5100, "SwimTransition", json!({
        "peer": node_id_array(dead_peer_hex),
        "from": "Suspect",
        "to": "Dead",
        "reason": "suspicion-timeout",
    })));
    out
}

fn stage_events(self_hex: &str, is_dead_peer: bool) -> Vec<Value> {
    let mut out = Vec::new();
    let mut seq: u64 = 0;
    // Both stages see the orchestrator come Alive and exchange
    // pings, regardless of which one ends up Dead.
    out.push(event(&mut seq, self_hex, 1020, "SwimTransition", json!({
        "peer": node_id_array(ORCH_HEX),
        "from": "Unknown",
        "to": "Alive",
        "reason": "joined",
    })));
    out.push(event(&mut seq, self_hex, 2010, "MessageReceived", json!({
        "peer": node_id_array(ORCH_HEX),
        "kind": "ping", "size": 32u32,
    })));
    if !is_dead_peer {
        // Healthy stage answers the ping; the dying one goes quiet.
        out.push(event(&mut seq, self_hex, 2020, "MessageSent", json!({
            "peer": node_id_array(ORCH_HEX),
            "kind": "pong", "size": 16u32,
        })));
    }
    out
}

fn orchestrator_snapshot(
    run_id: &str,
    snap_seq: u64,
    wall_ms: u64,
    dead_peer_hex: &str,
    pre_incident: bool,
) -> Value {
    let other_alive_hex = if dead_peer_hex == STAGE1_HEX { STAGE0_HEX } else { STAGE1_HEX };
    let dead_opinion = if pre_incident { "Alive" } else { "Dead" };
    let dead_conn = if pre_incident { "Direct" } else { "None" };
    json!({
        "identity": identity_value(ORCH_HEX, "orchestrator", None, None),
        "run_id": run_id,
        "snapshot_id": format!("orch-{snap_seq}"),
        "wall_ms": wall_ms,
        "monotonic_seq": (snap_seq * 100u64),
        "trigger": {"kind": if pre_incident { "Periodic" } else { "OnDemand" }},
        "body": {
            "reachability": [
                reach_entry(other_alive_hex, "Alive"),
                reach_entry(dead_peer_hex, dead_opinion),
            ],
            "events": [],
            "iroh": {
                "home_relay_url": "https://relay.example.test/",
                "peers": [
                    iroh_peer(other_alive_hex, "Direct"),
                    iroh_peer(dead_peer_hex, dead_conn),
                ],
                "metrics": [],
                "connection_cache": [],
                "api_gaps": [],
                "scraped_at_ms": wall_ms
            },
            "probes": {
                "probes": [
                    {
                        "target": "collector-udp-echo",
                        "kind": "udp_echo",
                        "resolved_addr": "127.0.0.1:9081",
                        "last_attempted_at_ms": wall_ms,
                        "last_outcome": "ok",
                        "last_rtt_ms": if pre_incident { 5u64 } else { 7u64 },
                        "attempts": (if pre_incident { 1u64 } else { 3u64 }),
                        "successes": (if pre_incident { 1u64 } else { 3u64 }),
                    }
                ],
                "scraped_at_ms": wall_ms
            },
        }
    })
}

fn stage_snapshot(
    run_id: &str,
    self_hex: &str,
    role: &str,
    stage_index: u32,
    snap_seq: u64,
    wall_ms: u64,
) -> Value {
    let (peer_a, peer_b) = if self_hex == STAGE0_HEX {
        (ORCH_HEX, STAGE1_HEX)
    } else {
        (ORCH_HEX, STAGE0_HEX)
    };
    json!({
        "identity": identity_value(self_hex, role, Some(stage_index), Some(2)),
        "run_id": run_id,
        "snapshot_id": format!("stage-{stage_index}-{snap_seq}"),
        "wall_ms": wall_ms,
        "monotonic_seq": (snap_seq * 100u64),
        "trigger": {"kind": "Periodic"},
        "body": {
            "reachability": [
                reach_entry(peer_a, "Alive"),
                reach_entry(peer_b, "Alive"),
            ],
            "events": [],
            "iroh": {
                "home_relay_url": "https://relay.example.test/",
                "peers": [
                    iroh_peer(peer_a, "Direct"),
                    iroh_peer(peer_b, "Direct"),
                ],
                "metrics": [],
                "connection_cache": [],
                "api_gaps": [],
                "scraped_at_ms": wall_ms
            }
        }
    })
}

fn reach_entry(peer_hex: &str, opinion: &str) -> Value {
    json!({
        "peer_node_id_hex": peer_hex,
        "last_inbound_packet_at_ms": null,
        "last_inbound_via": null,
        "last_outbound_success_at_ms": null,
        "last_outbound_via": null,
        "last_dial_started_at_ms": null,
        "last_dial_outcome": null,
        "last_dial_duration_ms": null,
        "current_swim_opinion": opinion,
        "current_swim_opinion_since_ms": null,
        "swim_transition_history": [],
        "metadata_version_seen": null,
        "metadata_relay_url_seen": null,
    })
}

fn iroh_peer(peer_hex: &str, conn_type: &str) -> Value {
    json!({
        "peer_node_id_hex": peer_hex,
        "conn_type": conn_type,
        "direct_addresses": [],
        "relay_urls": [],
    })
}

fn event(seq: &mut u64, node_hex: &str, wall_ms: u64, ty: &str, mut payload: Value) -> Value {
    *seq += 1;
    let header = json!({
        "node_id": node_id_array(node_hex),
        "monotonic_seq": *seq,
        "wall_ms": wall_ms,
        "type": ty,
    });
    if let (Value::Object(mut h), Value::Object(p)) = (header, payload.take_or_object()) {
        for (k, v) in p {
            h.insert(k, v);
        }
        Value::Object(h)
    } else {
        unreachable!("event payload must be an object")
    }
}

trait TakeOrObject {
    fn take_or_object(&mut self) -> Value;
}

impl TakeOrObject for Value {
    fn take_or_object(&mut self) -> Value {
        std::mem::replace(self, Value::Object(serde_json::Map::new()))
    }
}

// ---------- Test scaffolding ----------

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/diag-bundle-n3")
}

struct TestEnv {
    tmp_root: PathBuf,
    collector_root: PathBuf,
}

impl TestEnv {
    fn new(label: &str) -> Self {
        let pid = std::process::id();
        let nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let tmp_root = std::env::temp_dir().join(format!(
            "swactor-diag-postproc-{pid}-{label}-{nano}-{n}"
        ));
        let collector_root = tmp_root.join("collector");
        std::fs::create_dir_all(&collector_root).expect("tempdir");
        Self {
            tmp_root,
            collector_root,
        }
    }
}

impl Drop for TestEnv {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.tmp_root);
    }
}

