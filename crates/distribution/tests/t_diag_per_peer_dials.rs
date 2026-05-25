//! Spec §9 (per-peer dial rollup, gap 9).
//!
//! The post-processor's per-peer dial table accounts for every
//! `DialStarted` event in the bundle. When fewer `DialOutcome` events
//! were observed than `DialStarted`, the drift is attributed to a
//! specific peer in the `in_flight` column — the bundle reader can
//! immediately tell which peer's dials never completed without grepping
//! the event stream.

#![cfg(feature = "collector")]

use std::fs;

use distribution::diagnostics::Event;
use distribution::diagnostics::event::{DialOutcome as DialOutcomeKind, EventRecord};
use distribution::diagnostics::postproc::{Bundle, per_peer_dial_rollup, render_summary};
use distribution::types::NodeId;

/// Replay of the 2026-05-25 drift: 83 starts, 80 outcomes, the 3-event
/// gap belonging entirely to one peer.
#[test]
fn dial_rollup_accounts_for_every_started_and_attributes_drift_to_peer() {
    let tmp = tempdir();
    let path = build_incident_bundle(tmp.path());
    let bundle = Bundle::parse_path(&path).expect("bundle parse");

    // Sanity: the raw event totals match the postmortem.
    let mut started: u64 = 0;
    let mut outcomes: u64 = 0;
    for node in bundle.nodes.values() {
        for rec in &node.events {
            match &rec.event {
                Event::DialStarted { .. } => started += 1,
                Event::DialOutcome { .. } => outcomes += 1,
                _ => {}
            }
        }
    }
    assert_eq!(started, 83, "fixture should match the postmortem's 83 starts");
    assert_eq!(outcomes, 80, "fixture should match the postmortem's 80 outcomes");

    let rollups = per_peer_dial_rollup(&bundle);
    let total_started: u64 = rollups.iter().map(|r| r.started).sum();
    let total_outcomes: u64 = rollups.iter().map(|r| r.succeeded + r.failed).sum();
    let total_in_flight: u64 = rollups.iter().map(|r| r.in_flight()).sum();
    assert_eq!(
        total_started, 83,
        "rollup must account for every DialStarted (spec §9 acceptance)",
    );
    assert_eq!(
        total_outcomes, 80,
        "rollup succeeded+failed must equal observed DialOutcome count",
    );
    assert_eq!(
        total_in_flight, 3,
        "the 3-event drift must surface as in-flight",
    );

    let stage2 = rollups
        .iter()
        .find(|r| r.peer_label == "stage-2")
        .expect("stage-2 must appear in the rollup");
    assert_eq!(
        stage2.in_flight(),
        3,
        "stage-2 owns all 3 unfinished dials (which peer never completed); got {stage2:?}",
    );

    let md = render_summary(&bundle);
    assert!(
        md.contains("## Per-peer dials"),
        "summary must contain the per-peer dials section; got:\n{md}",
    );
    assert!(
        md.contains("stage-2"),
        "summary must call out the peer with drift; got:\n{md}",
    );
    assert!(
        md.contains("started=83"),
        "summary totals line must mention started=83; got:\n{md}",
    );
}

/// Build a minimal bundle on disk modelling the 2026-05-25 incident.
fn build_incident_bundle(dir: &std::path::Path) -> std::path::PathBuf {
    let observer_hex = "aa".repeat(32);
    let peer_a_hex = "bb".repeat(32);
    let peer_b_hex = "cc".repeat(32);
    let peer_c_hex = "dd".repeat(32);
    let run_id = "run-dial-rollup";

    let observer_id = node_id_from_hex(&observer_hex);
    let peer_a = node_id_from_hex(&peer_a_hex);
    let peer_b = node_id_from_hex(&peer_b_hex);
    let peer_c = node_id_from_hex(&peer_c_hex);

    // Emit shape:
    //   stage-0: 30 starts, 28 ok, 2 timeouts → 0 in-flight
    //   stage-1: 30 starts, 26 ok, 4 timeouts → 0 in-flight
    //   stage-2: 23 starts, 17 ok,  3 timeouts → 3 in-flight (never completed)
    let mut events: Vec<EventRecord> = Vec::new();
    let mut seq: u64 = 0;
    let push_start = |events: &mut Vec<EventRecord>, seq: &mut u64, peer: NodeId| {
        *seq += 1;
        events.push(EventRecord {
            node_id: observer_id,
            monotonic_seq: *seq,
            wall_ms: *seq,
            event: Event::DialStarted {
                peer,
                attempt: 1,
                timeout_ms: 500,
            },
        });
    };
    let push_outcome =
        |events: &mut Vec<EventRecord>, seq: &mut u64, peer: NodeId, success: bool| {
            *seq += 1;
            events.push(EventRecord {
                node_id: observer_id,
                monotonic_seq: *seq,
                wall_ms: *seq,
                event: Event::DialOutcome {
                    peer,
                    attempt: 1,
                    outcome: if success {
                        DialOutcomeKind::Success
                    } else {
                        DialOutcomeKind::Timeout
                    },
                    duration_ms: 5,
                },
            });
        };

    let plan: &[(NodeId, u64, u64, u64)] = &[
        (peer_a, 30, 28, 2),
        (peer_b, 30, 26, 4),
        (peer_c, 23, 17, 3),
    ];
    for &(peer, starts, oks, fails) in plan {
        for _ in 0..starts {
            push_start(&mut events, &mut seq, peer);
        }
        for _ in 0..oks {
            push_outcome(&mut events, &mut seq, peer, true);
        }
        for _ in 0..fails {
            push_outcome(&mut events, &mut seq, peer, false);
        }
    }

    let events_bytes = serde_json::to_vec_pretty(&events).unwrap();

    let tarball = dir.join(format!("{run_id}.tar.gz"));
    let f = std::fs::File::create(&tarball).unwrap();
    let gz = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);

    let manifest = serde_json::json!({
        "run_id": run_id,
        "run_start_collector_ms": 1,
        "run_end_collector_ms": 1000,
        "finalize_received": true,
        "nodes": [
            { "node_id_hex": observer_hex, "label": "orchestrator", "role": "orchestrator", "boot_recorded": true, "event_batches": 1, "snapshots": 0, "finalize_recorded": true },
            { "node_id_hex": peer_a_hex,   "label": "stage-0",      "role": "stage", "stage_index": 0, "boot_recorded": true, "event_batches": 0, "snapshots": 0, "finalize_recorded": false },
            { "node_id_hex": peer_b_hex,   "label": "stage-1",      "role": "stage", "stage_index": 1, "boot_recorded": true, "event_batches": 0, "snapshots": 0, "finalize_recorded": false },
            { "node_id_hex": peer_c_hex,   "label": "stage-2",      "role": "stage", "stage_index": 2, "boot_recorded": true, "event_batches": 0, "snapshots": 0, "finalize_recorded": false },
        ],
    });
    append_bytes(
        &mut tar,
        &format!("{run_id}/MANIFEST.json"),
        &serde_json::to_vec_pretty(&manifest).unwrap(),
    );

    // Minimal boot.json per node (the parser tolerates missing fields
    // via `#[serde(default)]`).
    let boot = |hex: &str, role: &str, stage_index: Option<u32>| {
        serde_json::json!({
            "node_id_hex": hex,
            "node_id_short": &hex[..8],
            "role": role,
            "stage_index": stage_index,
            "stage_count": stage_index.map(|_| 3u32),
            "run_id": run_id,
            "process_start_unix_ms": 1,
            "boot_sequence": 0,
        })
    };
    for (label, hex, role, sx) in [
        ("orchestrator", &observer_hex, "orchestrator", None),
        ("stage-0", &peer_a_hex, "stage", Some(0u32)),
        ("stage-1", &peer_b_hex, "stage", Some(1u32)),
        ("stage-2", &peer_c_hex, "stage", Some(2u32)),
    ] {
        append_bytes(
            &mut tar,
            &format!("{run_id}/{label}/boot.json"),
            &serde_json::to_vec_pretty(&boot(hex, role, sx)).unwrap(),
        );
    }

    append_bytes(
        &mut tar,
        &format!("{run_id}/orchestrator/events/events-000001.json"),
        &events_bytes,
    );

    tar.finish().unwrap();
    tarball
}

fn append_bytes(
    tar: &mut tar::Builder<flate2::write::GzEncoder<std::fs::File>>,
    dst: &str,
    bytes: &[u8],
) {
    let mut header = tar::Header::new_gnu();
    header.set_size(bytes.len() as u64);
    header.set_mode(0o644);
    header.set_mtime(0);
    header.set_entry_type(tar::EntryType::Regular);
    header.set_cksum();
    tar.append_data(&mut header, dst, bytes).unwrap();
}

fn node_id_from_hex(hex: &str) -> NodeId {
    let mut out = [0u8; 32];
    for (i, pair) in hex.as_bytes().chunks_exact(2).enumerate() {
        let hi = hex_val(pair[0]);
        let lo = hex_val(pair[1]);
        out[i] = (hi << 4) | lo;
    }
    NodeId(out)
}

fn hex_val(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

struct TempDir {
    path: std::path::PathBuf,
}

impl TempDir {
    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.path);
    }
}

fn tempdir() -> TempDir {
    let mut path = std::env::temp_dir();
    let n: u32 = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| (d.as_nanos() as u32) ^ std::process::id())
        .unwrap_or(0);
    path.push(format!("swactor-dial-rollup-{n:x}"));
    fs::create_dir_all(&path).unwrap();
    TempDir { path }
}
