//! Spec §1 (relay observability, gap 1).
//!
//! After this work, the bundle answers, for every relay-mediated peer
//! connection that died during a run:
//!   - who initiated the close (relay / remote / idle_timeout),
//!   - what the close reason was,
//!   - how long the session had been open and how many bytes had crossed,
//!   - the relay's own counters (active, opens, closes, bytes, breakdown
//!     by close reason) at end-of-run.
//!
//! The post-processor's `## Relay sessions` section correlates the
//! relay's report with the node-side `connection_cache[peer].last_failure_reason`
//! that's already in the bundle, so the bundle reader can answer
//! "was this a relay-side eviction" without consulting any external
//! system. When the relay was not observed (legacy run or relay
//! observability not configured), the section explicitly names the
//! gap and points at it.

#![cfg(feature = "collector")]

use std::fs;
use std::sync::Arc;

use distribution::diagnostics::event::{Event, EventRecord};
use distribution::diagnostics::identity::Identity;
use distribution::diagnostics::postproc::{render_summary, Bundle};
use distribution::diagnostics::snapshot::{
    RelayServerIntrospector, Snapshot, SnapshotBody, SnapshotTrigger, Tier2ConnectionCache,
    Tier2IrohState, Tier3RelayServer,
};
use distribution::diagnostics::sink::{DynEmitter, EventEmitter, InMemorySink};
use distribution::diagnostics::{Aggregator, RelayObservability, Role};
use distribution::types::NodeId;

#[test]
fn relay_observability_records_aggregate_totals_and_emits_lifecycle_events() {
    let obs = Arc::new(RelayObservability::new());
    let id = Identity::new(NodeId([0x77; 32]), Role::custom("relay"), "run-relay1");
    let agg = Arc::new(Aggregator::new(id, InMemorySink::new()));
    let emitter: DynEmitter = agg.clone() as Arc<dyn EventEmitter + Send + Sync + 'static>;
    obs.set_emitter(emitter);
    agg.set_relay_server_introspector(obs.clone() as Arc<dyn RelayServerIntrospector>);

    obs.note_session_opened("aa".repeat(32), 100);
    obs.note_session_opened("bb".repeat(32), 200);
    obs.note_session_closed("aa".repeat(32), 100, 600, "relay", "idle_timeout", 1024, 4096);
    obs.note_session_closed("bb".repeat(32), 200, 700, "remote", "eof", 512, 256);

    let snap = agg.snapshot(SnapshotTrigger::Periodic);
    let rs = snap.body.relay_server.expect("relay_server snapshot present");
    assert_eq!(rs.active_sessions, 0);
    assert_eq!(rs.total_opens, 2);
    assert_eq!(rs.total_closes, 2);
    assert_eq!(rs.bytes_rx_total, 1024 + 512);
    assert_eq!(rs.bytes_tx_total, 4096 + 256);
    assert!(
        rs.closes_by_reason
            .iter()
            .any(|(k, v)| k == "idle_timeout" && *v == 1),
        "closes_by_reason must break down: {:?}",
        rs.closes_by_reason,
    );

    // Lifecycle events fired through the aggregator's sink.
    let records = agg.sink().records();
    let opens = records
        .iter()
        .filter(|r| matches!(r.event, Event::RelaySessionOpened { .. }))
        .count();
    let closes = records
        .iter()
        .filter(|r| matches!(r.event, Event::RelaySessionClosed { .. }))
        .count();
    assert_eq!(opens, 2);
    assert_eq!(closes, 2);
}

#[test]
fn postproc_relay_sessions_section_renders_gap_line_when_no_relay_present() {
    // Spec §1: "When the relay was not observed (legacy run, relay
    // observability not configured), the section renders one line
    // explaining that and pointing at this gap."
    let tmp = tempdir();
    let path = build_node_only_bundle(tmp.path());
    let bundle = Bundle::parse_path(&path).expect("parse bundle");
    let md = render_summary(&bundle);
    assert!(
        md.contains("## Relay sessions"),
        "relay-sessions section must always render; got:\n{md}",
    );
    assert!(
        md.contains("gap 1"),
        "absence path must name the gap explicitly; got:\n{md}",
    );
    assert!(
        md.contains("SWACTOR_DIAG_COLLECTOR_URL"),
        "absence path must point at how to enable; got:\n{md}",
    );
}

#[test]
fn postproc_relay_sessions_correlates_close_reason_with_node_cache() {
    // Spec §1 acceptance: a bundle reader sees who closed and why,
    // joined with the node-side last_failure_reason, in one place.
    let tmp = tempdir();
    let path = build_relay_plus_node_bundle(tmp.path());
    let bundle = Bundle::parse_path(&path).expect("parse bundle");
    let md = render_summary(&bundle);
    assert!(
        md.contains("## Relay sessions"),
        "relay sessions section must render; got:\n{md}",
    );
    assert!(
        md.contains("closed by relay"),
        "summary must name the close initiator; got:\n{md}",
    );
    assert!(
        md.contains("idle_timeout"),
        "summary must name the close reason; got:\n{md}",
    );
    assert!(
        md.contains("last_failure_reason=\"connection-closed\""),
        "summary must surface the node-side cache reason for correlation; got:\n{md}",
    );
    assert!(
        md.contains("relay relay-0"),
        "summary must mention the relay's bundle label; got:\n{md}",
    );
}

fn build_node_only_bundle(dir: &std::path::Path) -> std::path::PathBuf {
    let node_hex = "11".repeat(32);
    write_bundle(
        dir,
        "run-norelay",
        &[(
            "stage-0",
            node_hex.clone(),
            "stage",
            Some(0u32),
            Vec::new(),
            vec![simple_snapshot(&node_hex, "run-norelay", 100, None, None)],
        )],
    )
}

fn build_relay_plus_node_bundle(dir: &std::path::Path) -> std::path::PathBuf {
    let relay_hex = "ff".repeat(32);
    let node_hex = "22".repeat(32);

    // Node-side: cache shows last_failure_reason="connection-closed"
    // for the peer the relay observed (peer = the relay itself? No —
    // peer means the *other* iroh node behind the relay; here the
    // node is stage-2 and the relay sees stage-2's session). For the
    // test correlation we use the same hex on both sides so the
    // post-processor's join hits.
    let cache_entry = Tier2ConnectionCache {
        peer_node_id_hex: relay_hex.clone(),
        generation: 1,
        created_at_ms: Some(50),
        last_successful_send_at_ms: Some(150),
        last_failure_at_ms: Some(600),
        last_failure_reason: Some("connection-closed".into()),
        observed_conn_type_at_last_use: None,
    };
    let node_snap = simple_snapshot(
        &node_hex,
        "run-relay-correlation",
        700,
        Some(Tier2IrohState {
            connection_cache: vec![cache_entry],
            iroh_version: Some("0.98.2".into()),
            ..Tier2IrohState::default()
        }),
        None,
    );

    // Relay-side: one RelaySessionClosed event naming the same peer
    // hex + the snapshot's Tier3RelayServer totals.
    let relay_close_event = EventRecord {
        node_id: node_id_from_hex(&relay_hex),
        monotonic_seq: 1,
        wall_ms: 600,
        event: Event::RelaySessionClosed {
            peer_node_id_hex: relay_hex.clone(),
            opened_at_ms: 50,
            closed_at_ms: 600,
            duration_ms: 550,
            close_initiator: "relay".into(),
            close_reason: "idle_timeout".into(),
            bytes_rx: 4096,
            bytes_tx: 1024,
        },
    };
    let relay_snap = simple_snapshot(
        &relay_hex,
        "run-relay-correlation",
        650,
        None,
        Some(Tier3RelayServer {
            active_sessions: 0,
            total_opens: 1,
            total_closes: 1,
            bytes_rx_total: 4096,
            bytes_tx_total: 1024,
            closes_by_reason: vec![("idle_timeout".into(), 1)],
            scraped_at_ms: 650,
        }),
    );

    write_bundle(
        dir,
        "run-relay-correlation",
        &[
            (
                "stage-2",
                node_hex,
                "stage",
                Some(2u32),
                Vec::new(),
                vec![node_snap],
            ),
            (
                "relay-0",
                relay_hex,
                "relay",
                None,
                vec![relay_close_event],
                vec![relay_snap],
            ),
        ],
    )
}

fn simple_snapshot(
    node_hex: &str,
    run_id: &str,
    wall_ms: u64,
    iroh: Option<Tier2IrohState>,
    relay_server: Option<Tier3RelayServer>,
) -> Snapshot {
    let id = Identity::new(node_id_from_hex(node_hex), Role::stage(), run_id);
    Snapshot {
        identity: id.clone(),
        run_id: id.run_id.clone(),
        snapshot_id: format!("snap-{wall_ms}"),
        wall_ms,
        monotonic_seq: wall_ms,
        trigger: SnapshotTrigger::Periodic,
        body: SnapshotBody {
            iroh,
            relay_server,
            ..SnapshotBody::default()
        },
    }
}

type NodeEntry = (
    &'static str,
    String,
    &'static str,
    Option<u32>,
    Vec<EventRecord>,
    Vec<Snapshot>,
);

fn write_bundle(
    dir: &std::path::Path,
    run_id: &str,
    nodes: &[NodeEntry],
) -> std::path::PathBuf {
    let tarball = dir.join(format!("{run_id}.tar.gz"));
    let f = fs::File::create(&tarball).unwrap();
    let gz = flate2::write::GzEncoder::new(f, flate2::Compression::default());
    let mut tar = tar::Builder::new(gz);

    let manifest_nodes: Vec<_> = nodes
        .iter()
        .map(|(label, hex, role, sx, events, snaps)| {
            serde_json::json!({
                "node_id_hex": hex,
                "label": label,
                "role": role,
                "stage_index": sx,
                "boot_recorded": true,
                "event_batches": if events.is_empty() { 0 } else { 1 } as u64,
                "snapshots": snaps.len() as u64,
                "finalize_recorded": false,
            })
        })
        .collect();
    let manifest = serde_json::json!({
        "run_id": run_id,
        "run_start_collector_ms": 1,
        "run_end_collector_ms": 1000,
        "finalize_received": false,
        "nodes": manifest_nodes,
    });
    append_bytes(
        &mut tar,
        &format!("{run_id}/MANIFEST.json"),
        &serde_json::to_vec_pretty(&manifest).unwrap(),
    );

    for (label, hex, role, sx, events, snaps) in nodes {
        let boot = serde_json::json!({
            "node_id_hex": hex,
            "node_id_short": &hex[..8],
            "role": role,
            "stage_index": sx,
            "stage_count": sx.map(|_| 3u32),
            "run_id": run_id,
            "process_start_unix_ms": 1,
            "boot_sequence": 0,
        });
        append_bytes(
            &mut tar,
            &format!("{run_id}/{label}/boot.json"),
            &serde_json::to_vec_pretty(&boot).unwrap(),
        );
        if !events.is_empty() {
            append_bytes(
                &mut tar,
                &format!("{run_id}/{label}/events/events-000001.json"),
                &serde_json::to_vec_pretty(events).unwrap(),
            );
        }
        for (i, snap) in snaps.iter().enumerate() {
            append_bytes(
                &mut tar,
                &format!("{run_id}/{label}/snapshots/snapshot-{:06}.json", i + 1),
                &serde_json::to_vec_pretty(snap).unwrap(),
            );
        }
    }

    tar.finish().unwrap();
    tarball
}

fn append_bytes(
    tar: &mut tar::Builder<flate2::write::GzEncoder<fs::File>>,
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
    path.push(format!("swactor-relay-obs-{n:x}"));
    fs::create_dir_all(&path).unwrap();
    TempDir { path }
}
