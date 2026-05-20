//! Tier-1 end-to-end test for the diagnostics stack (S5 / T1.7 gate).
//!
//! Spins up an in-process collector and N=3 aggregator+HttpSink nodes
//! (one orchestrator, two stages) inside one tokio runtime. Drives a
//! realistic miniature run: each node boots, emits SWIM/dial/message
//! events about the other two, lets the periodic and local-transition
//! snapshot triggers fire, then the orchestrator finalizes. The
//! collector waits for stragglers, asks every node for a final
//! snapshot via the `snapshot_now` hint, and tars the bundle.
//!
//! What the test asserts (from `DIAGNOSTICS_PLAN.md` T1.2/T1.4/T1.7):
//!
//! - The bundle parses as a gzipped tar.
//! - `MANIFEST.json` is at `{run_id}/` with one entry per node and
//!   the orchestrator/stage-N labels in place.
//! - Each node directory contains `boot.json`, a non-empty
//!   `snapshots/` directory, and at least one `events/` file.
//! - The reachability log inside at least one snapshot per node
//!   names the other nodes (T1.2 — asymmetric routing observable).
//! - At least one snapshot per node uses the `OnDemand` trigger —
//!   the finalize-time snapshot_now hint actually reached the nodes
//!   (T1.4 pull-trigger).
//! - At least one snapshot per node uses the `Transition` trigger
//!   (T1.4 local-transition snapshot fired).

#![cfg(feature = "collector")]

use std::collections::BTreeSet;
use std::io::Read;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use distribution::diagnostics::aggregator::{PeriodicConfig, spawn_periodic_snapshots};
use distribution::diagnostics::collector::{CollectorState, Manifest, bind, serve};
use distribution::diagnostics::{
    Aggregator, Event, HttpSink, Identity, PeerState, Role, SinkConfig, SnapshotSignal,
};
use distribution::types::NodeId;
use flate2::read::GzDecoder;
use serde_json::Value;

const RUN_ID: &str = "run-t1-e2e";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_produce_a_parseable_tier_1_bundle() {
    let env = TestEnv::new();

    // Collector — short finalize wait so the test is fast but still
    // exercises the wait + drain window.
    let listener = bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("addr");
    let state = Arc::new(
        CollectorState::new(&env.collector_root)
            .with_finalize_wait(Duration::from_millis(600)),
    );
    let server = tokio::spawn({
        let state = Arc::clone(&state);
        async move {
            let _ = serve(listener, state).await;
        }
    });
    // Give the server a moment to start accepting.
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Build three nodes: orchestrator + stage-0 + stage-1. Each gets
    // its own deterministic NodeId, its own aggregator + HttpSink,
    // and its own periodic-snapshot task.
    let mut nodes = Vec::with_capacity(3);
    nodes.push(Node::start(&env, addr, NodeShape::orchestrator()).await);
    nodes.push(Node::start(&env, addr, NodeShape::stage(0, 2)).await);
    nodes.push(Node::start(&env, addr, NodeShape::stage(1, 2)).await);

    // Wait for every node's boot record to land on the collector
    // before emitting events. Otherwise the manifest's per-node
    // labels can race and a node shows up as `node-<short>` instead
    // of `orchestrator` / `stage-N`.
    wait_until(Duration::from_secs(5), || {
        nodes
            .iter()
            .all(|n| collector_has_boot(&env.collector_root, &n.identity.node_id_hex))
    })
    .await;

    // Emit a realistic burst on every node — dial, transition, send,
    // receive — referencing the *other* nodes' ids so the reachability
    // log gets populated. Local-transition snapshots fire inline on
    // every SwimTransition.
    for i in 0..nodes.len() {
        let me = nodes[i].identity.clone();
        for (j, other) in nodes.iter().enumerate() {
            if j == i {
                continue;
            }
            let peer_bytes = decode_hex(&other.identity.node_id_hex);
            let peer = NodeId(peer_bytes);
            nodes[i].aggregator.emit(Event::DialStarted {
                peer,
                attempt: 1,
                timeout_ms: 500,
            });
            nodes[i].aggregator.emit(Event::DialOutcome {
                peer,
                attempt: 1,
                outcome: distribution::diagnostics::DialOutcome::Success,
                duration_ms: 12,
            });
            nodes[i].aggregator.emit(Event::SwimTransition {
                peer,
                from: PeerState::Unknown,
                to: PeerState::Alive,
                reason: format!("joined-from-{}", me.node_id_short),
            });
            nodes[i].aggregator.emit(Event::MessageSent {
                peer,
                kind: "ping".into(),
                size: 32 + j as u32,
            });
            nodes[i].aggregator.emit(Event::MessageReceived {
                peer,
                kind: "pong".into(),
                size: 16 + j as u32,
            });
        }
    }

    // Wait until each node has flushed at least one events batch and
    // the local-transition snapshots have landed.
    wait_until(Duration::from_secs(5), || {
        nodes.iter().all(|n| {
            let dir = env.collector_root.join(RUN_ID).join(&n.identity.node_id_hex);
            count_files(&dir, "events-") >= 1 && count_files(&dir, "snapshot-") >= 1
        })
    })
    .await;

    // Orchestrator finalizes. The collector marks every reporter for
    // snapshot_now, waits its finalize window, then tars. Pp-smoke-run
    // is the orchestrator in production, so we mirror that wiring here.
    nodes[0]
        .aggregator
        .finalize(serde_json::json!({"exit_reason": "ok"}));

    // Wait for the bundle file to exist on disk.
    let bundle_path = env.collector_root.join("bundles").join(format!("{RUN_ID}.tar.gz"));
    wait_until(Duration::from_secs(10), || bundle_path.exists()).await;

    // Now shut everything down cleanly. Sinks first so the drainer
    // tasks stop posting; then the server.
    for node in nodes.drain(..) {
        node.shutdown().await;
    }
    server.abort();
    let _ = server.await;

    // --- Inspect the bundle ---
    let bundle_bytes = std::fs::read(&bundle_path).expect("read bundle");
    let entries = list_tar_entries(&bundle_bytes);
    let entry_str = entries.join("\n");
    assert!(
        entries.iter().any(|e| e == &format!("{RUN_ID}/MANIFEST.json")),
        "MANIFEST.json missing from bundle; entries:\n{entry_str}"
    );

    let manifest_bytes = read_tar_file(&bundle_bytes, &format!("{RUN_ID}/MANIFEST.json"));
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).expect("parse manifest");
    assert_eq!(manifest.run_id, RUN_ID);
    assert_eq!(manifest.nodes.len(), 3, "expected 3 nodes in manifest");
    assert!(manifest.finalize_received);
    let labels: BTreeSet<String> = manifest.nodes.iter().map(|n| n.label.clone()).collect();
    for required in ["orchestrator", "stage-0", "stage-1"] {
        assert!(
            labels.contains(required),
            "manifest missing label {required}; got {labels:?}"
        );
    }

    // Every node should have boot.json + at least one snapshot + at
    // least one events file in the tarball.
    for label in ["orchestrator", "stage-0", "stage-1"] {
        let boot_path = format!("{RUN_ID}/{label}/boot.json");
        assert!(
            entries.iter().any(|e| e == &boot_path),
            "missing {boot_path}; entries:\n{entry_str}"
        );
        assert!(
            entries
                .iter()
                .any(|e| e.starts_with(&format!("{RUN_ID}/{label}/snapshots/"))),
            "no snapshots/ entry for {label}; entries:\n{entry_str}"
        );
        assert!(
            entries
                .iter()
                .any(|e| e.starts_with(&format!("{RUN_ID}/{label}/events/"))),
            "no events/ entry for {label}; entries:\n{entry_str}"
        );
    }

    // T1.2: every node's reachability log must mention the other two
    // node hex ids (asymmetric routing analysis depends on this).
    let id_by_label: std::collections::HashMap<String, String> = manifest
        .nodes
        .iter()
        .map(|n| (n.label.clone(), n.node_id_hex.clone()))
        .collect();
    for label in ["orchestrator", "stage-0", "stage-1"] {
        let snapshots = collect_snapshots(&bundle_bytes, label);
        let mut peers_seen: BTreeSet<String> = BTreeSet::new();
        for snap in &snapshots {
            if let Some(body) = snap.get("body").and_then(|b| b.get("reachability"))
                && let Some(arr) = body.as_array()
            {
                for p in arr {
                    if let Some(hex) = p.get("peer_node_id_hex").and_then(|v| v.as_str()) {
                        peers_seen.insert(hex.to_string());
                    }
                }
            }
        }
        let expected_peers: BTreeSet<String> = id_by_label
            .iter()
            .filter(|(other, _)| other.as_str() != label)
            .map(|(_, hex)| hex.clone())
            .collect();
        assert!(
            expected_peers.is_subset(&peers_seen),
            "node {label} reachability log missing peers — expected {expected_peers:?}, saw {peers_seen:?}"
        );
    }

    // T1.4: each node should have at least one OnDemand snapshot
    // (proves the snapshot_now hint round-tripped) and one
    // Transition snapshot (proves the local-transition trigger
    // fired inline on the SwimTransition emits we drove above).
    for label in ["orchestrator", "stage-0", "stage-1"] {
        let snapshots = collect_snapshots(&bundle_bytes, label);
        let triggers: Vec<String> = snapshots
            .iter()
            .filter_map(|s| {
                s.get("trigger")
                    .and_then(|t| t.get("kind"))
                    .and_then(|k| k.as_str())
                    .map(str::to_string)
            })
            .collect();
        assert!(
            triggers.iter().any(|k| k == "OnDemand"),
            "node {label} never produced an OnDemand snapshot; triggers seen: {triggers:?}"
        );
        assert!(
            triggers.iter().any(|k| k == "Transition"),
            "node {label} never produced a Transition snapshot; triggers seen: {triggers:?}"
        );
    }
}

// ---------- Node + env plumbing ----------

#[derive(Clone)]
struct NodeShape {
    label: &'static str,
    role: Role,
    stage_index: Option<u32>,
    stage_count: Option<u32>,
    node_byte: u8,
}

impl NodeShape {
    fn orchestrator() -> Self {
        Self {
            label: "orchestrator",
            role: Role::orchestrator(),
            stage_index: None,
            stage_count: None,
            node_byte: 0x10,
        }
    }
    fn stage(index: u32, count: u32) -> Self {
        Self {
            label: if index == 0 { "stage-0" } else { "stage-1" },
            role: Role::stage(),
            stage_index: Some(index),
            stage_count: Some(count),
            node_byte: 0x20 + index as u8,
        }
    }
}

struct Node {
    identity: Identity,
    aggregator: Arc<Aggregator<HttpSink>>,
    snapshot_task: tokio::task::JoinHandle<()>,
}

impl Node {
    async fn start(env: &TestEnv, collector_addr: SocketAddr, shape: NodeShape) -> Self {
        let mut bytes = [0u8; 32];
        bytes.fill(shape.node_byte);
        let mut identity = Identity::new(NodeId(bytes), shape.role.clone(), RUN_ID);
        if let (Some(i), Some(c)) = (shape.stage_index, shape.stage_count) {
            identity = identity.with_stage(i, c);
        }
        let node_id_hex = identity.node_id_hex.clone();

        let signal = SnapshotSignal::new();
        let spool_dir = env.spool_dir.join(shape.label);
        std::fs::create_dir_all(&spool_dir).unwrap();
        let config = SinkConfig::new(
            format!("http://{collector_addr}"),
            RUN_ID,
            &node_id_hex,
            &spool_dir,
        )
        .with_batch_interval(Duration::from_millis(60))
        .with_retry_initial(Duration::from_millis(60))
        .with_retry_max(Duration::from_millis(400))
        .with_request_timeout(Duration::from_millis(800))
        .with_snapshot_signal(signal.clone());
        let sink = HttpSink::new(config).expect("http sink");
        let aggregator = Arc::new(Aggregator::new(identity.clone(), sink));
        let snapshot_task = spawn_periodic_snapshots(
            Arc::clone(&aggregator),
            PeriodicConfig::new(Duration::from_millis(400)),
            signal,
        );
        Self {
            identity,
            aggregator,
            snapshot_task,
        }
    }

    async fn shutdown(self) {
        self.snapshot_task.abort();
        let _ = self.snapshot_task.await;
        self.aggregator.sink().handle().shutdown().await;
    }
}

struct TestEnv {
    _tmp: TempDir,
    collector_root: PathBuf,
    spool_dir: PathBuf,
}

impl TestEnv {
    fn new() -> Self {
        let tmp = TempDir::new("t1");
        let root = tmp.path().to_path_buf();
        let collector_root = root.join("collector");
        let spool_dir = root.join("spool");
        std::fs::create_dir_all(&collector_root).unwrap();
        std::fs::create_dir_all(&spool_dir).unwrap();
        Self {
            _tmp: tmp,
            collector_root,
            spool_dir,
        }
    }
}

// ---------- helpers ----------

async fn wait_until<F: Fn() -> bool>(budget: Duration, predicate: F) {
    let deadline = Instant::now() + budget;
    loop {
        if predicate() {
            return;
        }
        if Instant::now() > deadline {
            panic!("predicate never became true within {budget:?}");
        }
        tokio::time::sleep(Duration::from_millis(40)).await;
    }
}

fn collector_has_boot(root: &Path, node_id_hex: &str) -> bool {
    let dir = root.join(RUN_ID).join(node_id_hex);
    count_files(&dir, "boot-") >= 1
}

fn count_files(dir: &Path, prefix: &str) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    rd.filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(prefix)
        })
        .count()
}

fn decode_hex(hex: &str) -> [u8; 32] {
    assert_eq!(hex.len(), 64);
    let mut out = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks_exact(2).enumerate() {
        let hi = nibble(chunk[0]);
        let lo = nibble(chunk[1]);
        out[i] = (hi << 4) | lo;
    }
    out
}

fn nibble(c: u8) -> u8 {
    match c {
        b'0'..=b'9' => c - b'0',
        b'a'..=b'f' => c - b'a' + 10,
        b'A'..=b'F' => c - b'A' + 10,
        _ => 0,
    }
}

fn list_tar_entries(gz_bytes: &[u8]) -> Vec<String> {
    let gz = GzDecoder::new(gz_bytes);
    let mut ar = tar::Archive::new(gz);
    let mut out = Vec::new();
    for entry in ar.entries().expect("tar entries") {
        let entry = entry.expect("tar entry");
        let path = entry.path().expect("tar path");
        let mut s = path.to_string_lossy().into_owned();
        if s.ends_with('/') {
            s.pop();
        }
        out.push(s);
    }
    out
}

fn read_tar_file(gz_bytes: &[u8], path: &str) -> Vec<u8> {
    let gz = GzDecoder::new(gz_bytes);
    let mut ar = tar::Archive::new(gz);
    for entry in ar.entries().expect("tar entries") {
        let mut entry = entry.expect("tar entry");
        let entry_path = entry.path().expect("tar path").to_string_lossy().into_owned();
        if entry_path == path {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).expect("read tar file");
            return buf;
        }
    }
    panic!("file {path} not found in tarball");
}

fn collect_snapshots(gz_bytes: &[u8], label: &str) -> Vec<Value> {
    let prefix = format!("{RUN_ID}/{label}/snapshots/");
    let gz = GzDecoder::new(gz_bytes);
    let mut ar = tar::Archive::new(gz);
    let mut out = Vec::new();
    for entry in ar.entries().expect("tar entries") {
        let mut entry = entry.expect("tar entry");
        let p = entry.path().expect("tar path").to_string_lossy().into_owned();
        if !p.starts_with(&prefix) || p.ends_with('/') {
            continue;
        }
        let mut buf = Vec::new();
        entry.read_to_end(&mut buf).expect("read snapshot");
        if let Ok(v) = serde_json::from_slice::<Value>(&buf) {
            out.push(v);
        }
    }
    out
}

// ---------- tempdir helper ----------

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new(label: &str) -> Self {
        let pid = std::process::id();
        let nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "swactor-diag-t1-e2e-{pid}-{label}-{nano}-{n}"
        ));
        std::fs::create_dir_all(&path).expect("tempdir");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
