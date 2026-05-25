//! Visual replay viewer for diagnostics bundles.
//!
//! Loads a finalized deployment bundle (`vastai-N3-*.tar.gz`), an
//! uncompressed collector spool dir (`vastai-N3-*/`), or a simulation
//! bundle dir (`manifest.json` + `events.ndjson` + `snapshots/...`),
//! normalizes both formats into a single in-memory event timeline, and
//! serves a one-page HTML viewer on localhost.
//!
//! Run:
//!   cargo run --example replay_viewer -p dashboard \
//!     --features replay-viewer -- <path-to-bundle>

use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use serde::Serialize;
use serde_json::Value;

use distribution::diagnostics::postproc::Bundle;

// ─── wire types ────────────────────────────────────────────────────────────

#[derive(Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
enum SourceKind {
    Deployment,
    Sim,
}

#[derive(Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum Severity {
    Info,
    Notable,
    Error,
}

#[derive(Serialize)]
struct UnifiedEvent {
    t_ms: f64,
    node_label: String,
    kind: String,
    severity: Severity,
    fields: Value,
}

#[derive(Serialize)]
struct NodeInfo {
    label: String,
    role: Option<String>,
    color: String,
}

#[derive(Serialize)]
struct KindInfo {
    kind: String,
    count: u64,
    color: String,
}

#[derive(Serialize)]
struct SnapshotInfo {
    t_ms: f64,
    node_label: String,
    summary: String,
}

#[derive(Serialize)]
struct BundleView {
    run_id: String,
    source_kind: SourceKind,
    t_start_ms: f64,
    t_end_ms: f64,
    nodes: Vec<NodeInfo>,
    event_kinds: Vec<KindInfo>,
    events: Vec<UnifiedEvent>,
    snapshots: Vec<SnapshotInfo>,
}

// ─── format detection ──────────────────────────────────────────────────────

enum DetectedFormat {
    DeploymentTar(PathBuf),
    DeploymentDir(PathBuf),
    Sim(PathBuf),
}

fn detect_format(path: &Path) -> Result<DetectedFormat> {
    let md = fs::metadata(path)
        .with_context(|| format!("stat {}", path.display()))?;
    if md.is_file() {
        let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
        if name.ends_with(".tar.gz") || name.ends_with(".tgz") {
            return Ok(DetectedFormat::DeploymentTar(path.to_path_buf()));
        }
        bail!("unrecognized file: {} (expected .tar.gz)", path.display());
    }
    if path.join("MANIFEST.json").is_file() {
        return Ok(DetectedFormat::DeploymentDir(path.to_path_buf()));
    }
    if path.join("manifest.json").is_file() && path.join("events.ndjson").is_file() {
        return Ok(DetectedFormat::Sim(path.to_path_buf()));
    }
    bail!(
        "could not classify {} — expected .tar.gz, dir with MANIFEST.json, or sim dir with manifest.json+events.ndjson",
        path.display()
    )
}

// ─── deployment loaders ────────────────────────────────────────────────────

fn load_deployment_tar(path: &Path) -> Result<BundleView> {
    let bundle = Bundle::parse_path(path)
        .map_err(|e| anyhow!("parse {}: {e}", path.display()))?;
    let nodes_meta: Vec<(String, Option<String>)> = bundle
        .manifest
        .nodes
        .iter()
        .map(|n| (n.label.clone(), n.role.clone()))
        .collect();

    let mut events: Vec<UnifiedEvent> = Vec::new();
    let mut snapshots: Vec<SnapshotInfo> = Vec::new();
    let mut t0: u64 = bundle.manifest.run_start_collector_ms.unwrap_or(u64::MAX);
    for node in bundle.nodes.values() {
        for ev in &node.events {
            if ev.wall_ms < t0 {
                t0 = ev.wall_ms;
            }
        }
        for snap in &node.snapshots {
            if snap.wall_ms < t0 {
                t0 = snap.wall_ms;
            }
        }
    }
    if t0 == u64::MAX {
        t0 = 0;
    }

    let mut t_end: f64 = 0.0;
    for node in bundle.nodes.values() {
        for ev in &node.events {
            let value = serde_json::to_value(&ev.event).unwrap_or(Value::Null);
            let kind = unified_kind_from_value(&value);
            let severity = severity_for(&kind, &value);
            let t_ms = (ev.wall_ms.saturating_sub(t0)) as f64;
            if t_ms > t_end {
                t_end = t_ms;
            }
            events.push(UnifiedEvent {
                t_ms,
                node_label: node.label.clone(),
                kind,
                severity,
                fields: value,
            });
        }
        for snap in &node.snapshots {
            let t_ms = (snap.wall_ms.saturating_sub(t0)) as f64;
            if t_ms > t_end {
                t_end = t_ms;
            }
            let body = serde_json::to_value(&snap.body).unwrap_or(Value::Null);
            snapshots.push(SnapshotInfo {
                t_ms,
                node_label: node.label.clone(),
                summary: summarize_snapshot(&body),
            });
        }
    }
    events.sort_by(|a, b| a.t_ms.partial_cmp(&b.t_ms).unwrap_or(std::cmp::Ordering::Equal));
    snapshots.sort_by(|a, b| a.t_ms.partial_cmp(&b.t_ms).unwrap_or(std::cmp::Ordering::Equal));

    Ok(BundleView {
        run_id: bundle.run_id,
        source_kind: SourceKind::Deployment,
        t_start_ms: 0.0,
        t_end_ms: t_end,
        nodes: assign_node_colors(nodes_meta),
        event_kinds: tally_event_kinds(&events),
        events,
        snapshots,
    })
}

#[derive(serde::Deserialize)]
struct DirManifestNode {
    node_id_hex: String,
    label: String,
    #[serde(default)]
    role: Option<String>,
}

#[derive(serde::Deserialize)]
struct DirManifest {
    run_id: String,
    #[serde(default)]
    run_start_collector_ms: Option<u64>,
    #[serde(default)]
    nodes: Vec<DirManifestNode>,
}

fn load_deployment_dir(root: &Path) -> Result<BundleView> {
    let manifest_bytes = fs::read(root.join("MANIFEST.json"))
        .with_context(|| format!("read MANIFEST.json under {}", root.display()))?;
    let manifest: DirManifest = serde_json::from_slice(&manifest_bytes)
        .context("parse MANIFEST.json")?;

    let hex_to_label: BTreeMap<String, String> = manifest
        .nodes
        .iter()
        .map(|n| (n.node_id_hex.to_lowercase(), n.label.clone()))
        .collect();
    let nodes_meta: Vec<(String, Option<String>)> = manifest
        .nodes
        .iter()
        .map(|n| (n.label.clone(), n.role.clone()))
        .collect();

    let mut events: Vec<UnifiedEvent> = Vec::new();
    let mut snapshots: Vec<SnapshotInfo> = Vec::new();
    let mut t0: u64 = manifest.run_start_collector_ms.unwrap_or(u64::MAX);
    let mut raw_events: Vec<(String, Value)> = Vec::new();
    let mut raw_snapshots: Vec<(String, Value)> = Vec::new();

    for entry in fs::read_dir(root).with_context(|| format!("readdir {}", root.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let dir_name = entry.file_name().to_string_lossy().into_owned();
        let label = hex_to_label
            .get(&dir_name.to_lowercase())
            .cloned()
            .unwrap_or_else(|| {
                if dir_name.len() >= 8 {
                    format!("node-{}", &dir_name[..8])
                } else {
                    dir_name.clone()
                }
            });
        for f in fs::read_dir(entry.path())? {
            let f = f?;
            let fname = f.file_name().to_string_lossy().into_owned();
            if fname.starts_with("events-") && fname.ends_with(".json") {
                let bytes = fs::read(f.path())
                    .with_context(|| format!("read {}", f.path().display()))?;
                let batch: Value = serde_json::from_slice(&bytes)
                    .with_context(|| format!("parse {}", f.path().display()))?;
                if let Some(arr) = batch.as_array() {
                    for ev in arr {
                        if let Some(wall) = ev.get("wall_ms").and_then(Value::as_u64) {
                            if wall < t0 {
                                t0 = wall;
                            }
                        }
                        raw_events.push((label.clone(), ev.clone()));
                    }
                }
            } else if fname.starts_with("snapshot-") && fname.ends_with(".json") {
                let bytes = fs::read(f.path())?;
                let snap: Value = serde_json::from_slice(&bytes)
                    .with_context(|| format!("parse {}", f.path().display()))?;
                if let Some(wall) = snap.get("wall_ms").and_then(Value::as_u64) {
                    if wall < t0 {
                        t0 = wall;
                    }
                }
                raw_snapshots.push((label.clone(), snap));
            }
        }
    }
    if t0 == u64::MAX {
        t0 = 0;
    }

    let mut t_end: f64 = 0.0;
    for (label, ev) in raw_events {
        let wall = ev.get("wall_ms").and_then(Value::as_u64).unwrap_or(t0);
        let t_ms = wall.saturating_sub(t0) as f64;
        if t_ms > t_end {
            t_end = t_ms;
        }
        let kind = unified_kind_from_value(&ev);
        let severity = severity_for(&kind, &ev);
        events.push(UnifiedEvent {
            t_ms,
            node_label: label,
            kind,
            severity,
            fields: ev,
        });
    }
    for (label, snap) in raw_snapshots {
        let wall = snap.get("wall_ms").and_then(Value::as_u64).unwrap_or(t0);
        let t_ms = wall.saturating_sub(t0) as f64;
        if t_ms > t_end {
            t_end = t_ms;
        }
        let body = snap.get("body").cloned().unwrap_or(Value::Null);
        snapshots.push(SnapshotInfo {
            t_ms,
            node_label: label,
            summary: summarize_snapshot(&body),
        });
    }
    events.sort_by(|a, b| a.t_ms.partial_cmp(&b.t_ms).unwrap_or(std::cmp::Ordering::Equal));
    snapshots.sort_by(|a, b| a.t_ms.partial_cmp(&b.t_ms).unwrap_or(std::cmp::Ordering::Equal));

    Ok(BundleView {
        run_id: manifest.run_id,
        source_kind: SourceKind::Deployment,
        t_start_ms: 0.0,
        t_end_ms: t_end,
        nodes: assign_node_colors(nodes_meta),
        event_kinds: tally_event_kinds(&events),
        events,
        snapshots,
    })
}

// ─── sim loader ────────────────────────────────────────────────────────────

fn load_sim(dir: &Path) -> Result<BundleView> {
    let manifest_bytes = fs::read(dir.join("manifest.json"))
        .with_context(|| format!("read manifest.json under {}", dir.display()))?;
    let manifest: Value = serde_json::from_slice(&manifest_bytes).context("parse manifest.json")?;
    let run_id = manifest
        .get("scenario_name")
        .and_then(Value::as_str)
        .or_else(|| manifest.get("name").and_then(Value::as_str))
        .map(|s| s.to_string())
        .unwrap_or_else(|| dir.file_name().and_then(|s| s.to_str()).unwrap_or("sim").to_string());

    let f = fs::File::open(dir.join("events.ndjson"))
        .with_context(|| format!("open events.ndjson under {}", dir.display()))?;
    let reader = BufReader::new(f);

    let mut hosts: BTreeMap<String, ()> = BTreeMap::new();
    let mut events: Vec<UnifiedEvent> = Vec::new();
    let mut t_end: f64 = 0.0;

    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let t_ns = v.get("virtual_time_ns").and_then(Value::as_u64).unwrap_or(0);
        let host = v
            .get("host_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_string();
        let kind = v
            .get("kind_tag")
            .and_then(Value::as_str)
            .unwrap_or("event")
            .to_string();
        let payload = v.get("event").cloned().unwrap_or_else(|| v.clone());
        let severity = severity_for(&kind, &payload);
        let t_ms = (t_ns as f64) / 1.0e6;
        if t_ms > t_end {
            t_end = t_ms;
        }
        hosts.entry(host.clone()).or_insert(());
        events.push(UnifiedEvent {
            t_ms,
            node_label: host,
            kind,
            severity,
            fields: payload,
        });
    }
    events.sort_by(|a, b| a.t_ms.partial_cmp(&b.t_ms).unwrap_or(std::cmp::Ordering::Equal));

    let mut snapshots: Vec<SnapshotInfo> = Vec::new();
    let snap_root = dir.join("snapshots");
    if snap_root.is_dir() {
        for host_entry in fs::read_dir(&snap_root)? {
            let host_entry = host_entry?;
            if !host_entry.file_type()?.is_dir() {
                continue;
            }
            let host = host_entry.file_name().to_string_lossy().into_owned();
            for f in fs::read_dir(host_entry.path())? {
                let f = f?;
                let bytes = fs::read(f.path())?;
                let snap: Value = match serde_json::from_slice(&bytes) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                let t_ns = snap.get("virtual_time_ns").and_then(Value::as_u64).unwrap_or(0);
                let t_ms = (t_ns as f64) / 1.0e6;
                if t_ms > t_end {
                    t_end = t_ms;
                }
                snapshots.push(SnapshotInfo {
                    t_ms,
                    node_label: host.clone(),
                    summary: summarize_snapshot(&snap),
                });
            }
        }
        snapshots.sort_by(|a, b| a.t_ms.partial_cmp(&b.t_ms).unwrap_or(std::cmp::Ordering::Equal));
    }

    let nodes_meta: Vec<(String, Option<String>)> =
        hosts.into_keys().map(|h| (h, None)).collect();

    Ok(BundleView {
        run_id,
        source_kind: SourceKind::Sim,
        t_start_ms: 0.0,
        t_end_ms: t_end,
        nodes: assign_node_colors(nodes_meta),
        event_kinds: tally_event_kinds(&events),
        events,
        snapshots,
    })
}

// ─── helpers ───────────────────────────────────────────────────────────────

fn unified_kind_from_value(v: &Value) -> String {
    // Deployment event records use serde tag `type`; collector spool
    // files use a flat `kind` string. The `Custom` variant carries an
    // inner `kind` field we want to surface as `Custom:<inner>`.
    if let Some(tag) = v.get("type").and_then(Value::as_str) {
        if tag == "Custom" {
            if let Some(inner) = v.get("kind").and_then(Value::as_str) {
                return format!("Custom:{inner}");
            }
        }
        return tag.to_string();
    }
    if let Some(k) = v.get("kind").and_then(Value::as_str) {
        return k.to_string();
    }
    "event".to_string()
}

fn severity_for(kind: &str, fields: &Value) -> Severity {
    if kind == "Error" || kind.starts_with("error") {
        return Severity::Error;
    }
    if kind == "DialOutcome" {
        if let Some(outcome) = fields.get("outcome") {
            let ok = outcome
                .as_str()
                .map(|s| s == "Success")
                .or_else(|| {
                    outcome
                        .as_object()
                        .map(|m| m.keys().next().map(|k| k == "Success").unwrap_or(false))
                })
                .unwrap_or(false);
            return if ok { Severity::Info } else { Severity::Error };
        }
    }
    if kind == "SwimTransition" {
        if fields.get("to").and_then(Value::as_str) == Some("Dead") {
            return Severity::Error;
        }
        return Severity::Notable;
    }
    if kind == "ConnectionCacheInvalidated"
        || kind == "RelayChanged"
        || kind == "IrohConnTypeChanged"
    {
        return Severity::Notable;
    }
    if kind.to_ascii_lowercase().contains("drop") {
        return Severity::Notable;
    }
    Severity::Info
}

fn summarize_snapshot(body: &Value) -> String {
    let reach = body
        .get("reachability")
        .and_then(Value::as_array)
        .map(|a| a.len())
        .unwrap_or(0);
    let tail = body
        .get("events")
        .and_then(Value::as_array)
        .map(|a| a.len())
        .unwrap_or(0);
    let mut parts = vec![format!("peers={reach}"), format!("tail={tail}")];
    if body.get("iroh").is_some() && !body.get("iroh").unwrap().is_null() {
        parts.push("iroh".into());
    }
    if body.get("swim").is_some() && !body.get("swim").unwrap().is_null() {
        parts.push("swim".into());
    }
    if body.get("host").is_some() && !body.get("host").unwrap().is_null() {
        parts.push("host".into());
    }
    parts.join(" ")
}

const NODE_PALETTE: &[&str] = &[
    "#4a90e2", "#f06292", "#81c784", "#ffb74d", "#ba68c8", "#4dd0e1", "#aed581", "#ff8a65",
];

fn assign_node_colors(meta: Vec<(String, Option<String>)>) -> Vec<NodeInfo> {
    meta.into_iter()
        .enumerate()
        .map(|(i, (label, role))| NodeInfo {
            label,
            role,
            color: NODE_PALETTE[i % NODE_PALETTE.len()].to_string(),
        })
        .collect()
}

const KIND_PALETTE: &[&str] = &[
    "#4a90e2", "#ffb74d", "#81c784", "#f06292", "#ba68c8", "#4dd0e1", "#aed581", "#ff8a65",
    "#e57373", "#9575cd", "#64b5f6", "#ffd54f",
];

fn tally_event_kinds(events: &[UnifiedEvent]) -> Vec<KindInfo> {
    let mut counts: BTreeMap<String, u64> = BTreeMap::new();
    for ev in events {
        *counts.entry(ev.kind.clone()).or_insert(0) += 1;
    }
    let mut pairs: Vec<(String, u64)> = counts.into_iter().collect();
    pairs.sort_by(|a, b| b.1.cmp(&a.1));
    pairs
        .into_iter()
        .enumerate()
        .map(|(i, (kind, count))| KindInfo {
            kind,
            count,
            color: KIND_PALETTE[i % KIND_PALETTE.len()].to_string(),
        })
        .collect()
}

// ─── routes ────────────────────────────────────────────────────────────────

async fn get_index() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn get_bundle(State(view): State<Arc<String>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "application/json")],
        view.as_str().to_owned(),
    )
}

const INDEX_HTML: &str = include_str!("replay_viewer.html");

// ─── main ──────────────────────────────────────────────────────────────────

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let path = std::env::args().nth(1).ok_or_else(|| {
        anyhow!("usage: replay_viewer <path-to-bundle.tar.gz | bundle-dir | sim-dir>")
    })?;
    let path = PathBuf::from(path);

    let view = match detect_format(&path)? {
        DetectedFormat::DeploymentTar(p) => load_deployment_tar(&p)?,
        DetectedFormat::DeploymentDir(p) => load_deployment_dir(&p)?,
        DetectedFormat::Sim(p) => load_sim(&p)?,
    };

    let n_events = view.events.len();
    let n_nodes = view.nodes.len();
    let span_s = view.t_end_ms / 1000.0;
    let run_id = view.run_id.clone();
    let payload = Arc::new(serde_json::to_string(&view).context("serialize bundle view")?);

    let app = Router::new()
        .route("/", get(get_index))
        .route("/api/bundle", get(get_bundle))
        .with_state(payload);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .context("bind 127.0.0.1:0")?;
    let addr: SocketAddr = listener.local_addr()?;

    println!(
        "replay-viewer: run={run_id} nodes={n_nodes} events={n_events} span={span_s:.1}s"
    );
    println!("  open: http://{addr}");

    axum::serve(listener, app).await.context("axum serve")?;
    Ok(())
}
