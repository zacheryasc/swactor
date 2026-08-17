//! Demo-only Fleet Control view (`demo-control` feature).
//!
//! The merged control surface: one row per provisioned node fusing the
//! process table (`proc.<node>.lifecycle`, `node.status`) with the
//! provisioning reconciler's stage snapshot (fetched client-side from
//! `/api/view/reconciler` when that view is registered), plus provision and
//! kill actions. Inert in regular builds — this module compiles only under
//! `demo-control`.

use std::collections::BTreeMap;
use std::time::Instant;

use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;
use telemetry::frame::{Frame, StreamId};

use crate::view::DashboardView;
use crate::FrameEvent;

#[derive(Clone, Serialize)]
struct ProcessEntry {
    node: String,
    pid: Option<u32>,
    state: String,
    seen_ms_ago: u64,
}

#[derive(Serialize)]
struct ControlSnapshot {
    nodes: Vec<ProcessEntry>,
}

#[derive(Default)]
struct ProcessState {
    pid: Option<u32>,
    state: String,
    seen: Option<Instant>,
}

#[derive(Default)]
pub struct DemoControlView {
    processes: Mutex<BTreeMap<String, ProcessState>>,
}

fn is_lifecycle_channel(channel: &str) -> bool {
    channel.starts_with("proc.") && channel.ends_with(".lifecycle")
}

impl DashboardView for DemoControlView {
    fn id(&self) -> &'static str {
        "demo-control"
    }

    fn title(&self) -> &'static str {
        "Fleet Control"
    }

    fn path(&self) -> &'static str {
        "demo-control"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[]
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, event: &FrameEvent) {
        let Ok(payload) = serde_json::from_slice::<Value>(&event.payload) else {
            return;
        };
        if event.channel == "myelin.provisioning.events" {
            let provision = payload.get("event").unwrap_or(&payload);
            let Some(run_id) = provision.get("run_id").and_then(Value::as_u64) else {
                return;
            };
            let Some(node_id) = provision.get("node_id").and_then(Value::as_u64) else {
                return;
            };
            if node_id == 0 {
                return;
            }
            let node = format!("myelin-node-{run_id}-{node_id}");
            let mut processes = self.processes.lock();
            let entry = processes.entry(node).or_default();
            entry.seen = Some(Instant::now());
            entry.state = match provision.get("kind").and_then(Value::as_str) {
                Some("ProvisionStart") => "provisioning",
                Some("NodeLive") => "running",
                Some("ProvisionFailed") => "failed",
                Some("NodeStopped") => "exited",
                Some(other) => other,
                None => return,
            }
            .to_owned();
            return;
        }
        if !is_lifecycle_channel(&event.channel) && event.channel != "node.status" {
            return;
        }
        let node = event.stream.node.clone();
        let mut processes = self.processes.lock();
        let entry = processes.entry(node).or_default();
        entry.seen = Some(Instant::now());
        if let Some(pid) = payload.get("pid").and_then(Value::as_u64) {
            entry.pid = Some(pid as u32);
        }
        if let Some(kind) = payload.get("event").and_then(Value::as_str) {
            entry.state = match kind {
                "started" => "running".to_owned(),
                "exited" => "exited".to_owned(),
                "spawn_failed" | "error" => "failed".to_owned(),
                other => other.to_owned(),
            };
        }
    }

    fn snapshot_json(&self) -> Value {
        let now = Instant::now();
        let nodes: Vec<ProcessEntry> = self
            .processes
            .lock()
            .iter()
            .map(|(node, state)| ProcessEntry {
                node: node.clone(),
                pid: state.pid,
                state: state.state.clone(),
                seen_ms_ago: state
                    .seen
                    .map(|seen| now.duration_since(seen).as_millis() as u64)
                    .unwrap_or(u64::MAX),
            })
            .collect();
        serde_json::to_value(ControlSnapshot { nodes })
            .unwrap_or_else(|_| serde_json::json!({ "nodes": [] }))
    }

    fn html(&self) -> Option<&'static str> {
        Some(include_str!("demo_control_page.html"))
    }
}
