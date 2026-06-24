//! MVP-system dashboard adapter.
//!
//! This module owns the MVP-specific view over `mvp.lifecycle` datastream
//! records. The generic `dashboard` crate remains only an HTTP/SSE/plugin host;
//! MVP semantics live here.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ::dashboard as dash;
use datastream::Record;
use datastream::frame::{Frame, Lifetime, NodeId, Position, StreamId};
use parking_lot::Mutex;
use serde_json::json;
use swactor::actor::ActorAddress;
use swactor::stats::{ActorInfo, RuntimeStats, WorkerInfo};

use crate::observability_surface as obs;
use crate::telemetry::{MVP_LIFECYCLE, MvpLifecycleRecord};

const RECENT_LIMIT: usize = 64;
const DEFAULT_PORT: u16 = 9090;
const MVP_PAGE: &str = r#"<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>MVP Dashboard</title>
<style>
body { margin: 0; font-family: Menlo, Consolas, monospace; background: #0f1117; color: #e0e0e0; font-size: 13px; }
.header { display: flex; align-items: center; justify-content: space-between; padding: 12px 20px; background: #161822; border-bottom: 1px solid #2a2d3e; }
a { color: #9ab; text-decoration: none; margin-right: 12px; }
.content { padding: 16px 20px; }
.cards { display: grid; grid-template-columns: repeat(5, minmax(120px, 1fr)); gap: 10px; margin-bottom: 16px; }
.card, .panel { background: #161822; border: 1px solid #2a2d3e; border-radius: 6px; padding: 12px; }
.value { font-size: 22px; font-weight: 700; color: #fff; }
.label { color: #888; font-size: 10px; text-transform: uppercase; margin-top: 3px; }
table { width: 100%; border-collapse: collapse; }
th, td { text-align: left; border-bottom: 1px solid #25283a; padding: 6px 8px; }
th { color: #888; font-size: 10px; text-transform: uppercase; }
.ok { color: #4caf50; } .warn { color: #ff9800; } .bad { color: #f44336; }
pre { white-space: pre-wrap; line-height: 1.5; margin: 0; max-height: 420px; overflow: auto; }
</style>
</head>
<body>
<div class="header"><div><strong>MVP Dashboard</strong> <a href="/">Overview</a><a href="/actors">Actors</a><a href="/plugin/mvp">MVP</a></div><span id="status">connecting</span></div>
<div class="content">
  <div class="cards">
    <div class="card"><div class="value" id="run">-</div><div class="label">run</div></div>
    <div class="card"><div class="value" id="state">-</div><div class="label">state</div></div>
    <div class="card"><div class="value" id="events">0</div><div class="label">events</div></div>
    <div class="card"><div class="value" id="completed">0</div><div class="label">completed</div></div>
    <div class="card"><div class="value" id="faulted">0</div><div class="label">faulted</div></div>
  </div>
  <div class="panel"><h3>Stages</h3><table><thead><tr><th>Stage</th><th>Status</th><th>Events</th><th>Last event</th></tr></thead><tbody id="stages"></tbody></table></div>
  <div class="panel" style="margin-top:16px"><h3>Recent lifecycle</h3><pre id="recent"></pre></div>
</div>
<script>
function cls(s) { return s === 'faulted' ? 'bad' : (s === 'ready' || s === 'completed' || s === 'torn_down' ? 'ok' : 'warn'); }
function render(m) {
  document.getElementById('status').textContent = 'live';
  document.getElementById('run').textContent = m.run_id ?? '-';
  document.getElementById('state').textContent = m.state || '-';
  document.getElementById('state').className = 'value ' + cls(m.state);
  document.getElementById('events').textContent = m.event_count || 0;
  document.getElementById('completed').textContent = m.completed_runs || 0;
  document.getElementById('faulted').textContent = m.faulted_runs || 0;
  const tbody = document.getElementById('stages'); tbody.innerHTML = '';
  Object.entries(m.stages || {}).forEach(([stage, s]) => {
    const tr = document.createElement('tr');
    tr.innerHTML = '<td>' + stage + '</td><td class="' + cls(s.status) + '">' + s.status + '</td><td>' + s.event_count + '</td><td>' + (s.last_event || '') + '</td>';
    tbody.appendChild(tr);
  });
  document.getElementById('recent').textContent = (m.recent || []).join('\n');
}
fetch('/api/plugin/mvp').then(r => r.json()).then(render).catch(() => {});
const es = new EventSource('/events');
es.addEventListener('mvp', e => { try { render(JSON.parse(e.data)); } catch (_) {} });
es.addEventListener('done', () => { document.getElementById('status').textContent = 'done'; es.close(); });
</script>
</body>
</html>"#;

/// Running MVP dashboard handle used by the local E2E observation mode.
pub struct MvpDashboard {
    handle: Arc<dash::DashboardHandle>,
    cache: Arc<Mutex<Option<String>>>,
    view: MvpView,
    stream: StreamId,
    next_position: u64,
    port: u16,
}

impl MvpDashboard {
    /// Start the dashboard using `MVP_DASHBOARD_PORT` or 9090.
    pub fn start_from_env() -> Result<Self, String> {
        let port = match std::env::var("MVP_DASHBOARD_PORT") {
            Ok(raw) => raw
                .trim()
                .parse::<u16>()
                .map_err(|e| format!("MVP_DASHBOARD_PORT: {e}"))?,
            Err(_) => DEFAULT_PORT,
        };
        let handle = Arc::new(dash::start_dashboard(dash::DashboardConfig {
            port,
            ..Default::default()
        }));
        let cache = Arc::new(Mutex::new(None));
        handle.register_plugin(Arc::new(MvpPlugin {
            cache: Arc::clone(&cache),
        }));
        handle.start_http_standalone();
        Ok(Self {
            handle,
            cache,
            view: MvpView::default(),
            stream: StreamId::new(NodeId::new("mvp-local-e2e"), Lifetime(now_secs())),
            next_position: 0,
            port,
        })
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    pub fn url(&self) -> String {
        format!("http://127.0.0.1:{}/", self.port)
    }

    /// Convenience path for local producers: encode the event as the MVP-owned
    /// datastream record, then ingest the frame through the same consumer path.
    pub fn record_event(&mut self, event: obs::Event) {
        let record = MvpLifecycleRecord::new(event);
        let frame = Frame::new(
            MvpLifecycleRecord::CHANNEL,
            Position(self.next_position),
            record.encode(),
        );
        self.next_position = self.next_position.saturating_add(1);
        let stream = self.stream.clone();
        self.ingest(&stream, &frame);
    }

    /// Fold a delivered datastream frame into the MVP dashboard view.
    pub fn ingest(&mut self, _stream: &StreamId, frame: &Frame) {
        if frame.channel.as_str() != MVP_LIFECYCLE {
            return;
        }
        let Ok(record) = MvpLifecycleRecord::decode(&frame.payload) else {
            return;
        };
        let is_warn = fault_event(&record.event);
        let line = format_event(&record.event);
        self.view.observe(&record.event);
        let json = self.view.json();
        *self.cache.lock() = Some(json);
        self.handle.push_activity(is_warn, line);
        self.handle.set_stats(self.view.runtime_stats());
    }

    pub fn shutdown(&self) {
        self.handle.shutdown();
    }
}

#[derive(Default)]
struct MvpView {
    run_id: Option<u64>,
    state: &'static str,
    event_count: u64,
    completed_runs: u64,
    faulted_runs: u64,
    torn_down_runs: u64,
    nodes: BTreeMap<u64, NodeView>,
    stages: BTreeMap<u32, StageView>,
    recent: VecDeque<String>,
}

#[derive(Default)]
struct NodeView {
    status: &'static str,
    event_count: u64,
    last_event: String,
}

#[derive(Default)]
struct StageView {
    status: &'static str,
    event_count: u64,
    last_event: String,
}

impl MvpView {
    fn observe(&mut self, event: &obs::Event) {
        self.event_count = self.event_count.saturating_add(1);
        if let Some(run_id) = run_id(event) {
            self.run_id = Some(run_id);
        }

        let kind = event.kind();
        match event {
            obs::Event::RunScoped { kind, .. } => self.observe_run(*kind),
            obs::Event::NodeScoped { node_id, kind, .. } => {
                let node = self.nodes.entry(node_id.0).or_default();
                node.event_count = node.event_count.saturating_add(1);
                node.last_event = format!("{kind:?}");
                node.status = match kind {
                    obs::EventKind::NodeStarted => "started",
                    obs::EventKind::NodeAvailable => "available",
                    _ => node.status,
                };
            }
            obs::Event::StageScoped {
                stage_index, kind, ..
            } => {
                let stage = self.stages.entry(stage_index.0).or_default();
                stage.event_count = stage.event_count.saturating_add(1);
                stage.last_event = format!("{kind:?}");
                stage.status = match kind {
                    obs::EventKind::StageProvisionStarted => "provisioning",
                    obs::EventKind::StageReady => "ready",
                    obs::EventKind::StopRunSent => "stopping",
                    obs::EventKind::StageStopped => "stopped",
                    obs::EventKind::StageFaulted => "faulted",
                    _ => stage.status,
                };
            }
            _ => {}
        }

        push_recent(&mut self.recent, format_event(event));
        if matches!(kind, obs::EventKind::RunCompleted) {
            self.completed_runs = self.completed_runs.saturating_add(1);
        }
        if matches!(kind, obs::EventKind::RunFaulted) {
            self.faulted_runs = self.faulted_runs.saturating_add(1);
        }
        if matches!(kind, obs::EventKind::RunTornDown) {
            self.torn_down_runs = self.torn_down_runs.saturating_add(1);
        }
    }

    fn observe_run(&mut self, kind: obs::EventKind) {
        self.state = match kind {
            obs::EventKind::PoolReady => "pool_ready",
            obs::EventKind::RunPlanned => "planned",
            obs::EventKind::ReadinessBarrierPassed => "ready",
            obs::EventKind::PromptInjected => "running",
            obs::EventKind::RunCompleted => "completed",
            obs::EventKind::RunFaulted => "faulted",
            obs::EventKind::RunTornDown => "torn_down",
            _ => self.state,
        };
    }

    fn json(&self) -> String {
        let nodes = self
            .nodes
            .iter()
            .map(|(id, node)| {
                (
                    id.to_string(),
                    json!({
                        "status": node.status,
                        "event_count": node.event_count,
                        "last_event": node.last_event,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        let stages = self
            .stages
            .iter()
            .map(|(idx, stage)| {
                (
                    idx.to_string(),
                    json!({
                        "status": stage.status,
                        "event_count": stage.event_count,
                        "last_event": stage.last_event,
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>();
        json!({
            "run_id": self.run_id,
            "state": self.state,
            "event_count": self.event_count,
            "completed_runs": self.completed_runs,
            "faulted_runs": self.faulted_runs,
            "torn_down_runs": self.torn_down_runs,
            "nodes": nodes,
            "stages": stages,
            "recent": self.recent.iter().cloned().collect::<Vec<_>>(),
        })
        .to_string()
    }

    fn runtime_stats(&self) -> RuntimeStats {
        let mut actor_details = Vec::new();
        actor_details.push(actor_info(
            0,
            "mvp-orchestrator",
            self.state,
            self.event_count,
        ));
        for (node_id, node) in &self.nodes {
            actor_details.push(actor_info(
                1 + (*node_id as usize % 64),
                &format!("mvp-node-{node_id}"),
                node.status,
                node.event_count,
            ));
        }
        for (stage_index, stage) in &self.stages {
            actor_details.push(actor_info(
                128 + *stage_index as usize,
                &format!("mvp-stage-{stage_index}"),
                stage.status,
                stage.event_count,
            ));
        }
        let actors = actor_details
            .iter()
            .map(|actor| (actor.address, actor.worker_id))
            .collect::<Vec<_>>();
        RuntimeStats {
            num_workers: 1,
            uptime_ms: self.event_count.saturating_mul(100),
            actors,
            workers: vec![WorkerInfo {
                id: 0,
                num_actors: actor_details.len(),
                mailbox_depth: 0,
                messages_processed: self.event_count,
                local_sends: self.event_count,
                cross_sends: 0,
                inbox_sends: 0,
                type_mismatches: 0,
                panics: self.faulted_runs,
                messages_dropped: 0,
                restarts: self.completed_runs.saturating_sub(1),
                stops: self.torn_down_runs,
            }],
            actor_details,
            tick_timings: Vec::new(),
        }
    }
}

struct MvpPlugin {
    cache: Arc<Mutex<Option<String>>>,
}

impl dash::plugin::DashboardPlugin for MvpPlugin {
    fn name(&self) -> &str {
        "mvp"
    }

    fn snapshot_json(&self) -> Option<String> {
        self.cache.lock().clone()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &std::collections::HashMap<String, String>,
        _body: &[u8],
    ) -> dash::plugin::PluginResponse {
        match (method, path) {
            ("GET", "" | "model" | "snapshot") => dash::plugin::PluginResponse::json(
                self.cache.lock().clone().unwrap_or_else(|| "{}".into()),
            ),
            _ => dash::plugin::PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(MVP_PAGE)
    }
}

fn actor_info(slot: usize, name: &str, status: &str, messages_processed: u64) -> ActorInfo {
    ActorInfo {
        address: actor_address(slot),
        worker_id: 0,
        mailbox_depth: 0,
        last_msg_type: Some(status.to_owned()),
        messages_processed,
        poisoned: status == "faulted",
        name: Some(name.to_owned()),
        message_type_counts: vec![(status.to_owned(), messages_processed)],
    }
}

fn actor_address(slot: usize) -> ActorAddress {
    let mut bytes = [0_u8; 32];
    bytes[..8].copy_from_slice(&(slot as u64).to_be_bytes());
    bytes[8..11].copy_from_slice(b"mvp");
    ActorAddress(bytes)
}

fn push_recent(recent: &mut VecDeque<String>, line: String) {
    if recent.len() >= RECENT_LIMIT {
        recent.pop_front();
    }
    recent.push_back(line);
}

fn run_id(event: &obs::Event) -> Option<u64> {
    match event {
        obs::Event::RunScoped { run_id, .. } | obs::Event::StageScoped { run_id, .. } => {
            Some(run_id.0)
        }
        _ => None,
    }
}

fn fault_event(event: &obs::Event) -> bool {
    matches!(
        event.kind(),
        obs::EventKind::RunFaulted | obs::EventKind::StageFaulted
    )
}

fn format_event(event: &obs::Event) -> String {
    match event {
        obs::Event::RunScoped {
            kind,
            run_id,
            reason,
            ..
        } => format!("run {} {kind:?}{}", run_id.0, reason_text(*reason)),
        obs::Event::NodeScoped { kind, node_id, .. } => format!("node {} {kind:?}", node_id.0),
        obs::Event::StageScoped {
            kind,
            run_id,
            stage_index,
            reason,
            ..
        } => format!(
            "run {} stage {} {kind:?}{}",
            run_id.0,
            stage_index.0,
            reason_text(*reason)
        ),
        obs::Event::EdgeScoped { kind, edge_id, .. } => format!("edge {} {kind:?}", edge_id.0),
        obs::Event::RingScoped { kind, ring_id, .. } => format!("ring {} {kind:?}", ring_id.0),
        obs::Event::ObjectScoped {
            kind,
            object_id,
            sequence,
            ..
        } => format!("object {} seq {} {kind:?}", object_id.0, sequence.0),
        obs::Event::StepScoped { kind, step_id, .. } => format!("step {} {kind:?}", step_id.0),
        obs::Event::WorkerScoped {
            kind,
            worker_generation,
            ..
        } => format!("worker generation {} {kind:?}", worker_generation.0),
    }
}

fn reason_text(reason: Option<obs::FaultReason>) -> String {
    reason
        .map(|reason| format!(" ({reason:?})"))
        .unwrap_or_default()
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
