//! Dashboard view over MVP cluster provisioning and lifecycle datastream records.

use std::collections::{BTreeMap, VecDeque};

use dashboard::FrameEvent;
use dashboard::view::DashboardView;
use datastream::Record;
use datastream::frame::{Frame, StreamId};
use serde::Serialize;
use serde_json::{Value, json};
use std::sync::RwLock;

use crate::observability_surface as obs;
use crate::provisioning::{ProvisionEventKind, ProvisionLogStream};
use crate::telemetry::{
    MVP_LIFECYCLE, MVP_PROVISIONING_EVENTS, MVP_PROVISIONING_LOGS, MvpLifecycleRecord,
    MvpProvisionEventRecord, MvpProvisionLogRecord,
};

const CHANNELS: &[&str] = &[];
const PROVISIONING_LOG_PREFIX: &str = "mvp.provisioning.logs.node.";
const EVENT_LOG_CAP: usize = 256;
const LOG_TAIL_CAP: usize = 128;

#[derive(Default)]
pub struct MvpClusterDashboardView {
    state: RwLock<MvpClusterDashboardState>,
}

impl MvpClusterDashboardView {
    pub fn new() -> Self {
        Self::default()
    }
}

#[derive(Default, Serialize)]
struct MvpClusterDashboardState {
    events: VecDeque<DashboardEventEntry>,
    nodes: BTreeMap<u64, ProvisionNodeView>,
}

#[derive(Clone, Serialize)]
struct DashboardEventEntry {
    channel: String,
    position: u64,
    label: String,
    run_id: Option<u64>,
    node_id: Option<u64>,
}

#[derive(Default, Serialize)]
struct ProvisionNodeView {
    node_id: u64,
    phase: String,
    last_message: Option<String>,
    stdout_tail: VecDeque<String>,
    stderr_tail: VecDeque<String>,
    provider_tail: VecDeque<String>,
}

impl MvpClusterDashboardState {
    fn push_event(&mut self, entry: DashboardEventEntry) {
        if self.events.len() == EVENT_LOG_CAP {
            self.events.pop_front();
        }
        self.events.push_back(entry);
    }

    fn apply_provision_event(&mut self, frame: &Frame, record: MvpProvisionEventRecord) {
        let phase = provision_kind_label(record.event.kind);
        let node = self
            .nodes
            .entry(record.event.node_id)
            .or_insert_with(|| ProvisionNodeView {
                node_id: record.event.node_id,
                ..ProvisionNodeView::default()
            });
        node.phase = phase.to_owned();
        node.last_message = record.event.message.clone();
        self.push_event(DashboardEventEntry {
            channel: MVP_PROVISIONING_EVENTS.to_owned(),
            position: frame.position.0,
            label: phase.to_owned(),
            run_id: Some(record.event.run_id),
            node_id: Some(record.event.node_id),
        });
    }

    fn apply_provision_log(&mut self, record: MvpProvisionLogRecord) {
        let node = self
            .nodes
            .entry(record.line.node_id)
            .or_insert_with(|| ProvisionNodeView {
                node_id: record.line.node_id,
                ..ProvisionNodeView::default()
            });
        let tail = match record.line.stream {
            ProvisionLogStream::Stdout => &mut node.stdout_tail,
            ProvisionLogStream::Stderr => &mut node.stderr_tail,
            ProvisionLogStream::Provider => &mut node.provider_tail,
        };
        if tail.len() == LOG_TAIL_CAP {
            tail.pop_front();
        }
        tail.push_back(record.line.line);
    }

    fn apply_lifecycle(&mut self, frame: &Frame, record: MvpLifecycleRecord) {
        let (run_id, node_id) = lifecycle_ids(&record.event);
        self.push_event(DashboardEventEntry {
            channel: MVP_LIFECYCLE.to_owned(),
            position: frame.position.0,
            label: format!("{:?}", record.event.kind()),
            run_id,
            node_id,
        });
    }
}

impl DashboardView for MvpClusterDashboardView {
    fn id(&self) -> &'static str {
        "mvp/cluster"
    }

    fn title(&self) -> &'static str {
        "MVP cluster"
    }

    fn channels(&self) -> &'static [&'static str] {
        CHANNELS
    }

    fn ingest(&self, _stream: &StreamId, frame: &Frame, _event: &FrameEvent) {
        let mut state = self.state.write().expect("MVP dashboard state poisoned");
        match frame.channel.as_str() {
            MVP_PROVISIONING_EVENTS => {
                if let Ok(record) = MvpProvisionEventRecord::decode(&frame.payload) {
                    state.apply_provision_event(frame, record);
                }
            }
            channel
                if channel == MVP_PROVISIONING_LOGS
                    || channel.starts_with(PROVISIONING_LOG_PREFIX) =>
            {
                if let Ok(record) = MvpProvisionLogRecord::decode(&frame.payload) {
                    state.apply_provision_log(record);
                }
            }
            MVP_LIFECYCLE => {
                if let Ok(record) = MvpLifecycleRecord::decode(&frame.payload) {
                    state.apply_lifecycle(frame, record);
                }
            }
            _ => {}
        }
    }

    fn snapshot_json(&self) -> Value {
        let state = self.state.read().expect("MVP dashboard state poisoned");
        json!({
            "events": state.events,
            "nodes": state.nodes.values().collect::<Vec<_>>(),
        })
    }

    fn html(&self) -> Option<&'static str> {
        Some(MVP_CLUSTER_HTML)
    }
}

fn provision_kind_label(kind: ProvisionEventKind) -> &'static str {
    match kind {
        ProvisionEventKind::ProvisionStart => "provision_start",
        ProvisionEventKind::NodeLive => "node_live",
        ProvisionEventKind::ProvisionFailed => "provision_failed",
        ProvisionEventKind::NodeStopped => "node_stopped",
    }
}

fn lifecycle_ids(event: &obs::Event) -> (Option<u64>, Option<u64>) {
    match event {
        obs::Event::RunScoped { run_id, .. } | obs::Event::StageScoped { run_id, .. } => {
            (Some(run_id.0), None)
        }
        obs::Event::NodeScoped { node_id, .. } | obs::Event::NodeFaulted { node_id, .. } => {
            (None, Some(node_id.0))
        }
        obs::Event::EdgeScoped { .. }
        | obs::Event::RingScoped { .. }
        | obs::Event::ObjectScoped { .. }
        | obs::Event::StepScoped { .. }
        | obs::Event::WorkerScoped { .. } => (None, None),
    }
}

const MVP_CLUSTER_HTML: &str = r#"<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>MVP cluster</title>
  <style>
    body { font: 13px system-ui, sans-serif; margin: 1rem; background: #0f1115; color: #e8e8e8; }
    table { border-collapse: collapse; width: 100%; margin-bottom: 1rem; }
    th, td { border-bottom: 1px solid #30343d; padding: .35rem .5rem; text-align: left; vertical-align: top; }
    th { color: #aab; font-weight: 600; }
    pre { white-space: pre-wrap; margin: 0; max-height: 12rem; overflow: auto; }
    .ok { color: #7ee787; }
    .bad { color: #ff7b72; }
    .muted { color: #8b949e; }
    .grid { display: grid; grid-template-columns: 1fr 1fr; gap: 1rem; }
  </style>
</head>
<body>
  <h1>MVP cluster</h1>
  <p id="status" class="muted">loading…</p>
  <h2>Provisioned nodes</h2>
  <table><thead><tr><th>node</th><th>phase</th><th>message</th></tr></thead><tbody id="nodes"></tbody></table>
  <div class="grid">
    <section><h2>Event log</h2><table><thead><tr><th>pos</th><th>channel</th><th>run</th><th>node</th><th>event</th></tr></thead><tbody id="events"></tbody></table></section>
    <section><h2>Selected node logs</h2><div id="logs" class="muted">select a node row</div></section>
  </div>
<script>
let selected = null;
async function refresh() {
  const data = await fetch('/api/view/mvp/cluster').then(r => r.json());
  document.getElementById('status').textContent = `${data.nodes.length} node(s), ${data.events.length} event(s)`;
  const nodes = document.getElementById('nodes');
  nodes.innerHTML = data.nodes.map(n => `<tr data-node="${n.node_id}"><td>${n.node_id}</td><td class="${n.phase === 'provision_failed' ? 'bad' : 'ok'}">${n.phase || ''}</td><td>${n.last_message || ''}</td></tr>`).join('') || '<tr><td colspan="3" class="muted">No provisioning records yet.</td></tr>';
  for (const row of nodes.querySelectorAll('tr[data-node]')) row.onclick = () => { selected = Number(row.dataset.node); renderLogs(data); };
  const events = document.getElementById('events');
  events.innerHTML = data.events.slice().reverse().map(e => `<tr><td>${e.position}</td><td>${e.channel}</td><td>${e.run_id ?? ''}</td><td>${e.node_id ?? ''}</td><td>${e.label}</td></tr>`).join('');
  renderLogs(data);
}
function renderLogs(data) {
  if (selected == null) return;
  const n = data.nodes.find(n => n.node_id === selected);
  if (!n) return;
  document.getElementById('logs').innerHTML = `<h3>node ${n.node_id}</h3><h4>stdout</h4><pre>${esc(n.stdout_tail.join('\n'))}</pre><h4>stderr</h4><pre>${esc(n.stderr_tail.join('\n'))}</pre><h4>provider</h4><pre>${esc(n.provider_tail.join('\n'))}</pre>`;
}
function esc(s) { return s.replace(/[&<>]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;'}[c])); }
refresh(); setInterval(refresh, 1000);
</script>
</body>
</html>"#;
