//! Server-side fold of the vast.ai record stream into a per-stage board model.
//!
//! This is a Rust port of `buildModel` from the original browser mockup
//! (`crates/dashboard/examples/vastai_mockups/vastai_model.js`). The JS folded a
//! flat record stream client-side; here the dashboard owns the fold so the wire
//! payload is the already-folded [`VastaiBoardModel`] and the HTML is a thin
//! renderer. Field names are serialized to the **camelCase** shape the renderer
//! reads (`gpuName`, `totalCost`, `lifecycleAll`, `tEnd`, …).
//!
//! The fold is a pure function of its input records; [`VastaiLivePlugin`] owns the
//! ingest buffer and re-folds on demand.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::Mutex;

use serde::Serialize;

use distribution::diagnostics::collector::protocol::LiveRecord;
use distribution::diagnostics::vastai::record::{
    LifecycleEvent, LogStream, VastaiBody, VastaiRecord,
};

use crate::plugin::{DashboardPlugin, PluginResponse};

/// Max records retained per run before the oldest are dropped. Folding this many
/// every ~200ms is cheap; the cap just bounds memory for very long runs.
const BUFFER_CAP: usize = 50_000;

const STAGE_COLORS: [&str; 8] = [
    "#4a90e2", "#f06292", "#ffb74d", "#81c784", "#ba68c8", "#4dd0e1", "#aed581", "#ff8a65",
];

fn stage_color(idx: u32) -> String {
    STAGE_COLORS[(idx as usize) % STAGE_COLORS.len()].to_string()
}

// ── Serialized model (camelCase to match the JS renderer) ────────────────────

#[derive(Serialize)]
pub struct Meta {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stages: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct VastaiBoardModel {
    pub meta: Meta,
    pub stages: Vec<StageModel>,
    pub lifecycle_all: Vec<LifecycleItem>,
    pub all_logs: Vec<LogItem>,
    pub deploy_start: Option<DeployStart>,
    pub t_end: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StageModel {
    pub idx: u32,
    pub label: String,
    pub color: String,
    pub gpu_name: Option<String>,
    pub geo: Option<String>,
    pub dph: Option<f64>,
    pub samples: Vec<Sample>,
    pub instances: Vec<Instance>,
    pub logs: Vec<LogItem>,
    pub lifecycle: Vec<LifecycleItem>,
    pub contract_list: Vec<Contract>,
    pub total_cost: f64,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Sample {
    pub at: u64,
    pub contract_id: Option<u64>,
    pub util: Option<f64>,
    pub vram: Option<f64>,
    pub vram_total: Option<f64>,
    pub temp: Option<f64>,
    pub power: Option<f64>,
    pub power_limit: Option<f64>,
    pub cpu: Option<f64>,
    pub load1: Option<f64>,
    pub num_cpus: Option<u32>,
    pub mem_used_kb: Option<u64>,
    pub mem_total_kb: Option<u64>,
    pub disk_r: Option<f64>,
    pub disk_w: Option<f64>,
    pub fs_used: Option<u64>,
    pub fs_total: Option<u64>,
    pub net_rx: Option<f64>,
    pub net_tx: Option<f64>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Instance {
    pub at: u64,
    pub contract_id: u64,
    pub status: Option<String>,
    pub status_msg: Option<String>,
    pub cost: Option<f64>,
    pub dph: Option<f64>,
    pub gpu_util: Option<f64>,
    pub gpu_temp: Option<f64>,
    pub cpu_util: Option<f64>,
    pub mem_usage_mb: Option<f64>,
    pub gpu_ram: Option<f64>,
    pub num_gpus: Option<u32>,
    pub disk_usage: Option<f64>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LogItem {
    pub at: u64,
    pub stream: String,
    pub line: u64,
    pub text: String,
    pub contract_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage: Option<u32>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct Contract {
    pub id: u64,
    pub leased_at: Option<u64>,
    pub end_at: Option<u64>,
    pub reason: Option<String>,
    pub terminal_status: Option<String>,
    pub offer: Option<u64>,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LifecycleItem {
    pub at: u64,
    pub stage: Option<u32>,
    pub event: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub contract_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub offer_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub num_stages: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployStart {
    pub at: u64,
    pub num_stages: Option<u32>,
}

// ── Fold ─────────────────────────────────────────────────────────────────────

/// Internal mutable accumulator for one stage during the fold.
struct StageAccum {
    idx: u32,
    label: String,
    gpu_name: Option<String>,
    geo: Option<String>,
    dph: Option<f64>,
    samples: Vec<Sample>,
    instances: Vec<Instance>,
    logs: Vec<LogItem>,
    lifecycle: Vec<LifecycleItem>,
    contracts: HashMap<u64, Contract>,
}

impl StageAccum {
    fn new(idx: u32) -> Self {
        StageAccum {
            idx,
            label: format!("stage-{idx}"),
            gpu_name: None,
            geo: None,
            dph: None,
            samples: Vec::new(),
            instances: Vec::new(),
            logs: Vec::new(),
            lifecycle: Vec::new(),
            contracts: HashMap::new(),
        }
    }
}

fn log_stream_str(s: LogStream) -> &'static str {
    match s {
        LogStream::Stdout => "stdout",
        LogStream::Stderr => "stderr",
        LogStream::VastRequestLogs => "vast_request_logs",
    }
}

/// Fold the record stream into the board model the UI renders.
///
/// `run_id` and `run_label` come from the collector/ingest context (not the
/// record bodies): `run_label`, when set, is the run prefix trimmed off per-stage
/// labels, mirroring the JS `meta.label` behavior.
pub fn fold(records: &[VastaiRecord], run_id: &str, run_label: Option<&str>) -> VastaiBoardModel {
    let run_prefix = run_label.map(|l| format!("{l}-"));
    let trim = |label: &str| -> String {
        match &run_prefix {
            Some(p) if label.starts_with(p.as_str()) => label[p.len()..].to_string(),
            _ => label.to_string(),
        }
    };

    let mut stages: BTreeMap<u32, StageAccum> = BTreeMap::new();
    let mut lifecycle_all: Vec<LifecycleItem> = Vec::new();
    let mut deploy_start: Option<DeployStart> = None;
    let mut max_at: u64 = 0;
    let mut num_stages_seen: Option<u32> = None;

    // Reproduces the JS `stage(idx, label)`: create on first sight; upgrade a
    // default `stage-N` label once a real (trimmed) label arrives.
    fn stage<'a>(
        stages: &'a mut BTreeMap<u32, StageAccum>,
        idx: u32,
        label: Option<String>,
    ) -> &'a mut StageAccum {
        let s = stages.entry(idx).or_insert_with(|| StageAccum::new(idx));
        if let Some(label) = label
            && !label.is_empty()
            && s.label.starts_with("stage-")
        {
            s.label = label;
        }
        s
    }

    for rec in records {
        let node = &rec.vastai_node;
        let s_idx = node.stage_index;
        let at = rec.wall_ms;
        max_at = max_at.max(at);
        let trimmed_label = node.label.as_deref().map(&trim);

        match &rec.body {
            VastaiBody::Lifecycle(ev) => match ev {
                LifecycleEvent::DeployStart { num_stages } => {
                    deploy_start = Some(DeployStart {
                        at,
                        num_stages: *num_stages,
                    });
                    if let Some(n) = num_stages {
                        num_stages_seen = Some(*n);
                    }
                    lifecycle_all.push(LifecycleItem {
                        at,
                        stage: None,
                        event: "deploy_start".to_string(),
                        contract_id: None,
                        offer_id: None,
                        reason: None,
                        num_stages: *num_stages,
                    });
                }
                LifecycleEvent::ContractLeased {
                    contract_id,
                    offer_id,
                } => {
                    let Some(idx) = s_idx else { continue };
                    let item = LifecycleItem {
                        at,
                        stage: Some(idx),
                        event: "contract_leased".to_string(),
                        contract_id: Some(*contract_id),
                        offer_id: *offer_id,
                        reason: None,
                        num_stages: None,
                    };
                    lifecycle_all.push(item.clone());
                    let s = stage(&mut stages, idx, trimmed_label.clone());
                    s.lifecycle.push(item);
                    let c = s.contracts.entry(*contract_id).or_insert_with(|| Contract {
                        id: *contract_id,
                        leased_at: None,
                        end_at: None,
                        reason: None,
                        terminal_status: None,
                        offer: None,
                    });
                    c.leased_at = Some(at);
                    c.offer = *offer_id;
                }
                LifecycleEvent::Teardown {
                    contract_id,
                    reason,
                } => {
                    let Some(idx) = s_idx else { continue };
                    let item = LifecycleItem {
                        at,
                        stage: Some(idx),
                        event: "teardown".to_string(),
                        contract_id: Some(*contract_id),
                        offer_id: None,
                        reason: reason.clone(),
                        num_stages: None,
                    };
                    lifecycle_all.push(item.clone());
                    let s = stage(&mut stages, idx, trimmed_label.clone());
                    s.lifecycle.push(item);
                    let c = s.contracts.entry(*contract_id).or_insert_with(|| Contract {
                        id: *contract_id,
                        leased_at: None,
                        end_at: None,
                        reason: None,
                        terminal_status: None,
                        offer: None,
                    });
                    c.end_at = Some(at);
                    c.reason = reason.clone();
                }
            },

            VastaiBody::Instance(obs) => {
                let Some(idx) = s_idx else { continue };
                let s = stage(&mut stages, idx, trimmed_label.clone());
                if s.gpu_name.is_none() {
                    s.gpu_name = obs.gpu_name.clone();
                }
                if s.geo.is_none() {
                    s.geo = obs.geolocation.clone();
                }
                if let Some(dph) = obs.dph_total {
                    s.dph = Some(dph);
                }
                s.instances.push(Instance {
                    at,
                    contract_id: obs.id,
                    status: obs.actual_status.clone(),
                    status_msg: obs.status_msg.clone(),
                    cost: obs.accumulated_cost,
                    dph: obs.dph_total,
                    gpu_util: obs.gpu_util,
                    gpu_temp: obs.gpu_temp,
                    cpu_util: obs.cpu_util,
                    mem_usage_mb: obs.mem_usage,
                    gpu_ram: obs.gpu_ram,
                    num_gpus: obs.num_gpus,
                    disk_usage: obs.disk_usage,
                });
                let c = s.contracts.entry(obs.id).or_insert_with(|| Contract {
                    id: obs.id,
                    leased_at: None,
                    end_at: None,
                    reason: None,
                    terminal_status: None,
                    offer: None,
                });
                if matches!(obs.actual_status.as_deref(), Some("exited") | Some("offline")) {
                    c.terminal_status = obs.actual_status.clone();
                }
            }

            VastaiBody::HostSample(hs) => {
                let Some(idx) = s_idx else { continue };
                let s = stage(&mut stages, idx, trimmed_label.clone());
                let g = hs.gpus.first();
                let disk = hs.disk.first();
                let net = hs.net.first();
                if s.gpu_name.is_none()
                    && let Some(name) = g.and_then(|g| g.name.clone())
                {
                    s.gpu_name = Some(name);
                }
                s.samples.push(Sample {
                    at,
                    contract_id: node.contract_id,
                    util: g.and_then(|g| g.util_pct),
                    vram: g.and_then(|g| g.mem_used_mb),
                    vram_total: g.and_then(|g| g.mem_total_mb),
                    temp: g.and_then(|g| g.temp_c),
                    power: g.and_then(|g| g.power_w),
                    power_limit: g.and_then(|g| g.power_limit_w),
                    cpu: hs.cpu.util_pct,
                    load1: hs.cpu.load1,
                    num_cpus: hs.cpu.num_cpus,
                    mem_used_kb: hs.mem.used_kb,
                    mem_total_kb: hs.mem.total_kb,
                    disk_r: disk.and_then(|d| d.read_bytes_per_s),
                    disk_w: disk.and_then(|d| d.write_bytes_per_s),
                    fs_used: disk.and_then(|d| d.fs_used_bytes),
                    fs_total: disk.and_then(|d| d.fs_total_bytes),
                    net_rx: net.and_then(|n| n.rx_bytes_per_s),
                    net_tx: net.and_then(|n| n.tx_bytes_per_s),
                });
            }

            VastaiBody::Logs(batch) => {
                let Some(idx) = s_idx else { continue };
                let stream = log_stream_str(batch.stream);
                let contract_id = node.contract_id;
                let s = stage(&mut stages, idx, trimmed_label.clone());
                for ln in &batch.lines {
                    s.logs.push(LogItem {
                        at,
                        stream: stream.to_string(),
                        line: ln.line,
                        text: ln.text.clone(),
                        contract_id,
                        stage: None,
                    });
                }
            }
        }
    }

    // Sort each series by time, build contract lists, compute total cost.
    let mut stage_models: Vec<StageModel> = Vec::with_capacity(stages.len());
    let mut all_logs: Vec<LogItem> = Vec::new();
    for (_idx, mut s) in stages {
        s.samples.sort_by_key(|x| x.at);
        s.instances.sort_by_key(|x| x.at);
        s.logs.sort_by_key(|x| x.at);
        s.lifecycle.sort_by_key(|x| x.at);

        let mut contract_list: Vec<Contract> = s.contracts.into_values().collect();
        contract_list.sort_by_key(|c| c.leased_at.unwrap_or(0));

        // Final accumulated cost = last observation's cost per contract, summed.
        let mut by_contract: HashMap<u64, f64> = HashMap::new();
        for o in &s.instances {
            by_contract.insert(o.contract_id, o.cost.unwrap_or(0.0));
        }
        let total_cost: f64 = by_contract.values().sum();

        for l in &s.logs {
            let mut l = l.clone();
            l.stage = Some(s.idx);
            all_logs.push(l);
        }

        stage_models.push(StageModel {
            idx: s.idx,
            label: s.label,
            color: stage_color(s.idx),
            gpu_name: s.gpu_name,
            geo: s.geo,
            dph: s.dph,
            samples: s.samples,
            instances: s.instances,
            logs: s.logs,
            lifecycle: s.lifecycle,
            contract_list,
            total_cost,
        });
    }
    all_logs.sort_by(|a, b| a.at.cmp(&b.at).then(a.line.cmp(&b.line)));
    lifecycle_all.sort_by_key(|x| x.at);

    let stages_count = num_stages_seen.or_else(|| Some(stage_models.len() as u32));

    VastaiBoardModel {
        meta: Meta {
            run_id: run_id.to_string(),
            duration_ms: None,
            stages: stages_count,
            label: run_label.map(|s| s.to_string()),
        },
        stages: stage_models,
        lifecycle_all,
        all_logs,
        deploy_start,
        t_end: max_at,
    }
}

// ── Plugin ───────────────────────────────────────────────────────────────────

/// Strip a trailing `-<digits>` from a per-stage label to recover the run prefix
/// (e.g. `"pp-synth-3"` → `"pp-synth"`), mirroring the JS `meta.label` trimming.
fn run_label_from(node_label: &str) -> Option<String> {
    let (prefix, suffix) = node_label.rsplit_once('-')?;
    if !suffix.is_empty() && suffix.bytes().all(|b| b.is_ascii_digit()) && !prefix.is_empty() {
        Some(prefix.to_string())
    } else {
        None
    }
}

/// Parse the collector node-id convention `vastai-stage-{i}` → `i`. Used only as a
/// fallback when a record body lacks an explicit `stage_index`.
fn stage_index_from_node_id(node_id: &str) -> Option<u32> {
    node_id.strip_prefix("vastai-stage-")?.parse().ok()
}

/// Dashboard plugin (name `"vastai"`) that buffers the vast.ai record stream and
/// serves the server-side fold as JSON over `/events`, `/api/plugin/vastai/model`.
pub struct VastaiLivePlugin {
    buffer: Mutex<VecDeque<VastaiRecord>>,
    run_id: Mutex<Option<String>>,
    run_label: Mutex<Option<String>>,
}

impl VastaiLivePlugin {
    pub fn new() -> Self {
        VastaiLivePlugin {
            buffer: Mutex::new(VecDeque::new()),
            run_id: Mutex::new(None),
            run_label: Mutex::new(None),
        }
    }

    /// Ingest one live record forwarded from the collector broadcast.
    pub fn ingest(&self, rec: &LiveRecord) {
        let mut record: VastaiRecord = match serde_json::from_value(rec.body.clone()) {
            Ok(r) => r,
            Err(_) => return,
        };
        // Fall back to the node-id convention only when the body omits the stage.
        if record.vastai_node.stage_index.is_none()
            && let Some(idx) = stage_index_from_node_id(&rec.node_id)
        {
            record.vastai_node.stage_index = Some(idx);
        }
        if self.run_id.lock().unwrap().is_none() {
            *self.run_id.lock().unwrap() = Some(rec.run_id.clone());
        }
        if self.run_label.lock().unwrap().is_none()
            && let Some(label) = record.vastai_node.label.as_deref()
            && let Some(rl) = run_label_from(label)
        {
            *self.run_label.lock().unwrap() = Some(rl);
        }
        let mut buf = self.buffer.lock().unwrap();
        if buf.len() >= BUFFER_CAP {
            buf.pop_front();
        }
        buf.push_back(record);
    }

    fn model_json(&self) -> Option<String> {
        let buf = self.buffer.lock().unwrap();
        if buf.is_empty() {
            return None;
        }
        let records: Vec<VastaiRecord> = buf.iter().cloned().collect();
        drop(buf);
        let run_id = self.run_id.lock().unwrap().clone().unwrap_or_default();
        let run_label = self.run_label.lock().unwrap().clone();
        let model = fold(&records, &run_id, run_label.as_deref());
        serde_json::to_string(&model).ok()
    }
}

impl Default for VastaiLivePlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl DashboardPlugin for VastaiLivePlugin {
    fn name(&self) -> &str {
        "vastai"
    }

    fn snapshot_json(&self) -> Option<String> {
        self.model_json()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        _body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "") | ("GET", "model") => {
                PluginResponse::json(self.model_json().unwrap_or_else(|| "null".into()))
            }
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(super::FLEET_HTML)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use distribution::diagnostics::vastai::record::{
        GpuSample, HostSample, InstanceObservation, LifecycleEvent, LogBatch, LogLine, LogStream,
        Source, VastaiBody, VastaiNodeRef, VastaiRecord, VASTAI_SCHEMA_VERSION,
    };

    fn rec(stage: Option<u32>, contract: Option<u64>, wall_ms: u64, body: VastaiBody) -> VastaiRecord {
        VastaiRecord {
            v: VASTAI_SCHEMA_VERSION,
            vastai_node: VastaiNodeRef {
                contract_id: contract,
                stage_index: stage,
                label: None,
            },
            source: Source::External,
            seq: 0,
            wall_ms,
            body,
        }
    }

    /// Story test: a tiny two-stage deploy folds into the per-stage board the UI
    /// renders. Asserts the observable contract of `fold` — grouping, cost
    /// semantics, log ordering, contract windows — not its internal steps.
    #[test]
    fn fold_groups_a_two_stage_deploy() {
        let mut records = vec![
            rec(
                None,
                None,
                100,
                VastaiBody::Lifecycle(LifecycleEvent::DeployStart {
                    num_stages: Some(2),
                }),
            ),
            rec(
                Some(0),
                Some(10),
                200,
                VastaiBody::Lifecycle(LifecycleEvent::ContractLeased {
                    contract_id: 10,
                    offer_id: Some(999),
                }),
            ),
            rec(
                Some(1),
                Some(11),
                210,
                VastaiBody::Lifecycle(LifecycleEvent::ContractLeased {
                    contract_id: 11,
                    offer_id: None,
                }),
            ),
        ];
        // Stage 0 polled twice with rising accumulated cost; the LAST cost stands.
        for (t, cost, status) in [(300u64, 0.10, "loading"), (600, 0.40, "running")] {
            records.push(rec(
                Some(0),
                Some(10),
                t,
                VastaiBody::Instance(InstanceObservation {
                    id: 10,
                    gpu_name: Some("RTX 4090".into()),
                    dph_total: Some(0.5),
                    accumulated_cost: Some(cost),
                    actual_status: Some(status.into()),
                    ..Default::default()
                }),
            ));
        }
        records.push(rec(
            Some(0),
            Some(10),
            700,
            VastaiBody::HostSample(HostSample {
                gpus: vec![GpuSample {
                    index: 0,
                    util_pct: Some(80.0),
                    ..Default::default()
                }],
                ..Default::default()
            }),
        ));
        // Logs arrive out of order — the fold must time-order them.
        records.push(rec(
            Some(0),
            Some(10),
            900,
            VastaiBody::Logs(LogBatch {
                stream: LogStream::Stdout,
                lines: vec![LogLine {
                    wall_ms: 900,
                    line: 2,
                    text: "second".into(),
                }],
            }),
        ));
        records.push(rec(
            Some(0),
            Some(10),
            800,
            VastaiBody::Logs(LogBatch {
                stream: LogStream::Stdout,
                lines: vec![LogLine {
                    wall_ms: 800,
                    line: 1,
                    text: "first".into(),
                }],
            }),
        ));
        records.push(rec(
            Some(0),
            Some(10),
            1000,
            VastaiBody::Lifecycle(LifecycleEvent::Teardown {
                contract_id: 10,
                reason: Some("done".into()),
            }),
        ));

        let model = fold(&records, "pp-run", None);

        // Stages are grouped by stage_index and emitted in order.
        assert_eq!(
            model.stages.iter().map(|s| s.idx).collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(model.deploy_start.as_ref().unwrap().num_stages, Some(2));
        assert_eq!(model.t_end, 1000);

        let s0 = &model.stages[0];
        // Cost folds to the last poll's accumulated cost, not the sum of polls.
        assert_eq!(s0.total_cost, 0.40);
        assert_eq!(s0.gpu_name.as_deref(), Some("RTX 4090"));
        assert_eq!(s0.label, "stage-0");
        // Logs are time-ordered regardless of arrival order.
        assert_eq!(
            s0.logs.iter().map(|l| l.text.as_str()).collect::<Vec<_>>(),
            vec!["first", "second"]
        );
        // The contract's lease→teardown window and offer are captured.
        let c = s0.contract_list.iter().find(|c| c.id == 10).unwrap();
        assert_eq!(c.leased_at, Some(200));
        assert_eq!(c.end_at, Some(1000));
        assert_eq!(c.offer, Some(999));

        // allLogs is tagged with its stage for the merged log tail.
        assert!(!model.all_logs.is_empty());
        assert!(model.all_logs.iter().all(|l| l.stage == Some(0)));
    }
}
