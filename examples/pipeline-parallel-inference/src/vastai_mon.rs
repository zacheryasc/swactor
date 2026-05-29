//! Orchestrator-side wiring for the independent vastai monitoring layer.
//!
//! This is the *external view* producer: a background poller that pulls the
//! vast.ai REST API for every leased contract — continuously, including the
//! pre-`running` image-pull window — and ships a full
//! [`InstanceObservation`](distribution::diagnostics::vastai::record::InstanceObservation)
//! per contract per tick into the collector via a
//! [`VastaiShipper`](distribution::diagnostics::vastai::VastaiShipper).
//!
//! It is deliberately decoupled from swactor: it never touches the iroh driver or
//! the swactor aggregator, and it ships under its own synthetic collector node id
//! (`vastai-external`). Enable it with `VASTAI_MON_COLLECTOR_URL` (falling back to
//! `SWACTOR_DIAG_COLLECTOR_URL` when unset); leaving both unset disables it.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use distribution::diagnostics::vastai::record::{
    InstanceObservation, LifecycleEvent, Source, VastaiNodeRef,
};
use distribution::diagnostics::vastai::{
    spawn_sampler, LogFlusherHandle, LogForwarder, LogForwarderConfig, NoGpu, Sampler, SamplerHandle,
    VastaiShipper, VastaiShipperConfig,
};
use reqwest::Client;

/// Synthetic collector node id under which all external observations land.
pub const EXTERNAL_NODE_ID: &str = "vastai-external";

/// One contract under observation.
#[derive(Debug, Clone)]
pub struct ContractRef {
    pub contract_id: u64,
    pub stage_index: Option<u32>,
    pub label: Option<String>,
}

impl ContractRef {
    fn node_ref(&self) -> VastaiNodeRef {
        VastaiNodeRef {
            contract_id: Some(self.contract_id),
            stage_index: self.stage_index,
            label: self.label.clone(),
        }
    }
}

/// Cheap, cloneable handle that the lease path uses to register/unregister
/// contracts as they are created and destroyed. Shares the poller's contract set.
#[derive(Clone)]
pub struct ContractTracker {
    contracts: Arc<Mutex<Vec<ContractRef>>>,
    shipper: VastaiShipper,
}

impl ContractTracker {
    /// Begin observing a contract. Emits a `ContractLeased` lifecycle marker so
    /// the timeline records when the slot was filled — before the instance is
    /// even running.
    pub fn track(&self, contract_id: u64, stage_index: Option<u32>, label: Option<String>) {
        {
            let mut g = self.contracts.lock().expect("contracts mutex poisoned");
            if g.iter().any(|c| c.contract_id == contract_id) {
                return;
            }
            g.push(ContractRef {
                contract_id,
                stage_index,
                label: label.clone(),
            });
        }
        let node = VastaiNodeRef {
            contract_id: Some(contract_id),
            stage_index,
            label,
        };
        self.shipper
            .with_node(node)
            .lifecycle(LifecycleEvent::ContractLeased {
                contract_id,
                offer_id: None,
            });
    }

    /// Stop observing a contract (e.g. it was destroyed during replacement).
    pub fn untrack(&self, contract_id: u64) {
        let mut g = self.contracts.lock().expect("contracts mutex poisoned");
        g.retain(|c| c.contract_id != contract_id);
    }
}

/// Configuration for the external poller.
#[derive(Debug, Clone)]
pub struct VastaiPollerConfig {
    pub collector_url: String,
    pub run_id: String,
    pub api_key: String,
    pub base_url: String,
    pub poll_interval: Duration,
    pub spool_dir: std::path::PathBuf,
}

impl VastaiPollerConfig {
    /// Build from process env, returning `None` when monitoring is disabled
    /// (no collector URL). `VASTAI_MON_COLLECTOR_URL` wins; otherwise the
    /// swactor collector URL is reused so a single collector serves both layers.
    pub fn from_env(run_id: &str, api_key: &str, base_url: &str) -> Option<Self> {
        let collector_url = std::env::var("VASTAI_MON_COLLECTOR_URL")
            .ok()
            .or_else(|| std::env::var("SWACTOR_DIAG_COLLECTOR_URL").ok())
            .or_else(|| std::env::var("PP_DASHBOARD_URL").ok())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())?;
        let poll_interval = std::env::var("VASTAI_MON_POLL_SECS")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .filter(|&n| n > 0)
            .map(Duration::from_secs)
            .unwrap_or(Duration::from_secs(10));
        let spool_dir = std::env::var("VASTAI_MON_SPOOL_DIR")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| std::env::temp_dir().join("vastai-mon-spool"));
        Some(Self {
            collector_url,
            run_id: run_id.to_string(),
            api_key: api_key.to_string(),
            base_url: base_url.to_string(),
            poll_interval,
            spool_dir,
        })
    }
}

/// A running external poller. Spawns a background task; hold it for the lifetime
/// of the deploy and call [`VastaiPoller::shutdown`] at teardown.
pub struct VastaiPoller {
    contracts: Arc<Mutex<Vec<ContractRef>>>,
    shipper: VastaiShipper,
    task: tokio::task::JoinHandle<()>,
    stop: Arc<tokio::sync::Notify>,
}

impl VastaiPoller {
    /// Build the shipper, emit a `DeployStart` marker, and spawn the poll loop.
    pub fn spawn(config: VastaiPollerConfig, num_stages: Option<u32>) -> std::io::Result<Self> {
        let shipper_cfg = VastaiShipperConfig::new(
            config.collector_url.clone(),
            config.run_id.clone(),
            EXTERNAL_NODE_ID,
            config.spool_dir.clone(),
        );
        let shipper = VastaiShipper::spawn(shipper_cfg, VastaiNodeRef::default(), Source::External)?;
        shipper.lifecycle(LifecycleEvent::DeployStart { num_stages });

        let contracts: Arc<Mutex<Vec<ContractRef>>> = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(tokio::sync::Notify::new());
        let client = Client::new();

        let loop_contracts = Arc::clone(&contracts);
        let loop_shipper = shipper.clone();
        let loop_stop = Arc::clone(&stop);
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(config.poll_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = loop_stop.notified() => break,
                    _ = ticker.tick() => {
                        let snapshot: Vec<ContractRef> = {
                            loop_contracts.lock().expect("contracts mutex poisoned").clone()
                        };
                        for c in snapshot {
                            match poll_once(&client, &config.base_url, &config.api_key, c.contract_id).await {
                                Ok(obs) => loop_shipper.with_node(c.node_ref()).instance(obs),
                                Err(e) => eprintln!(
                                    "vastai-mon: poll contract {} failed: {e}",
                                    c.contract_id
                                ),
                            }
                        }
                    }
                }
            }
        });

        Ok(Self {
            contracts,
            shipper,
            task,
            stop,
        })
    }

    /// A handle the lease path uses to register/unregister contracts.
    pub fn tracker(&self) -> ContractTracker {
        ContractTracker {
            contracts: Arc::clone(&self.contracts),
            shipper: self.shipper.clone(),
        }
    }

    /// Emit a teardown marker for a contract being destroyed.
    pub fn note_teardown(&self, contract_id: u64, reason: Option<String>) {
        let node = VastaiNodeRef {
            contract_id: Some(contract_id),
            ..Default::default()
        };
        self.shipper
            .with_node(node)
            .lifecycle(LifecycleEvent::Teardown {
                contract_id,
                reason,
            });
    }

    /// Stop the poll loop and flush the shipper.
    pub async fn shutdown(self) {
        self.stop.notify_waiters();
        let _ = self.task.await;
        self.shipper.handle().shutdown().await;
    }
}

// ── In-VM side (runs inside each rented container) ───────────────────────────

/// Synthetic collector node id for a stage container's in-VM telemetry.
pub fn in_vm_node_id(stage_index: Option<u32>) -> String {
    match stage_index {
        Some(i) => format!("vastai-stage-{i}"),
        None => "vastai-stage".to_string(),
    }
}

/// The in-VM monitor: a metrics sampler + a live log forwarder, both shipping to
/// the collector under this container's synthetic node id. Independent of the
/// swactor diagnostics in `diag::install_from_env` — it does not touch the iroh
/// driver or aggregator and runs even when swactor diagnostics are off.
pub struct InVmMonitor {
    forwarder: LogForwarder,
    sampler: SamplerHandle,
    flusher: LogFlusherHandle,
    shipper: VastaiShipper,
}

impl InVmMonitor {
    /// A clone of the log forwarder for the stage actor to hold.
    pub fn forwarder(&self) -> LogForwarder {
        self.forwarder.clone()
    }

    /// Stop sampling + flushing and flush the shipper.
    pub async fn shutdown(self) {
        self.flusher.shutdown().await;
        self.sampler.shutdown().await;
        self.shipper.handle().shutdown().await;
    }
}

/// Build the in-VM monitor from container env, returning `None` when monitoring
/// is disabled. Must be called inside a tokio runtime (spawns background tasks).
///
/// Env: `VASTAI_MON_COLLECTOR_URL` (falls back to `SWACTOR_DIAG_COLLECTOR_URL`)
/// enables it; `SWACTOR_DIAG_RUN_ID` pins the run; `STAGE` (or
/// `SWACTOR_DIAG_STAGE_INDEX`) gives the stage slot for correlation.
pub fn install_in_vm_from_env() -> Option<InVmMonitor> {
    let collector_url = std::env::var("VASTAI_MON_COLLECTOR_URL")
        .ok()
        .or_else(|| std::env::var("SWACTOR_DIAG_COLLECTOR_URL").ok())
        .or_else(|| std::env::var("PP_DASHBOARD_URL").ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;
    let run_id = std::env::var("SWACTOR_DIAG_RUN_ID")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "pp-run".to_string());
    let stage_index = std::env::var("STAGE")
        .ok()
        .or_else(|| std::env::var("SWACTOR_DIAG_STAGE_INDEX").ok())
        .and_then(|s| s.trim().parse::<u32>().ok());
    let spool_dir = std::env::var("VASTAI_MON_SPOOL_DIR")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::env::temp_dir().join("vastai-mon-spool"));
    let sample_interval = std::env::var("VASTAI_MON_SAMPLE_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .map(Duration::from_secs)
        .unwrap_or(Duration::from_secs(5));

    let node = VastaiNodeRef {
        contract_id: None,
        stage_index,
        label: None,
    };
    let cfg = VastaiShipperConfig::new(
        collector_url,
        run_id,
        in_vm_node_id(stage_index),
        spool_dir,
    );
    let shipper = match VastaiShipper::spawn(cfg, node, Source::InVm) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("vastai-mon: in-VM monitor disabled: {e}");
            return None;
        }
    };

    let sampler = spawn_sampler(Sampler::new(NoGpu), shipper.clone(), sample_interval);
    let forwarder = LogForwarder::new(shipper.clone(), LogForwarderConfig::default());
    let flusher = forwarder.spawn_flusher();
    eprintln!("vastai-mon: in-VM monitor enabled (sampler + log stream)");
    Some(InVmMonitor {
        forwarder,
        sampler,
        flusher,
        shipper,
    })
}

/// Fetch one instance and map vast's JSON into an [`InstanceObservation`],
/// keeping the verbatim JSON in `raw`.
pub async fn poll_once(
    client: &Client,
    base_url: &str,
    api_key: &str,
    contract_id: u64,
) -> Result<InstanceObservation, String> {
    let url = format!("{base_url}/api/v0/instances/{contract_id}/");
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("request error: {e}"))?;
    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }
    let value: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("parse error: {e}"))?;
    // vast wraps the instance under "instances".
    let instance = value
        .get("instances")
        .cloned()
        .unwrap_or(serde_json::Value::Null);
    Ok(instance_observation_from_value(contract_id, instance))
}

/// Pure mapping from a vast `instances` JSON object to an
/// [`InstanceObservation`]. Unmodeled fields are preserved in `raw`.
pub fn instance_observation_from_value(
    contract_id: u64,
    v: serde_json::Value,
) -> InstanceObservation {
    let f = |k: &str| v.get(k).and_then(|x| x.as_f64());
    let u = |k: &str| v.get(k).and_then(|x| x.as_u64());
    let s = |k: &str| v.get(k).and_then(|x| x.as_str()).map(|x| x.to_string());

    InstanceObservation {
        id: u("id").unwrap_or(contract_id),
        gpu_name: s("gpu_name"),
        gpu_ram: f("gpu_ram").or_else(|| f("gpu_totalram")),
        num_gpus: u("num_gpus").map(|x| x as u32),
        geolocation: s("geolocation"),
        host_id: u("host_id"),
        machine_id: u("machine_id"),
        dph_total: f("dph_total"),
        accumulated_cost: f("accumulated_cost").or_else(|| f("cost")),
        start_date: f("start_date"),
        actual_status: s("actual_status"),
        intended_status: s("intended_status"),
        status_msg: s("status_msg"),
        disk_usage: f("disk_usage"),
        disk_space: f("disk_space"),
        public_ipaddr: s("public_ipaddr"),
        ssh_host: s("ssh_host"),
        ssh_port: u("ssh_port").map(|x| x as u16),
        gpu_util: f("gpu_util"),
        gpu_temp: f("gpu_temp"),
        cpu_util: f("cpu_util"),
        mem_usage: f("mem_usage"),
        inet_up: f("inet_up"),
        inet_down: f("inet_down"),
        raw: v,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_known_vast_fields_and_preserves_raw() {
        // A representative vast instance object during the image-pull window:
        // not yet running, with the live fields vast fills in later still absent.
        let v = serde_json::json!({
            "id": 12345,
            "gpu_name": "RTX 4090",
            "gpu_ram": 24576.0,
            "num_gpus": 1,
            "geolocation": "US",
            "host_id": 777,
            "dph_total": 0.42,
            "start_date": 1_700_000_000.0,
            "actual_status": "loading",
            "intended_status": "running",
            "status_msg": "Pulling from registry",
            "disk_usage": 3.5,
            "an_unmodeled_field": {"nested": [1, 2, 3]}
        });
        let obs = instance_observation_from_value(999, v);

        assert_eq!(obs.id, 12345);
        assert_eq!(obs.gpu_name.as_deref(), Some("RTX 4090"));
        assert_eq!(obs.gpu_ram, Some(24576.0));
        assert_eq!(obs.actual_status.as_deref(), Some("loading"));
        assert_eq!(obs.status_msg.as_deref(), Some("Pulling from registry"));
        // Live util fields vast hasn't filled in yet stay None, not zero.
        assert_eq!(obs.gpu_util, None);
        // The unmodeled field survives for later mining.
        assert_eq!(obs.raw["an_unmodeled_field"]["nested"], serde_json::json!([1, 2, 3]));
    }

    #[test]
    fn falls_back_to_contract_id_when_json_lacks_id() {
        let obs = instance_observation_from_value(42, serde_json::Value::Null);
        assert_eq!(obs.id, 42);
        assert!(obs.gpu_name.is_none());
    }
}
