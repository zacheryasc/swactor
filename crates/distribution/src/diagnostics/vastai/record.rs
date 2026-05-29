//! Wire schema for the vastai monitoring layer.
//!
//! This is an *independent* telemetry layer: it tracks the state of rented
//! vast.ai GPU nodes and carries no swactor (actor / SWIM / iroh) concepts. The
//! records here are serialized as the opaque JSON body the collector stores under
//! the `vastai_*` [`RecordKind`](crate::diagnostics::collector::RecordKind)s, and
//! they never pass through the swactor [`Aggregator`](crate::diagnostics::Aggregator)
//! or its `Event` enum.
//!
//! Two producers emit these records:
//!   * the **external poller** in the orchestrator, which pulls the vast.ai REST
//!     API and ships an [`InstanceObservation`] per contract per tick — including
//!     the pre-`running` image-pull window the in-VM sampler can't observe;
//!   * the **in-VM sampler** inside each rented container, which ships a
//!     [`HostSample`] per tick plus live [`LogBatch`]es.
//!
//! "Collect everything, prune later": every payload field is optional and the
//! external observation keeps the raw vast JSON verbatim, so nothing we failed to
//! model is lost.

use serde::{Deserialize, Serialize};

/// Current schema version. Bump only for breaking changes; new fields are added
/// as `Option`/`#[serde(default)]` so old readers tolerate new writers.
pub const VASTAI_SCHEMA_VERSION: u32 = 1;

/// Which producer emitted a record.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    /// The orchestrator-side REST poller (vast.ai API view).
    External,
    /// The in-container sampler (OS / GPU / log view).
    InVm,
}

/// The logical vastai node a record is about. Carried inside every record body so
/// the external view (which knows the contract id from leasing) and the in-VM view
/// (which knows its stage index from env) can be joined after the fact, regardless
/// of which synthetic collector `node_id` each was filed under.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct VastaiNodeRef {
    /// vast.ai contract/instance id — vast's source of truth.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_id: Option<u64>,
    /// Pipeline stage slot, correlating external ↔ in-VM ↔ swactor stage.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stage_index: Option<u32>,
    /// The vast `--label` set at instance creation, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// One vastai observation on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VastaiRecord {
    /// Schema version (see [`VASTAI_SCHEMA_VERSION`]).
    pub v: u32,
    pub vastai_node: VastaiNodeRef,
    pub source: Source,
    /// Producer-local monotonic sequence — orders records from one producer
    /// across body kinds (the collector also assigns its own per-kind seq).
    pub seq: u64,
    /// Producer wall clock at capture (ms since epoch).
    pub wall_ms: u64,
    pub body: VastaiBody,
}

impl VastaiRecord {
    /// The [`RecordKind`](crate::diagnostics::collector::RecordKind) this record
    /// ships under, derived from its body. Keeps the body→kind mapping in one
    /// place so the shipper can't mis-route.
    #[cfg(feature = "collector")]
    pub fn kind(&self) -> crate::diagnostics::collector::RecordKind {
        use crate::diagnostics::collector::RecordKind;
        match self.body {
            VastaiBody::Instance(_) => RecordKind::VastaiInstance,
            VastaiBody::HostSample(_) => RecordKind::VastaiSample,
            VastaiBody::Logs(_) => RecordKind::VastaiLogs,
            VastaiBody::Lifecycle(_) => RecordKind::VastaiLifecycle,
        }
    }
}

/// The payload of a [`VastaiRecord`].
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum VastaiBody {
    /// External: full vast.ai instance/contract view (one per poll per contract).
    Instance(InstanceObservation),
    /// In-VM: one host metrics sample (one per sampler tick).
    HostSample(HostSample),
    /// In-VM (or vast `request_logs` at teardown): a batch of framed log lines.
    Logs(LogBatch),
    /// Lifecycle markers: deploy start, contract leased, teardown.
    Lifecycle(LifecycleEvent),
}

/// Full external view of one vast.ai instance. All fields optional so a partial
/// or in-flux instance (e.g. still pulling its image) still ships; `raw` keeps the
/// verbatim vast JSON so unmodeled fields survive.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct InstanceObservation {
    /// Contract/instance id (vast's primary key for the rental).
    pub id: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_ram: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_gpus: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub geolocation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub machine_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dph_total: Option<f64>,
    /// Accumulated cost so far for this rental, if vast reports it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub accumulated_cost: Option<f64>,
    /// Epoch seconds the instance started — time-since-deploy is derived from this.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_date: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub actual_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub intended_status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_msg: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_usage: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub disk_space: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub public_ipaddr: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ssh_port: Option<u16>,
    // Live utilization vast returns once the instance is running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_util: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub gpu_temp: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_util: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_usage: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inet_up: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub inet_down: Option<f64>,
    /// The raw vast JSON for this instance, verbatim — the "collect everything"
    /// hedge so any field we didn't model is still in the bundle.
    #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
    pub raw: serde_json::Value,
}

/// One atomic in-VM metrics tick. Rates (net/disk/cpu) are pre-computed by the
/// sampler from counter deltas so consumers don't need adjacent samples.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct HostSample {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub gpus: Vec<GpuSample>,
    pub cpu: CpuSample,
    pub mem: MemSample,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub disk: Vec<DiskSample>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub net: Vec<NetSample>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cgroup: Option<CgroupSample>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct GpuSample {
    pub index: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub util_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_used_mb: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_total_mb: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temp_c: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_w: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub power_limit_w: Option<f64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CpuSample {
    /// Aggregate CPU utilization since the previous tick, 0..100.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub util_pct: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load1: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load5: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load15: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub num_cpus: Option<u32>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemSample {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub total_kb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub available_kb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_kb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap_total_kb: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub swap_free_kb: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DiskSample {
    pub device: String,
    /// Read/write throughput since the previous tick.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_bytes_per_s: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_bytes_per_s: Option<f64>,
    /// Filesystem usage for a mounted device (when sampled).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs_total_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fs_used_bytes: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NetSample {
    pub iface: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rx_bytes_per_s: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_bytes_per_s: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rx_bytes_total: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tx_bytes_total: Option<u64>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CgroupSample {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_limit_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mem_current_bytes: Option<u64>,
    /// CPU quota as cores (e.g. 2.0); `None` when unlimited.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_quota_cores: Option<f64>,
}

/// Which output stream a [`LogLine`] came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LogStream {
    Stdout,
    Stderr,
    /// Logs fetched out-of-band from vast's `request_logs` endpoint.
    VastRequestLogs,
}

/// A batch of log lines from one stream, framed for ordered delivery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogBatch {
    pub stream: LogStream,
    pub lines: Vec<LogLine>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogLine {
    /// Producer wall clock when the line was observed.
    pub wall_ms: u64,
    /// Monotonic line number within (node, stream) for exact ordering.
    pub line: u64,
    /// Line text, already truncated by the forwarder.
    pub text: String,
}

/// Coarse lifecycle markers for a deploy / contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum LifecycleEvent {
    /// The orchestrator began a deploy (emitted once, before leasing).
    DeployStart {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        num_stages: Option<u32>,
    },
    /// A contract was leased for a stage.
    ContractLeased {
        contract_id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offer_id: Option<u64>,
    },
    /// A contract is being torn down.
    Teardown {
        contract_id: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_ref() -> VastaiNodeRef {
        VastaiNodeRef {
            contract_id: Some(42),
            stage_index: Some(0),
            label: Some("pp-run-0".into()),
        }
    }

    fn round_trip(body: VastaiBody) -> VastaiRecord {
        let rec = VastaiRecord {
            v: VASTAI_SCHEMA_VERSION,
            vastai_node: node_ref(),
            source: Source::External,
            seq: 7,
            wall_ms: 1234,
            body,
        };
        let json = serde_json::to_string(&rec).expect("serialize");
        serde_json::from_str(&json).expect("deserialize")
    }

    #[test]
    fn every_body_variant_round_trips_through_json() {
        // A consumer reading the bundle must be able to recover each body kind
        // from the stored JSON without losing the discriminant.
        let bodies = vec![
            VastaiBody::Instance(InstanceObservation {
                id: 42,
                gpu_name: Some("RTX 4090".into()),
                raw: serde_json::json!({"unmodeled": true}),
                ..Default::default()
            }),
            VastaiBody::HostSample(HostSample {
                gpus: vec![GpuSample {
                    index: 0,
                    util_pct: Some(73.5),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            VastaiBody::Logs(LogBatch {
                stream: LogStream::Stderr,
                lines: vec![LogLine {
                    wall_ms: 1,
                    line: 0,
                    text: "boom".into(),
                }],
            }),
            VastaiBody::Lifecycle(LifecycleEvent::Teardown {
                contract_id: 42,
                reason: Some("done".into()),
            }),
        ];
        for body in bodies {
            let back = round_trip(body.clone());
            // Discriminant survives the round trip.
            assert_eq!(
                std::mem::discriminant(&back.body),
                std::mem::discriminant(&body)
            );
            assert_eq!(back.vastai_node.contract_id, Some(42));
        }
    }

    #[test]
    fn unmodeled_vast_fields_survive_in_raw() {
        // "Collect everything": a field we never modeled must still be present
        // after a round trip so it can be mined later.
        let obs = InstanceObservation {
            id: 1,
            raw: serde_json::json!({"some_future_field": [1, 2, 3]}),
            ..Default::default()
        };
        let json = serde_json::to_string(&obs).unwrap();
        let back: InstanceObservation = serde_json::from_str(&json).unwrap();
        assert_eq!(back.raw["some_future_field"], serde_json::json!([1, 2, 3]));
    }
}
