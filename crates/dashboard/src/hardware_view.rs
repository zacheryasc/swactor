use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};


use telemetry::hardware::cpu::{
    CpuCoreSample, CpuHostSample, CpuProcessSample, HOST_CPU_CHANNEL, HostCpuSample,
};
use telemetry::hardware::gpu::{
    GpuDeviceSample, GpuProcessSample, HOST_GPU_CHANNEL, HostGpuSample,
};
use telemetry::hardware::net::{HOST_NET_CHANNEL, HostNetSample, NetInterfaceSample};
use telemetry::Record;

use serde::Serialize;



const HISTORY_CAP: usize = 300;
const HISTORY_MIN_INTERVAL: Duration = Duration::from_millis(900);

/// Managed-process state folded from `proc.<label>.lifecycle` frames.
#[derive(Clone, Serialize)]
pub(crate) struct ProcessSnapshot {
    pub(crate) pid: Option<u32>,
    pub(crate) state: String,
}

pub(crate) struct NodeHardwareState {
    pub(crate) last_seen: Instant,
    decode_errors: BTreeMap<&'static str, String>,
    pub(crate) cpu: Option<HostCpuSample>,
    pub(crate) gpu: Option<HostGpuSample>,
    pub(crate) net: Option<NetSnapshot>,
    pub(crate) process: Option<ProcessSnapshot>,
    pub(crate) history: VecDeque<HardwareHistoryState>,
}

impl NodeHardwareState {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            last_seen: now,
            decode_errors: BTreeMap::new(),
            cpu: None,
            gpu: None,
            net: None,
            process: None,
            history: VecDeque::with_capacity(HISTORY_CAP),
        }
    }

    pub(crate) fn update(&mut self, channel: &str, payload: &[u8], now: Instant) {
        self.last_seen = now;
        match channel {
            HOST_CPU_CHANNEL => match HostCpuSample::decode(payload) {
                Ok(sample) => {
                    self.cpu = Some(sample);
                    self.decode_errors.remove(HOST_CPU_CHANNEL);
                    self.update_history(now);
                }
                Err(error) => self.store_decode_error(HOST_CPU_CHANNEL, error),
            },
            HOST_GPU_CHANNEL => match HostGpuSample::decode(payload) {
                Ok(sample) => {
                    self.gpu = Some(sample);
                    self.decode_errors.remove(HOST_GPU_CHANNEL);
                    self.update_history(now);
                }
                Err(error) => self.store_decode_error(HOST_GPU_CHANNEL, error),
            },
            HOST_NET_CHANNEL => match HostNetSample::decode(payload) {
                Ok(sample) => {
                    self.net = Some(NetSnapshot::from_sample(sample, self.net.as_ref()));
                    self.decode_errors.remove(HOST_NET_CHANNEL);
                    self.update_history(now);
                }
                Err(error) => self.store_decode_error(HOST_NET_CHANNEL, error),
            },
            _ => {
                if channel.starts_with("proc.") && channel.ends_with(".lifecycle") {
                    self.process = decode_process_snapshot(payload);
                    // Lifecycle frames also prove liveness.
                    self.last_seen = now;
                } else if channel == "node.status" {
                    // Liveness heartbeat from the supervisor.
                    self.last_seen = now;
                    if let Some(status) = decode_process_snapshot(payload) {
                        self.process.get_or_insert(status);
                    }
                }
            }
        }
    }

    fn store_decode_error(&mut self, channel: &'static str, error: serde_json::Error) {
        self.decode_errors
            .insert(channel, format!("{channel} decode error: {error}"));
    }

    pub(crate) fn summary(&self) -> HardwareSummary {
        let cpu_total_percent = self
            .cpu
            .as_ref()
            .and_then(|sample| sample.host.as_ref())
            .and_then(|host| host.total_percent);

        let mut gpu_max_percent = None;
        let mut gpu_memory_used_mib = 0_u64;
        let mut gpu_memory_total_mib = 0_u64;
        if let Some(gpu) = &self.gpu {
            for device in &gpu.gpus {
                if let Some(percent) = device.utilization_gpu_percent {
                    gpu_max_percent =
                        Some(gpu_max_percent.map_or(percent, |current: u64| current.max(percent)));
                }
                gpu_memory_used_mib = gpu_memory_used_mib
                    .saturating_add(device.memory_used_mib.unwrap_or_default());
                gpu_memory_total_mib = gpu_memory_total_mib
                    .saturating_add(device.memory_total_mib.unwrap_or_default());
            }
        }
        let (net_rx_bps, net_tx_bps) = self.net.as_ref().map_or((0.0, 0.0), |net| {
            net.interfaces.iter().fold((0.0, 0.0), |totals, interface| {
                (
                    totals.0 + interface.rx_bps.unwrap_or_default(),
                    totals.1 + interface.tx_bps.unwrap_or_default(),
                )
            })
        });

        HardwareSummary {
            sample_unix_ms: [
                self.cpu.as_ref().map(|sample| sample.sample_unix_ms),
                self.gpu.as_ref().map(|sample| sample.sample_unix_ms),
                self.net.as_ref().map(|sample| sample.sample_unix_ms),
            ]
            .into_iter()
            .flatten()
            .max(),
            cpu_total_percent,
            gpu_max_percent,
            gpu_memory_used_mib,
            gpu_memory_total_mib,
            net_rx_bps,
            net_tx_bps,
        }
    }

    fn update_history(&mut self, now: Instant) {
        let summary = self.summary();
        if let Some(last) = self.history.back_mut()
            && now.duration_since(last.at) < HISTORY_MIN_INTERVAL
        {
            last.sample_unix_ms = summary.sample_unix_ms;
            last.cpu_total_percent = summary.cpu_total_percent;
            last.gpu_max_percent = summary.gpu_max_percent;
            last.gpu_memory_used_mib = summary.gpu_memory_used_mib;
            last.gpu_memory_total_mib = summary.gpu_memory_total_mib;
            last.net_rx_bps = summary.net_rx_bps;
            last.net_tx_bps = summary.net_tx_bps;
            return;
        }

        if self.history.len() == HISTORY_CAP {
            self.history.pop_front();
        }
        self.history.push_back(HardwareHistoryState {
            at: now,
            sample_unix_ms: summary.sample_unix_ms,
            cpu_total_percent: summary.cpu_total_percent,
            gpu_max_percent: summary.gpu_max_percent,
            gpu_memory_used_mib: summary.gpu_memory_used_mib,
            gpu_memory_total_mib: summary.gpu_memory_total_mib,
            net_rx_bps: summary.net_rx_bps,
            net_tx_bps: summary.net_tx_bps,
        });
    }

    pub(crate) fn errors(&self) -> Vec<String> {
        let mut errors = self.decode_errors.values().cloned().collect::<Vec<_>>();
        if let Some(error) = self.cpu.as_ref().and_then(|sample| sample.error.as_ref()) {
            errors.push(format!("{HOST_CPU_CHANNEL}: {error}"));
        }
        if let Some(error) = self.gpu.as_ref().and_then(|sample| sample.error.as_ref()) {
            errors.push(format!("{HOST_GPU_CHANNEL}: {error}"));
        }
        if let Some(error) = self.net.as_ref().and_then(|sample| sample.error.as_ref()) {
            errors.push(format!("{HOST_NET_CHANNEL}: {error}"));
        }
        errors
    }
}

/// Decode a `swactor_process.lifecycle.v1` payload into fleet-card state.
pub(crate) fn decode_process_snapshot(payload: &[u8]) -> Option<ProcessSnapshot> {
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    let pid = value
        .get("pid")
        .and_then(serde_json::Value::as_u64)
        .map(|pid| pid as u32);
    let state = match value.get("event").and_then(serde_json::Value::as_str) {
        Some("started") => "running",
        Some("exited") => "exited",
        Some("spawn_failed") | Some("error") => "failed",
        Some(other) => other,
        None => match value.get("alive").and_then(serde_json::Value::as_bool) {
            Some(true) => "running",
            Some(false) => "exited",
            None => return None,
        },
    }
    .to_owned();
    Some(ProcessSnapshot { pid, state })
}

#[derive(Clone, Serialize)]
pub(crate) struct NetSnapshot {
    seq: u64,
    sample_unix_ms: u64,
    pub(crate) interfaces: Vec<NetInterfaceSnapshot>,
    error: Option<String>,
}

impl NetSnapshot {
    fn from_sample(sample: HostNetSample, previous: Option<&Self>) -> Self {
        let previous_interfaces = previous
            .map(|net| {
                net.interfaces
                    .iter()
                    .map(|interface| (interface.name.as_str(), interface))
                    .collect::<BTreeMap<_, _>>()
            })
            .unwrap_or_default();
        let elapsed_secs = previous.and_then(|net| {
            sample
                .sample_unix_ms
                .checked_sub(net.sample_unix_ms)
                .filter(|elapsed_ms| *elapsed_ms > 0)
                .map(|elapsed_ms| elapsed_ms as f64 / 1000.0)
        });
        let interfaces = sample
            .interfaces
            .into_iter()
            .map(|interface| {
                let previous = previous_interfaces.get(interface.name.as_str()).copied();
                NetInterfaceSnapshot::from_sample(interface, previous, elapsed_secs)
            })
            .collect();

        Self {
            seq: sample.seq,
            sample_unix_ms: sample.sample_unix_ms,
            interfaces,
            error: sample.error,
        }
    }
}
#[derive(Clone, Serialize)]
pub(crate) struct NetInterfaceSnapshot {
    pub(crate) name: String,
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
    rx_errors: u64,
    tx_errors: u64,
    rx_dropped: u64,
    tx_dropped: u64,
    pub(crate) rx_bps: Option<f64>,
    pub(crate) tx_bps: Option<f64>,
    rx_pps: Option<f64>,
    tx_pps: Option<f64>,
}

impl NetInterfaceSnapshot {
    fn from_sample(
        sample: NetInterfaceSample,
        previous: Option<&Self>,
        elapsed_secs: Option<f64>,
    ) -> Self {
        let rate = |new: u64, old: u64| {
            elapsed_secs.and_then(|elapsed| {
                new.checked_sub(old)
                    .map(|difference| difference as f64 / elapsed)
            })
        };
        let (rx_bps, tx_bps, rx_pps, tx_pps) =
            previous.map_or((None, None, None, None), |previous| {
                (
                    rate(sample.rx_bytes, previous.rx_bytes),
                    rate(sample.tx_bytes, previous.tx_bytes),
                    rate(sample.rx_packets, previous.rx_packets),
                    rate(sample.tx_packets, previous.tx_packets),
                )
            });

        Self {
            name: sample.name,
            rx_bytes: sample.rx_bytes,
            tx_bytes: sample.tx_bytes,
            rx_packets: sample.rx_packets,
            tx_packets: sample.tx_packets,
            rx_errors: sample.rx_errors,
            tx_errors: sample.tx_errors,
            rx_dropped: sample.rx_dropped,
            tx_dropped: sample.tx_dropped,
            rx_bps,
            tx_bps,
            rx_pps,
            tx_pps,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct HardwareSummary {
    pub(crate) sample_unix_ms: Option<u64>,
    pub(crate) cpu_total_percent: Option<f64>,
    pub(crate) gpu_max_percent: Option<u64>,
    pub(crate) gpu_memory_used_mib: u64,
    pub(crate) gpu_memory_total_mib: u64,
    pub(crate) net_rx_bps: f64,
    pub(crate) net_tx_bps: f64,
}

pub(crate) struct HardwareHistoryState {
    pub(crate) at: Instant,
    pub(crate) sample_unix_ms: Option<u64>,
    pub(crate) cpu_total_percent: Option<f64>,
    pub(crate) gpu_max_percent: Option<u64>,
    pub(crate) gpu_memory_used_mib: u64,
    pub(crate) gpu_memory_total_mib: u64,
    pub(crate) net_rx_bps: f64,
    pub(crate) net_tx_bps: f64,
}


#[derive(Serialize)]
pub(crate) struct CpuSnapshot {
    pub(crate) seq: u64,
    pub(crate) sample_unix_ms: u64,
    pub(crate) query_elapsed_ms: Option<u64>,
    pub(crate) host: Option<CpuHostSample>,
    pub(crate) cores: Vec<CpuCoreSample>,
    pub(crate) processes: Vec<CpuProcessSample>,
    pub(crate) error: Option<String>,
}

impl From<&HostCpuSample> for CpuSnapshot {
    fn from(sample: &HostCpuSample) -> Self {
        Self {
            seq: sample.seq,
            sample_unix_ms: sample.sample_unix_ms,
            query_elapsed_ms: sample.query_elapsed_ms,
            host: sample.host.clone(),
            cores: sample.cores.clone(),
            processes: sample.processes.clone(),
            error: sample.error.clone(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct GpuSnapshot {
    pub(crate) seq: u64,
    pub(crate) sample_unix_ms: u64,
    pub(crate) query_elapsed_ms: Option<u64>,
    pub(crate) gpus: Vec<GpuDeviceSample>,
    pub(crate) processes: Vec<GpuProcessSample>,
    pub(crate) error: Option<String>,
}

impl From<&HostGpuSample> for GpuSnapshot {
    fn from(sample: &HostGpuSample) -> Self {
        Self {
            seq: sample.seq,
            sample_unix_ms: sample.sample_unix_ms,
            query_elapsed_ms: sample.query_elapsed_ms,
            gpus: sample.gpus.clone(),
            processes: sample.processes.clone(),
            error: sample.error.clone(),
        }
    }
}

#[derive(Serialize)]
pub(crate) struct HardwareHistorySnapshot {
    pub(crate) ms_ago: u64,
    pub(crate) sample_unix_ms: Option<u64>,
    pub(crate) cpu_total_percent: Option<f64>,
    pub(crate) gpu_max_percent: Option<u64>,
    pub(crate) gpu_memory_used_mib: u64,
    pub(crate) gpu_memory_total_mib: u64,
    pub(crate) net_rx_bps: f64,
    pub(crate) net_tx_bps: f64,
}


pub(crate) fn duration_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

pub(crate) fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

