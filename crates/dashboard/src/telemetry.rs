//! Dashboard-owned datastream channel records and local samplers.
//!
//! These records describe what the dashboard/fleet views consume. They are not a
//! datastream catalog: applications opt into them by importing this module and
//! submitting the records through `datastream`'s generic APIs.

use std::time::Instant;

use datastream::{ChannelId, Record};
use serde::{Deserialize, Serialize};

/// Identity / boot — emitted first in a stream; identifies the node and context.
pub const IDENTITY: &str = "identity";
/// Host / resource samples — periodic snapshots of machine resources.
pub const HOST_RESOURCE: &str = "host.resource";
/// Runtime stats — aggregate actor-runtime metrics.
pub const RUNTIME_STATS: &str = "runtime.stats";
/// Per-actor runtime detail — one row per live actor.
pub const RUNTIME_ACTORS: &str = "runtime.actors";
/// Worker-runtime counters — routing/error tallies and tick timing.
pub const RUNTIME_WORKERS: &str = "runtime.workers";

/// Which standard stream a span of process output came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcStream {
    Stdout,
    Stderr,
}

impl ProcStream {
    pub fn as_str(self) -> &'static str {
        match self {
            ProcStream::Stdout => "stdout",
            ProcStream::Stderr => "stderr",
        }
    }
}

/// A raw-text process-output channel: `proc.<label>.stdout|stderr`.
pub fn process_output(label: &str, stream: ProcStream) -> ChannelId {
    ChannelId::new(format!("proc.{label}.{}", stream.as_str()))
}

/// Adapter for `DatastreamEmitter::process_observer_with`.
pub fn process_output_for_observer(label: &str, is_stderr: bool) -> ChannelId {
    let stream = if is_stderr {
        ProcStream::Stderr
    } else {
        ProcStream::Stdout
    };
    process_output(label, stream)
}

/// Identity / boot record.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdentityRecord {
    pub node: String,
    #[serde(default)]
    pub life: u64,
    #[serde(default)]
    pub node_name: String,
    #[serde(default)]
    pub listen_addr: String,
    #[serde(default)]
    pub relay_url: String,
    #[serde(default)]
    pub version: String,
}

/// Host / resource sample.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResourceSample {
    #[serde(default)]
    pub cpu_pct: f32,
    #[serde(default)]
    pub mem_used_mb: u32,
    #[serde(default)]
    pub mem_total_mb: u32,
    #[serde(default)]
    pub gpu_pct: f32,
    #[serde(default)]
    pub disk_used_gb: u32,
    #[serde(default)]
    pub net_rx_kbps: u32,
    #[serde(default)]
    pub net_tx_kbps: u32,
}

/// Aggregate runtime stats.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeStats {
    #[serde(default)]
    pub actors_live: u32,
    #[serde(default)]
    pub mailbox_depth: u32,
    #[serde(default)]
    pub scheduled_tasks: u32,
}

/// Per-actor runtime detail.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRuntimeDetail {
    #[serde(default)]
    pub actors: Vec<ActorRec>,
}

/// One live actor's stats.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActorRec {
    #[serde(default)]
    pub address: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub mailbox_depth: u32,
    #[serde(default)]
    pub messages_processed: u64,
    #[serde(default)]
    pub last_msg_type: String,
    #[serde(default)]
    pub poisoned: bool,
    #[serde(default)]
    pub message_type_counts: Vec<(String, u64)>,
}

/// Worker-runtime counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCounters {
    #[serde(default)]
    pub num_workers: u32,
    #[serde(default)]
    pub scheduled_tasks: u32,
    #[serde(default)]
    pub local_sends: u64,
    #[serde(default)]
    pub cross_sends: u64,
    #[serde(default)]
    pub inbox_sends: u64,
    #[serde(default)]
    pub type_mismatches: u64,
    #[serde(default)]
    pub panics: u64,
    #[serde(default)]
    pub messages_dropped: u64,
    #[serde(default)]
    pub restarts: u64,
    #[serde(default)]
    pub stops: u64,
    #[serde(default)]
    pub messages_processed: u64,
    #[serde(default)]
    pub tick_p50_us: u64,
}

impl Record for IdentityRecord {
    const CHANNEL: &'static str = IDENTITY;
}
impl Record for ResourceSample {
    const CHANNEL: &'static str = HOST_RESOURCE;
}
impl Record for RuntimeStats {
    const CHANNEL: &'static str = RUNTIME_STATS;
}
impl Record for ActorRuntimeDetail {
    const CHANNEL: &'static str = RUNTIME_ACTORS;
}
impl Record for WorkerCounters {
    const CHANNEL: &'static str = RUNTIME_WORKERS;
}

/// Build the identity record a node emits first on its stream.
pub fn identity_record(node: &str, life: u64) -> IdentityRecord {
    IdentityRecord {
        node: node.to_string(),
        life,
        ..Default::default()
    }
}

/// Samples host CPU usage across calls.
#[derive(Default)]
pub struct CpuSampler {
    prev: Option<(u64, Instant)>,
}

impl CpuSampler {
    pub fn sample(&mut self) -> f32 {
        let Some(now_busy) = read_proc_stat_busy_jiffies() else {
            return 0.0;
        };
        let now = Instant::now();
        let out = if let Some((prev_busy, prev_at)) = self.prev {
            let busy = now_busy.saturating_sub(prev_busy) as f64;
            let secs = now.duration_since(prev_at).as_secs_f64().max(0.001);
            let cpus = num_cpus().max(1) as f64;
            ((busy / clock_ticks_per_sec()) / secs / cpus * 100.0).clamp(0.0, 100.0) as f32
        } else {
            0.0
        };
        self.prev = Some((now_busy, now));
        out
    }
}

/// Samples host network throughput across calls.
#[derive(Default)]
pub struct NetSampler {
    prev: Option<((u64, u64), Instant)>,
}

impl NetSampler {
    pub fn sample(&mut self) -> (u32, u32) {
        let Some(now_bytes) = read_net_bytes() else {
            return (0, 0);
        };
        let now = Instant::now();
        let out = if let Some(((prev_rx, prev_tx), prev_at)) = self.prev {
            let secs = now.duration_since(prev_at).as_secs_f64().max(0.001);
            let rx = now_bytes.0.saturating_sub(prev_rx) as f64 / secs / 1024.0;
            let tx = now_bytes.1.saturating_sub(prev_tx) as f64 / secs / 1024.0;
            (
                rx.min(u32::MAX as f64) as u32,
                tx.min(u32::MAX as f64) as u32,
            )
        } else {
            (0, 0)
        };
        self.prev = Some((now_bytes, now));
        out
    }
}

/// Host-resource sampler state carried between ticks.
pub struct HostSampler {
    cpu: CpuSampler,
    net: NetSampler,
}

impl HostSampler {
    pub fn new() -> Self {
        Self::default()
    }
}

impl Default for HostSampler {
    fn default() -> Self {
        Self {
            cpu: CpuSampler::default(),
            net: NetSampler::default(),
        }
    }
}

/// Read a host-resource sample from real host sources where available.
pub fn read_host_resource(sampler: &mut HostSampler) -> ResourceSample {
    let cpu_pct = sampler.cpu.sample();
    let (net_rx_kbps, net_tx_kbps) = sampler.net.sample();
    let (mem_total_mb, mem_used_mb) = read_meminfo_mb().unwrap_or((0, 0));
    ResourceSample {
        cpu_pct,
        mem_used_mb,
        mem_total_mb,
        gpu_pct: read_gpu_pct(),
        disk_used_gb: read_disk_used_gb(),
        net_rx_kbps,
        net_tx_kbps,
    }
}

fn clock_ticks_per_sec() -> f64 {
    #[cfg(target_os = "linux")]
    unsafe {
        let ticks = libc::sysconf(libc::_SC_CLK_TCK);
        if ticks > 0 {
            return ticks as f64;
        }
    }
    100.0
}

fn num_cpus() -> u64 {
    #[cfg(target_os = "linux")]
    {
        if let Ok(s) = std::fs::read_to_string("/proc/stat") {
            let n = s
                .lines()
                .filter(|line| {
                    let rest = line.strip_prefix("cpu");
                    rest.is_some_and(|r| r.chars().next().is_some_and(|c| c.is_ascii_digit()))
                })
                .count() as u64;
            if n > 0 {
                return n;
            }
        }
    }
    std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1)
}

fn read_proc_stat_busy_jiffies() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/stat").ok()?;
        let line = s.lines().find(|l| l.starts_with("cpu "))?;
        let mut vals = line
            .split_whitespace()
            .skip(1)
            .filter_map(|v| v.parse::<u64>().ok());
        let user = vals.next()?;
        let nice = vals.next()?;
        let system = vals.next()?;
        let _idle = vals.next()?;
        let _iowait = vals.next().unwrap_or(0);
        let irq = vals.next().unwrap_or(0);
        let softirq = vals.next().unwrap_or(0);
        let steal = vals.next().unwrap_or(0);
        Some(user + nice + system + irq + softirq + steal)
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn read_disk_used_gb() -> u32 {
    #[cfg(target_os = "linux")]
    unsafe {
        let path = std::ffi::CString::new("/").expect("static path");
        let mut st: libc::statvfs = std::mem::zeroed();
        if libc::statvfs(path.as_ptr(), &mut st) == 0 {
            let total = st.f_blocks as u128 * st.f_frsize as u128;
            let free = st.f_bfree as u128 * st.f_frsize as u128;
            return ((total.saturating_sub(free)) / 1_000_000_000).min(u32::MAX as u128) as u32;
        }
    }
    0
}

fn read_net_bytes() -> Option<(u64, u64)> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/net/dev").ok()?;
        let mut rx = 0u64;
        let mut tx = 0u64;
        for line in s.lines().skip(2) {
            let (iface, rest) = line.split_once(':')?;
            if iface.trim() == "lo" {
                continue;
            }
            let vals: Vec<&str> = rest.split_whitespace().collect();
            if vals.len() >= 16 {
                rx = rx.saturating_add(vals[0].parse::<u64>().unwrap_or(0));
                tx = tx.saturating_add(vals[8].parse::<u64>().unwrap_or(0));
            }
        }
        Some((rx, tx))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}

fn read_gpu_pct() -> f32 {
    #[cfg(target_os = "linux")]
    {
        if std::path::Path::new("/proc/driver/nvidia/gpus")
            .read_dir()
            .map(|mut it| it.next().is_some())
            .unwrap_or(false)
        {
            return query_nvidia_smi_util().unwrap_or(0.0);
        }
    }
    0.0
}

#[cfg(target_os = "linux")]
fn query_nvidia_smi_util() -> Option<f32> {
    let out = std::process::Command::new("nvidia-smi")
        .args([
            "--query-gpu=utilization.gpu",
            "--format=csv,noheader,nounits",
        ])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?;
    let vals: Vec<f32> = s
        .lines()
        .filter_map(|l| l.trim().parse::<f32>().ok())
        .collect();
    if vals.is_empty() {
        None
    } else {
        Some(vals.iter().sum::<f32>() / vals.len() as f32)
    }
}

fn read_meminfo_mb() -> Option<(u32, u32)> {
    #[cfg(target_os = "linux")]
    {
        let s = std::fs::read_to_string("/proc/meminfo").ok()?;
        let mut total = None;
        let mut avail = None;
        for line in s.lines() {
            if let Some(rest) = line.strip_prefix("MemTotal:") {
                total = rest.split_whitespace().next()?.parse::<u64>().ok();
            } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
                avail = rest.split_whitespace().next()?.parse::<u64>().ok();
            }
        }
        let total = total? / 1024;
        let avail = avail? / 1024;
        Some((total as u32, total.saturating_sub(avail) as u32))
    }
    #[cfg(not(target_os = "linux"))]
    {
        None
    }
}
