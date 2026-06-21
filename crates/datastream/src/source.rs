//! Producer-side helpers: map a node's live state into the typed records of
//! the [`catalog`](super::catalog).
//!
//! These turn data the node already has — host counters, a runtime-stats
//! snapshot, the membership view — into the records a producer submits to its
//! mux. Nothing here touches the pipe; it is pure "live state in, record out"
//! so it can be unit-tested without a transport, and it deliberately carries
//! no dependency on the cluster transport (iroh) so it builds with any feature
//! set.

use std::collections::HashMap;
use std::time::Instant;

use super::catalog::{IdentityRecord, MembershipTransition, ResourceSample};

/// Build the identity record a node emits first on its stream (spec §6.1). The
/// descriptive fields (name, listen addr, relay, version) are learned lazily;
/// they start empty and the node re-emits a fuller identity via
/// [`DatastreamEmitter::update_identity`](super::emit::DatastreamEmitter::update_identity)
/// once it knows them.
pub fn identity_record(node: &str, life: u64) -> IdentityRecord {
    IdentityRecord {
        node: node.to_string(),
        life,
        ..Default::default()
    }
}

/// Samples host CPU usage across calls. CPU percent is a rate, so it needs two
/// observations to compute; the first call seeds the baseline and reports 0.
#[derive(Default)]
pub struct CpuSampler {
    prev: Option<(u64, Instant)>,
}

impl CpuSampler {
    pub fn new() -> Self {
        Self::default()
    }

    /// Host-average CPU busy percent since the previous call, in `[0, 100]`.
    /// Returns 0 on the first call (no baseline yet) and on any platform
    /// where `/proc/stat` is unavailable.
    pub fn sample(&mut self) -> f32 {
        let now = Instant::now();
        let busy = match read_proc_stat_busy_jiffies() {
            Some(j) => j,
            None => return 0.0,
        };
        let pct = match self.prev {
            Some((prev_busy, prev_at)) => {
                let elapsed = now.duration_since(prev_at).as_secs_f64();
                if elapsed <= 0.0 {
                    0.0
                } else {
                    // Jiffies are USER_HZ (typically 100/s) per CPU, and the
                    // aggregate `cpu` line sums every CPU — normalize by wall
                    // time, the tick rate, AND the CPU count, or any host
                    // with one busy core reads as a flat 100%.
                    let delta = busy.saturating_sub(prev_busy) as f64;
                    let hz = clock_ticks_per_sec();
                    let frac = delta / (hz * elapsed * num_cpus().max(1) as f64);
                    (frac * 100.0).clamp(0.0, 100.0) as f32
                }
            }
            None => 0.0,
        };
        self.prev = Some((busy, now));
        pct
    }
}

/// Samples host network throughput across calls. Like [`CpuSampler`], the rate
/// needs two observations: the first call seeds the byte baseline and reports 0.
#[derive(Default)]
pub struct NetSampler {
    /// `(rx_bytes_total, tx_bytes_total, observed_at)` from the previous call.
    prev: Option<(u64, u64, Instant)>,
}

impl NetSampler {
    pub fn new() -> Self {
        Self::default()
    }

    /// `(rx_kbps, tx_kbps)` since the previous call — **kilobits per second**
    /// summed across every non-loopback interface. Returns `(0, 0)` on the first
    /// call (no baseline) and on any platform without `/proc/net/dev`.
    pub fn sample(&mut self) -> (u32, u32) {
        let now = Instant::now();
        let (rx, tx) = match read_net_bytes() {
            Some(v) => v,
            None => return (0, 0),
        };
        let out = match self.prev {
            Some((prev_rx, prev_tx, prev_at)) => {
                let elapsed = now.duration_since(prev_at).as_secs_f64();
                if elapsed <= 0.0 {
                    (0, 0)
                } else {
                    let to_kbps = |delta: u64| {
                        ((delta as f64) * 8.0 / 1000.0 / elapsed).clamp(0.0, u32::MAX as f64) as u32
                    };
                    (
                        to_kbps(rx.saturating_sub(prev_rx)),
                        to_kbps(tx.saturating_sub(prev_tx)),
                    )
                }
            }
            None => (0, 0),
        };
        self.prev = Some((rx, tx, now));
        out
    }
}

/// All the host-resource sampler state a node carries between ticks: the CPU
/// rate baseline and the network byte baseline. Disk and GPU are point reads, so
/// they need no state.
pub struct HostSampler {
    cpu: CpuSampler,
    net: NetSampler,
}

impl HostSampler {
    pub fn new() -> Self {
        Self {
            cpu: CpuSampler::new(),
            net: NetSampler::new(),
        }
    }
}

impl Default for HostSampler {
    fn default() -> Self {
        Self::new()
    }
}

/// Read a host-resource sample: CPU busy percent and network throughput (rates,
/// via `sampler`'s baselines), memory from `/proc/meminfo`, disk usage from
/// `statvfs`, and GPU utilization best-effort (NVML/`nvidia-smi`). Every field
/// has a real host source; on a CPU-only box `gpu_pct` is an honest `0` (no GPU
/// present — never a fabricated load), and off-Linux the whole sample defaults.
pub fn read_host_resource(sampler: &mut HostSampler) -> ResourceSample {
    let cpu_pct = sampler.cpu.sample();
    let (mem_total_mb, mem_used_mb) = read_meminfo_mb().unwrap_or((0, 0));
    let (net_rx_kbps, net_tx_kbps) = sampler.net.sample();
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

/// Tracks the last-seen membership state per peer and emits a transition only
/// when a peer's state changes — membership is an event-driven channel
/// (spec §6.1), so a steady cluster produces no frames.
#[derive(Default)]
pub struct MembershipTracker {
    last: HashMap<String, String>,
}

impl MembershipTracker {
    pub fn new() -> Self {
        Self::default()
    }

    /// Diff the current `(peer, state)` view against the last one. A newly seen
    /// peer transitions from `"unknown"`; a peer whose state is unchanged
    /// produces nothing.
    pub fn diff(&mut self, members: &[(String, String)]) -> Vec<MembershipTransition> {
        let mut out = Vec::new();
        for (peer, state) in members {
            let changed = match self.last.get(peer) {
                Some(prev) => prev != state,
                None => true,
            };
            if changed {
                let from = self
                    .last
                    .get(peer)
                    .cloned()
                    .unwrap_or_else(|| "unknown".to_string());
                out.push(MembershipTransition {
                    peer: peer.clone(),
                    from,
                    to: state.clone(),
                    reason: String::new(),
                });
                self.last.insert(peer.clone(), state.clone());
            }
        }
        out
    }
}

// ── Host counters (Linux /proc) ────────────────────────────────────────────

fn clock_ticks_per_sec() -> f64 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: sysconf is a pure lookup with no preconditions.
        let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
        if hz > 0 {
            return hz as f64;
        }
    }
    100.0
}

/// CPU count for normalizing the aggregate busy-jiffies line. Counted from
/// the per-CPU `cpuN` lines of `/proc/stat` itself — the exact set the
/// aggregate line sums — NOT `available_parallelism()`, which reflects
/// cgroup quotas inside a container while `/proc/stat` still reports the
/// whole host. Falls back to `available_parallelism()` off-Linux.
fn num_cpus() -> u64 {
    let from_proc = std::fs::read_to_string("/proc/stat")
        .map(|text| {
            text.lines()
                .filter(|l| {
                    l.strip_prefix("cpu")
                        .and_then(|r| r.chars().next())
                        .is_some_and(|c| c.is_ascii_digit())
                })
                .count() as u64
        })
        .unwrap_or(0);
    if from_proc > 0 {
        return from_proc;
    }
    std::thread::available_parallelism()
        .map(|n| n.get() as u64)
        .unwrap_or(1)
}

/// Total non-idle jiffies from the aggregate `cpu` line of `/proc/stat`.
fn read_proc_stat_busy_jiffies() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/stat").ok()?;
    let line = text.lines().next()?; // the aggregate "cpu  ..." line
    let mut it = line.split_whitespace();
    if it.next()? != "cpu" {
        return None;
    }
    // user nice system idle iowait irq softirq steal guest guest_nice
    let vals: Vec<u64> = it.filter_map(|t| t.parse::<u64>().ok()).collect();
    if vals.len() < 4 {
        return None;
    }
    let total: u64 = vals.iter().sum();
    let idle = vals[3] + vals.get(4).copied().unwrap_or(0); // idle + iowait
    Some(total.saturating_sub(idle))
}

/// Used disk space in whole gigabytes (base-10) of the root filesystem, via
/// `statvfs("/")`. In a container the root overlay reports its backing store, so
/// this is a real reading. `0` on failure or off-Linux.
fn read_disk_used_gb() -> u32 {
    #[cfg(target_os = "linux")]
    {
        // SAFETY: `statvfs` only reads into the zeroed-out struct we provide, and
        // `"/"` is always a valid, NUL-terminated C path.
        unsafe {
            let mut stat: libc::statvfs = std::mem::zeroed();
            if libc::statvfs(b"/\0".as_ptr().cast(), &mut stat) == 0 {
                let frsize = stat.f_frsize as u64;
                let used_blocks = (stat.f_blocks as u64).saturating_sub(stat.f_bfree as u64);
                let used_bytes = used_blocks.saturating_mul(frsize);
                return (used_bytes / 1_000_000_000) as u32;
            }
        }
    }
    0
}

/// `(rx_bytes_total, tx_bytes_total)` summed across every interface except
/// loopback, from `/proc/net/dev`. `None` off-Linux or if the file is unreadable.
fn read_net_bytes() -> Option<(u64, u64)> {
    #[cfg(target_os = "linux")]
    {
        if let Ok(text) = std::fs::read_to_string("/proc/net/dev") {
            let mut rx_total = 0u64;
            let mut tx_total = 0u64;
            // Two header lines, then one `iface: rx_bytes ... tx_bytes ...` per nic.
            for line in text.lines().skip(2) {
                let Some((iface, stats)) = line.split_once(':') else {
                    continue;
                };
                if iface.trim() == "lo" {
                    continue; // loopback is local traffic, not network throughput
                }
                let cols: Vec<u64> = stats
                    .split_whitespace()
                    .filter_map(|c| c.parse::<u64>().ok())
                    .collect();
                // Receive bytes is column 0; transmit bytes is column 8.
                if cols.len() >= 9 {
                    rx_total = rx_total.saturating_add(cols[0]);
                    tx_total = tx_total.saturating_add(cols[8]);
                }
            }
            return Some((rx_total, tx_total));
        }
    }
    None
}

/// GPU utilization percent, best-effort. Only probes when an NVIDIA GPU is
/// actually present (the driver dir is populated); on a CPU-only host — the demo
/// default — this returns an honest `0`, never a fabricated load. Averaged across
/// GPUs when several are present.
fn read_gpu_pct() -> f32 {
    #[cfg(target_os = "linux")]
    {
        let present = std::fs::read_dir("/proc/driver/nvidia/gpus")
            .map(|mut entries| entries.next().is_some())
            .unwrap_or(false);
        if present
            && let Some(pct) = query_nvidia_smi_util()
        {
            return pct;
        }
    }
    0.0
}

/// Average `utilization.gpu` across GPUs via `nvidia-smi`. Only called once a GPU
/// is known present, so this never runs on the CPU-only demo. `None` on any error.
#[cfg(target_os = "linux")]
fn query_nvidia_smi_util() -> Option<f32> {
    let out = std::process::Command::new("nvidia-smi")
        .args(["--query-gpu=utilization.gpu", "--format=csv,noheader,nounits"])
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let text = String::from_utf8_lossy(&out.stdout);
    let samples: Vec<f32> = text
        .lines()
        .filter_map(|l| l.trim().parse::<f32>().ok())
        .collect();
    if samples.is_empty() {
        return None;
    }
    Some(samples.iter().sum::<f32>() / samples.len() as f32)
}

/// `(MemTotal, MemTotal - MemAvailable)` in MiB from `/proc/meminfo`.
fn read_meminfo_mb() -> Option<(u32, u32)> {
    let text = std::fs::read_to_string("/proc/meminfo").ok()?;
    let mut total_kb = None;
    let mut avail_kb = None;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            total_kb = rest.split_whitespace().next().and_then(|v| v.parse::<u64>().ok());
        } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
            avail_kb = rest.split_whitespace().next().and_then(|v| v.parse::<u64>().ok());
        }
    }
    let total = total_kb?;
    let avail = avail_kb.unwrap_or(total);
    let used = total.saturating_sub(avail);
    Some(((total / 1024) as u32, (used / 1024) as u32))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[cfg_attr(not(target_os = "linux"), ignore)]
    fn host_resource_reports_real_memory_on_linux() {
        // On a real Linux host the machine has some memory; a sample that
        // reported zero total would mean we never read the host at all.
        let mut sampler = HostSampler::new();
        let sample = read_host_resource(&mut sampler);
        assert!(sample.mem_total_mb > 0, "expected to read MemTotal from the host");
        assert!(
            sample.mem_used_mb <= sample.mem_total_mb,
            "used memory cannot exceed total"
        );
        // The root filesystem always has some space in use on a real host.
        assert!(sample.disk_used_gb > 0, "expected statvfs to report used disk");
    }

    #[test]
    fn membership_emits_only_when_a_peer_changes_state() {
        let mut tracker = MembershipTracker::new();

        // First sighting of a peer is a transition from the unknown state.
        let first = tracker.diff(&[("peer-a".into(), "alive".into())]);
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].from, "unknown");
        assert_eq!(first[0].to, "alive");

        // Re-reporting the same state is silence — a steady cluster is quiet.
        let steady = tracker.diff(&[("peer-a".into(), "alive".into())]);
        assert!(steady.is_empty(), "no transition when nothing changed");

        // A genuine state change surfaces, carrying the prior state.
        let changed = tracker.diff(&[("peer-a".into(), "suspect".into())]);
        assert_eq!(changed.len(), 1);
        assert_eq!(changed[0].from, "alive");
        assert_eq!(changed[0].to, "suspect");
    }
}
