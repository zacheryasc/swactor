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

use super::catalog::{IdentityRecord, MembershipTransition, ResourceSample, Role};

/// Build the identity record a node emits first on its stream (spec §6.1).
pub fn identity_record(node: &str, role: Role, region: &str, life: u64) -> IdentityRecord {
    IdentityRecord {
        node: node.to_string(),
        role,
        region: region.to_string(),
        life,
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

    /// CPU busy percent since the previous call, in `[0, 100*ncpu]` clamped to
    /// `[0, 100]`. Returns 0 on the first call (no baseline yet) and on any
    /// platform where `/proc/stat` is unavailable.
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
                    // Jiffies are USER_HZ (typically 100/s) per CPU. Normalize by
                    // wall time and the tick rate to get a busy fraction.
                    let delta = busy.saturating_sub(prev_busy) as f64;
                    let hz = clock_ticks_per_sec();
                    let frac = delta / (hz * elapsed);
                    (frac * 100.0).clamp(0.0, 100.0) as f32
                }
            }
            None => 0.0,
        };
        self.prev = Some((busy, now));
        pct
    }
}

/// Read a host-resource sample: CPU busy percent (via `sampler`) and memory
/// from `/proc/meminfo`. Fields we have no source for stay at their default
/// (honest zeros), and on a non-Linux host the whole sample is the default.
pub fn read_host_resource(sampler: &mut CpuSampler) -> ResourceSample {
    let cpu_pct = sampler.sample();
    let (mem_total_mb, mem_used_mb) = read_meminfo_mb().unwrap_or((0, 0));
    // Fields we have no host source for stay at honest zeros.
    ResourceSample {
        cpu_pct,
        mem_used_mb,
        mem_total_mb,
        gpu_pct: 0.0,
        disk_used_gb: 0,
        net_rx_kbps: 0,
        net_tx_kbps: 0,
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
        let mut sampler = CpuSampler::new();
        let sample = read_host_resource(&mut sampler);
        assert!(sample.mem_total_mb > 0, "expected to read MemTotal from the host");
        assert!(
            sample.mem_used_mb <= sample.mem_total_mb,
            "used memory cannot exceed total"
        );
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
