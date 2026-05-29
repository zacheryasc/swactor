//! In-VM OS/GPU sampler for the vastai monitoring layer.
//!
//! Runs inside a rented container and ships one [`HostSample`] per tick: GPU
//! (via a pluggable [`GpuSource`]), system CPU + load, memory + swap, per-device
//! disk I/O, per-interface network throughput, and cgroup limits. Counter-based
//! subsystems (cpu/disk/net) report *rates* computed from the delta against the
//! previous tick.
//!
//! The `/proc` parsing is split into pure functions taking the file text so they
//! can be tested against captured fixtures; the reading layer is Linux-only and
//! returns empty/`None` elsewhere.

use std::sync::Mutex;
use std::time::Instant;

use super::record::{
    CgroupSample, CpuSample, DiskSample, GpuSample, HostSample, MemSample, NetSample,
};

/// Source of GPU metrics. Abstracted so the concrete backend (shelling out to
/// `nvidia-smi` vs linking NVML) can be chosen later without touching the
/// sampler. The default backend choice is intentionally deferred.
pub trait GpuSource: Send + Sync {
    /// Snapshot all visible GPUs. Returns empty when none are present or the
    /// source is unavailable — GPU absence must never fail a tick.
    fn sample(&self) -> Vec<GpuSample>;
}

/// A [`GpuSource`] that always reports no GPUs. Used until a concrete backend is
/// wired; keeps the sampler shippable with the rest of the host metrics.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoGpu;

impl GpuSource for NoGpu {
    fn sample(&self) -> Vec<GpuSample> {
        Vec::new()
    }
}

/// Raw counters captured at a point in time, used to compute rates next tick.
#[derive(Debug, Clone, Default)]
struct Counters {
    at: Option<Instant>,
    cpu: Option<CpuTotals>,
    /// device -> (read_sectors, write_sectors)
    disk: Vec<(String, u64, u64)>,
    /// iface -> (rx_bytes, tx_bytes)
    net: Vec<(String, u64, u64)>,
}

#[derive(Debug, Clone, Copy, Default)]
struct CpuTotals {
    total: u64,
    idle: u64,
}

/// The sampler: holds the previous counters (for rate deltas) and a GPU source.
pub struct Sampler<G: GpuSource> {
    gpu: G,
    prev: Mutex<Counters>,
}

impl<G: GpuSource> Sampler<G> {
    pub fn new(gpu: G) -> Self {
        Self {
            gpu,
            prev: Mutex::new(Counters::default()),
        }
    }

    /// Build one host sample. On the first call, counter-based rates are absent
    /// (no prior snapshot) and only levels (load, mem totals, fs usage) are set.
    pub fn tick(&self) -> HostSample {
        let now = Instant::now();
        let cur = read_counters();
        let dt = {
            let prev = self.prev.lock().expect("sampler prev mutex poisoned");
            prev.at.map(|t| (now - t).as_secs_f64()).filter(|&s| s > 0.0)
        };

        let cpu = build_cpu(&cur, self.prev.lock().unwrap().cpu, dt);
        let disk = build_disk(&cur, &self.prev.lock().unwrap().disk, dt);
        let net = build_net(&cur, &self.prev.lock().unwrap().net, dt);

        // Store current counters for the next delta.
        {
            let mut prev = self.prev.lock().expect("sampler prev mutex poisoned");
            *prev = Counters {
                at: Some(now),
                ..cur
            };
        }

        HostSample {
            gpus: self.gpu.sample(),
            cpu,
            mem: read_mem(),
            disk,
            net,
            cgroup: read_cgroup(),
        }
    }
}

/// Handle for the background sampler task.
#[cfg(feature = "collector")]
pub struct SamplerHandle {
    task: tokio::task::JoinHandle<()>,
    stop: std::sync::Arc<tokio::sync::Notify>,
}

#[cfg(feature = "collector")]
impl SamplerHandle {
    /// Stop the sampler loop.
    pub async fn shutdown(self) {
        self.stop.notify_waiters();
        let _ = self.task.await;
    }
}

/// Spawn a background task that ticks the sampler on `interval` and ships each
/// [`HostSample`] through the shipper. The shipper already stamps the node ref.
#[cfg(feature = "collector")]
pub fn spawn_sampler<G: GpuSource + 'static>(
    sampler: Sampler<G>,
    shipper: super::shipper::VastaiShipper,
    interval: std::time::Duration,
) -> SamplerHandle {
    use std::sync::Arc;
    let stop = Arc::new(tokio::sync::Notify::new());
    let stop2 = Arc::clone(&stop);
    let task = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = stop2.notified() => break,
                _ = ticker.tick() => shipper.sample(sampler.tick()),
            }
        }
    });
    SamplerHandle { task, stop }
}

fn build_cpu(cur: &Counters, prev: Option<CpuTotals>, dt: Option<f64>) -> CpuSample {
    let mut s = read_loadavg();
    s.num_cpus = cur_num_cpus();
    if let (Some(c), Some(p)) = (cur.cpu, prev) {
        let dtotal = c.total.saturating_sub(p.total);
        let didle = c.idle.saturating_sub(p.idle);
        if dtotal > 0 {
            let busy = dtotal.saturating_sub(didle) as f64;
            s.util_pct = Some((busy / dtotal as f64) * 100.0);
        }
    }
    let _ = dt;
    s
}

fn build_disk(cur: &Counters, prev: &[(String, u64, u64)], dt: Option<f64>) -> Vec<DiskSample> {
    cur.disk
        .iter()
        .map(|(dev, r, w)| {
            let mut d = DiskSample {
                device: dev.clone(),
                ..Default::default()
            };
            if let (Some(dt), Some((_, pr, pw))) =
                (dt, prev.iter().find(|(n, _, _)| n == dev))
            {
                // sectors are 512 bytes.
                d.read_bytes_per_s = Some((r.saturating_sub(*pr) as f64) * 512.0 / dt);
                d.write_bytes_per_s = Some((w.saturating_sub(*pw) as f64) * 512.0 / dt);
            }
            d
        })
        .collect()
}

fn build_net(cur: &Counters, prev: &[(String, u64, u64)], dt: Option<f64>) -> Vec<NetSample> {
    cur.net
        .iter()
        .map(|(iface, rx, tx)| {
            let mut n = NetSample {
                iface: iface.clone(),
                rx_bytes_total: Some(*rx),
                tx_bytes_total: Some(*tx),
                ..Default::default()
            };
            if let (Some(dt), Some((_, prx, ptx))) =
                (dt, prev.iter().find(|(name, _, _)| name == iface))
            {
                n.rx_bytes_per_s = Some((rx.saturating_sub(*prx) as f64) / dt);
                n.tx_bytes_per_s = Some((tx.saturating_sub(*ptx) as f64) / dt);
            }
            n
        })
        .collect()
}

// ── Pure parsers (fixture-testable) ─────────────────────────────────────────

/// Parse the aggregate `cpu ...` line of `/proc/stat` into busy/idle jiffies.
pub fn parse_proc_stat_cpu(text: &str) -> Option<CpuTotalsPub> {
    let line = text.lines().find(|l| l.starts_with("cpu "))?;
    let nums: Vec<u64> = line
        .split_whitespace()
        .skip(1)
        .filter_map(|n| n.parse().ok())
        .collect();
    if nums.len() < 4 {
        return None;
    }
    // idle = idle + iowait (field 3 + field 4 when present).
    let idle = nums[3] + nums.get(4).copied().unwrap_or(0);
    let total: u64 = nums.iter().sum();
    Some(CpuTotalsPub { total, idle })
}

/// Public mirror of `CpuTotals` for the parser API/tests.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CpuTotalsPub {
    pub total: u64,
    pub idle: u64,
}

/// Parse `/proc/loadavg` into a [`CpuSample`] (load fields only).
pub fn parse_loadavg(text: &str) -> CpuSample {
    let mut parts = text.split_whitespace();
    CpuSample {
        load1: parts.next().and_then(|s| s.parse().ok()),
        load5: parts.next().and_then(|s| s.parse().ok()),
        load15: parts.next().and_then(|s| s.parse().ok()),
        ..Default::default()
    }
}

/// Parse `/proc/meminfo` into a [`MemSample`] (values are in kB).
pub fn parse_meminfo(text: &str) -> MemSample {
    let mut m = MemSample::default();
    // `rest` is the text after the `MemTotal:`-style prefix, so the numeric
    // value is the first whitespace-separated token.
    let get = |rest: &str| -> Option<u64> {
        rest.split_whitespace().next().and_then(|n| n.parse().ok())
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            m.total_kb = get(rest);
        } else if let Some(rest) = line.strip_prefix("MemAvailable:") {
            m.available_kb = get(rest);
        } else if let Some(rest) = line.strip_prefix("SwapTotal:") {
            m.swap_total_kb = get(rest);
        } else if let Some(rest) = line.strip_prefix("SwapFree:") {
            m.swap_free_kb = get(rest);
        }
    }
    if let (Some(t), Some(a)) = (m.total_kb, m.available_kb) {
        m.used_kb = Some(t.saturating_sub(a));
    }
    m
}

/// Parse `/proc/net/dev` into per-interface (name, rx_bytes, tx_bytes).
/// Skips the loopback interface.
pub fn parse_net_dev(text: &str) -> Vec<(String, u64, u64)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let Some((name, rest)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim();
        if name.is_empty() || name == "lo" || name == "Inter-|" {
            continue;
        }
        let nums: Vec<u64> = rest.split_whitespace().filter_map(|n| n.parse().ok()).collect();
        // /proc/net/dev: rx_bytes is field 0, tx_bytes is field 8.
        if nums.len() >= 9 {
            out.push((name.to_string(), nums[0], nums[8]));
        }
    }
    out
}

/// Parse `/proc/diskstats` into per-device (name, read_sectors, write_sectors).
/// Keeps only whole block devices (skips partitions / loop / ram).
pub fn parse_diskstats(text: &str) -> Vec<(String, u64, u64)> {
    let mut out = Vec::new();
    for line in text.lines() {
        let f: Vec<&str> = line.split_whitespace().collect();
        // Need at least up to field index 9 (sectors written).
        if f.len() < 10 {
            continue;
        }
        let name = f[2];
        if name.starts_with("loop") || name.starts_with("ram") || name.ends_with(|c: char| c.is_ascii_digit())
        {
            // Skip partitions (name ending in a digit) and pseudo devices.
            continue;
        }
        let read_sectors = f[5].parse().unwrap_or(0);
        let write_sectors = f[9].parse().unwrap_or(0);
        out.push((name.to_string(), read_sectors, write_sectors));
    }
    out
}

/// Parse a cgroup v2 `cpu.max` value (`"max"` or `"<quota> <period>"`) into a
/// CPU quota expressed in cores.
pub fn parse_cgroup_cpu_max(text: &str) -> Option<f64> {
    let mut parts = text.split_whitespace();
    let quota = parts.next()?;
    if quota == "max" {
        return None;
    }
    let quota: f64 = quota.parse().ok()?;
    let period: f64 = parts.next().and_then(|p| p.parse().ok()).unwrap_or(100_000.0);
    if period > 0.0 {
        Some(quota / period)
    } else {
        None
    }
}

// ── Reading layer (Linux only) ──────────────────────────────────────────────

#[cfg(target_os = "linux")]
fn read_counters() -> Counters {
    let cpu = std::fs::read_to_string("/proc/stat")
        .ok()
        .and_then(|t| parse_proc_stat_cpu(&t))
        .map(|c| CpuTotals {
            total: c.total,
            idle: c.idle,
        });
    let disk = std::fs::read_to_string("/proc/diskstats")
        .map(|t| parse_diskstats(&t))
        .unwrap_or_default();
    let net = std::fs::read_to_string("/proc/net/dev")
        .map(|t| parse_net_dev(&t))
        .unwrap_or_default();
    Counters {
        at: None,
        cpu,
        disk,
        net,
    }
}

#[cfg(not(target_os = "linux"))]
fn read_counters() -> Counters {
    Counters::default()
}

#[cfg(target_os = "linux")]
fn read_mem() -> MemSample {
    std::fs::read_to_string("/proc/meminfo")
        .map(|t| parse_meminfo(&t))
        .unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
fn read_mem() -> MemSample {
    MemSample::default()
}

#[cfg(target_os = "linux")]
fn read_loadavg() -> CpuSample {
    std::fs::read_to_string("/proc/loadavg")
        .map(|t| parse_loadavg(&t))
        .unwrap_or_default()
}

#[cfg(not(target_os = "linux"))]
fn read_loadavg() -> CpuSample {
    CpuSample::default()
}

fn cur_num_cpus() -> Option<u32> {
    std::thread::available_parallelism().ok().map(|n| n.get() as u32)
}

#[cfg(target_os = "linux")]
fn read_cgroup() -> Option<CgroupSample> {
    let mut c = CgroupSample::default();
    let read_u64 = |p: &str| std::fs::read_to_string(p).ok().and_then(|s| s.trim().parse::<u64>().ok());
    // cgroup v2 first.
    if let Ok(max) = std::fs::read_to_string("/sys/fs/cgroup/memory.max") {
        let max = max.trim();
        if max != "max" {
            c.mem_limit_bytes = max.parse().ok();
        }
        c.mem_current_bytes = read_u64("/sys/fs/cgroup/memory.current");
        c.cpu_quota_cores = std::fs::read_to_string("/sys/fs/cgroup/cpu.max")
            .ok()
            .and_then(|t| parse_cgroup_cpu_max(&t));
    } else {
        // cgroup v1 fallback.
        c.mem_limit_bytes = read_u64("/sys/fs/cgroup/memory/memory.limit_in_bytes");
        c.mem_current_bytes = read_u64("/sys/fs/cgroup/memory/memory.usage_in_bytes");
    }
    if c.mem_limit_bytes.is_none() && c.mem_current_bytes.is_none() && c.cpu_quota_cores.is_none() {
        None
    } else {
        Some(c)
    }
}

#[cfg(not(target_os = "linux"))]
fn read_cgroup() -> Option<CgroupSample> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn proc_stat_cpu_sums_total_and_idle_including_iowait() {
        // total = 100+0+200+700+50 = 1050; idle = idle(700)+iowait(50) = 750.
        let t = "cpu  100 0 200 700 50 0 0 0 0 0\ncpu0 ...\n";
        let c = parse_proc_stat_cpu(t).unwrap();
        assert_eq!(c.total, 1050);
        assert_eq!(c.idle, 750);
    }

    #[test]
    fn meminfo_derives_used_from_total_minus_available() {
        let t = "MemTotal:       16000 kB\nMemFree:         1000 kB\nMemAvailable:    10000 kB\nSwapTotal:       2000 kB\nSwapFree:        1500 kB\n";
        let m = parse_meminfo(t);
        assert_eq!(m.total_kb, Some(16000));
        assert_eq!(m.available_kb, Some(10000));
        assert_eq!(m.used_kb, Some(6000));
        assert_eq!(m.swap_total_kb, Some(2000));
        assert_eq!(m.swap_free_kb, Some(1500));
    }

    #[test]
    fn net_dev_extracts_rx_tx_bytes_and_skips_loopback() {
        let t = "Inter-|   Receive ...\n \
                 face |bytes    packets errs drop fifo frame compressed multicast|bytes ...\n\
                 lo: 123 1 0 0 0 0 0 0 456 1 0 0 0 0 0 0\n\
                 eth0: 1000 5 0 0 0 0 0 0 2000 6 0 0 0 0 0 0\n";
        let v = parse_net_dev(t);
        assert_eq!(v.len(), 1, "loopback excluded");
        assert_eq!(v[0], ("eth0".to_string(), 1000, 2000));
    }

    #[test]
    fn diskstats_keeps_whole_devices_and_skips_partitions() {
        // fields: major minor name reads merged sectorsRead msRead writes merged sectorsWritten ...
        let t = "   8       0 sda 100 0 800 0 50 0 1600 0 0 0 0\n\
                 8       1 sda1 10 0 80 0 5 0 160 0 0 0 0\n\
                 7       0 loop0 1 0 2 0 0 0 0 0 0 0 0\n";
        let v = parse_diskstats(t);
        assert_eq!(v.len(), 1, "only whole device sda kept");
        assert_eq!(v[0], ("sda".to_string(), 800, 1600));
    }

    #[test]
    fn cgroup_cpu_max_converts_quota_period_to_cores() {
        assert_eq!(parse_cgroup_cpu_max("200000 100000"), Some(2.0));
        assert_eq!(parse_cgroup_cpu_max("50000 100000"), Some(0.5));
        assert_eq!(parse_cgroup_cpu_max("max 100000"), None);
    }

    #[test]
    fn net_rates_are_delta_over_elapsed_time() {
        // Two snapshots 2s apart: rx grew by 4000 bytes -> 2000 B/s.
        let cur = Counters {
            net: vec![("eth0".into(), 5000, 9000)],
            ..Default::default()
        };
        let prev = vec![("eth0".to_string(), 1000u64, 3000u64)];
        let out = build_net(&cur, &prev, Some(2.0));
        assert_eq!(out[0].rx_bytes_per_s, Some(2000.0));
        assert_eq!(out[0].tx_bytes_per_s, Some(3000.0));
        assert_eq!(out[0].rx_bytes_total, Some(5000));
    }

    #[test]
    fn first_tick_has_no_rates_but_still_reports_levels() {
        // Without a prior snapshot, counter rates are absent; the sample still
        // ships (levels like load/mem are best-effort from the host).
        let s = Sampler::new(NoGpu);
        let sample = s.tick();
        assert!(sample.net.iter().all(|n| n.rx_bytes_per_s.is_none()));
        assert!(sample.cpu.util_pct.is_none());
    }
}
