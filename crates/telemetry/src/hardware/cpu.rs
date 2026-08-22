use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::record::Record;

pub const HOST_CPU_CHANNEL: &str = "host.cpu";
pub const CPU_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

const SCHEMA: &str = "host.cpu.v1";

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostCpuSample {
    pub schema: String,
    pub seq: u64,
    pub sample_unix_ms: u64,
    pub query_elapsed_ms: Option<u64>,
    pub host: Option<CpuHostSample>,
    pub cores: Vec<CpuCoreSample>,
    pub processes: Vec<CpuProcessSample>,
    pub error: Option<String>,
}

impl HostCpuSample {
    pub fn error(seq: u64, error: impl Into<String>) -> Self {
        Self {
            schema: SCHEMA.to_string(),
            seq,
            sample_unix_ms: unix_ms_now(),
            query_elapsed_ms: None,
            host: None,
            cores: Vec::new(),
            processes: Vec::new(),
            error: Some(error.into()),
        }
    }
}

impl Record for HostCpuSample {
    const CHANNEL: &'static str = HOST_CPU_CHANNEL;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CpuHostSample {
    pub logical_cpus: u32,
    pub total_percent: Option<f64>,
    pub idle_percent: Option<f64>,
    pub iowait_percent: Option<f64>,
    pub steal_percent: Option<f64>,
    pub load1: Option<f64>,
    pub load5: Option<f64>,
    pub load15: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CpuCoreSample {
    pub index: u32,
    pub total_percent: Option<f64>,
    pub idle_percent: Option<f64>,
    pub iowait_percent: Option<f64>,
    pub steal_percent: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CpuProcessSample {
    pub pid: u32,
    pub process_name: String,
    pub cpu_percent: Option<f64>,
    pub rss_bytes: Option<u64>,
    pub vms_bytes: Option<u64>,
    pub thread_count: Option<u64>,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CpuSampler {
    watched_pids: Vec<u32>,
    previous_host: Option<CpuTimes>,
    previous_cores: BTreeMap<u32, CpuTimes>,
    previous_process_ticks: BTreeMap<u32, u64>,
}

impl CpuSampler {
    pub fn new(watched_pids: impl IntoIterator<Item = u32>) -> Self {
        Self {
            watched_pids: watched_pids.into_iter().collect(),
            previous_host: None,
            previous_cores: BTreeMap::new(),
            previous_process_ticks: BTreeMap::new(),
        }
    }

    pub fn sample(&mut self, seq: u64) -> HostCpuSample {
        let started = Instant::now();
        let sample_unix_ms = unix_ms_now();

        let cpu_snapshot = match read_cpu_snapshot() {
            Ok(snapshot) => snapshot,
            Err(error) => return HostCpuSample::error(seq, error),
        };
        let load = match read_load_average() {
            Ok(load) => Some(load),
            Err(error) => {
                let host = self.build_host_sample(&cpu_snapshot, None);
                let cores = self.build_core_samples(&cpu_snapshot);
                self.store_previous_cpu(&cpu_snapshot);
                return HostCpuSample {
                    schema: SCHEMA.to_string(),
                    seq,
                    sample_unix_ms,
                    query_elapsed_ms: Some(elapsed_ms(started)),
                    host: Some(host),
                    cores,
                    processes: self.sample_processes(&cpu_snapshot),
                    error: Some(error),
                };
            }
        };

        let host = self.build_host_sample(&cpu_snapshot, load);
        let cores = self.build_core_samples(&cpu_snapshot);
        let processes = self.sample_processes(&cpu_snapshot);
        self.store_previous_cpu(&cpu_snapshot);

        HostCpuSample {
            schema: SCHEMA.to_string(),
            seq,
            sample_unix_ms,
            query_elapsed_ms: Some(elapsed_ms(started)),
            host: Some(host),
            cores,
            processes,
            error: None,
        }
    }

    fn build_host_sample(
        &self,
        snapshot: &CpuSnapshot,
        load: Option<LoadAverage>,
    ) -> CpuHostSample {
        let percentages = self
            .previous_host
            .as_ref()
            .and_then(|previous| percentages(previous, &snapshot.host));
        CpuHostSample {
            logical_cpus: saturating_usize_to_u32(snapshot.cores.len()),
            total_percent: percentages.as_ref().map(|p| p.busy_percent),
            idle_percent: percentages.as_ref().map(|p| p.idle_percent),
            iowait_percent: percentages.as_ref().map(|p| p.iowait_percent),
            steal_percent: percentages.as_ref().map(|p| p.steal_percent),
            load1: load.as_ref().map(|load| load.one),
            load5: load.as_ref().map(|load| load.five),
            load15: load.as_ref().map(|load| load.fifteen),
        }
    }

    fn build_core_samples(&self, snapshot: &CpuSnapshot) -> Vec<CpuCoreSample> {
        snapshot
            .cores
            .iter()
            .map(|core| {
                let percentages = self
                    .previous_cores
                    .get(&core.index)
                    .and_then(|previous| percentages(previous, &core.times));
                CpuCoreSample {
                    index: core.index,
                    total_percent: percentages.as_ref().map(|p| p.busy_percent),
                    idle_percent: percentages.as_ref().map(|p| p.idle_percent),
                    iowait_percent: percentages.as_ref().map(|p| p.iowait_percent),
                    steal_percent: percentages.as_ref().map(|p| p.steal_percent),
                }
            })
            .collect()
    }

    fn sample_processes(&mut self, snapshot: &CpuSnapshot) -> Vec<CpuProcessSample> {
        let total_delta = self
            .previous_host
            .as_ref()
            .and_then(|previous| snapshot.host.total().checked_sub(previous.total()));
        let logical_cpus = snapshot.cores.len().max(1) as f64;
        let mut samples = Vec::with_capacity(self.watched_pids.len());

        for pid in &self.watched_pids {
            match read_process_counters(*pid) {
                Ok(process) => {
                    let cpu_percent =
                        match (total_delta, self.previous_process_ticks.get(pid).copied()) {
                            (Some(total_delta), Some(previous_ticks)) if total_delta > 0 => process
                                .cpu_ticks
                                .checked_sub(previous_ticks)
                                .map(|process_delta| {
                                    process_delta as f64 * logical_cpus * 100.0 / total_delta as f64
                                }),
                            _ => None,
                        };
                    self.previous_process_ticks.insert(*pid, process.cpu_ticks);
                    samples.push(CpuProcessSample {
                        pid: *pid,
                        process_name: process.name,
                        cpu_percent,
                        rss_bytes: process.rss_bytes,
                        vms_bytes: process.vms_bytes,
                        thread_count: process.thread_count,
                        error: None,
                    });
                }
                Err(error) => {
                    self.previous_process_ticks.remove(pid);
                    samples.push(CpuProcessSample {
                        pid: *pid,
                        process_name: String::new(),
                        cpu_percent: None,
                        rss_bytes: None,
                        vms_bytes: None,
                        thread_count: None,
                        error: Some(error),
                    });
                }
            }
        }

        samples
    }

    fn store_previous_cpu(&mut self, snapshot: &CpuSnapshot) {
        self.previous_host = Some(snapshot.host.clone());
        self.previous_cores = snapshot
            .cores
            .iter()
            .map(|core| (core.index, core.times.clone()))
            .collect();
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CpuSnapshot {
    host: CpuTimes,
    cores: Vec<CpuCoreTimes>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CpuCoreTimes {
    index: u32,
    times: CpuTimes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CpuTimes {
    user: u64,
    nice: u64,
    system: u64,
    idle: u64,
    iowait: u64,
    irq: u64,
    softirq: u64,
    steal: u64,
}

impl CpuTimes {
    fn total(&self) -> u64 {
        self.user
            .saturating_add(self.nice)
            .saturating_add(self.system)
            .saturating_add(self.idle)
            .saturating_add(self.iowait)
            .saturating_add(self.irq)
            .saturating_add(self.softirq)
            .saturating_add(self.steal)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct CpuPercentages {
    busy_percent: f64,
    idle_percent: f64,
    iowait_percent: f64,
    steal_percent: f64,
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct LoadAverage {
    one: f64,
    five: f64,
    fifteen: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ProcessCounters {
    name: String,
    cpu_ticks: u64,
    rss_bytes: Option<u64>,
    vms_bytes: Option<u64>,
    thread_count: Option<u64>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ProcessStatus {
    name: Option<String>,
    rss_bytes: Option<u64>,
    vms_bytes: Option<u64>,
    thread_count: Option<u64>,
}

fn read_cpu_snapshot() -> Result<CpuSnapshot, String> {
    let raw = fs::read_to_string("/proc/stat").map_err(|err| format!("read /proc/stat: {err}"))?;
    parse_cpu_snapshot(&raw).ok_or_else(|| "parse /proc/stat cpu rows".to_string())
}

fn parse_cpu_snapshot(raw: &str) -> Option<CpuSnapshot> {
    let mut host = None;
    let mut cores = Vec::new();

    for line in raw.lines() {
        let Some(label) = line.split_whitespace().next() else {
            continue;
        };
        if label == "cpu" {
            host = parse_cpu_times(line);
        } else if let Some(index) = label
            .strip_prefix("cpu")
            .and_then(|suffix| suffix.parse::<u32>().ok())
            && let Some(times) = parse_cpu_times(line)
        {
            cores.push(CpuCoreTimes { index, times });
        }
    }

    host.map(|host| CpuSnapshot { host, cores })
}

fn parse_cpu_times(line: &str) -> Option<CpuTimes> {
    let mut fields = line.split_whitespace().skip(1);
    Some(CpuTimes {
        user: fields.next()?.parse().ok()?,
        nice: fields.next()?.parse().ok()?,
        system: fields.next()?.parse().ok()?,
        idle: fields.next()?.parse().ok()?,
        iowait: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
        irq: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
        softirq: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
        steal: fields.next().and_then(|v| v.parse().ok()).unwrap_or(0),
    })
}

fn percentages(previous: &CpuTimes, current: &CpuTimes) -> Option<CpuPercentages> {
    let total_delta = current.total().checked_sub(previous.total())?;
    if total_delta == 0 {
        return None;
    }
    let idle_delta = current.idle.checked_sub(previous.idle)?;
    let iowait_delta = current.iowait.checked_sub(previous.iowait)?;
    let steal_delta = current.steal.checked_sub(previous.steal)?;
    let idle_all_delta = idle_delta.saturating_add(iowait_delta);
    let busy_delta = total_delta.saturating_sub(idle_all_delta);
    let total = total_delta as f64;
    Some(CpuPercentages {
        busy_percent: busy_delta as f64 * 100.0 / total,
        idle_percent: idle_delta as f64 * 100.0 / total,
        iowait_percent: iowait_delta as f64 * 100.0 / total,
        steal_percent: steal_delta as f64 * 100.0 / total,
    })
}

fn read_load_average() -> Result<LoadAverage, String> {
    let raw =
        fs::read_to_string("/proc/loadavg").map_err(|err| format!("read /proc/loadavg: {err}"))?;
    parse_load_average(&raw).ok_or_else(|| "parse /proc/loadavg".to_string())
}

fn parse_load_average(raw: &str) -> Option<LoadAverage> {
    let mut fields = raw.split_whitespace();
    Some(LoadAverage {
        one: fields.next()?.parse().ok()?,
        five: fields.next()?.parse().ok()?,
        fifteen: fields.next()?.parse().ok()?,
    })
}

fn read_process_counters(pid: u32) -> Result<ProcessCounters, String> {
    let stat_path = format!("/proc/{pid}/stat");
    let stat_raw =
        fs::read_to_string(&stat_path).map_err(|err| format!("read {stat_path}: {err}"))?;
    let (stat_name, cpu_ticks) =
        parse_process_stat(&stat_raw).ok_or_else(|| format!("parse {stat_path}"))?;

    let status_path = format!("/proc/{pid}/status");
    let status = fs::read_to_string(&status_path)
        .ok()
        .map(|raw| parse_process_status(&raw))
        .unwrap_or_default();

    Ok(ProcessCounters {
        name: status.name.unwrap_or(stat_name),
        cpu_ticks,
        rss_bytes: status.rss_bytes,
        vms_bytes: status.vms_bytes,
        thread_count: status.thread_count,
    })
}

fn parse_process_stat(raw: &str) -> Option<(String, u64)> {
    let open = raw.find('(')?;
    let close = raw.rfind(')')?;
    if close <= open {
        return None;
    }
    let name = raw[open + 1..close].to_string();
    let fields: Vec<&str> = raw[close + 1..].split_whitespace().collect();
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    Some((name, utime.saturating_add(stime)))
}

fn parse_process_status(raw: &str) -> ProcessStatus {
    let mut status = ProcessStatus::default();
    for line in raw.lines() {
        if let Some(value) = line.strip_prefix("Name:") {
            status.name = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("VmRSS:") {
            status.rss_bytes = parse_kib_value(value);
        } else if let Some(value) = line.strip_prefix("VmSize:") {
            status.vms_bytes = parse_kib_value(value);
        } else if let Some(value) = line.strip_prefix("Threads:") {
            status.thread_count = value.trim().parse().ok();
        }
    }
    status
}

fn parse_kib_value(value: &str) -> Option<u64> {
    let kib = value.split_whitespace().next()?.parse::<u64>().ok()?;
    kib.checked_mul(1024)
}

fn saturating_usize_to_u32(value: usize) -> u32 {
    value.min(u32::MAX as usize) as u32
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cpu_snapshot_rows() {
        let snapshot = parse_cpu_snapshot(
            "cpu  100 2 50 800 10 1 3 4 0 0\ncpu0 60 1 30 400 5 0 2 3 0 0\ncpu1 40 1 20 400 5 1 1 1 0 0\nintr 1\n",
        )
        .expect("snapshot");

        assert_eq!(snapshot.host.user, 100);
        assert_eq!(snapshot.host.total(), 970);
        assert_eq!(snapshot.cores.len(), 2);
        assert_eq!(snapshot.cores[0].index, 0);
        assert_eq!(snapshot.cores[1].times.system, 20);
    }

    #[test]
    fn calculates_cpu_percentages_from_deltas() {
        let previous = CpuTimes {
            user: 10,
            nice: 0,
            system: 10,
            idle: 80,
            iowait: 0,
            irq: 0,
            softirq: 0,
            steal: 0,
        };
        let current = CpuTimes {
            user: 30,
            nice: 0,
            system: 20,
            idle: 130,
            iowait: 10,
            irq: 0,
            softirq: 0,
            steal: 10,
        };

        let pct = percentages(&previous, &current).expect("percentages");

        assert_eq!(pct.busy_percent, 40.0);
        assert_eq!(pct.idle_percent, 50.0);
        assert_eq!(pct.iowait_percent, 10.0);
        assert_eq!(pct.steal_percent, 10.0);
    }

    #[test]
    fn parses_load_average() {
        let load = parse_load_average("1.25 0.75 0.50 1/234 5678\n").expect("load");

        assert_eq!(load.one, 1.25);
        assert_eq!(load.five, 0.75);
        assert_eq!(load.fifteen, 0.50);
    }

    #[test]
    fn parses_process_stat_with_spaces_in_name() {
        let stat = "123 (python worker) S 1 2 3 4 5 6 7 8 9 10 120 30 0 0 20 0 7 0 12345 4096 10";

        let (name, ticks) = parse_process_stat(stat).expect("process stat");

        assert_eq!(name, "python worker");
        assert_eq!(ticks, 150);
    }

    #[test]
    fn parses_process_status_memory_and_threads() {
        let status = parse_process_status(
            "Name:\tpython3\nVmSize:\t  1000 kB\nVmRSS:\t   128 kB\nThreads:\t4\n",
        );

        assert_eq!(status.name, Some("python3".to_string()));
        assert_eq!(status.vms_bytes, Some(1_024_000));
        assert_eq!(status.rss_bytes, Some(131_072));
        assert_eq!(status.thread_count, Some(4));
    }

    #[test]
    fn sample_error_uses_host_cpu_schema() {
        let sample = HostCpuSample::error(7, "no proc");

        assert_eq!(HostCpuSample::CHANNEL, "host.cpu");
        assert_eq!(sample.schema, "host.cpu.v1");
        assert_eq!(sample.seq, 7);
        assert_eq!(sample.error, Some("no proc".to_string()));
    }
}
