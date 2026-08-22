use std::collections::BTreeMap;
use std::fs;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use super::pressure::{self, PressureSample};
use crate::record::Record;

pub const HOST_MEMORY_CHANNEL: &str = "host.memory";
pub const MEMORY_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

const SCHEMA: &str = "host.memory.v1";
const KIB: u64 = 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct HostMemorySample {
    pub schema: String,
    pub seq: u64,
    pub sample_unix_ms: u64,
    pub query_elapsed_ms: Option<u64>,
    pub total_bytes: Option<u64>,
    pub available_bytes: Option<u64>,
    pub used_bytes: Option<u64>,
    pub cached_bytes: Option<u64>,
    pub swap_total_bytes: Option<u64>,
    pub swap_used_bytes: Option<u64>,
    pub pressure: Option<PressureSample>,
    pub error: Option<String>,
}

impl Record for HostMemorySample {
    const CHANNEL: &'static str = HOST_MEMORY_CHANNEL;
}

pub fn sample(seq: u64) -> HostMemorySample {
    let started = Instant::now();
    let sample_unix_ms = unix_ms_now();
    let memory = match fs::read_to_string("/proc/meminfo") {
        Ok(raw) => match parse_meminfo(&raw) {
            Some(memory) => memory,
            None => return HostMemorySample::error(seq, "parse /proc/meminfo"),
        },
        Err(error) => return HostMemorySample::error(seq, format!("read /proc/meminfo: {error}")),
    };
    HostMemorySample {
        schema: SCHEMA.to_owned(),
        seq,
        sample_unix_ms,
        query_elapsed_ms: Some(elapsed_ms(started)),
        total_bytes: Some(memory.total_bytes),
        available_bytes: Some(memory.available_bytes),
        used_bytes: Some(memory.total_bytes.saturating_sub(memory.available_bytes)),
        cached_bytes: Some(memory.cached_bytes),
        swap_total_bytes: Some(memory.swap_total_bytes),
        swap_used_bytes: Some(
            memory
                .swap_total_bytes
                .saturating_sub(memory.swap_free_bytes),
        ),
        pressure: pressure::read("memory").ok(),
        error: None,
    }
}

impl HostMemorySample {
    fn error(seq: u64, error: impl Into<String>) -> Self {
        Self {
            schema: SCHEMA.to_owned(),
            seq,
            sample_unix_ms: unix_ms_now(),
            query_elapsed_ms: None,
            total_bytes: None,
            available_bytes: None,
            used_bytes: None,
            cached_bytes: None,
            swap_total_bytes: None,
            swap_used_bytes: None,
            pressure: pressure::read("memory").ok(),
            error: Some(error.into()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct MemoryCounters {
    total_bytes: u64,
    available_bytes: u64,
    cached_bytes: u64,
    swap_total_bytes: u64,
    swap_free_bytes: u64,
}

fn parse_meminfo(raw: &str) -> Option<MemoryCounters> {
    let mut values = BTreeMap::new();
    for line in raw.lines() {
        let (name, rest) = line.split_once(':')?;
        let value_kib = rest.split_whitespace().next()?.parse::<u64>().ok()?;
        values.insert(name, value_kib.saturating_mul(KIB));
    }
    let cached_bytes = values
        .get("Cached")
        .copied()
        .unwrap_or(0)
        .saturating_add(values.get("SReclaimable").copied().unwrap_or(0));
    Some(MemoryCounters {
        total_bytes: *values.get("MemTotal")?,
        available_bytes: *values.get("MemAvailable")?,
        cached_bytes,
        swap_total_bytes: values.get("SwapTotal").copied().unwrap_or(0),
        swap_free_bytes: values.get("SwapFree").copied().unwrap_or(0),
    })
}

fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn elapsed_ms(started: Instant) -> u64 {
    started.elapsed().as_millis().try_into().unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::{parse_meminfo, sample};

    #[test]
    fn derives_used_cached_and_swap_memory() {
        let counters = parse_meminfo(
            "MemTotal:       1000 kB\nMemAvailable:    400 kB\nCached:          100 kB\nSReclaimable:     20 kB\nSwapTotal:       200 kB\nSwapFree:        150 kB\n",
        )
        .expect("memory counters");
        assert_eq!(counters.total_bytes, 1_024_000);
        assert_eq!(counters.available_bytes, 409_600);
        assert_eq!(counters.cached_bytes, 122_880);
        assert_eq!(counters.swap_total_bytes - counters.swap_free_bytes, 51_200);
    }

    #[test]
    fn samples_live_memory() {
        let sample = sample(7);
        assert_eq!(sample.seq, 7);
        assert!(sample.total_bytes.is_some_and(|total| total > 0));
        assert!(sample.error.is_none());
    }
}
