//! Process resource snapshot for tier-3 (`DIAGNOSTICS_PLAN.md` T3.5).
//!
//! Reads `/proc/self/status` for `VmRSS` / `VmSize`, counts entries
//! under `/proc/self/fd`, and parses `/proc/self/stat` for CPU time
//! (utime + stime, converted from clock ticks). All fields are
//! best-effort — if a file is unreadable or the host is non-Linux the
//! corresponding slot reports `None`.
//!
//! Tokio runtime stats are deliberately minimal. The richer fields
//! exposed by `tokio::runtime::Handle::metrics()` require enabling the
//! `tokio_unstable` cfg, which we do not want to force on every
//! downstream crate. We capture only the stable surface: the runtime
//! flavor (`current_thread` vs `multi_thread`).

use crate::diagnostics::snapshot::{
    ProcessIntrospector, Tier3ProcessStats, Tier3TokioStats,
};
use crate::diagnostics::wall_ms_now;

/// Stateless tier-3 process scraper. Each `capture` re-reads the
/// underlying files — there's nothing to cache because the values
/// change every snapshot.
#[derive(Debug, Default, Clone, Copy)]
pub struct ProcessStats;

impl ProcessStats {
    pub fn new() -> Self {
        Self
    }
}

impl ProcessIntrospector for ProcessStats {
    fn capture(&self) -> Tier3ProcessStats {
        let (rss_bytes, vm_size_bytes) = read_rss_and_vm();
        let open_fd_count = read_fd_count();
        let cpu_ms = read_cpu_ms();
        let tokio = capture_tokio();
        Tier3ProcessStats {
            rss_bytes,
            vm_size_bytes,
            open_fd_count,
            cpu_ms,
            tokio,
            captured_at_ms: wall_ms_now(),
        }
    }
}

#[cfg(target_os = "linux")]
fn read_rss_and_vm() -> (Option<u64>, Option<u64>) {
    let body = match std::fs::read_to_string("/proc/self/status") {
        Ok(b) => b,
        Err(_) => return (None, None),
    };
    let mut rss = None;
    let mut vm = None;
    for line in body.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            rss = parse_kb_to_bytes(rest);
        }
        if let Some(rest) = line.strip_prefix("VmSize:") {
            vm = parse_kb_to_bytes(rest);
        }
    }
    (rss, vm)
}

#[cfg(not(target_os = "linux"))]
fn read_rss_and_vm() -> (Option<u64>, Option<u64>) {
    (None, None)
}

#[cfg(target_os = "linux")]
fn parse_kb_to_bytes(s: &str) -> Option<u64> {
    let trimmed = s.trim();
    let num_part: String = trimmed.chars().take_while(|c| c.is_ascii_digit()).collect();
    let kb: u64 = num_part.parse().ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(target_os = "linux")]
fn read_fd_count() -> Option<u64> {
    let dir = std::fs::read_dir("/proc/self/fd").ok()?;
    let mut count: u64 = 0;
    for entry in dir {
        if entry.is_ok() {
            count += 1;
        }
    }
    Some(count)
}

#[cfg(not(target_os = "linux"))]
fn read_fd_count() -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_cpu_ms() -> Option<u64> {
    // `/proc/self/stat` layout: pid (comm) state ... utime stime ...
    // utime is field 14 (1-indexed), stime is field 15. The comm
    // field is wrapped in parens and may contain spaces, so the safe
    // parse is to find the last ')' and read from there.
    let body = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after_comm = body.rfind(')').map(|i| &body[i + 1..])?;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    // After the closing paren, field 1 is `state`. utime is the 12th
    // (state .. cnswap .. utime stime); zero-indexed offsets 11 and 12.
    let utime_ticks: u64 = fields.get(11)?.parse().ok()?;
    let stime_ticks: u64 = fields.get(12)?.parse().ok()?;
    let total_ticks = utime_ticks.saturating_add(stime_ticks);
    // sysconf(_SC_CLK_TCK) is typically 100 on Linux. We rely on
    // libc; if the call fails fall back to 100.
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let hz = if hz <= 0 { 100 } else { hz as u64 };
    Some(total_ticks.saturating_mul(1000) / hz)
}

#[cfg(not(target_os = "linux"))]
fn read_cpu_ms() -> Option<u64> {
    None
}

#[cfg(any(feature = "collector", feature = "iroh"))]
fn capture_tokio() -> Option<Tier3TokioStats> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    let flavor = match handle.runtime_flavor() {
        tokio::runtime::RuntimeFlavor::CurrentThread => "current_thread",
        tokio::runtime::RuntimeFlavor::MultiThread => "multi_thread",
        _ => "unknown",
    };
    Some(Tier3TokioStats {
        flavor: flavor.to_string(),
    })
}

#[cfg(not(any(feature = "collector", feature = "iroh")))]
fn capture_tokio() -> Option<Tier3TokioStats> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capture_returns_a_populated_struct() {
        let ps = ProcessStats::new();
        let snap = ps.capture();
        assert!(snap.captured_at_ms > 0);
        #[cfg(target_os = "linux")]
        {
            assert!(snap.rss_bytes.is_some(), "Linux must read VmRSS");
            assert!(snap.vm_size_bytes.is_some(), "Linux must read VmSize");
            assert!(snap.open_fd_count.is_some(), "Linux must count /proc/self/fd");
            // CPU time may legitimately read as 0 on first call but
            // the field must be present.
            assert!(snap.cpu_ms.is_some());
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn fd_count_is_within_a_sane_range_on_linux() {
        // Strict before/after comparisons are flaky under parallel test
        // execution because other threads open and close fds concurrently.
        // The honest contract is "we can read /proc/self/fd and it returns
        // a plausible count" — the FD ceiling here keeps the assertion
        // honest while never relying on cross-thread fd quietness.
        let ps = ProcessStats::new();
        let n = ps.capture().open_fd_count.expect("fd count on Linux");
        assert!(n >= 3, "every process has stdin/stdout/stderr at minimum");
        assert!(n < 1_000_000, "implausibly large fd count: {n}");
    }
}
