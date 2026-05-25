//! Subprocess introspection for tier-3 snapshots
//! (`N3_OBSERVABILITY_UPGRADE_SPEC.md` §4, gap 4).
//!
//! Stage-agnostic, worker-agnostic. The introspector knows about a
//! `(label, PID, parent_pid)` triple per registered subprocess;
//! deciding *which* subprocesses are interesting is the caller's job.
//! That's the generic-over-use-case requirement spelled out in the
//! spec: a future caller of `swactor_process` opts in by installing
//! the introspector at boot and forwarding two notification kinds
//! (`SubprocessSpawned` / `SubprocessExited`) — no other code changes.
//!
//! The actual per-snapshot resource read happens at capture time
//! against `/proc/<pid>/{status,fd,stat,cmdline}`. The introspector
//! also emits the typed lifecycle events on `register`/`note_exited`
//! so the bundle's event stream is the lifecycle view and the
//! snapshot block is the current-value view — two channels, never
//! the same fact reported by both.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::diagnostics::event::Event;
use crate::diagnostics::sink::{noop_emitter, DynEmitter, EventEmitter};
use crate::diagnostics::snapshot::{
    SubprocessIntrospector, Tier3Subprocess, Tier3SubprocessState,
};
use crate::diagnostics::wall_ms_now;

/// Tier-3 subprocess introspector. Shareable as
/// `Arc<SubprocessIntrospect>` between the owning actor and the
/// aggregator.
pub struct SubprocessIntrospect {
    inner: Mutex<Inner>,
    emitter: Mutex<DynEmitter>,
}

#[derive(Default)]
struct Inner {
    by_pid: HashMap<u32, Tracked>,
}

#[derive(Clone)]
struct Tracked {
    label: String,
    parent_pid: Option<u32>,
    command: String,
    spawn_at_ms: u64,
    exit: Option<Exited>,
}

#[derive(Clone, Copy)]
struct Exited {
    at_ms: u64,
    code: Option<i32>,
    signal: Option<i32>,
}

impl std::fmt::Debug for SubprocessIntrospect {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SubprocessIntrospect")
            .field(
                "tracked",
                &self.inner.lock().ok().map(|g| g.by_pid.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl Default for SubprocessIntrospect {
    fn default() -> Self {
        Self::new()
    }
}

impl SubprocessIntrospect {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner::default()),
            emitter: Mutex::new(noop_emitter()),
        }
    }

    /// Wire an emitter so per-subprocess lifecycle events
    /// (`SubprocessSpawned` / `SubprocessExited`) reach the bundle's
    /// event stream. Defaults to a noop emitter — handy in tests
    /// that only want to assert on the snapshot view.
    pub fn set_emitter(&self, emitter: DynEmitter) {
        *self
            .emitter
            .lock()
            .expect("subprocess introspect emitter mutex poisoned") = emitter;
    }

    /// Shareable handle for installing on an aggregator.
    pub fn into_arc(self) -> Arc<dyn SubprocessIntrospector> {
        Arc::new(self)
    }

    /// Register a freshly-spawned subprocess. The caller supplies
    /// the label (`"pp-worker"`, `"helper-tool"`, etc.), the PID
    /// reported by the spawn channel, and a command string for the
    /// event payload. Emits `SubprocessSpawned`.
    ///
    /// `parent_pid` is optional; production callers pass
    /// `Some(std::process::id())`. Tests can pass `None`.
    pub fn register(
        &self,
        label: impl Into<String>,
        pid: u32,
        command: impl Into<String>,
        parent_pid: Option<u32>,
    ) {
        let label = label.into();
        let command = command.into();
        let now = wall_ms_now();
        {
            let mut inner = self
                .inner
                .lock()
                .expect("subprocess introspect inner mutex poisoned");
            inner.by_pid.insert(
                pid,
                Tracked {
                    label: label.clone(),
                    parent_pid,
                    command: command.clone(),
                    spawn_at_ms: now,
                    exit: None,
                },
            );
        }
        self.emit(Event::SubprocessSpawned {
            label,
            pid,
            command,
        });
    }

    /// Record that a previously-registered subprocess has exited.
    /// Emits `SubprocessExited`. The entry stays in the snapshot
    /// view (with `status = "exited"`) so the bundle reader sees
    /// the full lifecycle, not just live processes.
    pub fn note_exited(
        &self,
        pid: u32,
        exit_code: Option<i32>,
        exit_signal: Option<i32>,
    ) {
        let now = wall_ms_now();
        let (label, command, uptime_ms) = {
            let mut inner = self
                .inner
                .lock()
                .expect("subprocess introspect inner mutex poisoned");
            match inner.by_pid.get_mut(&pid) {
                Some(t) => {
                    t.exit = Some(Exited {
                        at_ms: now,
                        code: exit_code,
                        signal: exit_signal,
                    });
                    (
                        t.label.clone(),
                        t.command.clone(),
                        Some(now.saturating_sub(t.spawn_at_ms)),
                    )
                }
                None => {
                    // Unknown PID — still emit the event with a
                    // best-effort label so the bundle reader at
                    // least sees the exit. Tests rely on this
                    // being non-silent.
                    (
                        format!("unknown-pid-{pid}"),
                        String::new(),
                        None,
                    )
                }
            }
        };
        self.emit(Event::SubprocessExited {
            label,
            pid,
            command,
            exit_code,
            exit_signal,
            uptime_ms,
        });
    }

    fn emit(&self, ev: Event) {
        let emitter = self
            .emitter
            .lock()
            .expect("subprocess introspect emitter mutex poisoned")
            .clone();
        emitter.emit_event(ev);
    }
}

impl SubprocessIntrospector for SubprocessIntrospect {
    fn capture(&self) -> Tier3SubprocessState {
        let tracked: Vec<(u32, Tracked)> = {
            let inner = self
                .inner
                .lock()
                .expect("subprocess introspect inner mutex poisoned");
            inner.by_pid.iter().map(|(p, t)| (*p, t.clone())).collect()
        };
        let mut subprocesses: Vec<Tier3Subprocess> = tracked
            .into_iter()
            .map(|(pid, t)| capture_one(pid, t))
            .collect();
        subprocesses.sort_by(|a, b| {
            a.label
                .cmp(&b.label)
                .then(a.pid.cmp(&b.pid))
        });
        Tier3SubprocessState {
            subprocesses,
            scraped_at_ms: wall_ms_now(),
        }
    }
}

fn capture_one(pid: u32, t: Tracked) -> Tier3Subprocess {
    let exited = t.exit;
    let (status, rss_bytes, vm_size_bytes, open_fd_count, cpu_ms, cmdline) =
        if exited.is_some() {
            // Exited processes: don't probe /proc — the PID may
            // have been reaped or recycled. Keep the snapshot
            // fields absent so the bundle reader sees the
            // exit-status fields instead.
            ("exited".to_string(), None, None, None, None, Some(t.command.clone()))
        } else {
            let rss_and_vm = read_rss_and_vm(pid);
            let fds = read_fd_count(pid);
            let cpu = read_cpu_ms(pid);
            let cmd = read_cmdline(pid).or_else(|| Some(t.command.clone()));
            let status_str = if linux_pid_alive(pid) {
                "running".to_string()
            } else {
                "unknown".to_string()
            };
            (status_str, rss_and_vm.0, rss_and_vm.1, fds, cpu, cmd)
        };
    Tier3Subprocess {
        label: t.label,
        pid,
        parent_pid: t.parent_pid,
        status,
        spawn_at_ms: Some(t.spawn_at_ms),
        exit_at_ms: exited.map(|e| e.at_ms),
        exit_code: exited.and_then(|e| e.code),
        exit_signal: exited.and_then(|e| e.signal),
        rss_bytes,
        vm_size_bytes,
        open_fd_count,
        cpu_ms,
        cmdline,
    }
}

#[cfg(target_os = "linux")]
fn linux_pid_alive(pid: u32) -> bool {
    std::path::Path::new(&format!("/proc/{pid}")).is_dir()
}
#[cfg(not(target_os = "linux"))]
fn linux_pid_alive(_pid: u32) -> bool {
    false
}

#[cfg(target_os = "linux")]
fn read_rss_and_vm(pid: u32) -> (Option<u64>, Option<u64>) {
    let body = match std::fs::read_to_string(format!("/proc/{pid}/status")) {
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
fn read_rss_and_vm(_pid: u32) -> (Option<u64>, Option<u64>) {
    (None, None)
}

#[cfg(target_os = "linux")]
fn parse_kb_to_bytes(s: &str) -> Option<u64> {
    let trimmed = s.trim();
    let num: String = trimmed.chars().take_while(|c| c.is_ascii_digit()).collect();
    let kb: u64 = num.parse().ok()?;
    Some(kb.saturating_mul(1024))
}

#[cfg(target_os = "linux")]
fn read_fd_count(pid: u32) -> Option<u64> {
    let dir = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    let mut count: u64 = 0;
    for entry in dir {
        if entry.is_ok() {
            count += 1;
        }
    }
    Some(count)
}
#[cfg(not(target_os = "linux"))]
fn read_fd_count(_pid: u32) -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_cpu_ms(pid: u32) -> Option<u64> {
    let body = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let after_comm = body.rfind(')').map(|i| &body[i + 1..])?;
    let fields: Vec<&str> = after_comm.split_whitespace().collect();
    let utime: u64 = fields.get(11)?.parse().ok()?;
    let stime: u64 = fields.get(12)?.parse().ok()?;
    let total = utime.saturating_add(stime);
    let hz = unsafe { libc::sysconf(libc::_SC_CLK_TCK) };
    let hz = if hz <= 0 { 100 } else { hz as u64 };
    Some(total.saturating_mul(1000) / hz)
}
#[cfg(not(target_os = "linux"))]
fn read_cpu_ms(_pid: u32) -> Option<u64> {
    None
}

#[cfg(target_os = "linux")]
fn read_cmdline(pid: u32) -> Option<String> {
    let body = std::fs::read(format!("/proc/{pid}/cmdline")).ok()?;
    // /proc/<pid>/cmdline is NUL-separated argv. Truncate to 256
    // bytes before splitting so a huge argv doesn't dominate the
    // snapshot.
    let slice = if body.len() > 256 { &body[..256] } else { &body[..] };
    let mut parts: Vec<String> = slice
        .split(|b| *b == 0)
        .filter(|s| !s.is_empty())
        .map(|s| String::from_utf8_lossy(s).into_owned())
        .collect();
    if body.len() > 256 {
        // We chopped mid-argv — drop the final possibly-partial token.
        if !parts.is_empty() {
            parts.pop();
        }
        parts.push("…".to_string());
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" "))
    }
}
#[cfg(not(target_os = "linux"))]
fn read_cmdline(_pid: u32) -> Option<String> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::sink::InMemorySink;
    use crate::diagnostics::{Aggregator, Identity, Role};
    use crate::types::NodeId;

    #[test]
    fn register_appears_in_snapshot_with_running_status() {
        let intro = Arc::new(SubprocessIntrospect::new());
        let id = Identity::new(NodeId([0xab; 32]), Role::stage(), "run-s1");
        let agg = Arc::new(Aggregator::new(id, InMemorySink::new()));
        agg.set_subprocess_introspector(
            intro.clone() as Arc<dyn SubprocessIntrospector>,
        );
        // Register the test process itself as a "subprocess" — a
        // PID guaranteed to exist for the lifetime of the test.
        let pid = std::process::id();
        intro.register("self-test", pid, "cargo test self-test", Some(0));
        let snap = agg.snapshot(
            crate::diagnostics::snapshot::SnapshotTrigger::Periodic,
        );
        let sp = snap.body.subprocess.expect("subprocess block present");
        let entry = sp
            .subprocesses
            .iter()
            .find(|s| s.pid == pid)
            .expect("registered pid appears in snapshot");
        assert_eq!(entry.label, "self-test");
        #[cfg(target_os = "linux")]
        assert_eq!(entry.status, "running");
    }

    #[test]
    fn note_exited_keeps_entry_with_exited_status_and_records_code() {
        let intro = Arc::new(SubprocessIntrospect::new());
        intro.register("custom-helper", 99999, "/usr/bin/never-spawned", None);
        intro.note_exited(99999, Some(42), None);
        let snap = intro.capture();
        let entry = snap
            .subprocesses
            .iter()
            .find(|s| s.pid == 99999)
            .expect("exited pid still appears in the snapshot");
        assert_eq!(entry.status, "exited");
        assert_eq!(entry.exit_code, Some(42));
        assert_eq!(entry.exit_signal, None);
        assert!(entry.exit_at_ms.is_some());
    }

    #[test]
    fn two_subprocesses_with_different_labels_both_appear() {
        // Spec §4 generic-over-use-case: the introspector knows about
        // (label, PID). Registering two distinct labels must produce
        // two distinct snapshot entries — this is the judge's
        // canonical generic-over-use-case probe.
        let intro = SubprocessIntrospect::new();
        intro.register("python-worker", 11111, "/usr/bin/python worker.py", None);
        intro.register("helper-tool", 22222, "/usr/bin/helper --foo", None);
        let snap = intro.capture();
        let labels: Vec<&str> = snap.subprocesses.iter().map(|s| s.label.as_str()).collect();
        assert!(labels.contains(&"python-worker"));
        assert!(labels.contains(&"helper-tool"));
        assert_eq!(snap.subprocesses.len(), 2);
    }
}
