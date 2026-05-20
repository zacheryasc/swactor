//! Outbound reachability probes for tier-3 snapshots
//! (`DIAGNOSTICS_PLAN.md` T3.3).
//!
//! A [`ProbeScheduler`] holds a registered set of probe targets and a
//! per-target counter of attempts/successes. Each refresh sends one
//! small UDP datagram to every target with a short read timeout, then
//! records the outcome both into the per-target aggregate (which lands
//! on the tier-3 [`Tier3ProbeState`] block of every snapshot) and onto
//! the event stream as a [`Event::ProbeSent`] / [`Event::ProbeReceived`]
//! pair. Independent of iroh — the whole point of this layer is to
//! distinguish "iroh can't reach the relay" from "this host can't
//! reach the relay at all."
//!
//! The same shape used elsewhere in tier-3: a synchronous
//! `refresh_now()` entry point safe to call from any context, plus a
//! `start()` helper (feature-gated on `collector`) that drives periodic
//! refreshes on the tokio runtime.

use std::collections::HashMap;
use std::net::{SocketAddr, ToSocketAddrs, UdpSocket};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::diagnostics::event::Event;
use crate::diagnostics::sink::{DynEmitter, noop_emitter};
use crate::diagnostics::snapshot::{ProbeIntrospector, Tier3Probe, Tier3ProbeState};
use crate::diagnostics::wall_ms_now;

/// How often the background task refreshes probes by default — every
/// `~10s` per `DIAGNOSTICS_PLAN.md` T3.3.
pub const DEFAULT_REFRESH_INTERVAL: Duration = Duration::from_secs(10);

/// How long to wait for a probe response before giving up. Kept short
/// so the refresh cadence is the limit, not the timeout.
pub const DEFAULT_PROBE_TIMEOUT: Duration = Duration::from_millis(750);

/// Default probe kinds. Strings so adding a new kind doesn't churn the
/// wire format; constants are here so callers can stay consistent.
pub mod kinds {
    pub const UDP_ECHO: &str = "udp_echo";
    pub const UDP_RELAY: &str = "udp_relay";
    pub const STUN: &str = "stun";
}

/// One registered probe target plus its running aggregates.
#[derive(Debug, Clone)]
struct Target {
    label: String,
    kind: String,
    /// The string the caller registered. Looked up via
    /// `ToSocketAddrs` on each refresh so DNS changes are observed.
    address_string: String,
    /// Cached resolved address — `None` until the first successful
    /// resolution. The wire shape carries this so the post-processor
    /// can correlate "different nodes hit different IPs."
    resolved_addr: Option<SocketAddr>,
    last_attempted_at_ms: u64,
    last_outcome: String,
    last_rtt_ms: Option<u64>,
    last_error: Option<String>,
    attempts: u64,
    successes: u64,
}

impl Target {
    fn into_wire(&self) -> Tier3Probe {
        Tier3Probe {
            target: self.label.clone(),
            kind: self.kind.clone(),
            resolved_addr: self.resolved_addr.map(|a| a.to_string()),
            last_attempted_at_ms: self.last_attempted_at_ms,
            last_outcome: self.last_outcome.clone(),
            last_rtt_ms: self.last_rtt_ms,
            last_error: self.last_error.clone(),
            attempts: self.attempts,
            successes: self.successes,
        }
    }
}

/// Periodic UDP probe scheduler. Cheap to construct; install via
/// [`crate::diagnostics::Aggregator::set_probe_introspector`].
pub struct ProbeScheduler {
    targets: Mutex<HashMap<String, Target>>,
    emitter: Mutex<DynEmitter>,
    timeout: Mutex<Duration>,
    /// Optional override: payload to send. Defaults to `b"PROBE"`.
    payload: Mutex<Vec<u8>>,
}

impl std::fmt::Debug for ProbeScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProbeScheduler")
            .field(
                "targets",
                &self.targets.lock().ok().map(|g| g.len()).unwrap_or(0),
            )
            .finish()
    }
}

impl Default for ProbeScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl ProbeScheduler {
    pub fn new() -> Self {
        Self {
            targets: Mutex::new(HashMap::new()),
            emitter: Mutex::new(noop_emitter()),
            timeout: Mutex::new(DEFAULT_PROBE_TIMEOUT),
            payload: Mutex::new(b"PROBE".to_vec()),
        }
    }

    /// Wire an emitter so per-probe `ProbeSent` / `ProbeReceived`
    /// events reach the bundle's event stream. The default is a no-op
    /// emitter.
    pub fn set_emitter(&self, emitter: DynEmitter) {
        *self
            .emitter
            .lock()
            .expect("probe scheduler emitter mutex poisoned") = emitter;
    }

    /// Override the per-probe read timeout. The default is
    /// [`DEFAULT_PROBE_TIMEOUT`].
    pub fn set_timeout(&self, timeout: Duration) {
        *self
            .timeout
            .lock()
            .expect("probe scheduler timeout mutex poisoned") = timeout;
    }

    /// Override the probe payload. Default is `b"PROBE"`. Keep it
    /// small — UDP MTU friendly and the echo server only ever
    /// returns what it received.
    pub fn set_payload(&self, bytes: Vec<u8>) {
        *self
            .payload
            .lock()
            .expect("probe scheduler payload mutex poisoned") = bytes;
    }

    /// Register a probe target. Duplicate labels are a no-op (the
    /// existing target is kept so accumulated counters survive).
    /// `address` is anything `ToSocketAddrs` understands — bare IPs,
    /// `host:port`, etc. Resolution happens at refresh time.
    pub fn add_target(
        &self,
        label: impl Into<String>,
        address: impl Into<String>,
        kind: impl Into<String>,
    ) {
        let label = label.into();
        let mut targets = self
            .targets
            .lock()
            .expect("probe scheduler targets mutex poisoned");
        targets.entry(label.clone()).or_insert_with(|| Target {
            label,
            kind: kind.into(),
            address_string: address.into(),
            resolved_addr: None,
            last_attempted_at_ms: 0,
            last_outcome: "pending".to_string(),
            last_rtt_ms: None,
            last_error: None,
            attempts: 0,
            successes: 0,
        });
    }

    /// Number of registered targets. Mainly for tests.
    pub fn target_count(&self) -> usize {
        self.targets
            .lock()
            .expect("probe scheduler targets mutex poisoned")
            .len()
    }

    /// Synchronously probe every registered target once. Each target
    /// gets its own ephemeral UDP socket. Safe to call from any
    /// context; performs blocking IO bounded by the configured
    /// per-probe timeout.
    pub fn refresh_now(&self) {
        let snapshot: Vec<Target> = {
            let targets = self
                .targets
                .lock()
                .expect("probe scheduler targets mutex poisoned");
            targets.values().cloned().collect()
        };
        let timeout = *self
            .timeout
            .lock()
            .expect("probe scheduler timeout mutex poisoned");
        let payload = self
            .payload
            .lock()
            .expect("probe scheduler payload mutex poisoned")
            .clone();
        for target in snapshot {
            let updated = probe_once(&target, timeout, &payload);
            self.emit_pair(&updated);
            let mut targets = self
                .targets
                .lock()
                .expect("probe scheduler targets mutex poisoned");
            if let Some(slot) = targets.get_mut(&updated.label) {
                slot.resolved_addr = updated.resolved_addr;
                slot.last_attempted_at_ms = updated.last_attempted_at_ms;
                slot.last_outcome = updated.last_outcome.clone();
                slot.last_rtt_ms = updated.last_rtt_ms;
                slot.last_error = updated.last_error.clone();
                slot.attempts = slot.attempts.saturating_add(1);
                if updated.successes > 0 {
                    slot.successes = slot.successes.saturating_add(1);
                }
            }
        }
    }

    fn emit_pair(&self, t: &Target) {
        let emitter = self
            .emitter
            .lock()
            .expect("probe scheduler emitter mutex poisoned")
            .clone();
        let target_label = match t.resolved_addr {
            Some(addr) => format!("{} ({})", t.label, addr),
            None => t.label.clone(),
        };
        use crate::diagnostics::sink::EventEmitter;
        emitter.emit_event(Event::ProbeSent {
            target: target_label.clone(),
            kind: t.kind.clone(),
        });
        emitter.emit_event(Event::ProbeReceived {
            target: target_label,
            kind: t.kind.clone(),
            rtt_ms: t.last_rtt_ms,
            outcome: t.last_outcome.clone(),
        });
    }

    /// Spawn a periodic refresh task on the current tokio runtime.
    /// Refreshes immediately (so the first post-`start` snapshot has
    /// fresh data) then ticks every `interval`. Each refresh runs
    /// inside `tokio::task::spawn_blocking` so the scheduler is not
    /// starved by blocking UDP IO.
    #[cfg(feature = "collector")]
    pub fn start(
        self: std::sync::Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let s = self.clone();
            let _ = tokio::task::spawn_blocking(move || s.refresh_now()).await;
            let mut tick = tokio::time::interval(interval);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            tick.tick().await; // consume immediate fire
            loop {
                tick.tick().await;
                let s = self.clone();
                let _ = tokio::task::spawn_blocking(move || s.refresh_now()).await;
            }
        })
    }
}

impl ProbeIntrospector for ProbeScheduler {
    fn capture(&self) -> Tier3ProbeState {
        let mut probes: Vec<Tier3Probe> = self
            .targets
            .lock()
            .expect("probe scheduler targets mutex poisoned")
            .values()
            .map(|t| t.into_wire())
            .collect();
        probes.sort_by(|a, b| a.kind.cmp(&b.kind).then(a.target.cmp(&b.target)));
        Tier3ProbeState {
            probes,
            scraped_at_ms: wall_ms_now(),
        }
    }
}

fn probe_once(target: &Target, timeout: Duration, payload: &[u8]) -> Target {
    let mut updated = target.clone();
    updated.last_attempted_at_ms = wall_ms_now();
    updated.last_rtt_ms = None;
    updated.last_error = None;
    updated.successes = 0;
    let addr = match resolve(&updated.address_string) {
        Some(a) => a,
        None => {
            updated.resolved_addr = None;
            updated.last_outcome = "unresolved".to_string();
            updated.last_error = Some(format!(
                "no socket address for {}",
                updated.address_string
            ));
            return updated;
        }
    };
    updated.resolved_addr = Some(addr);
    let bind: SocketAddr = if addr.is_ipv4() {
        "0.0.0.0:0".parse().expect("static bind addr")
    } else {
        "[::]:0".parse().expect("static bind addr")
    };
    let socket = match UdpSocket::bind(bind) {
        Ok(s) => s,
        Err(e) => {
            updated.last_outcome = "error".to_string();
            updated.last_error = Some(format!("bind: {e}"));
            return updated;
        }
    };
    if let Err(e) = socket.set_read_timeout(Some(timeout)) {
        updated.last_outcome = "error".to_string();
        updated.last_error = Some(format!("set_read_timeout: {e}"));
        return updated;
    }
    let started = Instant::now();
    if let Err(e) = socket.send_to(payload, addr) {
        updated.last_outcome = classify_send_error(&e);
        updated.last_error = Some(e.to_string());
        return updated;
    }
    let mut buf = [0u8; 1500];
    match socket.recv_from(&mut buf) {
        Ok(_) => {
            let elapsed = started.elapsed();
            updated.last_outcome = "ok".to_string();
            updated.last_rtt_ms = Some(elapsed.as_millis() as u64);
            updated.successes = 1;
        }
        Err(e) => {
            updated.last_outcome = classify_recv_error(&e);
            updated.last_error = Some(e.to_string());
        }
    }
    updated
}

fn resolve(address: &str) -> Option<SocketAddr> {
    address.to_socket_addrs().ok()?.next()
}

fn classify_send_error(e: &std::io::Error) -> String {
    use std::io::ErrorKind::*;
    match e.kind() {
        ConnectionRefused => "refused".into(),
        TimedOut => "timeout".into(),
        _ => "error".into(),
    }
}

fn classify_recv_error(e: &std::io::Error) -> String {
    use std::io::ErrorKind::*;
    match e.kind() {
        TimedOut | WouldBlock => "timeout".into(),
        ConnectionRefused => "refused".into(),
        _ => "error".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::UdpSocket as StdUdpSocket;
    use std::sync::Arc;
    use std::thread;

    /// Spin up a tiny synchronous UDP echo loop in a background thread
    /// for unit tests, returning the bound socket address. The thread
    /// runs until the socket is dropped — caller pins the socket via
    /// the returned `Arc<UdpSocket>` so the loop stays alive.
    fn spawn_unit_echo() -> (SocketAddr, Arc<StdUdpSocket>) {
        let sock = Arc::new(StdUdpSocket::bind("127.0.0.1:0").expect("echo bind"));
        sock.set_read_timeout(Some(Duration::from_millis(500))).unwrap();
        let addr = sock.local_addr().expect("echo addr");
        let runner = sock.clone();
        thread::spawn(move || {
            let mut buf = [0u8; 1500];
            loop {
                match runner.recv_from(&mut buf) {
                    Ok((n, peer)) => {
                        let _ = runner.send_to(&buf[..n], peer);
                    }
                    Err(e) => {
                        if e.kind() == std::io::ErrorKind::WouldBlock
                            || e.kind() == std::io::ErrorKind::TimedOut
                        {
                            continue;
                        }
                        // Socket closed (Arc dropped) — exit.
                        break;
                    }
                }
            }
        });
        (addr, sock)
    }

    #[test]
    fn refresh_against_live_echo_records_success_and_rtt() {
        let (echo_addr, _sock) = spawn_unit_echo();
        let scheduler = ProbeScheduler::new();
        scheduler.set_timeout(Duration::from_millis(500));
        scheduler.add_target("echo", echo_addr.to_string(), kinds::UDP_ECHO);
        scheduler.refresh_now();
        let state = scheduler.capture();
        assert_eq!(state.probes.len(), 1);
        let p = &state.probes[0];
        assert_eq!(p.last_outcome, "ok");
        assert!(p.last_rtt_ms.is_some(), "rtt must be recorded");
        assert_eq!(p.attempts, 1);
        assert_eq!(p.successes, 1);
        assert!(p.resolved_addr.is_some());
        assert!(p.last_error.is_none());
    }

    #[test]
    fn refresh_against_silent_target_records_timeout() {
        // Reserve a port by binding then dropping immediately — gives
        // us an address that's almost certainly not listening for
        // anything. UDP send "succeeds" silently but recv times out.
        let probe_sock = StdUdpSocket::bind("127.0.0.1:0").unwrap();
        let dead_addr = probe_sock.local_addr().unwrap();
        drop(probe_sock);

        let scheduler = ProbeScheduler::new();
        scheduler.set_timeout(Duration::from_millis(50));
        scheduler.add_target("silent", dead_addr.to_string(), kinds::UDP_ECHO);
        scheduler.refresh_now();
        let state = scheduler.capture();
        let p = &state.probes[0];
        // Either timeout (no reply) or refused/error if the kernel
        // delivered an ICMP unreachable. Both are valid failures —
        // critical contract: no success, attempts counted.
        assert_ne!(p.last_outcome, "ok");
        assert_eq!(p.attempts, 1);
        assert_eq!(p.successes, 0);
        assert!(p.last_error.is_some());
    }

    #[test]
    fn unresolved_target_classified_as_unresolved() {
        let scheduler = ProbeScheduler::new();
        // Empty hostname has no socket address.
        scheduler.add_target("nothing", "", kinds::UDP_ECHO);
        scheduler.refresh_now();
        let state = scheduler.capture();
        let p = &state.probes[0];
        assert_eq!(p.last_outcome, "unresolved");
        assert!(p.resolved_addr.is_none());
        assert_eq!(p.attempts, 1);
        assert_eq!(p.successes, 0);
    }
}
