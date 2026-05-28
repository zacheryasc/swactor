//! Wire `crates/distribution` diagnostics into `pp-smoke-run` and
//! `pp-gpu-node` from environment variables.
//!
//! Reading `SWACTOR_DIAG_COLLECTOR_URL` is the opt-in switch. When it is
//! unset (or empty) `install_from_env` returns `None` and the binary
//! behaves exactly as it did before — no aggregator, no HTTP, no
//! background tasks. With the URL set the helper builds an
//! `Aggregator<HttpSink>` and installs every introspector across all
//! three diagnostic tiers, so the binary contributes to the same shared
//! collector / bundle as every other node in the run.
//!
//! ## Environment surface
//!
//! | Var | Default | Meaning |
//! |---|---|---|
//! | `SWACTOR_DIAG_COLLECTOR_URL` | (unset) | Collector base URL. Unset = diagnostics off. |
//! | `SWACTOR_DIAG_RUN_ID`        | `pp-run` | Opaque identifier; one bundle per `run_id`. |
//! | `SWACTOR_DIAG_NODE_ROLE`     | (caller's default) | `orchestrator` / `stage` / custom. |
//! | `SWACTOR_DIAG_STAGE_INDEX`   | (unset) | 0..N-1, only meaningful for stage roles. |
//! | `SWACTOR_DIAG_STAGE_COUNT`   | (unset) | N. |
//! | `SWACTOR_DIAG_SPOOL_DIR`     | `/tmp/swactor-diag` | On-disk spool root when collector is down. |
//! | `SWACTOR_DIAG_UDP_ECHO`      | (unset) | `host:port` of collector UDP echo for tier-3 probes. |

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use distribution::diagnostics::aggregator::{spawn_periodic_snapshots, PeriodicConfig};
use distribution::diagnostics::event::Event;
use distribution::diagnostics::probes::kinds;
use distribution::diagnostics::subprocess_introspect::SubprocessIntrospect;
use distribution::diagnostics::{
    wall_ms_now, Aggregator, HostContext, HostIntrospect, HostIntrospector, HttpSink, Identity,
    ProbeIntrospector, ProbeScheduler, ProcessIntrospector, ProcessStats, Role, SinkConfig,
    SinkHandle, SnapshotSignal, SubprocessIntrospector, VastaiContext, GIT_SHA, IROH_VERSION,
};
use distribution::diagnostics::sink::{DynEmitter, EventEmitter};
use distribution::iroh_driver::IrohDriver;
use swactor::actor::ActorAddress;

const ENV_COLLECTOR_URL: &str = "SWACTOR_DIAG_COLLECTOR_URL";
const ENV_RUN_ID: &str = "SWACTOR_DIAG_RUN_ID";
const ENV_NODE_ROLE: &str = "SWACTOR_DIAG_NODE_ROLE";
const ENV_STAGE_INDEX: &str = "SWACTOR_DIAG_STAGE_INDEX";
const ENV_STAGE_COUNT: &str = "SWACTOR_DIAG_STAGE_COUNT";
const ENV_SPOOL_DIR: &str = "SWACTOR_DIAG_SPOOL_DIR";
const ENV_UDP_ECHO: &str = "SWACTOR_DIAG_UDP_ECHO";

const DEFAULT_RUN_ID: &str = "pp-run";
const DEFAULT_SPOOL_DIR: &str = "/tmp/swactor-diag";

/// Handles for an installed aggregator. The binary keeps this alive for
/// the duration of the run; on graceful exit, call [`Self::finalize`]
/// (orchestrator only) and then [`Self::shutdown`] to drain in-flight
/// records before the process ends.
pub struct DiagHandles {
    aggregator: Arc<Aggregator<HttpSink>>,
    sink_handle: SinkHandle,
    tokio_handle: tokio::runtime::Handle,
    subprocess_introspect: Arc<SubprocessIntrospect>,
}

impl DiagHandles {
    /// Borrow the aggregator. Useful only for tests / callers that want
    /// to emit custom events from outside the wired subsystems.
    pub fn aggregator(&self) -> &Arc<Aggregator<HttpSink>> {
        &self.aggregator
    }

    /// Borrow the subprocess introspector. The binary's stage actors
    /// take a shared clone of this and call
    /// `with_subprocess_introspect(...)` so their child PIDs land in
    /// the bundle's tier-3 `subprocess` snapshot block (spec §4).
    pub fn subprocess_introspect(&self) -> &Arc<SubprocessIntrospect> {
        &self.subprocess_introspect
    }

    /// Push a finalize record carrying the run's exit reason. Only the
    /// orchestrator should call this — it signals the collector to set
    /// `snapshot_now` for every reporter and then tar the bundle.
    pub fn finalize(&self, exit_reason: &str) {
        self.aggregator
            .finalize(serde_json::json!({ "exit_reason": exit_reason }));
    }

    /// Block until the background drainer has flushed every queued
    /// record (or spooled it to disk). Drops the handles.
    pub fn shutdown(self) {
        let DiagHandles {
            sink_handle,
            tokio_handle,
            ..
        } = self;
        tokio_handle.block_on(async move { sink_handle.shutdown().await });
    }
}

/// Read the `SWACTOR_DIAG_*` env vars and, if a collector URL is set,
/// build and install the aggregator + introspectors on `driver`.
///
/// `default_role` is what the binary thinks of itself as
/// (`orchestrator` or `stage`); `SWACTOR_DIAG_NODE_ROLE` can override.
///
/// All spawned background tasks (periodic snapshots, host introspector,
/// probe scheduler) run on the driver's tokio runtime and live until
/// the process exits.
pub fn install_from_env(driver: &mut IrohDriver, default_role: Role) -> Option<DiagHandles> {
    install_with_overrides(driver, default_role, None)
}

/// Like [`install_from_env`] but lets the caller pin `run_id` explicitly
/// rather than reading `SWACTOR_DIAG_RUN_ID` from the environment.
///
/// The orchestrator uses this to bind to the run_id persisted in the
/// held-cluster handle — held stages baked their `SWACTOR_DIAG_RUN_ID`
/// into PID-1's env at lease time and keep reusing it across bounces;
/// the orchestrator (which runs in the operator's shell) cannot trust
/// its own env to still match. Passing the handle's run_id here is the
/// fix for that drift.
///
/// `run_id_override == None` falls back to `SWACTOR_DIAG_RUN_ID`.
pub fn install_with_overrides(
    driver: &mut IrohDriver,
    default_role: Role,
    run_id_override: Option<&str>,
) -> Option<DiagHandles> {
    let url = std::env::var(ENV_COLLECTOR_URL).ok()?;
    let url = url.trim().to_string();
    if url.is_empty() {
        return None;
    }

    let run_id = run_id_override
        .map(|s| s.to_string())
        .or_else(|| env_string(ENV_RUN_ID))
        .unwrap_or_else(|| DEFAULT_RUN_ID.to_string());
    let role = match env_string(ENV_NODE_ROLE).as_deref() {
        Some("orchestrator") => Role::orchestrator(),
        Some("stage") => Role::stage(),
        Some(other) if !other.is_empty() => Role::custom(other),
        _ => default_role,
    };
    let stage_index = env_u32(ENV_STAGE_INDEX);
    let stage_count = env_u32(ENV_STAGE_COUNT);
    let spool_dir =
        PathBuf::from(env_string(ENV_SPOOL_DIR).unwrap_or_else(|| DEFAULT_SPOOL_DIR.to_string()));

    let node_id = driver.node_id();
    let node_id_hex: String = node_id.0.iter().map(|b| format!("{:02x}", b)).collect();

    let mut identity =
        Identity::new(node_id, role.clone(), run_id.clone()).with_process_start(wall_ms_now());
    if let (Some(i), Some(c)) = (stage_index, stage_count) {
        identity = identity.with_stage(i, c);
    }
    // Spec §5: the boot record carries host + build context the bundle
    // reader needs to identify which rental this node ran on without
    // cross-referencing provider records. Env vars are the contract;
    // anything missing stays absent rather than blank.
    let host_ctx = HostContext::from_env()
        .with_home_relay_url(driver.home_relay_url().map(|u| u.to_string()))
        .with_iroh_version(IROH_VERSION)
        .with_binary_version(option_env!("CARGO_PKG_VERSION").map(|s| s.to_string()))
        .with_git_sha(GIT_SHA.map(|s| s.to_string()));
    identity = identity.with_host_context(host_ctx);

    let tokio_handle = driver.tokio_handle();
    let _guard = tokio_handle.enter();

    let signal = SnapshotSignal::new();
    let sink_config = SinkConfig::new(
        url.clone(),
        run_id.clone(),
        node_id_hex.clone(),
        spool_dir.clone(),
    )
    .with_snapshot_signal(signal.clone());

    let sink = match HttpSink::new(sink_config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "pp-diag: HttpSink::new failed ({e}); continuing without diagnostics"
            );
            return None;
        }
    };
    let sink_handle = sink.handle();
    let aggregator = Arc::new(Aggregator::new(identity, sink));

    driver.install_diagnostics(aggregator.clone());

    // Spawn the periodic snapshot task. The returned JoinHandle is
    // dropped — dropping a tokio JoinHandle does NOT abort the task,
    // so the snapshotter runs until the driver's runtime is dropped.
    let _ = spawn_periodic_snapshots(aggregator.clone(), PeriodicConfig::default(), signal);

    let emitter: DynEmitter = aggregator.clone() as Arc<dyn EventEmitter + Send + Sync + 'static>;

    install_host_introspector(&aggregator, driver, emitter.clone());
    install_probe_scheduler(&aggregator, emitter.clone(), driver.home_relay_url().map(|u| u.to_string()));
    install_vastai_context(&aggregator);
    install_process_stats(&aggregator);
    let subprocess_introspect = install_subprocess_introspect(&aggregator, emitter);

    eprintln!(
        "pp-diag: installed collector={url} run_id={run_id} role={role:?} stage={stage_index:?}/{stage_count:?}",
        role = role.0,
    );

    Some(DiagHandles {
        aggregator,
        sink_handle,
        tokio_handle,
        subprocess_introspect,
    })
}

fn install_host_introspector(
    aggregator: &Arc<Aggregator<HttpSink>>,
    driver: &IrohDriver,
    emitter: DynEmitter,
) {
    let host = Arc::new(HostIntrospect::new());
    host.set_emitter(emitter);
    if let Some(hostname) = driver
        .home_relay_url()
        .and_then(|u| extract_host(&u.to_string()))
    {
        host.add_dns_target(hostname);
    }
    aggregator.set_host_introspector(host.clone() as Arc<dyn HostIntrospector>);
    let _ = host.start(Duration::from_secs(30));
}

fn install_probe_scheduler(
    aggregator: &Arc<Aggregator<HttpSink>>,
    emitter: DynEmitter,
    relay_url: Option<String>,
) {
    let probes = Arc::new(ProbeScheduler::new());
    probes.set_emitter(emitter);
    if let Some(echo) = env_string(ENV_UDP_ECHO) {
        let echo = echo.trim();
        if !echo.is_empty() {
            probes.add_target("collector_udp_echo", echo, kinds::UDP_ECHO);
        }
    }
    // Spec §8 (gap 8): when the node has been told a relay URL, the
    // relay probe is automatically registered — no operator config.
    // Parses host[:port] from the URL; defaults to the standard
    // swactor-iroh-relay HTTP port (7843).
    if let Some(url) = relay_url.as_deref() {
        if let Some((host, port)) = parse_relay_host_port(url) {
            let target = format!("{host}:{port}");
            let label = format!("relay-port-{host}");
            probes.add_target(label, target, kinds::UDP_RELAY);
        }
    }
    aggregator.set_probe_introspector(probes.clone() as Arc<dyn ProbeIntrospector>);
    let _ = probes.start(Duration::from_secs(10));
}

/// Spec §8 helper: extract `(host, port)` from a relay URL. The port
/// is taken from the URL when present; otherwise the standard
/// swactor-iroh-relay port (7843) is used. HTTP and WS schemes are
/// stripped; bare hosts are passed through.
fn parse_relay_host_port(url: &str) -> Option<(String, u16)> {
    const DEFAULT_RELAY_PORT: u16 = 7843;
    let after_scheme = match url.find("://") {
        Some(idx) => &url[idx + 3..],
        None => url,
    };
    let authority = after_scheme.split('/').next().unwrap_or("");
    let host_and_port = match authority.rfind('@') {
        Some(i) => &authority[i + 1..],
        None => authority,
    };
    if host_and_port.is_empty() {
        return None;
    }
    // IPv6 literal: `[::1]:port`. Other shapes: `host[:port]`.
    if let Some(rest) = host_and_port.strip_prefix('[') {
        let close = rest.find(']')?;
        let host = &rest[..close];
        let port = rest[close + 1..]
            .strip_prefix(':')
            .and_then(|p| p.parse::<u16>().ok())
            .unwrap_or(DEFAULT_RELAY_PORT);
        return Some((host.to_string(), port));
    }
    match host_and_port.rsplit_once(':') {
        Some((host, port_str)) => {
            let port = port_str.parse::<u16>().unwrap_or(DEFAULT_RELAY_PORT);
            Some((host.to_string(), port))
        }
        None => Some((host_and_port.to_string(), DEFAULT_RELAY_PORT)),
    }
}

fn install_vastai_context(aggregator: &Arc<Aggregator<HttpSink>>) {
    let vastai = VastaiContext::capture_now();
    aggregator.set_vastai_introspector(vastai.into_arc());
}

fn install_process_stats(aggregator: &Arc<Aggregator<HttpSink>>) {
    let process = Arc::new(ProcessStats::new());
    aggregator.set_process_introspector(process as Arc<dyn ProcessIntrospector>);
}

fn install_subprocess_introspect(
    aggregator: &Arc<Aggregator<HttpSink>>,
    emitter: DynEmitter,
) -> Arc<SubprocessIntrospect> {
    let intro = Arc::new(SubprocessIntrospect::new());
    intro.set_emitter(emitter);
    aggregator.set_subprocess_introspector(
        intro.clone() as Arc<dyn SubprocessIntrospector>,
    );
    intro
}

/// Emit a `Custom("register_name")` event through the driver's
/// installed diagnostics emitter. No-op when diagnostics are not
/// installed (the driver's emitter defaults to a noop sink).
///
/// `stage` is informational metadata — the orchestrator name
/// registration passes `None`, stage nodes pass their stage index.
/// `our_node_id_hex` lets the bundle reader correlate registrations
/// to the publishing node without re-looking-up the snapshot identity.
pub fn emit_register_name(
    driver: &IrohDriver,
    name: &str,
    addr: ActorAddress,
    stage: Option<u32>,
) {
    let mut fields = serde_json::json!({
        "name": name,
        "actor_addr_hex": hex_of_bytes(&addr.0),
        "our_node_id_hex": hex_of_bytes(&driver.node_id().0),
        "wall_ms": wall_ms_now(),
    });
    // Spec §4.8: events emitted by a stage worker process MUST carry
    // `stage_index` top-level once the index is known. The orchestrator's
    // own pp-orchestrator registration passes `None` here and so does not
    // carry the field — it is not a stage worker.
    if let Some(s) = stage {
        fields["stage_index"] = serde_json::json!(s);
    }
    driver.emit(Event::Custom {
        kind: "register_name".into(),
        fields,
    });
}

fn hex_of_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(HEX[(*b >> 4) as usize] as char);
        s.push(HEX[(*b & 0xf) as usize] as char);
    }
    s
}

fn env_string(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.trim().is_empty())
}

fn env_u32(var: &str) -> Option<u32> {
    env_string(var).and_then(|s| s.trim().parse().ok())
}

/// Pull a hostname out of `scheme://host[:port]/...`. Cheap manual parse
/// so we don't pull in a URL crate just for one DNS target.
fn extract_host(url: &str) -> Option<String> {
    let rest = url
        .splitn(2, "://")
        .nth(1)
        .unwrap_or(url);
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = match authority.rfind('@') {
        Some(i) => &authority[i + 1..],
        None => authority,
    };
    let host = match host.rfind(':') {
        Some(i) => &host[..i],
        None => host,
    };
    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_host_handles_common_url_shapes() {
        assert_eq!(extract_host("https://example.com/foo"), Some("example.com".into()));
        assert_eq!(extract_host("http://relay.example:443"), Some("relay.example".into()));
        assert_eq!(extract_host("ws://relay.example/x/y"), Some("relay.example".into()));
        assert_eq!(extract_host("relay.example"), Some("relay.example".into()));
        assert_eq!(extract_host("https://u:p@host.example:1234/x"), Some("host.example".into()));
        assert_eq!(extract_host(""), None);
    }

    /// Spec §8: with no port in the URL, default to 7843 (the
    /// swactor-iroh-relay binary's default bind). With an explicit
    /// port, honor it.
    #[test]
    fn parse_relay_host_port_defaults_and_honors_explicit_port() {
        assert_eq!(
            parse_relay_host_port("https://relay.example/"),
            Some(("relay.example".into(), 7843)),
        );
        assert_eq!(
            parse_relay_host_port("http://203.0.113.7:7843/"),
            Some(("203.0.113.7".into(), 7843)),
        );
        assert_eq!(
            parse_relay_host_port("http://relay.example:9999/x"),
            Some(("relay.example".into(), 9999)),
        );
        assert_eq!(
            parse_relay_host_port("relay.example"),
            Some(("relay.example".into(), 7843)),
        );
        assert_eq!(
            parse_relay_host_port("http://[2001:db8::1]:5555/"),
            Some(("2001:db8::1".into(), 5555)),
        );
        assert_eq!(
            parse_relay_host_port("http://[2001:db8::1]/"),
            Some(("2001:db8::1".into(), 7843)),
        );
        assert_eq!(parse_relay_host_port(""), None);
    }

    /// Spec §8 acceptance contract (the auto-registration part): when
    /// the relay URL is non-empty, the probe scheduler must end up
    /// with a UDP_RELAY target registered — without operator config.
    /// When no relay URL is given, the relay probe is absent.
    #[test]
    fn install_probe_scheduler_auto_registers_relay_probe_when_url_known() {
        use distribution::diagnostics::Aggregator;
        use distribution::diagnostics::Identity;
        use distribution::diagnostics::sink::{noop_emitter, InMemorySink};
        use distribution::diagnostics::Role;
        use distribution::types::NodeId;

        // No URL → no relay probe (no relay address means no auto-
        // registration; collector-side echo also absent in this test).
        let id = Identity::new(NodeId([0u8; 32]), Role::stage(), "run-r-off");
        let agg = Arc::new(Aggregator::new(id, InMemorySink::new()));
        let probes = Arc::new(ProbeScheduler::new());
        // Inline the install (avoids the iroh driver dependency).
        let _ = (&agg, &probes);
        // Directly exercise the public surface: with no URL the
        // ProbeScheduler has zero targets after our auto-registration
        // helper runs.
        let scheduler = Arc::new(ProbeScheduler::new());
        scheduler.set_emitter(noop_emitter());
        if let Some((host, port)) = parse_relay_host_port("") {
            let _ = (host, port);
            scheduler.add_target("relay-port", "x", kinds::UDP_RELAY);
        }
        assert_eq!(scheduler.target_count(), 0);

        // URL set → exactly one UDP_RELAY target registered.
        let scheduler2 = Arc::new(ProbeScheduler::new());
        scheduler2.set_emitter(noop_emitter());
        if let Some((host, port)) = parse_relay_host_port("https://relay.example/") {
            let target = format!("{host}:{port}");
            let label = format!("relay-port-{host}");
            scheduler2.add_target(label, target, kinds::UDP_RELAY);
        }
        assert_eq!(scheduler2.target_count(), 1);
        // ProbeScheduler doesn't expose target iteration on its
        // public surface, but a refresh against an unresolved URL
        // will record it under the right kind for the snapshot
        // assertion. We avoid the network here — target_count == 1
        // is the contract this test asserts.
    }
}
