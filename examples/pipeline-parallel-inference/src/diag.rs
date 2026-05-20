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
use distribution::diagnostics::probes::kinds;
use distribution::diagnostics::{
    wall_ms_now, Aggregator, HostIntrospect, HostIntrospector, HttpSink, Identity,
    ProbeIntrospector, ProbeScheduler, ProcessIntrospector, ProcessStats, Role, SinkConfig,
    SinkHandle, SnapshotSignal, VastaiContext,
};
use distribution::diagnostics::sink::{DynEmitter, EventEmitter};
use distribution::iroh_driver::IrohDriver;

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
}

impl DiagHandles {
    /// Borrow the aggregator. Useful only for tests / callers that want
    /// to emit custom events from outside the wired subsystems.
    pub fn aggregator(&self) -> &Arc<Aggregator<HttpSink>> {
        &self.aggregator
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
    let url = std::env::var(ENV_COLLECTOR_URL).ok()?;
    let url = url.trim().to_string();
    if url.is_empty() {
        return None;
    }

    let run_id = env_string(ENV_RUN_ID).unwrap_or_else(|| DEFAULT_RUN_ID.to_string());
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
    if let Some(name) = env_string("HOSTNAME") {
        identity.hostname = Some(name);
    }
    identity.home_relay_url_at_boot = driver.home_relay_url().map(|u| u.to_string());
    identity.binary_version = option_env!("CARGO_PKG_VERSION").map(|s| s.to_string());

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
    install_probe_scheduler(&aggregator, emitter);
    install_vastai_context(&aggregator);
    install_process_stats(&aggregator);

    eprintln!(
        "pp-diag: installed collector={url} run_id={run_id} role={role:?} stage={stage_index:?}/{stage_count:?}",
        role = role.0,
    );

    Some(DiagHandles {
        aggregator,
        sink_handle,
        tokio_handle,
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

fn install_probe_scheduler(aggregator: &Arc<Aggregator<HttpSink>>, emitter: DynEmitter) {
    let probes = Arc::new(ProbeScheduler::new());
    probes.set_emitter(emitter);
    if let Some(echo) = env_string(ENV_UDP_ECHO) {
        let echo = echo.trim();
        if !echo.is_empty() {
            probes.add_target("collector_udp_echo", echo, kinds::UDP_ECHO);
        }
    }
    aggregator.set_probe_introspector(probes.clone() as Arc<dyn ProbeIntrospector>);
    let _ = probes.start(Duration::from_secs(10));
}

fn install_vastai_context(aggregator: &Arc<Aggregator<HttpSink>>) {
    let vastai = VastaiContext::capture_now();
    aggregator.set_vastai_introspector(vastai.into_arc());
}

fn install_process_stats(aggregator: &Arc<Aggregator<HttpSink>>) {
    let process = Arc::new(ProcessStats::new());
    aggregator.set_process_introspector(process as Arc<dyn ProcessIntrospector>);
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
}
