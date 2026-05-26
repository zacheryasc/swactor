//! `swactor-iroh-relay` — standalone iroh-relay server.
//!
//! Wraps `iroh_relay::server::Server` so the same canary-avoiding HTTP relay
//! we previously embedded inside the iroh driver can run on a small VPS
//! reachable from both the orchestrator and rented stages. Clients point at
//! it by setting `SWACTOR_IROH_RELAY_URL=http://<host>:<port>/` and we then
//! select `RelayMode::Custom(url)` instead of the canary default.
//!
//! Defaults to plain HTTP on `0.0.0.0:7843`. No TLS — meant for diagnostic
//! / experimental deployments behind a firewall the operator controls.
//!
//! ## Observability (spec §1, gap 1)
//!
//! When `SWACTOR_DIAG_COLLECTOR_URL` is set this binary boots its own
//! diagnostics aggregator with `Role::custom("relay")` and installs a
//! [`distribution::diagnostics::RelayObservability`] helper on it. The
//! aggregator reports into the same collector / bundle as the cluster's
//! nodes, so the post-processor's `## Relay sessions` section can
//! correlate relay-reported close reasons against node-side
//! `connection_cache[peer].last_failure_reason`. Per-session lifecycle
//! events are emitted via [`RelayObservability::note_session_opened`]
//! / `note_session_closed` — wired today as a skeleton (iroh-relay's
//! native server does not expose session hooks); when the upstream
//! relay grows them, the call sites slot in here and the bundle
//! starts answering "who closed and why" automatically.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

use distribution::diagnostics::aggregator::{spawn_periodic_snapshots, PeriodicConfig};
use distribution::diagnostics::{
    wall_ms_now, Aggregator, HttpSink, Identity, RelayObservability, RelayServerIntrospector,
    Role, SinkConfig, SnapshotSignal, GIT_SHA, IROH_VERSION,
};
use distribution::diagnostics::sink::{DynEmitter, EventEmitter};
use distribution::types::NodeId;

const DEFAULT_BIND: &str = "0.0.0.0:7843";
const ENV_COLLECTOR_URL: &str = "SWACTOR_DIAG_COLLECTOR_URL";
const ENV_RUN_ID: &str = "SWACTOR_DIAG_RUN_ID";
const ENV_SPOOL_DIR: &str = "SWACTOR_DIAG_SPOOL_DIR";
const ENV_RELAY_LABEL: &str = "SWACTOR_DIAG_RELAY_LABEL";
const DEFAULT_RUN_ID: &str = "pp-run";
const DEFAULT_SPOOL_DIR: &str = "/tmp/swactor-diag-relay";

fn print_help() {
    eprintln!(
        "Usage:\n  \
         swactor-iroh-relay [--bind ADDR] [--public-host HOST]\n\n\
         Options:\n  \
         --bind ADDR          HTTP bind address (default {DEFAULT_BIND})\n  \
                              or via SWACTOR_IROH_RELAY_BIND\n  \
         --public-host HOST   Host clients should use in the relay URL.\n  \
                              Defaults to the bind IP — set this to the\n  \
                              VPS's public IP when --bind uses 0.0.0.0.\n  \
                              or via SWACTOR_IROH_RELAY_PUBLIC_HOST\n  \
         -h, --help           Show this help"
    );
}

fn parse_args() -> Result<(SocketAddr, Option<String>), String> {
    let mut bind: Option<String> = std::env::var("SWACTOR_IROH_RELAY_BIND").ok();
    let mut public_host: Option<String> =
        std::env::var("SWACTOR_IROH_RELAY_PUBLIC_HOST").ok();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--bind" => {
                bind = Some(args.next().ok_or("--bind needs ADDR")?);
            }
            "--public-host" => {
                public_host = Some(args.next().ok_or("--public-host needs HOST")?);
            }
            "-h" | "--help" => {
                print_help();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument: {other}")),
        }
    }
    let bind_str = bind.unwrap_or_else(|| DEFAULT_BIND.to_string());
    let bind: SocketAddr = bind_str
        .parse()
        .map_err(|e| format!("invalid bind addr {bind_str:?}: {e}"))?;
    Ok((bind, public_host))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let (bind, public_host) = match parse_args() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("swactor-iroh-relay: {e}");
            print_help();
            return ExitCode::from(2);
        }
    };

    // QUIC Address Discovery (QAD): lets clients learn their own public
    // address so iroh can hole-punch direct paths instead of pinning every
    // connection to this relay. QAD runs over QUIC, which mandates TLS; the
    // cert is self-signed because this is an operator-controlled diagnostic
    // relay behind a firewall, and clients are configured to trust a custom
    // relay's cert (see `iroh_driver`'s `ca_roots_config` for
    // `RelayMode::Custom`). With `quic: None` the relay can only forward bytes
    // and the cluster never escapes relay-only operation — which is what
    // produced the all-`conn_type=Relay`, no-direct-path runs.
    let quic = {
        let (_certs, server_config) =
            iroh_relay::server::testing::self_signed_tls_certs_and_config();
        let quic_bind =
            SocketAddr::new(bind.ip(), iroh_relay::defaults::DEFAULT_RELAY_QUIC_PORT);
        Some(iroh_relay::server::QuicConfig {
            bind_addr: quic_bind,
            server_config,
        })
    };

    let server = match iroh_relay::server::Server::spawn(
        iroh_relay::server::ServerConfig::<(), ()> {
            relay: Some(iroh_relay::server::RelayConfig {
                http_bind_addr: bind,
                tls: None,
                limits: Default::default(),
                key_cache_capacity: Some(1024),
                access: iroh_relay::server::AccessConfig::Everyone,
            }),
            quic,
            metrics_addr: None,
        },
    )
    .await
    {
        Ok(s) => s,
        Err(e) => {
            eprintln!("swactor-iroh-relay: failed to spawn relay server: {e}");
            return ExitCode::from(1);
        }
    };

    let addr = match server.http_addr() {
        Some(a) => a,
        None => {
            eprintln!("swactor-iroh-relay: relay has no HTTP address");
            return ExitCode::from(1);
        }
    };

    let url_host = public_host.unwrap_or_else(|| addr.ip().to_string());
    let url = format!("http://{}:{}/", url_host, addr.port());
    eprintln!(
        "swactor-iroh-relay: listening on {bind} (advertised URL: {url}); \
         QAD/QUIC on udp/{} (self-signed; open this port in the firewall)",
        iroh_relay::defaults::DEFAULT_RELAY_QUIC_PORT,
    );

    // Spec §1: when a collector is configured, this relay reports
    // into the same bundle as the cluster nodes under its own
    // identity. Holding `_diag` keeps the aggregator + spawned tasks
    // alive for the lifetime of the binary; dropping it at shutdown
    // flushes the sink.
    let _diag = install_relay_diagnostics(&url);

    if let Err(e) = tokio::signal::ctrl_c().await {
        eprintln!("swactor-iroh-relay: signal listen failed: {e}");
        return ExitCode::from(1);
    }
    eprintln!("swactor-iroh-relay: shutdown signal received");
    ExitCode::SUCCESS
}

/// Holder for the relay's diagnostics state. `RelayObservability` is
/// exposed so a future call site that hooks iroh-relay's session
/// lifecycle can record opens/closes through it.
struct RelayDiag {
    _agg: Arc<Aggregator<HttpSink>>,
    _observability: Arc<RelayObservability>,
}

fn install_relay_diagnostics(advertised_url: &str) -> Option<RelayDiag> {
    let collector_url = std::env::var(ENV_COLLECTOR_URL).ok()?;
    let collector_url = collector_url.trim().to_string();
    if collector_url.is_empty() {
        return None;
    }
    let run_id = env_string(ENV_RUN_ID).unwrap_or_else(|| DEFAULT_RUN_ID.to_string());
    let spool_dir = std::path::PathBuf::from(
        env_string(ENV_SPOOL_DIR).unwrap_or_else(|| DEFAULT_SPOOL_DIR.to_string()),
    );

    // The relay has no `iroh::Endpoint` and therefore no `NodeId`. We
    // synthesize a deterministic-per-process id from the advertised
    // URL so the bundle's manifest keeps a stable handle on this
    // relay across reboots within a run.
    let node_id = synthesize_node_id(advertised_url);
    let node_id_hex: String = node_id.0.iter().map(|b| format!("{:02x}", b)).collect();

    let mut identity = Identity::new(node_id, Role::custom("relay"), run_id.clone())
        .with_process_start(wall_ms_now());
    identity = identity.with_host_context(
        distribution::diagnostics::HostContext::from_env()
            .with_iroh_version(IROH_VERSION)
            .with_git_sha(GIT_SHA.map(|s| s.to_string()))
            .with_binary_version(option_env!("CARGO_PKG_VERSION").map(|s| s.to_string()))
            .with_home_relay_url(Some(advertised_url.to_string())),
    );
    if let Some(label) = env_string(ENV_RELAY_LABEL) {
        // Caller can override the friendly hostname carried in the
        // host context so the bundle reader recognises the relay by
        // its operational name rather than just its synthetic node id.
        identity.hostname = Some(label);
    }

    let signal = SnapshotSignal::new();
    let sink_config = SinkConfig::new(
        collector_url.clone(),
        run_id.clone(),
        node_id_hex,
        spool_dir,
    )
    .with_snapshot_signal(signal.clone());
    let sink = match HttpSink::new(sink_config) {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "swactor-iroh-relay: HttpSink::new failed ({e}); continuing without diagnostics"
            );
            return None;
        }
    };
    let aggregator = Arc::new(Aggregator::new(identity, sink));

    let observability = Arc::new(RelayObservability::new());
    let emitter: DynEmitter = aggregator.clone() as Arc<dyn EventEmitter + Send + Sync + 'static>;
    observability.set_emitter(emitter);
    aggregator.set_relay_server_introspector(
        observability.clone() as Arc<dyn RelayServerIntrospector>,
    );

    // Periodic snapshots: same cadence as nodes so the bundle reader
    // can line snapshots up by wall_ms.
    let _ = spawn_periodic_snapshots(aggregator.clone(), PeriodicConfig::default(), signal);

    eprintln!(
        "swactor-iroh-relay: diagnostics installed (collector={collector_url} run_id={run_id} \
         role=relay url={advertised_url})"
    );
    Some(RelayDiag {
        _agg: aggregator,
        _observability: observability,
    })
}

fn env_string(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
}

/// FNV-1a 64-bit folded across the URL bytes, repeated to fill 32
/// bytes. Deterministic per-URL so the relay's identity is stable
/// across restarts within a run, without taking on a key dependency.
fn synthesize_node_id(seed: &str) -> NodeId {
    const FNV_OFFSET: u64 = 0xcbf29ce484222325;
    const FNV_PRIME: u64 = 0x100000001b3;
    let mut hash: u64 = FNV_OFFSET;
    for b in seed.bytes() {
        hash ^= b as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    let mut out = [0u8; 32];
    for (i, chunk) in out.chunks_mut(8).enumerate() {
        let seeded = hash.wrapping_add(i as u64);
        chunk.copy_from_slice(&seeded.to_be_bytes());
    }
    NodeId(out)
}
