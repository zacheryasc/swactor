//! `swactor-iroh-relay` — standalone iroh-relay server.
//!
//! Wraps `iroh_relay::server::Server` so the same canary-avoiding HTTP relay
//! we previously embedded inside the iroh driver can run on a small VPS
//! reachable from both the orchestrator and rented stages. Clients point at
//! it by setting `SWACTOR_IROH_RELAY_URL=http://<host>:<port>/` and we then
//! select `RelayMode::Custom(url)` instead of the canary default.
//!
//! Defaults to plain HTTP on `0.0.0.0:7843`. No TLS — meant for
//! experimental deployments behind a firewall the operator controls.
//!
//! ## Telemetry
//!
//! The relay is not a swactor cluster member (it has no runtime/SWIM), so it
//! cannot ship telemetry over the cluster transport the way nodes do — and the
//! dedicated UDP datastream channel has been removed. It therefore runs the
//! shared [`DatastreamEmitter`] draining into a [`NoopSink`] (keeping the mux
//! bounded) and does not appear in the fleet view. If the relay ever needs to
//! be observable, it should join the cluster and use a `ClusterFrameSink`.

use std::net::SocketAddr;
use std::process::ExitCode;
use std::time::Duration;

use datastream::catalog::RuntimeStats;
use datastream::emit::{
    DatastreamEmitter, EmitterConfig, NoopSink, TickInput,
};
use distribution::types::NodeId;

const DEFAULT_BIND: &str = "0.0.0.0:7843";
const ENV_LIFETIME: &str = "SWACTOR_LIFETIME";

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
    // cert is self-signed because this is an operator-controlled relay
    // behind a firewall, and clients are configured to trust a custom
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

    // Datastream telemetry: the same per-node emitter every node runs, draining
    // into a no-op (the relay is not a cluster member; see the module docs).
    let mut emitter = build_emitter(&url);

    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                if let Err(e) = res {
                    eprintln!("swactor-iroh-relay: signal listen failed: {e}");
                    return ExitCode::from(1);
                }
                break;
            }
            _ = ticker.tick() => {
                emitter.tick(
                    TickInput {
                        // The relay is not a SWIM member and iroh-relay's
                        // native server exposes no session hooks yet, so
                        // membership and peer counts are honestly empty.
                        members: &[],
                        runtime: RuntimeStats::default(),
                        relay_connected: true,
                        relay_peers: 0,
                    },
                    true,
                );
            }
        }
    }
    eprintln!("swactor-iroh-relay: shutdown signal received");
    ExitCode::SUCCESS
}

/// Build the relay's datastream emitter. Identity is synthesized from the
/// advertised URL (stable across restarts); the lifetime discriminator comes
/// from `SWACTOR_LIFETIME` like the generic node. The sink is a [`NoopSink`]:
/// the relay is not a cluster member and the dedicated UDP channel is gone.
fn build_emitter(advertised_url: &str) -> DatastreamEmitter {
    let node_id = synthesize_node_id(advertised_url);
    let node_hex: String = node_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let life = env_string(ENV_LIFETIME)
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(0);

    DatastreamEmitter::new(
        EmitterConfig {
            node_hex,
            life,
            mux_capacity: 4096,
        },
        Box::new(NoopSink),
    )
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
