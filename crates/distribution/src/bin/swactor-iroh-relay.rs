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

use std::net::SocketAddr;
use std::process::ExitCode;

const DEFAULT_BIND: &str = "0.0.0.0:7843";

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

    let server = match iroh_relay::server::Server::spawn(
        iroh_relay::server::ServerConfig::<(), ()> {
            relay: Some(iroh_relay::server::RelayConfig {
                http_bind_addr: bind,
                tls: None,
                limits: Default::default(),
                key_cache_capacity: Some(1024),
                access: iroh_relay::server::AccessConfig::Everyone,
            }),
            quic: None,
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
    eprintln!("swactor-iroh-relay: listening on {bind} (advertised URL: {url})");

    if let Err(e) = tokio::signal::ctrl_c().await {
        eprintln!("swactor-iroh-relay: signal listen failed: {e}");
        return ExitCode::from(1);
    }
    eprintln!("swactor-iroh-relay: shutdown signal received");
    ExitCode::SUCCESS
}
