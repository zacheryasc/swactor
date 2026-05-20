//! `swactor-diag-collector` — standalone diagnostics collector.
//!
//! Deploys to a small VPS reachable from both the orchestrator (on the
//! user's laptop, NAT'd) and every stage (on vast.ai). Accepts the four
//! `POST /diag/{boot,events,snapshot,finalize}` endpoints and serves
//! finalized bundles via `GET /diag/bundle/{run_id}`. See the
//! [`distribution::diagnostics::collector`] module for the protocol.
//!
//! ## Usage
//!
//! ```text
//! swactor-diag-collector --bind 0.0.0.0:9080 --root /var/lib/swactor-diag
//! ```
//!
//! Both flags also read from env vars (`SWACTOR_DIAG_BIND`,
//! `SWACTOR_DIAG_ROOT`) so the binary slots cleanly into systemd /
//! docker without a config file.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use distribution::diagnostics::collector::{CollectorState, bind, serve, spawn_udp_echo};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let config = match Config::parse(&args) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("swactor-diag-collector: {e}");
            eprintln!();
            eprintln!("{USAGE}");
            return ExitCode::from(2);
        }
    };

    if let Err(e) = std::fs::create_dir_all(&config.root) {
        eprintln!(
            "swactor-diag-collector: could not create root dir {}: {}",
            config.root.display(),
            e
        );
        return ExitCode::FAILURE;
    }

    let state = Arc::new(CollectorState::new(&config.root));
    let listener = match bind(config.bind).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!(
                "swactor-diag-collector: could not bind {}: {}",
                config.bind, e
            );
            return ExitCode::FAILURE;
        }
    };
    let local = listener
        .local_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| config.bind.to_string());
    eprintln!(
        "swactor-diag-collector: listening on http://{local} (root={})",
        config.root.display()
    );

    let _udp = match config.udp_bind {
        Some(addr) => match spawn_udp_echo(addr).await {
            Ok(handle) => {
                eprintln!(
                    "swactor-diag-collector: udp echo on udp://{}",
                    handle.local_addr
                );
                Some(handle)
            }
            Err(e) => {
                eprintln!(
                    "swactor-diag-collector: could not bind udp echo {addr}: {e} (continuing without)",
                );
                None
            }
        },
        None => None,
    };

    tokio::select! {
        result = serve(listener, state) => {
            if let Err(e) = result {
                eprintln!("swactor-diag-collector: server error: {e}");
                return ExitCode::FAILURE;
            }
        }
        _ = shutdown_signal() => {
            eprintln!("swactor-diag-collector: shutting down");
        }
    }
    ExitCode::SUCCESS
}

const USAGE: &str = "Usage:
  swactor-diag-collector [--bind ADDR] [--root DIR] [--udp ADDR]

Options:
  --bind ADDR  HTTP bind address (default 0.0.0.0:9080)
               or via SWACTOR_DIAG_BIND
  --root DIR   Storage root for records and bundles
               (default ./diag-data) or via SWACTOR_DIAG_ROOT
  --udp ADDR   Optional UDP echo bind address (e.g. 0.0.0.0:9081)
               for tier-3 reachability probes; or via SWACTOR_DIAG_UDP
  -h, --help   Show this help
";

struct Config {
    bind: SocketAddr,
    root: PathBuf,
    udp_bind: Option<SocketAddr>,
}

impl Config {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut bind: Option<String> = std::env::var("SWACTOR_DIAG_BIND").ok();
        let mut root: Option<String> = std::env::var("SWACTOR_DIAG_ROOT").ok();
        let mut udp: Option<String> = std::env::var("SWACTOR_DIAG_UDP").ok();
        let mut iter = args.iter().skip(1);
        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--bind" => {
                    bind = Some(
                        iter.next()
                            .cloned()
                            .ok_or_else(|| "--bind expects an address".to_string())?,
                    );
                }
                "--root" => {
                    root = Some(
                        iter.next()
                            .cloned()
                            .ok_or_else(|| "--root expects a path".to_string())?,
                    );
                }
                "--udp" => {
                    udp = Some(
                        iter.next()
                            .cloned()
                            .ok_or_else(|| "--udp expects an address".to_string())?,
                    );
                }
                "-h" | "--help" => {
                    println!("{USAGE}");
                    std::process::exit(0);
                }
                other => return Err(format!("unrecognized argument: {other}")),
            }
        }
        let bind = bind.unwrap_or_else(|| "0.0.0.0:9080".to_string());
        let bind: SocketAddr = bind
            .parse()
            .map_err(|e| format!("invalid bind address {bind:?}: {e}"))?;
        let root = PathBuf::from(root.unwrap_or_else(|| "./diag-data".to_string()));
        let udp_bind = match udp {
            None => None,
            Some(addr_str) => Some(
                addr_str
                    .parse()
                    .map_err(|e| format!("invalid udp address {addr_str:?}: {e}"))?,
            ),
        };
        Ok(Self {
            bind,
            root,
            udp_bind,
        })
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        use tokio::signal::unix::{SignalKind, signal};
        if let Ok(mut s) = signal(SignalKind::terminate()) {
            s.recv().await;
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
