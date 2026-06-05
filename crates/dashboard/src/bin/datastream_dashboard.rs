//! Datastream-backed dashboard: bind the demo cluster's UDP telemetry sink
//! (the same wire `swactor-datastream-collector` reads), demultiplex the
//! per-node frames, and serve the live HTTP/browser dashboard from them.
//!
//! Drop-in for the `collector` service in the datastream demo: same UDP bind,
//! but instead of printing frames it renders them in the existing dashboard UI.
//!
//! Usage: `swactor-datastream-dashboard [--bind HOST:PORT] [--port HTTP_PORT] [--node FILTER]`
//!   --bind   UDP sink to listen on (default `0.0.0.0:7700`)
//!   --port   HTTP dashboard port (default `9090`)
//!   --node   show the node whose id/region/role matches FILTER (default: first seen)

use dashboard::datastream_source::run_datastream_ingest;
use dashboard::{start_dashboard, DashboardConfig};
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

fn main() {
    let args = Args::parse();

    let dashboard = start_dashboard(DashboardConfig {
        port: args.port,
        ..Default::default()
    });
    // Route process-output / membership tracing into the activity log panel.
    // Cap at INFO so the panel shows our events, not tokio/mio TRACE internals.
    tracing_subscriber::registry()
        .with(dashboard.layer())
        .with(LevelFilter::INFO)
        .init();
    dashboard.start_http_standalone();

    eprintln!(
        "datastream dashboard: serving on container port {} (UDP sink {}) — \
         browse via the published host port:\n  \
         /                     Overview / Actors (single node)\n  \
         /plugin/distribution  SWIM connection graph (all nodes)\n  \
         /plugin/vastai        Fleet table (all nodes)",
        args.port, args.bind
    );

    // Blocks forever, demuxing frames into the dashboard's pushed stats.
    if let Err(e) = run_datastream_ingest(&args.bind, args.node.as_deref(), &dashboard) {
        eprintln!("datastream dashboard: fatal: failed to bind {}: {e}", args.bind);
        std::process::exit(1);
    }
}

struct Args {
    bind: String,
    port: u16,
    node: Option<String>,
}

impl Args {
    fn parse() -> Self {
        let mut bind = std::env::var("SWACTOR_DATASTREAM_BIND").ok();
        let mut port: u16 = 9090;
        let mut node: Option<String> = None;

        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--bind" => bind = it.next(),
                "--port" => port = it.next().and_then(|v| v.parse().ok()).unwrap_or(port),
                "--node" => node = it.next(),
                other => {
                    if let Some(v) = other.strip_prefix("--bind=") {
                        bind = Some(v.to_string());
                    } else if let Some(v) = other.strip_prefix("--port=") {
                        port = v.parse().unwrap_or(port);
                    } else if let Some(v) = other.strip_prefix("--node=") {
                        node = Some(v.to_string());
                    }
                }
            }
        }

        Self {
            bind: bind.unwrap_or_else(|| "0.0.0.0:7700".to_string()),
            port,
            node,
        }
    }
}
