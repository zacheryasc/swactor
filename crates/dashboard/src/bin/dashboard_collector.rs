//! Standalone dashboard + diagnostics collector for a live deployment.
//!
//! This is the process you run on the VPS: it *is* the HTTP collector (the real
//! vast.ai shippers and the orchestrator's distribution broadcaster POST into it)
//! and it serves the unified live fleet board over SSE. One port serves both the
//! `/diag/*` ingest routes and the dashboard UI.
//!
//! ```text
//! dashboard_collector [--port 9090] [--root <dir>]
//! ```

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use dashboard::live_collector::{FLEET_HTML, LiveCollector};
use dashboard::{start_dashboard, DashboardConfig};

fn main() {
    let mut port: u16 = 9090;
    let mut root = std::env::temp_dir().join("dashboard-collector");

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--port" => {
                if let Some(v) = args.next() {
                    port = v.trim().parse().unwrap_or_else(|_| {
                        eprintln!("dashboard_collector: invalid --port '{v}'");
                        std::process::exit(2);
                    });
                }
            }
            "--root" => {
                if let Some(v) = args.next() {
                    root = v.into();
                }
            }
            "-h" | "--help" => {
                println!("Usage: dashboard_collector [--port <PORT>] [--root <DIR>]");
                return;
            }
            other => {
                eprintln!("dashboard_collector: unknown argument '{other}'");
                std::process::exit(2);
            }
        }
    }

    // A small multi-thread runtime hosts the HTTP server and the broadcast fan-out.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let dashboard = start_dashboard(DashboardConfig {
        port,
        ..Default::default()
    });

    let collector = LiveCollector::new(&root);
    collector.install(&dashboard, rt.handle());
    dashboard.set_landing_html(FLEET_HTML);
    dashboard.start_http(rt.handle().clone());

    eprintln!("dashboard_collector: listening on http://0.0.0.0:{port}");
    eprintln!("  UI:      http://localhost:{port}/  (Fleet | Distribution)");
    eprintln!("  ingest:  POST http://localhost:{port}/diag/{{kind}}");
    eprintln!("  records: {}", root.display());

    // Block until Ctrl-C, then shut the dashboard's SSE clients down cleanly.
    let stop = Arc::new(AtomicBool::new(false));
    let s = Arc::clone(&stop);
    let _ = ctrlc::set_handler(move || s.store(true, Ordering::Release));
    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    dashboard.shutdown();
    eprintln!("dashboard_collector: shutting down");
}
