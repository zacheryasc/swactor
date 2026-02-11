use runtime_dashboard::tui::types::RuntimeEndpoint;
use runtime_dashboard::tui::{TuiConfig, start_tui_remote};

fn main() -> std::io::Result<()> {
    let url = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://localhost:9090".into());

    let endpoint = RuntimeEndpoint::from_url(&url);
    eprintln!("Connecting to {} ...", endpoint);

    start_tui_remote(endpoint, TuiConfig::default())
}
