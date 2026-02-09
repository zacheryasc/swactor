pub mod config;
mod dashboard_html;
mod server;

pub use server::serve_dashboard;

use std::fs;
use std::io;

use swactor_gossip::trace::SimulationTrace;

pub fn save_trace(trace: &SimulationTrace, path: &str) -> io::Result<()> {
    let json = serde_json::to_string_pretty(trace)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    fs::write(path, json)
}

pub fn load_trace(path: &str) -> io::Result<SimulationTrace> {
    let data = fs::read_to_string(path)?;
    let trace: SimulationTrace =
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    Ok(trace)
}
