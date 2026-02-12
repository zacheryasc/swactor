use simulation_dashboard::serve_dashboard;

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let trace_dir = args
        .get(1)
        .expect("Usage: replay <trace-dir> [port]");

    let port: u16 = args
        .get(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);

    serve_dashboard(trace_dir, port);
}
