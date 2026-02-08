use gossip_dashboard::{load_trace, serve_replay};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let path = args
        .get(1)
        .expect("Usage: replay <trace.json>");

    let trace = load_trace(path).expect("failed to load trace");

    eprintln!(
        "Loaded trace '{}': {} nodes, {} events",
        trace.name,
        trace.node_names.len(),
        trace.events.len()
    );

    serve_replay(&trace, 8081);
}
