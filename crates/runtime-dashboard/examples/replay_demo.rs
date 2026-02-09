use runtime_dashboard::{serve_replay, ReplayConfig};

fn main() {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: replay_demo <trace.json> [speed]");
        eprintln!("  speed: playback multiplier (default 1.0, e.g. 2.0 = 2x speed)");
        std::process::exit(1);
    }

    let path = &args[1];
    let speed = args
        .get(2)
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(1.0);

    eprintln!("Replaying {path} at {speed}x speed");

    if let Err(e) = serve_replay(path, ReplayConfig { port: 9090, speed }) {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}
