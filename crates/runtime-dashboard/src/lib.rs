pub mod layer;
pub mod trace;
mod dashboard_html;
mod server;

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeHandle};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::layer::{now_ms, DashboardLayer, EventStore};
use crate::trace::{RuntimeTrace, TimestampedStats};

/// Configuration for the runtime dashboard.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
    pub event_capacity: usize,
    /// Enable trace recording for `save_trace()`. When true, all events
    /// are kept in an unbounded log and stats are periodically sampled.
    pub record: bool,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            event_capacity: 10_000,
            record: false,
        }
    }
}

/// Configuration for replaying a recorded trace.
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub port: u16,
    /// Playback speed multiplier (1.0 = real-time, 2.0 = double speed).
    pub speed: f64,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            speed: 1.0,
        }
    }
}

/// Handle to a running dashboard. Allows attaching a runtime after creation.
pub struct DashboardHandle {
    store: Arc<EventStore>,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    shutdown: Arc<AtomicBool>,
    stats_timeline: Arc<Mutex<Vec<TimestampedStats>>>,
    recording: bool,
}

impl DashboardHandle {
    /// Install a global tracing subscriber with the dashboard layer.
    pub fn install_tracing(&self) {
        tracing_subscriber::registry()
            .with(DashboardLayer::new(Arc::clone(&self.store)))
            .init();
    }

    /// Return the raw `DashboardLayer` for users who want to compose their own subscriber.
    pub fn layer(&self) -> DashboardLayer {
        DashboardLayer::new(Arc::clone(&self.store))
    }

    /// Attach a runtime to the dashboard, enabling stats polling.
    pub fn set_runtime(&self, runtime: Arc<Runtime>) {
        *self.runtime.lock().unwrap() = Some(runtime);
    }

    /// Whether trace recording is enabled.
    pub fn is_recording(&self) -> bool {
        self.recording
    }

    /// Signal the dashboard to shut down (SSE clients receive "done").
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Save the recorded trace to a JSON file.
    ///
    /// Only works when `DashboardConfig::record` was set to `true`.
    pub fn save_trace(&self, path: &str) -> io::Result<()> {
        let events = self.store.all_events().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "recording not enabled (set DashboardConfig::record = true)",
            )
        })?;
        let stats_timeline = self.stats_timeline.lock().unwrap().clone();
        let trace = RuntimeTrace {
            events,
            stats_timeline,
        };
        let json = serde_json::to_string(&trace)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        std::fs::write(path, json)
    }
}

/// Start a dashboard server and return a handle.
///
/// The dashboard starts serving immediately. Call `install_tracing()` to set up
/// the global subscriber, and `set_runtime()` to enable stats polling.
pub fn start_dashboard(config: DashboardConfig) -> DashboardHandle {
    let store = Arc::new(EventStore::new(config.event_capacity, config.record));
    let runtime: Arc<Mutex<Option<Arc<Runtime>>>> = Arc::new(Mutex::new(None));
    let shutdown = Arc::new(AtomicBool::new(false));
    let stats_timeline = Arc::new(Mutex::new(Vec::new()));

    server::spawn_http_server(
        Arc::clone(&store),
        Arc::clone(&runtime),
        Arc::clone(&shutdown),
        config.port,
    );

    // Start stats recorder thread when recording is enabled
    if config.record {
        let rt_ref = Arc::clone(&runtime);
        let timeline = Arc::clone(&stats_timeline);
        let stop = Arc::clone(&shutdown);
        thread::spawn(move || {
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let maybe_rt = rt_ref.lock().unwrap().clone();
                if let Some(rt) = maybe_rt {
                    let stats = rt.stats();
                    let ts = TimestampedStats {
                        timestamp_ms: now_ms(),
                        stats,
                    };
                    timeline.lock().unwrap().push(ts);
                }
                thread::sleep(Duration::from_millis(200));
            }
        });
    }

    eprintln!("Runtime dashboard at http://localhost:{}", config.port);

    DashboardHandle {
        store,
        runtime,
        shutdown,
        stats_timeline,
        recording: config.record,
    }
}

/// Convenience: create a runtime, start a dashboard, install tracing, and run.
///
/// Returns the runtime handle and dashboard handle.
pub fn run_with_dashboard(
    rt_config: RuntimeConfig,
    dash_config: DashboardConfig,
) -> (RuntimeHandle, DashboardHandle) {
    let dash = start_dashboard(dash_config);
    dash.install_tracing();

    let rt = Runtime::new(rt_config);
    let handle = rt.run().expect("failed to start runtime");
    dash.set_runtime(Arc::clone(&handle.runtime));

    (handle, dash)
}

/// Load a trace file and serve a replay dashboard. Blocks indefinitely.
pub fn serve_replay(path: &str, config: ReplayConfig) -> io::Result<()> {
    let data = std::fs::read_to_string(path)?;
    let trace: RuntimeTrace = serde_json::from_str(&data)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    server::spawn_replay_server(Arc::new(trace), config.port, config.speed);

    eprintln!("Replay dashboard at http://localhost:{}", config.port);
    eprintln!("Press Ctrl+C to stop");

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
