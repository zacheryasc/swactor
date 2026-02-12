pub mod collector;
pub mod investigate;
pub mod layer;
pub mod trace;
mod actors_html;
mod dashboard_html;
mod server;

#[cfg(feature = "tui")]
pub mod tui;

#[cfg(feature = "distribution")]
mod distribution_html;
#[cfg(feature = "distribution")]
pub mod distribution_collector;

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crossbeam_queue::ArrayQueue;
use swactor::config::RuntimeConfig;
use swactor::runtime::{Runtime, RuntimeHandle};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::collector::StatsCollector;
use crate::layer::{now_ms, DashboardLayer, EventStore};
use crate::trace::{RuntimeTrace, TimestampedStats};

/// Configuration for the runtime dashboard.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
    pub event_capacity: usize,
    /// Enable trace recording for `save_trace()`. When true, events and stats
    /// are kept in lock-free bounded ring buffers and stats are periodically sampled.
    pub record: bool,
    /// Maximum events retained in the recording log. Only used when `record = true`.
    pub record_event_capacity: usize,
    /// Maximum stats snapshots retained in the timeline. Only used when `record = true`.
    pub record_stats_capacity: usize,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            event_capacity: 10_000,
            record: false,
            record_event_capacity: 100_000,
            record_stats_capacity: 18_000,
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
    collector: Arc<Mutex<Option<Arc<StatsCollector>>>>,
    shutdown: Arc<AtomicBool>,
    stats_timeline: Arc<ArrayQueue<TimestampedStats>>,
    recording: bool,
    #[cfg(feature = "distribution")]
    distribution: Arc<Mutex<Option<Arc<dyn distribution_collector::DistributionStatsProvider>>>>,
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

    /// Attach a runtime and its stats collector, enabling stats polling.
    pub fn set_runtime(&self, runtime: Arc<Runtime>, collector: Arc<StatsCollector>) {
        *self.runtime.lock().unwrap() = Some(runtime);
        *self.collector.lock().unwrap() = Some(collector);
    }

    /// Whether trace recording is enabled.
    pub fn is_recording(&self) -> bool {
        self.recording
    }

    /// Attach a distribution stats provider, enabling the `/distribution` page.
    #[cfg(feature = "distribution")]
    pub fn set_distribution(&self, provider: Arc<dyn distribution_collector::DistributionStatsProvider>) {
        *self.distribution.lock().unwrap() = Some(provider);
    }

    /// Signal the dashboard to shut down (SSE clients receive "done").
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// Save the recorded trace to a JSON file.
    ///
    /// Only works when `DashboardConfig::record` was set to `true`.
    /// This drains the recording buffers — each call consumes the buffered data.
    pub fn save_trace(&self, path: &str) -> io::Result<()> {
        let events = self.store.all_events().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::Other,
                "recording not enabled (set DashboardConfig::record = true)",
            )
        })?;
        let mut stats_timeline = Vec::new();
        while let Some(ts) = self.stats_timeline.pop() {
            stats_timeline.push(ts);
        }
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
    let store = Arc::new(EventStore::new(
        config.event_capacity,
        config.record,
        config.record_event_capacity,
    ));
    let runtime: Arc<Mutex<Option<Arc<Runtime>>>> = Arc::new(Mutex::new(None));
    let collector: Arc<Mutex<Option<Arc<StatsCollector>>>> = Arc::new(Mutex::new(None));
    let shutdown = Arc::new(AtomicBool::new(false));
    let stats_timeline = Arc::new(ArrayQueue::new(config.record_stats_capacity.max(1)));

    #[cfg(feature = "distribution")]
    let distribution: Arc<Mutex<Option<Arc<dyn distribution_collector::DistributionStatsProvider>>>> =
        Arc::new(Mutex::new(None));

    server::spawn_http_server(
        Arc::clone(&store),
        Arc::clone(&runtime),
        Arc::clone(&collector),
        Arc::clone(&shutdown),
        config.port,
        #[cfg(feature = "distribution")]
        Arc::clone(&distribution),
    );

    // Start stats recorder thread when recording is enabled
    if config.record {
        let rt_ref = Arc::clone(&runtime);
        let col_ref = Arc::clone(&collector);
        let timeline = Arc::clone(&stats_timeline);
        let stop = Arc::clone(&shutdown);
        thread::spawn(move || {
            loop {
                if stop.load(Ordering::Relaxed) {
                    return;
                }
                let maybe_rt = rt_ref.lock().unwrap().clone();
                if let Some(rt) = maybe_rt {
                    let mut stats = rt.stats();
                    if let Some(col) = col_ref.lock().unwrap().as_ref() {
                        col.enrich(&mut stats);
                    }
                    let ts = TimestampedStats {
                        timestamp_ms: now_ms(),
                        stats,
                    };
                    let _ = timeline.force_push(ts);
                }
                thread::sleep(Duration::from_millis(200));
            }
        });
    }

    eprintln!("Runtime dashboard at http://localhost:{}", config.port);

    DashboardHandle {
        store,
        runtime,
        collector,
        shutdown,
        stats_timeline,
        recording: config.record,
        #[cfg(feature = "distribution")]
        distribution,
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

    let num_workers = if rt_config.num_threads < 2 { 1 } else { rt_config.num_threads };
    let collector = StatsCollector::new(num_workers);

    let mut rt = Runtime::new(rt_config);
    rt.set_stats_hook(collector.clone());
    let handle = rt.run().expect("failed to start runtime");
    dash.set_runtime(Arc::clone(&handle.runtime), collector);

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
