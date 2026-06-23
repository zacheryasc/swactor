pub mod collector;
pub mod command;
pub mod history;
mod html;
pub mod investigate;
pub mod layer;
pub mod plugin;
mod server;
pub mod telemetry;
pub mod topology;
pub mod warnings;

#[cfg(feature = "tui")]
pub mod tui;

pub mod datastream_source;

/// The canonical Distribution page (the SWIM connection-graph view). Owned by
/// the dashboard crate so every front-end that serves it — a live node's
/// `DistributionPlugin` and the datastream dashboard — renders the exact same
/// page and chrome, fed by the distribution-page JSON the datastream consumer
/// reconstructs (see [`crate::datastream_source`]).
pub const DISTRIBUTION_PAGE_HTML: &str = include_str!("distribution_page.html");

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

use serde::{Deserialize, Serialize};
use swactor::stats::RuntimeStats;

use crate::collector::StatsCollector;
use crate::history::{DashboardHistory, HistoryConfig};
use crate::layer::{DashboardEvent, DashboardLayer, EventStore, now_ms};
use crate::plugin::PluginRegistry;

// ─── Trace Types ────────────────────────────────────────────────────────────

/// Complete trace of a runtime execution, suitable for saving/loading.
#[derive(Debug, Serialize, Deserialize)]
pub struct RuntimeTrace {
    pub events: Vec<DashboardEvent>,
    pub stats_timeline: Vec<TimestampedStats>,
}

/// A stats snapshot with a wall-clock timestamp.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimestampedStats {
    pub timestamp_ms: u64,
    pub stats: RuntimeStats,
}

/// Peer info sent through the join channel.
pub struct JoinPeerInfo {
    pub node_id: [u8; 32],
    pub relay_url: Option<String>,
    pub direct_addrs: Vec<std::net::SocketAddr>,
}

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
    /// Externally pushed stats, used when no live `Runtime` is attached (e.g. a
    /// datastream-backed source synthesizes `RuntimeStats` and pushes them here).
    pushed_stats: Arc<Mutex<Option<RuntimeStats>>>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    stats_timeline: Arc<ArrayQueue<TimestampedStats>>,
    history: Arc<DashboardHistory>,
    recording: bool,
    port: u16,
    plugin_registry: Arc<PluginRegistry>,
    standalone_rt: Mutex<Option<tokio::runtime::Runtime>>,
    /// Optional override for the `/` landing page (e.g. a host serving a fleet
    /// board instead of the runtime-less actor dashboard).
    landing_html: Mutex<Option<Arc<str>>>,
    /// Optional extra axum router merged into the live server, for hosts that
    /// add disjoint routes of their own.
    extra_router: Mutex<Option<axum::Router>>,
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

    /// Push a stats snapshot from an external source (latest wins). When no live
    /// `Runtime` is attached, the SSE loop serves these to the dashboard exactly
    /// as if they came from a runtime. Used by the datastream source.
    pub fn set_stats(&self, stats: RuntimeStats) {
        *self.pushed_stats.lock().unwrap() = Some(stats);
    }

    /// Whether trace recording is enabled.
    pub fn is_recording(&self) -> bool {
        self.recording
    }

    /// Register a plugin with the dashboard.
    pub fn register_plugin(&self, plugin: Arc<dyn plugin::DashboardPlugin>) {
        self.plugin_registry.register(plugin);
    }

    /// Override the `/` landing page with custom HTML. Used when the dashboard
    /// runs without a swactor runtime (e.g. the live collector) so `/` shows the
    /// unified fleet board rather than the empty actor dashboard.
    pub fn set_landing_html(&self, html: impl Into<Arc<str>>) {
        *self.landing_html.lock().unwrap() = Some(html.into());
    }

    /// Merge an extra axum router into the live HTTP server. The routes must be
    /// disjoint from the dashboard's own (the collector's `/diag/*` are). Must be
    /// called before `start_http`/`start_http_standalone`.
    pub fn set_extra_router(&self, router: axum::Router) {
        *self.extra_router.lock().unwrap() = Some(router);
    }

    /// Access the time-series history store (for TUI sparklines, etc.).
    pub fn history(&self) -> &Arc<DashboardHistory> {
        &self.history
    }

    /// Signal the dashboard to shut down (SSE clients receive "done").
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.shutdown_notify.notify_waiters();
    }

    /// Start the HTTP server on the provided tokio handle.
    /// Use this when a tokio runtime already exists (e.g. IrohDriver's runtime).
    pub fn start_http(&self, handle: tokio::runtime::Handle) {
        let state = self.build_app_state();
        let extra = self.extra_router.lock().unwrap().take();
        let port = self.port;
        handle.spawn(async move {
            server::run_server(state, port, extra).await;
        });
    }

    /// Start the HTTP server on a standalone tokio runtime (1 worker thread).
    /// Use this when no external tokio runtime is available (e.g. non-async transport).
    pub fn start_http_standalone(&self) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for dashboard HTTP");
        let handle = rt.handle().clone();
        *self.standalone_rt.lock().unwrap() = Some(rt);
        self.start_http(handle);
    }

    fn build_app_state(&self) -> server::AppState {
        server::AppState {
            store: Arc::clone(&self.store),
            runtime: Arc::clone(&self.runtime),
            collector: Arc::clone(&self.collector),
            pushed_stats: Arc::clone(&self.pushed_stats),
            shutdown: Arc::clone(&self.shutdown),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
            history: Arc::clone(&self.history),
            cmd_router: Arc::new(crate::command::CommandRouter::with_builtins()),
            plugins: Arc::clone(&self.plugin_registry),
            landing: self.landing_html.lock().unwrap().clone(),
        }
    }

    /// Save the recorded trace to a JSON file.
    ///
    /// Only works when `DashboardConfig::record` was set to `true`.
    /// This drains the recording buffers — each call consumes the buffered data.
    pub fn save_trace(&self, path: &str) -> io::Result<()> {
        let events = self.store.all_events().ok_or_else(|| {
            io::Error::other("recording not enabled (set DashboardConfig::record = true)")
        })?;
        let mut stats_timeline = Vec::new();
        while let Some(ts) = self.stats_timeline.pop() {
            stats_timeline.push(ts);
        }
        let trace = RuntimeTrace {
            events,
            stats_timeline,
        };
        let json = serde_json::to_string(&trace).map_err(|e| io::Error::other(e))?;
        std::fs::write(path, json)
    }
}

/// Start a dashboard and return a handle.
///
/// The dashboard state is created immediately but the HTTP server is NOT started.
/// Call `start_http()` or `start_http_standalone()` to begin serving.
/// Call `install_tracing()` to set up the global subscriber, and `set_runtime()`
/// to enable stats polling.
pub fn start_dashboard(config: DashboardConfig) -> DashboardHandle {
    let store = Arc::new(EventStore::new(
        config.event_capacity,
        config.record,
        config.record_event_capacity,
    ));
    let runtime: Arc<Mutex<Option<Arc<Runtime>>>> = Arc::new(Mutex::new(None));
    let collector: Arc<Mutex<Option<Arc<StatsCollector>>>> = Arc::new(Mutex::new(None));
    let pushed_stats: Arc<Mutex<Option<RuntimeStats>>> = Arc::new(Mutex::new(None));
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let stats_timeline = Arc::new(ArrayQueue::new(config.record_stats_capacity.max(1)));
    let history = Arc::new(DashboardHistory::new(HistoryConfig::default()));

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

    let port = config.port;
    let plugin_registry = Arc::new(PluginRegistry::new());

    DashboardHandle {
        store,
        runtime,
        collector,
        pushed_stats,
        shutdown,
        shutdown_notify,
        stats_timeline,
        history,
        recording: config.record,
        port,
        plugin_registry,
        standalone_rt: Mutex::new(None),
        landing_html: Mutex::new(None),
        extra_router: Mutex::new(None),
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
    dash.start_http_standalone();

    let num_workers = if rt_config.num_threads < 2 {
        1
    } else {
        rt_config.num_threads
    };
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
    let trace: RuntimeTrace =
        serde_json::from_str(&data).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(1)
        .enable_all()
        .build()
        .map_err(|e| io::Error::other(e))?;

    let state = server::ReplayState {
        trace: Arc::new(trace),
        speed: config.speed,
    };
    let port = config.port;

    rt.spawn(async move {
        server::run_replay_server(state, port).await;
    });

    eprintln!("Replay dashboard at http://localhost:{}", config.port);
    eprintln!("Press Ctrl+C to stop");

    loop {
        thread::sleep(Duration::from_secs(3600));
    }
}
