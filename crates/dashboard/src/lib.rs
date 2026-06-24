pub mod datastream_source;
pub mod history;
mod html;
mod layer;
pub mod plugin;
mod server;
pub mod telemetry;
pub mod topology;
pub mod warnings;
pub use crate::layer::{DashboardEvent, EventStore};

/// The canonical Distribution page (the SWIM connection-graph view). Owned by
/// the dashboard crate so every front-end that serves it — a live node's
/// `DistributionPlugin` and the datastream dashboard — renders the exact same
/// page and chrome, fed by the distribution-page JSON the datastream consumer
/// reconstructs (see [`crate::datastream_source`]).
pub const DISTRIBUTION_PAGE_HTML: &str = include_str!("distribution_page.html");

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use swactor::stats::RuntimeStats;

use crate::history::{DashboardHistory, HistoryConfig};
use crate::layer::now_ms;
use crate::plugin::PluginRegistry;

/// Configuration for the datastream-backed dashboard.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
    pub event_capacity: usize,
    /// Enable full activity-log retention inside the event store.
    pub record: bool,
    /// Maximum events retained in the recording log. Only used when `record = true`.
    pub record_event_capacity: usize,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            event_capacity: 10_000,
            record: false,
            record_event_capacity: 100_000,
        }
    }
}

/// Handle to a running datastream-backed dashboard.
pub struct DashboardHandle {
    store: Arc<EventStore>,
    /// Externally pushed stats from the datastream fold (latest wins).
    pushed_stats: Arc<Mutex<Option<RuntimeStats>>>,
    shutdown: Arc<AtomicBool>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    history: Arc<DashboardHistory>,
    port: u16,
    plugin_registry: Arc<PluginRegistry>,
    standalone_rt: Mutex<Option<tokio::runtime::Runtime>>,
    /// Optional override for the `/` landing page (e.g. a host serving a fleet
    /// board instead of the single-node actor dashboard).
    landing_html: Mutex<Option<Arc<str>>>,
    /// Optional extra axum router merged into the live server, for hosts that
    /// add disjoint routes of their own.
    extra_router: Mutex<Option<axum::Router>>,
}

impl DashboardHandle {
    /// Push a stats snapshot from the datastream fold (latest wins).
    pub fn set_stats(&self, stats: RuntimeStats) {
        *self.pushed_stats.lock().unwrap() = Some(stats);
    }

    /// Push a dashboard activity line into the SSE event stream.
    pub fn push_activity(&self, is_warn: bool, message: impl Into<String>) {
        self.store.push(DashboardEvent {
            seq: 0,
            timestamp_ms: now_ms(),
            level: if is_warn { "WARN" } else { "INFO" }.to_string(),
            message: message.into(),
            worker_id: None,
            actor_addr: None,
            fields: serde_json::Map::new(),
        });
    }

    /// Register a plugin with the dashboard.
    pub fn register_plugin(&self, plugin: Arc<dyn plugin::DashboardPlugin>) {
        self.plugin_registry.register(plugin);
    }

    /// Override the `/` landing page with custom HTML. Used when the dashboard
    /// shows a fleet board rather than the single-node actor dashboard.
    pub fn set_landing_html(&self, html: impl Into<Arc<str>>) {
        *self.landing_html.lock().unwrap() = Some(html.into());
    }

    /// Merge an extra axum router into the live HTTP server. The routes must be
    /// disjoint from the dashboard's own routes. Must be called before
    /// `start_http`/`start_http_standalone`.
    pub fn set_extra_router(&self, router: axum::Router) {
        *self.extra_router.lock().unwrap() = Some(router);
    }

    /// Access the time-series history store.
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
            pushed_stats: Arc::clone(&self.pushed_stats),
            shutdown: Arc::clone(&self.shutdown),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
            history: Arc::clone(&self.history),
            plugins: Arc::clone(&self.plugin_registry),
            landing: self.landing_html.lock().unwrap().clone(),
        }
    }
}

/// Start a dashboard and return a handle.
///
/// The dashboard state is created immediately but the HTTP server is NOT started.
/// Call `start_http()` or `start_http_standalone()` to begin serving.
/// Push datastream-folded stats with `set_stats()`; HTTP/SSE reads only those
/// snapshots and registered plugins.
pub fn start_dashboard(config: DashboardConfig) -> DashboardHandle {
    let store = Arc::new(EventStore::new(
        config.event_capacity,
        config.record,
        config.record_event_capacity,
    ));
    let pushed_stats: Arc<Mutex<Option<RuntimeStats>>> = Arc::new(Mutex::new(None));
    let shutdown = Arc::new(AtomicBool::new(false));
    let shutdown_notify = Arc::new(tokio::sync::Notify::new());
    let history = Arc::new(DashboardHistory::new(HistoryConfig::default()));

    let port = config.port;
    let plugin_registry = Arc::new(PluginRegistry::new());

    DashboardHandle {
        store,
        pushed_stats,
        shutdown,
        shutdown_notify,
        history,
        port,
        plugin_registry,
        standalone_rt: Mutex::new(None),
        landing_html: Mutex::new(None),
        extra_router: Mutex::new(None),
    }
}
