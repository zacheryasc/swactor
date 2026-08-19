#[cfg(feature = "demo-control")]
pub mod control;
mod control_plane;
#[cfg(feature = "demo-control")]
mod demo_control;
mod hardware_view;
mod live_explorer;
mod server;
mod store;
pub mod swactor;
pub mod view;

pub use control_plane::ControlPlaneView;

use std::sync::Arc;

use serde::Serialize;
use telemetry::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use tokio::sync::broadcast;

use crate::store::DashboardStore;
use crate::view::{DashboardView, ViewRegistry};

/// An application-owned page rendered inside the dashboard shell.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginPage {
    pub id: &'static str,
    pub title: &'static str,
    pub path: &'static str,
    pub html: &'static str,
}

impl PluginPage {
    pub const fn new(
        id: &'static str,
        title: &'static str,
        path: &'static str,
        html: &'static str,
    ) -> Self {
        Self {
            id,
            title,
            path,
            html,
        }
    }
}

/// Application routes and their dashboard-visible pages.
pub struct DashboardPlugin {
    pub pages: Vec<PluginPage>,
    pub routes: axum::Router,
}

impl DashboardPlugin {
    pub fn new(routes: axum::Router) -> Self {
        Self {
            pages: Vec::new(),
            routes,
        }
    }

    pub fn with_page(mut self, page: PluginPage) -> Self {
        self.pages.push(page);
        self
    }
}

/// Configuration for the telemetry dashboard server.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
    /// Number of raw frame events retained by the SSE channel for slow clients.
    pub frame_buffer: usize,
    /// Number of recent raw frames retained for `/api/frames`.
    pub raw_frame_history: usize,
    /// Embedding-owned scripts appended to dashboard pages. The dashboard does
    /// not define their behavior or gain mutation capabilities from them.
    pub page_script_urls: Vec<String>,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            frame_buffer: 1024,
            raw_frame_history: 1024,
            page_script_urls: Vec::new(),
        }
    }
}

/// JSON shape emitted for each incoming telemetry frame.
#[derive(Debug, Clone, Serialize)]
pub struct FrameEvent {
    pub stream: StreamEvent,
    pub channel: String,
    pub position: u64,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamEvent {
    pub node: String,
    pub life: u64,
    /// Stream-descriptor metadata when the publisher knows it (catalog
    /// truth). `None` on raw ingest paths that never saw a descriptor.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub origin: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl StreamEvent {
    fn new(stream: &StreamId) -> Self {
        Self {
            node: stream.node.as_str().to_string(),
            life: stream.life.0,
            origin: None,
            label: None,
        }
    }
}

impl FrameEvent {
    pub fn new(stream: &StreamId, frame: &Frame) -> Self {
        Self {
            stream: StreamEvent::new(stream),
            channel: frame.channel.to_string(),
            position: frame.position.0,
            payload: frame.payload.clone(),
        }
    }

    pub(crate) fn to_telemetry_parts(&self) -> Option<(StreamId, Frame)> {
        let stream = StreamId::new(NodeId::new(&self.stream.node), Lifetime(self.stream.life));
        let channel = self
            .channel
            .parse::<u32>()
            .map(ChannelId)
            .unwrap_or(ChannelId(0));
        let frame = Frame::new(channel, Position(self.position), self.payload.clone());
        Some((stream, frame))
    }
}

/// Handle to the read-only dashboard server.
///
/// The handle's data path publishes observed telemetry frames to HTTP clients
/// and registered views. It does not send signals back to producers or mutate
/// runtime state.
#[derive(Clone)]
pub struct DashboardHandle {
    port: u16,
    frames: broadcast::Sender<FrameEvent>,
    store: Arc<DashboardStore>,
    views: Arc<ViewRegistry>,
    page_script_urls: Arc<Vec<String>>,
    shutdown_notify: Arc<tokio::sync::Notify>,
}

impl DashboardHandle {
    /// Create the telemetry dashboard state.
    ///
    /// The HTTP server future is obtained from [`DashboardHandle::http_server`]
    /// and scheduled by the owning swactor engine.
    pub fn new(config: DashboardConfig) -> Self {
        let views = Arc::new(ViewRegistry::new());
        views.register(Arc::new(live_explorer::LiveTelemetryExplorer::default()));
        views.register(Arc::new(ControlPlaneView::default()));
        #[cfg(feature = "demo-control")]
        views.register(Arc::new(demo_control::DemoControlView::default()));
        let store = Arc::new(DashboardStore::new(
            config.raw_frame_history,
            Arc::clone(&views),
        ));
        let (frames, _) = broadcast::channel(config.frame_buffer.max(1));
        Self {
            port: config.port,
            frames,
            store,
            page_script_urls: Arc::new(config.page_script_urls),
            views,
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
        }
    }

    /// Register a read-only view. External crates can keep their interpretation
    /// code beside their component and plug it into this registry.
    pub fn register_view(&self, view: Arc<dyn DashboardView>) {
        self.views.register(view);
    }

    /// Publish one incoming telemetry frame to raw clients and all matching views.
    pub fn ingest(&self, stream: &StreamId, frame: &Frame) {
        let event = self.store.ingest(stream, frame);
        let _ = self.frames.send(event);
    }

    /// Publish an already-serialized frame event to raw clients and views.
    pub fn publish(&self, event: FrameEvent) {
        self.store.publish(event.clone());
        let _ = self.frames.send(event);
    }

    /// Stop the HTTP server.
    pub fn shutdown(&self) {
        self.shutdown_notify.notify_waiters();
    }

    /// Build the HTTP server future for an embedding runtime to poll directly.
    pub fn http_server(&self) -> impl Future<Output = ()> + Send + 'static {
        let state = server::AppState {
            frames: self.frames.clone(),
            store: Arc::clone(&self.store),
            views: Arc::clone(&self.views),
            plugin_pages: Arc::new(Vec::new()),
            page_script_urls: Arc::clone(&self.page_script_urls),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
        };
        let port = self.port;
        async move {
            server::run_server(state, port).await;
        }
    }

    /// Build the dashboard server with application-owned routes on the same
    /// origin. The dashboard router remains read-only; mutation handlers stay
    /// in the embedding application.
    pub fn http_server_with_routes(
        &self,
        routes: axum::Router,
    ) -> impl Future<Output = ()> + Send + 'static {
        let state = server::AppState {
            frames: self.frames.clone(),
            store: Arc::clone(&self.store),
            views: Arc::clone(&self.views),
            plugin_pages: Arc::new(Vec::new()),
            page_script_urls: Arc::clone(&self.page_script_urls),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
        };
        let port = self.port;
        async move {
            server::run_server_with_routes(state, port, routes).await;
        }
    }

    /// Build the dashboard with application plugins. Plugin pages are served
    /// by the dashboard and automatically participate in shared navigation.
    pub fn http_server_with_plugins(
        &self,
        plugins: Vec<DashboardPlugin>,
    ) -> impl Future<Output = ()> + Send + 'static {
        let mut routes = axum::Router::new();
        let mut plugin_pages = Vec::new();
        for plugin in plugins {
            routes = routes.merge(plugin.routes);
            plugin_pages.extend(plugin.pages);
        }
        let state = server::AppState {
            frames: self.frames.clone(),
            store: Arc::clone(&self.store),
            views: Arc::clone(&self.views),
            plugin_pages: Arc::new(plugin_pages),
            page_script_urls: Arc::clone(&self.page_script_urls),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
        };
        let port = self.port;
        async move {
            server::run_server_with_routes(state, port, routes).await;
        }
    }
}

/// Create the telemetry dashboard state.
///
/// The HTTP server future is obtained from [`DashboardHandle::http_server`] and
/// scheduled by the owning swactor engine.
pub fn start_dashboard(config: DashboardConfig) -> DashboardHandle {
    DashboardHandle::new(config)
}
