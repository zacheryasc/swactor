mod hardware_view;
mod live_explorer;
mod server;
mod store;
pub mod swactor;
pub mod view;

use std::sync::Arc;

use telemetry::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::store::DashboardStore;
use crate::view::{DashboardView, ViewRegistry};

/// Configuration for the telemetry dashboard server.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
    /// Number of raw frame events retained by the SSE channel for slow clients.
    pub frame_buffer: usize,
    /// Number of recent raw frames retained for `/api/frames`.
    pub raw_frame_history: usize,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            port: 9090,
            frame_buffer: 1024,
            raw_frame_history: 1024,
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
}

impl FrameEvent {
    pub fn new(stream: &StreamId, frame: &Frame) -> Self {
        Self {
            stream: StreamEvent {
                node: stream.node.as_str().to_string(),
                life: stream.life.0,
            },
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
pub struct DashboardHandle {
    port: u16,
    frames: broadcast::Sender<FrameEvent>,
    store: Arc<DashboardStore>,
    views: Arc<ViewRegistry>,
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
        views.register(Arc::new(hardware_view::HardwareDashboardView::default()));
        views.register(swactor::worker_view());
        views.register(swactor::actor_overview_view());
        views.register(swactor::actor_dossier_view());
        let store = Arc::new(DashboardStore::new(
            config.raw_frame_history,
            Arc::clone(&views),
        ));
        let (frames, _) = broadcast::channel(config.frame_buffer.max(1));
        Self {
            port: config.port,
            frames,
            store,
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
            shutdown_notify: Arc::clone(&self.shutdown_notify),
        };
        let port = self.port;
        async move {
            server::run_server(state, port).await;
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
