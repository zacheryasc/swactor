mod server;
mod store;
pub mod swactor;
pub mod view;

use std::sync::Arc;

use datastream::frame::{ChannelId, Frame, Lifetime, NodeId, Position, StreamId};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::store::DashboardStore;
use crate::view::{DashboardView, ViewRegistry};

/// Configuration for the datastream dashboard server.
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

/// JSON shape emitted for each incoming datastream frame.
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
            channel: frame.channel.as_str().to_string(),
            position: frame.position.0,
            payload: frame.payload.clone(),
        }
    }

    pub(crate) fn to_datastream_parts(&self) -> Option<(StreamId, Frame)> {
        let stream = StreamId::new(NodeId::new(&self.stream.node), Lifetime(self.stream.life));
        let frame = Frame::new(
            ChannelId::new(&self.channel),
            Position(self.position),
            self.payload.clone(),
        );
        Some((stream, frame))
    }
}

/// Handle to the read-only dashboard server.
///
/// The handle's data path publishes observed datastream frames to HTTP clients
/// and registered views. It does not send signals back to producers or mutate
/// runtime state.
pub struct DashboardHandle {
    port: u16,
    frames: broadcast::Sender<FrameEvent>,
    store: Arc<DashboardStore>,
    views: Arc<ViewRegistry>,
    shutdown_notify: Arc<tokio::sync::Notify>,
    standalone_rt: Mutex<Option<tokio::runtime::Runtime>>,
}

impl DashboardHandle {
    /// Register a read-only view. External crates can keep their interpretation
    /// code beside their component and plug it into this registry.
    pub fn register_view(&self, view: Arc<dyn DashboardView>) {
        self.views.register(view);
    }

    /// Publish one incoming datastream frame to raw clients and all matching views.
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

    /// Start the HTTP server on an existing Tokio runtime.
    pub fn start_http(&self, handle: tokio::runtime::Handle) {
        let state = server::AppState {
            frames: self.frames.clone(),
            store: Arc::clone(&self.store),
            views: Arc::clone(&self.views),
            shutdown_notify: Arc::clone(&self.shutdown_notify),
        };
        let port = self.port;
        handle.spawn(async move {
            server::run_server(state, port).await;
        });
    }

    /// Start the HTTP server on a standalone Tokio runtime.
    pub fn start_http_standalone(&self) {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for dashboard HTTP");
        let handle = rt.handle().clone();
        *self.standalone_rt.lock() = Some(rt);
        self.start_http(handle);
    }
}

/// Create the datastream dashboard state.
///
/// The HTTP server is not started until `start_http` or `start_http_standalone`
/// is called.
pub fn start_dashboard(config: DashboardConfig) -> DashboardHandle {
    let views = Arc::new(ViewRegistry::new());
    views.register(swactor::worker_view());
    let store = Arc::new(DashboardStore::new(
        config.raw_frame_history,
        Arc::clone(&views),
    ));
    let (frames, _) = broadcast::channel(config.frame_buffer.max(1));
    DashboardHandle {
        port: config.port,
        frames,
        store,
        views,
        shutdown_notify: Arc::new(tokio::sync::Notify::new()),
        standalone_rt: Mutex::new(None),
    }
}
