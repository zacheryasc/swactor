use std::sync::Arc;

use parking_lot::RwLock;
use serde::Serialize;
use serde_json::Value;
use telemetry::frame::{Frame, StreamId};

use crate::FrameEvent;

/// Read-only interpretation of one or more telemetry channels.
///
/// Views are observation-only: they fold incoming frames into local state and
/// expose JSON/HTML. They do not send control messages back to the runtime.
pub trait DashboardView: Send + Sync {
    fn id(&self) -> &'static str;
    fn title(&self) -> &'static str;
    fn path(&self) -> &'static str {
        self.id()
    }
    fn channels(&self) -> &'static [&'static str];
    fn ingest(&self, stream: &StreamId, frame: &Frame, event: &FrameEvent);
    fn snapshot_json(&self) -> Value;
    /// Bounded per-entity detail lookup for focused UI panes. `query` is the
    /// raw URL query string (e.g. `stream=...&actor=...`). Views that offer
    /// no detail endpoint leave this default.
    fn detail_json(&self, query: &str) -> Option<Value> {
        let _ = query;
        None
    }
    /// Whether this view appears in the unified top navbar. Registration
    /// order controls link order.
    fn show_in_nav(&self) -> bool {
        true
    }
    fn html(&self) -> Option<&'static str> {
        None
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewDescriptor {
    pub id: &'static str,
    pub title: &'static str,
    pub path: &'static str,
    pub page: String,
    pub api: String,
    pub channels: &'static [&'static str],
    pub show_in_nav: bool,
}

#[derive(Default)]
pub(crate) struct ViewRegistry {
    views: RwLock<Vec<Arc<dyn DashboardView>>>,
}

impl ViewRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&self, view: Arc<dyn DashboardView>) {
        let mut views = self.views.write();
        views.retain(|existing| existing.id() != view.id() && existing.path() != view.path());
        views.push(view);
    }

    pub fn dispatch(&self, stream: &StreamId, frame: &Frame, event: &FrameEvent) {
        let channel = event.channel.as_str();
        for view in self.views.read().iter() {
            let channels = view.channels();
            if channels.is_empty() || channels.contains(&channel) {
                view.ingest(stream, frame, event);
            }
        }
    }

    pub fn descriptors(&self) -> Vec<ViewDescriptor> {
        self.views
            .read()
            .iter()
            .map(|view| ViewDescriptor {
                id: view.id(),
                title: view.title(),
                path: view.path(),
                page: format!("/view/{}", view.path()),
                api: format!("/api/view/{}", view.path()),
                channels: view.channels(),
                show_in_nav: view.show_in_nav(),
            })
            .collect()
    }

    /// Per-entity detail lookup: `/api/view/<path>/detail?<query>`.
    pub fn detail(&self, path: &str, query: &str) -> Option<Value> {
        self.views
            .read()
            .iter()
            .find(|view| view.path() == path || view.id() == path)
            .and_then(|view| view.detail_json(query))
    }

    pub fn snapshot(&self, path: &str) -> Option<Value> {
        self.views
            .read()
            .iter()
            .find(|view| view.path() == path || view.id() == path)
            .map(|view| view.snapshot_json())
    }

    pub fn html(&self, path: &str) -> Option<&'static str> {
        self.views
            .read()
            .iter()
            .find(|view| view.path() == path || view.id() == path)
            .and_then(|view| view.html())
    }
}
