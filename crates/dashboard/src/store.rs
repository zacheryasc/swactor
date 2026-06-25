use std::collections::VecDeque;
use std::sync::Arc;

use datastream::frame::{Frame, StreamId};
use parking_lot::Mutex;

use crate::FrameEvent;
use crate::view::ViewRegistry;

pub(crate) struct DashboardStore {
    recent: Mutex<VecDeque<FrameEvent>>,
    recent_cap: usize,
    views: Arc<ViewRegistry>,
}

impl DashboardStore {
    pub fn new(recent_cap: usize, views: Arc<ViewRegistry>) -> Self {
        Self {
            recent: Mutex::new(VecDeque::with_capacity(recent_cap.min(4096))),
            recent_cap: recent_cap.max(1),
            views,
        }
    }

    pub fn ingest(&self, stream: &StreamId, frame: &Frame) -> FrameEvent {
        let event = FrameEvent::new(stream, frame);
        self.record(event.clone());
        self.views.dispatch(stream, frame, &event);
        event
    }

    pub fn publish(&self, event: FrameEvent) {
        if let Some((stream, frame)) = event.to_datastream_parts() {
            self.views.dispatch(&stream, &frame, &event);
        }
        self.record(event);
    }

    pub fn recent_frames(&self) -> Vec<FrameEvent> {
        self.recent.lock().iter().cloned().collect()
    }

    fn record(&self, event: FrameEvent) {
        let mut recent = self.recent.lock();
        if recent.len() == self.recent_cap {
            recent.pop_front();
        }
        recent.push_back(event);
    }
}
