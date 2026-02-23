use std::sync::Arc;

use crossbeam_queue::SegQueue;

use crate::event::ProcessEvent;

/// Thread-safe queue for buffering process events from I/O threads.
///
/// Cloneable via inner `Arc` — I/O threads push events, the driver's
/// `poll()` drains them.
#[derive(Clone)]
pub struct EventQueue {
    inner: Arc<SegQueue<ProcessEvent>>,
}

impl EventQueue {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(SegQueue::new()),
        }
    }

    /// Push an event (called from I/O threads).
    pub fn push(&self, event: ProcessEvent) {
        self.inner.push(event);
    }

    /// Drain all pending events (called from driver's `poll()`).
    pub fn drain(&self) -> Vec<ProcessEvent> {
        let mut events = Vec::new();
        while let Some(event) = self.inner.pop() {
            events.push(event);
        }
        events
    }
}

impl Default for EventQueue {
    fn default() -> Self {
        Self::new()
    }
}
