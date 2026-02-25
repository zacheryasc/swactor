use std::collections::VecDeque;

use crate::action::ProcessAction;
use crate::event::ProcessEvent;
use crate::types::ProcessDriver;

/// A test-oriented driver that records executed actions and lets you inject events.
pub struct MockDriver {
    pending_events: VecDeque<ProcessEvent>,
    executed_actions: Vec<ProcessAction>,
}

impl MockDriver {
    pub fn new() -> Self {
        Self {
            pending_events: VecDeque::new(),
            executed_actions: Vec::new(),
        }
    }

    /// Queue a single event to be returned by the next `poll()`.
    pub fn inject(&mut self, event: ProcessEvent) {
        self.pending_events.push_back(event);
    }

    /// Queue multiple events to be returned by subsequent `poll()` calls.
    pub fn inject_many(&mut self, events: impl IntoIterator<Item = ProcessEvent>) {
        self.pending_events.extend(events);
    }

    /// View all actions that have been executed so far.
    pub fn executed_actions(&self) -> &[ProcessAction] {
        &self.executed_actions
    }

    /// Take all executed actions, clearing the internal log.
    pub fn take_executed_actions(&mut self) -> Vec<ProcessAction> {
        std::mem::take(&mut self.executed_actions)
    }

    /// Number of events waiting to be polled.
    pub fn pending_event_count(&self) -> usize {
        self.pending_events.len()
    }
}

impl Default for MockDriver {
    fn default() -> Self {
        Self::new()
    }
}

impl ProcessDriver for MockDriver {
    fn execute(&mut self, action: ProcessAction) {
        self.executed_actions.push(action);
    }

    fn poll(&mut self) -> Vec<ProcessEvent> {
        self.pending_events.drain(..).collect()
    }
}
