use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use crate::actor::{ActorAddress, ActorExited, ExitReason, ExitValue};

/// Tracks watch relationships between actors.
///
/// Thread-safe via interior `Mutex`. Watch/unwatch operations are rare
/// relative to message sends, so contention is negligible.
pub struct WatchRegistry {
    inner: Mutex<WatchState>,
}

struct WatchState {
    /// target → set of watchers awaiting death notification
    watchers: HashMap<ActorAddress, HashSet<ActorAddress>>,
    /// watcher → set of targets it's watching (reverse index for cleanup)
    watching: HashMap<ActorAddress, HashSet<ActorAddress>>,
}

impl Default for WatchRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl WatchRegistry {
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(WatchState {
                watchers: HashMap::new(),
                watching: HashMap::new(),
            }),
        }
    }

    pub fn watch(&self, watcher: ActorAddress, target: ActorAddress) {
        let mut state = self.inner.lock().unwrap();
        state.watchers.entry(target).or_default().insert(watcher);
        state.watching.entry(watcher).or_default().insert(target);
    }

    pub fn unwatch(&self, watcher: ActorAddress, target: ActorAddress) {
        let mut state = self.inner.lock().unwrap();
        if let Some(set) = state.watchers.get_mut(&target) {
            set.remove(&watcher);
            if set.is_empty() {
                state.watchers.remove(&target);
            }
        }
        if let Some(set) = state.watching.get_mut(&watcher) {
            set.remove(&target);
            if set.is_empty() {
                state.watching.remove(&watcher);
            }
        }
    }

    /// Called when an actor dies. Returns (watcher_addr, ActorExited) pairs.
    pub fn notify_death(
        &self,
        target: ActorAddress,
        reason: ExitReason,
        exit_value: Option<ExitValue>,
    ) -> Vec<(ActorAddress, ActorExited)> {
        let mut state = self.inner.lock().unwrap();
        let notification = ActorExited {
            addr: target,
            reason,
            exit_value,
        };
        let mut result = Vec::new();

        if let Some(watcher_set) = state.watchers.remove(&target) {
            for watcher in &watcher_set {
                result.push((*watcher, notification.clone()));
                if let Some(set) = state.watching.get_mut(watcher) {
                    set.remove(&target);
                    if set.is_empty() {
                        state.watching.remove(watcher);
                    }
                }
            }
        }

        result
    }

    /// Called when a watcher itself dies. Cleans up all its watching entries.
    pub fn cleanup_watcher(&self, watcher: &ActorAddress) {
        let mut state = self.inner.lock().unwrap();
        if let Some(targets) = state.watching.remove(watcher) {
            for target in targets {
                if let Some(set) = state.watchers.get_mut(&target) {
                    set.remove(watcher);
                    if set.is_empty() {
                        state.watchers.remove(&target);
                    }
                }
            }
        }
    }
}
