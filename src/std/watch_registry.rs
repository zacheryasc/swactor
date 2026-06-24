use std::collections::{HashMap, HashSet};

use parking_lot::Mutex;

use crate::actor::{ActorAddress, ActorExited, ExitReason, ExitValue};

/// Tracks watch relationships between actors.
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
        let mut state = self.inner.lock();
        state.watchers.entry(target).or_default().insert(watcher);
        state.watching.entry(watcher).or_default().insert(target);
    }

    /// Called when an actor dies. Returns (watcher_addr, ActorExited) pairs.
    pub fn notify_death(
        &self,
        target: ActorAddress,
        reason: ExitReason,
        exit_value: Option<ExitValue>,
    ) -> Vec<(ActorAddress, ActorExited)> {
        let mut state = self.inner.lock();
        let watchers = state.watchers.remove(&target).unwrap_or_default();
        for watcher in &watchers {
            if let Some(targets) = state.watching.get_mut(watcher) {
                targets.remove(&target);
            }
        }
        watchers
            .into_iter()
            .map(|watcher| {
                (
                    watcher,
                    ActorExited {
                        addr: target,
                        reason: reason.clone(),
                        exit_value: exit_value.clone(),
                    },
                )
            })
            .collect()
    }

    /// Called when a watcher itself dies. Cleans up all its watching entries.
    pub fn cleanup_watcher(&self, watcher: &ActorAddress) {
        let mut state = self.inner.lock();
        if let Some(targets) = state.watching.remove(watcher) {
            for target in targets {
                if let Some(watchers) = state.watchers.get_mut(&target) {
                    watchers.remove(watcher);
                    if watchers.is_empty() {
                        state.watchers.remove(&target);
                    }
                }
            }
        }
    }
}
