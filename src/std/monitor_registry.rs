use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::actor::{ActorAddress, MonitorRef};
use crate::{AddrBuildHasher, AddrMap};

/// Tracks monitor subscriptions: watched actor → list of (MonitorRef, watcher address).
///
/// Write-rare (monitor/demonitor/death), read at cleanup time.
pub struct MonitorRegistry {
    /// watched_addr → [(mref, watcher_addr)]
    monitors: RwLock<AddrMap<Vec<(MonitorRef, ActorAddress)>>>,
    /// mref → watched_addr (for O(1) demonitor)
    ref_to_target: RwLock<HashMap<MonitorRef, ActorAddress>>,
    next_ref: AtomicU64,
}

impl Default for MonitorRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl MonitorRegistry {
    pub fn new() -> Self {
        Self {
            monitors: RwLock::new(HashMap::with_hasher(AddrBuildHasher)),
            ref_to_target: RwLock::new(HashMap::new()),
            next_ref: AtomicU64::new(1),
        }
    }

    /// Register a monitor: `watcher` wants to know when `target` dies.
    pub fn register(&self, watcher: ActorAddress, target: ActorAddress) -> MonitorRef {
        let id = self.next_ref.fetch_add(1, Ordering::Relaxed);
        let mref = MonitorRef::from_raw(id);
        self.monitors.write().unwrap()
            .entry(target)
            .or_default()
            .push((mref, watcher));
        self.ref_to_target.write().unwrap().insert(mref, target);
        mref
    }

    /// Cancel a monitor by its ref.
    pub fn deregister(&self, mref: MonitorRef) {
        if let Some(target) = self.ref_to_target.write().unwrap().remove(&mref) {
            let mut monitors = self.monitors.write().unwrap();
            if let Some(watchers) = monitors.get_mut(&target) {
                watchers.retain(|(r, _)| *r != mref);
                if watchers.is_empty() {
                    monitors.remove(&target);
                }
            }
        }
    }

    /// Remove and return all monitors for a dead actor.
    pub fn take_monitors(&self, target: &ActorAddress) -> Vec<(MonitorRef, ActorAddress)> {
        let watchers = self.monitors.write().unwrap().remove(target).unwrap_or_default();
        let mut ref_map = self.ref_to_target.write().unwrap();
        for (mref, _) in &watchers {
            ref_map.remove(mref);
        }
        watchers
    }

    /// Remove all monitor subscriptions where `addr` is the watcher (dead watcher cleanup).
    pub fn remove_watcher(&self, addr: &ActorAddress) {
        let mut monitors = self.monitors.write().unwrap();
        let mut ref_map = self.ref_to_target.write().unwrap();
        monitors.retain(|_target, watchers| {
            watchers.retain(|(mref, watcher)| {
                if watcher == addr {
                    ref_map.remove(mref);
                    false
                } else {
                    true
                }
            });
            !watchers.is_empty()
        });
    }
}
