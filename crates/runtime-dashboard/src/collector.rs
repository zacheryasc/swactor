//! Lock-free-ish stats collector that implements [`StatsHook`].
//!
//! Workers push per-actor snapshots here; dashboard code reads them back
//! via [`actor_details()`](StatsCollector::actor_details) or
//! [`enrich()`](StatsCollector::enrich).

use std::sync::{Arc, RwLock};

use swactor::stats::{ActorInfo, ActorSnapshot, RuntimeStats, StatsHook};

/// Collects per-actor snapshots pushed by worker threads.
///
/// Create with [`new()`](Self::new), pass to both
/// [`Runtime::set_stats_hook()`](swactor::runtime::Runtime::set_stats_hook)
/// and the dashboard.
pub struct StatsCollector {
    /// One slot per worker — writers never contend with each other.
    slots: Vec<RwLock<Vec<ActorInfo>>>,
}

impl StatsCollector {
    pub fn new(num_workers: usize) -> Arc<Self> {
        let slots = (0..num_workers)
            .map(|_| RwLock::new(Vec::new()))
            .collect();
        Arc::new(Self { slots })
    }

    /// Collect all per-actor details across workers.
    pub fn actor_details(&self) -> Vec<ActorInfo> {
        let mut out = Vec::new();
        for slot in &self.slots {
            let guard = slot.read().unwrap();
            out.extend(guard.iter().cloned());
        }
        out
    }

    /// Patch `actor_details` into an existing [`RuntimeStats`].
    pub fn enrich(&self, stats: &mut RuntimeStats) {
        stats.actor_details = self.actor_details();
    }
}

impl StatsHook for StatsCollector {
    fn on_tick(&self, worker_id: usize, snapshots: &[ActorSnapshot]) {
        if let Some(slot) = self.slots.get(worker_id) {
            let mut guard = slot.write().unwrap();
            guard.clear();
            guard.extend(snapshots.iter().map(|s| ActorInfo {
                address: s.address,
                worker_id,
                mailbox_depth: s.mailbox_depth,
                last_msg_type: s.last_msg_type.map(|t| t.to_string()),
                messages_processed: s.messages_processed,
                poisoned: s.poisoned,
            }));
        }
    }
}
