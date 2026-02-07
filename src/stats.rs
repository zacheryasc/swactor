use std::sync::atomic::{AtomicU64, AtomicUsize};

use crate::actor::ActorAddress;

/// Per-worker stats published via atomics. Readable from any thread.
pub struct WorkerStats {
    pub num_actors: AtomicUsize,
    pub total_mailbox_depth: AtomicUsize,
    pub messages_processed: AtomicU64,
}

impl WorkerStats {
    pub fn new() -> Self {
        Self {
            num_actors: AtomicUsize::new(0),
            total_mailbox_depth: AtomicUsize::new(0),
            messages_processed: AtomicU64::new(0),
        }
    }
}

/// Snapshot of per-worker state.
pub struct WorkerInfo {
    pub id: usize,
    pub num_actors: usize,
    pub mailbox_depth: usize,
    pub messages_processed: u64,
}

/// Snapshot of overall runtime state.
pub struct RuntimeStats {
    pub num_workers: usize,
    /// Each entry is (address, worker_id).
    pub actors: Vec<(ActorAddress, usize)>,
    pub workers: Vec<WorkerInfo>,
}
