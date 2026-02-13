use std::sync::atomic::{AtomicU64, AtomicUsize};

use crossbeam_queue::ArrayQueue;

use crate::actor::ActorAddress;

const TICK_BUFFER_CAP: usize = 1024;

/// Timing data for one tick_once invocation.
#[derive(Debug, Clone, Default)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct TickTiming {
    /// Microseconds spent in each of the 6 phases.
    pub phase_us: [u64; 6],
    /// Total messages processed this tick.
    pub messages_processed: usize,
    /// Whether any work was done this tick.
    pub did_work: bool,
}

/// Per-worker stats published via atomics. Readable from any thread.
pub struct WorkerStats {
    pub num_actors: AtomicUsize,
    pub total_mailbox_depth: AtomicUsize,
    pub messages_processed: AtomicU64,
    // Message routing counters
    pub local_sends: AtomicU64,
    pub cross_sends: AtomicU64,
    pub inbox_sends: AtomicU64,
    // Error counters
    pub type_mismatches: AtomicU64,
    pub panics: AtomicU64,
    /// Messages dropped due to mailbox overflow (bounded mailbox policy).
    pub messages_dropped: AtomicU64,
    /// Number of actor restarts after panic (restartable actors only).
    pub restarts: AtomicU64,
    /// Number of actors gracefully stopped via `ctx.stop_self()` or `Runtime::stop_actor()`.
    pub stops: AtomicU64,
    // Tick timing ring buffer (last N ticks, lock-free)
    tick_timings: ArrayQueue<TickTiming>,
}

impl WorkerStats {
    pub fn new() -> Self {
        Self {
            num_actors: AtomicUsize::new(0),
            total_mailbox_depth: AtomicUsize::new(0),
            messages_processed: AtomicU64::new(0),
            local_sends: AtomicU64::new(0),
            cross_sends: AtomicU64::new(0),
            inbox_sends: AtomicU64::new(0),
            type_mismatches: AtomicU64::new(0),
            panics: AtomicU64::new(0),
            messages_dropped: AtomicU64::new(0),
            restarts: AtomicU64::new(0),
            stops: AtomicU64::new(0),
            tick_timings: ArrayQueue::new(TICK_BUFFER_CAP),
        }
    }

    pub fn push_tick_timing(&self, timing: TickTiming) {
        if let Err(rejected) = self.tick_timings.push(timing) {
            // Ring full — drop oldest, then retry (best-effort for stats)
            let _ = self.tick_timings.pop();
            let _ = self.tick_timings.push(rejected);
        }
    }

    /// Returns a snapshot of recent tick timings (drains the buffer).
    pub fn drain_tick_timings(&self) -> Vec<TickTiming> {
        let mut out = Vec::new();
        while let Some(t) = self.tick_timings.pop() {
            out.push(t);
        }
        out
    }

    /// Create a point-in-time snapshot as a [`WorkerInfo`].
    pub fn snapshot(&self, id: usize) -> WorkerInfo {
        use std::sync::atomic::Ordering::Relaxed;
        WorkerInfo {
            id,
            num_actors: self.num_actors.load(Relaxed),
            mailbox_depth: self.total_mailbox_depth.load(Relaxed),
            messages_processed: self.messages_processed.load(Relaxed),
            local_sends: self.local_sends.load(Relaxed),
            cross_sends: self.cross_sends.load(Relaxed),
            inbox_sends: self.inbox_sends.load(Relaxed),
            type_mismatches: self.type_mismatches.load(Relaxed),
            panics: self.panics.load(Relaxed),
            messages_dropped: self.messages_dropped.load(Relaxed),
            restarts: self.restarts.load(Relaxed),
            stops: self.stops.load(Relaxed),
        }
    }
}

/// Snapshot of per-worker state.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct WorkerInfo {
    pub id: usize,
    pub num_actors: usize,
    pub mailbox_depth: usize,
    pub messages_processed: u64,
    pub local_sends: u64,
    pub cross_sends: u64,
    pub inbox_sends: u64,
    pub type_mismatches: u64,
    pub panics: u64,
    pub messages_dropped: u64,
    pub restarts: u64,
    pub stops: u64,
}

/// Per-actor snapshot transferred from worker to runtime (not serialized).
pub struct ActorSnapshot {
    pub address: ActorAddress,
    pub mailbox_depth: usize,
    pub last_msg_type: Option<&'static str>,
    pub messages_processed: u64,
    pub poisoned: bool,
    /// Per-message-type counts, sorted descending by count.
    pub message_type_counts: Vec<(&'static str, u64)>,
}

/// Observer hook called by workers after productive ticks.
///
/// Implement this to collect per-actor snapshot data outside the runtime.
/// The runtime itself stores nothing — snapshots are ephemeral and passed by reference.
pub trait StatsHook: Send + Sync {
    /// Called once per worker after a productive tick.
    ///
    /// `worker_id` is the index of the worker (0..num_workers).
    /// `snapshots` borrows the worker's scratch buffer — copy what you need.
    fn on_tick(&self, worker_id: usize, snapshots: &[ActorSnapshot]);
}

/// Per-actor info for the dashboard.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorInfo {
    pub address: ActorAddress,
    pub worker_id: usize,
    pub mailbox_depth: usize,
    /// Rust type name of the last message this actor successfully handled.
    #[cfg_attr(feature = "serde", serde(default))]
    pub last_msg_type: Option<String>,
    /// Total messages successfully processed by this actor.
    #[cfg_attr(feature = "serde", serde(default))]
    pub messages_processed: u64,
    /// Whether the actor has panicked and is no longer processing messages.
    #[cfg_attr(feature = "serde", serde(default))]
    pub poisoned: bool,
    /// Per-message-type counts, sorted descending by count. Top 32 types.
    #[cfg_attr(feature = "serde", serde(default))]
    pub message_type_counts: Vec<(String, u64)>,
}

/// Snapshot of overall runtime state.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RuntimeStats {
    pub num_workers: usize,
    /// Milliseconds since the runtime was created.
    #[cfg_attr(feature = "serde", serde(default))]
    pub uptime_ms: u64,
    /// Each entry is (address, worker_id).
    pub actors: Vec<(ActorAddress, usize)>,
    pub workers: Vec<WorkerInfo>,
    /// Per-actor detail including mailbox depth.
    pub actor_details: Vec<ActorInfo>,
    /// Recent tick timings per worker (index = worker id).
    pub tick_timings: Vec<Vec<TickTiming>>,
}
