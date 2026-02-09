use std::sync::atomic::{AtomicU64, AtomicUsize};

use crate::actor::ActorAddress;

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
    // Tick timing ring buffer (last N ticks)
    tick_timings: std::sync::Mutex<RingBuffer<TickTiming>>,
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
            tick_timings: std::sync::Mutex::new(RingBuffer::new(1024)),
        }
    }

    pub fn push_tick_timing(&self, timing: TickTiming) {
        self.tick_timings.lock().unwrap().push(timing);
    }

    /// Returns a snapshot of recent tick timings (drains the buffer).
    pub fn drain_tick_timings(&self) -> Vec<TickTiming> {
        self.tick_timings.lock().unwrap().drain()
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
}

/// Per-actor info for the dashboard.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct ActorInfo {
    pub address: ActorAddress,
    pub worker_id: usize,
    pub mailbox_depth: usize,
}

/// Snapshot of overall runtime state.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct RuntimeStats {
    pub num_workers: usize,
    /// Each entry is (address, worker_id).
    pub actors: Vec<(ActorAddress, usize)>,
    pub workers: Vec<WorkerInfo>,
    /// Per-actor detail including mailbox depth.
    pub actor_details: Vec<ActorInfo>,
    /// Recent tick timings per worker (index = worker id).
    pub tick_timings: Vec<Vec<TickTiming>>,
}

/// Simple ring buffer for storing recent values.
pub(crate) struct RingBuffer<T> {
    buf: Vec<T>,
    capacity: usize,
}

impl<T> RingBuffer<T> {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: Vec::with_capacity(capacity),
            capacity,
        }
    }

    pub fn push(&mut self, value: T) {
        if self.buf.len() >= self.capacity {
            self.buf.remove(0);
        }
        self.buf.push(value);
    }

    /// Drain all items, returning them and leaving the buffer empty.
    pub fn drain(&mut self) -> Vec<T> {
        std::mem::take(&mut self.buf)
    }
}

/// Per-actor mailbox depth snapshot, collected by workers.
pub(crate) struct MailboxSnapshot {
    pub depths: Vec<(ActorAddress, usize)>,
}

impl MailboxSnapshot {
    pub fn new() -> Self {
        Self { depths: Vec::new() }
    }
}
