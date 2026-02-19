//! Thread-safe metrics for the datastore, consumed by the runtime dashboard.
//!
//! `DatastoreMetrics` accumulates counters and event history from any thread
//! (API handlers run on `tiny_http` worker threads). The dashboard polls
//! `snapshot()` every ~200ms via the `DatastoreStatsProvider` trait.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

// ── Snapshot types (serializable, sent to the dashboard) ────────────────────

/// Point-in-time snapshot of the datastore's state and metrics.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatastoreSnapshot {
    pub node_id: String,
    pub object_count: u64,
    pub total_bytes: u64,
    pub put_ops: u64,
    pub get_ops: u64,
    pub delete_ops: u64,
    pub objects: Vec<ObjectSummary>,
    pub recent_events: Vec<DatastoreEvent>,
    pub active_transfers: Vec<TransferProgress>,
}

/// Summary of a single stored object (for the dashboard table).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObjectSummary {
    pub hash: String,
    pub name: Option<String>,
    pub size_bytes: u64,
}

/// A recorded datastore operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatastoreEvent {
    pub timestamp_ms: u64,
    pub kind: String,
    pub hash: String,
    pub name: Option<String>,
    pub size_bytes: u64,
}

/// Progress of an in-flight chunk transfer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferProgress {
    pub hash: String,
    pub chunks_received: usize,
    pub chunks_total: usize,
}

// ── Live metrics (thread-safe, mutated from API handlers) ───────────────────

const MAX_EVENTS: usize = 200;

/// Thread-safe metrics accumulator for the datastore.
///
/// Atomic counters for the hot path (put/get/delete counts). A `Mutex`-guarded
/// ring buffer for the event timeline and a small vec for active transfers.
pub struct DatastoreMetrics {
    node_id: Mutex<String>,
    put_ops: AtomicU64,
    get_ops: AtomicU64,
    delete_ops: AtomicU64,
    objects: Mutex<Vec<ObjectSummary>>,
    events: Mutex<VecDeque<DatastoreEvent>>,
    transfers: Mutex<Vec<TransferProgress>>,
}

impl DatastoreMetrics {
    pub fn new() -> Self {
        Self {
            node_id: Mutex::new(String::new()),
            put_ops: AtomicU64::new(0),
            get_ops: AtomicU64::new(0),
            delete_ops: AtomicU64::new(0),
            objects: Mutex::new(Vec::new()),
            events: Mutex::new(VecDeque::with_capacity(MAX_EVENTS + 1)),
            transfers: Mutex::new(Vec::new()),
        }
    }

    pub fn set_node_id(&self, hex: String) {
        *self.node_id.lock().unwrap() = hex;
    }

    /// Record a successful PUT operation.
    pub fn record_put(&self, hash: &str, name: Option<&str>, size_bytes: u64) {
        self.put_ops.fetch_add(1, Ordering::Relaxed);
        self.push_event("put", hash, name, size_bytes);
        let mut objs = self.objects.lock().unwrap();
        objs.push(ObjectSummary {
            hash: hash.to_string(),
            name: name.map(|s| s.to_string()),
            size_bytes,
        });
    }

    /// Record a successful GET operation.
    pub fn record_get(&self, hash: &str) {
        self.get_ops.fetch_add(1, Ordering::Relaxed);
        self.push_event("get", hash, None, 0);
    }

    /// Record a successful DELETE operation.
    pub fn record_delete(&self, hash: &str, size_bytes: u64) {
        self.delete_ops.fetch_add(1, Ordering::Relaxed);
        self.push_event("delete", hash, None, size_bytes);
        let mut objs = self.objects.lock().unwrap();
        objs.retain(|o| o.hash != hash);
    }

    /// Begin tracking a chunk transfer.
    pub fn begin_transfer(&self, hash: &str, chunks_total: usize) {
        let mut transfers = self.transfers.lock().unwrap();
        transfers.push(TransferProgress {
            hash: hash.to_string(),
            chunks_received: 0,
            chunks_total,
        });
    }

    /// Advance a tracked transfer by one chunk.
    pub fn advance_transfer(&self, hash: &str) {
        let mut transfers = self.transfers.lock().unwrap();
        if let Some(t) = transfers.iter_mut().find(|t| t.hash == hash) {
            t.chunks_received += 1;
        }
    }

    /// Remove a completed/failed transfer from tracking.
    pub fn end_transfer(&self, hash: &str) {
        let mut transfers = self.transfers.lock().unwrap();
        transfers.retain(|t| t.hash != hash);
    }

    /// Seed the object list (e.g. from an initial LIST query at startup).
    pub fn seed_objects(&self, objects: Vec<ObjectSummary>) {
        *self.objects.lock().unwrap() = objects;
    }

    /// Capture a serializable snapshot of the current metrics.
    pub fn snapshot(&self) -> DatastoreSnapshot {
        let objs = self.objects.lock().unwrap();
        let total_bytes: u64 = objs.iter().map(|o| o.size_bytes).sum();
        let events = self.events.lock().unwrap();
        let transfers = self.transfers.lock().unwrap();

        DatastoreSnapshot {
            node_id: self.node_id.lock().unwrap().clone(),
            object_count: objs.len() as u64,
            total_bytes,
            put_ops: self.put_ops.load(Ordering::Relaxed),
            get_ops: self.get_ops.load(Ordering::Relaxed),
            delete_ops: self.delete_ops.load(Ordering::Relaxed),
            objects: objs.clone(),
            recent_events: events.iter().cloned().collect(),
            active_transfers: transfers.clone(),
        }
    }

    fn push_event(&self, kind: &str, hash: &str, name: Option<&str>, size_bytes: u64) {
        let event = DatastoreEvent {
            timestamp_ms: now_ms(),
            kind: kind.to_string(),
            hash: hash.to_string(),
            name: name.map(|s| s.to_string()),
            size_bytes,
        };
        let mut events = self.events.lock().unwrap();
        if events.len() >= MAX_EVENTS {
            events.pop_front();
        }
        events.push_back(event);
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

// ── Dashboard integration ────────────────────────────────────────────────────

impl dashboard::datastore_collector::DatastoreStatsProvider for DatastoreMetrics {
    fn snapshot_json(&self) -> Option<String> {
        let snap = self.snapshot();
        serde_json::to_string(&snap).ok()
    }
}
