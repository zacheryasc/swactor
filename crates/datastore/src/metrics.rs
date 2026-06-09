//! Thread-safe metrics for the datastore.
//!
//! `DatastoreMetrics` accumulates counters and object state from any thread
//! (API handlers run on `tiny_http` worker threads). It is a *source*, not a
//! sink: the node frames a [`readout`](DatastoreMetrics::readout) onto the
//! datastream each refresh for the steady totals, and every recorded operation
//! is pushed to an optional [`DatastoreEventObserver`] so the op timeline
//! streams live (the streaming successor of the old fixed-size event ring).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

// ── Readout types (plain data handed to the framing layer) ──────────────────

/// Point-in-time readout of the datastore's steady metrics. The node maps this
/// onto the datastream's `datastore.state` record; it is deliberately not a wire
/// type itself, so the datastore crate carries no datastream dependency.
#[derive(Debug, Clone, PartialEq)]
pub struct DatastoreReadout {
    pub object_count: u64,
    pub total_bytes: u64,
    pub put_ops: u64,
    pub get_ops: u64,
    pub delete_ops: u64,
    pub objects: Vec<ObjectSummary>,
    pub active_transfers: Vec<TransferProgress>,
}

/// Summary of a single stored object (for the dashboard table).
#[derive(Debug, Clone, PartialEq)]
pub struct ObjectSummary {
    pub hash: String,
    pub name: Option<String>,
    pub size_bytes: u64,
}

/// Progress of an in-flight chunk transfer.
#[derive(Debug, Clone, PartialEq)]
pub struct TransferProgress {
    pub hash: String,
    pub chunks_received: usize,
    pub chunks_total: usize,
}

/// Receives every recorded datastore operation as it happens. The node installs
/// an observer that frames each call onto the `datastore.events` channel; the
/// datastore itself defines the trait (it owns the events) so it depends on no
/// transport — the same split as the runtime's process-output observer.
pub trait DatastoreEventObserver: Send + Sync {
    fn on_event(
        &self,
        timestamp_ms: u64,
        kind: &str,
        hash: &str,
        name: Option<&str>,
        size_bytes: u64,
    );
}

// ── Live metrics (thread-safe, mutated from API handlers) ───────────────────

/// Thread-safe metrics accumulator for the datastore.
///
/// Atomic counters for the hot path (put/get/delete counts). A `Mutex`-guarded
/// vec for the object table and active transfers. An optional event observer the
/// node uses to stream each operation onto the datastream.
pub struct DatastoreMetrics {
    node_id: Mutex<String>,
    put_ops: AtomicU64,
    get_ops: AtomicU64,
    delete_ops: AtomicU64,
    objects: Mutex<Vec<ObjectSummary>>,
    transfers: Mutex<Vec<TransferProgress>>,
    event_observer: OnceLock<Arc<dyn DatastoreEventObserver>>,
}

impl Default for DatastoreMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl DatastoreMetrics {
    pub fn new() -> Self {
        Self {
            node_id: Mutex::new(String::new()),
            put_ops: AtomicU64::new(0),
            get_ops: AtomicU64::new(0),
            delete_ops: AtomicU64::new(0),
            objects: Mutex::new(Vec::new()),
            transfers: Mutex::new(Vec::new()),
            event_observer: OnceLock::new(),
        }
    }

    pub fn set_node_id(&self, hex: String) {
        *self.node_id.lock().unwrap() = hex;
    }

    /// Hex-encoded id of the node these metrics belong to.
    pub fn node_id(&self) -> String {
        self.node_id.lock().unwrap().clone()
    }

    /// Install the observer that streams each recorded operation. Best-effort and
    /// idempotent: the first installation wins (the node installs once at startup).
    pub fn set_event_observer(&self, observer: Arc<dyn DatastoreEventObserver>) {
        let _ = self.event_observer.set(observer);
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

    /// Capture a readout of the current steady metrics.
    pub fn readout(&self) -> DatastoreReadout {
        let objs = self.objects.lock().unwrap();
        let total_bytes: u64 = objs.iter().map(|o| o.size_bytes).sum();
        let transfers = self.transfers.lock().unwrap();

        DatastoreReadout {
            object_count: objs.len() as u64,
            total_bytes,
            put_ops: self.put_ops.load(Ordering::Relaxed),
            get_ops: self.get_ops.load(Ordering::Relaxed),
            delete_ops: self.delete_ops.load(Ordering::Relaxed),
            objects: objs.clone(),
            active_transfers: transfers.clone(),
        }
    }

    fn push_event(&self, kind: &str, hash: &str, name: Option<&str>, size_bytes: u64) {
        if let Some(obs) = self.event_observer.get() {
            obs.on_event(now_ms(), kind, hash, name, size_bytes);
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
