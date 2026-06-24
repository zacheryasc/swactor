use std::collections::VecDeque;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_queue::ArrayQueue;
use serde::{Deserialize, Serialize};

/// A single dashboard activity event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DashboardEvent {
    pub seq: u64,
    pub timestamp_ms: u64,
    pub level: String,
    pub message: String,
    pub worker_id: Option<usize>,
    pub actor_addr: Option<String>,
    pub fields: serde_json::Map<String, serde_json::Value>,
}

/// Thread-safe ring buffer for dashboard events, with optional full-log recording.
pub struct EventStore {
    events: Mutex<VecDeque<DashboardEvent>>,
    capacity: usize,
    next_seq: AtomicU64,
    /// When recording is enabled, events are kept in a lock-free bounded ring buffer.
    full_log: Option<ArrayQueue<DashboardEvent>>,
}

impl EventStore {
    pub fn new(capacity: usize, record: bool, record_event_capacity: usize) -> Self {
        Self {
            events: Mutex::new(VecDeque::with_capacity(capacity)),
            capacity,
            next_seq: AtomicU64::new(0),
            full_log: if record {
                Some(ArrayQueue::new(record_event_capacity.max(1)))
            } else {
                None
            },
        }
    }

    pub fn push(&self, mut event: DashboardEvent) {
        event.seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        if let Some(ref log) = self.full_log {
            let _ = log.force_push(event.clone());
        }
        let mut events = self.events.lock().unwrap();
        if events.len() >= self.capacity {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// Read events starting from `cursor`. Returns the new events and the updated cursor.
    pub fn read_from(&self, cursor: u64) -> (Vec<DashboardEvent>, u64) {
        let events = self.events.lock().unwrap();
        if events.is_empty() {
            return (Vec::new(), cursor);
        }

        let first_seq = events.front().unwrap().seq;
        let last_seq = events.back().unwrap().seq;

        if cursor > last_seq {
            return (Vec::new(), cursor);
        }

        let start = if cursor <= first_seq {
            0
        } else {
            (cursor - first_seq) as usize
        };

        let batch: Vec<DashboardEvent> = events.iter().skip(start).cloned().collect();
        let new_cursor = last_seq + 1;
        (batch, new_cursor)
    }

    /// Read recent events for a specific actor address (hex prefix match).
    /// Returns up to `limit` most recent matching events.
    pub fn read_for_actor(&self, actor_hex: &str, limit: usize) -> Vec<DashboardEvent> {
        let events = self.events.lock().unwrap();
        let lower = actor_hex.to_lowercase();
        events
            .iter()
            .rev()
            .filter(|e| {
                e.actor_addr
                    .as_ref()
                    .map(|a| {
                        a.to_lowercase().starts_with(&lower) || a.to_lowercase().contains(&lower)
                    })
                    .unwrap_or(false)
            })
            .take(limit)
            .cloned()
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect()
    }

    /// Drains the full recording log. Only available when recording is enabled.
    /// This is destructive — events are consumed by callers that export logs.
    pub fn all_events(&self) -> Option<Vec<DashboardEvent>> {
        self.full_log.as_ref().map(|log| {
            let mut out = Vec::new();
            while let Some(ev) = log.pop() {
                out.push(ev);
            }
            out
        })
    }
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
