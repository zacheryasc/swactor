use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use crossbeam_queue::ArrayQueue;
use serde::{Deserialize, Serialize};
use tracing::field::{Field, Visit};
use tracing::span;
use tracing::Subscriber;
use tracing_subscriber::layer::Context;
use tracing_subscriber::registry::LookupSpan;
use tracing_subscriber::Layer;

/// A single captured tracing event.
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
                    .map(|a| a.to_lowercase().starts_with(&lower) || a.to_lowercase().contains(&lower))
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
    /// This is destructive — events are consumed. Intended for `save_trace()`.
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

/// Visitor that extracts the message string and collects other fields.
struct FieldVisitor {
    message: String,
    fields: serde_json::Map<String, serde_json::Value>,
}

impl FieldVisitor {
    fn new() -> Self {
        Self {
            message: String::new(),
            fields: serde_json::Map::new(),
        }
    }
}

impl Visit for FieldVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::String(format!("{:?}", value)),
            );
        }
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::Number(value.into()),
            );
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::Number(value.into()),
            );
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "message" {
            self.message = value.to_string();
        } else {
            self.fields.insert(
                field.name().to_string(),
                serde_json::Value::String(value.to_string()),
            );
        }
    }
}

/// A tracing Layer that captures events into an EventStore.
pub struct DashboardLayer {
    store: std::sync::Arc<EventStore>,
}

impl DashboardLayer {
    pub fn new(store: std::sync::Arc<EventStore>) -> Self {
        Self { store }
    }
}

impl<S> Layer<S> for DashboardLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(&self, event: &tracing::Event<'_>, ctx: Context<'_, S>) {
        let mut visitor = FieldVisitor::new();
        event.record(&mut visitor);

        // Walk span context to find worker_id and actor_addr
        let mut worker_id = None;
        let mut actor_addr = None;
        if let Some(scope) = ctx.event_scope(event) {
            for span in scope {
                let exts = span.extensions();
                if worker_id.is_none() {
                    if let Some(wid) = exts.get::<WorkerIdField>() {
                        worker_id = Some(wid.0);
                    }
                }
                if actor_addr.is_none() {
                    if let Some(aa) = exts.get::<ActorAddrField>() {
                        actor_addr = Some(aa.0.clone());
                    }
                }
                if worker_id.is_some() && actor_addr.is_some() {
                    break;
                }
            }
        }

        // Also check if worker_id or actor_addr was a field on the event itself
        if worker_id.is_none() {
            if let Some(serde_json::Value::Number(n)) = visitor.fields.get("worker_id") {
                worker_id = n.as_u64().map(|v| v as usize);
            }
        }
        if actor_addr.is_none() {
            if let Some(serde_json::Value::String(s)) = visitor.fields.get("actor_addr") {
                actor_addr = Some(s.clone());
            }
        }

        let dashboard_event = DashboardEvent {
            seq: 0, // filled by push()
            timestamp_ms: now_ms(),
            level: event.metadata().level().to_string(),
            message: visitor.message,
            worker_id,
            actor_addr,
            fields: visitor.fields,
        };

        self.store.push(dashboard_event);
    }

    fn on_new_span(&self, attrs: &span::Attributes<'_>, id: &span::Id, ctx: Context<'_, S>) {
        // Extract worker_id and actor_addr from span fields and store in extensions
        let mut visitor = FieldVisitor::new();
        attrs.record(&mut visitor);

        if let Some(span) = ctx.span(id) {
            if let Some(serde_json::Value::Number(n)) = visitor.fields.get("worker_id") {
                if let Some(wid) = n.as_u64() {
                    span.extensions_mut().insert(WorkerIdField(wid as usize));
                }
            }
            if let Some(serde_json::Value::String(s)) = visitor.fields.get("actor_addr") {
                span.extensions_mut().insert(ActorAddrField(s.clone()));
            }
        }
    }
}

/// Stored in span extensions to propagate worker_id to child events.
struct WorkerIdField(usize);

/// Stored in span extensions to propagate actor_addr to child events.
struct ActorAddrField(String);
