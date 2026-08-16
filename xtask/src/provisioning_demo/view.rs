//! Reconciler dashboard view — API only.
//!
//! The snapshot/feed JSON at `/api/view/reconciler` feeds the merged Fleet
//! Control page (`/view/demo-control`), which fuses it with the process
//! table. No standalone page: `html()` is `None` and the view stays out of
//! the navbar while its API remains registered.

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;
use telemetry::frame::{Frame, StreamId};

use dashboard::view::DashboardView;
use dashboard::FrameEvent;

const EVENTS_CHANNEL: &str = "prov.reconciler.events";
const SNAPSHOT_CHANNEL: &str = "prov.reconciler.snapshot";
const FEED_CAP: usize = 250;

#[derive(Clone, Serialize)]
struct FeedLine {
    at_ms: u64,
    kind: String,
    node: String,
    detail: String,
}

#[derive(Serialize)]
struct ReconcilerSnapshot {
    ready: u64,
    desired: u64,
    generation: u64,
    converged: bool,
    age_ms: u64,
    nodes: Vec<Value>,
    feed: Vec<FeedLine>,
}

struct ViewState {
    header: Option<Value>,
    nodes: Vec<Value>,
    feed: VecDeque<FeedLine>,
    snapshot_at_ms: u64,
}

impl ViewState {
    fn push_feed(&mut self, line: FeedLine) {
        self.feed.push_back(line);
        while self.feed.len() > FEED_CAP {
            self.feed.pop_front();
        }
    }
}

pub struct ReconcilerDashboardView {
    state: Mutex<ViewState>,
}

impl Default for ReconcilerDashboardView {
    fn default() -> Self {
        Self {
            state: Mutex::new(ViewState {
                header: None,
                nodes: Vec::new(),
                feed: VecDeque::new(),
                snapshot_at_ms: 0,
            }),
        }
    }
}

fn unix_ms(now: SystemTime) -> u64 {
    now.duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

impl DashboardView for ReconcilerDashboardView {
    fn id(&self) -> &'static str {
        "reconciler"
    }

    fn title(&self) -> &'static str {
        "Provisioning Reconciler"
    }

    fn path(&self) -> &'static str {
        "reconciler"
    }

    fn channels(&self) -> &'static [&'static str] {
        &[EVENTS_CHANNEL, SNAPSHOT_CHANNEL]
    }

    fn show_in_nav(&self) -> bool {
        false
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, event: &FrameEvent) {
        let Ok(payload) = serde_json::from_slice::<Value>(&event.payload) else {
            return;
        };
        let mut state = self.state.lock();
        match event.channel.as_str() {
            SNAPSHOT_CHANNEL => {
                state.nodes = payload
                    .get("nodes")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                state.snapshot_at_ms = payload
                    .get("at_ms")
                    .and_then(Value::as_u64)
                    .unwrap_or_else(|| unix_ms(SystemTime::now()));
                state.header = Some(payload);
            }
            EVENTS_CHANNEL => {
                let line = FeedLine {
                    at_ms: payload
                        .get("at_ms")
                        .and_then(Value::as_u64)
                        .unwrap_or_default(),
                    kind: payload
                        .get("kind")
                        .and_then(Value::as_str)
                        .unwrap_or("event")
                        .to_owned(),
                    node: payload
                        .get("node")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                    detail: payload
                        .get("detail")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_owned(),
                };
                state.push_feed(line);
            }
            _ => {}
        }
    }

    fn snapshot_json(&self) -> Value {
        let state = self.state.lock();
        let header = state.header.clone().unwrap_or(Value::Null);
        let now = unix_ms(SystemTime::now());
        serde_json::to_value(ReconcilerSnapshot {
            ready: header.get("ready").and_then(Value::as_u64).unwrap_or(0),
            desired: header.get("desired").and_then(Value::as_u64).unwrap_or(0),
            generation: header
                .get("generation")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            converged: header
                .get("converged")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            age_ms: now.saturating_sub(state.snapshot_at_ms),
            nodes: state.nodes.clone(),
            feed: state.feed.iter().cloned().collect(),
        })
        .unwrap_or_else(|_| serde_json::json!({}))
    }

    fn html(&self) -> Option<&'static str> {
        None
    }
}

