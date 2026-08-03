//! Actor-centric read-only view over `runtime.actors` / `runtime.stats` frames.
//!
//! Pure frame consumer: it folds incoming per-actor snapshots into local state
//! and exposes JSON + HTML. It sends nothing back to observed runtimes. Parsing
//! is intentionally tolerant of publisher shape — a frame is a flat record that
//! may come from the per-worker `DatastreamStatsHook` envelope
//! (`{worker_id, actors:[...]}`) or from a process that publishes a merged
//! `{actors:[...]}` payload (the dashboard dummy node, app runtimes). Fields the
//! publisher includes (name, worker_id, lifecycle flags) are displayed; ones it
//! omits are left blank rather than fabricated.
//!
//! Reliably on the frame today: address, mailbox depth, throughput, last
//! message, poisoned flag, and the message-type diet. `worker_id` and `name`
//! appear when the publisher sends them. Finer lifecycle granularity
//! (new/suspended/stopping), actor Rust type, and spawn age are not on the
//! current frame and are therefore not shown — enriching the feed is a separate
//! core concern, not a dashboard one.
//!
//! Two pages share one data model: the fused overview+roster
//! (`/view/swactor/actor-overview`) and the per-actor dossier
//! (`/view/swactor/actor-dossier`). Each is a self-contained `DashboardView`
//! instance ingesting the same channels.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use datastream::frame::{Frame, StreamId};
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{Value, json};

use crate::swactor::{RUNTIME_ACTORS, RUNTIME_STATS};
use crate::view::DashboardView;
use crate::{FrameEvent, StreamEvent};

const CHANNELS: &[&str] = &[RUNTIME_ACTORS, RUNTIME_STATS];
const HISTORY_CAP: usize = 512;
const HISTORY_MIN_INTERVAL: Duration = Duration::from_millis(250);
const PER_ACTOR_HISTORY_CAP: usize = 120;
const GROWTH_WINDOW: usize = 12;
const LIVE_TTL: Duration = Duration::from_secs(8);

/// Read-only actor panel. One instance per served page so each page owns its
/// state independently; both ingest the same frames.
pub struct ActorPanelView {
    id: &'static str,
    title: &'static str,
    path: &'static str,
    html: &'static str,
    state: RwLock<PanelState>,
}

impl ActorPanelView {
    /// Fused overview + roster at `/view/swactor/actor-overview`.
    pub fn overview() -> Self {
        Self::new(
            "swactor-actor-overview",
            "Actor overview",
            "swactor/actor-overview",
            include_str!("actor_overview.html"),
        )
    }

    /// Per-actor dossier at `/view/swactor/actor-dossier`.
    pub fn dossier() -> Self {
        Self::new(
            "swactor-actor-dossier",
            "Actor dossier",
            "swactor/actor-dossier",
            include_str!("actor_dossier.html"),
        )
    }

    fn new(id: &'static str, title: &'static str, path: &'static str, html: &'static str) -> Self {
        Self {
            id,
            title,
            path,
            html,
            state: RwLock::new(PanelState::default()),
        }
    }
}

#[derive(Default)]
struct PanelState {
    runtimes: BTreeMap<String, RuntimeState>,
}

struct RuntimeState {
    stream: StreamEvent,
    last_seen: Instant,
    num_workers: Option<u32>,
    uptime_ms: Option<u64>,
    actors: BTreeMap<String, ActorState>,
    history: VecDeque<HistorySample>,
}

impl RuntimeState {
    fn new(stream: StreamEvent, now: Instant) -> Self {
        Self {
            stream,
            last_seen: now,
            num_workers: None,
            uptime_ms: None,
            actors: BTreeMap::new(),
            history: VecDeque::with_capacity(HISTORY_CAP),
        }
    }

    fn update(&mut self, channel: &str, payload: &[u8], now: Instant) {
        self.last_seen = now;
        let Ok(value) = serde_json::from_slice::<Value>(payload) else {
            return;
        };
        match channel {
            RUNTIME_ACTORS => self.apply_actors(&value, now),
            RUNTIME_STATS => self.apply_stats(&value, now),
            _ => {}
        }
        self.push_history(now);
    }

    fn apply_actors(&mut self, value: &Value, now: Instant) {
        // Per-worker `worker_id` wrapper (DatastreamStatsHook shape) is the
        // default placement for actors that do not carry one inline.
        let wrapper_worker = u32_field(value, &["worker_id", "worker"]);
        if let Some(actors) = value.get("actors").and_then(Value::as_array) {
            for actor in actors {
                self.apply_actor(actor, now, wrapper_worker);
            }
            return;
        }
        // A bare actor object per frame (no envelope).
        self.apply_actor(value, now, wrapper_worker);
    }

    fn apply_actor(&mut self, value: &Value, now: Instant, default_worker: Option<u32>) {
        let Some(address) = string_field(value, &["address", "addr", "actor_addr"]) else {
            return;
        };
        let actor = self
            .actors
            .entry(address.clone())
            .or_insert_with(|| ActorState::new(address));
        actor.apply_json(value, now, default_worker);
    }

    fn apply_stats(&mut self, value: &Value, now: Instant) {
        if let Some(num_workers) = u32_field(value, &["num_workers", "workers_live"]) {
            self.num_workers = Some(num_workers);
        }
        if let Some(uptime_ms) = u64_field(value, &["uptime_ms"]) {
            self.uptime_ms = Some(uptime_ms);
        }
        // address -> worker_id mapping; runtime.stats `actors` is [[addr, wid]].
        if let Some(actors) = value.get("actors").and_then(Value::as_array) {
            for entry in actors {
                if let Some(items) = entry.as_array()
                    && items.len() >= 2
                    && let (Some(address), Some(worker_id)) =
                        (value_to_string(&items[0]), value_to_u32(&items[1]))
                {
                    self.actors
                        .entry(address.clone())
                        .or_insert_with(|| ActorState::new(address))
                        .worker_id = Some(worker_id);
                }
            }
        }
        // Some publishers carry full per-actor detail under `actor_details`.
        if let Some(details) = value.get("actor_details").and_then(Value::as_array) {
            for actor in details {
                self.apply_actor(actor, now, None);
            }
        }
    }

    fn push_history(&mut self, now: Instant) {
        let totals = self.totals();
        if let Some(last) = self.history.back_mut()
            && now.duration_since(last.at) < HISTORY_MIN_INTERVAL
        {
            last.mailbox_depth = totals.mailbox_depth;
            last.msg_per_sec = totals.msg_per_sec;
            return;
        }
        if self.history.len() == HISTORY_CAP {
            self.history.pop_front();
        }
        self.history.push_back(HistorySample {
            at: now,
            mailbox_depth: totals.mailbox_depth,
            msg_per_sec: totals.msg_per_sec,
        });
    }

    fn totals(&self) -> Totals {
        let mut totals = Totals::default();
        totals.actors = self.actors.len().min(u32::MAX as usize) as u32;
        for actor in self.actors.values() {
            totals.mailbox_depth = totals.mailbox_depth.saturating_add(actor.mailbox_depth);
            totals.msg_per_sec += actor.msg_per_sec;
            if actor.poisoned {
                totals.poisoned = totals.poisoned.saturating_add(1);
            }
        }
        totals
    }
}

#[derive(Clone)]
struct ActorState {
    address: String,
    name: Option<String>,
    actor_type: Option<String>,
    message_type: Option<String>,
    worker_id: Option<u32>,
    mailbox_depth: u32,
    mailbox_growth: f64,
    messages_processed: u64,
    msg_per_sec: f64,
    last_msg_type: Option<String>,
    poisoned: bool,
    message_type_counts: Vec<(String, u64)>,
    history: VecDeque<ActorHistorySample>,
    last_update: Option<Instant>,
}

impl ActorState {
    fn new(address: String) -> Self {
        Self {
            address,
            name: None,
            actor_type: None,
            message_type: None,
            worker_id: None,
            mailbox_depth: 0,
            mailbox_growth: 0.0,
            messages_processed: 0,
            msg_per_sec: 0.0,
            last_msg_type: None,
            poisoned: false,
            message_type_counts: Vec::new(),
            history: VecDeque::new(),
            last_update: None,
        }
    }

    fn apply_json(&mut self, value: &Value, now: Instant, default_worker: Option<u32>) {
        let elapsed = self
            .last_update
            .map(|then| now.duration_since(then).as_secs_f64())
            .unwrap_or(0.0);
        if let Some(name) = string_field(value, &["name"]).filter(|name| !name.is_empty()) {
            self.name = Some(name);
        }
        if let Some(actor_type) = string_field(value, &["actor_type"]).filter(|t| !t.is_empty()) {
            self.actor_type = Some(actor_type);
        }
        if let Some(message_type) = string_field(value, &["message_type"]).filter(|t| !t.is_empty()) {
            self.message_type = Some(message_type);
        }
        if let Some(worker_id) = u32_field(value, &["worker_id", "worker"]) {
            self.worker_id = Some(worker_id);
        } else if self.worker_id.is_none() {
            self.worker_id = default_worker;
        }
        assign_u32(&mut self.mailbox_depth, value, &["mailbox_depth", "queued"]);
        assign_u64_rate(
            &mut self.messages_processed,
            &mut self.msg_per_sec,
            value,
            &["messages_processed", "processed", "messages_handled"],
            elapsed,
        );
        if let Some(last) = string_field(
            value,
            &["last_msg_type", "last_message", "last_message_type"],
        )
        .filter(|last| !last.is_empty())
        {
            self.last_msg_type = Some(last);
        }
        if let Some(poisoned) = value.get("poisoned").and_then(Value::as_bool) {
            self.poisoned = poisoned;
        }
        if let Some(counts) = parse_message_type_counts(value.get("message_type_counts")) {
            self.message_type_counts = counts;
        }
        self.last_update = Some(now);
        self.push_history(now);
        self.recompute_growth();
    }

    fn push_history(&mut self, now: Instant) {
        if let Some(last) = self.history.back_mut()
            && now.duration_since(last.at) < HISTORY_MIN_INTERVAL
        {
            last.mailbox_depth = self.mailbox_depth;
            last.msg_per_sec = self.msg_per_sec;
            return;
        }
        if self.history.len() == PER_ACTOR_HISTORY_CAP {
            self.history.pop_front();
        }
        self.history.push_back(ActorHistorySample {
            at: now,
            mailbox_depth: self.mailbox_depth,
            msg_per_sec: self.msg_per_sec,
        });
    }

    /// Mailbox depth/s over the recent window, for trend colouring.
    fn recompute_growth(&mut self) {
        let n = self.history.len();
        if n < 2 {
            self.mailbox_growth = 0.0;
            return;
        }
        let start = n - n.min(GROWTH_WINDOW);
        let first = &self.history[start];
        let last = &self.history[n - 1];
        let span = last.at.duration_since(first.at).as_secs_f64();
        self.mailbox_growth = if span > 0.0 {
            (last.mailbox_depth as f64 - first.mailbox_depth as f64) / span
        } else {
            0.0
        };
    }
}

#[derive(Clone, Copy)]
struct HistorySample {
    at: Instant,
    mailbox_depth: u32,
    msg_per_sec: f64,
}

#[derive(Clone, Copy)]
struct ActorHistorySample {
    at: Instant,
    mailbox_depth: u32,
    msg_per_sec: f64,
}

#[derive(Default)]
struct Totals {
    actors: u32,
    mailbox_depth: u32,
    msg_per_sec: f64,
    poisoned: u32,
}

#[derive(Serialize)]
struct PanelSnapshot {
    runtimes: Vec<RuntimeSnapshot>,
}

#[derive(Serialize)]
struct RuntimeSnapshot {
    stream: StreamSnapshot,
    live: bool,
    last_seen_ms_ago: u64,
    summary: SummarySnapshot,
    actors: Vec<ActorSnapshot>,
    history: Vec<HistorySnapshot>,
}

#[derive(Serialize)]
struct StreamSnapshot {
    key: String,
    node: String,
    life: u64,
}

#[derive(Serialize)]
struct SummarySnapshot {
    actors: u32,
    msg_per_sec: f64,
    mailbox_depth: u32,
    poisoned: u32,
    uptime_ms: Option<u64>,
    num_workers: Option<u32>,
}

#[derive(Serialize)]
struct ActorSnapshot {
    address: String,
    name: Option<String>,
    actor_type: Option<String>,
    message_type: Option<String>,
    worker_id: Option<u32>,
    /// Single derived display state. The frame carries only `poisoned`, so the
    /// granularity is poisoned | running until the feed is enriched.
    state: &'static str,
    mailbox_depth: u32,
    mailbox_growth: f64,
    messages_processed: u64,
    msg_per_sec: f64,
    last_msg_type: Option<String>,
    poisoned: bool,
    message_type_counts: Vec<MessageTypeCountSnapshot>,
    history: Vec<ActorHistorySnapshot>,
    last_seen_ms_ago: u64,
}

#[derive(Serialize)]
struct MessageTypeCountSnapshot {
    message_type: String,
    count: u64,
}

#[derive(Serialize)]
struct HistorySnapshot {
    ms_ago: u64,
    msg_per_sec: f64,
    mailbox_depth: u32,
}

#[derive(Serialize)]
struct ActorHistorySnapshot {
    ms_ago: u64,
    mailbox_depth: u32,
    msg_per_sec: f64,
}

impl DashboardView for ActorPanelView {
    fn id(&self) -> &'static str {
        self.id
    }

    fn title(&self) -> &'static str {
        self.title
    }

    fn path(&self) -> &'static str {
        self.path
    }

    fn channels(&self) -> &'static [&'static str] {
        CHANNELS
    }

    fn ingest(&self, _stream: &StreamId, _frame: &Frame, event: &FrameEvent) {
        let now = Instant::now();
        let mut state = self.state.write();
        let key = stream_key(&event.stream);
        state
            .runtimes
            .entry(key)
            .or_insert_with(|| RuntimeState::new(event.stream.clone(), now))
            .update(&event.channel, &event.payload, now);
    }

    fn snapshot_json(&self) -> Value {
        let now = Instant::now();
        let snapshot = PanelSnapshot {
            runtimes: self
                .state
                .read()
                .runtimes
                .values()
                .map(|runtime| runtime_snapshot(runtime, now))
                .collect(),
        };
        serde_json::to_value(snapshot).unwrap_or_else(|_| json!({ "runtimes": [] }))
    }

    fn html(&self) -> Option<&'static str> {
        Some(self.html)
    }
}

fn runtime_snapshot(runtime: &RuntimeState, now: Instant) -> RuntimeSnapshot {
    let totals = runtime.totals();
    RuntimeSnapshot {
        stream: StreamSnapshot {
            key: stream_key(&runtime.stream),
            node: runtime.stream.node.clone(),
            life: runtime.stream.life,
        },
        live: now.duration_since(runtime.last_seen) <= LIVE_TTL,
        last_seen_ms_ago: now.duration_since(runtime.last_seen).as_millis() as u64,
        summary: SummarySnapshot {
            actors: totals.actors,
            msg_per_sec: totals.msg_per_sec,
            mailbox_depth: totals.mailbox_depth,
            poisoned: totals.poisoned,
            uptime_ms: runtime.uptime_ms,
            num_workers: runtime.num_workers,
        },
        actors: runtime
            .actors
            .values()
            .map(|actor| ActorSnapshot {
                address: actor.address.clone(),
                name: actor.name.clone(),
                actor_type: actor.actor_type.clone(),
                message_type: actor.message_type.clone(),
                worker_id: actor.worker_id,
                state: if actor.poisoned { "poisoned" } else { "running" },
                mailbox_depth: actor.mailbox_depth,
                mailbox_growth: actor.mailbox_growth,
                messages_processed: actor.messages_processed,
                msg_per_sec: actor.msg_per_sec,
                last_msg_type: actor.last_msg_type.clone(),
                poisoned: actor.poisoned,
                message_type_counts: actor
                    .message_type_counts
                    .iter()
                    .map(|(message_type, count)| MessageTypeCountSnapshot {
                        message_type: message_type.clone(),
                        count: *count,
                    })
                    .collect(),
                history: actor
                    .history
                    .iter()
                    .map(|sample| ActorHistorySnapshot {
                        ms_ago: now.duration_since(sample.at).as_millis() as u64,
                        mailbox_depth: sample.mailbox_depth,
                        msg_per_sec: sample.msg_per_sec,
                    })
                    .collect(),
                last_seen_ms_ago: actor
                    .last_update
                    .map(|then| now.duration_since(then).as_millis() as u64)
                    .unwrap_or(0),
            })
            .collect(),
        history: runtime
            .history
            .iter()
            .map(|sample| HistorySnapshot {
                ms_ago: now.duration_since(sample.at).as_millis() as u64,
                msg_per_sec: sample.msg_per_sec,
                mailbox_depth: sample.mailbox_depth,
            })
            .collect(),
    }
}

// --- tolerant JSON helpers (publisher-shape-agnostic readers) ---------------

fn assign_u32(slot: &mut u32, value: &Value, names: &[&str]) {
    if let Some(v) = u32_field(value, names) {
        *slot = v;
    }
}

fn assign_u64_rate(slot: &mut u64, rate: &mut f64, value: &Value, names: &[&str], elapsed: f64) {
    if let Some(next) = u64_field(value, names) {
        if elapsed > 0.0 && next > *slot {
            *rate = (next - *slot) as f64 / elapsed;
        } else if next < *slot {
            *rate = 0.0;
        }
        *slot = next;
    }
}

fn u32_field(value: &Value, names: &[&str]) -> Option<u32> {
    u64_field(value, names).and_then(|v| u32::try_from(v).ok())
}

fn u64_field(value: &Value, names: &[&str]) -> Option<u64> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(value_to_u64))
}

fn string_field(value: &Value, names: &[&str]) -> Option<String> {
    names
        .iter()
        .find_map(|name| value.get(*name).and_then(value_to_string))
}

fn value_to_u32(value: &Value) -> Option<u32> {
    value_to_u64(value).and_then(|v| u32::try_from(v).ok())
}

fn value_to_u64(value: &Value) -> Option<u64> {
    value
        .as_u64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<u64>().ok()))
}

fn value_to_string(value: &Value) -> Option<String> {
    value.as_str().map(ToOwned::to_owned).or_else(|| {
        if value.is_null() {
            None
        } else {
            Some(value.to_string())
        }
    })
}

/// Accepts pair form `["Type", N]`, object form `{"ty": "Type", "count": N}`,
/// and map form `{"Type": N}`.
fn parse_message_type_counts(value: Option<&Value>) -> Option<Vec<(String, u64)>> {
    let value = value?;
    if let Some(items) = value.as_array() {
        let mut out = Vec::new();
        for item in items {
            if let Some(pair) = item.as_array()
                && pair.len() >= 2
                && let (Some(name), Some(count)) =
                    (value_to_string(&pair[0]), value_to_u64(&pair[1]))
            {
                out.push((name, count));
                continue;
            }
            if let Some(name) = string_field(item, &["ty", "message_type", "type", "name"])
                && let Some(count) = u64_field(item, &["count"])
            {
                out.push((name, count));
            }
        }
        return Some(out);
    }
    if let Some(map) = value.as_object() {
        let mut out: Vec<(String, u64)> = map
            .iter()
            .filter_map(|(name, count)| value_to_u64(count).map(|count| (name.clone(), count)))
            .collect();
        out.sort_by(|a, b| b.1.cmp(&a.1));
        return Some(out);
    }
    None
}

fn stream_key(stream: &StreamEvent) -> String {
    format!("{}#{}", stream.node, stream.life)
}
