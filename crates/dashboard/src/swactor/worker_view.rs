use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use telemetry::frame::{Frame, StreamId};
use parking_lot::RwLock;
use serde::Serialize;
use serde_json::{Value, json};

use crate::swactor::worker_page::WORKER_HTML;
use crate::swactor::{RUNTIME_ACTORS, RUNTIME_STATS, RUNTIME_WORKERS};
use crate::view::DashboardView;
use crate::{FrameEvent, StreamEvent};

const CHANNELS: &[&str] = &[RUNTIME_STATS, RUNTIME_WORKERS, RUNTIME_ACTORS];
const HISTORY_CAP: usize = 512;
const HISTORY_MIN_INTERVAL: Duration = Duration::from_millis(250);
const LIVE_TTL: Duration = Duration::from_secs(8);

#[derive(Default)]
pub struct SwactorWorkerView {
    state: RwLock<WorkerViewState>,
}

#[derive(Default)]
struct WorkerViewState {
    runtimes: BTreeMap<String, RuntimeState>,
}

struct RuntimeState {
    stream: StreamEvent,
    last_seen: Instant,
    summary: RuntimeSummary,
    aggregate: Option<WorkerState>,
    workers: BTreeMap<u32, WorkerState>,
    actors: BTreeMap<String, ActorState>,
    history: VecDeque<HistorySample>,
}

#[derive(Default)]
struct RuntimeSummary {
    num_workers: Option<u32>,
    uptime_ms: Option<u64>,
    actors_live: Option<u32>,
    mailbox_depth: Option<u32>,
    scheduled_tasks: Option<u32>,
}

#[derive(Clone)]
struct WorkerState {
    id: Option<u32>,
    actor_count: u32,
    mailbox_depth: u32,
    messages_processed: u64,
    local_sends: u64,
    cross_sends: u64,
    inbox_sends: u64,
    type_mismatches: u64,
    panics: u64,
    messages_dropped: u64,
    restarts: u64,
    stops: u64,
    tick_p50_us: u64,
    msg_per_sec: f64,
    local_per_sec: f64,
    cross_per_sec: f64,
    inbox_per_sec: f64,
    last_update: Option<Instant>,
}

impl WorkerState {
    fn new(id: Option<u32>) -> Self {
        Self {
            id,
            actor_count: 0,
            mailbox_depth: 0,
            messages_processed: 0,
            local_sends: 0,
            cross_sends: 0,
            inbox_sends: 0,
            type_mismatches: 0,
            panics: 0,
            messages_dropped: 0,
            restarts: 0,
            stops: 0,
            tick_p50_us: 0,
            msg_per_sec: 0.0,
            local_per_sec: 0.0,
            cross_per_sec: 0.0,
            inbox_per_sec: 0.0,
            last_update: None,
        }
    }

    fn apply_json(&mut self, value: &Value, now: Instant) {
        let elapsed = self
            .last_update
            .map(|then| now.duration_since(then).as_secs_f64())
            .unwrap_or(0.0);

        if let Some(id) = u32_field(value, &["id", "worker_id"]) {
            self.id = Some(id);
        }
        assign_u32(
            &mut self.actor_count,
            value,
            &["num_actors", "actor_count", "actors_live"],
        );
        assign_u32(
            &mut self.mailbox_depth,
            value,
            &["mailbox_depth", "total_mailbox_depth"],
        );
        assign_u64_rate(
            &mut self.messages_processed,
            &mut self.msg_per_sec,
            value,
            &["messages_processed", "processed"],
            elapsed,
        );
        assign_u64_rate(
            &mut self.local_sends,
            &mut self.local_per_sec,
            value,
            &["local_sends"],
            elapsed,
        );
        assign_u64_rate(
            &mut self.cross_sends,
            &mut self.cross_per_sec,
            value,
            &["cross_sends"],
            elapsed,
        );
        assign_u64_rate(
            &mut self.inbox_sends,
            &mut self.inbox_per_sec,
            value,
            &["inbox_sends"],
            elapsed,
        );
        assign_u64(&mut self.type_mismatches, value, &["type_mismatches"]);
        assign_u64(&mut self.panics, value, &["panics"]);
        assign_u64(
            &mut self.messages_dropped,
            value,
            &["messages_dropped", "dropped"],
        );
        assign_u64(&mut self.restarts, value, &["restarts"]);
        assign_u64(&mut self.stops, value, &["stops"]);
        assign_u64(&mut self.tick_p50_us, value, &["tick_p50_us", "tick_p50"]);
        self.last_update = Some(now);
    }
}

#[derive(Clone)]
struct ActorState {
    address: String,
    name: Option<String>,
    worker_id: Option<u32>,
    mailbox_depth: u32,
    messages_processed: u64,
    msg_per_sec: f64,
    last_msg_type: Option<String>,
    poisoned: bool,
    message_type_counts: Vec<(String, u64)>,
    last_update: Option<Instant>,
}

impl ActorState {
    fn new(address: String) -> Self {
        Self {
            address,
            name: None,
            worker_id: None,
            mailbox_depth: 0,
            messages_processed: 0,
            msg_per_sec: 0.0,
            last_msg_type: None,
            poisoned: false,
            message_type_counts: Vec::new(),
            last_update: None,
        }
    }

    fn apply_json(&mut self, value: &Value, now: Instant) {
        let elapsed = self
            .last_update
            .map(|then| now.duration_since(then).as_secs_f64())
            .unwrap_or(0.0);
        if let Some(name) = string_field(value, &["name"]).filter(|name| !name.is_empty()) {
            self.name = Some(name);
        }
        if let Some(worker_id) = u32_field(value, &["worker_id", "worker"]) {
            self.worker_id = Some(worker_id);
        }
        assign_u32(&mut self.mailbox_depth, value, &["mailbox_depth", "queued"]);
        assign_u64_rate(
            &mut self.messages_processed,
            &mut self.msg_per_sec,
            value,
            &["messages_processed", "processed"],
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
    }
}

struct HistorySample {
    at: Instant,
    mailbox_depth: u32,
    msg_per_sec: f64,
    tick_p50_us: u64,
}

impl RuntimeState {
    fn new(stream: StreamEvent, now: Instant) -> Self {
        Self {
            stream,
            last_seen: now,
            summary: RuntimeSummary::default(),
            aggregate: None,
            workers: BTreeMap::new(),
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
            RUNTIME_STATS => self.apply_runtime_stats(&value, now),
            RUNTIME_WORKERS => self.apply_workers(&value, now),
            RUNTIME_ACTORS => self.apply_actors(&value, now),
            _ => {}
        }
        self.push_history(now);
    }

    fn apply_runtime_stats(&mut self, value: &Value, now: Instant) {
        if let Some(num_workers) = u32_field(value, &["num_workers", "workers_live"]) {
            self.summary.num_workers = Some(num_workers);
        }
        if let Some(uptime_ms) = u64_field(value, &["uptime_ms"]) {
            self.summary.uptime_ms = Some(uptime_ms);
        }
        if let Some(actors_live) = u32_field(value, &["actors_live", "num_actors", "actor_count"]) {
            self.summary.actors_live = Some(actors_live);
        }
        if let Some(mailbox_depth) = u32_field(value, &["mailbox_depth", "total_mailbox_depth"]) {
            self.summary.mailbox_depth = Some(mailbox_depth);
        }
        if let Some(scheduled_tasks) = u32_field(value, &["scheduled_tasks"]) {
            self.summary.scheduled_tasks = Some(scheduled_tasks);
        }

        if let Some(workers) = value.get("workers").and_then(Value::as_array) {
            for worker in workers {
                self.apply_worker(worker, now);
            }
        }
        if let Some(actors) = value.get("actors").and_then(Value::as_array) {
            self.summary.actors_live = Some(actors.len().min(u32::MAX as usize) as u32);
            for actor in actors {
                self.apply_actor_mapping(actor);
            }
        }
        if let Some(actors) = value.get("actor_details").and_then(Value::as_array) {
            for actor in actors {
                self.apply_actor(actor, now);
            }
        }
        if let Some(ticks) = value.get("tick_timings").and_then(Value::as_array) {
            self.apply_tick_timings(ticks);
        }
    }

    fn apply_workers(&mut self, value: &Value, now: Instant) {
        if let Some(workers) = value.get("workers").and_then(Value::as_array) {
            for worker in workers {
                self.apply_worker(worker, now);
            }
            return;
        }
        if value.get("id").is_some() || value.get("worker_id").is_some() {
            self.apply_worker(value, now);
            return;
        }
        let worker = self.aggregate.get_or_insert_with(|| WorkerState::new(None));
        worker.apply_json(value, now);
        if let Some(num_workers) = u32_field(value, &["num_workers"]) {
            self.summary.num_workers = Some(num_workers);
        }
        if let Some(scheduled_tasks) = u32_field(value, &["scheduled_tasks"]) {
            self.summary.scheduled_tasks = Some(scheduled_tasks);
        }
    }

    fn apply_actors(&mut self, value: &Value, now: Instant) {
        let Some(actors) = value.get("actors").and_then(Value::as_array) else {
            self.apply_actor(value, now);
            return;
        };
        self.summary.actors_live = Some(actors.len().min(u32::MAX as usize) as u32);
        for actor in actors {
            self.apply_actor(actor, now);
        }
    }

    fn apply_worker(&mut self, value: &Value, now: Instant) {
        let Some(id) = u32_field(value, &["id", "worker_id"]) else {
            let worker = self.aggregate.get_or_insert_with(|| WorkerState::new(None));
            worker.apply_json(value, now);
            return;
        };
        self.workers
            .entry(id)
            .or_insert_with(|| WorkerState::new(Some(id)))
            .apply_json(value, now);
    }

    fn apply_actor(&mut self, value: &Value, now: Instant) {
        let Some(address) = string_field(value, &["address", "addr", "actor_addr"]) else {
            return;
        };
        self.actors
            .entry(address.clone())
            .or_insert_with(|| ActorState::new(address))
            .apply_json(value, now);
    }

    fn apply_actor_mapping(&mut self, value: &Value) {
        if let Some(items) = value.as_array()
            && items.len() >= 2
        {
            let Some(address) = value_to_string(&items[0]) else {
                return;
            };
            let Some(worker_id) = value_to_u32(&items[1]) else {
                return;
            };
            self.actors
                .entry(address.clone())
                .or_insert_with(|| ActorState::new(address))
                .worker_id = Some(worker_id);
            return;
        }
        let Some(address) = string_field(value, &["address", "addr", "actor_addr"]) else {
            return;
        };
        if let Some(worker_id) = u32_field(value, &["worker_id", "worker"]) {
            self.actors
                .entry(address.clone())
                .or_insert_with(|| ActorState::new(address))
                .worker_id = Some(worker_id);
        }
    }

    fn apply_tick_timings(&mut self, ticks: &[Value]) {
        for (worker_id, worker_ticks) in ticks.iter().enumerate() {
            let Some(series) = worker_ticks.as_array() else {
                continue;
            };
            let mut totals: Vec<u64> = series
                .iter()
                .filter_map(|tick| tick.get("phase_us").and_then(Value::as_array))
                .map(|phases| phases.iter().filter_map(Value::as_u64).sum())
                .collect();
            if totals.is_empty() {
                continue;
            }
            totals.sort_unstable();
            let p50 = totals[totals.len() / 2];
            self.workers
                .entry(worker_id as u32)
                .or_insert_with(|| WorkerState::new(Some(worker_id as u32)))
                .tick_p50_us = p50;
        }
    }

    fn push_history(&mut self, now: Instant) {
        let totals = self.totals();
        if let Some(last) = self.history.back_mut()
            && now.duration_since(last.at) < HISTORY_MIN_INTERVAL
        {
            last.mailbox_depth = totals.mailbox_depth;
            last.msg_per_sec = totals.msg_per_sec;
            last.tick_p50_us = totals.tick_p50_us;
            return;
        }
        if self.history.len() == HISTORY_CAP {
            self.history.pop_front();
        }
        self.history.push_back(HistorySample {
            at: now,
            mailbox_depth: totals.mailbox_depth,
            msg_per_sec: totals.msg_per_sec,
            tick_p50_us: totals.tick_p50_us,
        });
    }

    fn totals(&self) -> Totals {
        let mut totals = Totals::default();
        totals.workers = self
            .summary
            .num_workers
            .unwrap_or_else(|| self.workers.len().min(u32::MAX as usize) as u32);
        totals.actors = self
            .summary
            .actors_live
            .unwrap_or_else(|| self.actors.len().min(u32::MAX as usize) as u32);
        if let Some(aggregate) = &self.aggregate {
            totals.mailbox_depth = aggregate.mailbox_depth;
            totals.msg_per_sec = aggregate.msg_per_sec;
            totals.local_per_sec = aggregate.local_per_sec;
            totals.cross_per_sec = aggregate.cross_per_sec;
            totals.inbox_per_sec = aggregate.inbox_per_sec;
            totals.messages_dropped = aggregate.messages_dropped;
            totals.panics = aggregate.panics;
            totals.tick_p50_us = aggregate.tick_p50_us;
            return totals;
        }
        for worker in self.workers.values() {
            totals.mailbox_depth = totals.mailbox_depth.saturating_add(worker.mailbox_depth);
            totals.msg_per_sec += worker.msg_per_sec;
            totals.local_per_sec += worker.local_per_sec;
            totals.cross_per_sec += worker.cross_per_sec;
            totals.inbox_per_sec += worker.inbox_per_sec;
            totals.messages_dropped = totals
                .messages_dropped
                .saturating_add(worker.messages_dropped);
            totals.panics = totals.panics.saturating_add(worker.panics);
            totals.tick_p50_us = totals.tick_p50_us.max(worker.tick_p50_us);
        }
        if totals.mailbox_depth == 0 {
            totals.mailbox_depth = self.summary.mailbox_depth.unwrap_or(0);
        }
        totals
    }
}

#[derive(Default, Serialize)]
struct Totals {
    workers: u32,
    actors: u32,
    mailbox_depth: u32,
    msg_per_sec: f64,
    local_per_sec: f64,
    cross_per_sec: f64,
    inbox_per_sec: f64,
    messages_dropped: u64,
    panics: u64,
    tick_p50_us: u64,
}

#[derive(Serialize)]
struct WorkerViewSnapshot {
    runtimes: Vec<RuntimeSnapshot>,
}

#[derive(Serialize)]
struct RuntimeSnapshot {
    stream: StreamSnapshot,
    live: bool,
    last_seen_ms_ago: u64,
    summary: SummarySnapshot,
    totals: Totals,
    workers: Vec<WorkerSnapshot>,
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
    num_workers: Option<u32>,
    uptime_ms: Option<u64>,
    actors_live: Option<u32>,
    mailbox_depth: Option<u32>,
    scheduled_tasks: Option<u32>,
}

#[derive(Serialize)]
struct WorkerSnapshot {
    id: Option<u32>,
    actor_count: u32,
    mailbox_depth: u32,
    messages_processed: u64,
    local_sends: u64,
    cross_sends: u64,
    inbox_sends: u64,
    type_mismatches: u64,
    panics: u64,
    messages_dropped: u64,
    restarts: u64,
    stops: u64,
    tick_p50_us: u64,
    msg_per_sec: f64,
    local_per_sec: f64,
    cross_per_sec: f64,
    inbox_per_sec: f64,
}

impl WorkerSnapshot {
    fn from_state(state: &WorkerState, actor_count: u32) -> Self {
        Self {
            id: state.id,
            actor_count: state.actor_count.max(actor_count),
            mailbox_depth: state.mailbox_depth,
            messages_processed: state.messages_processed,
            local_sends: state.local_sends,
            cross_sends: state.cross_sends,
            inbox_sends: state.inbox_sends,
            type_mismatches: state.type_mismatches,
            panics: state.panics,
            messages_dropped: state.messages_dropped,
            restarts: state.restarts,
            stops: state.stops,
            tick_p50_us: state.tick_p50_us,
            msg_per_sec: state.msg_per_sec,
            local_per_sec: state.local_per_sec,
            cross_per_sec: state.cross_per_sec,
            inbox_per_sec: state.inbox_per_sec,
        }
    }
}

#[derive(Serialize)]
struct ActorSnapshot {
    address: String,
    name: Option<String>,
    worker_id: Option<u32>,
    mailbox_depth: u32,
    messages_processed: u64,
    msg_per_sec: f64,
    last_msg_type: Option<String>,
    poisoned: bool,
    message_type_counts: Vec<(String, u64)>,
}

#[derive(Serialize)]
struct HistorySnapshot {
    ms_ago: u64,
    mailbox_depth: u32,
    msg_per_sec: f64,
    tick_p50_us: u64,
}

impl DashboardView for SwactorWorkerView {
    fn id(&self) -> &'static str {
        "swactor-workers"
    }

    fn title(&self) -> &'static str {
        "Swactor workers"
    }

    fn path(&self) -> &'static str {
        "swactor/workers"
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
        let snapshot = WorkerViewSnapshot {
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
        Some(WORKER_HTML)
    }
}

fn runtime_snapshot(runtime: &RuntimeState, now: Instant) -> RuntimeSnapshot {
    let mut worker_actor_counts: BTreeMap<Option<u32>, u32> = BTreeMap::new();
    for actor in runtime.actors.values() {
        *worker_actor_counts.entry(actor.worker_id).or_default() += 1;
    }

    let mut workers = Vec::new();
    if let Some(aggregate) = &runtime.aggregate {
        workers.push(WorkerSnapshot::from_state(
            aggregate,
            worker_actor_counts.get(&None).copied().unwrap_or_default(),
        ));
    }
    workers.extend(runtime.workers.iter().map(|(id, worker)| {
        WorkerSnapshot::from_state(
            worker,
            worker_actor_counts
                .get(&Some(*id))
                .copied()
                .unwrap_or_default(),
        )
    }));
    for id in worker_actor_counts.keys().flatten() {
        if !runtime.workers.contains_key(id) {
            let mut worker = WorkerState::new(Some(*id));
            worker.actor_count = worker_actor_counts
                .get(&Some(*id))
                .copied()
                .unwrap_or_default();
            workers.push(WorkerSnapshot::from_state(&worker, worker.actor_count));
        }
    }

    RuntimeSnapshot {
        stream: StreamSnapshot {
            key: stream_key(&runtime.stream),
            node: runtime.stream.node.clone(),
            life: runtime.stream.life,
        },
        live: now.duration_since(runtime.last_seen) <= LIVE_TTL,
        last_seen_ms_ago: now.duration_since(runtime.last_seen).as_millis() as u64,
        summary: SummarySnapshot {
            num_workers: runtime.summary.num_workers,
            uptime_ms: runtime.summary.uptime_ms,
            actors_live: runtime.summary.actors_live,
            mailbox_depth: runtime.summary.mailbox_depth,
            scheduled_tasks: runtime.summary.scheduled_tasks,
        },
        totals: runtime.totals(),
        workers,
        actors: runtime
            .actors
            .values()
            .map(|actor| ActorSnapshot {
                address: actor.address.clone(),
                name: actor.name.clone(),
                worker_id: actor.worker_id,
                mailbox_depth: actor.mailbox_depth,
                messages_processed: actor.messages_processed,
                msg_per_sec: actor.msg_per_sec,
                last_msg_type: actor.last_msg_type.clone(),
                poisoned: actor.poisoned,
                message_type_counts: actor.message_type_counts.clone(),
            })
            .collect(),
        history: runtime
            .history
            .iter()
            .map(|sample| HistorySnapshot {
                ms_ago: now.duration_since(sample.at).as_millis() as u64,
                mailbox_depth: sample.mailbox_depth,
                msg_per_sec: sample.msg_per_sec,
                tick_p50_us: sample.tick_p50_us,
            })
            .collect(),
    }
}

fn assign_u32(slot: &mut u32, value: &Value, names: &[&str]) {
    if let Some(v) = u32_field(value, names) {
        *slot = v;
    }
}

fn assign_u64(slot: &mut u64, value: &Value, names: &[&str]) {
    if let Some(v) = u64_field(value, names) {
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
