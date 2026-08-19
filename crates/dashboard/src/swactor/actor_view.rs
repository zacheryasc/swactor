//! Actor-side folding for the fused control-plane view.
//!
//! Pure frame consumer: folds per-actor snapshots from `runtime.actors` /
//! `runtime.stats` frames into [`RuntimeState`] / [`ActorState`]. It sends
//! nothing back to observed runtimes. Parsing is intentionally tolerant of
//! publisher shape — a frame is a flat record that may come from the per-worker
//! `TelemetryStatsHook` envelope (`{worker_id, actors:[...]}`) or from a
//! process that publishes a merged `{actors:[...]}` payload. Fields the
//! publisher includes are displayed; omitted ones stay blank rather than
//! fabricated.
//!
//! Reliably on the frame: address, mailbox depth, throughput, last message,
//! poisoned flag, the message-type diet, and the actor/message Rust type names
//! (`std::any::type_name` strings published by the stats hook). The Rust type
//! is the actor's display name; the address is the unique key.
//!
//! Message *history* is folded here from consecutive frames: a jump in
//! `messages_processed` between frames yields one receipt carrying
//! `last_msg_type`; the messages hidden inside the jump are counted as sampled
//! out. Receipts are bounded and interval-spaced so noisy actors cannot flood
//! the page.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use serde_json::Value;

use crate::swactor::{RUNTIME_ACTORS, RUNTIME_STATS};

const HISTORY_CAP: usize = 512;
const HISTORY_MIN_INTERVAL: Duration = Duration::from_millis(250);
const PER_ACTOR_HISTORY_CAP: usize = 120;
const GROWTH_WINDOW: usize = 12;
/// Sampled message-receipt ring bound (view side).
pub(crate) const RECEIPT_CAP: usize = 16;
/// Minimum spacing between receipts; bounds ring churn for noisy actors.
pub(crate) const RECEIPT_MIN_INTERVAL: Duration = Duration::from_millis(250);

/// One sampled message receipt folded from a `messages_processed` jump.
#[derive(Clone)]
pub(crate) struct ActorReceipt {
    pub(crate) at: Instant,
    /// Rust type name of the last message handled in the jump.
    pub(crate) ty: String,
    /// Messages folded into this receipt whose types were not visible.
    pub(crate) folded: u64,
}

pub(crate) struct RuntimeState {
    pub(crate) last_seen: Instant,
    pub(crate) num_workers: Option<u32>,
    pub(crate) uptime_ms: Option<u64>,
    pub(crate) actors: BTreeMap<String, ActorState>,
    pub(crate) history: VecDeque<HistorySample>,
    actor_snapshot_generation: u64,
}

impl RuntimeState {
    pub(crate) fn new(now: Instant) -> Self {
        Self {
            last_seen: now,
            num_workers: None,
            uptime_ms: None,
            actors: BTreeMap::new(),
            history: VecDeque::with_capacity(HISTORY_CAP),
            actor_snapshot_generation: 0,
        }
    }

    pub(crate) fn update(&mut self, channel: &str, payload: &[u8], now: Instant) {
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
        // A wrapped actor list is a complete snapshot for one worker. A list
        // without a worker is a complete merged-runtime snapshot. Bare actor
        // frames remain incremental.
        let wrapper_worker = u32_field(value, &["worker_id", "worker"]);
        let Some(actors) = value.get("actors").and_then(Value::as_array) else {
            let _ = self.apply_actor(value, now, wrapper_worker);
            return;
        };

        self.actor_snapshot_generation = self.actor_snapshot_generation.wrapping_add(1);
        let generation = self.actor_snapshot_generation;
        for actor in actors {
            if let Some(actor) = self.apply_actor(actor, now, wrapper_worker) {
                actor.snapshot_generation = generation;
            }
        }
        match wrapper_worker {
            Some(worker_id) => self.actors.retain(|_, actor| {
                actor.worker_id != Some(worker_id) || actor.snapshot_generation == generation
            }),
            None => self
                .actors
                .retain(|_, actor| actor.snapshot_generation == generation),
        }
    }

    fn apply_actor(
        &mut self,
        value: &Value,
        now: Instant,
        default_worker: Option<u32>,
    ) -> Option<&mut ActorState> {
        let address = string_field(value, &["address", "addr", "actor_addr"])?;
        let actor = self
            .actors
            .entry(address.clone())
            .or_insert_with(|| ActorState::new(address));
        actor.apply_json(value, now, default_worker);
        Some(actor)
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
                let _ = self.apply_actor(actor, now, None);
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

    pub(crate) fn totals(&self) -> Totals {
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
pub(crate) struct ActorState {
    pub(crate) address: String,
    pub(crate) name: Option<String>,
    pub(crate) actor_type: Option<String>,
    pub(crate) message_type: Option<String>,
    pub(crate) worker_id: Option<u32>,
    pub(crate) mailbox_depth: u32,
    pub(crate) mailbox_growth: f64,
    pub(crate) messages_processed: u64,
    pub(crate) msg_per_sec: f64,
    pub(crate) last_msg_type: Option<String>,
    pub(crate) poisoned: bool,
    pub(crate) message_type_counts: Vec<(String, u64)>,
    pub(crate) history: VecDeque<ActorHistorySample>,
    pub(crate) receipts: VecDeque<ActorReceipt>,
    /// Messages folded away by the receipt sampling interval.
    pub(crate) sampled_out: u64,
    pub(crate) last_update: Option<Instant>,
    snapshot_generation: u64,
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
            receipts: VecDeque::with_capacity(RECEIPT_CAP),
            sampled_out: 0,
            last_update: None,
            snapshot_generation: 0,
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
        if let Some(message_type) = string_field(value, &["message_type"]).filter(|t| !t.is_empty())
        {
            self.message_type = Some(message_type);
        }
        if let Some(worker_id) = u32_field(value, &["worker_id", "worker"]) {
            self.worker_id = Some(worker_id);
        } else if self.worker_id.is_none() {
            self.worker_id = default_worker;
        }
        let processed_before = self.messages_processed;
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
        self.fold_receipt(
            now,
            self.messages_processed.saturating_sub(processed_before),
        );
        self.push_history(now);
        self.recompute_growth();
    }

    /// Fold a `messages_processed` jump into the sampled receipt ring. The
    /// receipt carries the last message type visible on the frame; messages
    /// hidden inside the jump (or skipped by the interval) are counted, not
    /// logged, so noisy actors cannot flood the page.
    fn fold_receipt(&mut self, now: Instant, delta: u64) {
        if delta == 0 {
            return;
        }
        if let Some(last) = self.receipts.back_mut()
            && now.duration_since(last.at) < RECEIPT_MIN_INTERVAL
        {
            self.sampled_out += delta;
            return;
        }
        if self.receipts.len() == RECEIPT_CAP {
            self.receipts.pop_front();
        }
        let ty = self.last_msg_type.clone().unwrap_or_else(|| "?".to_owned());
        self.receipts.push_back(ActorReceipt {
            at: now,
            ty,
            folded: delta.saturating_sub(1),
        });
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
pub(crate) struct HistorySample {
    pub(crate) at: Instant,
    pub(crate) mailbox_depth: u32,
    pub(crate) msg_per_sec: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct ActorHistorySample {
    pub(crate) at: Instant,
    pub(crate) mailbox_depth: u32,
    pub(crate) msg_per_sec: f64,
}

#[derive(Default)]
pub(crate) struct Totals {
    pub(crate) actors: u32,
    pub(crate) mailbox_depth: u32,
    pub(crate) msg_per_sec: f64,
    pub(crate) poisoned: u32,
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn per_worker_snapshots_remove_stopped_actors_without_touching_other_workers() {
        let now = Instant::now();
        let mut runtime = RuntimeState::new(now);

        runtime.apply_actors(
            &json!({
                "worker_id": 0,
                "actors": [
                    {"address": "stable", "actor_type": "StableActor"},
                    {"address": "observer", "actor_type": "ControlReplyObserver"},
                ],
            }),
            now,
        );
        runtime.apply_actors(
            &json!({
                "worker_id": 1,
                "actors": [
                    {"address": "other-worker", "actor_type": "OtherActor"},
                ],
            }),
            now,
        );
        assert_eq!(runtime.actors.len(), 3);

        runtime.apply_actors(
            &json!({
                "worker_id": 0,
                "actors": [
                    {"address": "stable", "actor_type": "StableActor"},
                ],
            }),
            now,
        );
        assert_eq!(
            runtime
                .actors
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["other-worker", "stable"]
        );

        runtime.apply_actors(&json!({"worker_id": 0, "actors": []}), now);
        assert_eq!(
            runtime
                .actors
                .keys()
                .map(String::as_str)
                .collect::<Vec<_>>(),
            vec!["other-worker"]
        );
    }
}
