//! In-process time-series history for dashboard sparklines and trend detection.
//!
//! Stores bounded ring buffers of per-worker and per-actor stats, sampled at
//! a configurable interval. All data is kept in memory with automatic eviction
//! of the oldest samples when capacity is reached.

use std::collections::{HashMap, VecDeque};
use std::sync::RwLock;

use swactor::actor::ActorAddress;
use swactor::stats::{ActorInfo, RuntimeStats};

/// Configuration for history collection.
#[derive(Debug, Clone)]
pub struct HistoryConfig {
    /// Maximum samples per worker (default: 300 = 5 min at 1/sec).
    pub max_worker_samples: usize,
    /// Maximum samples per actor (default: 300).
    pub max_actor_samples: usize,
    /// Maximum number of actors tracked (LRU eviction). Default: 1000.
    pub max_tracked_actors: usize,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            max_worker_samples: 300,
            max_actor_samples: 300,
            max_tracked_actors: 1000,
        }
    }
}

/// Time-series data for a single worker.
#[derive(Debug, Clone)]
pub struct WorkerHistory {
    pub message_rates: VecDeque<f64>,
    pub mailbox_depths: VecDeque<u64>,
    pub actor_counts: VecDeque<u32>,
    prev_messages: u64,
}

impl WorkerHistory {
    fn new() -> Self {
        Self {
            message_rates: VecDeque::new(),
            mailbox_depths: VecDeque::new(),
            actor_counts: VecDeque::new(),
            prev_messages: 0,
        }
    }

    fn push(&mut self, messages_processed: u64, mailbox_depth: usize, num_actors: usize, cap: usize) {
        let rate = messages_processed.saturating_sub(self.prev_messages) as f64;
        self.prev_messages = messages_processed;

        push_bounded(&mut self.message_rates, rate, cap);
        push_bounded(&mut self.mailbox_depths, mailbox_depth as u64, cap);
        push_bounded(&mut self.actor_counts, num_actors as u32, cap);
    }
}

/// Time-series data for a single actor.
#[derive(Debug, Clone)]
pub struct ActorHistory {
    pub mailbox_depths: VecDeque<u64>,
    pub message_rates: VecDeque<f64>,
    prev_messages: u64,
    last_seen_sample: u64,
}

impl ActorHistory {
    fn new(sample_counter: u64) -> Self {
        Self {
            mailbox_depths: VecDeque::new(),
            message_rates: VecDeque::new(),
            prev_messages: 0,
            last_seen_sample: sample_counter,
        }
    }

    fn push(&mut self, info: &ActorInfo, cap: usize, sample_counter: u64) {
        let rate = info.messages_processed.saturating_sub(self.prev_messages) as f64;
        self.prev_messages = info.messages_processed;
        self.last_seen_sample = sample_counter;

        push_bounded(&mut self.mailbox_depths, info.mailbox_depth as u64, cap);
        push_bounded(&mut self.message_rates, rate, cap);
    }
}

fn push_bounded<T>(buf: &mut VecDeque<T>, val: T, cap: usize) {
    if buf.len() >= cap {
        buf.pop_front();
    }
    buf.push_back(val);
}

/// Thread-safe history store. Written by the sampler, read by SSE/TUI.
pub struct DashboardHistory {
    inner: RwLock<HistoryInner>,
    config: HistoryConfig,
}

struct HistoryInner {
    workers: Vec<WorkerHistory>,
    actors: HashMap<ActorAddress, ActorHistory>,
    sample_counter: u64,
}

impl DashboardHistory {
    pub fn new(config: HistoryConfig) -> Self {
        Self {
            inner: RwLock::new(HistoryInner {
                workers: Vec::new(),
                actors: HashMap::new(),
                sample_counter: 0,
            }),
            config,
        }
    }

    /// Record a stats snapshot. Called by the sampler thread.
    pub fn record(&self, stats: &RuntimeStats) {
        let mut inner = self.inner.write().unwrap();
        inner.sample_counter += 1;
        let counter = inner.sample_counter;

        // Resize workers vec if needed
        while inner.workers.len() < stats.workers.len() {
            inner.workers.push(WorkerHistory::new());
        }

        // Record per-worker data
        for w in &stats.workers {
            if let Some(wh) = inner.workers.get_mut(w.id) {
                wh.push(
                    w.messages_processed,
                    w.mailbox_depth,
                    w.num_actors,
                    self.config.max_worker_samples,
                );
            }
        }

        // Record per-actor data
        for a in &stats.actor_details {
            let ah = inner.actors.entry(a.address).or_insert_with(|| ActorHistory::new(counter));
            ah.push(a, self.config.max_actor_samples, counter);
        }

        // LRU eviction: remove actors not seen recently if over capacity
        if inner.actors.len() > self.config.max_tracked_actors {
            let mut entries: Vec<(ActorAddress, u64)> = inner
                .actors
                .iter()
                .map(|(addr, ah)| (*addr, ah.last_seen_sample))
                .collect();
            entries.sort_by_key(|&(_, seen)| seen);
            let to_remove = inner.actors.len() - self.config.max_tracked_actors;
            for (addr, _) in entries.into_iter().take(to_remove) {
                inner.actors.remove(&addr);
            }
        }
    }

    /// Get a snapshot of worker history for rendering sparklines.
    /// Returns Vec indexed by worker_id, each containing recent message rates.
    pub fn worker_sparklines(&self) -> Vec<Vec<u64>> {
        let inner = self.inner.read().unwrap();
        inner
            .workers
            .iter()
            .map(|wh| wh.message_rates.iter().map(|r| *r as u64).collect())
            .collect()
    }

    /// Get worker mailbox depth history.
    pub fn worker_mailbox_sparklines(&self) -> Vec<Vec<u64>> {
        let inner = self.inner.read().unwrap();
        inner
            .workers
            .iter()
            .map(|wh| wh.mailbox_depths.iter().copied().collect())
            .collect()
    }

    /// Get sparkline data for a specific actor.
    pub fn actor_sparkline(&self, addr: &ActorAddress) -> Option<(Vec<u64>, Vec<u64>)> {
        let inner = self.inner.read().unwrap();
        inner.actors.get(addr).map(|ah| {
            let mailbox: Vec<u64> = ah.mailbox_depths.iter().copied().collect();
            let rates: Vec<u64> = ah.message_rates.iter().map(|r| *r as u64).collect();
            (mailbox, rates)
        })
    }

    /// Get total sample count (useful for knowing if history is available).
    pub fn sample_count(&self) -> u64 {
        self.inner.read().unwrap().sample_counter
    }

    /// Serialize worker history as JSON for the SSE initial payload.
    pub fn worker_history_json(&self) -> String {
        let sparklines = self.worker_sparklines();
        let mailbox = self.worker_mailbox_sparklines();
        serde_json::json!({
            "workers": sparklines.iter().enumerate().map(|(i, rates)| {
                serde_json::json!({
                    "id": i,
                    "message_rates": rates,
                    "mailbox_depths": mailbox.get(i).unwrap_or(&Vec::new()),
                })
            }).collect::<Vec<_>>(),
        })
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swactor::stats::{ActorInfo, WorkerInfo};

    fn make_stats(workers: Vec<(u64, usize, usize)>, actors: Vec<ActorInfo>) -> RuntimeStats {
        RuntimeStats {
            num_workers: workers.len(),
            uptime_ms: 0,
            actors: actors.iter().map(|a| (a.address, a.worker_id)).collect(),
            workers: workers
                .into_iter()
                .enumerate()
                .map(|(id, (msgs, depth, n_actors))| WorkerInfo {
                    id,
                    num_actors: n_actors,
                    mailbox_depth: depth,
                    messages_processed: msgs,
                    local_sends: 0,
                    cross_sends: 0,
                    inbox_sends: 0,
                    type_mismatches: 0,
                    panics: 0,
                    messages_dropped: 0,
                    restarts: 0,
                    stops: 0,
                })
                .collect(),
            actor_details: actors,
            tick_timings: Vec::new(),
        }
    }

    fn make_actor(id: u8, worker: usize, depth: usize, msgs: u64) -> ActorInfo {
        ActorInfo {
            address: ActorAddress([id; 32]),
            name: None,
            worker_id: worker,
            mailbox_depth: depth,
            last_msg_type: None,
            messages_processed: msgs,
            poisoned: false,
            message_type_counts: Vec::new(),
        }
    }

    #[test]
    fn worker_rates_accumulate_over_samples() {
        let history = DashboardHistory::new(HistoryConfig::default());

        // First sample: establishes baseline (rate will be the raw value since prev=0)
        let stats1 = make_stats(vec![(100, 5, 2)], vec![]);
        history.record(&stats1);

        // Second sample: delta = 150 - 100 = 50
        let stats2 = make_stats(vec![(150, 3, 2)], vec![]);
        history.record(&stats2);

        let sparklines = history.worker_sparklines();
        assert_eq!(sparklines.len(), 1);
        assert_eq!(sparklines[0].len(), 2);
        assert_eq!(sparklines[0][0], 100); // first sample: 100 - 0
        assert_eq!(sparklines[0][1], 50); // second sample: 150 - 100
    }

    #[test]
    fn bounded_eviction_drops_oldest() {
        let config = HistoryConfig {
            max_worker_samples: 3,
            ..Default::default()
        };
        let history = DashboardHistory::new(config);

        for i in 0..5u64 {
            let stats = make_stats(vec![(i * 10, 0, 0)], vec![]);
            history.record(&stats);
        }

        let sparklines = history.worker_sparklines();
        assert_eq!(sparklines[0].len(), 3); // capped at 3
    }

    #[test]
    fn actor_lru_eviction_keeps_most_recent() {
        let config = HistoryConfig {
            max_tracked_actors: 2,
            ..Default::default()
        };
        let history = DashboardHistory::new(config);

        // Sample 1: actors A and B
        let stats1 = make_stats(
            vec![(0, 0, 2)],
            vec![make_actor(1, 0, 0, 0), make_actor(2, 0, 0, 0)],
        );
        history.record(&stats1);

        // Sample 2: actors B and C (A not seen)
        let stats2 = make_stats(
            vec![(0, 0, 2)],
            vec![make_actor(2, 0, 0, 0), make_actor(3, 0, 0, 0)],
        );
        history.record(&stats2);

        // A should be evicted (LRU), B and C kept
        assert!(history.actor_sparkline(&ActorAddress([1; 32])).is_none());
        assert!(history.actor_sparkline(&ActorAddress([2; 32])).is_some());
        assert!(history.actor_sparkline(&ActorAddress([3; 32])).is_some());
    }

    #[test]
    fn actor_rates_track_deltas() {
        let history = DashboardHistory::new(HistoryConfig::default());

        let stats1 = make_stats(vec![(0, 0, 1)], vec![make_actor(1, 0, 5, 100)]);
        history.record(&stats1);

        let stats2 = make_stats(vec![(0, 0, 1)], vec![make_actor(1, 0, 3, 175)]);
        history.record(&stats2);

        let (mailbox, rates) = history.actor_sparkline(&ActorAddress([1; 32])).unwrap();
        assert_eq!(mailbox, vec![5, 3]);
        assert_eq!(rates[0], 100); // first: 100 - 0
        assert_eq!(rates[1], 75); // second: 175 - 100
    }
}
