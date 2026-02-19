//! Automated anomaly detection for the runtime dashboard.
//!
//! Runs on each stats sample, comparing consecutive snapshots to detect
//! growing mailboxes, stalled actors, worker imbalance, and other conditions.

use std::collections::HashMap;

use swactor::actor::ActorAddress;
use swactor::stats::RuntimeStats;

/// Types of warnings the detector can produce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WarningType {
    GrowingMailbox,
    StalledActor,
    PoisonedActor,
    WorkerImbalance,
    EmptyWorker,
    MailboxOverflow,
}

/// Severity levels for warnings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Low,
    Medium,
    High,
    Critical,
}

/// An active warning.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Warning {
    pub warning_type: WarningType,
    pub severity: Severity,
    pub entity: String,
    pub description: String,
}

/// Configuration for warning thresholds.
#[derive(Debug, Clone)]
pub struct WarningConfig {
    /// Consecutive samples with increasing mailbox depth before warning.
    pub growing_mailbox_threshold: usize,
    /// Consecutive ticks with no message processing while mailbox > 0.
    pub stalled_actor_threshold: usize,
    /// A worker is "imbalanced" if it has > this ratio times the average load.
    pub worker_imbalance_ratio: f64,
}

impl Default for WarningConfig {
    fn default() -> Self {
        Self {
            growing_mailbox_threshold: 5,
            stalled_actor_threshold: 10,
            worker_imbalance_ratio: 2.0,
        }
    }
}

/// Per-actor tracking state.
struct ActorState {
    prev_mailbox: usize,
    prev_messages: u64,
    growing_streak: usize,
    stalled_streak: usize,
}

/// Warning detection engine. Call `check()` on each stats sample.
pub struct WarningDetector {
    config: WarningConfig,
    actors: HashMap<ActorAddress, ActorState>,
}

impl WarningDetector {
    pub fn new(config: WarningConfig) -> Self {
        Self {
            config,
            actors: HashMap::new(),
        }
    }

    /// Analyze a stats snapshot and return all active warnings.
    pub fn check(&mut self, stats: &RuntimeStats) -> Vec<Warning> {
        let mut warnings = Vec::new();

        // Track which actors are still alive
        let mut live_addrs: std::collections::HashSet<ActorAddress> =
            std::collections::HashSet::new();

        for actor in &stats.actor_details {
            live_addrs.insert(actor.address);

            // Poisoned actor — immediate critical warning
            if actor.poisoned {
                warnings.push(Warning {
                    warning_type: WarningType::PoisonedActor,
                    severity: Severity::Critical,
                    entity: format!("{}", actor.address),
                    description: "Actor is poisoned (panicked)".to_string(),
                });
            }

            let state = self.actors.entry(actor.address).or_insert(ActorState {
                prev_mailbox: actor.mailbox_depth,
                prev_messages: actor.messages_processed,
                growing_streak: 0,
                stalled_streak: 0,
            });

            // Growing mailbox detection
            if actor.mailbox_depth > state.prev_mailbox && actor.mailbox_depth > 0 {
                state.growing_streak += 1;
            } else {
                state.growing_streak = 0;
            }

            if state.growing_streak >= self.config.growing_mailbox_threshold {
                warnings.push(Warning {
                    warning_type: WarningType::GrowingMailbox,
                    severity: Severity::Medium,
                    entity: format!("{}", actor.address),
                    description: format!(
                        "Mailbox growing for {} consecutive samples (depth: {})",
                        state.growing_streak, actor.mailbox_depth,
                    ),
                });
            }

            // Stalled actor detection
            if actor.messages_processed == state.prev_messages && actor.mailbox_depth > 0 {
                state.stalled_streak += 1;
            } else {
                state.stalled_streak = 0;
            }

            if state.stalled_streak >= self.config.stalled_actor_threshold {
                warnings.push(Warning {
                    warning_type: WarningType::StalledActor,
                    severity: Severity::High,
                    entity: format!("{}", actor.address),
                    description: format!(
                        "No messages processed for {} ticks with {} pending",
                        state.stalled_streak, actor.mailbox_depth,
                    ),
                });
            }

            state.prev_mailbox = actor.mailbox_depth;
            state.prev_messages = actor.messages_processed;
        }

        // Clean up dead actors
        self.actors.retain(|addr, _| live_addrs.contains(addr));

        // Mailbox overflow detection
        for w in &stats.workers {
            if w.messages_dropped > 0 {
                warnings.push(Warning {
                    warning_type: WarningType::MailboxOverflow,
                    severity: Severity::Medium,
                    entity: format!("Worker {}", w.id),
                    description: format!("{} messages dropped", w.messages_dropped),
                });
            }
        }

        // Worker imbalance and empty worker detection
        if stats.workers.len() > 1 {
            let total_actors: usize = stats.workers.iter().map(|w| w.num_actors).sum();
            let avg = total_actors as f64 / stats.workers.len() as f64;

            for w in &stats.workers {
                if avg > 0.0 && w.num_actors as f64 > avg * self.config.worker_imbalance_ratio {
                    warnings.push(Warning {
                        warning_type: WarningType::WorkerImbalance,
                        severity: Severity::Low,
                        entity: format!("Worker {}", w.id),
                        description: format!(
                            "{} actors vs {:.0} average ({:.1}x)",
                            w.num_actors, avg, w.num_actors as f64 / avg,
                        ),
                    });
                }

                if w.num_actors == 0 && total_actors > 0 {
                    warnings.push(Warning {
                        warning_type: WarningType::EmptyWorker,
                        severity: Severity::Low,
                        entity: format!("Worker {}", w.id),
                        description: "Worker has no actors while others do".to_string(),
                    });
                }
            }
        }

        // Sort by severity (critical first)
        warnings.sort_by(|a, b| b.severity.cmp(&a.severity));
        warnings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use swactor::stats::{ActorInfo, WorkerInfo};

    fn make_worker(id: usize, actors: usize, dropped: u64) -> WorkerInfo {
        WorkerInfo {
            id,
            num_actors: actors,
            mailbox_depth: 0,
            messages_processed: 0,
            local_sends: 0,
            cross_sends: 0,
            inbox_sends: 0,
            type_mismatches: 0,
            panics: 0,
            messages_dropped: dropped,
            restarts: 0,
            stops: 0,
        }
    }

    fn make_actor(id: u8, depth: usize, msgs: u64, poisoned: bool) -> ActorInfo {
        ActorInfo {
            address: ActorAddress([id; 32]),
            name: None,
            worker_id: 0,
            mailbox_depth: depth,
            last_msg_type: None,
            messages_processed: msgs,
            poisoned,
            message_type_counts: Vec::new(),
        }
    }

    fn make_stats(workers: Vec<WorkerInfo>, actors: Vec<ActorInfo>) -> RuntimeStats {
        RuntimeStats {
            num_workers: workers.len(),
            uptime_ms: 0,
            actors: actors.iter().map(|a| (a.address, a.worker_id)).collect(),
            workers,
            actor_details: actors,
            tick_timings: Vec::new(),
        }
    }

    #[test]
    fn poisoned_actor_triggers_critical_warning() {
        let mut detector = WarningDetector::new(WarningConfig::default());
        let stats = make_stats(
            vec![make_worker(0, 1, 0)],
            vec![make_actor(1, 0, 10, true)],
        );
        let warnings = detector.check(&stats);
        assert!(warnings.iter().any(|w| w.warning_type == WarningType::PoisonedActor));
        assert!(warnings.iter().any(|w| w.severity == Severity::Critical));
    }

    #[test]
    fn growing_mailbox_triggers_after_threshold() {
        let config = WarningConfig {
            growing_mailbox_threshold: 3,
            ..Default::default()
        };
        let mut detector = WarningDetector::new(config);

        // 4 samples with increasing mailbox: should trigger at sample 4
        for depth in 1..=4 {
            let stats = make_stats(
                vec![make_worker(0, 1, 0)],
                vec![make_actor(1, depth, 0, false)],
            );
            let warnings = detector.check(&stats);
            if depth < 4 {
                assert!(!warnings.iter().any(|w| w.warning_type == WarningType::GrowingMailbox),
                    "should not trigger at depth {}", depth);
            } else {
                assert!(warnings.iter().any(|w| w.warning_type == WarningType::GrowingMailbox),
                    "should trigger at depth {}", depth);
            }
        }
    }

    #[test]
    fn growing_mailbox_resets_on_decrease() {
        let config = WarningConfig {
            growing_mailbox_threshold: 3,
            ..Default::default()
        };
        let mut detector = WarningDetector::new(config);

        // Grow for 2 samples, then decrease, then grow again
        for depth in [1, 2, 1, 2, 3, 4] {
            let stats = make_stats(
                vec![make_worker(0, 1, 0)],
                vec![make_actor(1, depth, 0, false)],
            );
            detector.check(&stats);
        }
        // After 1,2 → streak=2; then 1 → streak=0; then 2,3,4 → streak=3 → triggers
        let stats = make_stats(
            vec![make_worker(0, 1, 0)],
            vec![make_actor(1, 5, 0, false)],
        );
        let warnings = detector.check(&stats);
        assert!(warnings.iter().any(|w| w.warning_type == WarningType::GrowingMailbox));
    }

    #[test]
    fn stalled_actor_triggers_when_not_processing() {
        let config = WarningConfig {
            stalled_actor_threshold: 3,
            ..Default::default()
        };
        let mut detector = WarningDetector::new(config);

        // Same messages_processed, nonzero mailbox for 4 ticks
        for _ in 0..4 {
            let stats = make_stats(
                vec![make_worker(0, 1, 0)],
                vec![make_actor(1, 5, 100, false)],
            );
            let warnings = detector.check(&stats);
            // Last one should trigger
            if warnings.iter().any(|w| w.warning_type == WarningType::StalledActor) {
                return; // test passed
            }
        }
        panic!("expected StalledActor warning");
    }

    #[test]
    fn worker_imbalance_detected() {
        let mut detector = WarningDetector::new(WarningConfig::default());
        // Worker 0: 10 actors, Worker 1: 1 actor. Avg=5.5, ratio=10/5.5=1.8
        // With ratio threshold 2.0, this should NOT trigger
        let stats = make_stats(
            vec![make_worker(0, 10, 0), make_worker(1, 1, 0)],
            vec![],
        );
        let warnings = detector.check(&stats);
        assert!(!warnings.iter().any(|w| w.warning_type == WarningType::WorkerImbalance));

        // Worker 0: 20 actors, Worker 1: 1 actor. Avg=10.5, ratio=20/10.5=1.9 — still no
        // Worker 0: 30 actors, Worker 1: 1 actor. Avg=15.5, ratio=30/15.5=1.9 — still no
        // Worker 0: 100 actors, Worker 1: 1 actor. Avg=50.5, ratio=100/50.5=1.98 — almost
        // Worker 0: 100 actors, Worker 1: 0 actor. Avg=50, ratio=100/50=2.0 — at threshold

        let stats2 = make_stats(
            vec![make_worker(0, 100, 0), make_worker(1, 1, 0)],
            vec![],
        );
        let warnings2 = detector.check(&stats2);
        // 100 / 50.5 = 1.98 — not > 2.0
        assert!(!warnings2.iter().any(|w| w.warning_type == WarningType::WorkerImbalance));

        // Now 200 vs 1: 200/100.5 = ~1.99 — still not. Let's do 300 vs 1: 300/150.5 = ~2.0
        // Actually need > 2x. Let's do 50 vs 1: avg=25.5, ratio=50/25.5=1.96. Nope.
        // 10 vs 1 vs 1: avg=4, ratio=10/4=2.5 — triggers!
        let stats3 = make_stats(
            vec![make_worker(0, 10, 0), make_worker(1, 1, 0), make_worker(2, 1, 0)],
            vec![],
        );
        let warnings3 = detector.check(&stats3);
        assert!(warnings3.iter().any(|w| w.warning_type == WarningType::WorkerImbalance));
    }

    #[test]
    fn empty_worker_detected() {
        let mut detector = WarningDetector::new(WarningConfig::default());
        let stats = make_stats(
            vec![make_worker(0, 5, 0), make_worker(1, 0, 0)],
            vec![],
        );
        let warnings = detector.check(&stats);
        assert!(warnings.iter().any(|w| w.warning_type == WarningType::EmptyWorker));
    }

    #[test]
    fn mailbox_overflow_detected() {
        let mut detector = WarningDetector::new(WarningConfig::default());
        let stats = make_stats(
            vec![make_worker(0, 1, 42)],
            vec![],
        );
        let warnings = detector.check(&stats);
        assert!(warnings.iter().any(|w| w.warning_type == WarningType::MailboxOverflow));
    }
}
