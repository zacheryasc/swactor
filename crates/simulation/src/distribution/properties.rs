use crate::trace::SimulationTrace;

use super::trace::{DistributionEventKind, DistributionSnapshot};

/// Metrics computed from a distribution simulation trace.
#[derive(Debug, Clone)]
pub struct DistributionMetrics {
    /// First round where all alive nodes see all other alive nodes.
    pub join_convergence_round: Option<usize>,
    /// Fraction of alive nodes with correct membership at end.
    pub membership_accuracy: f64,
    /// resolved / attempted.
    pub actor_resolve_success_rate: f64,
    /// Number of successful resolutions.
    pub actor_resolve_success: usize,
    /// Number of failed resolutions.
    pub actor_resolve_failed: usize,
    pub num_nodes: usize,
    pub num_rounds: usize,
}

type DistTrace = SimulationTrace<DistributionEventKind, DistributionSnapshot>;

/// Analyze a distribution simulation trace to compute metrics.
pub fn analyze(trace: &DistTrace) -> DistributionMetrics {
    let num_nodes = trace.node_names.len();
    let num_rounds = trace.num_rounds;

    // Find join convergence round: first round where all alive nodes see
    // (alive_count - 1) other members.
    let mut join_convergence_round: Option<usize> = None;

    for (round_idx, round_snaps) in trace.snapshots_per_round.iter().enumerate() {
        let alive_count = round_snaps.iter().filter(|(_, s)| s.is_alive).count();
        if alive_count <= 1 {
            if join_convergence_round.is_none() {
                join_convergence_round = Some(round_idx + 1);
            }
            continue;
        }
        let all_see_others = round_snaps
            .iter()
            .filter(|(_, s)| s.is_alive)
            .all(|(_, s)| s.member_count >= alive_count - 1);
        if all_see_others && join_convergence_round.is_none() {
            join_convergence_round = Some(round_idx + 1);
        }
    }

    // Membership accuracy at end.
    let membership_accuracy = if let Some(last_round) = trace.snapshots_per_round.last() {
        let alive_count = last_round.iter().filter(|(_, s)| s.is_alive).count();
        if alive_count <= 1 {
            1.0
        } else {
            let correct = last_round
                .iter()
                .filter(|(_, s)| s.is_alive && s.member_count >= alive_count - 1)
                .count();
            correct as f64 / alive_count as f64
        }
    } else {
        0.0
    };

    // Actor resolution stats.
    let mut resolve_success = 0usize;
    let mut resolve_failed = 0usize;

    for event in &trace.events {
        match &event.kind {
            DistributionEventKind::ActorResolved { .. } => resolve_success += 1,
            DistributionEventKind::ActorResolveFailed { .. } => resolve_failed += 1,
            _ => {}
        }
    }

    let total_resolve = resolve_success + resolve_failed;
    let actor_resolve_success_rate = if total_resolve > 0 {
        resolve_success as f64 / total_resolve as f64
    } else {
        1.0
    };

    DistributionMetrics {
        join_convergence_round,
        membership_accuracy,
        actor_resolve_success_rate,
        actor_resolve_success: resolve_success,
        actor_resolve_failed: resolve_failed,
        num_nodes,
        num_rounds,
    }
}

/// Check that the cluster forms within a bounded number of rounds.
pub fn check_join_convergence(
    metrics: &DistributionMetrics,
    max_rounds: usize,
) -> crate::properties::PropertyResult {
    let passed = metrics
        .join_convergence_round
        .map(|r| r <= max_rounds)
        .unwrap_or(false);
    crate::properties::PropertyResult {
        name: "join_convergence".into(),
        category: "Cluster Formation".into(),
        passed,
        expected: format!("≤ {max_rounds} rounds"),
        actual: metrics
            .join_convergence_round
            .map(|r| format!("{r} rounds"))
            .unwrap_or("never".into()),
        description: "Cluster membership converges within bound".into(),
    }
}

/// Check that membership accuracy meets a minimum threshold.
pub fn check_membership_accuracy(
    metrics: &DistributionMetrics,
    min_accuracy: f64,
) -> crate::properties::PropertyResult {
    crate::properties::PropertyResult {
        name: "membership_accuracy".into(),
        category: "Cluster Formation".into(),
        passed: metrics.membership_accuracy >= min_accuracy,
        expected: format!("≥ {min_accuracy}"),
        actual: format!("{:.3}", metrics.membership_accuracy),
        description: "Fraction of alive nodes with correct membership".into(),
    }
}

/// Check that actor resolution succeeds at least `min_rate` of the time.
pub fn check_actor_resolution(
    metrics: &DistributionMetrics,
    min_rate: f64,
) -> crate::properties::PropertyResult {
    crate::properties::PropertyResult {
        name: "actor_resolution".into(),
        category: "Actor Directory".into(),
        passed: metrics.actor_resolve_success_rate >= min_rate,
        expected: format!("≥ {min_rate}"),
        actual: format!(
            "{:.3} ({}/{} resolved)",
            metrics.actor_resolve_success_rate,
            metrics.actor_resolve_success,
            metrics.actor_resolve_success + metrics.actor_resolve_failed
        ),
        description: "Actor resolution success rate".into(),
    }
}

/// Check that a killed node is detected (survivors have reduced member count).
pub fn check_failure_detection(
    trace: &DistTrace,
    max_member_count: usize,
) -> crate::properties::PropertyResult {
    let passed = if let Some(last_round) = trace.snapshots_per_round.last() {
        last_round
            .iter()
            .filter(|(_, s)| s.is_alive)
            .all(|(_, s)| s.member_count <= max_member_count)
    } else {
        false
    };
    crate::properties::PropertyResult {
        name: "failure_detection".into(),
        category: "Fault Tolerance".into(),
        passed,
        expected: format!("alive nodes see ≤ {max_member_count} members"),
        actual: if let Some(last_round) = trace.snapshots_per_round.last() {
            let counts: Vec<usize> = last_round
                .iter()
                .filter(|(_, s)| s.is_alive)
                .map(|(_, s)| s.member_count)
                .collect();
            format!("{counts:?}")
        } else {
            "no data".into()
        },
        description: "Survivors detect node death".into(),
    }
}
