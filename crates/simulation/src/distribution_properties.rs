use crate::SimulationTrace;

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

/// SWIM Completeness: every killed node is eventually detected by all survivors.
///
/// Scans all rounds after `killed_at_round`. Passes if there exists a round where
/// every surviving node's member_count has decreased below the original count.
pub fn check_completeness(
    trace: &DistTrace,
    killed_at_round: usize,
    original_alive: usize,
) -> crate::properties::PropertyResult {
    let detected = trace
        .snapshots_per_round
        .iter()
        .skip(killed_at_round)
        .any(|round_snaps| {
            let survivors: Vec<_> = round_snaps.iter().filter(|(_, s)| s.is_alive).collect();
            !survivors.is_empty()
                && survivors
                    .iter()
                    .all(|(_, s)| s.member_count < original_alive)
        });
    crate::properties::PropertyResult {
        name: "completeness".into(),
        category: "SWIM Invariant".into(),
        passed: detected,
        expected: format!("all survivors detect death (member_count < {original_alive})"),
        actual: if detected {
            "all survivors detected".into()
        } else {
            let final_counts: Vec<usize> = trace
                .snapshots_per_round
                .last()
                .map(|r| {
                    r.iter()
                        .filter(|(_, s)| s.is_alive)
                        .map(|(_, s)| s.member_count)
                        .collect()
                })
                .unwrap_or_default();
            format!("final member_counts: {final_counts:?}")
        },
        description: "Every killed node detected by all survivors".into(),
    }
}

/// SWIM Accuracy: no alive node is permanently declared dead.
///
/// At end of simulation, every node that is actually alive should appear
/// in at least `min_fraction` of other alive nodes' member lists.
pub fn check_accuracy(
    trace: &DistTrace,
    min_fraction: f64,
) -> crate::properties::PropertyResult {
    let last_round = match trace.snapshots_per_round.last() {
        Some(r) => r,
        None => {
            return crate::properties::PropertyResult {
                name: "accuracy".into(),
                category: "SWIM Invariant".into(),
                passed: false,
                expected: "trace data".into(),
                actual: "no rounds".into(),
                description: "No alive node permanently dead".into(),
            }
        }
    };

    let alive_count = last_round.iter().filter(|(_, s)| s.is_alive).count();
    if alive_count <= 1 {
        return crate::properties::PropertyResult {
            name: "accuracy".into(),
            category: "SWIM Invariant".into(),
            passed: true,
            expected: format!("≥ {min_fraction:.0}% nodes well-connected"),
            actual: "≤1 alive node".into(),
            description: "No alive node permanently dead".into(),
        };
    }

    // Fraction of alive nodes that see at least (alive_count - 1) members
    let well_connected = last_round
        .iter()
        .filter(|(_, s)| s.is_alive && s.member_count >= alive_count - 1)
        .count();
    let fraction = well_connected as f64 / alive_count as f64;
    let passed = fraction >= min_fraction;

    crate::properties::PropertyResult {
        name: "accuracy".into(),
        category: "SWIM Invariant".into(),
        passed,
        expected: format!("≥ {:.0}% of alive nodes well-connected", min_fraction * 100.0),
        actual: format!("{well_connected}/{alive_count} = {fraction:.2}"),
        description: "No alive node permanently declared dead".into(),
    }
}

/// SWIM Convergence: after all faults stabilize, surviving nodes' member_count
/// values converge to the same value within a bounded number of rounds.
pub fn check_convergence(
    trace: &DistTrace,
    stable_after_round: usize,
    tolerance: usize,
) -> crate::properties::PropertyResult {
    let converged = trace
        .snapshots_per_round
        .iter()
        .skip(stable_after_round)
        .any(|round_snaps| {
            let counts: Vec<usize> = round_snaps
                .iter()
                .filter(|(_, s)| s.is_alive)
                .map(|(_, s)| s.member_count)
                .collect();
            if counts.is_empty() {
                return true;
            }
            let min = *counts.iter().min().unwrap();
            let max = *counts.iter().max().unwrap();
            max - min <= tolerance
        });

    crate::properties::PropertyResult {
        name: "convergence".into(),
        category: "SWIM Invariant".into(),
        passed: converged,
        expected: format!("member_counts converge (spread ≤ {tolerance}) after round {stable_after_round}"),
        actual: if converged {
            "converged".into()
        } else {
            let final_counts: Vec<usize> = trace
                .snapshots_per_round
                .last()
                .map(|r| {
                    r.iter()
                        .filter(|(_, s)| s.is_alive)
                        .map(|(_, s)| s.member_count)
                        .collect()
                })
                .unwrap_or_default();
            format!("final spread: {:?}", final_counts)
        },
        description: "Membership views converge after faults stabilize".into(),
    }
}

// ─── Registry & Lifecycle Property Checks ─────────────────────────────────

/// Check that all alive nodes have at least `min_registry_size` registry entries at the end.
pub fn check_registry_propagation(
    trace: &DistTrace,
    min_registry_size: usize,
) -> crate::properties::PropertyResult {
    let passed = if let Some(last_round) = trace.snapshots_per_round.last() {
        last_round
            .iter()
            .filter(|(_, s)| s.is_alive)
            .all(|(_, s)| s.registry_size >= min_registry_size)
    } else {
        false
    };
    let actual = if let Some(last_round) = trace.snapshots_per_round.last() {
        let sizes: Vec<usize> = last_round
            .iter()
            .filter(|(_, s)| s.is_alive)
            .map(|(_, s)| s.registry_size)
            .collect();
        format!("{sizes:?}")
    } else {
        "no data".into()
    };
    crate::properties::PropertyResult {
        name: "registry_propagation".into(),
        category: "Registry".into(),
        passed,
        expected: format!("all alive nodes have ≥{min_registry_size} registry entries"),
        actual,
        description: "Registry entries propagate to all nodes via gossip".into(),
    }
}

/// Check that all alive nodes agree on registry tombstone count at the end.
pub fn check_registry_tombstones(
    trace: &DistTrace,
    min_tombstones: usize,
) -> crate::properties::PropertyResult {
    let passed = if let Some(last_round) = trace.snapshots_per_round.last() {
        last_round
            .iter()
            .filter(|(_, s)| s.is_alive)
            .all(|(_, s)| s.registry_tombstone_count >= min_tombstones)
    } else {
        false
    };
    let actual = if let Some(last_round) = trace.snapshots_per_round.last() {
        let counts: Vec<usize> = last_round
            .iter()
            .filter(|(_, s)| s.is_alive)
            .map(|(_, s)| s.registry_tombstone_count)
            .collect();
        format!("{counts:?}")
    } else {
        "no data".into()
    };
    crate::properties::PropertyResult {
        name: "registry_tombstones".into(),
        category: "Registry".into(),
        passed,
        expected: format!("all alive nodes have ≥{min_tombstones} tombstones"),
        actual,
        description: "Registry tombstones propagate to all nodes".into(),
    }
}

/// Check that at least one survivor has a non-empty repair queue after a node death.
pub fn check_repair_queue_populated(
    trace: &DistTrace,
    after_round: usize,
) -> crate::properties::PropertyResult {
    let populated = trace
        .snapshots_per_round
        .iter()
        .skip(after_round)
        .any(|round_snaps| {
            round_snaps
                .iter()
                .any(|(_, s)| s.is_alive && s.repair_queue_size > 0)
        });
    crate::properties::PropertyResult {
        name: "repair_queue_populated".into(),
        category: "Lifecycle".into(),
        passed: populated,
        expected: format!("repair queue populated after round {after_round}"),
        actual: if populated {
            "populated".into()
        } else {
            let final_sizes: Vec<usize> = trace
                .snapshots_per_round
                .last()
                .map(|r| {
                    r.iter()
                        .filter(|(_, s)| s.is_alive)
                        .map(|(_, s)| s.repair_queue_size)
                        .collect()
                })
                .unwrap_or_default();
            format!("final repair_queue_sizes: {final_sizes:?}")
        },
        description: "Repair queue populated after node death".into(),
    }
}

/// Check that routing_table_size ≤ alive_count for all alive nodes at every round.
pub fn check_routing_table_bounded(
    trace: &DistTrace,
) -> crate::properties::PropertyResult {
    let mut violation = None;

    for (round_idx, round_snaps) in trace.snapshots_per_round.iter().enumerate() {
        let alive_count = round_snaps.iter().filter(|(_, s)| s.is_alive).count();
        for (name, snap) in round_snaps {
            if snap.is_alive && snap.routing_table_size > alive_count {
                violation = Some(format!(
                    "round {}: {} has routing_table_size={} but alive_count={}",
                    round_idx + 1, name, snap.routing_table_size, alive_count
                ));
                break;
            }
        }
        if violation.is_some() {
            break;
        }
    }

    crate::properties::PropertyResult {
        name: "routing_table_bounded".into(),
        category: "Invariant".into(),
        passed: violation.is_none(),
        expected: "routing_table_size ≤ alive_count at every round".into(),
        actual: violation.unwrap_or_else(|| "all within bounds".into()),
        description: "Routing table never exceeds alive membership".into(),
    }
}

/// Check that cache_size ≤ cache_capacity for all alive nodes at every round.
pub fn check_cache_bounded(
    trace: &DistTrace,
    cache_capacity: usize,
) -> crate::properties::PropertyResult {
    let mut violation = None;

    for (round_idx, round_snaps) in trace.snapshots_per_round.iter().enumerate() {
        for (name, snap) in round_snaps {
            if snap.is_alive && snap.cache_size > cache_capacity {
                violation = Some(format!(
                    "round {}: {} has cache_size={} but capacity={}",
                    round_idx + 1, name, snap.cache_size, cache_capacity
                ));
                break;
            }
        }
        if violation.is_some() {
            break;
        }
    }

    crate::properties::PropertyResult {
        name: "cache_bounded".into(),
        category: "Invariant".into(),
        passed: violation.is_none(),
        expected: format!("cache_size ≤ {cache_capacity} at every round"),
        actual: violation.unwrap_or_else(|| "all within bounds".into()),
        description: "Cache never exceeds configured capacity".into(),
    }
}

// ─── Deployment Topology Property Checks ────────────────────────────────────

/// Check convergence within a specific group of nodes (not the whole cluster).
///
/// Passes if there exists a round after `after_round` where all alive nodes
/// in `group_indices` have member_count within `tolerance` of each other.
pub fn check_group_convergence(
    trace: &DistTrace,
    group_indices: &[usize],
    after_round: usize,
    tolerance: usize,
) -> crate::properties::PropertyResult {
    let converged = trace
        .snapshots_per_round
        .iter()
        .skip(after_round)
        .any(|round_snaps| {
            let counts: Vec<usize> = group_indices
                .iter()
                .filter_map(|&idx| {
                    if idx < round_snaps.len() {
                        let (_, s) = &round_snaps[idx];
                        if s.is_alive { Some(s.member_count) } else { None }
                    } else {
                        None
                    }
                })
                .collect();
            if counts.is_empty() {
                return true;
            }
            let min = *counts.iter().min().unwrap();
            let max = *counts.iter().max().unwrap();
            max - min <= tolerance
        });

    crate::properties::PropertyResult {
        name: "group_convergence".into(),
        category: "Deployment Topology".into(),
        passed: converged,
        expected: format!(
            "group {:?} converges (spread ≤ {tolerance}) after round {after_round}",
            group_indices
        ),
        actual: if converged {
            "converged".into()
        } else {
            let final_counts: Vec<usize> = group_indices
                .iter()
                .filter_map(|&idx| {
                    trace.snapshots_per_round.last().and_then(|r| {
                        if idx < r.len() {
                            let (_, s) = &r[idx];
                            if s.is_alive { Some(s.member_count) } else { None }
                        } else {
                            None
                        }
                    })
                })
                .collect();
            format!("final group member_counts: {final_counts:?}")
        },
        description: "Membership views converge within a node group".into(),
    }
}

/// Detects membership oscillation (suspect→dead→alive cycling).
///
/// For each alive node, counts how many times `member_count` changes direction
/// (increase→decrease or vice versa) after `after_round`. Fails if any node
/// exceeds `max_flips`.
pub fn check_membership_stability(
    trace: &DistTrace,
    after_round: usize,
    max_flips: usize,
) -> crate::properties::PropertyResult {
    let mut worst_node = String::new();
    let mut worst_flips = 0usize;

    let num_nodes = trace.node_names.len();
    for node_idx in 0..num_nodes {
        let rounds: Vec<(usize, bool)> = trace
            .snapshots_per_round
            .iter()
            .skip(after_round)
            .map(|round_snaps| {
                let (_, s) = &round_snaps[node_idx];
                (s.member_count, s.is_alive)
            })
            .collect();

        let mut flips = 0usize;
        // Track direction: +1 = increasing, -1 = decreasing, 0 = no change yet
        let mut direction: i32 = 0;
        let mut prev_count: Option<usize> = None;

        for (count, is_alive) in &rounds {
            if !is_alive {
                prev_count = None;
                direction = 0;
                continue;
            }
            if let Some(prev) = prev_count {
                let new_dir = if *count > prev {
                    1
                } else if *count < prev {
                    -1
                } else {
                    direction // no change keeps previous direction
                };
                if direction != 0 && new_dir != 0 && new_dir != direction {
                    flips += 1;
                }
                if new_dir != 0 {
                    direction = new_dir;
                }
            }
            prev_count = Some(*count);
        }

        if flips > worst_flips {
            worst_flips = flips;
            worst_node = trace.node_names[node_idx].clone();
        }
    }

    crate::properties::PropertyResult {
        name: "membership_stability".into(),
        category: "Topology Adversarial".into(),
        passed: worst_flips <= max_flips,
        expected: format!("≤{max_flips} direction flips per node after round {after_round}"),
        actual: format!("{worst_node} had {worst_flips} flips"),
        description: "Membership count does not oscillate excessively".into(),
    }
}

/// Checks that the spread (max - min) of `member_count` across alive nodes
/// stays within `max_spread` for at least one round after `after_round`.
///
/// Asymmetric relay links cause some nodes to see the full cluster while others
/// see a reduced view — this detects that divergence.
pub fn check_view_asymmetry(
    trace: &DistTrace,
    after_round: usize,
    max_spread: usize,
) -> crate::properties::PropertyResult {
    let within_spread = trace
        .snapshots_per_round
        .iter()
        .skip(after_round)
        .any(|round_snaps| {
            let counts: Vec<usize> = round_snaps
                .iter()
                .filter(|(_, s)| s.is_alive)
                .map(|(_, s)| s.member_count)
                .collect();
            if counts.is_empty() {
                return true;
            }
            let min = *counts.iter().min().unwrap();
            let max = *counts.iter().max().unwrap();
            max - min <= max_spread
        });

    let final_spread = trace
        .snapshots_per_round
        .last()
        .map(|round_snaps| {
            let counts: Vec<usize> = round_snaps
                .iter()
                .filter(|(_, s)| s.is_alive)
                .map(|(_, s)| s.member_count)
                .collect();
            if counts.is_empty() {
                return (0, Vec::new());
            }
            let min = *counts.iter().min().unwrap();
            let max = *counts.iter().max().unwrap();
            (max - min, counts)
        })
        .unwrap_or((0, Vec::new()));

    crate::properties::PropertyResult {
        name: "view_asymmetry".into(),
        category: "Topology Adversarial".into(),
        passed: within_spread,
        expected: format!("member_count spread ≤{max_spread} for at least one round after {after_round}"),
        actual: format!("final spread={}, counts={:?}", final_spread.0, final_spread.1),
        description: "Membership views across alive nodes do not diverge excessively".into(),
    }
}

/// Detect total convergence failure: all alive nodes have member_count == 0
/// for every round after `after_round`. This catches the deploy auth race
/// failure mode where peer introductions happen but SWIM joins never complete.
pub fn check_zero_convergence(
    trace: &DistTrace,
    after_round: usize,
) -> crate::properties::PropertyResult {
    let all_zero = trace
        .snapshots_per_round
        .iter()
        .skip(after_round)
        .all(|round_snaps| {
            let alive: Vec<_> = round_snaps.iter().filter(|(_, s)| s.is_alive).collect();
            !alive.is_empty() && alive.iter().all(|(_, s)| s.member_count == 0)
        });

    crate::properties::PropertyResult {
        name: "zero_convergence".into(),
        category: "Cluster Formation".into(),
        passed: !all_zero,
        expected: format!("at least one alive node has member_count > 0 after round {after_round}"),
        actual: if all_zero {
            "all alive nodes stuck at member_count=0".into()
        } else {
            "membership progressing".into()
        },
        description: "Detects total SWIM convergence failure (auth race / join never completed)".into(),
    }
}

/// Check that staggered-join nodes eventually reach min_members by a deadline.
///
/// Passes if by `by_round`, at least `min_members` of the `expected_joined` nodes
/// are alive and have member_count >= 1.
pub fn check_staggered_join(
    trace: &DistTrace,
    expected_joined: &[usize],
    min_members: usize,
    by_round: usize,
) -> crate::properties::PropertyResult {
    let joined_count = trace
        .snapshots_per_round
        .iter()
        .take(by_round)
        .next_back()
        .map(|round_snaps| {
            expected_joined
                .iter()
                .filter(|&&idx| {
                    if idx < round_snaps.len() {
                        let (_, s) = &round_snaps[idx];
                        s.is_alive && s.member_count >= 1
                    } else {
                        false
                    }
                })
                .count()
        })
        .unwrap_or(0);

    crate::properties::PropertyResult {
        name: "staggered_join".into(),
        category: "Deployment Topology".into(),
        passed: joined_count >= min_members,
        expected: format!(
            "≥{min_members} of {:?} joined with ≥1 member by round {by_round}",
            expected_joined
        ),
        actual: format!("{joined_count} nodes joined"),
        description: "Staggered-join nodes reach membership by deadline".into(),
    }
}
