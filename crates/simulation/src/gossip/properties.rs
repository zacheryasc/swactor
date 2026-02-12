use std::collections::HashMap;

use crate::properties::{PropertyResult, chi_squared_uniform, coeff_of_variation};
use crate::topology::Topology;

use super::sim::GossipSimConfig;
use super::trace::{GossipEventKind, SimulationTrace};

// ── Metrics ─────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct GossipMetrics {
    /// Fraction of nodes holding all keys at end.
    pub delivery_ratio: f64,
    /// Per-key: all nodes have it or none do.
    pub atomic_delivery: bool,
    /// First round where all nodes hold all keys.
    pub convergence_round: Option<usize>,
    /// Round the final node got all keys.
    pub last_node_round: Option<usize>,
    /// Count of GossipRoundStarted events (pushes sent).
    pub total_pushes: usize,
    /// Count of PushReceived with keys_updated == 0.
    pub redundant_pushes: usize,
    /// redundant / total.
    pub redundancy_ratio: f64,
    /// Push-sends per node.
    pub pushes_sent_per_node: HashMap<String, usize>,
    /// Push-receives per node.
    pub pushes_received_per_node: HashMap<String, usize>,
    /// Coefficient of variation of per-node receive load.
    pub load_balance_cv: f64,
    /// total_pushes / num_nodes.
    pub amplification_factor: f64,
    /// Per-round fraction of converged nodes.
    pub convergence_curve: Vec<f64>,
    /// 1.0 - curve[last].
    pub residue: f64,
    /// Per-node target selection histogram.
    pub peer_selection_distribution: HashMap<String, HashMap<String, usize>>,
    /// Disagreeing node-pairs per round.
    pub entropy_per_round: Vec<usize>,
    /// Distinct (value, version) tuples per key at end.
    pub final_value_divergence: HashMap<String, usize>,
    /// Mean entries per node per round.
    pub avg_state_size_per_round: Vec<f64>,
    pub num_nodes: usize,
    pub num_edges: usize,
    pub num_rounds: usize,
    pub total_keys: usize,
}

// ── Analysis ────────────────────────────────────────────────────────────────

pub fn analyze(trace: &SimulationTrace) -> GossipMetrics {
    let num_nodes = trace.node_names.len();
    let num_edges = trace.topology_edges.len();
    let num_rounds = trace.num_rounds;
    let total_keys = trace.total_keys;

    // ── Pass 1: events ──────────────────────────────────────────────────
    let mut total_pushes = 0usize;
    let mut redundant_pushes = 0usize;
    let mut pushes_sent: HashMap<String, usize> = HashMap::new();
    let mut pushes_received: HashMap<String, usize> = HashMap::new();
    let mut peer_selection: HashMap<String, HashMap<String, usize>> = HashMap::new();

    for event in &trace.events {
        match &event.kind {
            GossipEventKind::GossipRoundStarted { target_name } => {
                total_pushes += 1;
                *pushes_sent.entry(event.node_name.clone()).or_default() += 1;
                *peer_selection
                    .entry(event.node_name.clone())
                    .or_default()
                    .entry(target_name.clone())
                    .or_default() += 1;
            }
            GossipEventKind::PushReceived { keys_updated, .. } => {
                *pushes_received
                    .entry(event.node_name.clone())
                    .or_default() += 1;
                if *keys_updated == 0 {
                    redundant_pushes += 1;
                }
            }
            _ => {}
        }
    }

    let redundancy_ratio = if total_pushes > 0 {
        redundant_pushes as f64 / total_pushes as f64
    } else {
        0.0
    };

    // ── Pass 2: snapshots ───────────────────────────────────────────────
    let mut convergence_curve = Vec::with_capacity(num_rounds);
    let mut entropy_per_round = Vec::with_capacity(num_rounds);
    let mut avg_state_size_per_round = Vec::with_capacity(num_rounds);
    let mut convergence_round: Option<usize> = None;
    let mut last_node_round: Option<usize> = None;
    let mut node_converged_at: HashMap<String, usize> = HashMap::new();

    for (round_idx, round_snaps) in trace.snapshots_per_round.iter().enumerate() {
        let round_num = round_idx + 1;

        let converged_count = if total_keys > 0 {
            round_snaps
                .iter()
                .filter(|(_, snap)| snap.entries.len() >= total_keys)
                .count()
        } else {
            num_nodes
        };
        let frac = if num_nodes > 0 {
            converged_count as f64 / num_nodes as f64
        } else {
            1.0
        };
        convergence_curve.push(frac);

        if convergence_round.is_none() && converged_count == num_nodes {
            convergence_round = Some(round_num);
        }

        for (name, snap) in round_snaps {
            if total_keys > 0 && snap.entries.len() >= total_keys {
                node_converged_at.entry(name.clone()).or_insert(round_num);
            }
        }

        let entropy = if num_nodes <= 1500 {
            compute_entropy_pairwise(round_snaps, total_keys)
        } else {
            compute_entropy_majority(round_snaps, total_keys)
        };
        entropy_per_round.push(entropy);

        let total_entries: usize = round_snaps.iter().map(|(_, s)| s.entries.len()).sum();
        let avg = if round_snaps.is_empty() {
            0.0
        } else {
            total_entries as f64 / round_snaps.len() as f64
        };
        avg_state_size_per_round.push(avg);
    }

    if !node_converged_at.is_empty() {
        last_node_round = node_converged_at.values().max().copied();
    }

    let delivery_ratio = convergence_curve.last().copied().unwrap_or(0.0);
    let atomic_delivery = check_atomic_delivery_inner(trace);
    let final_value_divergence = compute_final_divergence(trace);

    let recv_counts: Vec<f64> = trace
        .node_names
        .iter()
        .map(|n| *pushes_received.get(n).unwrap_or(&0) as f64)
        .collect();
    let load_balance_cv = coeff_of_variation(&recv_counts);

    let amplification_factor = if num_nodes > 0 {
        total_pushes as f64 / num_nodes as f64
    } else {
        0.0
    };

    let residue = 1.0 - convergence_curve.last().copied().unwrap_or(0.0);

    GossipMetrics {
        delivery_ratio,
        atomic_delivery,
        convergence_round,
        last_node_round,
        total_pushes,
        redundant_pushes,
        redundancy_ratio,
        pushes_sent_per_node: pushes_sent,
        pushes_received_per_node: pushes_received,
        load_balance_cv,
        amplification_factor,
        convergence_curve,
        residue,
        peer_selection_distribution: peer_selection,
        entropy_per_round,
        final_value_divergence,
        avg_state_size_per_round,
        num_nodes,
        num_edges,
        num_rounds,
        total_keys,
    }
}

// ── Entropy helpers ─────────────────────────────────────────────────────────

fn compute_entropy_pairwise(
    round_snaps: &[(String, super::trace::NodeSnapshot)],
    total_keys: usize,
) -> usize {
    if total_keys == 0 {
        return 0;
    }
    let mut disagreements = 0usize;
    for i in 0..round_snaps.len() {
        for j in (i + 1)..round_snaps.len() {
            let (_, snap_i) = &round_snaps[i];
            let (_, snap_j) = &round_snaps[j];
            if snap_i.entries.len() != snap_j.entries.len() {
                disagreements += 1;
                continue;
            }
            let mut agree = true;
            for (key, val_i) in &snap_i.entries {
                match snap_j.entries.get(key) {
                    Some(val_j) if val_j.version == val_i.version => {}
                    _ => {
                        agree = false;
                        break;
                    }
                }
            }
            if !agree {
                disagreements += 1;
            }
        }
    }
    disagreements
}

fn compute_entropy_majority(
    round_snaps: &[(String, super::trace::NodeSnapshot)],
    total_keys: usize,
) -> usize {
    if total_keys == 0 || round_snaps.is_empty() {
        return 0;
    }
    let mut deviating_nodes = std::collections::HashSet::new();

    let mut all_keys = std::collections::HashSet::new();
    for (_, snap) in round_snaps {
        for key in snap.entries.keys() {
            all_keys.insert(key.clone());
        }
    }

    for key in &all_keys {
        let mut version_counts: HashMap<u64, usize> = HashMap::new();
        let mut missing_count = 0usize;
        for (_, snap) in round_snaps {
            match snap.entries.get(key) {
                Some(val) => *version_counts.entry(val.version).or_default() += 1,
                None => missing_count += 1,
            }
        }
        let majority_version = version_counts
            .iter()
            .max_by_key(|(_, c)| *c)
            .map(|(&v, _)| v);

        if let Some(mv) = majority_version {
            for (i, (_, snap)) in round_snaps.iter().enumerate() {
                match snap.entries.get(key) {
                    Some(val) if val.version == mv => {}
                    _ => {
                        deviating_nodes.insert(i);
                    }
                }
            }
        }
        if missing_count > 0 {
            for (i, (_, snap)) in round_snaps.iter().enumerate() {
                if !snap.entries.contains_key(key) {
                    deviating_nodes.insert(i);
                }
            }
        }
    }

    let d = deviating_nodes.len();
    let n = round_snaps.len();
    let agreeing = n - d;
    d * agreeing + d * d.saturating_sub(1) / 2
}

fn check_atomic_delivery_inner(trace: &SimulationTrace) -> bool {
    if let Some(last_round) = trace.snapshots_per_round.last() {
        let mut all_keys = std::collections::HashSet::new();
        for (_, snap) in last_round {
            for key in snap.entries.keys() {
                all_keys.insert(key.clone());
            }
        }
        for key in &all_keys {
            let has_it = last_round
                .iter()
                .filter(|(_, snap)| snap.entries.contains_key(key))
                .count();
            if has_it != 0 && has_it != last_round.len() {
                return false;
            }
        }
        true
    } else {
        true
    }
}

fn compute_final_divergence(trace: &SimulationTrace) -> HashMap<String, usize> {
    let mut divergence = HashMap::new();
    if let Some(last_round) = trace.snapshots_per_round.last() {
        let mut all_keys = std::collections::HashSet::new();
        for (_, snap) in last_round {
            for key in snap.entries.keys() {
                all_keys.insert(key.clone());
            }
        }
        for key in &all_keys {
            let mut distinct = std::collections::HashSet::new();
            for (_, snap) in last_round {
                if let Some(val) = snap.entries.get(key) {
                    distinct.insert((val.value.clone(), val.version));
                }
            }
            divergence.insert(key.clone(), distinct.len());
        }
    }
    divergence
}

// ── Property check functions ────────────────────────────────────────────────

pub fn check_delivery_ratio(metrics: &GossipMetrics, expected: f64) -> PropertyResult {
    PropertyResult {
        name: "delivery_ratio".into(),
        category: "Reliability".into(),
        passed: (metrics.delivery_ratio - expected).abs() < 1e-9,
        expected: format!("{expected}"),
        actual: format!("{}", metrics.delivery_ratio),
        description: "Fraction of nodes holding all keys at end".into(),
    }
}

pub fn check_atomic_delivery(metrics: &GossipMetrics) -> PropertyResult {
    PropertyResult {
        name: "atomic_delivery".into(),
        category: "Reliability".into(),
        passed: metrics.atomic_delivery,
        expected: "true".into(),
        actual: format!("{}", metrics.atomic_delivery),
        description: "Per-key: all nodes have it or none do".into(),
    }
}

pub fn check_convergence_bound(
    metrics: &GossipMetrics,
    max_rounds: usize,
) -> PropertyResult {
    let passed = metrics
        .convergence_round
        .map(|r| r <= max_rounds)
        .unwrap_or(false);
    PropertyResult {
        name: "convergence_bound".into(),
        category: "Latency".into(),
        passed,
        expected: format!("≤ {max_rounds}"),
        actual: metrics
            .convergence_round
            .map(|r| r.to_string())
            .unwrap_or("never".into()),
        description: "Convergence within expected round bound".into(),
    }
}

pub fn check_last_node_latency(
    metrics: &GossipMetrics,
    max_gap: usize,
) -> PropertyResult {
    let passed = match (metrics.convergence_round, metrics.last_node_round) {
        (Some(c), Some(l)) => l.abs_diff(c) <= max_gap,
        _ => false,
    };
    PropertyResult {
        name: "last_node_latency".into(),
        category: "Latency".into(),
        passed,
        expected: format!("gap ≤ {max_gap}"),
        actual: format!(
            "convergence={}, last_node={}",
            metrics
                .convergence_round
                .map(|r| r.to_string())
                .unwrap_or("none".into()),
            metrics
                .last_node_round
                .map(|r| r.to_string())
                .unwrap_or("none".into())
        ),
        description: "Last node converges close to overall convergence".into(),
    }
}

pub fn check_total_pushes_eq(
    metrics: &GossipMetrics,
    expected: usize,
) -> PropertyResult {
    PropertyResult {
        name: "total_pushes".into(),
        category: "Message Complexity".into(),
        passed: metrics.total_pushes == expected,
        expected: format!("{expected}"),
        actual: format!("{}", metrics.total_pushes),
        description: "Total push messages equals expected count".into(),
    }
}

pub fn check_redundancy_above(
    metrics: &GossipMetrics,
    min_ratio: f64,
) -> PropertyResult {
    PropertyResult {
        name: "redundancy_ratio".into(),
        category: "Message Complexity".into(),
        passed: metrics.redundancy_ratio > min_ratio,
        expected: format!("> {min_ratio}"),
        actual: format!("{:.3}", metrics.redundancy_ratio),
        description: "Redundancy ratio exceeds threshold".into(),
    }
}

pub fn check_hub_is_hotspot(
    metrics: &GossipMetrics,
    hub_name: &str,
) -> PropertyResult {
    let hub_recv = *metrics.pushes_received_per_node.get(hub_name).unwrap_or(&0);
    let max_recv = metrics
        .pushes_received_per_node
        .values()
        .max()
        .copied()
        .unwrap_or(0);
    PropertyResult {
        name: "hub_hotspot".into(),
        category: "Bandwidth/Load".into(),
        passed: hub_recv == max_recv && hub_recv > 0,
        expected: format!("{hub_name} receives most"),
        actual: format!("{hub_name} received {hub_recv}, max was {max_recv}"),
        description: "Star hub receives the most pushes".into(),
    }
}

pub fn check_load_balance_cv(
    metrics: &GossipMetrics,
    max_cv: f64,
) -> PropertyResult {
    PropertyResult {
        name: "load_balance_cv".into(),
        category: "Bandwidth/Load".into(),
        passed: metrics.load_balance_cv < max_cv,
        expected: format!("< {max_cv}"),
        actual: format!("{:.4}", metrics.load_balance_cv),
        description: "Load balance coefficient of variation".into(),
    }
}

pub fn check_amplification(
    metrics: &GossipMetrics,
    expected_approx: f64,
    tolerance: f64,
) -> PropertyResult {
    let diff = (metrics.amplification_factor - expected_approx).abs();
    PropertyResult {
        name: "amplification_factor".into(),
        category: "Bandwidth/Load".into(),
        passed: diff <= tolerance,
        expected: format!("{expected_approx} ± {tolerance}"),
        actual: format!("{:.2}", metrics.amplification_factor),
        description: "Amplification factor (pushes / nodes)".into(),
    }
}

pub fn check_curve_monotonic(metrics: &GossipMetrics) -> PropertyResult {
    let mono = metrics
        .convergence_curve
        .windows(2)
        .all(|w| w[1] >= w[0] - 1e-9);
    PropertyResult {
        name: "curve_monotonic".into(),
        category: "Convergence".into(),
        passed: mono,
        expected: "monotonically non-decreasing".into(),
        actual: if mono {
            "monotonic".into()
        } else {
            "non-monotonic".into()
        },
        description: "Convergence curve never decreases".into(),
    }
}

pub fn check_curve_s_shape(metrics: &GossipMetrics) -> PropertyResult {
    let curve = &metrics.convergence_curve;
    if curve.len() < 3 {
        return PropertyResult {
            name: "curve_s_shape".into(),
            category: "Convergence".into(),
            passed: false,
            expected: "S-shaped curve".into(),
            actual: "too few data points".into(),
            description: "Convergence curve has S-shape".into(),
        };
    }
    let starts_low = curve[0] < 0.5;
    let ends_high = *curve.last().unwrap() >= 1.0 - 1e-9;
    let has_steep = curve.windows(2).any(|w| (w[1] - w[0]) > 0.05);
    let passed = starts_low && ends_high && has_steep;
    PropertyResult {
        name: "curve_s_shape".into(),
        category: "Convergence".into(),
        passed,
        expected: "starts < 0.5, ends ≥ 1.0, steep middle".into(),
        actual: format!(
            "start={:.2}, end={:.2}, steep={}",
            curve[0],
            curve.last().unwrap(),
            has_steep
        ),
        description: "Convergence curve has S-shape".into(),
    }
}

pub fn check_zero_residue(metrics: &GossipMetrics) -> PropertyResult {
    PropertyResult {
        name: "zero_residue".into(),
        category: "Convergence".into(),
        passed: metrics.residue.abs() < 1e-9,
        expected: "0.0".into(),
        actual: format!("{:.6}", metrics.residue),
        description: "All nodes converged (zero residue)".into(),
    }
}

pub fn check_partition_no_converge(metrics: &GossipMetrics) -> PropertyResult {
    PropertyResult {
        name: "partition_no_converge".into(),
        category: "Fault Tolerance".into(),
        passed: metrics.delivery_ratio < 1.0,
        expected: "< 1.0".into(),
        actual: format!("{}", metrics.delivery_ratio),
        description: "Partitioned network does not fully converge".into(),
    }
}

pub fn check_partition_heals(metrics: &GossipMetrics) -> PropertyResult {
    PropertyResult {
        name: "partition_heals".into(),
        category: "Fault Tolerance".into(),
        passed: (metrics.delivery_ratio - 1.0).abs() < 1e-9,
        expected: "1.0".into(),
        actual: format!("{}", metrics.delivery_ratio),
        description: "Healed partition reaches full convergence".into(),
    }
}

pub fn check_partial_before_heal(
    metrics: &GossipMetrics,
    heal_round: usize,
) -> PropertyResult {
    let before_heal = if heal_round > 0 && heal_round <= metrics.convergence_curve.len() {
        metrics.convergence_curve[heal_round - 1]
    } else {
        1.0
    };
    let at_end = *metrics.convergence_curve.last().unwrap_or(&0.0);
    let passed = before_heal < 1.0 && (at_end - 1.0).abs() < 1e-9;
    PropertyResult {
        name: "partial_before_heal".into(),
        category: "Fault Tolerance".into(),
        passed,
        expected: "< 1.0 before heal, 1.0 after".into(),
        actual: format!("before_heal={before_heal:.2}, end={at_end:.2}"),
        description: "Partial convergence before healing, full after".into(),
    }
}

pub fn check_sublinear_scaling(
    convergence_times: &[(usize, usize)],
) -> PropertyResult {
    let mut sorted: Vec<(usize, usize)> = convergence_times.to_vec();
    sorted.sort_by_key(|&(n, _)| n);
    let passed = if sorted.len() >= 2 {
        let mut all_sublinear = true;
        for i in 1..sorted.len() {
            let (n1, t1) = sorted[i - 1];
            let (n2, t2) = sorted[i];
            if n2 > n1 && t1 > 0 {
                let n_ratio = n2 as f64 / n1 as f64;
                let t_ratio = t2 as f64 / t1 as f64;
                if t_ratio >= n_ratio {
                    all_sublinear = false;
                    break;
                }
            }
        }
        all_sublinear
    } else {
        false
    };
    PropertyResult {
        name: "sublinear_scaling".into(),
        category: "Scalability".into(),
        passed,
        expected: "convergence time scales sublinearly".into(),
        actual: format!("{:?}", convergence_times),
        description: "Doubling N does not double convergence time".into(),
    }
}

pub fn check_linear_message_scaling(
    pushes_per_n: &[(usize, usize)],
    fixed_rounds: usize,
) -> PropertyResult {
    let ratios: Vec<f64> = pushes_per_n
        .iter()
        .map(|&(n, p)| p as f64 / n as f64)
        .collect();
    let cv = coeff_of_variation(&ratios);
    let passed = cv < 0.15;
    PropertyResult {
        name: "linear_message_scaling".into(),
        category: "Scalability".into(),
        passed,
        expected: format!("pushes/N ≈ {fixed_rounds}, CV < 0.15"),
        actual: format!("ratios={:?}, CV={cv:.4}", ratios),
        description: "Total messages scale linearly with N".into(),
    }
}

pub fn check_one_push_per_node_per_round(
    metrics: &GossipMetrics,
    num_rounds_checked: usize,
) -> PropertyResult {
    let expected_total = metrics.num_nodes * num_rounds_checked;
    let passed = metrics.total_pushes <= expected_total;
    PropertyResult {
        name: "one_push_per_node_per_round".into(),
        category: "Push Protocol".into(),
        passed,
        expected: format!("≤ {expected_total}"),
        actual: format!("{}", metrics.total_pushes),
        description: "At most one push per node per round".into(),
    }
}

pub fn check_no_push_without_peers(
    trace: &SimulationTrace,
    node_name: &str,
) -> PropertyResult {
    let has_push = trace.events.iter().any(|e| {
        e.node_name == node_name
            && matches!(e.kind, GossipEventKind::GossipRoundStarted { .. })
    });
    let has_no_peers = trace.events.iter().any(|e| {
        e.node_name == node_name && matches!(e.kind, GossipEventKind::GossipRoundNoPeers)
    });
    PropertyResult {
        name: "no_push_without_peers".into(),
        category: "Push Protocol".into(),
        passed: !has_push && has_no_peers,
        expected: "only GossipRoundNoPeers".into(),
        actual: format!("has_push={has_push}, has_no_peers={has_no_peers}"),
        description: "Node without peers emits NoPeers, not Push".into(),
    }
}

pub fn check_peer_selection_uniform(
    metrics: &GossipMetrics,
    chi_squared_critical: f64,
) -> PropertyResult {
    let mut worst_chi2 = 0.0f64;
    let mut worst_node = String::new();
    for (node, targets) in &metrics.peer_selection_distribution {
        if targets.is_empty() {
            continue;
        }
        let counts: Vec<f64> = targets.values().map(|&c| c as f64).collect();
        let chi2 = chi_squared_uniform(&counts);
        if chi2 > worst_chi2 {
            worst_chi2 = chi2;
            worst_node = node.clone();
        }
    }
    PropertyResult {
        name: "peer_selection_uniform".into(),
        category: "Peer Selection".into(),
        passed: worst_chi2 < chi_squared_critical,
        expected: format!("χ² < {chi_squared_critical}"),
        actual: format!("worst χ²={worst_chi2:.2} at {worst_node}"),
        description: "Peer selection approximately uniform (chi-squared)".into(),
    }
}

pub fn check_lww_single_value(metrics: &GossipMetrics) -> PropertyResult {
    let all_single = metrics
        .final_value_divergence
        .values()
        .all(|&count| count == 1);
    let details: Vec<String> = metrics
        .final_value_divergence
        .iter()
        .filter(|(_, c)| **c != 1)
        .map(|(k, c)| format!("{k}:{c}"))
        .collect();
    PropertyResult {
        name: "lww_single_value".into(),
        category: "Consistency".into(),
        passed: all_single,
        expected: "1 distinct value per key".into(),
        actual: if all_single {
            "all keys have 1 value".into()
        } else {
            format!("divergent: {:?}", details)
        },
        description: "LWW ensures single final value per key".into(),
    }
}

pub fn check_entropy_zero_at_convergence(
    metrics: &GossipMetrics,
) -> PropertyResult {
    let passed = if let Some(cr) = metrics.convergence_round {
        metrics
            .entropy_per_round
            .iter()
            .skip(cr.saturating_sub(1))
            .all(|&e| e == 0)
    } else {
        false
    };
    PropertyResult {
        name: "entropy_zero_at_convergence".into(),
        category: "Consistency".into(),
        passed,
        expected: "entropy = 0 after convergence".into(),
        actual: format!(
            "convergence_round={:?}, final_entropy={}",
            metrics.convergence_round,
            metrics.entropy_per_round.last().unwrap_or(&0)
        ),
        description: "Entropy reaches zero at convergence".into(),
    }
}

pub fn check_entropy_decreases(metrics: &GossipMetrics) -> PropertyResult {
    let mono = metrics
        .entropy_per_round
        .windows(2)
        .all(|w| w[1] <= w[0]);
    PropertyResult {
        name: "entropy_decreases".into(),
        category: "Consistency".into(),
        passed: mono,
        expected: "monotonically non-increasing".into(),
        actual: if mono {
            "monotonic".into()
        } else {
            let violations: Vec<usize> = metrics
                .entropy_per_round
                .windows(2)
                .enumerate()
                .filter(|(_, w)| w[1] > w[0])
                .map(|(i, _)| i + 1)
                .collect();
            format!("increases at rounds {:?}", violations)
        },
        description: "Entropy never increases".into(),
    }
}

pub fn check_no_stale_reads(metrics: &GossipMetrics) -> PropertyResult {
    let all_single = metrics
        .final_value_divergence
        .values()
        .all(|&c| c == 1);
    let passed = all_single && (metrics.delivery_ratio - 1.0).abs() < 1e-9;
    PropertyResult {
        name: "no_stale_reads".into(),
        category: "Consistency".into(),
        passed,
        expected: "all nodes agree post-convergence".into(),
        actual: format!(
            "delivery={}, all_single={}",
            metrics.delivery_ratio, all_single
        ),
        description: "No stale reads after convergence".into(),
    }
}

pub fn check_state_size_stabilizes(
    metrics: &GossipMetrics,
    expected_final: f64,
) -> PropertyResult {
    let final_avg = metrics.avg_state_size_per_round.last().copied().unwrap_or(0.0);
    let passed = (final_avg - expected_final).abs() < 0.5;
    PropertyResult {
        name: "state_size_stabilizes".into(),
        category: "Practical".into(),
        passed,
        expected: format!("{expected_final}"),
        actual: format!("{final_avg:.2}"),
        description: "Final average state size matches key count".into(),
    }
}

pub fn check_state_size_monotonic(metrics: &GossipMetrics) -> PropertyResult {
    let mono = metrics
        .avg_state_size_per_round
        .windows(2)
        .all(|w| w[1] >= w[0] - 1e-9);
    PropertyResult {
        name: "state_size_monotonic".into(),
        category: "Practical".into(),
        passed: mono,
        expected: "non-decreasing".into(),
        actual: if mono {
            "monotonic".into()
        } else {
            "non-monotonic".into()
        },
        description: "Average state size never decreases".into(),
    }
}

// ── Helper: base GossipSimConfig ──────────────────────────────────────────────

pub fn base_config(
    name: &str,
    topology: Topology,
    num_nodes: usize,
    num_keys: usize,
) -> GossipSimConfig {
    GossipSimConfig {
        name: name.into(),
        topology,
        num_nodes,
        initial_data: (0..num_keys)
            .map(|i| (format!("key-{i}"), format!("value-{i}").into_bytes()))
            .collect(),
        num_rounds: 30,
        ticks_per_round: 4,
        heal_after_round: None,
        num_threads: 1,
    }
}
