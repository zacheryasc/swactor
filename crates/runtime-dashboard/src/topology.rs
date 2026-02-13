//! Actor-to-actor (and worker-to-worker) message flow topology.
//!
//! Currently derives topology from per-worker cross_sends/local_sends stats.
//! Future: sample-based per-actor source→destination tracking with core instrumentation.

use swactor::stats::RuntimeStats;

/// An edge in the topology graph.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopologyEdge {
    pub source: String,
    pub target: String,
    pub weight: u64,
    pub label: String,
}

/// A node in the topology graph.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopologyNode {
    pub id: String,
    pub label: String,
    pub actor_count: usize,
    pub group: usize,
}

/// A snapshot of the current topology.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopologySnapshot {
    pub nodes: Vec<TopologyNode>,
    pub edges: Vec<TopologyEdge>,
}

/// Build a worker-level topology from RuntimeStats.
///
/// Workers are nodes, edges represent message flow:
/// - Self-loops for local_sends
/// - Cross-edges distributed proportionally (until per-destination tracking exists)
pub fn worker_topology(stats: &RuntimeStats) -> TopologySnapshot {
    let mut nodes = Vec::new();
    let mut edges = Vec::new();

    for w in &stats.workers {
        nodes.push(TopologyNode {
            id: format!("w{}", w.id),
            label: format!("W{}", w.id),
            actor_count: w.num_actors,
            group: w.id,
        });

        // Local sends = self-loop
        if w.local_sends > 0 {
            edges.push(TopologyEdge {
                source: format!("w{}", w.id),
                target: format!("w{}", w.id),
                weight: w.local_sends,
                label: format!("{} local", w.local_sends),
            });
        }

        // Cross sends — without per-destination data, distribute evenly to other workers
        if w.cross_sends > 0 && stats.workers.len() > 1 {
            let others: Vec<&swactor::stats::WorkerInfo> =
                stats.workers.iter().filter(|o| o.id != w.id).collect();
            let per_worker = w.cross_sends / others.len() as u64;
            let remainder = w.cross_sends % others.len() as u64;

            for (i, other) in others.iter().enumerate() {
                let count = per_worker + if (i as u64) < remainder { 1 } else { 0 };
                if count > 0 {
                    edges.push(TopologyEdge {
                        source: format!("w{}", w.id),
                        target: format!("w{}", other.id),
                        weight: count,
                        label: format!("{} cross", count),
                    });
                }
            }
        }
    }

    TopologySnapshot { nodes, edges }
}
