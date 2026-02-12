use crate::topology::Topology;

/// Generic simulation configuration (protocol-agnostic).
#[derive(Debug, Clone)]
pub struct SimConfig {
    pub name: String,
    pub topology: Topology,
    pub num_nodes: usize,
    pub num_rounds: usize,
    pub ticks_per_round: usize,
    /// If `Some(r)`, cross-partition links are added after round `r`.
    pub heal_after_round: Option<usize>,
    /// Number of worker threads: 1 = deterministic single-threaded.
    pub num_threads: usize,
}
