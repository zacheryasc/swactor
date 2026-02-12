/// Network topology shapes for simulation.
#[derive(Debug, Clone)]
pub enum Topology {
    /// Each node gossips to the next; last gossips to first.
    Ring,
    /// Node 0 is the hub; all others gossip to/from it.
    Star,
    /// Every node gossips to every other node.
    FullMesh,
    /// Unidirectional chain: 0→1→2→…→(n-1).
    Chain,
    /// Two halves with no cross-links (healed later via `heal_after_round`).
    Partitioned,
}

impl Topology {
    /// Compute abstract index-pair edges for `num_nodes` nodes.
    pub fn edges(&self, num_nodes: usize) -> Vec<(usize, usize)> {
        let n = num_nodes;
        let mut edges = Vec::new();

        match self {
            Topology::Ring => {
                for i in 0..n {
                    edges.push((i, (i + 1) % n));
                }
            }
            Topology::Star => {
                for i in 1..n {
                    edges.push((0, i));
                    edges.push((i, 0));
                }
            }
            Topology::FullMesh => {
                for i in 0..n {
                    for j in 0..n {
                        if i != j {
                            edges.push((i, j));
                        }
                    }
                }
            }
            Topology::Chain => {
                for i in 0..n.saturating_sub(1) {
                    edges.push((i, i + 1));
                }
            }
            Topology::Partitioned => {
                let half = n / 2;
                for i in 0..half {
                    for j in 0..half {
                        if i != j {
                            edges.push((i, j));
                        }
                    }
                }
                for i in half..n {
                    for j in half..n {
                        if i != j {
                            edges.push((i, j));
                        }
                    }
                }
            }
        }
        edges
    }

    /// Partition healing edges: bidirectional links between the two halves.
    pub fn heal_edges(&self, num_nodes: usize) -> Vec<(usize, usize)> {
        if !matches!(self, Topology::Partitioned) {
            return Vec::new();
        }
        let half = num_nodes / 2;
        if half > 0 && half < num_nodes {
            vec![(half - 1, half), (half, half - 1)]
        } else {
            Vec::new()
        }
    }
}
