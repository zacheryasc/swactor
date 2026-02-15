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
    ///
    /// Connects up to 3 evenly-spaced node pairs across the partition boundary.
    /// A single bridge link is unreliable under random peer selection: with half=50,
    /// there is only a 1/50 chance per round that the bridge node gossips across,
    /// giving a ~1.7% probability of zero crossings in 200 rounds.
    pub fn heal_edges(&self, num_nodes: usize) -> Vec<(usize, usize)> {
        if !matches!(self, Topology::Partitioned) {
            return Vec::new();
        }
        let half = num_nodes / 2;
        if half == 0 || half >= num_nodes {
            return Vec::new();
        }
        let right = num_nodes - half;
        let num_bridges = half.min(3);
        let mut edges = Vec::with_capacity(num_bridges * 2);
        for b in 0..num_bridges {
            let a_node = b * half / num_bridges;
            let b_node = half + b * right / num_bridges;
            edges.push((a_node, b_node));
            edges.push((b_node, a_node));
        }
        edges
    }
}
