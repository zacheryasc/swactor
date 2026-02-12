use std::collections::BTreeMap;
use std::fs;
use std::io;

use serde::Deserialize;
use simulation::gossip::sim::GossipSimConfig;
use simulation::topology::Topology;

#[derive(Deserialize)]
pub struct SimFileConfig {
    pub name: String,
    pub topology: String,
    pub num_nodes: usize,
    pub num_rounds: usize,
    pub ticks_per_round: usize,
    pub num_threads: usize,
    pub heal_after_round: Option<usize>,
    pub initial_data: Option<BTreeMap<String, String>>,
}

impl SimFileConfig {
    pub fn load(path: &str) -> io::Result<Self> {
        let contents = fs::read_to_string(path)?;
        toml::from_str(&contents).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }

    pub fn into_sim_config(self) -> GossipSimConfig {
        let topology = match self.topology.to_lowercase().as_str() {
            "ring" => Topology::Ring,
            "star" => Topology::Star,
            "full_mesh" => Topology::FullMesh,
            "chain" => Topology::Chain,
            "partitioned" => Topology::Partitioned,
            other => panic!("unknown topology: {other:?} (expected ring, star, full_mesh, chain, or partitioned)"),
        };

        let initial_data = self
            .initial_data
            .unwrap_or_default()
            .into_iter()
            .map(|(k, v)| (k, v.into_bytes()))
            .collect();

        GossipSimConfig {
            name: self.name,
            topology,
            num_nodes: self.num_nodes,
            initial_data,
            num_rounds: self.num_rounds,
            ticks_per_round: self.ticks_per_round,
            heal_after_round: self.heal_after_round,
            num_threads: self.num_threads,
        }
    }
}
