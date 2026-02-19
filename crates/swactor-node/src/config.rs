//! TOML configuration file support for swactor-node.
//!
//! CLI flags take precedence over config file values, which take
//! precedence over compiled defaults.

use serde::Deserialize;

#[derive(Deserialize, Default)]
#[allow(dead_code)] // fields may be unused depending on feature flags
pub struct SwactorNodeConfig {
    pub transport: Option<String>,
    pub dashboard_port: Option<u16>,
    pub storage_path: Option<String>,
    pub identity_dir: Option<String>,
    pub peers_file: Option<String>,
    pub seed_node_id: Option<String>,
    pub seed: Option<String>,
    pub listen: Option<String>,
    pub auth: Option<bool>,
    pub auth_dir: Option<String>,
    pub actors: Option<usize>,
    pub chunk_size: Option<u32>,
    pub gc_interval: Option<u64>,
    pub disseminate_interval: Option<u64>,
    pub no_datastore: Option<bool>,
    pub node_name: Option<String>,
    pub relay: Option<bool>,
    pub relay_port: Option<u16>,
    pub relay_bind: Option<String>,
    pub relay_hosts: Option<Vec<String>>,
}

impl SwactorNodeConfig {
    pub fn load(path: &std::path::Path) -> Result<Self, String> {
        let contents = std::fs::read_to_string(path)
            .map_err(|e| format!("failed to read config file {}: {e}", path.display()))?;
        toml::from_str(&contents)
            .map_err(|e| format!("failed to parse config file {}: {e}", path.display()))
    }
}
