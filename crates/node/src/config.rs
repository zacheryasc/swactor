//! TOML configuration file support for the swactor node binary.
//!
//! CLI flags take precedence over config file values, which take
//! precedence over compiled defaults.

use serde::Deserialize;

#[derive(Deserialize, Default)]
#[allow(dead_code)] // fields may be unused depending on feature flags
pub struct SwactorNodeConfig {
    pub transport: Option<String>,
    pub dashboard_port: Option<u16>,
    pub identity_dir: Option<String>,
    pub peers_file: Option<String>,
    pub seed_node_id: Option<String>,
    pub seed: Option<String>,
    pub listen: Option<String>,
    pub node_name: Option<String>,
    pub actors: Option<usize>,
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
