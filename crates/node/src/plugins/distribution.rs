//! Distribution plugin adapter for the dashboard plugin system.
//!
//! Wraps a cached `DistributionNodeSnapshot` into a `DashboardPlugin` that the
//! dashboard can poll for snapshots and serve the distribution page.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dashboard::plugin::{DashboardPlugin, PluginResponse};
use distribution::snapshot::DistributionNodeSnapshot;

/// HTML page for the distribution plugin.
const DISTRIBUTION_HTML: &str = include_str!("distribution_page.html");

/// Dashboard plugin that exposes distribution node snapshots.
pub struct DistributionPlugin {
    cached: Arc<Mutex<Option<DistributionNodeSnapshot>>>,
}

impl DistributionPlugin {
    pub fn new(cached: Arc<Mutex<Option<DistributionNodeSnapshot>>>) -> Self {
        Self { cached }
    }
}

impl DashboardPlugin for DistributionPlugin {
    fn name(&self) -> &str {
        "distribution"
    }

    fn snapshot_json(&self) -> Option<String> {
        let guard = self.cached.lock().unwrap();
        let snapshot = guard.as_ref()?;
        serde_json::to_string(snapshot).ok()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        _body: &[u8],
    ) -> PluginResponse {
        match (method, path) {
            ("GET", "") => {
                let guard = self.cached.lock().unwrap();
                match guard.as_ref() {
                    Some(snapshot) => match serde_json::to_string(snapshot) {
                        Ok(json) => PluginResponse::json(json),
                        Err(_) => PluginResponse::json("{}".into()),
                    },
                    None => PluginResponse::json("{}".into()),
                }
            }
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(DISTRIBUTION_HTML)
    }
}
