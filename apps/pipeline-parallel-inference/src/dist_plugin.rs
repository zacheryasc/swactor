//! Orchestrator distribution dashboard plugin.
//!
//! [`DistDashPlugin`] — a read-only [`DashboardPlugin`] named `"distribution"`,
//! which otherwise has no backing (the dashboard ships the nav link but nothing
//! serves `/plugin/distribution`). It serves a cached
//! [`DistributionNodeSnapshot`] (SWIM members, routing table, location cache,
//! name registry) plus a lean HTML page that draws a SWIM membership graph.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dashboard::plugin::{DashboardPlugin, PluginResponse};
use distribution::snapshot::DistributionNodeSnapshot;

/// Shared snapshot cell the orchestrator refreshes each tick.
pub type SharedSnapshot = Arc<Mutex<Option<DistributionNodeSnapshot>>>;

/// Render a distribution snapshot as the JSON the distribution UI consumes,
/// served by [`DistDashPlugin`].
pub fn render_dist_json(snap: &DistributionNodeSnapshot) -> Option<String> {
    serde_json::to_string(snap).ok()
}

/// Read-only dashboard plugin backing `/plugin/distribution`.
pub struct DistDashPlugin {
    cached: SharedSnapshot,
}

impl DistDashPlugin {
    pub fn new(cached: SharedSnapshot) -> Self {
        Self { cached }
    }

    /// Serialize the cached snapshot.
    fn rendered_json(&self) -> Option<String> {
        let guard = self.cached.lock().unwrap();
        let snap = guard.as_ref()?;
        render_dist_json(snap)
    }
}

const DIST_HTML: &str = include_str!("dist_page.html");

impl DashboardPlugin for DistDashPlugin {
    fn name(&self) -> &str {
        "distribution"
    }

    fn snapshot_json(&self) -> Option<String> {
        self.rendered_json()
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
                PluginResponse::json(self.rendered_json().unwrap_or_else(|| "{}".into()))
            }
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(DIST_HTML)
    }
}
