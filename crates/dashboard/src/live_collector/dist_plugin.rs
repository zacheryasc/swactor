//! Distribution plugin for the remote dashboard.
//!
//! Unlike the orchestrator's in-process `DistDashPlugin` (which reads a live
//! snapshot directly), this plugin runs on the dashboard/collector host and only
//! ever sees the orchestrator's distribution snapshot as a **pushed** JSON body —
//! the orchestrator POSTs it to `/diag/snapshot` under the synthetic node-id
//! `pp-distribution`. We cache the last body verbatim and re-serve it, so the
//! existing distribution UI (same `"distribution"` event name + JSON shape) works
//! unchanged.

use std::collections::HashMap;
use std::sync::Mutex;

use distribution::diagnostics::collector::protocol::LiveRecord;

use crate::plugin::{DashboardPlugin, PluginResponse};

/// The synthetic collector node-id the orchestrator's distribution broadcaster
/// files its snapshots under (kept out of the real swactor node directory).
pub const DIST_NODE_ID: &str = "pp-distribution";

/// Dashboard plugin (name `"distribution"`) that re-serves the last pushed
/// orchestrator distribution snapshot.
pub struct PushedDistPlugin {
    /// Last received rendered JSON (snapshot + `msg_counts`), serialized.
    cached: Mutex<Option<String>>,
}

impl PushedDistPlugin {
    pub fn new() -> Self {
        PushedDistPlugin {
            cached: Mutex::new(None),
        }
    }

    /// Cache the pushed snapshot body verbatim.
    pub fn ingest(&self, rec: &LiveRecord) {
        if let Ok(s) = serde_json::to_string(&rec.body) {
            *self.cached.lock().unwrap() = Some(s);
        }
    }
}

impl Default for PushedDistPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl DashboardPlugin for PushedDistPlugin {
    fn name(&self) -> &str {
        "distribution"
    }

    fn snapshot_json(&self) -> Option<String> {
        self.cached.lock().unwrap().clone()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        _query: &HashMap<String, String>,
        _body: &[u8],
    ) -> PluginResponse {
        // The `/api/plugin/{name}/{*rest}` route requires a non-empty segment, so
        // the page seeds from `/api/plugin/distribution/snapshot`.
        match (method, path) {
            ("GET", "" | "snapshot") => PluginResponse::json(
                self.cached
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "{}".into()),
            ),
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(super::FLEET_HTML)
    }
}
