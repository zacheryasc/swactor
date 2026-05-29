//! Orchestrator "fleet" dashboard plugin — a remote-sourced vast.ai view.
//!
//! In production the diagnostics collector runs on a VPS: the stage containers
//! ship their vast.ai/host-metric records to it, and it folds them into a fleet
//! board model. The orchestrator runs locally and hosts the *full* swactor
//! dashboard (overview / actors / topology / distribution / netmap). To surface
//! the fleet alongside those live views, this plugin **pulls** the collector's
//! already-folded model (`GET {collector}/api/plugin/vastai/model`) ~1/s and
//! re-serves it verbatim under the same `"vastai"` name — so the Fleet page
//! renders identically to the standalone collector board, with no extra ingest.
//!
//! It is the read mirror of the orchestrator's distribution broadcaster
//! ([`crate::dist_broadcast`]), which *pushes* its snapshot to the same
//! collector. The poll loop tolerates an unreachable collector: a failed fetch
//! leaves the last good model in place, so a transient blip never blanks the UI.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dashboard::plugin::{DashboardPlugin, PluginResponse};

/// The Fleet page. Reuses the standalone collector board's Fleet renderer but
/// with the orchestrator dashboard's nav, so it sits beside the live tabs.
const FLEET_HTML: &str = include_str!("fleet_page.html");

/// Read-only dashboard plugin backing `/plugin/vastai`. Holds the last good fleet
/// model JSON pulled from the remote collector; serves it to the SSE stream
/// (event `vastai`) and the seed API (`/api/plugin/vastai/model`).
pub struct RemoteVastaiPlugin {
    /// Last successfully fetched fleet model JSON (the collector's folded board).
    cached: Arc<Mutex<Option<String>>>,
}

impl RemoteVastaiPlugin {
    pub fn new() -> Self {
        Self {
            cached: Arc::new(Mutex::new(None)),
        }
    }

    /// Spawn the poll loop on `rt`. Every ~1s it GETs
    /// `{collector_url}/api/plugin/vastai/model`; a successful response with a
    /// non-`null` body replaces the cache, anything else (error, non-2xx, empty,
    /// `null`) leaves the last good model untouched. The task ends when `rt`'s
    /// runtime is dropped at end of run. Cadence matches [`crate::dist_broadcast`].
    pub fn spawn_poller(&self, rt: &tokio::runtime::Handle, collector_url: String) {
        let url = format!(
            "{}/api/plugin/vastai/model",
            collector_url.trim_end_matches('/')
        );
        let cached = Arc::clone(&self.cached);
        rt.spawn(async move {
            let http = reqwest::Client::new();
            let mut ticker = tokio::time::interval(Duration::from_secs(1));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                let body = match http.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => resp.text().await.ok(),
                    _ => None,
                };
                if let Some(body) = body {
                    let trimmed = body.trim();
                    if !trimmed.is_empty() && trimmed != "null" {
                        *cached.lock().unwrap() = Some(body);
                    }
                }
            }
        });
    }
}

impl Default for RemoteVastaiPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl DashboardPlugin for RemoteVastaiPlugin {
    fn name(&self) -> &str {
        "vastai"
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
        // The page seeds from `/api/plugin/vastai/model` (the route requires a
        // non-empty trailing segment), matching the standalone collector board.
        match (method, path) {
            ("GET", "" | "model") => PluginResponse::json(
                self.cached
                    .lock()
                    .unwrap()
                    .clone()
                    .unwrap_or_else(|| "null".into()),
            ),
            _ => PluginResponse::not_found(),
        }
    }

    fn html_page(&self) -> Option<&str> {
        Some(FLEET_HTML)
    }
}
