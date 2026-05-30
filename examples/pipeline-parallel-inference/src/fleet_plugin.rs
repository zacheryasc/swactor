//! Orchestrator "fleet" dashboard plugin — a remote-sourced vast.ai view.
//!
//! In production the diagnostics collector runs on a VPS: the stage containers
//! ship their vast.ai/host-metric records to it. The orchestrator runs locally
//! and hosts the *full* swactor dashboard (overview / actors / topology /
//! distribution / netmap). To surface the fleet alongside those live views, this
//! plugin **subscribes** to the collector's raw record stream
//! (`GET {collector}/diag/stream/{run_id}`, an SSE feed of `LiveRecord`s) and
//! folds the vast.ai records locally — reusing the dashboard's own server-side
//! [`VastaiLivePlugin`] fold — then serves the result same-origin at
//! `/api/plugin/vastai/model` and over `/events` (the `vastai` event), so the
//! Fleet page renders without any cross-origin calls to the collector.
//!
//! It is the read mirror of the orchestrator's distribution broadcaster
//! ([`crate::dist_broadcast`]), which *pushes* its snapshot to the same
//! collector. The stream loop reconnects on drops and tolerates an unreachable
//! collector: until records arrive, the Fleet tab simply waits.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use dashboard::live_collector::VastaiLivePlugin;
use dashboard::plugin::{DashboardPlugin, PluginResponse};
use distribution::diagnostics::collector::protocol::{LiveRecord, RecordKind};
use futures_util::StreamExt;

/// Rebuild a [`LiveRecord`] from one stream frame's `data:` JSON. `LiveRecord` is
/// a serialize-only wire type, so we parse the fields by hand. Returns `None` for
/// non-vast.ai kinds (e.g. the pushed dist snapshot) so we skip them cheaply.
fn parse_vastai_record(data: &str) -> Option<LiveRecord> {
    let v: serde_json::Value = serde_json::from_str(data).ok()?;
    let kind = RecordKind::parse(v.get("kind")?.as_str()?)?;
    if !kind.is_vastai() {
        return None;
    }
    Some(LiveRecord {
        run_id: v.get("run_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        node_id: v.get("node_id").and_then(|x| x.as_str()).unwrap_or("").to_string(),
        kind,
        recv_ms: v.get("recv_ms").and_then(|x| x.as_u64()).unwrap_or(0),
        seq: v.get("seq").and_then(|x| x.as_u64()).unwrap_or(0),
        body: v.get("body").cloned().unwrap_or(serde_json::Value::Null),
    })
}

/// The Fleet page. Reuses the Swactor Runtime Dashboard layout/CSS so it sits
/// beside the live tabs (no scrubber / Live control — this is a live-only view).
const FLEET_HTML: &str = include_str!("fleet_page.html");

/// Dashboard plugin (name `"vastai"`) backing `/plugin/vastai`. Wraps the
/// dashboard's [`VastaiLivePlugin`] (which buffers records and serves the fold)
/// and feeds it from the remote collector's record stream; overrides only the
/// HTML page so the Fleet tab wears the dashboard chrome.
pub struct RemoteVastaiPlugin {
    inner: Arc<VastaiLivePlugin>,
}

impl RemoteVastaiPlugin {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(VastaiLivePlugin::new()),
        }
    }

    /// Subscribe to the collector's `/diag/stream/{run_id}` SSE feed and fold each
    /// vast.ai `LiveRecord` into the inner plugin. Reconnects every ~2s on drop or
    /// while the collector is unreachable. Ends when `rt`'s runtime is dropped at
    /// end of run. `run_id` must match what the stages ship under
    /// (`SWACTOR_DIAG_RUN_ID`).
    pub fn spawn_stream(&self, rt: &tokio::runtime::Handle, collector_url: String, run_id: String) {
        let url = format!(
            "{}/diag/stream/{}",
            collector_url.trim_end_matches('/'),
            run_id
        );
        let inner = Arc::clone(&self.inner);
        eprintln!("pp-orchestrator: fleet tab streaming vast.ai records from {url}");
        rt.spawn(async move {
            let http = reqwest::Client::new();
            // Log the first record and the first error once, so a blank Fleet tab
            // is easy to localize (collector empty vs. stream unreachable) without
            // spamming the orchestrator log.
            let mut logged_first = false;
            let mut logged_err = false;
            loop {
                match http.get(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        let mut stream = resp.bytes_stream();
                        let mut buf: Vec<u8> = Vec::new();
                        while let Some(chunk) = stream.next().await {
                            let Ok(chunk) = chunk else { break };
                            buf.extend_from_slice(&chunk);
                            // SSE frames are separated by a blank line. Parse on
                            // byte boundaries so a chunk split mid-frame is safe.
                            while let Some(idx) = buf.windows(2).position(|w| w == b"\n\n") {
                                let frame: Vec<u8> = buf.drain(..idx + 2).collect();
                                let Ok(text) = std::str::from_utf8(&frame) else {
                                    continue;
                                };
                                for line in text.lines() {
                                    let Some(data) = line.strip_prefix("data:") else {
                                        continue;
                                    };
                                    let data = data.trim_start();
                                    if let Some(rec) = parse_vastai_record(data) {
                                        inner.ingest(&rec);
                                        if !logged_first {
                                            logged_first = true;
                                            eprintln!(
                                                "pp-orchestrator: fleet tab is receiving vast.ai records from the collector"
                                            );
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(resp) => {
                        if !logged_err {
                            logged_err = true;
                            eprintln!("pp-orchestrator: fleet stream got HTTP {}", resp.status());
                        }
                    }
                    Err(e) => {
                        if !logged_err {
                            logged_err = true;
                            eprintln!("pp-orchestrator: fleet stream error (collector reachable?): {e}");
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(2)).await;
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
        self.inner.snapshot_json()
    }

    fn handle_request(
        &self,
        method: &str,
        path: &str,
        query: &HashMap<String, String>,
        body: &[u8],
    ) -> PluginResponse {
        self.inner.handle_request(method, path, query, body)
    }

    fn html_page(&self) -> Option<&str> {
        Some(FLEET_HTML)
    }
}
