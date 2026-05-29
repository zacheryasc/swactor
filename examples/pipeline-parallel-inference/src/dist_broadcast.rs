//! Distribution snapshot broadcaster.
//!
//! The orchestrator's distribution view (SWIM membership, routing, registry,
//! message counts) is normally rendered by the in-process [`DistDashPlugin`].
//! When the dashboard runs *remotely* (the `dashboard_collector` on a VPS), the
//! orchestrator instead POSTs the same rendered JSON to the collector ~1/s under
//! a synthetic node-id, and the remote `PushedDistPlugin` caches and re-serves it.
//!
//! Reusing [`render_dist_json`] guarantees the broadcast bytes are identical to
//! what the local plugin would produce.
//!
//! [`DistDashPlugin`]: crate::dist_plugin::DistDashPlugin

use std::sync::Arc;
use std::time::Duration;

use crate::dist_plugin::{render_dist_json, MsgCounts, SharedSnapshot};

/// Synthetic collector node-id the snapshot is filed under. Kept out of the real
/// swactor node directory (same trick as `vastai-external`) so it never collides
/// with a SWIM peer and the diagnostics bundle stays clean. Must match the
/// dashboard's `live_collector::DIST_NODE_ID`.
pub const DIST_NODE_ID: &str = "pp-distribution";

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Spawn the broadcast loop on `rt`. It POSTs the latest cached distribution
/// snapshot to `{dashboard_url}/diag/snapshot` once per second; ticks where the
/// snapshot cell is empty (e.g. before convergence) are skipped. The task ends
/// when `rt`'s runtime is dropped at end of run.
pub fn spawn_dist_broadcast(
    rt: tokio::runtime::Handle,
    cached: SharedSnapshot,
    counts: Arc<MsgCounts>,
    dashboard_url: String,
    run_id: String,
) {
    let url = format!("{}/diag/snapshot", dashboard_url.trim_end_matches('/'));
    rt.spawn(async move {
        let http = reqwest::Client::new();
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        loop {
            interval.tick().await;
            // Render under the lock, then drop the guard before awaiting the POST.
            let body = {
                let guard = cached.lock().unwrap();
                guard.as_ref().and_then(|snap| render_dist_json(snap, &counts))
            };
            let Some(body) = body else { continue };
            let _ = http
                .post(&url)
                .header("x-run-id", run_id.as_str())
                .header("x-node-id", DIST_NODE_ID)
                .header("x-node-send-ms", now_ms().to_string())
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await;
        }
    });
}
