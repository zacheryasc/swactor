//! Standalone HTTP collector for diagnostic records.
//!
//! Architecturally, this is the **A.2** component described in
//! `examples/pipeline-parallel-inference/DIAGNOSTICS_PLAN.md` — a tiny
//! HTTP server deployed once to a stable VPS that every node POSTs
//! into. The collector does not interpret records; it stores them and
//! produces a tarball at end of run. All interpretation lives in the
//! (later) post-processor binary.
//!
//! ## Wire shape (T1.6)
//!
//! Every POST carries `x-run-id`, `x-node-id`, and `x-node-send-ms`
//! headers, plus a JSON body (kind-specific). Every response is a
//! [`protocol::PostAck`] with the [`protocol::ClockEcho`] timing block
//! used by [`crate::diagnostics`] to align clocks (T1.5), and an
//! optional [`protocol::Hints`] block used by the snapshot pull-trigger
//! mechanism (T1.4 — wired in S5).
//!
//! ## Storage layout
//!
//! ```text
//! {root}/
//!   {run_id}/
//!     {node_id}/
//!       boot-000001.json
//!       events-000001.json
//!       events-000002.json
//!       snapshot-000001.json
//!       finalize-000001.json
//!   bundles/
//!     {run_id}.tar.gz
//! ```
//!
//! The bundle is rewritten into the spec layout (role-named dirs,
//! `MANIFEST.json` at root) on assembly — see [`bundle`].
//!
//! ## What's here vs. what's in later stages
//!
//! - **S2:** server, storage, on-finalize tarball.
//! - **S3:** the client-side `HttpSink` that posts into this server
//!   and spools to disk when the server is unreachable.
//! - **S5 (this stage):** snapshot pull-trigger plumbing — finalize
//!   marks every reporter for `snapshot_now`, waits
//!   [`state::CollectorState::finalize_wait`] for stragglers, then
//!   tars. Non-finalize POSTs return any pending hint and clear it.

pub mod bundle;
pub mod handlers;
pub mod protocol;
pub mod state;
pub mod udp_echo;

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::net::TcpListener;

pub use handlers::router;
pub use protocol::{ClockEcho, Hints, Manifest, ManifestNode, PostAck, RecordKind};
pub use state::CollectorState;
pub use udp_echo::{UdpEchoHandle, spawn_udp_echo};

/// Configuration for a running collector.
#[derive(Debug, Clone)]
pub struct CollectorConfig {
    /// Filesystem root for record storage and bundles. Must be
    /// writable; created on first POST.
    pub root: PathBuf,
    /// Address to bind the HTTP listener to.
    pub bind: SocketAddr,
}

/// Bind the HTTP listener without serving, so callers (binary, tests)
/// can learn the actual address before kicking the server off. Useful
/// when binding to port 0.
pub async fn bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    TcpListener::bind(addr).await
}

/// Run the server to completion. Returns when the listener errors or
/// the task is cancelled.
pub async fn serve(listener: TcpListener, state: Arc<CollectorState>) -> std::io::Result<()> {
    let app = router(state);
    axum::serve(listener, app).await
}

/// Current wall clock in milliseconds since the UNIX epoch. Shared
/// helper for the handlers; pinned in one place so a future move to
/// monotonic-base-plus-offset is a single edit.
pub fn wall_ms_now() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
