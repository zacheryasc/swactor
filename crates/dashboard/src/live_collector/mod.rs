//! Live collector: turn the dashboard process into the HTTP diagnostics collector
//! *and* a unified live SSE UI.
//!
//! The dashboard already fans plugin snapshots onto `/events` every ~200ms. Here
//! we additionally:
//!   1. own an [`Arc<CollectorState>`] and merge the existing collector router
//!      (`/diag/*` ingest, unchanged) onto the dashboard's listener — so the real
//!      shippers POST into this process and get the `PostAck` clock-echo they rely
//!      on;
//!   2. subscribe to the collector's broadcast and fan each [`LiveRecord`] to the
//!      right plugin: vast.ai records fold into the fleet board, the orchestrator's
//!      pushed distribution snapshot is cached and re-served.
//!
//! See [`vastai_model`] for the record→board fold and [`dist_plugin`] for the
//! pushed-snapshot distribution plugin.

pub mod dist_plugin;
pub mod vastai_model;

use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::broadcast::error::RecvError;

use distribution::diagnostics::collector::protocol::RecordKind;
use distribution::diagnostics::collector::state::CollectorState;

use crate::DashboardHandle;

pub use dist_plugin::{PushedDistPlugin, DIST_NODE_ID};
pub use vastai_model::{fold, VastaiBoardModel, VastaiLivePlugin};

/// The unified fleet board: one tabbed page (Fleet | Distribution) fed by the
/// single `/events` stream. Served at `/` and by both plugins' `html_page()`.
pub const FLEET_HTML: &str = include_str!("fleet.html");

/// Owns the collector state and the two ingest plugins, and wires the broadcast
/// fan-out into the dashboard.
pub struct LiveCollector {
    state: Arc<CollectorState>,
    vastai: Arc<VastaiLivePlugin>,
    dist: Arc<PushedDistPlugin>,
}

impl LiveCollector {
    /// Build a collector rooted at `root` (where records are spooled to disk).
    pub fn new(root: impl Into<PathBuf>) -> Self {
        LiveCollector {
            state: Arc::new(CollectorState::new(root)),
            vastai: Arc::new(VastaiLivePlugin::new()),
            dist: Arc::new(PushedDistPlugin::new()),
        }
    }

    /// The collector's data-plane ingest router (`/diag/*`), ready to merge. Uses
    /// the ingest-only variant so it doesn't collide with the dashboard's own `/`.
    pub fn router(&self) -> axum::Router {
        distribution::diagnostics::collector::ingest_router(Arc::clone(&self.state))
    }

    /// Register the plugins, merge the collector router into the dashboard, and
    /// spawn the broadcast→plugin fan-out on `rt`. Call before `start_http`.
    pub fn install(&self, handle: &DashboardHandle, rt: &tokio::runtime::Handle) {
        handle.register_plugin(Arc::clone(&self.vastai) as Arc<dyn crate::plugin::DashboardPlugin>);
        handle.register_plugin(Arc::clone(&self.dist) as Arc<dyn crate::plugin::DashboardPlugin>);
        handle.set_extra_router(self.router());

        let mut rx = self.state.subscribe();
        let vastai = Arc::clone(&self.vastai);
        let dist = Arc::clone(&self.dist);
        rt.spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(rec) => match rec.kind {
                        RecordKind::VastaiInstance
                        | RecordKind::VastaiSample
                        | RecordKind::VastaiLogs
                        | RecordKind::VastaiLifecycle => vastai.ingest(&rec),
                        RecordKind::Snapshot if rec.node_id == DIST_NODE_ID => dist.ingest(&rec),
                        // Other Snapshot node-ids are the swactor diag stream — not
                        // for these plugins. Boot/Events/Finalize are ignored too.
                        _ => {}
                    },
                    // A slow subscriber just skips the gap — the live head view does
                    // not need every record, and the next fold reflects the buffer.
                    Err(RecvError::Lagged(_)) => continue,
                    Err(RecvError::Closed) => break,
                }
            }
        });
    }
}
