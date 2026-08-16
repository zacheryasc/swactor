//! Demo-only control plane: write actions out of the dashboard.
//!
//! This module exists only under the `demo-control` feature. The dashboard's
//! data path stays read-only in every regular build; the provisioning
//! reconciler demo turns this feature on so a human can kill provisioned
//! processes and request new ones from the fleet view.
//!
//! Routes (only present when the feature is enabled and a control sink is
//! installed):
//! - `POST /control/kill`     body `{"node": "<stream node id>"}`
//! - `POST /control/provision` body `{"count": 1}`

use std::sync::mpsc::Sender;
use std::sync::OnceLock;

use serde::Deserialize;

/// A control command issued from the dashboard UI.
#[derive(Clone, Debug, Deserialize)]
pub enum ControlCommand {
    /// Kill the process backing the fleet card identified by its stream node.
    Kill { node: String },
    /// Ask the reconciler to provision `count` additional nodes.
    Provision { count: u32 },
    /// Lower the desired cluster size by `count` nodes (graceful scale
    /// down: teardown through the reconciler, not a kill).
    Remove { count: u32 },
    /// Establish (or replace) the data-plane edge toward one node. The
    /// supervisor provisions the node's inbound edge over the control
    /// plane and dials it over EDGE_ALPN.
    EstablishEdge { node: String },
}

static CONTROL_SENDER: OnceLock<Sender<ControlCommand>> = OnceLock::new();

/// Install the sink that receives dashboard-issued control commands.
///
/// Called once by the embedding demo before the HTTP server starts. Without a
/// sink the control routes answer `503 Service Unavailable`.
pub fn set_control_sender(sender: Sender<ControlCommand>) {
    let _ = CONTROL_SENDER.set(sender);
}

pub(crate) fn dispatch(command: ControlCommand) -> bool {
    CONTROL_SENDER
        .get()
        .is_some_and(|sender| sender.send(command).is_ok())
}
