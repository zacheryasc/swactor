//! MVP node-local runtime public surface.
//!
//! Worker-node runtime behavior lives behind this module boundary; binaries
//! only wire entrypoints into it.

pub mod actor;
pub mod boot_lifecycle;
pub mod data_plane_bridge;
pub mod worker_node_runtime;
