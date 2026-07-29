//! MVP node-local runtime public surface.
//!
//! Worker-node runtime behavior lives behind this module boundary; binaries
//! only wire entrypoints into it.

pub(super) mod worker_node_runtime;
