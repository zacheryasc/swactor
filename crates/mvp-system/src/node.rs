//! MVP node-local runtime public surface.
//!
//! Worker-node runtime behavior lives behind this module boundary; binaries
//! only wire entrypoints into it.

pub mod worker_node_runtime;

pub mod actor {
    pub use crate::actors::node_agent::*;
}

pub mod node_boot_lifecycle {
    pub use crate::node_boot_lifecycle::*;
}

pub mod node_image {
    pub use crate::node_image::*;
}

pub mod resource_inventory {
    pub use crate::resource_inventory::*;
}
