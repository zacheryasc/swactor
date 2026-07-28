//! MVP node-local data-plane adapter public surface.
//!
//! Reusable arena, ring, and object-record contracts live in `data-plane`.
//! This module binds those contracts to MVP worker ingress, worker egress,
//! and edge actor behavior.

pub mod arena;
pub mod edge_actor;
pub mod ingress;

pub mod ring {
    pub use data_plane::ring::*;
}

pub mod object {
    pub use data_plane::object_record::*;
}

pub mod egress {
    pub use crate::worker::egress::*;
}

pub mod reusable {
    pub use data_plane::{arena, object_record, ring};
}
