//! MVP node-local data-plane adapter public surface.
//!
//! Reusable arena, ring, and object-record contracts live in `data-plane`.
//! This module is the MVP boundary that binds those contracts to worker ingress,
//! worker egress, edge actors, and local transport behavior.

pub mod arena {
    pub use crate::arena_manager::*;
}

pub mod ring {
    pub use data_plane::ring::*;
}

pub mod object {
    pub use crate::gpu_worker_ingress_parser::{
        FLAG_BEGIN_SEQUENCE, FLAG_END_OF_SEQUENCE, HEADER_LEN, KNOWN_FLAGS_MASK, OBJECT_MAGIC,
        OBJECT_MAGIC_BYTES, OBJECT_VERSION, ObjectFailureReason, ObjectFlags, ObjectHeader,
        ObjectId, ObjectLayout, ObjectRecord, ObjectRecordBuilder, ObjectRecordRead, ObjectSpec,
        read_object_record,
    };
}

pub mod ingress {
    pub use crate::gpu_worker_ingress_parser::*;
}

pub mod egress {
    pub use crate::worker::egress::*;
}

pub mod local_transport {
    pub use crate::driver_pumps::*;
}

pub mod edge_actor {
    pub use crate::tx_rx_edge_actor::*;
}

pub mod reusable {
    pub use data_plane::{arena, object_record, ring};
}
