//! Reusable actor-oriented data-plane contracts for wire edges, local IPC rings,
//! arena-backed byte movement, and GPU worker object movement.
//!
//! Composition lives in [`edge_runtime`]: [`edge_runtime::EdgeRuntime`] drives
//! the edge lifecycle ([`edge_lifecycle`]) over a byte transport
//! ([`edge_wire`]), the arena ([`arena`]), and an application worker port.

pub mod arena;
pub mod blob;
pub mod blob_transfer;
pub mod bootstrap;
pub mod byte_ring;
pub mod control;
pub mod data_plane;
pub mod edge_lifecycle;
pub mod edge_runtime;
pub mod edge_wire;
pub mod host;
pub mod ids;
pub mod mapped_arena;
pub mod namespace;
pub mod namespace_store;
pub mod object_record;
pub mod path;
pub mod protocol;
pub mod ring;
pub mod source;
pub mod stream_transport;
