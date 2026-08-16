//! Reusable actor-oriented data-plane contracts for wire edges, local IPC rings,
//! arena-backed byte movement, and GPU worker object movement.
//!
//! Composition lives in [`edge_runtime`]: [`edge_runtime::EdgeRuntime`] drives
//! the edge lifecycle ([`edge_lifecycle`]) over a byte transport
//! ([`edge_wire`]), the arena ([`arena`]), and an application worker port.

pub mod arena;
pub mod edge_lifecycle;
pub mod edge_runtime;
pub mod edge_wire;
pub mod ids;
pub mod object_record;
pub mod ring;
