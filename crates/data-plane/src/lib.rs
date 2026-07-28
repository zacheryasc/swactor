//! Reusable actor-oriented data-plane contracts for wire edges, local IPC rings,
//! arena-backed byte movement, and GPU worker object movement.

pub mod actor;
pub mod arena;
pub mod edge_actor;
pub mod edge_lifecycle;
pub mod egress;
pub mod ingress;
pub mod object_record;
pub mod ring;
