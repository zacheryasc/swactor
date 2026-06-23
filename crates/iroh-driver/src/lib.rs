//! `iroh-driver` — iroh-backed transport driver for the actorized distribution stack.
//!
//! This crate owns the concrete iroh endpoint/QUIC/relay machinery. The
//! distribution crate owns cluster dynamics, protocol actors, routing claims, and
//! wire message definitions.

pub mod iroh_driver;

pub use iroh_driver::{
    ConnType, IrohDriver, IrohDriverConfig, JoinPhase, JoinStatus, conn_type_of, discover_lan_ips,
};
