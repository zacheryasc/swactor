//! Transport infrastructure for the swactor ecosystem.
//!
//! Provides cryptographic identity (ed25519 keypairs), encoding utilities,
//! and TCP transport primitives.

pub mod crypto;
pub mod identity;

pub use swactor::transport::NodeId;

#[cfg(feature = "tcp")]
pub mod tcp;
