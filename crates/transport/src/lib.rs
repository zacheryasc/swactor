//! Transport infrastructure for the swactor ecosystem.
//!
//! Provides cryptographic identity (ed25519 keypairs) and encoding utilities.

pub mod crypto;
pub mod identity;

pub use swactor::transport::NodeId;
