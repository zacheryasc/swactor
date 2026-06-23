//! `swactor-transport` — Ed25519 identity + encoding helpers for swactor peers.
//!
//! This crate is the wire-format anchor for distribution / node. The keypair is
//! a thin wrapper over `ed25519-dalek::SigningKey`.
//! The on-disk identity file is JSON of the form
//! `{"secret_key_hex": "<64 hex chars>"}` — 32 bytes of secret seed.

pub mod codec;
pub mod crypto;
pub mod identity;
pub mod transport;

pub use codec::{
    hex_decode, hex_encode, Codec, CodecRegistry, NetworkMessage, NodeId, WireEnvelope,
};
pub use transport::{CodecRemoteSink, InMemoryTransport, Transport, TransportRouter};
