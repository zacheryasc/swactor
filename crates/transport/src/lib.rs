//! `swactor-transport` — Ed25519 identity + encoding helpers for swactor peers.
//!
//! This crate is the wire-format anchor for distribution / datastore /
//! node. The keypair is a thin wrapper over `ed25519-dalek::SigningKey`.
//! The on-disk identity file is JSON of the form
//! `{"secret_key_hex": "<64 hex chars>"}` — 32 bytes of secret seed.

pub mod crypto;
pub mod identity;
