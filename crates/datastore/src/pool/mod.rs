//! Pooled datastore protocol.
//!
//! A shared storage pool where multiple nodes contribute storage capacity
//! and converge on a shared view of what content lives where.

pub mod disseminator;
pub mod messages;
pub mod coordinator;
