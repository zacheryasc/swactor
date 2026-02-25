//! Ed25519 cryptographic primitives for distribution.

pub use swactor_transport::crypto::{Keypair, Signature, verify};

use crate::types::{DirectoryEntry, DirectoryEntryPayload};

// ─── Directory Entry Helpers ────────────────────────────────────────────────

/// Extension methods for Keypair specific to distribution directory entries.
pub trait KeypairExt {
    /// Sign a directory entry payload, returning a complete `DirectoryEntry`.
    fn sign_directory_entry(
        &self,
        actor_addr: swactor::actor::ActorAddress,
        generation: u64,
    ) -> DirectoryEntry;
}

impl KeypairExt for Keypair {
    fn sign_directory_entry(
        &self,
        actor_addr: swactor::actor::ActorAddress,
        generation: u64,
    ) -> DirectoryEntry {
        let payload = DirectoryEntryPayload {
            actor_addr,
            node_id: self.node_id(),
            generation,
        };
        let bytes = serde_json::to_vec(&payload).expect("DirectoryEntryPayload is always serializable");
        let signature = self.sign(&bytes);
        DirectoryEntry {
            actor_addr,
            node_id: self.node_id(),
            generation,
            signature,
        }
    }
}

/// Verify a `DirectoryEntry`'s signature against its embedded `node_id`.
pub fn verify_directory_entry(entry: &DirectoryEntry) -> bool {
    let payload = entry.payload();
    let Ok(bytes) = serde_json::to_vec(&payload) else {
        return false;
    };
    verify(&entry.node_id, &bytes, &entry.signature)
}
