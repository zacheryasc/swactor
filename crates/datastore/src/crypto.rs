//! Ed25519 cryptographic primitives for the datastore.

use std::fmt;

use ed25519_dalek::{Signer, Verifier};
use serde::{Deserialize, Serialize};

use swactor::transport::NodeId;

// ─── Signature ──────────────────────────────────────────────────────────────

/// An ed25519 signature (64 bytes).
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Signature(pub [u8; 64]);

impl Serialize for Signature {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(&self.0)
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        if bytes.len() != 64 {
            return Err(serde::de::Error::custom(format!(
                "expected 64 bytes for Signature, got {}",
                bytes.len()
            )));
        }
        let mut arr = [0u8; 64];
        arr.copy_from_slice(&bytes);
        Ok(Signature(arr))
    }
}

impl fmt::Debug for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Sig(")?;
        for b in &self.0[..4] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026})")
    }
}

// ─── Keypair ────────────────────────────────────────────────────────────────

/// Node identity keypair — wraps ed25519-dalek.
pub struct Keypair {
    inner: ed25519_dalek::SigningKey,
}

impl Keypair {
    /// Generate a new random keypair.
    pub fn generate() -> Self {
        let mut csprng = rand_core::OsRng;
        Self {
            inner: ed25519_dalek::SigningKey::generate(&mut csprng),
        }
    }

    /// Reconstruct from raw secret key bytes (32 bytes).
    pub fn from_bytes(secret: &[u8; 32]) -> Self {
        Self {
            inner: ed25519_dalek::SigningKey::from_bytes(secret),
        }
    }

    /// The public key as a `NodeId`.
    pub fn node_id(&self) -> NodeId {
        NodeId(self.inner.verifying_key().to_bytes())
    }

    /// Raw secret key bytes.
    pub fn secret_bytes(&self) -> [u8; 32] {
        self.inner.to_bytes()
    }

    /// Sign arbitrary bytes.
    pub fn sign(&self, msg: &[u8]) -> Signature {
        let sig = self.inner.sign(msg);
        Signature(sig.to_bytes())
    }
}

// ─── Verification ───────────────────────────────────────────────────────────

/// Verify a signature against a `NodeId` (public key) and message bytes.
pub fn verify(node_id: &NodeId, msg: &[u8], sig: &Signature) -> bool {
    let Ok(vk) = ed25519_dalek::VerifyingKey::from_bytes(&node_id.0) else {
        return false;
    };
    let signature = ed25519_dalek::Signature::from_bytes(&sig.0);
    vk.verify(msg, &signature).is_ok()
}
