//! Ed25519 keypair, signing, and verification.

use ed25519_dalek::{Signer, SigningKey, Verifier, VerifyingKey};
use serde::de::{SeqAccess, Visitor};
use serde::ser::SerializeTuple;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use crate::NodeId;

/// Ed25519 signing keypair.
///
/// Wraps `ed25519_dalek::SigningKey`. The public key half is exposed as a
/// `swactor_transport::NodeId` so peer identity is uniform across the
/// distribution stack.
#[derive(Clone)]
pub struct Keypair {
    signing: SigningKey,
}

impl Keypair {
    /// Generate a fresh keypair from the OS RNG.
    pub fn generate() -> Self {
        let mut rng = rand_core::OsRng;
        Self {
            signing: SigningKey::generate(&mut rng),
        }
    }

    /// Reconstruct a keypair from its 32-byte secret seed.
    ///
    /// # Panics
    ///
    /// Panics if `bytes` is shorter than 32 bytes. Extra bytes are ignored.
    pub fn from_bytes(bytes: &[u8]) -> Self {
        let mut seed = [0u8; 32];
        seed.copy_from_slice(&bytes[..32]);
        Self {
            signing: SigningKey::from_bytes(&seed),
        }
    }

    /// The 32-byte secret seed for this keypair.
    pub fn secret_bytes(&self) -> [u8; 32] {
        self.signing.to_bytes()
    }

    /// The public node identifier (raw ed25519 public key bytes).
    pub fn node_id(&self) -> NodeId {
        NodeId(self.signing.verifying_key().to_bytes())
    }

    /// Sign `msg` with the secret half. Result is a 64-byte ed25519 signature.
    pub fn sign(&self, msg: &[u8]) -> Signature {
        let sig = self.signing.sign(msg);
        Signature(sig.to_bytes())
    }
}

impl core::fmt::Debug for Keypair {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        // Don't leak the secret half through Debug.
        f.debug_struct("Keypair")
            .field("node_id", &self.node_id())
            .finish_non_exhaustive()
    }
}

/// 64-byte ed25519 signature.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct Signature(pub [u8; 64]);

impl Serialize for Signature {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        let mut tup = ser.serialize_tuple(64)?;
        for byte in &self.0 {
            tup.serialize_element(byte)?;
        }
        tup.end()
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D: Deserializer<'de>>(de: D) -> Result<Self, D::Error> {
        struct SigVisitor;
        impl<'de> Visitor<'de> for SigVisitor {
            type Value = Signature;
            fn expecting(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
                write!(f, "a 64-byte ed25519 signature")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Signature, A::Error> {
                let mut bytes = [0u8; 64];
                for (i, byte) in bytes.iter_mut().enumerate() {
                    *byte = seq
                        .next_element()?
                        .ok_or_else(|| serde::de::Error::invalid_length(i, &self))?;
                }
                Ok(Signature(bytes))
            }
        }
        de.deserialize_tuple(64, SigVisitor)
    }
}

impl core::fmt::Debug for Signature {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "Signature(")?;
        for b in &self.0[..4] {
            write!(f, "{b:02x}")?;
        }
        write!(f, "\u{2026})")
    }
}

/// Verify `msg` against `sig` using the public key encoded in `node_id`.
///
/// Returns `false` if the key bytes are not a valid ed25519 point, the
/// signature bytes are not a valid signature, or the check fails.
pub fn verify(node_id: &NodeId, msg: &[u8], sig: &Signature) -> bool {
    let Ok(vk) = VerifyingKey::from_bytes(&node_id.0) else {
        return false;
    };
    let sig = ed25519_dalek::Signature::from_bytes(&sig.0);
    vk.verify(msg, &sig).is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sign_then_verify_succeeds() {
        let kp = Keypair::generate();
        let sig = kp.sign(b"hello");
        assert!(verify(&kp.node_id(), b"hello", &sig));
    }

    #[test]
    fn verify_rejects_wrong_message() {
        let kp = Keypair::generate();
        let sig = kp.sign(b"original");
        assert!(!verify(&kp.node_id(), b"tampered", &sig));
    }

    #[test]
    fn verify_rejects_wrong_key() {
        let a = Keypair::generate();
        let b = Keypair::generate();
        let sig = a.sign(b"msg");
        assert!(!verify(&b.node_id(), b"msg", &sig));
    }

    #[test]
    fn from_bytes_reproduces_identity() {
        let kp = Keypair::generate();
        let restored = Keypair::from_bytes(&kp.secret_bytes());
        assert_eq!(kp.node_id(), restored.node_id());
        // And the signatures match too (ed25519 is deterministic).
        assert_eq!(kp.sign(b"deterministic").0, restored.sign(b"deterministic").0);
    }
}
