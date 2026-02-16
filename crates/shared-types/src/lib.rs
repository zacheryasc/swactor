//! Shared types used across the swactor crate ecosystem.
//!
//! Contains `ContentHash` — the blake3-based content address used by
//! both the datastore and distribution layers.

use std::fmt;

use serde::{Deserialize, Serialize};

// ─── ContentHash ────────────────────────────────────────────────────────────

/// A blake3 content hash (32 bytes).
///
/// The primary identifier for blobs and the DHT key. XOR distance for DHT
/// routing, compact Debug/Display for logging.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContentHash(pub [u8; 32]);

impl ContentHash {
    /// Compute the blake3 hash of the given data.
    pub fn of(data: &[u8]) -> Self {
        let hash = blake3::hash(data);
        ContentHash(*hash.as_bytes())
    }

    /// XOR distance between two content hashes (Kademlia metric).
    pub fn xor_distance(&self, other: &ContentHash) -> [u8; 32] {
        let mut out = [0u8; 32];
        for i in 0..32 {
            out[i] = self.0[i] ^ other.0[i];
        }
        out
    }

    /// Number of leading zero bits in the XOR distance to `other`.
    /// Returns 0..=256. Used to select the k-bucket index in the metadata DHT.
    pub fn xor_leading_zeros(&self, other: &ContentHash) -> u32 {
        let dist = self.xor_distance(other);
        let mut zeros = 0u32;
        for byte in dist {
            if byte == 0 {
                zeros += 8;
            } else {
                zeros += byte.leading_zeros();
                break;
            }
        }
        zeros
    }

    /// Parse a 64-character hex string into a ContentHash.
    /// Returns `None` if the string is not exactly 64 hex characters.
    pub fn from_hex(hex: &str) -> Option<Self> {
        if hex.len() != 64 {
            return None;
        }
        let mut bytes = [0u8; 32];
        for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
            let hi = hex_digit(chunk[0])?;
            let lo = hex_digit(chunk[1])?;
            bytes[i] = (hi << 4) | lo;
        }
        Some(ContentHash(bytes))
    }

    /// Encode as lowercase hex string.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in &self.0 {
            use fmt::Write;
            write!(s, "{:02x}", b).unwrap();
        }
        s
    }

    /// The zero hash (all zeroes). Used as a sentinel.
    pub const ZERO: ContentHash = ContentHash([0u8; 32]);
}

impl fmt::Debug for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Hash(")?;
        for b in &self.0[..4] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026})")
    }
}

impl fmt::Display for ContentHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in &self.0[..8] {
            write!(f, "{:02x}", b)?;
        }
        write!(f, "\u{2026}")
    }
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
