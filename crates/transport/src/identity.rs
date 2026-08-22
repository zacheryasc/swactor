//! On-disk identity persistence and string encodings used by peer CLIs.

use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::crypto::Keypair;

/// Hex-encode a byte slice as lowercase ASCII.
pub fn hex_encode(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push(hex_digit(b >> 4));
        s.push(hex_digit(b & 0xf));
    }
    s
}

/// Hex-decode a string into bytes. Returns `None` on invalid input.
pub fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if !hex.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let hi = from_hex_digit(chunk[0])?;
        let lo = from_hex_digit(chunk[1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_digit(n: u8) -> char {
    match n {
        0..=9 => (b'0' + n) as char,
        10..=15 => (b'a' + n - 10) as char,
        _ => unreachable!(),
    }
}

fn from_hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ─── Base58 ──────────────────────────────────────────────────────────────
//
// Bitcoin-flavoured base58 alphabet: no `0OIl`. Encoding preserves leading
// zero bytes as leading `'1'` characters.

const B58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

/// Base58-encode a byte slice (Bitcoin alphabet).
pub fn base58_encode(bytes: &[u8]) -> String {
    let zeros = bytes.iter().take_while(|b| **b == 0).count();
    let mut buf: Vec<u8> = Vec::with_capacity(bytes.len() * 138 / 100 + 1);
    for &b in &bytes[zeros..] {
        let mut carry = b as u32;
        for digit in buf.iter_mut() {
            carry += (*digit as u32) << 8;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            buf.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = String::with_capacity(zeros + buf.len());
    for _ in 0..zeros {
        out.push(B58_ALPHABET[0] as char);
    }
    for digit in buf.iter().rev() {
        out.push(B58_ALPHABET[*digit as usize] as char);
    }
    out
}

/// Base58-decode a string into exactly 32 bytes (the swactor node-id width).
///
/// Returns `None` on invalid characters or when the decoded byte length is
/// not 32.
pub fn base58_decode(input: &str) -> Option<[u8; 32]> {
    let mut zeros = 0usize;
    let mut chars = input.chars();
    let mut first_nonzero = None;
    for c in chars.by_ref() {
        if c == B58_ALPHABET[0] as char {
            zeros += 1;
        } else {
            first_nonzero = Some(c);
            break;
        }
    }
    let mut buf: Vec<u8> = Vec::with_capacity(input.len());
    let rest = first_nonzero.into_iter().chain(chars).collect::<String>();
    for c in rest.chars() {
        let v = B58_ALPHABET.iter().position(|&a| a == c as u8)? as u32;
        let mut carry = v;
        for digit in buf.iter_mut() {
            carry += (*digit as u32) * 58;
            *digit = (carry & 0xff) as u8;
            carry >>= 8;
        }
        while carry > 0 {
            buf.push((carry & 0xff) as u8);
            carry >>= 8;
        }
    }
    let mut out = vec![0u8; zeros];
    for b in buf.into_iter().rev() {
        out.push(b);
    }
    if out.len() != 32 {
        return None;
    }
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    Some(arr)
}

// ─── Persistent keypair file ─────────────────────────────────────────────

#[derive(Serialize, Deserialize)]
struct KeyFile {
    secret_key_hex: String,
}

/// Load a keypair from `path`, or generate and write a new one if absent.
///
/// File format: JSON `{"secret_key_hex": "<64 hex chars>"}`.
///
/// # Panics
///
/// Panics if `path` exists but cannot be parsed, or if writing a freshly
/// generated keypair fails. The CLI uses this on startup; a corrupt
/// identity file is operator error worth crashing on.
pub fn load_or_generate_keypair(path: &Path) -> Keypair {
    if path.exists() {
        let raw = fs::read_to_string(path)
            .unwrap_or_else(|e| panic!("failed to read keypair at {}: {e}", path.display()));
        let file: KeyFile = serde_json::from_str(&raw)
            .unwrap_or_else(|e| panic!("invalid keypair file at {}: {e}", path.display()));
        let bytes = hex_decode(&file.secret_key_hex)
            .unwrap_or_else(|| panic!("keypair file {} has invalid hex", path.display()));
        if bytes.len() != 32 {
            panic!(
                "keypair file {} has wrong length ({} bytes, expected 32)",
                path.display(),
                bytes.len()
            );
        }
        Keypair::from_bytes(&bytes)
    } else {
        let kp = Keypair::generate();
        let file = KeyFile {
            secret_key_hex: hex_encode(&kp.secret_bytes()),
        };
        let raw = serde_json::to_string_pretty(&file).expect("KeyFile is always serializable");
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .unwrap_or_else(|e| panic!("failed to create dir {}: {e}", parent.display()));
        }
        fs::write(path, raw)
            .unwrap_or_else(|e| panic!("failed to write keypair at {}: {e}", path.display()));
        kp
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hex_roundtrip_arbitrary_lengths() {
        for bytes in &[
            vec![],
            vec![0x00, 0xff, 0x7e],
            (0u8..=255).collect::<Vec<_>>(),
        ] {
            let s = hex_encode(bytes);
            assert_eq!(hex_decode(&s).as_deref(), Some(bytes.as_slice()));
        }
    }

    #[test]
    fn base58_roundtrip_node_id_width() {
        let inputs: [[u8; 32]; 3] = [[0u8; 32], [0xff; 32], {
            let mut a = [0u8; 32];
            for (i, slot) in a.iter_mut().enumerate() {
                *slot = (i as u8).wrapping_mul(31);
            }
            a
        }];
        for bytes in inputs {
            let s = base58_encode(&bytes);
            assert_eq!(base58_decode(&s), Some(bytes));
        }
    }

    #[test]
    fn base58_rejects_non_32_byte_payloads() {
        assert!(base58_decode("").is_none());
        // 8 bytes encoded — should reject because the decoded width is wrong.
        let small = base58_encode(b"abcdefgh");
        assert!(base58_decode(&small).is_none());
    }

    #[test]
    fn load_or_generate_is_stable_across_calls() {
        let dir = std::env::temp_dir().join(format!(
            "swactor-transport-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let key_path = dir.join("node.key.json");

        let first = load_or_generate_keypair(&key_path);
        let second = load_or_generate_keypair(&key_path);
        assert_eq!(first.node_id(), second.node_id());

        std::fs::remove_dir_all(&dir).ok();
    }
}
