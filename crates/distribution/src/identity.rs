//! Shared keypair persistence — load or generate an ed25519 keypair on disk.
//!
//! Reused by `swactor-node`, `store_node`, and xtask tooling.

use std::path::Path;

use crate::crypto::Keypair;

/// Hex-encode a byte slice.
pub fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Hex-decode a string into bytes. Returns `None` on invalid input.
pub fn hex_decode(hex: &str) -> Option<Vec<u8>> {
    if hex.len() % 2 != 0 {
        return None;
    }
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for chunk in hex.as_bytes().chunks(2) {
        let hi = hex_digit(chunk[0])?;
        let lo = hex_digit(chunk[1])?;
        bytes.push((hi << 4) | lo);
    }
    Some(bytes)
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Base58-encode a byte slice (Bitcoin alphabet).
pub fn base58_encode(bytes: &[u8]) -> String {
    bs58::encode(bytes).into_string()
}

/// Base58-decode a string into a 32-byte array. Returns `None` on invalid input.
pub fn base58_decode(s: &str) -> Option<[u8; 32]> {
    let bytes = bs58::decode(s).into_vec().ok()?;
    bytes.try_into().ok()
}

/// Load a keypair from `path`, or generate a new one and persist it.
///
/// The file format is JSON:
/// ```json
/// {
///   "version": 1,
///   "secret_key": "<hex>",
///   "public_key": "<hex>",
///   "created_at": "2025-01-01T00:00:00Z"
/// }
/// ```
pub fn load_or_generate_keypair(path: &Path) -> Keypair {
    if path.exists() {
        let data = std::fs::read_to_string(path).expect("failed to read key file");
        let json: serde_json::Value =
            serde_json::from_str(&data).expect("invalid key file JSON");
        let secret_hex = json
            .get("secret_key")
            .and_then(|v| v.as_str())
            .expect("key file missing secret_key");
        let secret_bytes = hex_decode(secret_hex).expect("invalid secret_key hex");
        let secret: [u8; 32] = secret_bytes
            .try_into()
            .expect("secret_key must be 32 bytes");
        Keypair::from_bytes(&secret)
    } else {
        let keypair = Keypair::generate();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let json = serde_json::json!({
            "version": 1,
            "secret_key": hex_encode(&keypair.secret_bytes()),
            "public_key": hex_encode(&keypair.node_id().0),
            "created_at": format_timestamp(now),
        });
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).expect("failed to create key file directory");
        }
        std::fs::write(path, serde_json::to_string_pretty(&json).unwrap())
            .expect("failed to write key file");
        keypair
    }
}

/// Simple ISO-8601 UTC timestamp from epoch seconds.
pub fn format_timestamp(secs: u64) -> String {
    let s = secs % 60;
    let m = (secs / 60) % 60;
    let h = (secs / 3600) % 24;
    let days = secs / 86400;
    let (y, mo, d) = days_to_ymd(days);
    format!("{y:04}-{mo:02}-{d:02}T{h:02}:{m:02}:{s:02}Z")
}

/// Convert days since epoch to (year, month, day).
/// Algorithm from <http://howardhinnant.github.io/date_algorithms.html>.
pub fn days_to_ymd(mut days: u64) -> (u64, u64, u64) {
    days += 719468;
    let era = days / 146097;
    let doe = days - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}
