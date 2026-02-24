//! swactor-store — CLI client for the datastore node.
//!
//! Talks to a running `swactor-store-node` over its HTTP API.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::{Parser, Subcommand};

use swactor_datastore::crypto::Keypair;
use swactor_datastore::content_hash::ContentHash;
use swactor_datastore::auth::{sign_request, DatastoreAction, SignedRequestPayload};

#[derive(Parser)]
#[command(name = "swactor-store", about = "Swactor datastore CLI")]
struct Args {
    /// Base URL of the datastore node
    #[arg(long, default_value = "http://localhost:9091")]
    url: String,

    /// Path to key.json file for auth signing
    #[arg(long)]
    key: Option<PathBuf>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Store a file in the datastore
    Put {
        /// Path to the local file to store
        path: PathBuf,
        /// Optional name label
        #[arg(long)]
        name: Option<String>,
    },
    /// Retrieve object metadata (or download with --output)
    Get {
        /// Content hash (hex)
        hash: String,
        /// Download file to this path
        #[arg(long)]
        output: Option<PathBuf>,
    },
    /// Delete an object
    Delete {
        /// Content hash (hex)
        hash: String,
    },
    /// List stored objects
    List {
        /// Filter by name substring
        #[arg(long)]
        name: Option<String>,
        /// List from all nodes (swarm-wide)
        #[arg(long)]
        all: bool,
    },
    /// Query node status
    Status,
    /// Authorize a public key (owner only)
    Grant {
        /// Public key (64 hex chars) or name to authorize
        key: String,
        /// Optional human-readable name for the key
        #[arg(long)]
        name: Option<String>,
    },
    /// Revoke a public key (owner only)
    Revoke {
        /// Public key (64 hex chars) or name to revoke
        key: String,
    },
    /// List pending access requests (owner only)
    Requests,
    /// List authorized keys with names (owner only)
    Keys,
    /// Deny (dismiss) a pending access request (owner only)
    Deny {
        /// Public key (64 hex chars) or name to deny
        key: String,
    },
}

// ── Key file helpers ────────────────────────────────────────────────────────

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
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

fn load_keypair(path: &std::path::Path) -> Keypair {
    let data = fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("Error reading key file {}: {e}", path.display());
        std::process::exit(1);
    });
    let json: serde_json::Value = serde_json::from_str(&data).unwrap_or_else(|e| {
        eprintln!("Error parsing key file: {e}");
        std::process::exit(1);
    });
    let secret_hex = json
        .get("secret_key")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            eprintln!("Key file missing secret_key field");
            std::process::exit(1);
        });
    let secret_bytes = hex_decode(secret_hex).unwrap_or_else(|| {
        eprintln!("Invalid secret_key hex in key file");
        std::process::exit(1);
    });
    let secret: [u8; 32] = secret_bytes.try_into().unwrap_or_else(|_| {
        eprintln!("secret_key must be exactly 32 bytes");
        std::process::exit(1);
    });
    Keypair::from_bytes(&secret)
}

// ── Auth signing ────────────────────────────────────────────────────────────

fn sign_action(keypair: &Keypair, action: DatastoreAction) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut nonce = [0u8; 16];
    getrandom::getrandom(&mut nonce).expect("failed to generate random nonce");
    let payload = SignedRequestPayload {
        action,
        timestamp,
        nonce,
    };
    let signed = sign_request(keypair, payload);
    serde_json::to_string(&signed).expect("SignedRequest is always serializable")
}

// ── Name resolution helpers ────────────────────────────────────────────────

fn is_hex_key(s: &str) -> bool {
    s.len() == 64 && hex_decode(s).is_some()
}

/// Parse `"alice (c9d0e1f2)"` → `("alice", Some("c9d0e1f2"))`.
/// Returns `(input, None)` if no suffix found.
fn parse_disambiguated_name(input: &str) -> (&str, Option<&str>) {
    if let Some(paren_start) = input.rfind(" (") {
        if input.ends_with(')') {
            let prefix = &input[paren_start + 2..input.len() - 1];
            if prefix.len() == 8 && hex_decode(prefix).is_some() {
                return (&input[..paren_start], Some(prefix));
            }
        }
    }
    (input, None)
}

/// Resolve a human-readable name to a hex key from the pending requests list.
fn resolve_pending_request_key(base: &str, name_input: &str, kp: &Keypair) -> String {
    let url = format!("{base}/api/auth/requests");
    let req = ureq::get(&url)
        .set("X-Signed-Request", &sign_action(kp, DatastoreAction::Access));

    let resp = match req.call() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error fetching requests: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing requests response: {e}");
            std::process::exit(1);
        }
    };

    let requests = match body.as_array() {
        Some(arr) => arr,
        None => {
            eprintln!("No pending request named '{name_input}'");
            std::process::exit(1);
        }
    };

    let (search_name, disambig_prefix) = parse_disambiguated_name(name_input);

    let matches: Vec<&serde_json::Value> = requests
        .iter()
        .filter(|r| {
            let name = r.get("name").and_then(|v| v.as_str()).unwrap_or("");
            if !name.eq_ignore_ascii_case(search_name) {
                return false;
            }
            if let Some(prefix) = disambig_prefix {
                let key = r.get("key").and_then(|v| v.as_str()).unwrap_or("");
                return key.starts_with(prefix);
            }
            true
        })
        .collect();

    match matches.len() {
        0 => {
            eprintln!("No pending request named '{name_input}'");
            std::process::exit(1);
        }
        1 => matches[0]
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string(),
        _ => {
            eprintln!("Multiple pending requests named '{search_name}':");
            for m in &matches {
                let key = m.get("key").and_then(|v| v.as_str()).unwrap_or("?");
                let prefix = &key[..8];
                eprintln!("  {search_name} ({prefix})");
            }
            eprintln!("Re-run with the disambiguated name.");
            std::process::exit(1);
        }
    }
}

/// Resolve a human-readable name to a hex key from the authorized keys list.
fn resolve_authorized_key(base: &str, name_input: &str, kp: &Keypair) -> String {
    let url = format!("{base}/api/auth/keys");
    let req = ureq::get(&url)
        .set("X-Signed-Request", &sign_action(kp, DatastoreAction::Access));

    let resp = match req.call() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error fetching keys: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing keys response: {e}");
            std::process::exit(1);
        }
    };

    let keys = match body.as_array() {
        Some(arr) => arr,
        None => {
            eprintln!("No authorized key named '{name_input}'");
            std::process::exit(1);
        }
    };

    let (search_name, disambig_prefix) = parse_disambiguated_name(name_input);

    let matches: Vec<&serde_json::Value> = keys
        .iter()
        .filter(|k| {
            let label = k.get("label").and_then(|v| v.as_str()).unwrap_or("");
            if !label.eq_ignore_ascii_case(search_name) {
                return false;
            }
            if let Some(prefix) = disambig_prefix {
                let key = k.get("key").and_then(|v| v.as_str()).unwrap_or("");
                return key.starts_with(prefix);
            }
            true
        })
        .collect();

    match matches.len() {
        0 => {
            eprintln!("No authorized key named '{name_input}'");
            std::process::exit(1);
        }
        1 => matches[0]
            .get("key")
            .and_then(|v| v.as_str())
            .unwrap()
            .to_string(),
        _ => {
            eprintln!("Multiple authorized keys named '{search_name}':");
            for m in &matches {
                let key = m.get("key").and_then(|v| v.as_str()).unwrap_or("?");
                let prefix = &key[..8];
                eprintln!("  {search_name} ({prefix})");
            }
            eprintln!("Re-run with the disambiguated name.");
            std::process::exit(1);
        }
    }
}

// ── Key file helpers ────────────────────────────────────────────────────────

fn hex_decode(hex: &str) -> Option<Vec<u8>> {
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

fn load_keypair(path: &std::path::Path) -> Keypair {
    let data = fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("Error reading key file {}: {e}", path.display());
        std::process::exit(1);
    });
    let json: serde_json::Value = serde_json::from_str(&data).unwrap_or_else(|e| {
        eprintln!("Error parsing key file: {e}");
        std::process::exit(1);
    });
    let secret_hex = json
        .get("secret_key")
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| {
            eprintln!("Key file missing secret_key field");
            std::process::exit(1);
        });
    let secret_bytes = hex_decode(secret_hex).unwrap_or_else(|| {
        eprintln!("Invalid secret_key hex in key file");
        std::process::exit(1);
    });
    let secret: [u8; 32] = secret_bytes.try_into().unwrap_or_else(|_| {
        eprintln!("secret_key must be exactly 32 bytes");
        std::process::exit(1);
    });
    Keypair::from_bytes(&secret)
}

// ── Auth signing ────────────────────────────────────────────────────────────

fn sign_action(keypair: &Keypair, action: DatastoreAction) -> String {
    let timestamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let mut nonce = [0u8; 16];
    getrandom::getrandom(&mut nonce).expect("failed to generate random nonce");
    let payload = SignedRequestPayload {
        action,
        timestamp,
        nonce,
    };
    let signed = sign_request(keypair, payload);
    serde_json::to_string(&signed).expect("SignedRequest is always serializable")
}

fn main() {
    let args = Args::parse();
    let base = args.url.trim_end_matches('/');

    let keypair = args.key.as_deref().map(load_keypair);

    match args.command {
        Command::Put { path, name } => cmd_put(base, &path, name.as_deref(), keypair.as_ref()),
        Command::Get { hash, output } => {
            cmd_get(base, &hash, output.as_deref(), keypair.as_ref())
        }
        Command::Delete { hash } => cmd_delete(base, &hash, keypair.as_ref()),
        Command::List { name, all } => cmd_list(base, name.as_deref(), all, keypair.as_ref()),
        Command::Status => cmd_status(base),
        Command::Grant { key, name } => cmd_grant(base, &key, name.as_deref(), keypair.as_ref()),
        Command::Revoke { key } => cmd_revoke(base, &key, keypair.as_ref()),
        Command::Requests => cmd_requests(base, keypair.as_ref()),
        Command::Keys => cmd_keys(base, keypair.as_ref()),
        Command::Deny { key } => cmd_deny(base, &key, keypair.as_ref()),
    }
}

fn cmd_put(base: &str, path: &PathBuf, name: Option<&str>, keypair: Option<&Keypair>) {
    let data = match fs::read(path) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("Error reading {}: {e}", path.display());
            std::process::exit(1);
        }
    };

    let label = name
        .map(|n| n.to_string())
        .or_else(|| {
            path.file_name()
                .and_then(|f| f.to_str())
                .map(|s| s.to_string())
        });

    let mut url = format!("{base}/api/put");
    if let Some(ref n) = label {
        url.push_str(&format!("?name={}", url_encode(n)));
    }

    let mut req = ureq::post(&url);
    if let Some(kp) = keypair {
        let content_hash = ContentHash::of(&data);
        let action = DatastoreAction::Put {
            name: label.clone(),
            content_hash,
            size_bytes: data.len() as u64,
            tags: BTreeMap::new(),
        };
        req = req.set("X-Signed-Request", &sign_action(kp, action));
    }

    let resp = match req.send_bytes(&data) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if let Some(hash) = body.get("content_hash").and_then(|v| v.as_str()) {
        println!("{hash}");
    } else if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

fn cmd_get(base: &str, hash: &str, output: Option<&std::path::Path>, keypair: Option<&Keypair>) {
    if let Some(out_path) = output {
        // Download raw data
        let url = format!("{base}/api/data?hash={hash}");
        let mut req = ureq::get(&url);
        if let Some(kp) = keypair {
            if let Some(ch) = ContentHash::from_hex(hash) {
                let action = DatastoreAction::Get { content_hash: ch };
                req = req.set("X-Signed-Request", &sign_action(kp, action));
            }
        }

        let resp = match req.call() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        };

        if resp.status() != 200 {
            let body = resp.into_string().unwrap_or_default();
            eprintln!("Error: {body}");
            std::process::exit(1);
        }

        let mut data = Vec::new();
        if let Err(e) = resp.into_reader().read_to_end(&mut data) {
            eprintln!("Error reading response: {e}");
            std::process::exit(1);
        }

        if let Err(e) = fs::write(out_path, &data) {
            eprintln!("Error writing {}: {e}", out_path.display());
            std::process::exit(1);
        }
        println!("Written {} bytes to {}", data.len(), out_path.display());
    } else {
        // Metadata only
        let url = format!("{base}/api/get?hash={hash}");
        let mut req = ureq::get(&url);
        if let Some(kp) = keypair {
            if let Some(ch) = ContentHash::from_hex(hash) {
                let action = DatastoreAction::Get { content_hash: ch };
                req = req.set("X-Signed-Request", &sign_action(kp, action));
            }
        }

        let resp = match req.call() {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error: {e}");
                std::process::exit(1);
            }
        };

        let body: serde_json::Value = match resp.into_json() {
            Ok(v) => v,
            Err(e) => {
                eprintln!("Error parsing response: {e}");
                std::process::exit(1);
            }
        };

        if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
            eprintln!("Error: {err}");
            std::process::exit(1);
        }

        if let Some(entry) = body.get("entry") {
            println!("Hash:    {}", entry.get("content_hash").and_then(|v| v.as_str()).unwrap_or("?"));
            println!(
                "Name:    {}",
                entry.get("name").and_then(|v| v.as_str()).unwrap_or("(none)")
            );
            println!(
                "Size:    {} bytes",
                entry.get("size_bytes").and_then(|v| v.as_u64()).unwrap_or(0)
            );
            println!(
                "Node:    {}",
                entry.get("node_id").and_then(|v| v.as_str()).unwrap_or("?")
            );
            if let Some(tags) = entry.get("tags").and_then(|v| v.as_object()) {
                if !tags.is_empty() {
                    println!("Tags:");
                    for (k, v) in tags {
                        println!("  {k}: {v}");
                    }
                }
            }
        }
        if let Some(manifest) = body.get("manifest") {
            println!(
                "Chunks:  {}",
                manifest
                    .get("chunks")
                    .and_then(|v| v.as_array())
                    .map(|a| a.len())
                    .unwrap_or(0)
            );
        }
    }
}

fn cmd_delete(base: &str, hash: &str, keypair: Option<&Keypair>) {
    let url = format!("{base}/api/delete?hash={hash}");
    let mut req = ureq::post(&url);
    if let Some(kp) = keypair {
        if let Some(ch) = ContentHash::from_hex(hash) {
            let action = DatastoreAction::Delete { content_hash: ch };
            req = req.set("X-Signed-Request", &sign_action(kp, action));
        }
    }

    let resp = match req.send_bytes(&[]) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if let Some(h) = body.get("content_hash").and_then(|v| v.as_str()) {
        println!("Deleted {h}");
    } else if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

fn cmd_list(base: &str, name: Option<&str>, all: bool, keypair: Option<&Keypair>) {
    let mut url = format!("{base}/api/list");
    let mut sep = '?';
    if let Some(n) = name {
        url.push_str(&format!("{sep}name={}", url_encode(n)));
        sep = '&';
    }
    if all {
        url.push_str(&format!("{sep}all=true"));
    }

    let mut req = ureq::get(&url);
    if let Some(kp) = keypair {
        let action = DatastoreAction::List {
            name_filter: name.map(|s| s.to_string()),
        };
        req = req.set("X-Signed-Request", &sign_action(kp, action));
    }

    let resp = match req.call() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    if let Some(entries) = body.get("entries").and_then(|v| v.as_array()) {
        if entries.is_empty() {
            println!("(no entries)");
            return;
        }
        // Print header
        println!("{:<64}  {:>10}  {}", "HASH", "SIZE", "NAME");
        println!("{}", "-".repeat(90));
        for entry in entries {
            let hash = entry
                .get("content_hash")
                .and_then(|v| v.as_str())
                .unwrap_or("?");
            let size = entry
                .get("size_bytes")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let name = entry
                .get("name")
                .and_then(|v| v.as_str())
                .unwrap_or("(none)");
            println!("{hash:<64}  {size:>10}  {name}");
        }
    }
}

fn cmd_status(base: &str) {
    let url = format!("{base}/api/status");
    let resp = match ureq::get(&url).call() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if let Some(node_id) = body.get("node_id").and_then(|v| v.as_str()) {
        println!("Node ID: {node_id}");
    } else if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

fn cmd_grant(base: &str, key_input: &str, name: Option<&str>, keypair: Option<&Keypair>) {
    let kp = match keypair {
        Some(kp) => kp,
        None => {
            eprintln!("Error: --key is required for grant (must be the owner key)");
            std::process::exit(1);
        }
    };

    let key_hex = if is_hex_key(key_input) {
        key_input.to_string()
    } else {
        resolve_pending_request_key(base, key_input, kp)
    };

    let mut url = format!("{base}/api/auth/grant?key={key_hex}");
    if let Some(n) = name {
        url.push_str(&format!("&name={}", url_encode(n)));
    }

    let req = ureq::post(&url)
        .set("X-Signed-Request", &sign_action(kp, DatastoreAction::Access));

    let resp = match req.send_bytes(&[]) {
        Ok(r) => r,
        Err(ureq::Error::Status(status, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            eprintln!("Error ({status}): {body}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if body.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        println!("Granted {key_hex}");
    } else if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

fn cmd_revoke(base: &str, key_input: &str, keypair: Option<&Keypair>) {
    let kp = match keypair {
        Some(kp) => kp,
        None => {
            eprintln!("Error: --key is required for revoke (must be the owner key)");
            std::process::exit(1);
        }
    };

    let key_hex = if is_hex_key(key_input) {
        key_input.to_string()
    } else {
        resolve_authorized_key(base, key_input, kp)
    };

    let url = format!("{base}/api/auth/revoke?key={key_hex}");
    let req = ureq::post(&url)
        .set("X-Signed-Request", &sign_action(kp, DatastoreAction::Access));

    let resp = match req.send_bytes(&[]) {
        Ok(r) => r,
        Err(ureq::Error::Status(status, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            eprintln!("Error ({status}): {body}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if body.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        println!("Revoked {key_hex}");
    } else if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

fn cmd_requests(base: &str, keypair: Option<&Keypair>) {
    let kp = match keypair {
        Some(kp) => kp,
        None => {
            eprintln!("Error: --key is required for requests (must be the owner key)");
            std::process::exit(1);
        }
    };

    let url = format!("{base}/api/auth/requests");
    let req = ureq::get(&url)
        .set("X-Signed-Request", &sign_action(kp, DatastoreAction::Access));

    let resp = match req.call() {
        Ok(r) => r,
        Err(ureq::Error::Status(status, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            eprintln!("Error ({status}): {body}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    let requests = match body.as_array() {
        Some(arr) => arr,
        None => {
            println!("(no pending requests)");
            return;
        }
    };

    if requests.is_empty() {
        println!("(no pending requests)");
        return;
    }

    println!("{:<64}  {:<16}  {}", "KEY", "NAME", "MESSAGE");
    println!("{}", "-".repeat(100));
    for req in requests {
        let key = req.get("key").and_then(|v| v.as_str()).unwrap_or("?");
        let name = req.get("name").and_then(|v| v.as_str()).unwrap_or("?");
        let message = req.get("message").and_then(|v| v.as_str()).unwrap_or("");
        let msg_truncated = if message.len() > 40 {
            format!("{}...", &message[..37])
        } else {
            message.to_string()
        };
        println!("{key:<64}  {name:<16}  {msg_truncated}");
    }
}

fn cmd_keys(base: &str, keypair: Option<&Keypair>) {
    let kp = match keypair {
        Some(kp) => kp,
        None => {
            eprintln!("Error: --key is required for keys (must be the owner key)");
            std::process::exit(1);
        }
    };

    let url = format!("{base}/api/auth/keys");
    let req = ureq::get(&url)
        .set("X-Signed-Request", &sign_action(kp, DatastoreAction::Access));

    let resp = match req.call() {
        Ok(r) => r,
        Err(ureq::Error::Status(status, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            eprintln!("Error ({status}): {body}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }

    let keys = match body.as_array() {
        Some(arr) => arr,
        None => {
            println!("(no authorized keys)");
            return;
        }
    };

    if keys.is_empty() {
        println!("(no authorized keys)");
        return;
    }

    println!("{:<64}  {}", "KEY", "NAME");
    println!("{}", "-".repeat(80));
    for k in keys {
        let key = k.get("key").and_then(|v| v.as_str()).unwrap_or("?");
        let label = k.get("label").and_then(|v| v.as_str()).unwrap_or("");
        println!("{key:<64}  {label}");
    }
}

fn cmd_deny(base: &str, key_input: &str, keypair: Option<&Keypair>) {
    let kp = match keypair {
        Some(kp) => kp,
        None => {
            eprintln!("Error: --key is required for deny (must be the owner key)");
            std::process::exit(1);
        }
    };

    let key_hex = if is_hex_key(key_input) {
        key_input.to_string()
    } else {
        resolve_pending_request_key(base, key_input, kp)
    };

    let url = format!("{base}/api/auth/deny?key={key_hex}");
    let req = ureq::post(&url)
        .set("X-Signed-Request", &sign_action(kp, DatastoreAction::Access));

    let resp = match req.send_bytes(&[]) {
        Ok(r) => r,
        Err(ureq::Error::Status(status, resp)) => {
            let body = resp.into_string().unwrap_or_default();
            eprintln!("Error ({status}): {body}");
            std::process::exit(1);
        }
        Err(e) => {
            eprintln!("Error: {e}");
            std::process::exit(1);
        }
    };

    let body: serde_json::Value = match resp.into_json() {
        Ok(v) => v,
        Err(e) => {
            eprintln!("Error parsing response: {e}");
            std::process::exit(1);
        }
    };

    if body.get("ok").and_then(|v| v.as_bool()) == Some(true) {
        println!("Denied {key_hex}");
    } else if let Some(err) = body.get("error").and_then(|v| v.as_str()) {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

fn url_encode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                result.push(b as char);
            }
            _ => {
                result.push_str(&format!("%{b:02X}"));
            }
        }
    }
    result
}
