//! swactor-store — CLI client for the datastore node.
//!
//! Talks to a running `swactor-store-node` over its HTTP API.

use std::fs;
use std::io::Read;
use std::path::PathBuf;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(name = "swactor-store", about = "Swactor datastore CLI")]
struct Args {
    /// Base URL of the datastore node
    #[arg(long, default_value = "http://localhost:9091")]
    url: String,

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
}

fn main() {
    let args = Args::parse();
    let base = args.url.trim_end_matches('/');

    match args.command {
        Command::Put { path, name } => cmd_put(base, &path, name.as_deref()),
        Command::Get { hash, output } => cmd_get(base, &hash, output.as_deref()),
        Command::Delete { hash } => cmd_delete(base, &hash),
        Command::List { name, all } => cmd_list(base, name.as_deref(), all),
        Command::Status => cmd_status(base),
    }
}

fn cmd_put(base: &str, path: &PathBuf, name: Option<&str>) {
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

    let resp = match ureq::post(&url).send_bytes(&data) {
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

fn cmd_get(base: &str, hash: &str, output: Option<&std::path::Path>) {
    if let Some(out_path) = output {
        // Download raw data
        let url = format!("{base}/api/data?hash={hash}");
        let resp = match ureq::get(&url).call() {
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

fn cmd_delete(base: &str, hash: &str) {
    let url = format!("{base}/api/delete?hash={hash}");
    let resp = match ureq::post(&url).send_bytes(&[]) {
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

fn cmd_list(base: &str, name: Option<&str>, all: bool) {
    let mut url = format!("{base}/api/list");
    let mut sep = '?';
    if let Some(n) = name {
        url.push_str(&format!("{sep}name={}", url_encode(n)));
        sep = '&';
    }
    if all {
        url.push_str(&format!("{sep}all=true"));
    }

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
