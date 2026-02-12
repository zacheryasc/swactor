use std::fs;
use std::path::{Path, PathBuf};

use serde::Serialize;

use crate::dashboard_html::DASHBOARD_HTML;

// ── Trace directory scanning ──────────────────────────────────────────

#[derive(Debug, Clone, Serialize)]
struct TraceEntry {
    file: String,
    name: String,
    nodes: usize,
    events: usize,
}

fn scan_traces(dir: &Path) -> Vec<TraceEntry> {
    let mut entries = Vec::new();
    let Ok(read_dir) = fs::read_dir(dir) else {
        return entries;
    };
    for entry in read_dir.flatten() {
        let path = entry.path();
        let fname = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        if !fname.ends_with(".trace.json") {
            continue;
        }
        let Ok(data) = fs::read_to_string(&path) else {
            continue;
        };
        // Parse as generic JSON to extract metadata without full deserialization.
        let Ok(val) = serde_json::from_str::<serde_json::Value>(&data) else {
            continue;
        };
        let name = val
            .get("name")
            .and_then(|v| v.as_str())
            .unwrap_or(&fname)
            .to_string();
        let nodes = val
            .get("node_names")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        let events = val
            .get("events")
            .and_then(|v| v.as_array())
            .map(|a| a.len())
            .unwrap_or(0);
        entries.push(TraceEntry {
            file: fname,
            name,
            nodes,
            events,
        });
    }
    entries.sort_by(|a, b| a.file.cmp(&b.file));
    entries
}

// ── HTTP server ───────────────────────────────────────────────────────

pub fn serve_dashboard(trace_dir: &str, port: u16) {
    let dir = PathBuf::from(trace_dir);
    assert!(dir.is_dir(), "trace directory does not exist: {trace_dir}");

    let addr = format!("0.0.0.0:{port}");
    let server = tiny_http::Server::http(&addr).expect("failed to bind HTTP server");

    eprintln!("Dashboard at http://localhost:{port}");
    eprintln!("Serving traces from: {trace_dir}");

    loop {
        let request = match server.recv() {
            Ok(r) => r,
            Err(_) => break,
        };

        let url = request.url().to_string();
        match url.as_str() {
            "/" => {
                let response = tiny_http::Response::from_string(DASHBOARD_HTML).with_header(
                    "Content-Type: text/html; charset=utf-8"
                        .parse::<tiny_http::Header>()
                        .unwrap(),
                );
                let _ = request.respond(response);
            }
            "/traces" => {
                let entries = scan_traces(&dir);
                let json = serde_json::to_string(&entries).unwrap();
                let response = tiny_http::Response::from_string(json).with_header(
                    "Content-Type: application/json"
                        .parse::<tiny_http::Header>()
                        .unwrap(),
                );
                let _ = request.respond(response);
            }
            _ if url.starts_with("/trace.json?file=") => {
                let raw = url.strip_prefix("/trace.json?file=").unwrap();
                let file = percent_decode(raw);

                // Reject path traversal attempts.
                if file.contains('/')
                    || file.contains('\\')
                    || file.contains("..")
                    || file.is_empty()
                {
                    let response =
                        tiny_http::Response::from_string("Bad Request").with_status_code(400);
                    let _ = request.respond(response);
                    continue;
                }

                let path = dir.join(&file);
                match fs::read_to_string(&path) {
                    Ok(data) => {
                        let response = tiny_http::Response::from_string(data).with_header(
                            "Content-Type: application/json"
                                .parse::<tiny_http::Header>()
                                .unwrap(),
                        );
                        let _ = request.respond(response);
                    }
                    Err(_) => {
                        let response =
                            tiny_http::Response::from_string("Not Found").with_status_code(404);
                        let _ = request.respond(response);
                    }
                }
            }
            _ => {
                let response =
                    tiny_http::Response::from_string("Not Found").with_status_code(404);
                let _ = request.respond(response);
            }
        }
    }
}

// ── Helpers ───────────────────────────────────────────────────────────

fn percent_decode(s: &str) -> String {
    let mut result = Vec::new();
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                result.push(h << 4 | l);
                i += 3;
                continue;
            }
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).to_string()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}
