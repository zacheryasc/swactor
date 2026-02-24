//! HTTP API for the datastore node.
//!
//! Bridges HTTP requests to actor messages using `Runtime::new_inbox()` +
//! `try_recv()` polling for synchronous request/response with actors.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use swactor::actor::ActorAddress;
use swactor::runtime::{Inbox, Runtime};

use swactor::transport::NodeId;

use crate::auth::SignedRequest;
use crate::chunking::reassemble_blob;
use crate::messages::{BlobStoreMsg, DatastoreNodeMsg, DatastoreResponse, GatewayMsg, MetadataMsg};
use crate::metrics::DatastoreMetrics;
use crate::types::ContentHash;

/// Per-peer actor addresses needed for remote operations.
#[derive(Clone)]
pub struct PeerInfo {
    pub metadata: ActorAddress,
    pub blob_store: ActorAddress,
}

/// Shared state passed to HTTP handler threads.
struct ApiState {
    runtime: Arc<Runtime>,
    datastore_addr: ActorAddress,
    metadata_addr: ActorAddress,
    blob_store_addr: ActorAddress,
    gateway_addr: Option<ActorAddress>,
    peers: Arc<Mutex<Vec<PeerInfo>>>,
    metrics: Arc<DatastoreMetrics>,
}

const CRYPTO_WASM: &[u8] = include_bytes!("crypto_wasm.wasm");

const POLL_TIMEOUT: Duration = Duration::from_secs(5);
const POLL_INTERVAL: Duration = Duration::from_millis(1);

/// Poll an inbox for a response with timeout.
fn poll_response(inbox: &Inbox<DatastoreResponse>, timeout: Duration) -> Option<DatastoreResponse> {
    let start = Instant::now();
    loop {
        if let Some(resp) = inbox.try_recv() {
            return Some(resp);
        }
        if start.elapsed() > timeout {
            return None;
        }
        thread::sleep(POLL_INTERVAL);
    }
}

/// Check auth and return the caller's identity (public key).
/// Returns Ok(NodeId) if no gateway is configured (zero NodeId) or if authorized.
/// Returns Err((status_code, message)) if denied.
fn check_auth_identity(request: &tiny_http::Request, state: &ApiState) -> Result<NodeId, (u16, String)> {
    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => return Ok(NodeId([0; 32])), // no auth configured
    };

    let header_value = request
        .headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case("x-signed-request"))
        .map(|h| h.value.as_str().to_string());

    let header_value = match header_value {
        Some(v) => v,
        None => return Err((401, "missing X-Signed-Request header".to_string())),
    };

    let signed_request: SignedRequest = serde_json::from_str(&header_value)
        .map_err(|e| (400, format!("invalid X-Signed-Request: {e}")))?;

    let public_key = signed_request.public_key;

    let inbox = state
        .runtime
        .new_inbox::<DatastoreResponse>()
        .map_err(|_| (500, "failed to create inbox".to_string()))?;

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::Authorize {
            request: signed_request,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::Bool(true)) => Ok(public_key),
        Some(DatastoreResponse::Denied { reason }) => {
            Err((403, format!("{reason:?}")))
        }
        _ => Err((504, "auth timeout".to_string())),
    }
}

/// Check auth by sending a GatewayMsg::Authorize to the gateway actor.
/// Returns Ok(()) if no gateway is configured or if authorized.
/// Returns Err((status_code, message)) if denied.
fn check_auth(request: &tiny_http::Request, state: &ApiState) -> Result<(), (u16, String)> {
    check_auth_identity(request, state).map(|_| ())
}

/// Verify the signature only (no ACL check).
/// Used for endpoints where the caller proves key ownership without needing authorization.
/// Returns Ok(NodeId) on valid signature, Err on failure.
fn check_auth_signature_only(request: &tiny_http::Request, state: &ApiState) -> Result<NodeId, (u16, String)> {
    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => return Ok(NodeId([0; 32])),
    };

    let header_value = request
        .headers()
        .iter()
        .find(|h| h.field.as_str().as_str().eq_ignore_ascii_case("x-signed-request"))
        .map(|h| h.value.as_str().to_string());

    let header_value = match header_value {
        Some(v) => v,
        None => return Err((401, "missing X-Signed-Request header".to_string())),
    };

    let signed_request: SignedRequest = serde_json::from_str(&header_value)
        .map_err(|e| (400, format!("invalid X-Signed-Request: {e}")))?;

    let public_key = signed_request.public_key;

    let inbox = state
        .runtime
        .new_inbox::<DatastoreResponse>()
        .map_err(|_| (500, "failed to create inbox".to_string()))?;

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::VerifySignature {
            request: signed_request,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::Bool(true)) => Ok(public_key),
        Some(DatastoreResponse::Denied { reason }) => {
            Err((403, format!("{reason:?}")))
        }
        _ => Err((504, "auth timeout".to_string())),
    }
}

fn respond_json(request: tiny_http::Request, json: &str) {
    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn respond_bytes(request: tiny_http::Request, data: &[u8]) {
    let response = tiny_http::Response::from_data(data.to_vec()).with_header(
        "Content-Type: application/octet-stream"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn respond_html(request: tiny_http::Request) {
    let response =
        tiny_http::Response::from_string(crate::ui_html::DATASTORE_UI_HTML).with_header(
            "Content-Type: text/html; charset=utf-8"
                .parse::<tiny_http::Header>()
                .unwrap(),
        );
    let _ = request.respond(response);
}

fn respond_wasm(request: tiny_http::Request) {
    let response = tiny_http::Response::from_data(CRYPTO_WASM.to_vec()).with_header(
        "Content-Type: application/wasm"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn respond_admin_html(request: tiny_http::Request) {
    let response =
        tiny_http::Response::from_string(crate::ui_html::DATASTORE_ADMIN_HTML).with_header(
            "Content-Type: text/html; charset=utf-8"
                .parse::<tiny_http::Header>()
                .unwrap(),
        );
    let _ = request.respond(response);
}

fn respond_error(request: tiny_http::Request, status: u16, msg: &str) {
    let json = serde_json::json!({ "error": msg }).to_string();
    let response = tiny_http::Response::from_string(json)
        .with_status_code(status)
        .with_header(
            "Content-Type: application/json"
                .parse::<tiny_http::Header>()
                .unwrap(),
        );
    let _ = request.respond(response);
}

fn parse_query_string(url: &str) -> BTreeMap<String, String> {
    let mut params = BTreeMap::new();
    if let Some(qs) = url.split('?').nth(1) {
        for pair in qs.split('&') {
            let mut kv = pair.splitn(2, '=');
            if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
                params.insert(
                    url_decode(k),
                    url_decode(v),
                );
            }
        }
    }
    params
}

fn url_decode(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.bytes();
    while let Some(b) = chars.next() {
        match b {
            b'%' => {
                let hi = chars.next().and_then(hex_val);
                let lo = chars.next().and_then(hex_val);
                if let (Some(h), Some(l)) = (hi, lo) {
                    result.push((h << 4 | l) as char);
                }
            }
            b'+' => result.push(' '),
            _ => result.push(b as char),
        }
    }
    result
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

// ── JSON serialization helpers ───────────────────────────────────────────
//
// ContentHash/NodeId derive Serialize as byte arrays ([u8; 32]).
// The API should expose them as hex strings. These helpers convert
// domain types into JSON with human-readable hex fields.

fn entry_to_json(entry: &crate::types::ObjectEntry) -> serde_json::Value {
    let node_hex: String = entry.node_id.0.iter().map(|b| format!("{b:02x}")).collect();
    serde_json::json!({
        "content_hash": entry.content_hash.to_hex(),
        "name": entry.name,
        "node_id": node_hex,
        "tags": entry.tags,
        "size_bytes": entry.size_bytes,
        "created_at": entry.created_at,
    })
}

fn manifest_to_json(manifest: &crate::types::ObjectManifest) -> serde_json::Value {
    let chunks: Vec<serde_json::Value> = manifest
        .chunks
        .iter()
        .map(|c| {
            serde_json::json!({
                "hash": c.hash.to_hex(),
                "offset": c.offset,
                "size": c.size,
            })
        })
        .collect();
    serde_json::json!({
        "content_hash": manifest.content_hash.to_hex(),
        "chunks": chunks,
        "total_size": manifest.total_size,
        "chunk_size": manifest.chunk_size,
        "content_type": manifest.content_type,
    })
}

fn entries_to_json(entries: &[crate::types::ObjectEntry]) -> Vec<serde_json::Value> {
    entries.iter().map(entry_to_json).collect()
}

// ── PUT handler ─────────────────────────────────────────────────────────

fn handle_put(mut request: tiny_http::Request, url: &str, state: &ApiState) {
    if let Err((status, msg)) = check_auth(&request, state) {
        respond_error(request, status, &msg);
        return;
    }
    let params = parse_query_string(url);
    let name = params.get("name").cloned();

    // Collect tags from query params (skip "name")
    let mut tags = BTreeMap::new();
    for (k, v) in &params {
        if k != "name" {
            tags.insert(k.clone(), v.clone());
        }
    }

    // Read body
    let mut body = Vec::new();
    if request.as_reader().read_to_end(&mut body).is_err() {
        // Can't respond — request consumed
        return;
    }
    let body_len = body.len();

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        state.datastore_addr,
        DatastoreNodeMsg::Put {
            data: body,
            name,
            tags,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::PutOk { content_hash }) => {
            let hex = content_hash.to_hex();
            state.metrics.record_put(
                &hex,
                params.get("name").map(|s| s.as_str()),
                body_len as u64,
            );
            let json = serde_json::json!({ "content_hash": hex }).to_string();
            respond_json(request, &json);
        }
        Some(DatastoreResponse::Error { reason }) => {
            respond_error(request, 500, &reason);
        }
        _ => {
            respond_error(request, 504, "timeout waiting for put response");
        }
    }
}

// ── GET handler (metadata) ──────────────────────────────────────────────

fn handle_get(request: tiny_http::Request, url: &str, state: &ApiState) {
    if let Err((status, msg)) = check_auth(&request, state) {
        respond_error(request, status, &msg);
        return;
    }
    let params = parse_query_string(url);
    let hash_hex = match params.get("hash") {
        Some(h) => h,
        None => {
            respond_error(request, 400, "missing ?hash= parameter");
            return;
        }
    };

    let content_hash = match ContentHash::from_hex(hash_hex) {
        Some(h) => h,
        None => {
            respond_error(request, 400, "invalid content hash hex");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        state.datastore_addr,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::GetOk { entry, manifest }) => {
            state.metrics.record_get(&content_hash.to_hex());
            let json = serde_json::json!({
                "entry": entry_to_json(&entry),
                "manifest": manifest_to_json(&manifest),
            })
            .to_string();
            respond_json(request, &json);
        }
        Some(DatastoreResponse::NotFound) => {
            respond_error(request, 404, "not found");
        }
        Some(DatastoreResponse::Error { reason }) => {
            respond_error(request, 500, &reason);
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

// ── DATA handler (reassembled binary) ───────────────────────────────────

fn handle_data(request: tiny_http::Request, url: &str, state: &ApiState) {
    if let Err((status, msg)) = check_auth(&request, state) {
        respond_error(request, status, &msg);
        return;
    }
    let params = parse_query_string(url);
    let hash_hex = match params.get("hash") {
        Some(h) => h,
        None => {
            respond_error(request, 400, "missing ?hash= parameter");
            return;
        }
    };

    let content_hash = match ContentHash::from_hex(hash_hex) {
        Some(h) => h,
        None => {
            respond_error(request, 400, "invalid content hash hex");
            return;
        }
    };

    // Step 1: Get entry + manifest (try local first)
    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        state.datastore_addr,
        DatastoreNodeMsg::Get {
            content_hash,
            reply_to: *inbox.addr(),
        },
    );

    state.metrics.record_get(&content_hash.to_hex());


    let (entry, manifest) = match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::GetOk { entry, manifest }) => (entry, manifest),
        Some(DatastoreResponse::NotFound) => {
            // Try remote GET
            match try_remote_get(content_hash, state) {
                Some((e, m)) => (e, m),
                None => {
                    respond_error(request, 404, "not found");
                    return;
                }
            }
        }
        Some(DatastoreResponse::Error { reason }) => {
            respond_error(request, 500, &reason);
            return;
        }
        _ => {
            respond_error(request, 504, "timeout");
            return;
        }
    };

    // Step 2: Read all chunks
    let _ = entry; // entry used for metadata context, manifest for chunks
    let mut chunk_data = Vec::new();
    for chunk_ref in &manifest.chunks {
        let chunk_inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
            Ok(i) => i,
            Err(_) => {
                respond_error(request, 500, "failed to create inbox");
                return;
            }
        };

        let _ = state.runtime.send_to(
            state.datastore_addr,
            DatastoreNodeMsg::ReadChunk {
                hash: chunk_ref.hash,
                reply_to: *chunk_inbox.addr(),
            },
        );

        match poll_response(&chunk_inbox, POLL_TIMEOUT) {
            Some(DatastoreResponse::ChunkOk { hash, data }) => {
                chunk_data.push((hash, data));
            }
            _ => {
                respond_error(request, 500, "failed to read chunk");
                return;
            }
        }
    }

    // Step 3: Reassemble
    match reassemble_blob(&manifest, &chunk_data) {
        Ok(data) => respond_bytes(request, &data),
        Err(e) => respond_error(request, 500, &format!("reassembly failed: {e:?}")),
    }
}

// ── DELETE handler ──────────────────────────────────────────────────────

fn handle_delete(request: tiny_http::Request, url: &str, state: &ApiState) {
    if let Err((status, msg)) = check_auth(&request, state) {
        respond_error(request, status, &msg);
        return;
    }
    let params = parse_query_string(url);
    let hash_hex = match params.get("hash") {
        Some(h) => h,
        None => {
            respond_error(request, 400, "missing ?hash= parameter");
            return;
        }
    };

    let content_hash = match ContentHash::from_hex(hash_hex) {
        Some(h) => h,
        None => {
            respond_error(request, 400, "invalid content hash hex");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        state.datastore_addr,
        DatastoreNodeMsg::Delete {
            content_hash,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::DeleteOk { content_hash }) => {
            let hex = content_hash.to_hex();
            state.metrics.record_delete(&hex, 0);
            let json = serde_json::json!({ "content_hash": hex }).to_string();
            respond_json(request, &json);
        }
        Some(DatastoreResponse::NotFound) => {
            respond_error(request, 404, "not found");
        }
        Some(DatastoreResponse::Error { reason }) => {
            respond_error(request, 500, &reason);
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

// ── LIST handler ────────────────────────────────────────────────────────

fn handle_list(request: tiny_http::Request, url: &str, state: &ApiState) {
    if let Err((status, msg)) = check_auth(&request, state) {
        respond_error(request, status, &msg);
        return;
    }
    let params = parse_query_string(url);
    let name_filter = params.get("name").cloned();
    let all = params.get("all").map_or(false, |v| v == "true" || v == "1");

    if all {
        handle_list_swarm(request, name_filter, state);
    } else {
        handle_list_local(request, name_filter, state);
    }
}

fn handle_list_local(
    request: tiny_http::Request,
    name_filter: Option<String>,
    state: &ApiState,
) {
    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        state.datastore_addr,
        DatastoreNodeMsg::List {
            name_filter,
            all: false,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::ListOk { entries }) => {
            let json = serde_json::json!({ "entries": entries_to_json(&entries) }).to_string();
            respond_json(request, &json);
        }
        Some(DatastoreResponse::Error { reason }) => {
            respond_error(request, 500, &reason);
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

/// ListSwarm fan-out: query local + all peers, merge and deduplicate.
fn handle_list_swarm(
    request: tiny_http::Request,
    name_filter: Option<String>,
    state: &ApiState,
) {
    let mut all_entries = Vec::new();

    // Query local
    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        state.metadata_addr,
        MetadataMsg::ListLocal {
            name_filter: name_filter.clone(),
            reply_to: *inbox.addr(),
        },
    );

    if let Some(DatastoreResponse::ListOk { entries }) = poll_response(&inbox, POLL_TIMEOUT) {
        all_entries.extend(entries);
    }

    // Query each peer
    let peers = state.peers.lock().unwrap().clone();
    for peer in &peers {
        let peer_inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
            Ok(i) => i,
            Err(_) => continue,
        };

        let _ = state.runtime.send_to(
            peer.metadata,
            MetadataMsg::ListLocal {
                name_filter: name_filter.clone(),
                reply_to: *peer_inbox.addr(),
            },
        );

        if let Some(DatastoreResponse::ListOk { entries }) =
            poll_response(&peer_inbox, Duration::from_secs(2))
        {
            all_entries.extend(entries);
        }
    }

    // Deduplicate by content_hash
    let mut seen = std::collections::HashSet::new();
    all_entries.retain(|e| seen.insert(e.content_hash));

    let json = serde_json::json!({ "entries": entries_to_json(&all_entries) }).to_string();
    respond_json(request, &json);
}

// ── STATUS handler ──────────────────────────────────────────────────────

fn handle_status(request: tiny_http::Request, state: &ApiState) {
    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        state.datastore_addr,
        DatastoreNodeMsg::Status {
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::NodeStatus { node_id }) => {
            let hex: String = node_id.0.iter().map(|b| format!("{b:02x}")).collect();
            let json = serde_json::json!({ "node_id": hex }).to_string();
            respond_json(request, &json);
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

// ── Remote GET orchestration ────────────────────────────────────────────

/// Try to fetch an object from peers when not found locally.
/// Returns (entry, manifest) on success, stores chunks locally as a side effect.
fn try_remote_get(
    content_hash: ContentHash,
    state: &ApiState,
) -> Option<(crate::types::ObjectEntry, crate::types::ObjectManifest)> {
    let peers = state.peers.lock().unwrap().clone();

    for peer in &peers {
        // Ask peer's metadata actor for the object
        let find_inbox = state.runtime.new_inbox::<DatastoreResponse>().ok()?;
        let _ = state.runtime.send_to(
            peer.metadata,
            MetadataMsg::HandleFindObject {
                from: swactor::transport::NodeId([0; 32]), // placeholder
                content_hash,
                reply_to: *find_inbox.addr(),
            },
        );

        let (entry, _) = match poll_response(&find_inbox, Duration::from_secs(2)) {
            Some(DatastoreResponse::GetOk { entry, manifest }) => (entry, manifest),
            _ => continue,
        };

        // Get manifest from peer's blob store
        let manifest_inbox = state.runtime.new_inbox::<DatastoreResponse>().ok()?;
        let _ = state.runtime.send_to(
            peer.blob_store,
            BlobStoreMsg::ReadManifest {
                hash: content_hash,
                reply_to: *manifest_inbox.addr(),
            },
        );

        let manifest = match poll_response(&manifest_inbox, Duration::from_secs(2)) {
            Some(DatastoreResponse::ManifestOk { manifest }) => manifest,
            _ => continue,
        };

        // Fetch each chunk from peer and store locally
        let hash_hex = content_hash.to_hex();
        state.metrics.begin_transfer(&hash_hex, manifest.chunks.len());
        let mut all_ok = true;
        for chunk_ref in &manifest.chunks {
            let chunk_inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
                Ok(i) => i,
                Err(_) => {
                    all_ok = false;
                    break;
                }
            };

            let _ = state.runtime.send_to(
                peer.blob_store,
                BlobStoreMsg::ReadChunk {
                    hash: chunk_ref.hash,
                    reply_to: *chunk_inbox.addr(),
                },
            );

            match poll_response(&chunk_inbox, Duration::from_secs(2)) {
                Some(DatastoreResponse::ChunkOk { hash, data }) => {
                    // Store locally
                    let store_inbox =
                        match state.runtime.new_inbox::<DatastoreResponse>() {
                            Ok(i) => i,
                            Err(_) => {
                                all_ok = false;
                                break;
                            }
                        };
                    let _ = state.runtime.send_to(
                        state.blob_store_addr,
                        BlobStoreMsg::WriteChunk {
                            hash,
                            data,
                            reply_to: *store_inbox.addr(),
                        },
                    );
                    // Wait for confirmation
                    let _ = poll_response(&store_inbox, Duration::from_secs(2));
                    state.metrics.advance_transfer(&hash_hex);
                }
                _ => {
                    all_ok = false;
                    break;
                }
            }
        }

        state.metrics.end_transfer(&hash_hex);


        if !all_ok {
            continue;
        }

        // Store manifest locally
        let m_inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
            Ok(i) => i,
            Err(_) => continue,
        };
        let _ = state.runtime.send_to(
            state.blob_store_addr,
            BlobStoreMsg::WriteManifest {
                manifest: manifest.clone(),
                reply_to: *m_inbox.addr(),
            },
        );
        let _ = poll_response(&m_inbox, Duration::from_secs(2));

        // Store entry+manifest in local metadata
        let put_inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
            Ok(i) => i,
            Err(_) => continue,
        };
        let _ = state.runtime.send_to(
            state.metadata_addr,
            MetadataMsg::PutObject {
                entry: entry.clone(),
                manifest: manifest.clone(),
                reply_to: *put_inbox.addr(),
            },
        );
        let _ = poll_response(&put_inbox, Duration::from_secs(2));

        return Some((entry, manifest));
    }

    None
}

// ── Auth grant/revoke handlers ──────────────────────────────────────────

fn parse_node_id_hex(hex: &str) -> Option<NodeId> {
    if hex.len() != 64 {
        return None;
    }
    let mut bytes = [0u8; 32];
    for (i, chunk) in hex.as_bytes().chunks(2).enumerate() {
        let hi = hex_val(chunk[0])?;
        let lo = hex_val(chunk[1])?;
        bytes[i] = (hi << 4) | lo;
    }
    Some(NodeId(bytes))
}

fn handle_auth_grant(request: tiny_http::Request, url: &str, state: &ApiState) {
    let requester = match check_auth_identity(&request, state) {
        Ok(id) => id,
        Err((status, msg)) => {
            respond_error(request, status, &msg);
            return;
        }
    };

    let params = parse_query_string(url);
    let key_hex = match params.get("key") {
        Some(k) => k,
        None => {
            respond_error(request, 400, "missing ?key= parameter");
            return;
        }
    };

    let key = match parse_node_id_hex(key_hex) {
        Some(k) => k,
        None => {
            respond_error(request, 400, "invalid key hex (expected 64 hex chars)");
            return;
        }
    };

    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => {
            respond_error(request, 400, "auth not enabled on this node");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let label = params.get("name").cloned();

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::Grant {
            requester,
            key,
            label,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::Bool(true)) => {
            respond_json(request, &serde_json::json!({ "ok": true }).to_string());
        }
        Some(DatastoreResponse::Denied { reason }) => {
            respond_error(request, 403, &format!("{reason:?}"));
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

fn handle_auth_revoke(request: tiny_http::Request, url: &str, state: &ApiState) {
    let requester = match check_auth_identity(&request, state) {
        Ok(id) => id,
        Err((status, msg)) => {
            respond_error(request, status, &msg);
            return;
        }
    };

    let params = parse_query_string(url);
    let key_hex = match params.get("key") {
        Some(k) => k,
        None => {
            respond_error(request, 400, "missing ?key= parameter");
            return;
        }
    };

    let key = match parse_node_id_hex(key_hex) {
        Some(k) => k,
        None => {
            respond_error(request, 400, "invalid key hex (expected 64 hex chars)");
            return;
        }
    };

    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => {
            respond_error(request, 400, "auth not enabled on this node");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::Revoke {
            requester,
            key,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::Bool(true)) => {
            respond_json(request, &serde_json::json!({ "ok": true }).to_string());
        }
        Some(DatastoreResponse::Denied { reason }) => {
            respond_error(request, 403, &format!("{reason:?}"));
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

// ── Access request handlers ──────────────────────────────────────────────

fn handle_auth_request(mut request: tiny_http::Request, state: &ApiState) {
    let caller = match check_auth_signature_only(&request, state) {
        Ok(id) => id,
        Err((status, msg)) => {
            respond_error(request, status, &msg);
            return;
        }
    };

    // Read JSON body
    let mut body_bytes = Vec::new();
    if request.as_reader().read_to_end(&mut body_bytes).is_err() {
        return;
    }

    let body: serde_json::Value = match serde_json::from_slice(&body_bytes) {
        Ok(v) => v,
        Err(e) => {
            respond_error(request, 400, &format!("invalid JSON: {e}"));
            return;
        }
    };

    let name = match body.get("name").and_then(|v| v.as_str()) {
        Some(n) if !n.trim().is_empty() => n.trim().to_string(),
        _ => {
            respond_error(request, 400, "name is required");
            return;
        }
    };

    if name.len() > 64 {
        respond_error(request, 400, "name must be 64 characters or fewer");
        return;
    }

    let message = body
        .get("message")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    if message.len() > 256 {
        respond_error(request, 400, "message must be 256 characters or fewer");
        return;
    }

    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => {
            respond_error(request, 400, "auth not enabled on this node");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::SubmitAccessRequest {
            key: caller,
            name,
            message,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::Bool(true)) => {
            respond_json(request, &serde_json::json!({ "ok": true }).to_string());
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

fn handle_auth_requests_list(request: tiny_http::Request, state: &ApiState) {
    let requester = match check_auth_identity(&request, state) {
        Ok(id) => id,
        Err((status, msg)) => {
            respond_error(request, status, &msg);
            return;
        }
    };

    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => {
            respond_error(request, 400, "auth not enabled on this node");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::ListAccessRequests {
            requester,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::AccessRequests { requests }) => {
            let json_list: Vec<serde_json::Value> = requests
                .iter()
                .map(|r| {
                    let key_hex: String = r.key.0.iter().map(|b| format!("{b:02x}")).collect();
                    serde_json::json!({
                        "key": key_hex,
                        "name": r.name,
                        "message": r.message,
                        "requested_at": r.requested_at,
                    })
                })
                .collect();
            respond_json(request, &serde_json::json!(json_list).to_string());
        }
        Some(DatastoreResponse::Denied { reason }) => {
            respond_error(request, 403, &format!("{reason:?}"));
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

fn handle_auth_keys_list(request: tiny_http::Request, state: &ApiState) {
    let requester = match check_auth_identity(&request, state) {
        Ok(id) => id,
        Err((status, msg)) => {
            respond_error(request, status, &msg);
            return;
        }
    };

    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => {
            respond_error(request, 400, "auth not enabled on this node");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::ListAuthorizedKeys {
            requester,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::AuthorizedKeys { keys }) => {
            let json_list: Vec<serde_json::Value> = keys
                .iter()
                .map(|k| {
                    let key_hex: String = k.key.0.iter().map(|b| format!("{b:02x}")).collect();
                    serde_json::json!({
                        "key": key_hex,
                        "label": k.label,
                    })
                })
                .collect();
            respond_json(request, &serde_json::json!(json_list).to_string());
        }
        Some(DatastoreResponse::Denied { reason }) => {
            respond_error(request, 403, &format!("{reason:?}"));
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

fn handle_auth_deny(request: tiny_http::Request, url: &str, state: &ApiState) {
    let requester = match check_auth_identity(&request, state) {
        Ok(id) => id,
        Err((status, msg)) => {
            respond_error(request, status, &msg);
            return;
        }
    };

    let params = parse_query_string(url);
    let key_hex = match params.get("key") {
        Some(k) => k,
        None => {
            respond_error(request, 400, "missing ?key= parameter");
            return;
        }
    };

    let key = match parse_node_id_hex(key_hex) {
        Some(k) => k,
        None => {
            respond_error(request, 400, "invalid key hex (expected 64 hex chars)");
            return;
        }
    };

    let gateway_addr = match state.gateway_addr {
        Some(addr) => addr,
        None => {
            respond_error(request, 400, "auth not enabled on this node");
            return;
        }
    };

    let inbox = match state.runtime.new_inbox::<DatastoreResponse>() {
        Ok(i) => i,
        Err(_) => {
            respond_error(request, 500, "failed to create inbox");
            return;
        }
    };

    let _ = state.runtime.send_to(
        gateway_addr,
        GatewayMsg::DenyAccessRequest {
            requester,
            key,
            reply_to: *inbox.addr(),
        },
    );

    match poll_response(&inbox, POLL_TIMEOUT) {
        Some(DatastoreResponse::Bool(true)) => {
            respond_json(request, &serde_json::json!({ "ok": true }).to_string());
        }
        Some(DatastoreResponse::Denied { reason }) => {
            respond_error(request, 403, &format!("{reason:?}"));
        }
        _ => {
            respond_error(request, 504, "timeout");
        }
    }
}

// ── Server startup ──────────────────────────────────────────────────────

/// Start the HTTP API server for the datastore.
///
/// Returns a shared shutdown flag (set to `true` to stop the server)
/// and a peer list that can be updated to enable remote operations.
pub fn start_api_server(
    runtime: Arc<Runtime>,
    datastore_addr: ActorAddress,
    metadata_addr: ActorAddress,
    blob_store_addr: ActorAddress,
    gateway_addr: Option<ActorAddress>,
    port: u16,
    metrics: Arc<DatastoreMetrics>,
) -> (Arc<AtomicBool>, Arc<Mutex<Vec<PeerInfo>>>) {
    let shutdown = Arc::new(AtomicBool::new(false));
    let peers: Arc<Mutex<Vec<PeerInfo>>> = Arc::new(Mutex::new(Vec::new()));

    let state = Arc::new(ApiState {
        runtime,
        datastore_addr,
        metadata_addr,
        blob_store_addr,
        gateway_addr,
        peers: Arc::clone(&peers),
        metrics,
    });

    let addr = format!("0.0.0.0:{port}");
    let server = tiny_http::Server::http(&addr).expect("failed to bind datastore API server");
    let server = Arc::new(server);

    for _ in 0..4 {
        let server = Arc::clone(&server);
        let state = Arc::clone(&state);
        let shutdown = Arc::clone(&shutdown);
        thread::spawn(move || {
            loop {
                if shutdown.load(Ordering::Relaxed) {
                    break;
                }
                let request = match server.recv_timeout(Duration::from_millis(500)) {
                    Ok(Some(r)) => r,
                    Ok(None) => continue,
                    Err(_) => break,
                };

                let url = request.url().to_string();
                let path = url.split('?').next().unwrap_or(&url);
                let method = request.method().as_str();

                match (method, path) {
                    ("POST", "/api/put") => handle_put(request, &url, &state),
                    ("GET", "/api/get") => handle_get(request, &url, &state),
                    ("GET", "/api/data") => handle_data(request, &url, &state),
                    ("POST", "/api/delete") => handle_delete(request, &url, &state),
                    ("GET", "/api/list") => handle_list(request, &url, &state),
                    ("GET", "/api/status") => handle_status(request, &state),
                    ("POST", "/api/auth/grant") => handle_auth_grant(request, &url, &state),
                    ("POST", "/api/auth/revoke") => handle_auth_revoke(request, &url, &state),
                    ("POST", "/api/auth/request") => handle_auth_request(request, &state),
                    ("GET", "/api/auth/requests") => handle_auth_requests_list(request, &state),
                    ("GET", "/api/auth/keys") => handle_auth_keys_list(request, &state),
                    ("POST", "/api/auth/deny") => handle_auth_deny(request, &url, &state),
                    ("GET", "/") => respond_html(request),
                    ("GET", "/crypto.wasm") => respond_wasm(request),
                    ("GET", "/admin") => respond_admin_html(request),
                    _ => {
                        respond_error(request, 404, "not found");
                    }
                }
            }
        });
    }

    (shutdown, peers)
}
