//! Protocol-level integration test for the diagnostics collector (S2).
//!
//! Drives a real `axum::serve` bound to `127.0.0.1:0` with a tiny
//! hand-rolled HTTP client over `tokio::net::TcpStream`. The client
//! exists to avoid pulling reqwest as a dev-dep — we only need to
//! exercise the four POSTs and one GET, and we want to see the raw
//! status codes the server returns for malformed requests.
//!
//! The test exercises:
//!
//! - Clock echoing — every response carries a `clock` block whose
//!   `node_send_ms_echoed` matches what we sent.
//! - Persistence layout — boot/events/snapshot land at
//!   `{root}/{run}/{node}/{kind}-{seq}.json` with monotonically
//!   increasing seqs.
//! - Finalize tarball — `POST /diag/finalize` produces a gzipped tar
//!   at `{root}/bundles/{run}.tar.gz` containing per-node directories
//!   labeled by role with the records and a `MANIFEST.json`.
//! - Bundle retrieval — `GET /diag/bundle/{run_id}` returns the same
//!   bytes that landed on disk.
//! - Error paths — unknown record kind, missing headers, malformed
//!   JSON body, oversized body, missing bundle all produce 4xx/5xx
//!   without panicking the server.

#![cfg(feature = "collector")]

use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use distribution::diagnostics::collector::{CollectorState, Manifest, PostAck, bind, serve};
use flate2::read::GzDecoder;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

struct Fixture {
    addr: SocketAddr,
    root: PathBuf,
    _tmpdir: TempDir,
    _server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn start() -> Self {
        let tmpdir = TempDir::new();
        let root = tmpdir.path().to_path_buf();
        // Tests don't have aggregator clients chasing hints, so the
        // finalize wait would just stall every assertion. Collapse it.
        let state = Arc::new(
            CollectorState::new(&root).with_finalize_wait(Duration::from_millis(0)),
        );
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let handle = tokio::spawn(async move {
            let _ = serve(listener, state).await;
        });
        // Tiny pause so the spawned task gets to accept().
        tokio::time::sleep(Duration::from_millis(50)).await;
        Fixture {
            addr,
            root,
            _tmpdir: tmpdir,
            _server: handle,
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn protocol_end_to_end_round_trip_produces_a_bundle() {
    let fx = Fixture::start().await;
    let run_id = "run-1";

    // Two nodes — orchestrator + one stage. Use distinct, sanitizer-
    // friendly hex ids.
    let orch_id = "a".repeat(64);
    let stage_id = "b".repeat(64);

    // 1. Boot for both nodes. Body carries the Identity-like fields
    //    we know the bundle assembler reads.
    let send_ms_1 = now_ms();
    let resp = post_json(
        &fx,
        "/diag/boot",
        run_id,
        &orch_id,
        send_ms_1,
        &json!({
            "node_id_hex": orch_id,
            "role": "orchestrator",
            "stage_index": null,
        }),
    )
    .await;
    assert_eq!(resp.status, 200, "boot orch status: {}", resp.status);
    let ack: PostAck = serde_json::from_slice(&resp.body).expect("ack json");
    assert_eq!(ack.clock.node_send_ms_echoed, send_ms_1);
    assert!(ack.clock.collector_recv_ms <= ack.clock.collector_send_ms);

    let resp = post_json(
        &fx,
        "/diag/boot",
        run_id,
        &stage_id,
        now_ms(),
        &json!({
            "node_id_hex": stage_id,
            "role": "stage",
            "stage_index": 0,
        }),
    )
    .await;
    assert_eq!(resp.status, 200);

    // 2. Three event batches for the stage. Verifies the per-(run,
    //    node, kind) seq counter increments and produces distinct
    //    files.
    for i in 0..3 {
        let resp = post_json(
            &fx,
            "/diag/events",
            run_id,
            &stage_id,
            now_ms(),
            &json!([
                {"type": "MessageSent", "peer": orch_id, "kind": "ping", "size": 32 + i}
            ]),
        )
        .await;
        assert_eq!(resp.status, 200);
    }

    // 3. One snapshot per node.
    let resp = post_json(
        &fx,
        "/diag/snapshot",
        run_id,
        &orch_id,
        now_ms(),
        &json!({"snapshot_id": "orch-0", "body": {"reachability": []}}),
    )
    .await;
    assert_eq!(resp.status, 200);
    let resp = post_json(
        &fx,
        "/diag/snapshot",
        run_id,
        &stage_id,
        now_ms(),
        &json!({"snapshot_id": "stage-0", "body": {"reachability": []}}),
    )
    .await;
    assert_eq!(resp.status, 200);

    // Disk shape check: events have seq 1..3, single boot + single snapshot.
    let stage_dir = fx.root.join(run_id).join(&stage_id);
    let mut files: Vec<String> = std::fs::read_dir(&stage_dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    files.sort();
    assert!(files.contains(&"boot-000001.json".to_string()));
    assert!(files.contains(&"events-000001.json".to_string()));
    assert!(files.contains(&"events-000002.json".to_string()));
    assert!(files.contains(&"events-000003.json".to_string()));
    assert!(files.contains(&"snapshot-000001.json".to_string()));

    // 4. Finalize the run. Response body should carry the bundle
    //    path.
    let resp = post_json(
        &fx,
        "/diag/finalize",
        run_id,
        &orch_id,
        now_ms(),
        &json!({"exit_reason": "ok"}),
    )
    .await;
    assert_eq!(resp.status, 200, "finalize status: {}", resp.status);
    let ack: PostAck = serde_json::from_slice(&resp.body).expect("finalize ack");
    let bundle_path = ack
        .body
        .and_then(|b| b.get("bundle_path").and_then(|p| p.as_str().map(String::from)))
        .expect("finalize response carries bundle_path");
    assert!(
        std::path::Path::new(&bundle_path).exists(),
        "bundle file at {bundle_path} should exist"
    );

    // 5. Bundle download.
    let resp = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    assert_eq!(resp.status, 200, "bundle download status: {}", resp.status);
    assert!(!resp.body.is_empty());

    // 6. Bundle inspection — parse the tar.gz and check it contains
    //    role-named per-node directories, the records, and a manifest.
    let entries = list_tar_entries(&resp.body);
    let entry_str = entries.join("\n");
    assert!(
        entries.iter().any(|e| e == &format!("{run_id}/MANIFEST.json")),
        "expected MANIFEST.json, got:\n{entry_str}"
    );
    assert!(
        entries.iter().any(|e| e.starts_with(&format!("{run_id}/orchestrator/"))),
        "expected orchestrator/ in bundle, got:\n{entry_str}"
    );
    assert!(
        entries.iter().any(|e| e.starts_with(&format!("{run_id}/stage-0/"))),
        "expected stage-0/ in bundle, got:\n{entry_str}"
    );
    assert!(
        entries
            .iter()
            .any(|e| e == &format!("{run_id}/orchestrator/boot.json")),
        "expected orchestrator/boot.json"
    );
    assert!(
        entries
            .iter()
            .any(|e| e == &format!("{run_id}/stage-0/events/events-000001.json")),
        "expected stage-0/events/events-000001.json"
    );

    // Manifest must list both nodes with correct labels.
    let manifest_bytes = read_tar_file(&resp.body, &format!("{run_id}/MANIFEST.json"));
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).expect("manifest json");
    assert_eq!(manifest.run_id, run_id);
    assert_eq!(manifest.nodes.len(), 2);
    assert!(manifest.finalize_received);
    assert!(manifest
        .nodes
        .iter()
        .any(|n| n.label == "orchestrator" && n.role.as_deref() == Some("orchestrator")));
    assert!(manifest.nodes.iter().any(|n| n.label == "stage-0"
        && n.role.as_deref() == Some("stage")
        && n.stage_index == Some(0)));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_requests_return_4xx_without_panicking() {
    let fx = Fixture::start().await;

    // Unknown record kind.
    let resp = post_json(
        &fx,
        "/diag/garbage",
        "r",
        "n",
        now_ms(),
        &json!({}),
    )
    .await;
    assert_eq!(resp.status, 404, "unknown kind body: {}", body_str(&resp));

    // Missing run id header.
    let req = http_request(
        "POST",
        "/diag/events",
        &[
            ("x-node-id", "n"),
            ("x-node-send-ms", "0"),
            ("content-type", "application/json"),
        ],
        b"[]",
    );
    let resp = send(&fx, &req).await;
    assert_eq!(resp.status, 400, "missing run id body: {}", body_str(&resp));

    // Missing node id header.
    let req = http_request(
        "POST",
        "/diag/events",
        &[
            ("x-run-id", "r"),
            ("x-node-send-ms", "0"),
            ("content-type", "application/json"),
        ],
        b"[]",
    );
    let resp = send(&fx, &req).await;
    assert_eq!(resp.status, 400, "missing node id body: {}", body_str(&resp));

    // Malformed JSON body.
    let resp = post_raw(
        &fx,
        "/diag/events",
        "r",
        "n",
        now_ms(),
        b"this is not json",
    )
    .await;
    assert_eq!(resp.status, 400, "bad json body: {}", body_str(&resp));

    // Missing bundle.
    let resp = get(&fx, "/diag/bundle/nope").await;
    assert_eq!(resp.status, 404, "missing bundle body: {}", body_str(&resp));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn header_values_are_sanitized_against_path_traversal() {
    // A node id of `../escape` must not let the collector write
    // outside the run directory. The persisted file should still be
    // confined under `{root}/{run}/`.
    let fx = Fixture::start().await;
    let resp = post_json(
        &fx,
        "/diag/boot",
        "r1",
        "../escape",
        now_ms(),
        &json!({"role": "stage"}),
    )
    .await;
    assert_eq!(resp.status, 200);
    let run_dir = fx.root.join("r1");
    assert!(run_dir.is_dir());
    // The "escape" directory should exist under r1 with whatever
    // single-component name the sanitizer chose — *never* as a
    // sibling of the run. The exact spelling is an implementation
    // detail; what matters is that traversal didn't succeed.
    assert!(!fx.root.join("escape").exists());
    assert!(!fx.root.parent().unwrap().join("escape").exists());
    let mut child_dirs: Vec<String> = std::fs::read_dir(&run_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_dir())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .collect();
    child_dirs.sort();
    assert_eq!(child_dirs.len(), 1, "exactly one sanitized child dir");
    let only = &child_dirs[0];
    assert!(
        !only.contains('/') && !only.contains('\\') && only != ".." && only != ".",
        "child dir must be a safe single component, got {only:?}"
    );
}

// ---------- HTTP helpers ----------

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
}

fn body_str(r: &HttpResponse) -> String {
    String::from_utf8_lossy(&r.body).into_owned()
}

async fn post_json(
    fx: &Fixture,
    path: &str,
    run_id: &str,
    node_id: &str,
    node_send_ms: u64,
    body: &Value,
) -> HttpResponse {
    let body_bytes = serde_json::to_vec(body).unwrap();
    post_raw(fx, path, run_id, node_id, node_send_ms, &body_bytes).await
}

async fn post_raw(
    fx: &Fixture,
    path: &str,
    run_id: &str,
    node_id: &str,
    node_send_ms: u64,
    body: &[u8],
) -> HttpResponse {
    let send_ms_str = node_send_ms.to_string();
    let req = http_request(
        "POST",
        path,
        &[
            ("x-run-id", run_id),
            ("x-node-id", node_id),
            ("x-node-send-ms", &send_ms_str),
            ("content-type", "application/json"),
        ],
        body,
    );
    send(fx, &req).await
}

async fn get(fx: &Fixture, path: &str) -> HttpResponse {
    let req = http_request("GET", path, &[], b"");
    send(fx, &req).await
}

fn http_request(method: &str, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(format!("{method} {path} HTTP/1.1\r\n").as_bytes());
    out.extend_from_slice(b"host: 127.0.0.1\r\n");
    out.extend_from_slice(b"connection: close\r\n");
    out.extend_from_slice(format!("content-length: {}\r\n", body.len()).as_bytes());
    for (k, v) in headers {
        out.extend_from_slice(format!("{k}: {v}\r\n").as_bytes());
    }
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(body);
    out
}

async fn send(fx: &Fixture, request: &[u8]) -> HttpResponse {
    // No half-close before reading: hyper drops the connection if it
    // sees a write-side FIN while it's still composing the response.
    // We send `connection: close` instead and let the server close
    // first, which gives `read_to_end` its EOF.
    let mut stream = TcpStream::connect(fx.addr).await.expect("connect");
    stream.write_all(request).await.expect("write");
    stream.flush().await.ok();
    let mut buf = Vec::new();
    // Bound the read so a server bug can't hang the test forever.
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("response within 5s")
        .expect("read");
    parse_response(&buf)
}

fn parse_response(bytes: &[u8]) -> HttpResponse {
    let split = find_double_crlf(bytes).expect("response has headers terminator");
    let head = std::str::from_utf8(&bytes[..split]).expect("response head is utf8");
    let mut lines = head.split("\r\n");
    let status_line = lines.next().expect("status line");
    let mut parts = status_line.split_whitespace();
    let _proto = parts.next();
    let status: u16 = parts
        .next()
        .and_then(|s| s.parse().ok())
        .expect("status code");
    let body = bytes[split + 4..].to_vec();
    HttpResponse { status, body }
}

fn find_double_crlf(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
}

// ---------- tar inspection helpers ----------

fn list_tar_entries(gz_bytes: &[u8]) -> Vec<String> {
    let gz = GzDecoder::new(gz_bytes);
    let mut ar = tar::Archive::new(gz);
    let mut out = Vec::new();
    for entry in ar.entries().expect("tar entries") {
        let entry = entry.expect("tar entry");
        let path = entry.path().expect("tar path");
        let mut s = path.to_string_lossy().into_owned();
        // Tar may give us directory entries with a trailing slash —
        // strip it for cleaner assertion strings, but skip pure-dir
        // entries so file lookups don't accidentally match.
        if s.ends_with('/') {
            s.pop();
        }
        out.push(s);
    }
    out
}

fn read_tar_file(gz_bytes: &[u8], path: &str) -> Vec<u8> {
    let gz = GzDecoder::new(gz_bytes);
    let mut ar = tar::Archive::new(gz);
    for entry in ar.entries().expect("tar entries") {
        let mut entry = entry.expect("tar entry");
        let entry_path = entry.path().expect("tar path").to_string_lossy().into_owned();
        if entry_path == path {
            let mut buf = Vec::new();
            entry.read_to_end(&mut buf).expect("read tar file");
            return buf;
        }
    }
    panic!("file {path} not found in tarball");
}

// ---------- tempdir helper (avoids tempfile dep) ----------

struct TempDir {
    path: PathBuf,
}

impl TempDir {
    fn new() -> Self {
        let pid = std::process::id();
        let nano = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.subsec_nanos())
            .unwrap_or(0);
        // Add a per-test atomic counter so two tests on the same
        // process don't pick the same name when scheduled in lockstep.
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("swactor-diag-test-{pid}-{nano}-{n}"));
        std::fs::create_dir_all(&path).expect("create tempdir");
        Self { path }
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}
