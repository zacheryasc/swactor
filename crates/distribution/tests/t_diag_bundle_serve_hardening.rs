//! Coverage 2.5 — bundle serve hardening under run-id reuse
//! (`N3_COVERAGE_EXTENSION_SPEC.md §2.5`).
//!
//! Spec close criterion: "a collector unit test writes two phases of
//! staging with an intervening finalize, deletes the first-phase
//! node, and verifies the second `GET` serves the richer bundle and
//! that the cleared node does not appear in the manifest."
//!
//! The `1779733878` postmortem named the bug: when a run id is reused
//! across the failed-first-lease / successful-second-lease shape, a
//! finalize record from the first phase pins a stale canonical bundle
//! in the collector's cache. A subsequent `GET` serves the stale 5.3 KB
//! bundle instead of synthesizing the rich 9.3 MB one from current
//! staging.
//!
//! This test exercises the spec's two-phase scenario at the unit
//! level: phase-1 finalize lands → canonical builds (1 node);
//! phase-2 boot adds a second node to staging; the subsequent GET
//! must reflect both nodes in the served bundle's manifest, not just
//! the canonical's stale single-node snapshot.

#![cfg(feature = "collector")]

use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use distribution::diagnostics::collector::{CollectorState, Manifest, bind, serve};
use flate2::read::GzDecoder;
use serde_json::{Value, json};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn run_id_reuse_serves_richer_bundle_not_stale_canonical() {
    // Spec §2.5 D-layer close criterion. Phase 1 lands node A's
    // records + finalize, producing a 1-node canonical bundle.
    // Phase 2 adds node B's boot to staging. The subsequent GET
    // must return a 2-node bundle (the richer surface), not the
    // cached 1-node canonical.
    let fx = Fixture::start().await;
    let run_id = "reused-run-id";
    let node_a = "a".repeat(64);
    let node_b = "b".repeat(64);

    // ── Phase 1: node A's full lifecycle ──────────────────────────
    let boot_a = boot_payload(run_id, &node_a, "orchestrator", 0);
    assert_eq!(
        post_json(&fx, "/diag/boot", run_id, &node_a, 100, &boot_a).await.status,
        200,
        "phase-1 boot must succeed"
    );
    let events_a = json!([]);
    assert_eq!(
        post_json(&fx, "/diag/events", run_id, &node_a, 200, &events_a).await.status,
        200,
        "phase-1 events must succeed"
    );
    let finalize_a = json!({"finalize_at_ms": 300});
    assert_eq!(
        post_json(&fx, "/diag/finalize", run_id, &node_a, 300, &finalize_a).await.status,
        200,
        "phase-1 finalize must succeed (builds canonical)"
    );

    // Verify the canonical was built and a GET serves it correctly
    // at this point (1 node).
    let r1 = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    assert_eq!(r1.status, 200, "phase-1 GET must succeed");
    let m1: Manifest = serde_json::from_slice(&read_tar_file(
        &r1.body,
        &format!("{run_id}/MANIFEST.json"),
    ))
    .expect("phase-1 manifest parses");
    assert_eq!(
        m1.nodes.len(),
        1,
        "phase-1 manifest must list exactly node A; got {:#?}",
        m1.nodes
    );
    assert!(
        m1.finalize_received,
        "phase-1 manifest must show finalize_received=true",
    );

    // ── Phase 2: node B's boot lands after the phase-1 finalize ──
    let boot_b = boot_payload(run_id, &node_b, "stage", 0);
    assert_eq!(
        post_json(&fx, "/diag/boot", run_id, &node_b, 1000, &boot_b).await.status,
        200,
        "phase-2 boot must succeed"
    );

    // ── The contract: GET must now reflect the richer 2-node
    // surface, not the stale 1-node canonical. Without coverage 2.5's
    // node-count heuristic in `download_bundle`, the handler would
    // serve the cached canonical from phase 1 (1 node only) and the
    // bundle reader would not see node B. With the heuristic, the
    // current `nodes.len()` (2) exceeds the canonical's snapshot
    // (1) and the handler falls through to synthesis.
    let r2 = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    assert_eq!(r2.status, 200, "phase-2 GET must succeed");
    let m2: Manifest = serde_json::from_slice(&read_tar_file(
        &r2.body,
        &format!("{run_id}/MANIFEST.json"),
    ))
    .expect("phase-2 manifest parses");
    assert_eq!(
        m2.nodes.len(),
        2,
        "phase-2 GET must return the richer 2-node surface (not the stale 1-node canonical); got {:#?}",
        m2.nodes
    );
    // Both nodes are present.
    assert!(
        m2.nodes.iter().any(|n| n.node_id_hex == node_a),
        "phase-2 manifest must include node A from phase 1",
    );
    assert!(
        m2.nodes.iter().any(|n| n.node_id_hex == node_b),
        "phase-2 manifest must include node B from phase 2",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stable_canonical_keeps_serving_when_staging_has_not_grown() {
    // The other side of the contract: when staging matches the
    // canonical's snapshot (no new node has arrived), the handler
    // continues to serve the cached canonical. This is the
    // optimization the coverage 2.5 heuristic preserves — only stale
    // canonicals get re-synthesized. Without this branch the cache
    // would be useless.
    let fx = Fixture::start().await;
    let run_id = "stable-run";
    let node_id = "c".repeat(64);

    let boot = boot_payload(run_id, &node_id, "stage", 0);
    let _ = post_json(&fx, "/diag/boot", run_id, &node_id, 100, &boot).await;
    let _ = post_json(&fx, "/diag/finalize", run_id, &node_id, 200, &json!({})).await;

    let r1 = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    let r2 = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    assert_eq!(r1.status, 200);
    assert_eq!(r2.status, 200);
    // Byte-identical: serving the cached canonical, not re-synthesizing.
    assert_eq!(
        r1.body, r2.body,
        "consecutive GETs against an unchanged run must return byte-identical bundles",
    );
}

// ─── fixture + helpers (slimmed copy of t_diag_bundle_without_finalize) ─

fn boot_payload(run_id: &str, node_id: &str, role: &str, stage_index: u32) -> Value {
    json!({
        "node_id_hex": node_id,
        "node_id_short": &node_id[..8],
        "role": role,
        "stage_index": stage_index,
        "stage_count": 3,
        "run_id": run_id,
        "process_start_unix_ms": 1,
        "boot_sequence": 0,
    })
}

struct Fixture {
    addr: SocketAddr,
    _tmpdir: TempDir,
    _server: tokio::task::JoinHandle<()>,
}

impl Fixture {
    async fn start() -> Self {
        let tmpdir = TempDir::new();
        let root = tmpdir.path().to_path_buf();
        let state = Arc::new(
            CollectorState::new(&root).with_finalize_wait(Duration::from_millis(0)),
        );
        let listener = bind("127.0.0.1:0".parse().unwrap()).await.expect("bind");
        let addr = listener.local_addr().expect("local_addr");
        let handle = tokio::spawn(async move {
            let _ = serve(listener, state).await;
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        Fixture {
            addr,
            _tmpdir: tmpdir,
            _server: handle,
        }
    }
}

struct HttpResponse {
    status: u16,
    body: Vec<u8>,
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
        &body_bytes,
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
    let mut stream = TcpStream::connect(fx.addr).await.expect("connect");
    stream.write_all(request).await.expect("write");
    stream.flush().await.ok();
    let mut buf = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("response within 5s")
        .expect("read");
    parse_response(&buf)
}

fn parse_response(bytes: &[u8]) -> HttpResponse {
    let split = bytes
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("response has headers terminator");
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
        let mut path = std::env::temp_dir();
        path.push(format!("swactor-bundle-serve-hardening-{pid}-{nano:x}"));
        std::fs::create_dir_all(&path).unwrap();
        TempDir { path }
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
