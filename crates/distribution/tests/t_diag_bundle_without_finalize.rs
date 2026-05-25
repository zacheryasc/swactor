//! Spec §7 (bundle without finalize, gap 7).
//!
//! Acceptance: "kill an orchestrator with SIGKILL mid-run. A
//! subsequent `GET /diag/bundle/<run_id>` returns a usable bundle
//! with `finalize_received: false` in its manifest."
//!
//! We simulate the SIGKILL by simply *not* posting a finalize
//! record — the on-wire effect is identical from the collector's
//! point of view. The collector must:
//!   - Synthesize a bundle on demand from staging files.
//!   - Set `finalize_received: false` in the manifest.
//!   - Return a tarball with the per-node records that landed before
//!     the kill.

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
async fn sigkill_mid_run_still_yields_a_retrievable_bundle_with_finalize_false() {
    let fx = Fixture::start().await;
    let run_id = "sim-sigkill-run";
    let node_id = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    // Boot + one events batch land before the "SIGKILL".
    let boot_body = json!({
        "node_id_hex": node_id,
        "node_id_short": &node_id[..8],
        "role": "stage",
        "stage_index": 2,
        "stage_count": 3,
        "run_id": run_id,
        "process_start_unix_ms": 1,
        "boot_sequence": 0,
    });
    let boot = post_json(&fx, "/diag/boot", run_id, node_id, 100, &boot_body).await;
    assert_eq!(boot.status, 200);

    let events_body = json!([]);
    let events = post_json(&fx, "/diag/events", run_id, node_id, 200, &events_body).await;
    assert_eq!(events.status, 200);

    // No /diag/finalize POST — this models the orchestrator being
    // killed before it could send finalize.

    let resp = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    assert_eq!(
        resp.status, 200,
        "bundle GET must succeed even without finalize; body={:?}",
        String::from_utf8_lossy(&resp.body),
    );
    assert!(resp.body.starts_with(&[0x1f, 0x8b]), "body must be gzipped");

    // Parse the synthesized bundle and verify the manifest's finalize
    // discriminator.
    let manifest_bytes = read_tar_file(&resp.body, &format!("{run_id}/MANIFEST.json"));
    let manifest: Manifest = serde_json::from_slice(&manifest_bytes).expect("manifest parses");
    assert_eq!(manifest.run_id, run_id);
    assert!(
        !manifest.finalize_received,
        "synthesized bundle's manifest must carry finalize_received: false",
    );
    assert!(
        !manifest.nodes.is_empty(),
        "manifest must list the node that posted boot before the kill; got: {:#?}",
        manifest.nodes,
    );
    let node_entry = manifest
        .nodes
        .iter()
        .find(|n| n.node_id_hex == node_id)
        .expect("the stage-2 node must appear in the synthesized manifest");
    assert!(node_entry.boot_recorded, "boot must be reflected in manifest");
    assert!(
        !node_entry.finalize_recorded,
        "node-level finalize_recorded must also be false",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn truly_unknown_run_id_still_returns_404() {
    let fx = Fixture::start().await;
    let resp = get(&fx, "/diag/bundle/no-such-run").await;
    assert_eq!(
        resp.status, 404,
        "bundle GET on an unknown run id must 404 (no staging dir, no tarball)",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn synthesized_bundle_can_be_retrieved_more_than_once() {
    // The on-demand synthesis path should be idempotent — operators
    // re-running the GET after an incident should not see different
    // results unless new records have arrived. The cheapest contract
    // to check: two consecutive GETs return identical manifests.
    let fx = Fixture::start().await;
    let run_id = "repeat-get-run";
    let node_id = "abc".repeat(21) + "a";
    let boot_body = json!({
        "node_id_hex": node_id,
        "node_id_short": &node_id[..8],
        "role": "stage",
        "stage_index": 0,
        "stage_count": 1,
        "run_id": run_id,
        "process_start_unix_ms": 1,
        "boot_sequence": 0,
    });
    let _ = post_json(&fx, "/diag/boot", run_id, &node_id, 100, &boot_body).await;
    let r1 = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    let r2 = get(&fx, &format!("/diag/bundle/{run_id}")).await;
    assert_eq!(r1.status, 200);
    assert_eq!(r2.status, 200);
    let m1: Manifest = serde_json::from_slice(&read_tar_file(
        &r1.body,
        &format!("{run_id}/MANIFEST.json"),
    ))
    .unwrap();
    let m2: Manifest = serde_json::from_slice(&read_tar_file(
        &r2.body,
        &format!("{run_id}/MANIFEST.json"),
    ))
    .unwrap();
    assert_eq!(m1.run_id, m2.run_id);
    assert_eq!(m1.finalize_received, m2.finalize_received);
    assert_eq!(m1.nodes.len(), m2.nodes.len());
}

// ─── fixture + helpers (slimmed copy of t_diag_collector pattern) ─────

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
        path.push(format!("swactor-bundle-sigkill-{pid}-{nano:x}"));
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
