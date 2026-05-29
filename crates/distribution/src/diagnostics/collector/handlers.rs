//! axum handlers for the collector's HTTP API.
//!
//! Endpoints:
//!
//! - `POST /diag/{boot,events,snapshot,finalize}` — record ingest.
//!   Request headers: `x-run-id`, `x-node-id`, `x-node-send-ms`.
//!   Request body: JSON record (shape per kind, treated opaquely).
//!   Response: [`PostAck`] with [`ClockEcho`] and optional [`Hints`].
//! - `GET /diag/bundle/{run_id}` — download the tarball produced by
//!   the most recent `/diag/finalize` for the given run.
//!
//! Handlers are intentionally thin: they validate, persist, attach any
//! pending hints from [`CollectorState::take_pending_hints`], and (for
//! finalize) drive the T1.7 snapshot-then-tar sequence.

use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;
use serde_json::Value;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

use super::protocol::{ClockEcho, Hints, PostAck, RecordKind};
use super::state::{CollectorState, MAX_BODY_BYTES, NodeStats, RunStats};
use super::{bundle, wall_ms_now};

/// Build the axum router. Wire this into `axum::serve` from the
/// binary, or into `tower::ServiceExt::oneshot` from tests.
/// The data-plane routes (ingest, stream, bundle, runs) shared by both the
/// standalone collector and the embeddable ingest router.
fn diag_routes() -> Router<Arc<CollectorState>> {
    Router::new()
        .route("/diag/{kind}", post(ingest))
        .route("/diag/bundle/{run_id}", get(download_bundle))
        .route("/diag/runs", get(list_runs))
        .route("/diag/stream/{run_id}", get(stream_run))
}

pub fn router(state: Arc<CollectorState>) -> Router {
    diag_routes()
        .route("/", get(dashboard_index))
        .route("/dashboard", get(dashboard_page))
        .route("/dashboard.js", get(dashboard_js))
        .with_state(state)
}

/// Data-plane only (ingest + stream + bundle + runs), without the collector's
/// built-in fleet UI. For embedding in another HTTP server (e.g. the dashboard)
/// that brings its own landing page — merging the full [`router`] would collide
/// on `GET /`, `/dashboard`, `/dashboard.js`.
pub fn ingest_router(state: Arc<CollectorState>) -> Router {
    diag_routes().with_state(state)
}

/// The live fleet dashboard, served same-origin with `/diag/stream` so the
/// page's `EventSource` needs no CORS. Bundled into the binary so the
/// collector is a single self-contained artifact.
const DASHBOARD_HTML: &str = include_str!("assets/fleet_live.html");
const DASHBOARD_JS: &str = include_str!("assets/fleet_model.js");

/// `GET /` — bounce to the dashboard, pinned to the newest run we've seen so
/// the page connects to the live stream immediately. With no runs yet, land on
/// `/dashboard` bare; the page polls `/diag/runs` until one appears.
async fn dashboard_index(State(state): State<Arc<CollectorState>>) -> Redirect {
    match newest_run(&state) {
        Some(run_id) => Redirect::to(&format!("/dashboard?run={run_id}")),
        None => Redirect::to("/dashboard"),
    }
}

async fn dashboard_page() -> Response {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], DASHBOARD_HTML).into_response()
}

async fn dashboard_js() -> Response {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        DASHBOARD_JS,
    )
        .into_response()
}

/// Newest run id by earliest-record timestamp — `run_summaries()` sorts by id,
/// which isn't chronological, so pick the max `run_start_collector_ms`.
fn newest_run(state: &CollectorState) -> Option<String> {
    state
        .run_summaries()
        .into_iter()
        .max_by_key(|(_, stats)| stats.run_start_collector_ms.unwrap_or(0))
        .map(|(run_id, _)| run_id)
}

/// SSE event for an in-memory record. The `event` field carries the
/// `RecordKind` so clients can `addEventListener("snapshot", …)`.
fn format_sse(event: &str, data: &str) -> Event {
    Event::default().event(event).data(data)
}

/// Per-node accounting shape returned from `/diag/runs`. Mirrors
/// `NodeStats` minus the cached `identity` blob (potentially large;
/// belongs in the bundle, not a directory listing).
#[derive(Serialize)]
struct NodeSummary {
    node_id: String,
    boot_recorded: bool,
    event_batches: u64,
    snapshots: u64,
    finalize_recorded: bool,
}

/// Per-run accounting shape returned from `/diag/runs`.
#[derive(Serialize)]
struct RunSummary {
    run_id: String,
    run_start_collector_ms: Option<u64>,
    run_end_collector_ms: Option<u64>,
    finalize_received: bool,
    nodes: Vec<NodeSummary>,
}

impl RunSummary {
    fn from_stats(run_id: String, stats: RunStats) -> Self {
        let mut nodes: Vec<NodeSummary> = stats
            .nodes
            .into_iter()
            .map(|(node_id, n): (String, NodeStats)| NodeSummary {
                node_id,
                boot_recorded: n.boot_recorded,
                event_batches: n.event_batches,
                snapshots: n.snapshots,
                finalize_recorded: n.finalize_recorded,
            })
            .collect();
        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        RunSummary {
            run_id,
            run_start_collector_ms: stats.run_start_collector_ms,
            run_end_collector_ms: stats.run_end_collector_ms,
            finalize_received: stats.finalize_received,
            nodes,
        }
    }
}

async fn list_runs(State(state): State<Arc<CollectorState>>) -> Json<Vec<RunSummary>> {
    let summaries = state
        .run_summaries()
        .into_iter()
        .map(|(run_id, stats)| RunSummary::from_stats(run_id, stats))
        .collect();
    Json(summaries)
}

/// Subscribe to live persisted records for a single run as
/// Server-Sent Events. Each event's name is the record `kind`
/// (`boot`, `events`, `snapshot`, `finalize`); the data payload is
/// the JSON-serialized `LiveRecord`.
///
/// Unknown `run_id`s are accepted — the connection stays open and
/// the client will see records once they arrive. Slow subscribers
/// that fall behind the broadcast capacity silently skip the gap
/// (`/diag/bundle/{run_id}` is the catch-up path).
async fn stream_run(
    Path(run_id): Path<String>,
    State(state): State<Arc<CollectorState>>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let rx = state.subscribe();
    let stream = BroadcastStream::new(rx).filter_map(move |item| match item {
        Ok(rec) if rec.run_id == run_id => {
            let json = serde_json::to_string(&*rec).ok()?;
            Some(Ok(format_sse(rec.kind.as_str(), &json)))
        }
        Ok(_) => None,
        Err(BroadcastStreamRecvError::Lagged(_)) => None,
    });
    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("ping"),
    )
}

async fn ingest(
    State(state): State<Arc<CollectorState>>,
    Path(kind): Path<String>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let recv_ms = wall_ms_now();
    let kind = match RecordKind::parse(&kind) {
        Some(k) => k,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                format!("unknown record kind: {kind:?}"),
            );
        }
    };
    if body.len() > MAX_BODY_BYTES {
        return error_response(
            StatusCode::PAYLOAD_TOO_LARGE,
            format!(
                "body of {} bytes exceeds collector limit of {MAX_BODY_BYTES}",
                body.len()
            ),
        );
    }
    let run_id = match header_str(&headers, "x-run-id") {
        Some(v) if !v.is_empty() => v,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "missing or empty x-run-id header".to_string(),
            );
        }
    };
    let node_id = match header_str(&headers, "x-node-id") {
        Some(v) if !v.is_empty() => v,
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                "missing or empty x-node-id header".to_string(),
            );
        }
    };
    let node_send_ms = header_u64(&headers, "x-node-send-ms").unwrap_or(0);
    let body_json: Value = if body.is_empty() {
        Value::Null
    } else {
        match serde_json::from_slice(&body) {
            Ok(v) => v,
            Err(e) => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    format!("body is not valid JSON: {e}"),
                );
            }
        }
    };
    if let Err(e) = state.persist(&run_id, &node_id, kind, recv_ms, &body_json) {
        return error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not persist record: {e}"),
        );
    }
    let payload = match kind {
        RecordKind::Finalize => {
            // T1.7: queue a snapshot_now hint for every node that has
            // touched this run, give them a chance to post their final
            // snapshot, then tar the bundle.
            state.mark_run_for_snapshot_now(&run_id);
            let wait = state.finalize_wait();
            if !wait.is_zero() {
                tokio::time::sleep(wait).await;
            }
            match bundle::assemble(&state, &run_id) {
                Ok(path) => Some(serde_json::json!({
                    "bundle_path": path.to_string_lossy(),
                })),
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("could not assemble bundle: {e}"),
                    );
                }
            }
        }
        _ => None,
    };
    let hints = state.take_pending_hints(&run_id, &node_id);
    finish_response(node_send_ms, recv_ms, payload, hints)
}

async fn download_bundle(
    State(state): State<Arc<CollectorState>>,
    Path(run_id): Path<String>,
) -> Response {
    // Spec §7 (gap 7) — `GET /diag/bundle/<run>` succeeds whether or
    // not a finalize record was received:
    //   1. canonical tarball exists on disk (finalize landed cleanly)
    //      → serve it; cheap, no synthesis.
    //   2. canonical tarball missing but staging files present
    //      → synthesize on-demand from staging; the manifest carries
    //      `finalize_received: false` so the bundle reader is never
    //      left guessing. Per spec: "the latency is fine because
    //      unfinalized bundles are by definition retrieved during
    //      incident response."
    //   3. neither tarball nor staging → 404 (truly unknown run).
    //
    // Coverage 2.5 — bundle serve hardening under run-id reuse: the
    // canonical bytes on disk may be stale if staging has grown past
    // the canonical's snapshot (the `1779733878` shape: phase-1
    // finalize lands; phase-2 boot adds a new node to staging; a
    // later GET should return the *richer* bundle, not the cached
    // canonical). We use the node-count heuristic the spec names:
    // compare current in-memory `nodes.len()` against the count
    // recorded when the canonical was written. If staging is bigger,
    // skip the cache and re-synthesize.
    let path = state.bundle_path(&run_id);
    let canonical = tokio::fs::read(&path).await;
    match canonical {
        Ok(bytes) => {
            let current_count = state
                .run_stats(&run_id)
                .map(|s| s.nodes.len())
                .unwrap_or(0);
            let canonical_count = state.canonical_node_count(&run_id).unwrap_or(current_count);
            if current_count > canonical_count {
                // Stale: fall through to synthesis so the richer
                // surface lands in the response.
            } else {
                return ok_response(&run_id, bytes);
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            // Fall through to on-demand synthesis.
        }
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("could not read bundle: {e}"),
            );
        }
    }

    let run_id_for_blocking = run_id.clone();
    let state_for_blocking = Arc::clone(&state);
    let synth = tokio::task::spawn_blocking(move || {
        bundle::assemble_bytes(&state_for_blocking, &run_id_for_blocking)
    })
    .await;
    match synth {
        Ok(Ok(bytes)) => ok_response(&run_id, bytes),
        Ok(Err(e)) if e.kind() == std::io::ErrorKind::NotFound => error_response(
            StatusCode::NOT_FOUND,
            format!("no records on disk for run_id {run_id}"),
        ),
        Ok(Err(e)) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("could not synthesize bundle: {e}"),
        ),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("synthesis task failed: {e}"),
        ),
    }
}

fn ok_response(run_id: &str, bytes: Vec<u8>) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/gzip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{run_id}.tar.gz\""),
        )
        .body(Body::from(bytes))
        .unwrap()
}

fn finish_response(
    node_send_ms: u64,
    recv_ms: u64,
    body: Option<Value>,
    hints: Option<Hints>,
) -> Response {
    let send_ms = wall_ms_now();
    let ack = PostAck::<Value> {
        clock: ClockEcho {
            node_send_ms_echoed: node_send_ms,
            collector_recv_ms: recv_ms,
            collector_send_ms: send_ms,
        },
        hints: hints.filter(|h| !h.is_empty()),
        body,
    };
    (StatusCode::OK, Json(ack)).into_response()
}

fn error_response(status: StatusCode, message: String) -> Response {
    (
        status,
        Json(serde_json::json!({ "error": message })),
    )
        .into_response()
}

fn header_str<'a>(headers: &'a HeaderMap, name: &str) -> Option<String> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
}

fn header_u64(headers: &HeaderMap, name: &str) -> Option<u64> {
    header_str(headers, name).and_then(|s| s.parse().ok())
}
