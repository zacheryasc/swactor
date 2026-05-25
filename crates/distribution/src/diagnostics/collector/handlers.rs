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

use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::Value;

use super::protocol::{ClockEcho, Hints, PostAck, RecordKind};
use super::state::{CollectorState, MAX_BODY_BYTES};
use super::{bundle, wall_ms_now};

/// Build the axum router. Wire this into `axum::serve` from the
/// binary, or into `tower::ServiceExt::oneshot` from tests.
pub fn router(state: Arc<CollectorState>) -> Router {
    Router::new()
        .route("/diag/{kind}", post(ingest))
        .route("/diag/bundle/{run_id}", get(download_bundle))
        .with_state(state)
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
    let path = state.bundle_path(&run_id);
    let canonical = tokio::fs::read(&path).await;
    match canonical {
        Ok(bytes) => return ok_response(&run_id, bytes),
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
