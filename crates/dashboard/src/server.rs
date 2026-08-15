use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::FrameEvent;
use crate::store::DashboardStore;
use crate::view::ViewRegistry;

#[derive(Clone)]
pub(crate) struct AppState {
    pub frames: broadcast::Sender<FrameEvent>,
    pub store: Arc<DashboardStore>,
    pub views: Arc<ViewRegistry>,
    pub shutdown_notify: Arc<tokio::sync::Notify>,
}

pub(crate) async fn run_server(state: AppState, port: u16) {
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind HTTP server");
    let shutdown = Arc::clone(&state.shutdown_notify);
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move { shutdown.notified().await })
        .await
        .expect("HTTP server error");
}

fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(root_page))
        .route("/events", get(frame_stream))
        .route("/api/frames", get(recent_frames))
        .route("/api/views", get(views_json))
        .route("/api/view/{*path}", get(view_snapshot))
        .route("/view/{*path}", get(view_page))
        .with_state(state)
}

async fn root_page(State(state): State<AppState>) -> Html<String> {
    let mut views = state.views.descriptors();
    views.sort_by(|left, right| left.title.cmp(right.title));
    let links = views
        .iter()
        .map(|view| {
            format!(
                "<li><a href=\"{}\">{}</a> <code>{}</code></li>",
                escape_html(&view.page),
                escape_html(view.title),
                escape_html(view.id)
            )
        })
        .collect::<Vec<_>>()
        .join("");
    Html(format!(
        r#"<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>swactor dashboard</title>
  <style>
    :root {{ color-scheme: dark; font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; background: #111827; color: #e5e7eb; }}
    body {{ margin: 0; padding: 32px; }}
    a {{ color: #93c5fd; }}
    code {{ color: #fbbf24; }}
    .card {{ max-width: 760px; background: #1f2937; border: 1px solid #374151; border-radius: 16px; padding: 24px; }}
    li {{ margin: 10px 0; }}
  </style>
</head>
<body>
  <main class="card">
    <h1>swactor dashboard</h1>
    <p>Read-only views over live telemetry frames.</p>
    <h2>Views</h2>
    <ul>{links}</ul>
    <h2>Raw APIs</h2>
    <ul>
      <li><a href="/api/views">Registered views JSON</a></li>
      <li><code>/events</code> streams raw incoming frames as SSE.</li>
      <li><code>/api/frames</code> returns the bounded recent raw frame window.</li>
    </ul>
  </main>
</body>
</html>"#
    ))
}

fn escape_html(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

async fn recent_frames(State(state): State<AppState>) -> Json<Vec<FrameEvent>> {
    Json(state.store.recent_frames())
}

async fn views_json(State(state): State<AppState>) -> Json<serde_json::Value> {
    Json(serde_json::json!({ "views": state.views.descriptors() }))
}

async fn view_snapshot(
    Path(path): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    match state.views.snapshot(&path) {
        Some(snapshot) => Json(snapshot).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown dashboard view").into_response(),
    }
}

async fn view_page(Path(path): Path<String>, State(state): State<AppState>) -> impl IntoResponse {
    match state.views.html(&path) {
        Some(html) => Html(html).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown dashboard view").into_response(),
    }
}

async fn frame_stream(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let mut rx = state.frames.subscribe();
    let (tx, out) = mpsc::channel::<Event>(32);

    tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(frame) => {
                    let Ok(json) = serde_json::to_string(&frame) else {
                        continue;
                    };
                    if tx
                        .send(Event::default().event("frame").data(json))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(skipped)) => {
                    let json = serde_json::json!({ "skipped": skipped }).to_string();
                    if tx
                        .send(Event::default().event("lagged").data(json))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    });

    Sse::new(ReceiverStream::new(out).map(Ok)).keep_alive(KeepAlive::default())
}
