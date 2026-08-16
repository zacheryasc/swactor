use std::convert::Infallible;
use std::sync::Arc;

use axum::extract::{Path, RawQuery, State};
use axum::http::StatusCode;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
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
    let router = Router::new()
        .route("/", get(root_page))
        .route("/events", get(frame_stream))
        .route("/api/frames", get(recent_frames))
        .route("/api/views", get(views_json))
        .route("/api/view/{*path}", get(view_snapshot))
        .route("/view/{*path}", get(view_page));
    #[cfg(feature = "demo-control")]
    let router = router
        .route("/control/kill", axum::routing::post(control_kill))
        .route("/control/provision", axum::routing::post(control_provision))
        .route("/control/remove", axum::routing::post(control_remove))
        .route("/control/edge", axum::routing::post(control_edge));
    router.with_state(state)
}

#[cfg(feature = "demo-control")]
async fn control_kill(
    Json(command): Json<crate::control::ControlCommand>,
) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::Kill { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

#[cfg(feature = "demo-control")]
async fn control_provision(
    Json(command): Json<crate::control::ControlCommand>,
) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::Provision { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

#[cfg(feature = "demo-control")]
async fn control_remove(
    Json(command): Json<crate::control::ControlCommand>,
) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::Remove { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

#[cfg(feature = "demo-control")]
async fn control_edge(
    Json(command): Json<crate::control::ControlCommand>,
) -> impl IntoResponse {
    match command {
        crate::control::ControlCommand::EstablishEdge { .. } => {
            if crate::control::dispatch(command) {
                StatusCode::ACCEPTED
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            }
        }
        _ => StatusCode::UNPROCESSABLE_ENTITY,
    }
}

async fn root_page(State(state): State<AppState>) -> Response {
    // Home is the fleet control-plane view.
    match state.views.html("fleet") {
        Some(html) => Html(inject_nav(html, "fleet", &state)).into_response(),
        None => (
            StatusCode::INTERNAL_SERVER_ERROR,
            "fleet control-plane view missing",
        )
            .into_response(),
    }
}

/// Placeholder marker pages include to receive the shared navbar.
const NAV_PLACEHOLDER: &str = "<!--swactor:nav-->";

/// Build the unified top navbar from the view registry (registration order,
/// active by served path). Fleet links to `/` — it is the home page.
fn inject_nav(html: &str, active_path: &str, state: &AppState) -> String {
    if !html.contains(NAV_PLACEHOLDER) {
        return html.to_owned();
    }
    let mut links = Vec::new();
    for view in state.views.descriptors() {
        if !view.show_in_nav {
            continue;
        }
        let href = if view.path == "fleet" {
            "/".to_owned()
        } else {
            view.page.clone()
        };
        let active = view.path == active_path;
        links.push(format!(
            r#"<a href="{}"{}>{}</a>"#,
            escape_html(&href),
            if active { r#" aria-current="page" data-active="true""# } else { "" },
            escape_html(view.title),
        ));
    }
    let nav = format!(
        concat!(
            r#"<script>try{{var t=localStorage.getItem('swactor-theme');document.documentElement.dataset.theme=t||(matchMedia('(prefers-color-scheme: light)').matches?'light':'dark')}}catch(e){{}}</script>"#,
            r#"<style>"#,
            r#".sw-nav{{display:flex;flex-wrap:wrap;align-items:center;gap:6px;margin:0 0 16px;padding:8px;"#,
            r#"background:#001220;border:1px solid #14406a;border-radius:2px;}}"#,
            r#".sw-nav a{{padding:6px 10px;color:#c7d3de;border:1px solid transparent;border-bottom:2px solid transparent;"#,
            r#"border-radius:2px;text-decoration:none;font:600 13px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;"#,
            r#"transition:color .12s,background .12s,border-color .12s;}}"#,
            r#".sw-nav a:hover{{color:#ffffff;background:#001c38;}}"#,
            r#".sw-nav a:focus-visible{{outline:2px solid #5cd5ff;outline-offset:2px;}}"#,
            r#".sw-nav a[data-active="true"]{{color:#ff9900;border-bottom-color:#ff9900;}}"#,
            r#".sw-nav .sw-theme{{margin-left:auto;cursor:pointer;background:transparent;border:1px solid #14406a;"#,
            r#"color:#c7d3de;border-radius:2px;font:600 13px ui-monospace,SFMono-Regular,Menlo,Consolas,monospace;padding:6px 10px;}}"#,
            r#".sw-nav .sw-theme:hover{{color:#ffffff;background:#001c38;border-color:#5cd5ff;}}"#,
            r#":root[data-theme="light"] .sw-nav{{background:#ffffff;border-color:#dcdce0;}}"#,
            r#":root[data-theme="light"] .sw-nav a{{color:#696c77;}}"#,
            r#":root[data-theme="light"] .sw-nav a:hover{{color:#383a42;background:#f0f0f0;}}"#,
            r#":root[data-theme="light"] .sw-nav a[data-active="true"]{{color:#986801;border-bottom-color:#986801;}}"#,
            r#":root[data-theme="light"] .sw-nav .sw-theme{{border-color:#dcdce0;color:#696c77;}}"#,
            r#":root[data-theme="light"] .sw-nav .sw-theme:hover{{color:#383a42;background:#f0f0f0;border-color:#0184bc;}}"#,
            r#"</style>"#,
            r#"<nav class="sw-nav" aria-label="Dashboard views">{}"#,
            r#"<button class="sw-theme" aria-label="Toggle light or dark theme" title="Toggle light or dark theme""#,
            r#" onclick="var d=document.documentElement;d.dataset.theme=d.dataset.theme==='light'?'dark':'light';try{{localStorage.setItem('swactor-theme',d.dataset.theme)}}catch(e){{}}">◐</button></nav>"#,
        ),
        links.join("")
    );
    html.replace(NAV_PLACEHOLDER, &nav)
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
    RawQuery(query): RawQuery,
    State(state): State<AppState>,
) -> Response {
    // `/api/view/<path>/detail?<query>` serves bounded per-entity detail.
    if let Some(base) = path.strip_suffix("/detail") {
        let query = query.unwrap_or_default();
        return match state.views.detail(base, &query) {
            Some(snapshot) => Json(snapshot).into_response(),
            None => (StatusCode::NOT_FOUND, "unknown dashboard detail").into_response(),
        };
    }
    match state.views.snapshot(&path) {
        Some(snapshot) => Json(snapshot).into_response(),
        None => (StatusCode::NOT_FOUND, "unknown dashboard view").into_response(),
    }
}

async fn view_page(Path(path): Path<String>, State(state): State<AppState>) -> Response {
    match state.views.html(&path) {
        Some(html) => Html(inject_nav(html, &path, &state)).into_response(),
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
