use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::Router;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

use crate::history::DashboardHistory;
use crate::html::{ACTOR_DETAIL_HTML, ACTORS_HTML, DASHBOARD_HTML, TOPOLOGY_HTML};
use crate::layer::EventStore;
use crate::topology;
use crate::warnings::{WarningConfig, WarningDetector};

use crate::plugin::PluginRegistry;

/// Format a server-sent event.
fn format_sse(event: &str, data: &str) -> Event {
    Event::default().event(event).data(data)
}

// ── Shared application state ────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct AppState {
    pub store: Arc<EventStore>,
    /// Datastream-folded stats snapshot (latest wins).
    pub pushed_stats: Arc<Mutex<Option<swactor::stats::RuntimeStats>>>,
    pub shutdown: Arc<AtomicBool>,
    pub shutdown_notify: Arc<tokio::sync::Notify>,
    pub history: Arc<DashboardHistory>,
    pub plugins: Arc<PluginRegistry>,
    /// Optional HTML served at `/` instead of the actor dashboard.
    pub landing: Option<Arc<str>>,
}

// ── Router builders ─────────────────────────────────────────────────────

pub(crate) fn build_live_router(state: AppState) -> Router {
    let router = Router::new()
        .route("/", get(page_dashboard))
        .route("/actors", get(page_actors))
        .route("/topology", get(page_topology))
        .route("/events", get(handle_live_sse))
        .route("/api/stats", get(handle_stats_api))
        .route("/api/history", get(handle_history_api))
        .route("/api/topology", get(handle_topology_api))
        .route("/api/logs", get(handle_logs_api))
        .route("/actor/{hex}", get(handle_actor_detail))
        // Plugin routes. The bare form must be registered separately: a
        // `{*rest}` wildcard never matches an empty remainder, and plugins
        // answer their model snapshot on the bare path.
        .route(
            "/api/plugin/{name}",
            get(handle_plugin_get_bare).post(handle_plugin_post_bare),
        )
        .route(
            "/api/plugin/{name}/{*rest}",
            get(handle_plugin_get).post(handle_plugin_post),
        )
        .route("/plugin/{name}", get(handle_plugin_page));

    router.with_state(state)
}

// ── Server startup ──────────────────────────────────────────────────────

pub(crate) async fn run_server(state: AppState, port: u16, extra: Option<Router>) {
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind HTTP server");
    let shutdown = state.shutdown_notify.clone();
    let mut app = build_live_router(state);
    if let Some(extra) = extra {
        // Disjoint route sets (dashboard UI/API vs collector `/diag/*`) compose
        // cleanly onto one listener; both are `Router<()>` after `with_state`.
        app = app.merge(extra);
    }
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { shutdown.notified().await })
        .await
        .expect("HTTP server error");
}

// ── HTML page handlers ──────────────────────────────────────────────────

fn html_response(template: &str, mode: &str) -> Response {
    let html = template.replace("__DASHBOARD_MODE__", mode);
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response()
}

async fn page_dashboard(State(state): State<AppState>) -> Response {
    if let Some(html) = &state.landing {
        return (
            [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
            html.to_string(),
        )
            .into_response();
    }
    html_response(DASHBOARD_HTML, "live")
}

async fn page_actors() -> Response {
    html_response(ACTORS_HTML, "live")
}

async fn page_topology() -> Response {
    html_response(TOPOLOGY_HTML, "live")
}

async fn handle_actor_detail(Path(hex_addr): Path<String>) -> Response {
    let html = ACTOR_DETAIL_HTML
        .replace("__DASHBOARD_MODE__", "live")
        .replace("__ACTOR_ADDR__", &hex_addr);
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response()
}

// ── Live SSE handler ────────────────────────────────────────────────────

async fn handle_live_sse(
    State(state): State<AppState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(32);

    tokio::spawn(async move {
        let mut cursor: u64 = 0;
        let mut warning_detector = WarningDetector::new(WarningConfig::default());
        let mut tick_count: u64 = 0;

        // Send initial history snapshot so sparklines render immediately
        if state.history.sample_count() > 0 {
            let json = state.history.worker_history_json();
            if tx.send(format_sse("history", &json)).await.is_err() {
                return;
            }
        }

        loop {
            // Send the latest datastream-folded stats snapshot, if any.
            let maybe_stats = { state.pushed_stats.lock().unwrap().clone() };
            if let Some(stats) = maybe_stats {
                state.history.record(&stats);

                let warnings = warning_detector.check(&stats);
                if !warnings.is_empty()
                    && let Ok(wjson) = serde_json::to_string(&warnings)
                    && tx.send(format_sse("warnings", &wjson)).await.is_err()
                {
                    return;
                }

                let json = serde_json::to_string(&stats).unwrap();
                if tx.send(format_sse("stats", &json)).await.is_err() {
                    return;
                }

                tick_count += 1;
                if tick_count.is_multiple_of(5) {
                    let topo = topology::worker_topology(&stats);
                    if let Ok(tjson) = serde_json::to_string(&topo)
                        && tx.send(format_sse("topology", &tjson)).await.is_err()
                    {
                        return;
                    }
                }
            }

            // Poll all registered plugins
            for plugin in state.plugins.snapshot() {
                if let Some(json) = plugin.snapshot_json()
                    && tx.send(format_sse(plugin.name(), &json)).await.is_err()
                {
                    return;
                }
            }

            // Send new activity events
            let (batch, new_cursor) = state.store.read_from(cursor);
            if !batch.is_empty() {
                let json = serde_json::to_string(&batch).unwrap();
                if tx.send(format_sse("activity", &json)).await.is_err() {
                    return;
                }
                cursor = new_cursor;
            }

            if state.shutdown.load(Ordering::Relaxed) {
                let _ = tx.send(format_sse("done", "{}")).await;
                return;
            }

            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    Sse::new(ReceiverStream::new(rx).map(Ok)).keep_alive(KeepAlive::default())
}

// ── JSON API handlers ───────────────────────────────────────────────────

fn json_response(json: String) -> Response {
    ([(header::CONTENT_TYPE, "application/json")], json).into_response()
}

fn json_error(status: StatusCode, msg: &str) -> Response {
    let json = serde_json::json!({ "error": msg }).to_string();
    (status, [(header::CONTENT_TYPE, "application/json")], json).into_response()
}

async fn handle_stats_api(State(state): State<AppState>) -> Response {
    let json = state
        .pushed_stats
        .lock()
        .unwrap()
        .as_ref()
        .map(|stats| serde_json::to_string(stats).unwrap())
        .unwrap_or_else(|| "{}".to_string());
    json_response(json)
}

async fn handle_topology_api(State(state): State<AppState>) -> Response {
    let json = state
        .pushed_stats
        .lock()
        .unwrap()
        .as_ref()
        .map(|stats| {
            let topo = topology::worker_topology(stats);
            serde_json::to_string(&topo).unwrap_or_else(|_| "{}".into())
        })
        .unwrap_or_else(|| "{}".to_string());
    json_response(json)
}

async fn handle_logs_api(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let actor = params.get("actor").cloned().unwrap_or_default();
    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let level = params.get("level").cloned();

    let mut events = state.store.read_for_actor(&actor, limit);

    // Filter by level if specified
    if let Some(ref lvl) = level {
        let lvl_upper = lvl.to_uppercase();
        events.retain(|e| e.level == lvl_upper);
    }

    let json = serde_json::to_string(&events).unwrap_or_else(|_| "[]".into());
    json_response(json)
}

async fn handle_history_api(State(state): State<AppState>) -> Response {
    let json = state.history.worker_history_json();
    json_response(json)
}

// ── Plugin handlers ─────────────────────────────────────────────────────

/// Dispatch one plugin API request and convert its [`PluginResponse`] to HTTP.
/// `rest` is the path after `/api/plugin/{name}/` — empty for the bare
/// `/api/plugin/{name}` form.
fn dispatch_plugin(
    state: &AppState,
    method: &str,
    name: &str,
    rest: &str,
    params: &HashMap<String, String>,
    body: &[u8],
) -> Response {
    let plugins = state.plugins.snapshot();
    let plugin = match plugins.iter().find(|p| p.name() == name) {
        Some(p) => p,
        None => return json_error(StatusCode::NOT_FOUND, &format!("plugin '{name}' not found")),
    };
    match plugin.handle_request(method, rest, params, body) {
        crate::plugin::PluginResponse::Json(json) => json_response(json),
        crate::plugin::PluginResponse::Binary { content_type, data } => {
            ([(header::CONTENT_TYPE, content_type)], data).into_response()
        }
        crate::plugin::PluginResponse::Error { status, message } => {
            let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            json_error(code, &message)
        }
        crate::plugin::PluginResponse::NotFound => json_error(StatusCode::NOT_FOUND, "not found"),
    }
}

async fn handle_plugin_get(
    State(state): State<AppState>,
    Path((name, rest)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    dispatch_plugin(&state, "GET", &name, &rest, &params, &[])
}

async fn handle_plugin_post(
    State(state): State<AppState>,
    Path((name, rest)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    dispatch_plugin(&state, "POST", &name, &rest, &params, &body)
}

/// `/api/plugin/{name}` with no trailing path. The `{*rest}` route cannot
/// match an empty remainder, so without this route the bare form — what the
/// distribution/netmap pages fetch for their first paint — would 404.
async fn handle_plugin_get_bare(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    dispatch_plugin(&state, "GET", &name, "", &params, &[])
}

async fn handle_plugin_post_bare(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    dispatch_plugin(&state, "POST", &name, "", &params, &body)
}

async fn handle_plugin_page(State(state): State<AppState>, Path(name): Path<String>) -> Response {
    let plugins = state.plugins.snapshot();
    let plugin = match plugins.iter().find(|p| p.name() == name) {
        Some(p) => p,
        None => {
            return (StatusCode::NOT_FOUND, "plugin not found").into_response();
        }
    };
    match plugin.html_page() {
        Some(html) => {
            let rendered = html.replace("__DASHBOARD_MODE__", "live");
            (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                rendered,
            )
                .into_response()
        }
        None => (StatusCode::NOT_FOUND, "no page for this plugin").into_response(),
    }
}
