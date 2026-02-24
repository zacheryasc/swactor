use std::collections::HashMap;
use std::convert::Infallible;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use swactor::runtime::Runtime;

use crate::actor_detail_html::ACTOR_DETAIL_HTML;
use crate::actors_html::ACTORS_HTML;
use crate::collector::StatsCollector;
use crate::command::CommandRouter;
use crate::dashboard_html::DASHBOARD_HTML;
use crate::history::DashboardHistory;
use crate::layer::EventStore;
use crate::topology;
use crate::topology_html::TOPOLOGY_HTML;
use crate::warnings::{WarningConfig, WarningDetector};

use crate::plugin::PluginRegistry;

use crate::trace::RuntimeTrace;

/// Format a server-sent event.
fn format_sse(event: &str, data: &str) -> Event {
    Event::default().event(event).data(data)
}

// ── Shared application state ────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct AppState {
    pub store: Arc<EventStore>,
    pub runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    pub collector: Arc<Mutex<Option<Arc<StatsCollector>>>>,
    pub shutdown: Arc<AtomicBool>,
    pub shutdown_notify: Arc<tokio::sync::Notify>,
    pub history: Arc<DashboardHistory>,
    pub cmd_router: Arc<CommandRouter>,
    pub plugins: Arc<PluginRegistry>,
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
        .route("/api/investigate", get(handle_investigate_api))
        .route("/api/logs", get(handle_logs_api))
        .route("/actor/{hex}", get(handle_actor_detail))
        // Plugin routes
        .route("/api/plugin/{name}/{*rest}", get(handle_plugin_get).post(handle_plugin_post))
        .route("/plugin/{name}", get(handle_plugin_page));

    router.with_state(state)
}

pub(crate) fn build_replay_router(state: ReplayState) -> Router {
    Router::new()
        .route("/", get(replay_page_dashboard))
        .route("/actors", get(replay_page_actors))
        .route("/events", get(handle_replay_sse))
        .with_state(state)
}

// ── Server startup ──────────────────────────────────────────────────────

pub(crate) async fn run_server(state: AppState, port: u16) {
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind HTTP server");
    let shutdown = state.shutdown_notify.clone();
    axum::serve(listener, build_live_router(state))
        .with_graceful_shutdown(async move { shutdown.notified().await })
        .await
        .expect("HTTP server error");
}

pub(crate) async fn run_replay_server(state: ReplayState, port: u16) {
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}"))
        .await
        .expect("failed to bind HTTP server");
    axum::serve(listener, build_replay_router(state))
        .await
        .expect("HTTP server error");
}

// ── HTML page handlers ──────────────────────────────────────────────────

fn html_response(template: &str, mode: &str) -> Response {
    let html = template.replace("__DASHBOARD_MODE__", mode);
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response()
}

async fn page_dashboard() -> Response {
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
            // Send stats if runtime is available
            {
                let maybe_rt = state.runtime.lock().unwrap().clone();
                if let Some(rt) = maybe_rt {
                    let mut stats = rt.stats();
                    if let Some(col) = state.collector.lock().unwrap().as_ref() {
                        col.enrich(&mut stats);
                    }
                    crate::collector::enrich_names(&mut stats, &rt);
                    state.history.record(&stats);

                    // Run warning detection
                    let warnings = warning_detector.check(&stats);
                    if !warnings.is_empty()
                        && let Ok(wjson) = serde_json::to_string(&warnings)
                            && tx.send(format_sse("warnings", &wjson)).await.is_err() {
                                return;
                            }

                    let json = serde_json::to_string(&stats).unwrap();
                    if tx.send(format_sse("stats", &json)).await.is_err() {
                        return;
                    }

                    // Send topology every 5th tick (~1/sec)
                    tick_count += 1;
                    if tick_count.is_multiple_of(5) {
                        let topo = topology::worker_topology(&stats);
                        if let Ok(tjson) = serde_json::to_string(&topo)
                            && tx.send(format_sse("topology", &tjson)).await.is_err() {
                                return;
                            }
                    }
                }
            }

            // Poll all registered plugins
            for plugin in state.plugins.snapshot() {
                if let Some(json) = plugin.snapshot_json()
                    && tx.send(format_sse(plugin.name(), &json)).await.is_err() {
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

    Sse::new(ReceiverStream::new(rx).map(Ok))
        .keep_alive(KeepAlive::default())
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
    let maybe_rt = state.runtime.lock().unwrap().clone();
    let json = match maybe_rt {
        Some(rt) => {
            let mut stats = rt.stats();
            if let Some(col) = state.collector.lock().unwrap().as_ref() {
                col.enrich(&mut stats);
            }
            crate::collector::enrich_names(&mut stats, &rt);
            serde_json::to_string(&stats).unwrap()
        }
        None => "{}".to_string(),
    };
    json_response(json)
}

async fn handle_investigate_api(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let maybe_rt = state.runtime.lock().unwrap().clone();
    let maybe_col = state.collector.lock().unwrap().clone();

    let json = match (maybe_rt, maybe_col) {
        (Some(rt), Some(col)) => {
            let ctx = crate::command::CommandContext::with_enricher(rt, col);
            let req = crate::command::from_query_params(&params);
            state.cmd_router.dispatch(&req, &ctx).to_json_line()
        }
        (Some(rt), None) => {
            let ctx = crate::command::CommandContext::new(rt);
            let req = crate::command::from_query_params(&params);
            state.cmd_router.dispatch(&req, &ctx).to_json_line()
        }
        _ => {
            let cmd = params.get("cmd").map(|s| s.as_str()).unwrap_or("help");
            serde_json::json!({
                "ok": false,
                "command": cmd,
                "error": "runtime not attached yet"
            })
            .to_string()
        }
    };

    json_response(json)
}

async fn handle_topology_api(State(state): State<AppState>) -> Response {
    let maybe_rt = state.runtime.lock().unwrap().clone();
    let json = match maybe_rt {
        Some(rt) => {
            let mut stats = rt.stats();
            if let Some(col) = state.collector.lock().unwrap().as_ref() {
                col.enrich(&mut stats);
            }
            let topo = topology::worker_topology(&stats);
            serde_json::to_string(&topo).unwrap_or_else(|_| "{}".into())
        }
        None => "{}".to_string(),
    };
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

async fn handle_plugin_get(
    State(state): State<AppState>,
    Path((name, rest)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let plugins = state.plugins.snapshot();
    let plugin = match plugins.iter().find(|p| p.name() == name) {
        Some(p) => p,
        None => return json_error(StatusCode::NOT_FOUND, &format!("plugin '{name}' not found")),
    };
    match plugin.handle_request("GET", &rest, &params, &[]) {
        crate::plugin::PluginResponse::Json(json) => json_response(json),
        crate::plugin::PluginResponse::Binary { content_type, data } => {
            ([(header::CONTENT_TYPE, content_type)], data).into_response()
        }
        crate::plugin::PluginResponse::Error { status, message } => {
            let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            json_error(code, &message)
        }
        crate::plugin::PluginResponse::NotFound => {
            json_error(StatusCode::NOT_FOUND, "not found")
        }
    }
}

async fn handle_plugin_post(
    State(state): State<AppState>,
    Path((name, rest)): Path<(String, String)>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let plugins = state.plugins.snapshot();
    let plugin = match plugins.iter().find(|p| p.name() == name) {
        Some(p) => p,
        None => return json_error(StatusCode::NOT_FOUND, &format!("plugin '{name}' not found")),
    };
    match plugin.handle_request("POST", &rest, &params, &body) {
        crate::plugin::PluginResponse::Json(json) => json_response(json),
        crate::plugin::PluginResponse::Binary { content_type, data } => {
            ([(header::CONTENT_TYPE, content_type)], data).into_response()
        }
        crate::plugin::PluginResponse::Error { status, message } => {
            let code = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            json_error(code, &message)
        }
        crate::plugin::PluginResponse::NotFound => {
            json_error(StatusCode::NOT_FOUND, "not found")
        }
    }
}

async fn handle_plugin_page(
    State(state): State<AppState>,
    Path(name): Path<String>,
) -> Response {
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
            ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], rendered).into_response()
        }
        None => (StatusCode::NOT_FOUND, "no page for this plugin").into_response(),
    }
}

// ── Replay server ───────────────────────────────────────────────────────

#[derive(Clone)]
pub(crate) struct ReplayState {
    pub trace: Arc<RuntimeTrace>,
    pub speed: f64,
}

async fn replay_page_dashboard() -> Response {
    html_response(DASHBOARD_HTML, "replay")
}

async fn replay_page_actors() -> Response {
    html_response(ACTORS_HTML, "replay")
}

async fn handle_replay_sse(
    State(state): State<ReplayState>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    let (tx, rx) = tokio::sync::mpsc::channel::<Event>(32);

    tokio::spawn(async move {
        let trace = &state.trace;
        let speed = state.speed;

        // Send replay metadata
        let meta = serde_json::json!({
            "total_events": trace.events.len(),
            "total_stats": trace.stats_timeline.len(),
            "speed": speed,
        });
        if tx.send(format_sse("replay_meta", &meta.to_string())).await.is_err() {
            return;
        }

        // Find the earliest timestamp across events and stats
        let base_time = trace
            .events
            .first()
            .map(|e| e.timestamp_ms)
            .into_iter()
            .chain(trace.stats_timeline.first().map(|s| s.timestamp_ms))
            .min()
            .unwrap_or(0);

        let playback_start = tokio::time::Instant::now();
        let mut event_idx = 0;
        let mut stats_idx = 0;

        loop {
            let elapsed_ms = (playback_start.elapsed().as_millis() as f64 * speed) as u64;
            let virtual_time = base_time + elapsed_ms;

            // Batch events up to virtual_time
            let mut batch = Vec::new();
            while event_idx < trace.events.len()
                && trace.events[event_idx].timestamp_ms <= virtual_time
            {
                batch.push(trace.events[event_idx].clone());
                event_idx += 1;
            }
            if !batch.is_empty() {
                let json = serde_json::to_string(&batch).unwrap();
                if tx.send(format_sse("activity", &json)).await.is_err() {
                    return;
                }
            }

            // Send stats snapshots up to virtual_time
            while stats_idx < trace.stats_timeline.len()
                && trace.stats_timeline[stats_idx].timestamp_ms <= virtual_time
            {
                let json =
                    serde_json::to_string(&trace.stats_timeline[stats_idx].stats).unwrap();
                if tx.send(format_sse("stats", &json)).await.is_err() {
                    return;
                }
                stats_idx += 1;
            }

            // Send progress
            let total = trace.events.len() + trace.stats_timeline.len();
            let done_count = event_idx + stats_idx;
            let progress = if total > 0 {
                done_count as f64 / total as f64
            } else {
                1.0
            };
            let progress_json = serde_json::json!({ "progress": progress });
            if tx
                .send(format_sse("replay_progress", &progress_json.to_string()))
                .await
                .is_err()
            {
                return;
            }

            // Check if replay is complete
            if event_idx >= trace.events.len()
                && stats_idx >= trace.stats_timeline.len()
            {
                let _ = tx.send(format_sse("done", "{}")).await;
                return;
            }

            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    });

    Sse::new(ReceiverStream::new(rx).map(Ok))
        .keep_alive(KeepAlive::default())
}
