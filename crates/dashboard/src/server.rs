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
use axum::routing::{get, post};
use axum::Router;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

use swactor::runtime::Runtime;

use crate::actor_detail_html::ACTOR_DETAIL_HTML;
use crate::actors_html::ACTORS_HTML;
use crate::collector::StatsCollector;
use crate::command::CommandRouter;
use crate::dashboard_html::DASHBOARD_HTML;
use crate::datastore_collector::{DatastoreFactory, DatastoreStatsProvider, ListScope};
use crate::datastore_html::DATASTORE_HTML;
use crate::history::DashboardHistory;
use crate::layer::EventStore;
use crate::topology;
use crate::topology_html::TOPOLOGY_HTML;
use crate::warnings::{WarningConfig, WarningDetector};

#[cfg(feature = "distribution")]
use crate::distribution_collector::DistributionStatsProvider;
#[cfg(feature = "distribution")]
use crate::distribution_html::DISTRIBUTION_HTML;

use crate::ci_collector::CiStatsProvider;

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
    #[cfg(feature = "distribution")]
    pub distribution: Arc<Mutex<Option<Arc<dyn DistributionStatsProvider>>>>,
    pub datastore: Arc<Mutex<Option<Arc<dyn DatastoreStatsProvider>>>>,
    pub datastore_factory: Arc<Mutex<Option<Arc<dyn DatastoreFactory>>>>,
    pub ci: Arc<Mutex<Option<Arc<dyn CiStatsProvider>>>>,
    pub peer_auth: Arc<Mutex<Option<Arc<Mutex<distribution::peer_auth::PeerAllowList>>>>>,
    pub join_sender: Arc<Mutex<Option<std::sync::mpsc::Sender<crate::JoinPeerInfo>>>>,
}

// ── Router builders ─────────────────────────────────────────────────────

pub(crate) fn build_live_router(state: AppState) -> Router {
    let router = Router::new()
        .route("/", get(page_dashboard))
        .route("/actors", get(page_actors))
        .route("/topology", get(page_topology))
        .route("/datastore", get(page_datastore))
        .route("/events", get(handle_live_sse))
        .route("/api/stats", get(handle_stats_api))
        .route("/api/history", get(handle_history_api))
        .route("/api/topology", get(handle_topology_api))
        .route("/api/investigate", get(handle_investigate_api))
        .route("/api/datastore", get(handle_datastore_api))
        .route("/api/logs", get(handle_logs_api))
        .route("/api/datastore/list", get(handle_ds_list))
        .route("/api/datastore/get", get(handle_ds_get))
        .route("/api/datastore/data", get(handle_ds_data))
        .route("/api/datastore/status", get(handle_ds_status))
        .route("/api/datastore/put", post(handle_ds_put))
        .route("/api/datastore/delete", post(handle_ds_delete))
        .route("/api/datastore/start", post(handle_ds_start))
        .route("/api/datastore/shutdown", post(handle_ds_shutdown))
        .route("/api/peers", get(handle_peers_list))
        .route("/api/peers/add", post(handle_peers_add))
        .route("/api/peers/sync", post(handle_peers_sync))
        .route("/api/peers/remove", post(handle_peers_remove))
        .route("/actor/{hex}", get(handle_actor_detail));

    #[cfg(feature = "distribution")]
    let router = router
        .route("/distribution", get(page_distribution))
        .route("/api/distribution", get(handle_distribution_api));

    let router = router.route("/api/ci/{*rest}", get(handle_ci_api));

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

async fn page_datastore() -> Response {
    html_response(DATASTORE_HTML, "live")
}

#[cfg(feature = "distribution")]
async fn page_distribution() -> Response {
    html_response(DISTRIBUTION_HTML, "live")
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
                    if !warnings.is_empty() {
                        if let Ok(wjson) = serde_json::to_string(&warnings) {
                            if tx.send(format_sse("warnings", &wjson)).await.is_err() {
                                return;
                            }
                        }
                    }

                    let json = serde_json::to_string(&stats).unwrap();
                    if tx.send(format_sse("stats", &json)).await.is_err() {
                        return;
                    }

                    // Send topology every 5th tick (~1/sec)
                    tick_count += 1;
                    if tick_count % 5 == 0 {
                        let topo = topology::worker_topology(&stats);
                        if let Ok(tjson) = serde_json::to_string(&topo) {
                            if tx.send(format_sse("topology", &tjson)).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }

            // Send distribution snapshot if provider is attached
            #[cfg(feature = "distribution")]
            {
                let maybe_dist = state.distribution.lock().unwrap().clone();
                if let Some(provider) = maybe_dist {
                    if let Some(snapshot) = provider.snapshot() {
                        if let Ok(json) = serde_json::to_string(&snapshot) {
                            if tx.send(format_sse("distribution", &json)).await.is_err() {
                                return;
                            }
                        }
                    }
                }
            }

            // Send datastore snapshot if provider is attached
            {
                let maybe_ds = state.datastore.lock().unwrap().clone();
                match maybe_ds {
                    Some(provider) => {
                        let is_running = provider.is_running();
                        let snap_json = provider.snapshot_json().unwrap_or_else(|| "null".into());
                        let envelope = format!(
                            r#"{{"is_running":{},"snapshot":{}}}"#,
                            is_running, snap_json
                        );
                        if tx.send(format_sse("datastore", &envelope)).await.is_err() {
                            return;
                        }
                    }
                    None => {
                        let envelope = r#"{"is_running":false,"snapshot":null}"#;
                        if tx.send(format_sse("datastore", envelope)).await.is_err() {
                            return;
                        }
                    }
                }
            }

            // Send CI snapshot if provider is attached
            {
                let maybe_ci = state.ci.lock().unwrap().clone();
                if let Some(provider) = maybe_ci {
                    let snapshot = provider.snapshot();
                    if let Ok(json) = serde_json::to_string(&snapshot) {
                        if tx.send(format_sse("ci", &json)).await.is_err() {
                            return;
                        }
                    }
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

#[cfg(feature = "distribution")]
async fn handle_distribution_api(State(state): State<AppState>) -> Response {
    let json = match state.distribution.lock().unwrap().as_ref() {
        Some(provider) => match provider.snapshot() {
            Some(snapshot) => serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".into()),
            None => "{}".to_string(),
        },
        None => serde_json::json!({
            "error": "distribution provider not attached"
        })
        .to_string(),
    };
    json_response(json)
}

async fn handle_datastore_api(State(state): State<AppState>) -> Response {
    let json = match state.datastore.lock().unwrap().as_ref() {
        Some(provider) => provider.snapshot_json().unwrap_or_else(|| "{}".into()),
        None => serde_json::json!({
            "error": "datastore provider not attached"
        })
        .to_string(),
    };
    json_response(json)
}

async fn handle_ci_api(
    State(state): State<AppState>,
    axum::extract::OriginalUri(uri): axum::extract::OriginalUri,
) -> Response {
    use crate::ci_collector;

    let path = uri.path();
    let route = ci_collector::parse_route(path);
    let json = match state.ci.lock().unwrap().as_ref() {
        Some(provider) => {
            let snapshot = provider.snapshot();
            ci_collector::handle_route(&route, &snapshot)
                .unwrap_or_else(|| r#"{"error":"not found"}"#.to_string())
        }
        None => serde_json::json!({
            "error": "CI provider not attached"
        })
        .to_string(),
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

// ── Datastore CRUD API handlers ─────────────────────────────────────────

fn get_ds_provider(
    datastore: &Arc<Mutex<Option<Arc<dyn DatastoreStatsProvider>>>>,
) -> Option<Arc<dyn DatastoreStatsProvider>> {
    datastore.lock().unwrap().clone()
}

async fn handle_ds_list(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let provider = match get_ds_provider(&state.datastore) {
        Some(p) => p,
        None => return json_error(StatusCode::SERVICE_UNAVAILABLE, "datastore not running"),
    };
    let scope = match params.get("scope").map(|s| s.as_str()) {
        Some("local") => ListScope::Local,
        _ => ListScope::Swarm,
    };
    let name_filter = params.get("name").map(|s| s.as_str());
    match provider.list_objects(name_filter, scope) {
        Ok(json) => json_response(json),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn handle_ds_get(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let provider = match get_ds_provider(&state.datastore) {
        Some(p) => p,
        None => return json_error(StatusCode::SERVICE_UNAVAILABLE, "datastore not running"),
    };
    let hash = match params.get("hash") {
        Some(h) => h.as_str(),
        None => return json_error(StatusCode::BAD_REQUEST, "missing ?hash= parameter"),
    };
    match provider.get_object(hash) {
        Ok(json) => json_response(json),
        Err(e) if e.contains("not found") => json_error(StatusCode::NOT_FOUND, &e),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn handle_ds_data(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let provider = match get_ds_provider(&state.datastore) {
        Some(p) => p,
        None => return json_error(StatusCode::SERVICE_UNAVAILABLE, "datastore not running"),
    };
    let hash = match params.get("hash") {
        Some(h) => h.as_str(),
        None => return json_error(StatusCode::BAD_REQUEST, "missing ?hash= parameter"),
    };
    match provider.get_data(hash) {
        Ok(data) => {
            ([(header::CONTENT_TYPE, "application/octet-stream")], data).into_response()
        }
        Err(e) if e.contains("not found") => json_error(StatusCode::NOT_FOUND, &e),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn handle_ds_status(State(state): State<AppState>) -> Response {
    let provider = match get_ds_provider(&state.datastore) {
        Some(p) => p,
        None => return json_error(StatusCode::SERVICE_UNAVAILABLE, "datastore not running"),
    };
    match provider.node_status() {
        Ok(json) => json_response(json),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn handle_ds_put(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
    body: Bytes,
) -> Response {
    let provider = match get_ds_provider(&state.datastore) {
        Some(p) => p,
        None => return json_error(StatusCode::SERVICE_UNAVAILABLE, "datastore not running"),
    };
    let name = params.get("name").cloned();
    match provider.put_data(body.to_vec(), name) {
        Ok(json) => json_response(json),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn handle_ds_delete(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    let provider = match get_ds_provider(&state.datastore) {
        Some(p) => p,
        None => return json_error(StatusCode::SERVICE_UNAVAILABLE, "datastore not running"),
    };
    let hash = match params.get("hash") {
        Some(h) => h.as_str(),
        None => return json_error(StatusCode::BAD_REQUEST, "missing ?hash= parameter"),
    };
    match provider.delete_object(hash) {
        Ok(json) => json_response(json),
        Err(e) if e.contains("not found") => json_error(StatusCode::NOT_FOUND, &e),
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn handle_ds_start(
    State(state): State<AppState>,
    Query(params): Query<HashMap<String, String>>,
) -> Response {
    // Check if already running
    {
        let ds = state.datastore.lock().unwrap();
        if ds.is_some() {
            return json_error(StatusCode::CONFLICT, "datastore already running");
        }
    }

    let fac = match state.datastore_factory.lock().unwrap().clone() {
        Some(f) => f,
        None => return json_error(StatusCode::NOT_IMPLEMENTED, "no datastore factory configured"),
    };

    let storage_path = params.get("storage_path").cloned();

    match fac.start_datastore(storage_path) {
        Ok(provider) => {
            *state.datastore.lock().unwrap() = Some(provider);
            json_response(r#"{"ok":true}"#.to_string())
        }
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

async fn handle_ds_shutdown(State(state): State<AppState>) -> Response {
    let provider = match get_ds_provider(&state.datastore) {
        Some(p) => p,
        None => return json_error(StatusCode::SERVICE_UNAVAILABLE, "datastore not running"),
    };

    match provider.shutdown_datastore() {
        Ok(()) => {
            *state.datastore.lock().unwrap() = None;
            json_response(r#"{"ok":true}"#.to_string())
        }
        Err(e) => json_error(StatusCode::INTERNAL_SERVER_ERROR, &e),
    }
}

// ── Peer Management API ─────────────────────────────────────────────────

async fn handle_peers_list(State(state): State<AppState>) -> Response {
    let maybe_auth = state.peer_auth.lock().unwrap().clone();
    match maybe_auth {
        Some(auth) => {
            let list = auth.lock().unwrap();
            let is_open = list.is_open();
            let peers: Vec<serde_json::Value> = list
                .list_peers()
                .iter()
                .map(|p| {
                    serde_json::json!({
                        "node_id": p.node_id,
                        "label": p.label,
                    })
                })
                .collect();
            let json = serde_json::json!({
                "mode": if is_open { "open" } else { "allow-list" },
                "peers": peers,
            })
            .to_string();
            json_response(json)
        }
        None => {
            let json = serde_json::json!({
                "mode": "open",
                "peers": [],
            })
            .to_string();
            json_response(json)
        }
    }
}

async fn handle_peers_add(State(state): State<AppState>, body: String) -> Response {
    let maybe_auth = state.peer_auth.lock().unwrap().clone();
    let auth = match maybe_auth {
        Some(a) => a,
        None => return json_error(StatusCode::BAD_REQUEST, "peer auth not configured"),
    };

    let parsed: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };

    let node_id_str = match parsed.get("node_id").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return json_error(StatusCode::BAD_REQUEST, "missing node_id field"),
    };
    let label = parsed
        .get("label")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    // Accept hex (64 chars) or base58 (~44 chars)
    let bytes: [u8; 32] = if let Some(b) = distribution::identity::hex_decode(node_id_str) {
        match b.try_into() {
            Ok(arr) => arr,
            Err(_) => {
                return json_error(
                    StatusCode::BAD_REQUEST,
                    "invalid node_id (hex decoded to wrong length)",
                );
            }
        }
    } else if let Some(arr) = distribution::identity::base58_decode(node_id_str) {
        arr
    } else {
        return json_error(
            StatusCode::BAD_REQUEST,
            "invalid node_id (expected 64-char hex or base58)",
        );
    };

    let relay_url = parsed
        .get("relay_url")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    let node_id = distribution::types::NodeId(bytes);
    let mut list = auth.lock().unwrap();
    list.add_peer(node_id, label);
    if let Err(e) = list.save() {
        eprintln!("warning: failed to persist peers.json: {e}");
    }
    drop(list);

    // Trigger a SWIM join for the newly added peer
    if let Some(tx) = state.join_sender.lock().unwrap().as_ref() {
        let _ = tx.send((bytes, relay_url));
    }

    json_response(r#"{"ok":true}"#.to_string())
}

async fn handle_peers_sync(State(state): State<AppState>, body: String) -> Response {
    let maybe_auth = state.peer_auth.lock().unwrap().clone();
    let auth = match maybe_auth {
        Some(a) => a,
        None => return json_error(StatusCode::BAD_REQUEST, "peer auth not configured"),
    };

    let parsed: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };

    let peers = match parsed.get("peers").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => return json_error(StatusCode::BAD_REQUEST, "missing peers array"),
    };

    // Parse all peers first, bail on any error
    let mut parsed_peers: Vec<(distribution::types::NodeId, String)> = Vec::new();
    for peer in peers {
        let node_id_str = match peer.get("node_id").and_then(|v| v.as_str()) {
            Some(s) => s,
            None => return json_error(StatusCode::BAD_REQUEST, "peer missing node_id"),
        };
        let label = peer
            .get("label")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let bytes: [u8; 32] = if let Some(b) = distribution::identity::hex_decode(node_id_str) {
            match b.try_into() {
                Ok(arr) => arr,
                Err(_) => {
                    return json_error(
                        StatusCode::BAD_REQUEST,
                        &format!("invalid node_id hex length for {node_id_str}"),
                    );
                }
            }
        } else if let Some(arr) = distribution::identity::base58_decode(node_id_str) {
            arr
        } else {
            return json_error(
                StatusCode::BAD_REQUEST,
                &format!("invalid node_id: {node_id_str}"),
            );
        };

        parsed_peers.push((distribution::types::NodeId(bytes), label));
    }

    // Add all peers in a single lock acquisition
    {
        let mut list = auth.lock().unwrap();
        for (node_id, label) in &parsed_peers {
            list.add_peer(*node_id, label.clone());
        }
        if let Err(e) = list.save() {
            eprintln!("warning: failed to persist peers.json: {e}");
        }
    }

    // Trigger a SWIM join to the seed peer if specified
    let join_seed = parsed.get("join_seed").and_then(|v| v.as_str());
    if let Some(seed_str) = join_seed {
        let seed_bytes: Option<[u8; 32]> =
            if let Some(b) = distribution::identity::hex_decode(seed_str) {
                b.try_into().ok()
            } else {
                distribution::identity::base58_decode(seed_str)
            };

        if let Some(bytes) = seed_bytes {
            // Find the relay_url for the seed from the peers array
            let relay_url = peers.iter().find_map(|p| {
                let nid = p.get("node_id").and_then(|v| v.as_str())?;
                // Match by checking if this peer's node_id resolves to the same bytes
                let peer_bytes: [u8; 32] =
                    if let Some(b) = distribution::identity::hex_decode(nid) {
                        b.try_into().ok()?
                    } else {
                        distribution::identity::base58_decode(nid)?
                    };
                if peer_bytes == bytes {
                    p.get("relay_url").and_then(|v| v.as_str()).map(|s| s.to_string())
                } else {
                    None
                }
            });

            if let Some(tx) = state.join_sender.lock().unwrap().as_ref() {
                let _ = tx.send((bytes, relay_url));
            }
        }
    }

    let added = parsed_peers.len();
    json_response(format!(r#"{{"ok":true,"added":{added}}}"#))
}

async fn handle_peers_remove(State(state): State<AppState>, body: String) -> Response {
    let maybe_auth = state.peer_auth.lock().unwrap().clone();
    let auth = match maybe_auth {
        Some(a) => a,
        None => return json_error(StatusCode::BAD_REQUEST, "peer auth not configured"),
    };

    let parsed: serde_json::Value = match serde_json::from_str(&body) {
        Ok(v) => v,
        Err(e) => return json_error(StatusCode::BAD_REQUEST, &format!("invalid JSON: {e}")),
    };

    let node_id_hex = match parsed.get("node_id").and_then(|v| v.as_str()) {
        Some(s) => s,
        None => return json_error(StatusCode::BAD_REQUEST, "missing node_id field"),
    };

    let bytes = match distribution::identity::hex_decode(node_id_hex) {
        Some(b) if b.len() == 32 => b,
        _ => {
            return json_error(
                StatusCode::BAD_REQUEST,
                "invalid node_id hex (must be 64 hex chars)",
            );
        }
    };

    let node_id = distribution::types::NodeId(bytes.try_into().unwrap());
    let mut list = auth.lock().unwrap();
    list.remove_peer(&node_id);
    if let Err(e) = list.save() {
        eprintln!("warning: failed to persist peers.json: {e}");
    }

    json_response(r#"{"ok":true}"#.to_string())
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
