use std::io::{self, Read as IoRead};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use std::collections::HashMap;

use swactor::runtime::Runtime;

use crate::actor_detail_html::ACTOR_DETAIL_HTML;
use crate::actors_html::ACTORS_HTML;
use crate::collector::StatsCollector;
use crate::dashboard_html::DASHBOARD_HTML;
use crate::history::DashboardHistory;
use crate::layer::EventStore;
use crate::topology;
use crate::topology_html::TOPOLOGY_HTML;
use crate::trace::RuntimeTrace;
use crate::warnings::{WarningConfig, WarningDetector};

#[cfg(feature = "distribution")]
use crate::distribution_collector::DistributionStatsProvider;
#[cfg(feature = "distribution")]
use crate::distribution_html::DISTRIBUTION_HTML;

use crate::datastore_collector::DatastoreStatsProvider;
use crate::datastore_html::DATASTORE_HTML;

#[cfg(feature = "ci")]
use crate::ci_collector::CiStatsProvider;

/// Format a server-sent event.
fn format_sse(event: &str, data: &str) -> Vec<u8> {
    format!("event: {event}\ndata: {data}\n\n").into_bytes()
}

/// Adapts an `mpsc::Receiver<Vec<u8>>` to `std::io::Read` for tiny_http streaming.
struct ChannelReader {
    rx: mpsc::Receiver<Vec<u8>>,
    buf: Vec<u8>,
    pos: usize,
}

impl ChannelReader {
    fn new(rx: mpsc::Receiver<Vec<u8>>) -> Self {
        Self {
            rx,
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl IoRead for ChannelReader {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        // Drain current buffer first.
        if self.pos < self.buf.len() {
            let n = std::cmp::min(out.len(), self.buf.len() - self.pos);
            out[..n].copy_from_slice(&self.buf[self.pos..self.pos + n]);
            self.pos += n;
            return Ok(n);
        }

        // Wait for next chunk.
        match self.rx.recv() {
            Ok(data) => {
                if data.is_empty() {
                    return Ok(0); // EOF signal
                }
                let n = std::cmp::min(out.len(), data.len());
                out[..n].copy_from_slice(&data[..n]);
                if n < data.len() {
                    self.buf = data;
                    self.pos = n;
                } else {
                    self.buf.clear();
                    self.pos = 0;
                }
                Ok(n)
            }
            Err(_) => Ok(0), // channel closed
        }
    }
}

fn make_sse_response(
    rx: mpsc::Receiver<Vec<u8>>,
) -> tiny_http::Response<Box<dyn IoRead + Send>> {
    let reader = ChannelReader::new(rx);
    tiny_http::Response::new(
        tiny_http::StatusCode(200),
        vec![
            "Content-Type: text/event-stream"
                .parse::<tiny_http::Header>()
                .unwrap(),
            "Cache-Control: no-cache"
                .parse::<tiny_http::Header>()
                .unwrap(),
            "Connection: keep-alive"
                .parse::<tiny_http::Header>()
                .unwrap(),
        ],
        Box::new(reader) as Box<dyn IoRead + Send>,
        None,
        None,
    )
}

fn respond_html(request: tiny_http::Request, html_template: &str, mode: &str) {
    let html = html_template.replace("__DASHBOARD_MODE__", mode);
    let response = tiny_http::Response::from_string(html).with_header(
        "Content-Type: text/html; charset=utf-8"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn respond_404(request: tiny_http::Request) {
    let response = tiny_http::Response::from_string("Not Found").with_status_code(404);
    let _ = request.respond(response);
}

// ── Live server ─────────────────────────────────────────────────────────

/// Start the live HTTP server with a pool of handler threads.
pub(crate) fn spawn_http_server(
    store: Arc<EventStore>,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    collector: Arc<Mutex<Option<Arc<StatsCollector>>>>,
    shutdown: Arc<AtomicBool>,
    history: Arc<DashboardHistory>,
    port: u16,
    #[cfg(feature = "distribution")]
    distribution: Arc<Mutex<Option<Arc<dyn DistributionStatsProvider>>>>,
    datastore: Arc<Mutex<Option<Arc<dyn DatastoreStatsProvider>>>>,
    #[cfg(feature = "ci")]
    ci: Arc<Mutex<Option<Arc<dyn CiStatsProvider>>>>,
) {
    let addr = format!("0.0.0.0:{port}");
    let server = tiny_http::Server::http(&addr).expect("failed to bind HTTP server");
    let server = Arc::new(server);
    let cmd_router = Arc::new(crate::command::CommandRouter::with_builtins());

    for _ in 0..4 {
        let server = Arc::clone(&server);
        let store = Arc::clone(&store);
        let runtime = Arc::clone(&runtime);
        let collector = Arc::clone(&collector);
        let shutdown = Arc::clone(&shutdown);
        let history = Arc::clone(&history);
        let cmd_router = Arc::clone(&cmd_router);
        #[cfg(feature = "distribution")]
        let distribution = Arc::clone(&distribution);
        let datastore = Arc::clone(&datastore);
        #[cfg(feature = "ci")]
        let ci = Arc::clone(&ci);
        thread::spawn(move || {
            loop {
                let request = match server.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };

                let url = request.url().to_string();
                let path = url.split('?').next().unwrap_or(&url);
                match path {
                    "/" => respond_html(request, DASHBOARD_HTML, "live"),
                    "/actors" => respond_html(request, ACTORS_HTML, "live"),
                    "/topology" => respond_html(request, TOPOLOGY_HTML, "live"),
                    #[cfg(feature = "distribution")]
                    "/distribution" => respond_html(request, DISTRIBUTION_HTML, "live"),
                    "/datastore" => respond_html(request, DATASTORE_HTML, "live"),
                    "/events" => {
                        handle_live_sse(
                            request,
                            Arc::clone(&store),
                            Arc::clone(&runtime),
                            Arc::clone(&collector),
                            Arc::clone(&shutdown),
                            Arc::clone(&history),
                            #[cfg(feature = "distribution")]
                            Arc::clone(&distribution),
                            Arc::clone(&datastore),
                            #[cfg(feature = "ci")]
                            Arc::clone(&ci),
                        );
                    }
                    "/api/stats" => {
                        handle_stats_api(
                            request,
                            Arc::clone(&runtime),
                            Arc::clone(&collector),
                        );
                    }
                    "/api/history" => {
                        handle_history_api(request, Arc::clone(&history));
                    }
                    "/api/topology" => {
                        handle_topology_api(
                            request,
                            Arc::clone(&runtime),
                            Arc::clone(&collector),
                        );
                    }
                    "/api/investigate" => {
                        handle_investigate_api(
                            request,
                            &url,
                            Arc::clone(&runtime),
                            Arc::clone(&collector),
                            Arc::clone(&cmd_router),
                        );
                    }
                    #[cfg(feature = "distribution")]
                    "/api/distribution" => {
                        handle_distribution_api(
                            request,
                            Arc::clone(&distribution),
                        );
                    }
                    "/api/datastore" => {
                        handle_datastore_api(
                            request,
                            Arc::clone(&datastore),
                        );
                    }
                    "/api/logs" => {
                        handle_logs_api(request, &url, Arc::clone(&store));
                    }
                    #[cfg(feature = "ci")]
                    _ if path.starts_with("/api/ci/") => {
                        handle_ci_api(request, path, Arc::clone(&ci));
                    }
                    _ if path.starts_with("/actor/") => {
                        let hex = &path[7..]; // strip "/actor/"
                        respond_actor_detail(request, hex);
                    }
                    _ => respond_404(request),
                }
            }
        });
    }
}

fn respond_actor_detail(request: tiny_http::Request, hex_addr: &str) {
    let html = ACTOR_DETAIL_HTML
        .replace("__DASHBOARD_MODE__", "live")
        .replace("__ACTOR_ADDR__", hex_addr);
    let response = tiny_http::Response::from_string(html).with_header(
        "Content-Type: text/html; charset=utf-8"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn handle_live_sse(
    request: tiny_http::Request,
    store: Arc<EventStore>,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    collector: Arc<Mutex<Option<Arc<StatsCollector>>>>,
    shutdown: Arc<AtomicBool>,
    history: Arc<DashboardHistory>,
    #[cfg(feature = "distribution")]
    distribution: Arc<Mutex<Option<Arc<dyn DistributionStatsProvider>>>>,
    datastore: Arc<Mutex<Option<Arc<dyn DatastoreStatsProvider>>>>,
    #[cfg(feature = "ci")]
    ci: Arc<Mutex<Option<Arc<dyn CiStatsProvider>>>>,
) {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let response = make_sse_response(rx);

    // Spawn producer thread
    thread::spawn(move || {
        let mut cursor: u64 = 0;
        let mut warning_detector = WarningDetector::new(WarningConfig::default());
        let mut tick_count: u64 = 0;

        // Send initial history snapshot so sparklines render immediately
        if history.sample_count() > 0 {
            let json = history.worker_history_json();
            let _ = tx.send(format_sse("history", &json));
        }

        loop {
            // Send stats if runtime is available
            {
                let maybe_rt = runtime.lock().unwrap().clone();
                if let Some(rt) = maybe_rt {
                    let mut stats = rt.stats();
                    if let Some(col) = collector.lock().unwrap().as_ref() {
                        col.enrich(&mut stats);
                    }
                    history.record(&stats);

                    // Run warning detection
                    let warnings = warning_detector.check(&stats);
                    if !warnings.is_empty() {
                        if let Ok(wjson) = serde_json::to_string(&warnings) {
                            if tx.send(format_sse("warnings", &wjson)).is_err() {
                                return;
                            }
                        }
                    }

                    let json = serde_json::to_string(&stats).unwrap();
                    if tx.send(format_sse("stats", &json)).is_err() {
                        return;
                    }

                    // Send topology every 5th tick (~1/sec)
                    tick_count += 1;
                    if tick_count % 5 == 0 {
                        let topo = topology::worker_topology(&stats);
                        if let Ok(tjson) = serde_json::to_string(&topo) {
                            if tx.send(format_sse("topology", &tjson)).is_err() {
                                return;
                            }
                        }
                    }
                }
            }

            // Send distribution snapshot if provider is attached
            #[cfg(feature = "distribution")]
            {
                let maybe_dist = distribution.lock().unwrap().clone();
                if let Some(provider) = maybe_dist {
                    if let Some(snapshot) = provider.snapshot() {
                        if let Ok(json) = serde_json::to_string(&snapshot) {
                            if tx.send(format_sse("distribution", &json)).is_err() {
                                return;
                            }
                        }
                    }
                }
            }

            // Send datastore snapshot if provider is attached
            {
                let maybe_ds = datastore.lock().unwrap().clone();
                if let Some(provider) = maybe_ds {
                    if let Some(json) = provider.snapshot_json() {
                        if tx.send(format_sse("datastore", &json)).is_err() {
                            return;
                        }
                    }
                }
            }

            // Send CI snapshot if provider is attached
            #[cfg(feature = "ci")]
            {
                let maybe_ci = ci.lock().unwrap().clone();
                if let Some(provider) = maybe_ci {
                    let snapshot = provider.snapshot();
                    if let Ok(json) = serde_json::to_string(&snapshot) {
                        if tx.send(format_sse("ci", &json)).is_err() {
                            return;
                        }
                    }
                }
            }

            // Send new activity events
            let (batch, new_cursor) = store.read_from(cursor);
            if !batch.is_empty() {
                let json = serde_json::to_string(&batch).unwrap();
                if tx.send(format_sse("activity", &json)).is_err() {
                    return;
                }
                cursor = new_cursor;
            }

            if shutdown.load(Ordering::Relaxed) {
                let _ = tx.send(format_sse("done", "{}"));
                let _ = tx.send(Vec::new()); // EOF
                return;
            }

            thread::sleep(Duration::from_millis(200));
        }
    });

    // Blocks until connection closes
    let _ = request.respond(response);
}

fn handle_stats_api(
    request: tiny_http::Request,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    collector: Arc<Mutex<Option<Arc<StatsCollector>>>>,
) {
    let maybe_rt = runtime.lock().unwrap().clone();
    let json = match maybe_rt {
        Some(rt) => {
            let mut stats = rt.stats();
            if let Some(col) = collector.lock().unwrap().as_ref() {
                col.enrich(&mut stats);
            }
            serde_json::to_string(&stats).unwrap()
        }
        None => "{}".to_string(),
    };
    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn handle_investigate_api(
    request: tiny_http::Request,
    url: &str,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    collector: Arc<Mutex<Option<Arc<StatsCollector>>>>,
    cmd_router: Arc<crate::command::CommandRouter>,
) {
    let params = parse_query_string(url);

    let maybe_rt = runtime.lock().unwrap().clone();
    let maybe_col = collector.lock().unwrap().clone();

    let json = match (maybe_rt, maybe_col) {
        (Some(rt), Some(col)) => {
            let ctx = crate::command::CommandContext::with_enricher(rt, col);
            let req = crate::command::from_query_params(&params);
            cmd_router.dispatch(&req, &ctx).to_json_line()
        }
        (Some(rt), None) => {
            let ctx = crate::command::CommandContext::new(rt);
            let req = crate::command::from_query_params(&params);
            cmd_router.dispatch(&req, &ctx).to_json_line()
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

    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

#[cfg(feature = "distribution")]
fn handle_distribution_api(
    request: tiny_http::Request,
    distribution: Arc<Mutex<Option<Arc<dyn DistributionStatsProvider>>>>,
) {
    let json = match distribution.lock().unwrap().as_ref() {
        Some(provider) => match provider.snapshot() {
            Some(snapshot) => serde_json::to_string(&snapshot).unwrap_or_else(|_| "{}".into()),
            None => "{}".to_string(),
        },
        None => serde_json::json!({
            "error": "distribution provider not attached"
        })
        .to_string(),
    };

    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn handle_datastore_api(
    request: tiny_http::Request,
    datastore: Arc<Mutex<Option<Arc<dyn DatastoreStatsProvider>>>>,
) {
    let json = match datastore.lock().unwrap().as_ref() {
        Some(provider) => provider.snapshot_json().unwrap_or_else(|| "{}".into()),
        None => serde_json::json!({
            "error": "datastore provider not attached"
        })
        .to_string(),
    };

    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

#[cfg(feature = "ci")]
fn handle_ci_api(
    request: tiny_http::Request,
    path: &str,
    ci: Arc<Mutex<Option<Arc<dyn CiStatsProvider>>>>,
) {
    use crate::ci_collector;

    let route = ci_collector::parse_route(path);
    let json = match ci.lock().unwrap().as_ref() {
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

    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn handle_topology_api(
    request: tiny_http::Request,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    collector: Arc<Mutex<Option<Arc<StatsCollector>>>>,
) {
    let maybe_rt = runtime.lock().unwrap().clone();
    let json = match maybe_rt {
        Some(rt) => {
            let mut stats = rt.stats();
            if let Some(col) = collector.lock().unwrap().as_ref() {
                col.enrich(&mut stats);
            }
            let topo = topology::worker_topology(&stats);
            serde_json::to_string(&topo).unwrap_or_else(|_| "{}".into())
        }
        None => "{}".to_string(),
    };
    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn handle_logs_api(request: tiny_http::Request, url: &str, store: Arc<EventStore>) {
    let params = parse_query_string(url);
    let actor = params.get("actor").cloned().unwrap_or_default();
    let limit: usize = params
        .get("limit")
        .and_then(|s| s.parse().ok())
        .unwrap_or(200);
    let level = params.get("level").cloned();

    let mut events = store.read_for_actor(&actor, limit);

    // Filter by level if specified
    if let Some(ref lvl) = level {
        let lvl_upper = lvl.to_uppercase();
        events.retain(|e| e.level == lvl_upper);
    }

    let json = serde_json::to_string(&events).unwrap_or_else(|_| "[]".into());
    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn handle_history_api(request: tiny_http::Request, history: Arc<DashboardHistory>) {
    let json = history.worker_history_json();
    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

fn parse_query_string(url: &str) -> HashMap<String, String> {
    let mut params = HashMap::new();
    if let Some(qs) = url.split('?').nth(1) {
        for pair in qs.split('&') {
            let mut kv = pair.splitn(2, '=');
            if let (Some(k), Some(v)) = (kv.next(), kv.next()) {
                params.insert(k.to_string(), v.to_string());
            }
        }
    }
    params
}

// ── Replay server ───────────────────────────────────────────────────────

/// Start a replay HTTP server that serves a pre-recorded trace.
pub(crate) fn spawn_replay_server(trace: Arc<RuntimeTrace>, port: u16, speed: f64) {
    let addr = format!("0.0.0.0:{port}");
    let server = tiny_http::Server::http(&addr).expect("failed to bind HTTP server");
    let server = Arc::new(server);

    for _ in 0..4 {
        let server = Arc::clone(&server);
        let trace = Arc::clone(&trace);
        thread::spawn(move || {
            loop {
                let request = match server.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };

                let url = request.url().to_string();
                match url.as_str() {
                    "/" => respond_html(request, DASHBOARD_HTML, "replay"),
                    "/actors" => respond_html(request, ACTORS_HTML, "replay"),
                    "/events" => {
                        handle_replay_sse(request, Arc::clone(&trace), speed);
                    }
                    _ => respond_404(request),
                }
            }
        });
    }
}

fn handle_replay_sse(request: tiny_http::Request, trace: Arc<RuntimeTrace>, speed: f64) {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let response = make_sse_response(rx);

    thread::spawn(move || {
        // Send replay metadata
        let meta = serde_json::json!({
            "total_events": trace.events.len(),
            "total_stats": trace.stats_timeline.len(),
            "speed": speed,
        });
        if tx.send(format_sse("replay_meta", &meta.to_string())).is_err() {
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

        let playback_start = Instant::now();
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
                if tx.send(format_sse("activity", &json)).is_err() {
                    return;
                }
            }

            // Send stats snapshots up to virtual_time
            while stats_idx < trace.stats_timeline.len()
                && trace.stats_timeline[stats_idx].timestamp_ms <= virtual_time
            {
                let json =
                    serde_json::to_string(&trace.stats_timeline[stats_idx].stats).unwrap();
                if tx.send(format_sse("stats", &json)).is_err() {
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
                .is_err()
            {
                return;
            }

            // Check if replay is complete
            if event_idx >= trace.events.len()
                && stats_idx >= trace.stats_timeline.len()
            {
                let _ = tx.send(format_sse("done", "{}"));
                let _ = tx.send(Vec::new()); // EOF
                return;
            }

            thread::sleep(Duration::from_millis(50));
        }
    });

    // Blocks until connection closes
    let _ = request.respond(response);
}
