use std::io::{self, Read as IoRead};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use swactor::runtime::Runtime;

use crate::dashboard_html::DASHBOARD_HTML;
use crate::layer::EventStore;
use crate::trace::RuntimeTrace;

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

fn respond_html(request: tiny_http::Request, mode: &str) {
    let html = DASHBOARD_HTML.replace("__DASHBOARD_MODE__", mode);
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
    shutdown: Arc<AtomicBool>,
    port: u16,
) {
    let addr = format!("0.0.0.0:{port}");
    let server = tiny_http::Server::http(&addr).expect("failed to bind HTTP server");
    let server = Arc::new(server);

    for _ in 0..4 {
        let server = Arc::clone(&server);
        let store = Arc::clone(&store);
        let runtime = Arc::clone(&runtime);
        let shutdown = Arc::clone(&shutdown);
        thread::spawn(move || {
            loop {
                let request = match server.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };

                let url = request.url().to_string();
                match url.as_str() {
                    "/" => respond_html(request, "live"),
                    "/events" => {
                        handle_live_sse(
                            request,
                            Arc::clone(&store),
                            Arc::clone(&runtime),
                            Arc::clone(&shutdown),
                        );
                    }
                    "/api/stats" => {
                        handle_stats_api(request, Arc::clone(&runtime));
                    }
                    _ => respond_404(request),
                }
            }
        });
    }
}

fn handle_live_sse(
    request: tiny_http::Request,
    store: Arc<EventStore>,
    runtime: Arc<Mutex<Option<Arc<Runtime>>>>,
    shutdown: Arc<AtomicBool>,
) {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let response = make_sse_response(rx);

    // Spawn producer thread
    thread::spawn(move || {
        let mut cursor: u64 = 0;

        loop {
            // Send stats if runtime is available
            {
                let maybe_rt = runtime.lock().unwrap().clone();
                if let Some(rt) = maybe_rt {
                    let stats = rt.stats();
                    let json = serde_json::to_string(&stats).unwrap();
                    if tx.send(format_sse("stats", &json)).is_err() {
                        return;
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

fn handle_stats_api(request: tiny_http::Request, runtime: Arc<Mutex<Option<Arc<Runtime>>>>) {
    let maybe_rt = runtime.lock().unwrap().clone();
    let json = match maybe_rt {
        Some(rt) => serde_json::to_string(&rt.stats()).unwrap(),
        None => "{}".to_string(),
    };
    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
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
                    "/" => respond_html(request, "replay"),
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
