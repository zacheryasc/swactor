use std::collections::HashMap;
use std::io::{self, Read as IoRead};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use std::time::Duration;

use serde::Serialize;
use swactor::actor::ActorAddress;
use swactor::config::RuntimeConfig;
use swactor::runtime::Runtime;
use swactor_gossip::protocol::{GossipActor, GossipMessage};
use swactor_gossip::sim::{heal_partition_via_handle, wire_topology, SimConfig};
use swactor_gossip::trace::{
    EventLog, GossipEvent, GossipEventKind, NameRegistry, NodeSnapshot, SimulationTrace,
    TickCounter,
};

use crate::dashboard_html::DASHBOARD_HTML;

// ── Configuration ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DashboardConfig {
    pub port: u16,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self { port: 8080 }
    }
}

// ── Shared dashboard state ─────────────────────────────────────────────

struct DashboardState {
    event_log: EventLog,
    name_registry: NameRegistry,
    init_data: Mutex<Option<InitData>>,
    stats: Mutex<StatsSnapshot>,
    done: AtomicBool,
}

#[derive(Debug, Clone, Serialize)]
struct InitData {
    name: String,
    nodes: Vec<NodeInfo>,
    edges: Vec<[String; 2]>,
    num_threads: usize,
}

#[derive(Debug, Clone, Serialize)]
struct NodeInfo {
    name: String,
    addr: String,
}

#[derive(Debug, Clone, Default, Serialize)]
struct StatsSnapshot {
    total_nodes: usize,
    total_edges: usize,
    total_messages: usize,
    current_round: u64,
    total_rounds: usize,
}

// ── SSE channel adapter ────────────────────────────────────────────────

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

// ── SSE formatting helpers ─────────────────────────────────────────────

fn format_sse(event: &str, data: &str) -> Vec<u8> {
    format!("event: {event}\ndata: {data}\n\n").into_bytes()
}

fn event_kind_name(kind: &GossipEventKind) -> &'static str {
    match kind {
        GossipEventKind::LocalSet { .. } => "LocalSet",
        GossipEventKind::GossipRoundStarted { .. } => "GossipRoundStarted",
        GossipEventKind::GossipRoundNoPeers => "GossipRoundNoPeers",
        GossipEventKind::PushReceived { .. } => "PushReceived",
        GossipEventKind::QueryReceived { .. } => "QueryReceived",
        GossipEventKind::PeerAdded { .. } => "PeerAdded",
        GossipEventKind::PeerRemoved { .. } => "PeerRemoved",
        GossipEventKind::StateSnapshot { .. } => "StateSnapshot",
    }
}

#[derive(Serialize)]
struct SseGossipEvent {
    seq: usize,
    tick: u64,
    node: String,
    thread: Option<String>,
    kind: String,
    detail: serde_json::Value,
}

fn gossip_event_to_sse(seq: usize, ev: &GossipEvent) -> SseGossipEvent {
    let detail = match &ev.kind {
        GossipEventKind::LocalSet { key } => {
            serde_json::json!({ "key": key })
        }
        GossipEventKind::GossipRoundStarted { target_name } => {
            serde_json::json!({ "target": target_name })
        }
        GossipEventKind::GossipRoundNoPeers => serde_json::json!({}),
        GossipEventKind::PushReceived {
            from_name,
            keys_updated,
        } => {
            serde_json::json!({ "from": from_name, "keys_updated": keys_updated })
        }
        GossipEventKind::QueryReceived { key } => {
            serde_json::json!({ "key": key })
        }
        GossipEventKind::PeerAdded { peer_name } => {
            serde_json::json!({ "peer": peer_name })
        }
        GossipEventKind::PeerRemoved { peer_name } => {
            serde_json::json!({ "peer": peer_name })
        }
        GossipEventKind::StateSnapshot { snapshot } => {
            serde_json::json!({
                "entries": snapshot.entries.len(),
                "peer_count": snapshot.peer_count,
            })
        }
    };

    SseGossipEvent {
        seq,
        tick: ev.tick,
        node: ev.node_name.clone(),
        thread: ev.thread_name.clone(),
        kind: event_kind_name(&ev.kind).to_string(),
        detail,
    }
}

// ── HTTP server ────────────────────────────────────────────────────────

fn spawn_http_server(state: Arc<DashboardState>, port: u16, mode: &str) {
    let addr = format!("0.0.0.0:{port}");
    let server = tiny_http::Server::http(&addr).expect("failed to bind HTTP server");
    let server = Arc::new(server);
    let mode = mode.to_string();

    // Spawn a pool of handler threads.
    for _ in 0..4 {
        let server = Arc::clone(&server);
        let state = Arc::clone(&state);
        let mode = mode.clone();
        thread::spawn(move || {
            loop {
                let request = match server.recv() {
                    Ok(r) => r,
                    Err(_) => break,
                };

                let url = request.url().to_string();
                match url.as_str() {
                    "/" => {
                        let html = DASHBOARD_HTML.replace("__DASHBOARD_MODE__", &mode);
                        let response = tiny_http::Response::from_string(html)
                            .with_header(
                                "Content-Type: text/html; charset=utf-8"
                                    .parse::<tiny_http::Header>()
                                    .unwrap(),
                            );
                        let _ = request.respond(response);
                    }
                    "/events" => {
                        handle_sse(request, Arc::clone(&state));
                    }
                    "/trace.json" => {
                        handle_trace_json(request, Arc::clone(&state));
                    }
                    _ => {
                        let response =
                            tiny_http::Response::from_string("Not Found").with_status_code(404);
                        let _ = request.respond(response);
                    }
                }
            }
        });
    }
}

fn handle_sse(request: tiny_http::Request, state: Arc<DashboardState>) {
    let (tx, rx) = mpsc::channel::<Vec<u8>>();
    let reader = ChannelReader::new(rx);

    // Send SSE headers via a streaming response.
    let response = tiny_http::Response::new(
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
    );

    // Spawn producer thread that polls for new events.
    thread::spawn(move || {
        let mut cursor: usize = 0;

        // Wait for init data.
        loop {
            if let Some(init) = state.init_data.lock().unwrap().as_ref() {
                let json = serde_json::to_string(init).unwrap();
                if tx.send(format_sse("init", &json)).is_err() {
                    return;
                }
                break;
            }
            thread::sleep(Duration::from_millis(50));
        }

        // Poll for events.
        loop {
            {
                let log = state.event_log.lock().unwrap();
                while cursor < log.len() {
                    let ev = &log[cursor];
                    // Filter out StateSnapshot events from SSE stream.
                    if !matches!(ev.kind, GossipEventKind::StateSnapshot { .. }) {
                        let sse_ev = gossip_event_to_sse(cursor, ev);
                        let json = serde_json::to_string(&sse_ev).unwrap();
                        if tx.send(format_sse("gossip", &json)).is_err() {
                            return;
                        }
                    }
                    cursor += 1;
                }
            }

            // Send stats update.
            {
                let stats = state.stats.lock().unwrap().clone();
                let json = serde_json::to_string(&stats).unwrap();
                if tx.send(format_sse("stats", &json)).is_err() {
                    return;
                }
            }

            if state.done.load(Ordering::Relaxed) {
                let _ = tx.send(format_sse("done", "{}"));
                let _ = tx.send(Vec::new()); // EOF
                return;
            }

            thread::sleep(Duration::from_millis(50));
        }
    });

    // This blocks until the reader is consumed / connection closes.
    let _ = request.respond(response);
}

fn handle_trace_json(request: tiny_http::Request, state: Arc<DashboardState>) {
    // Build a partial trace from current state.
    let events = state.event_log.lock().unwrap().clone();
    let names_map = state.name_registry.lock().unwrap().clone();
    let init = state.init_data.lock().unwrap().clone();

    let trace = SimulationTrace {
        name: init.as_ref().map(|i| i.name.clone()).unwrap_or_default(),
        node_names: init
            .as_ref()
            .map(|i| i.nodes.iter().map(|n| n.name.clone()).collect())
            .unwrap_or_default(),
        node_addrs: {
            let mut addrs: Vec<ActorAddress> = Vec::new();
            if let Some(init) = &init {
                // Reconstruct addrs from name_registry in node order.
                let inv: HashMap<String, ActorAddress> =
                    names_map.into_iter().map(|(a, n)| (n, a)).collect();
                for node in &init.nodes {
                    if let Some(&addr) = inv.get(&node.name) {
                        addrs.push(addr);
                    }
                }
            }
            addrs
        },
        topology_edges: init
            .as_ref()
            .map(|i| {
                i.edges
                    .iter()
                    .map(|e| (e[0].clone(), e[1].clone()))
                    .collect()
            })
            .unwrap_or_default(),
        events,
        snapshots_per_round: Vec::new(),
        num_rounds: init
            .as_ref()
            .map(|_| {
                state
                    .stats
                    .lock()
                    .unwrap()
                    .total_rounds
            })
            .unwrap_or(0),
        total_keys: 0,
    };

    let json = serde_json::to_string(&trace).unwrap();
    let response = tiny_http::Response::from_string(json).with_header(
        "Content-Type: application/json"
            .parse::<tiny_http::Header>()
            .unwrap(),
    );
    let _ = request.respond(response);
}

// ── Public API: run_with_dashboard ─────────────────────────────────────

pub fn run_with_dashboard(config: SimConfig, dash: DashboardConfig) -> SimulationTrace {
    let num_threads = config.num_threads.max(1);
    let event_log: EventLog = Arc::new(Mutex::new(Vec::new()));
    let tick_counter: TickCounter = Arc::new(AtomicU64::new(0));
    let name_registry: NameRegistry = Arc::new(Mutex::new(HashMap::new()));

    let state = Arc::new(DashboardState {
        event_log: Arc::clone(&event_log),
        name_registry: Arc::clone(&name_registry),
        init_data: Mutex::new(None),
        stats: Mutex::new(StatsSnapshot::default()),
        done: AtomicBool::new(false),
    });

    // Start HTTP server.
    spawn_http_server(Arc::clone(&state), dash.port, "live");

    if num_threads < 2 {
        run_dashboard_single_threaded(config, state, event_log, tick_counter, name_registry)
    } else {
        run_dashboard_multi_threaded(config, state, event_log, tick_counter, name_registry)
    }
}

fn run_dashboard_single_threaded(
    config: SimConfig,
    state: Arc<DashboardState>,
    event_log: EventLog,
    tick_counter: TickCounter,
    name_registry: NameRegistry,
) -> SimulationTrace {
    let rt = Runtime::new(RuntimeConfig {
        num_threads: 1,
        max_actors: (config.num_nodes + 64).next_power_of_two(),
        actor_max_messages: (config.num_nodes * 4).max(1_000),
        ..Default::default()
    });

    let mut addrs = Vec::with_capacity(config.num_nodes);
    let mut names = Vec::with_capacity(config.num_nodes);
    for i in 0..config.num_nodes {
        let name = format!("node-{i}");
        let actor = GossipActor::traced(
            Arc::clone(&event_log),
            Arc::clone(&tick_counter),
            Arc::clone(&name_registry),
        );
        let addr = rt.spawn(actor).unwrap();
        name_registry.lock().unwrap().insert(addr, name.clone());
        addrs.push(addr);
        names.push(name);
    }

    let edges = wire_topology(&rt, &config.topology, &addrs, &names);
    for _ in 0..3 {
        rt.tick();
    }

    // Publish init data.
    publish_init(&state, &config, &addrs, &names, &edges);

    let total_keys = config.initial_data.len();
    for (key, value) in &config.initial_data {
        rt.send_to(
            addrs[0],
            GossipMessage::Set {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .unwrap();
    }
    rt.tick();

    let mut snapshots_per_round: Vec<Vec<(String, NodeSnapshot)>> = Vec::new();

    for round in 0..config.num_rounds {
        if config.heal_after_round == Some(round) {
            swactor_gossip::sim::heal_partition(&rt, &config.topology, &addrs, &names);
            for _ in 0..3 {
                rt.tick();
            }
        }

        tick_counter.store((round + 1) as u64, Ordering::Relaxed);
        update_stats(&state, &config, round, &event_log);

        for &addr in &addrs {
            rt.send_to(addr, GossipMessage::DoGossipRound).unwrap();
        }
        for _ in 0..config.ticks_per_round {
            rt.tick();
        }

        for &addr in &addrs {
            rt.send_to(addr, GossipMessage::TakeSnapshot).unwrap();
        }
        for _ in 0..3 {
            rt.tick();
        }

        let current_round_tick = (round + 1) as u64;
        let log = event_log.lock().unwrap();
        let mut round_snapshots: Vec<(String, NodeSnapshot)> = Vec::new();
        for event in log.iter().rev() {
            if event.tick != current_round_tick {
                break;
            }
            if let GossipEventKind::StateSnapshot { ref snapshot } = event.kind {
                round_snapshots.push((event.node_name.clone(), snapshot.clone()));
            }
        }
        round_snapshots.reverse();
        snapshots_per_round.push(round_snapshots);
    }

    state.done.store(true, Ordering::Relaxed);

    let events = event_log.lock().unwrap().clone();
    SimulationTrace {
        name: config.name,
        node_names: names,
        node_addrs: addrs,
        topology_edges: edges,
        events,
        snapshots_per_round,
        num_rounds: config.num_rounds,
        total_keys,
    }
}

fn run_dashboard_multi_threaded(
    config: SimConfig,
    state: Arc<DashboardState>,
    event_log: EventLog,
    tick_counter: TickCounter,
    name_registry: NameRegistry,
) -> SimulationTrace {
    let ticks_per_round = config.ticks_per_round;
    let settle_ms = (ticks_per_round as u64 * 2).max(10);

    let rt = Runtime::new(RuntimeConfig {
        num_threads: config.num_threads,
        max_actors: (config.num_nodes + 64).next_power_of_two(),
        actor_max_messages: (config.num_nodes * 4).max(1_000),
        ..Default::default()
    });

    let mut addrs = Vec::with_capacity(config.num_nodes);
    let mut names = Vec::with_capacity(config.num_nodes);
    for i in 0..config.num_nodes {
        let name = format!("node-{i}");
        let actor = GossipActor::traced(
            Arc::clone(&event_log),
            Arc::clone(&tick_counter),
            Arc::clone(&name_registry),
        );
        let addr = rt.spawn(actor).unwrap();
        name_registry.lock().unwrap().insert(addr, name.clone());
        addrs.push(addr);
        names.push(name);
    }

    let edges = wire_topology(&rt, &config.topology, &addrs, &names);

    // Publish init data.
    publish_init(&state, &config, &addrs, &names, &edges);

    let total_keys = config.initial_data.len();
    for (key, value) in &config.initial_data {
        rt.send_to(
            addrs[0],
            GossipMessage::Set {
                key: key.clone(),
                value: value.clone(),
            },
        )
        .unwrap();
    }

    let handle = rt.run().expect("failed to start multi-threaded runtime");
    thread::sleep(Duration::from_millis(settle_ms * 2));

    let mut snapshots_per_round: Vec<Vec<(String, NodeSnapshot)>> = Vec::new();

    for round in 0..config.num_rounds {
        if config.heal_after_round == Some(round) {
            heal_partition_via_handle(&handle, &config.topology, &addrs, &names);
            thread::sleep(Duration::from_millis(settle_ms));
        }

        tick_counter.store((round + 1) as u64, Ordering::Relaxed);
        update_stats(&state, &config, round, &event_log);

        for &addr in &addrs {
            handle
                .runtime
                .send_to(addr, GossipMessage::DoGossipRound)
                .unwrap();
        }
        thread::sleep(Duration::from_millis(settle_ms));

        for &addr in &addrs {
            handle
                .runtime
                .send_to(addr, GossipMessage::TakeSnapshot)
                .unwrap();
        }
        thread::sleep(Duration::from_millis(settle_ms / 2));

        let current_round_tick = (round + 1) as u64;
        let log = event_log.lock().unwrap();
        let mut round_snapshots: Vec<(String, NodeSnapshot)> = Vec::new();
        for event in log.iter().rev() {
            if event.tick != current_round_tick {
                break;
            }
            if let GossipEventKind::StateSnapshot { ref snapshot } = event.kind {
                round_snapshots.push((event.node_name.clone(), snapshot.clone()));
            }
        }
        round_snapshots.reverse();
        snapshots_per_round.push(round_snapshots);
    }

    handle.shutdown();
    handle.join();

    state.done.store(true, Ordering::Relaxed);

    let events = event_log.lock().unwrap().clone();
    SimulationTrace {
        name: config.name,
        node_names: names,
        node_addrs: addrs,
        topology_edges: edges,
        events,
        snapshots_per_round,
        num_rounds: config.num_rounds,
        total_keys,
    }
}

// ── Helpers ────────────────────────────────────────────────────────────

fn publish_init(
    state: &DashboardState,
    config: &SimConfig,
    addrs: &[ActorAddress],
    names: &[String],
    edges: &[(String, String)],
) {
    let nodes: Vec<NodeInfo> = names
        .iter()
        .zip(addrs.iter())
        .map(|(name, addr)| NodeInfo {
            name: name.clone(),
            addr: format!("{addr}"),
        })
        .collect();
    let edge_pairs: Vec<[String; 2]> = edges
        .iter()
        .map(|(a, b)| [a.clone(), b.clone()])
        .collect();
    *state.init_data.lock().unwrap() = Some(InitData {
        name: config.name.clone(),
        nodes,
        edges: edge_pairs,
        num_threads: config.num_threads,
    });
    *state.stats.lock().unwrap() = StatsSnapshot {
        total_nodes: config.num_nodes,
        total_edges: edges.len(),
        total_messages: 0,
        current_round: 0,
        total_rounds: config.num_rounds,
    };
}

fn update_stats(state: &DashboardState, config: &SimConfig, round: usize, event_log: &EventLog) {
    let msg_count = event_log.lock().unwrap().len();
    let mut stats = state.stats.lock().unwrap();
    stats.current_round = (round + 1) as u64;
    stats.total_messages = msg_count;
    stats.total_rounds = config.num_rounds;
}

// ── Replay mode ────────────────────────────────────────────────────────

pub fn serve_replay(trace: &SimulationTrace, port: u16) {
    let event_log: EventLog = Arc::new(Mutex::new(trace.events.clone()));
    let name_registry: NameRegistry = Arc::new(Mutex::new(
        trace
            .node_names
            .iter()
            .zip(trace.node_addrs.iter())
            .map(|(n, a)| (*a, n.clone()))
            .collect(),
    ));

    let edges: Vec<[String; 2]> = trace
        .topology_edges
        .iter()
        .map(|(a, b)| [a.clone(), b.clone()])
        .collect();
    let nodes: Vec<NodeInfo> = trace
        .node_names
        .iter()
        .zip(trace.node_addrs.iter())
        .map(|(name, addr)| NodeInfo {
            name: name.clone(),
            addr: format!("{addr}"),
        })
        .collect();

    // Keep state alive for potential future SSE support in replay mode.
    let _state = Arc::new(DashboardState {
        event_log,
        name_registry,
        init_data: Mutex::new(Some(InitData {
            name: trace.name.clone(),
            nodes,
            edges,
            num_threads: 1,
        })),
        stats: Mutex::new(StatsSnapshot {
            total_nodes: trace.node_names.len(),
            total_edges: trace.topology_edges.len(),
            total_messages: trace.events.len(),
            current_round: trace.num_rounds as u64,
            total_rounds: trace.num_rounds,
        }),
        done: AtomicBool::new(true),
    });

    let addr = format!("0.0.0.0:{port}");
    let server = tiny_http::Server::http(&addr).expect("failed to bind HTTP server");

    eprintln!("Replay dashboard at http://localhost:{port}");

    loop {
        let request = match server.recv() {
            Ok(r) => r,
            Err(_) => break,
        };

        let url = request.url().to_string();
        match url.as_str() {
            "/" => {
                let html = DASHBOARD_HTML.replace("__DASHBOARD_MODE__", "replay");
                let response = tiny_http::Response::from_string(html).with_header(
                    "Content-Type: text/html; charset=utf-8"
                        .parse::<tiny_http::Header>()
                        .unwrap(),
                );
                let _ = request.respond(response);
            }
            "/trace.json" => {
                let json = serde_json::to_string(trace).unwrap();
                let response = tiny_http::Response::from_string(json).with_header(
                    "Content-Type: application/json"
                        .parse::<tiny_http::Header>()
                        .unwrap(),
                );
                let _ = request.respond(response);
            }
            _ => {
                let response =
                    tiny_http::Response::from_string("Not Found").with_status_code(404);
                let _ = request.respond(response);
            }
        }
    }
}
