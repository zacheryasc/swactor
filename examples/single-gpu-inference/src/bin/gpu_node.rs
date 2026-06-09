//! gpu-node — GPU inference node for distributed tinygrad inference.
//!
//! Runs inside a Docker container (or locally for testing). Joins an existing
//! cluster via `SEED_ADDR`, spawns an `InferenceActor` backed by
//! `tinygrad_worker.py`, and registers the bridge under the name `"inference"`
//! so the orchestrator can discover it.
//!
//! Environment variables:
//! - `SEED_ADDR` (required): hex-encoded NodeId of the seed node to join
//! - `SEED_DIRECT` (optional): comma-separated `ip:port` direct addresses for the seed
//! - `WORKER_CMD` (optional): path to Python interpreter (default: `python3`)
//! - `WORKER_SCRIPT` (optional): path to tinygrad_worker.py (default: `./tinygrad_worker.py`)
//! - `PYTHON` (optional): set to `1` for CPU fallback backend (no GPU/clang)

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::RelayMode;

use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_transport::CodecRegistry;

use single_gpu_inference::cluster::ClusterNode;
use single_gpu_inference::inference_actor::{InferenceActor, InferenceActorStatus, RequestBridge};
use single_gpu_inference::iroh_transport::{decode_wire, IrohActorTransport, ACTOR_ALPN};
use single_gpu_inference::messages::{inference_codec_registry, InferenceRequest};
use swactor_process::{ProcessMode, ProcessSpec};

fn parse_hex_node_id(s: &str) -> [u8; 32] {
    assert!(
        s.len() == 64,
        "SEED_ADDR must be 64 hex characters (32 bytes)"
    );
    let mut bytes = [0u8; 32];
    for i in 0..32 {
        bytes[i] = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .unwrap_or_else(|_| panic!("invalid hex at position {}", i * 2));
    }
    bytes
}

fn node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            // Probe one peer every ~200 ms (old config: 10 ticks at the
            // implicit ~20 ms tick).
            probe_interval: Duration::from_millis(200),
            // Wait for a direct ack before indirect probes (old: 15 ticks).
            probe_timeout: Duration::from_millis(300),
            indirect_probes: 2,
            // How long a node stays Suspect before Dead (old: 60 ticks).
            suspicion_timeout: Duration::from_secs(2),
            // Periodically reprobe dead peers (old: 100 ticks).
            dead_reprobe_interval: Duration::from_secs(2),
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn worker_spec() -> ProcessSpec {
    let cmd = std::env::var("WORKER_CMD").unwrap_or_else(|_| "python3".into());
    let script =
        std::env::var("WORKER_SCRIPT").unwrap_or_else(|_| "./tinygrad_worker.py".into());

    let mut env = HashMap::new();
    if let Ok(val) = std::env::var("PYTHON") {
        env.insert("PYTHON".into(), val);
    }

    ProcessSpec {
        command: cmd,
        args: vec![script],
        env,
        working_dir: None,
        mode: ProcessMode::Automated,
        initial_pty_size: None,
        kill_timeout: Some(Duration::from_secs(5)),
        stdin_buffer_limit: None,
    }
}

/// Drain incoming actor messages from iroh into the swactor runtime.
///
/// Like `drain_actor_messages` from the shared module, but also inspects
/// `InferenceRequest` payloads to extract `reply_to` addresses. Returns
/// the set of reply-to addresses found so the caller can dynamically
/// register transport routes for the response path.
fn drain_and_collect_reply_addrs(
    driver: &IrohDriver,
    codecs: &CodecRegistry,
    rt: &Runtime,
    drain_sleep: Duration,
) -> Vec<ActorAddress> {
    let conns = driver.drain_other_connections();
    if conns.is_empty() {
        return vec![];
    }
    let handle = driver.tokio_handle();
    let (tx, rx) = std::sync::mpsc::channel::<Vec<u8>>();

    for (_node_id, conn) in conns {
        let tx = tx.clone();
        handle.spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_secs(1), conn.accept_uni()).await {
                    Ok(Ok(mut recv)) => {
                        if let Ok(data) = recv.read_to_end(256 * 1024).await {
                            let _ = tx.send(data);
                        }
                    }
                    _ => break,
                }
            }
        });
    }
    drop(tx);
    std::thread::sleep(drain_sleep);

    let mut reply_addrs = Vec::new();
    for data in rx.try_iter() {
        if let Some(envelope) = decode_wire(&data) {
            // Inspect InferenceRequest payloads for reply_to addresses
            if envelope.type_tag == "smoke::InferenceRequest" {
                if let Ok(req) = serde_json::from_slice::<InferenceRequest>(&envelope.payload) {
                    reply_addrs.push(req.reply_to);
                }
            }
            if let Ok((addr, msg)) = codecs.receive(envelope) {
                let _ = rt.deliver_raw(addr, msg);
            }
        }
    }
    reply_addrs
}

fn main() {
    // Build the actorized cluster node: an IrohDriver transport bridge plus the
    // SWIM / registry / metadata / directory protocol actors hosted on one
    // swactor runtime. App actors (inference + bridge) live on the same runtime.
    // The codec registry must carry both the distribution protocol wire tags and
    // the inference app types — `inference_codec_registry` composes both.
    let mut cluster = ClusterNode::new(
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Default,
            node: node_config(),
            peer_auth: None,
            additional_alpns: vec![ACTOR_ALPN.to_vec()],
        },
        node_config(),
        inference_codec_registry(),
        |_rt| {},
    )
    .expect("failed to create cluster node");

    // Print node address info so tests/orchestrators can discover us
    let my_id = cluster.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let direct_addrs: Vec<String> = cluster
        .driver
        .direct_addresses()
        .iter()
        .map(|sa| sa.to_string())
        .collect();
    let direct_str = direct_addrs.join(",");
    eprintln!("gpu-node started (node_id: {my_hex})");
    eprintln!("GPU_NODE_ADDR {my_hex} {direct_str}");

    // Optionally join a seed node (required for remote deployments,
    // optional for localhost testing where the orchestrator joins us)
    if let Ok(seed_hex_raw) = std::env::var("SEED_ADDR") {
        let seed_hex = seed_hex_raw.trim().to_string();
        let seed_bytes = parse_hex_node_id(&seed_hex);
        let seed_key =
            iroh::PublicKey::from_bytes(&seed_bytes).expect("invalid seed public key");

        let mut seed_addr = iroh::EndpointAddr::from(seed_key);
        // Add relay URL so we can find the seed over the internet
        if let Ok(relay) = std::env::var("SEED_RELAY") {
            let relay = relay.trim().to_string();
            if let Ok(relay_url) = relay.parse::<iroh::RelayUrl>() {
                eprintln!("using seed relay: {relay}");
                seed_addr = seed_addr.with_relay_url(relay_url);
            }
        }
        if let Ok(direct) = std::env::var("SEED_DIRECT") {
            for part in direct.split(',') {
                if let Ok(sa) = part.trim().parse::<SocketAddr>() {
                    seed_addr = seed_addr.with_ip_addr(sa);
                }
            }
        }

        eprintln!("joining seed: {seed_hex}");
        cluster.join(&[seed_addr]);
    } else {
        eprintln!("no SEED_ADDR set — listening for incoming connections");
    }

    // Spawn InferenceActor + RequestBridge on the cluster runtime.
    let status_inbox = cluster.rt.new_inbox::<InferenceActorStatus>().unwrap();
    let sender = cluster.rt.create_sender();
    let actor =
        InferenceActor::new(worker_spec(), sender).with_status_addr(*status_inbox.addr());
    let inference_addr = cluster.rt.spawn(actor).unwrap();

    let bridge = RequestBridge { target: inference_addr };
    let bridge_addr = cluster.rt.spawn(bridge).unwrap();

    // Register "inference" name in the cluster (gossiped to peers).
    cluster.register_name("inference", bridge_addr);
    eprintln!("registered name 'inference' -> bridge {:?}", bridge_addr);

    // App-level return routes for `reply_to` addresses are registered
    // dynamically (as requests arrive) on the cluster's shared transport
    // router — the same one the distribution protocol actors use. Per-actor
    // routes here are the InferenceResponse return path; they don't collide
    // with the protocol's route-view egress.
    let router = Arc::clone(&cluster.transport_router);

    // Wait for worker to be ready (model download + load can take minutes)
    let start = Instant::now();
    let mut worker_ready = false;
    while start.elapsed() < Duration::from_secs(600) {
        cluster.pump_once();
        if let Some(status) = status_inbox.try_recv() {
            match status {
                InferenceActorStatus::WorkerReady { pid } => {
                    eprintln!("worker ready (pid: {:?})", pid);
                    worker_ready = true;
                    break;
                }
                InferenceActorStatus::ProcessStarted => {
                    eprintln!("worker process started");
                }
                InferenceActorStatus::ProcessExited { status } => {
                    eprintln!("worker exited during startup: {:?}", status);
                    std::process::exit(1);
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if !worker_ready {
        eprintln!("worker did not become ready within 600s");
        std::process::exit(1);
    }

    // Main loop
    eprintln!("entering main loop");
    loop {
        cluster.pump_once();

        // Drain incoming actor messages and collect reply_to addresses
        let reply_addrs = drain_and_collect_reply_addrs(
            &cluster.driver,
            &cluster.codecs,
            &cluster.rt,
            Duration::from_millis(50),
        );

        // Dynamically register transport routes for reply_to addresses.
        // These addresses live on the remote orchestrator node — we need a
        // transport route so InferenceActor can send InferenceResponse back.
        for reply_addr in reply_addrs {
            // Find the peer to route back to. With a single orchestrator peer,
            // we route all reply addresses to the only alive member.
            let snap = cluster.snapshot();
            for member in &snap.members {
                if member.state == "alive" && member.node_id.len() == 64 {
                    let peer_bytes = parse_hex_node_id(&member.node_id);
                    if let Ok(peer_key) = iroh::PublicKey::from_bytes(&peer_bytes) {
                        let peer_addr = iroh::EndpointAddr::from(peer_key);
                        let transport = Arc::new(IrohActorTransport::new(
                            cluster.driver.endpoint().clone(),
                            peer_addr,
                            cluster.driver.tokio_handle(),
                        ));
                        router.add_route(reply_addr, transport);
                        eprintln!("added return route for {:?}", reply_addr);
                    }
                }
            }
        }

        // Re-tick so the InferenceActor's outbound responses (routed via the
        // shared transport router) get delivered.
        cluster.rt.tick();

        // Check worker health — log but don't exit, so SWIM stays alive
        // for diagnostics when the worker crashes
        if let Some(status) = status_inbox.try_recv() {
            match status {
                InferenceActorStatus::ProcessExited { status } => {
                    eprintln!("worker process exited: {:?}", status);
                    eprintln!("keeping main loop alive for diagnostics");
                }
                other => {
                    eprintln!("worker status: {:?}", other);
                }
            }
        }

        std::thread::sleep(Duration::from_millis(10));
    }
}
