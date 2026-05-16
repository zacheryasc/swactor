//! pp-gpu-node — pipeline-parallel GPU inference node.
//!
//! Boots one stage of a pipeline-parallel inference run. Reads its
//! configuration from the environment (set at instance-create time on
//! vast.ai, or by the orchestrator on localhost):
//!
//! * `STAGE`         — this stage's 0-based index.
//! * `NUM_STAGES`    — total number of stages in the pipeline.
//! * `SEED_ADDR`     — hex node id of the seed (orchestrator) to join.
//! * `SEED_RELAY`    — optional iroh relay URL for WAN traversal.
//! * `SEED_DIRECT`   — optional comma-separated `ip:port` direct addrs.
//! * `MAX_TOKENS`    — optional cap for stage-1's decode loop (default 64).
//! * `MODEL`         — optional model identifier passed through to the worker.
//! * `WORKER_CMD`    — optional python interpreter (default `python3`).
//! * `WORKER_SCRIPT` — optional worker script path (default `./pp_tinygrad_worker.py`).
//! * `PP_WORKER_STUB`/`PYTHON` — pass-through env to the worker.
//!
//! Lifecycle:
//!
//! 1. Create an iroh driver and join the seed.
//! 2. Wait for SWIM convergence to at least 1 alive peer.
//! 3. Spawn the appropriate stage actor with a placeholder neighbour
//!    address. Register the stage's per-index SWIM name immediately so
//!    the neighbour stage can resolve it.
//! 4. Resolve neighbour names (and, on stage 1, the orchestrator name)
//!    via SWIM. Send a `SetNextStage` / `SetNeighbors` setup message to
//!    inject the real addresses into the actor.
//! 5. Register `pp-entry` / `pp-exit` so the orchestrator can submit a
//!    request and so the stage names are complete.
//! 6. Enter the pump loop: drain iroh messages, tick the runtime.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::actor::ActorAddress;
use swactor::runtime::{Runtime, RuntimeConfig};
use swactor::transport::{CodecRegistry, TransportRouter};

use pipeline_parallel_inference::iroh_transport::{
    ActorMessagePump, IrohActorTransport, ACTOR_ALPN,
};
use pipeline_parallel_inference::messages::inference_codec_registry;
use pipeline_parallel_inference::stage_actor::{
    Stage0Actor, Stage0Msg, Stage0NextTokenBridge, Stage0RequestBridge,
    Stage1ActivationBridge, Stage1Actor, Stage1Msg, StageActorStatus,
};
use pipeline_parallel_inference::topology::{
    next_stage_name, prev_stage_name, stage_name, ENTRY_NAME, EXIT_NAME,
};
use swactor_process::{ProcessMode, ProcessSpec};

/// SWIM name the orchestrator uses to publish the address of its
/// `InferenceResponse` inbox. Stage 1 resolves this name to learn where
/// to send the final response. Defined here (and re-declared in
/// `pp-smoke-run`) so the topology module stays test-shaped; the binary
/// is the only place that cares about this name.
const ORCHESTRATOR_NAME: &str = "pp-orchestrator";

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

fn require_env(name: &str) -> String {
    std::env::var(name)
        .unwrap_or_else(|_| {
            eprintln!("pp-gpu-node: env {name} is required");
            std::process::exit(2);
        })
        .trim()
        .to_string()
}

fn require_u32(name: &str) -> u32 {
    let raw = require_env(name);
    raw.parse::<u32>().unwrap_or_else(|_| {
        eprintln!("pp-gpu-node: env {name}={raw:?} must be a u32");
        std::process::exit(2);
    })
}

fn node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: 10,
            probe_timeout: 15,
            indirect_probes: 2,
            suspicion_timeout: 60,
            dead_reprobe_interval: 100,
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        republish_interval: 50,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn worker_spec(stage: u32, num_stages: u32) -> ProcessSpec {
    let cmd = std::env::var("WORKER_CMD").unwrap_or_else(|_| "python3".into());
    let script = std::env::var("WORKER_SCRIPT")
        .unwrap_or_else(|_| "./pp_tinygrad_worker.py".into());

    let mut env = HashMap::new();
    env.insert("STAGE".into(), stage.to_string());
    env.insert("NUM_STAGES".into(), num_stages.to_string());
    if let Ok(val) = std::env::var("PP_WORKER_STUB") {
        env.insert("PP_WORKER_STUB".into(), val);
    }
    if let Ok(val) = std::env::var("PYTHON") {
        env.insert("PYTHON".into(), val);
    }
    if let Ok(val) = std::env::var("MODEL") {
        env.insert("MODEL".into(), val);
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

/// Wait until SWIM reports at least one alive peer, ticking driver +
/// receiving messages. Returns true on success, false on timeout.
fn wait_for_cluster(driver: &mut IrohDriver, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        driver.recv();
        driver.tick();
        let snap = driver.snapshot();
        if snap.members.iter().any(|m| m.state == "alive") {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Resolve a SWIM name, ticking the driver in between attempts. Returns
/// `(addr, node_id_hex)` on success; the hex node id is what callers
/// turn into an `iroh::PublicKey` for transport routing.
fn resolve_name(
    driver: &mut IrohDriver,
    name: &str,
    timeout: Duration,
) -> Option<(ActorAddress, String)> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        driver.recv();
        driver.tick();
        if let Some((addr, node_id)) = driver.node().resolve_name(name) {
            let hex: String = node_id.0.iter().map(|b| format!("{:02x}", b)).collect();
            return Some((addr, hex));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    None
}

/// Build an `IrohActorTransport` for sending messages to a remote node
/// identified by its hex node id, using the local driver's endpoint.
fn build_route(
    driver: &IrohDriver,
    node_hex: &str,
) -> Result<Arc<IrohActorTransport>, String> {
    let bytes = parse_hex_node_id(node_hex);
    let key = PublicKey::from_bytes(&bytes)
        .map_err(|e| format!("invalid peer node id {node_hex}: {e}"))?;
    let addr = iroh::EndpointAddr::from(key);
    Ok(Arc::new(IrohActorTransport::new(
        driver.endpoint().clone(),
        addr,
        driver.tokio_handle(),
    )))
}

fn register_name(driver: &mut IrohDriver, name: &str, addr: ActorAddress) {
    driver.node_mut().register_name(name.into(), addr);
    eprintln!("pp-gpu-node: registered {name} -> {addr:?}");
}

fn main() {
    let stage = require_u32("STAGE");
    let num_stages = require_u32("NUM_STAGES");
    if num_stages == 0 || stage >= num_stages {
        eprintln!(
            "pp-gpu-node: STAGE={stage} out of range for NUM_STAGES={num_stages}"
        );
        std::process::exit(2);
    }
    // This plan implements two stages only. Larger N is out of scope.
    if num_stages != 2 {
        eprintln!(
            "pp-gpu-node: this plan supports NUM_STAGES=2 only, got {num_stages}"
        );
        std::process::exit(2);
    }

    let seed_hex = require_env("SEED_ADDR");
    let max_tokens: u32 = std::env::var("MAX_TOKENS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(64);

    // Relay mode: `Default` when a relay URL is provided (vast.ai / WAN);
    // `Disabled` when only direct addresses are given (localhost).
    let seed_relay_env = std::env::var("SEED_RELAY").ok();
    let relay_mode = if seed_relay_env.is_some() {
        RelayMode::Default
    } else {
        RelayMode::Disabled
    };

    let mut driver = IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode,
        node: node_config(),
        peer_auth: None,
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    })
    .expect("failed to create iroh driver");

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let direct_addrs: Vec<String> = driver
        .direct_addresses()
        .iter()
        .map(|sa| sa.to_string())
        .collect();
    eprintln!(
        "pp-gpu-node: stage {stage}/{num_stages} started (node_id: {my_hex})"
    );
    // PP_GPU_NODE_ADDR is printed to stdout (flushed) so a parent process
    // capturing this child's stdout can extract our addressing. The orchestrator
    // uses this to pass each stage's direct addresses to the other stage so
    // peer-to-peer iroh dials work without needing a relay.
    println!("PP_GPU_NODE_ADDR {my_hex} {}", direct_addrs.join(","));
    use std::io::Write;
    let _ = std::io::stdout().flush();

    // Build the seed endpoint and join.
    let seed_bytes = parse_hex_node_id(&seed_hex);
    let seed_key =
        PublicKey::from_bytes(&seed_bytes).expect("invalid seed public key");
    let mut seed_addr = iroh::EndpointAddr::from(seed_key);
    if let Some(relay) = seed_relay_env.as_deref() {
        if let Ok(relay_url) = relay.trim().parse::<iroh::RelayUrl>() {
            eprintln!("pp-gpu-node: using seed relay {relay}");
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
    let mut join_targets = vec![seed_addr];

    // PEER_NODE_ID + PEER_DIRECT optionally provide a sibling stage's full
    // endpoint address. When the orchestrator spawns stage 1 it sets these
    // to stage 0's addressing, so stage 1's iroh dials stage 0 here — that
    // outbound dial causes stage 0's iroh to learn stage 1's source-socket
    // addresses, so both peers know each other for actor-message dials.
    if let (Ok(peer_hex), Ok(peer_direct)) =
        (std::env::var("PEER_NODE_ID"), std::env::var("PEER_DIRECT"))
    {
        let peer_hex = peer_hex.trim();
        let peer_direct = peer_direct.trim();
        if !peer_hex.is_empty() && !peer_direct.is_empty() {
            let peer_bytes = parse_hex_node_id(peer_hex);
            let peer_key =
                PublicKey::from_bytes(&peer_bytes).expect("invalid peer node id");
            let mut peer_addr = iroh::EndpointAddr::from(peer_key);
            for part in peer_direct.split(',') {
                if let Ok(sa) = part.trim().parse::<SocketAddr>() {
                    peer_addr = peer_addr.with_ip_addr(sa);
                }
            }
            eprintln!("pp-gpu-node: also joining peer {peer_hex}");
            join_targets.push(peer_addr);
        }
    }

    eprintln!("pp-gpu-node: joining seed {seed_hex}");
    driver.join(&join_targets);

    if !wait_for_cluster(&mut driver, Duration::from_secs(120)) {
        eprintln!("pp-gpu-node: cluster did not converge in 120s");
        std::process::exit(1);
    }
    eprintln!("pp-gpu-node: cluster converged");

    // Create the actor runtime, codec registry, and transport router.
    let mut rt = Runtime::new(RuntimeConfig::default());
    let codecs = Arc::new(inference_codec_registry());
    let router = Arc::new(TransportRouter::new());
    rt.set_codec_registry(codecs.clone());
    rt.set_transport_router(router.clone());

    let sender = rt.create_sender();
    let status_inbox = rt.new_inbox::<StageActorStatus>().unwrap();

    if stage == 0 {
        run_stage_0(
            driver, rt, codecs, router, sender, status_inbox, num_stages, max_tokens,
        );
    } else {
        run_stage_1(
            driver, rt, codecs, router, sender, status_inbox, stage, num_stages, max_tokens,
        );
    }
}

fn wait_for_worker_ready(
    rt: &Runtime,
    driver: &mut IrohDriver,
    status_inbox: &swactor::runtime::Inbox<StageActorStatus>,
    timeout: Duration,
) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        rt.tick();
        driver.recv();
        driver.tick();
        if let Some(status) = status_inbox.try_recv() {
            match status {
                StageActorStatus::WorkerReady { pid } => {
                    eprintln!("pp-gpu-node: worker ready (pid: {pid:?})");
                    return true;
                }
                StageActorStatus::ProcessStarted => {
                    eprintln!("pp-gpu-node: worker process started");
                }
                StageActorStatus::ProcessExited { status } => {
                    eprintln!(
                        "pp-gpu-node: worker exited during startup: {status:?}"
                    );
                    return false;
                }
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    false
}

fn pump(
    driver: &mut IrohDriver,
    rt: &Runtime,
    codecs: &CodecRegistry,
    msg_pump: &ActorMessagePump,
    duration: Duration,
) {
    let end = Instant::now() + duration;
    while Instant::now() < end {
        driver.recv();
        driver.tick();
        msg_pump.pump(driver, codecs, rt);
        rt.tick();
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn run_stage_0(
    mut driver: IrohDriver,
    rt: Runtime,
    codecs: Arc<CodecRegistry>,
    router: Arc<TransportRouter>,
    sender: swactor::runtime::ExternalSender,
    status_inbox: swactor::runtime::Inbox<StageActorStatus>,
    num_stages: u32,
    _max_tokens: u32,
) {
    // Construct actor with a placeholder next_stage_addr. SetNextStage
    // overwrites this once pp-stage-1 is resolvable. In real-mode (no
    // PP_WORKER_STUB=1) we route tokenization through the worker; the
    // stub-mode whitespace splitter would produce synthetic ids that the
    // real GGUF embed lookup cannot handle.
    let placeholder = ActorAddress([0; 32]);
    let stub_mode = std::env::var("PP_WORKER_STUB")
        .map(|v| v.trim() == "1")
        .unwrap_or(false);
    let mut actor = Stage0Actor::new(worker_spec(0, num_stages), sender, placeholder)
        .with_status_addr(*status_inbox.addr());
    if !stub_mode {
        actor = actor.with_real_tokenization();
    }
    let stage0_actor_addr = rt.spawn(actor).unwrap();

    let request_bridge = Stage0RequestBridge { target: stage0_actor_addr };
    let request_bridge_addr = rt.spawn(request_bridge).unwrap();

    let nt_bridge = Stage0NextTokenBridge { target: stage0_actor_addr };
    let nt_bridge_addr = rt.spawn(nt_bridge).unwrap();

    // Register pp-stage-0 IMMEDIATELY so stage 1 can resolve us. Defer
    // pp-entry until our neighbour is wired up — that way the orchestrator
    // cannot send InferenceRequest before we are ready to forward it.
    register_name(&mut driver, &stage_name(0), nt_bridge_addr);

    // Worker boot can take time even in stub mode (Python startup +
    // tinygrad import on real mode). Generous timeout.
    if !wait_for_worker_ready(&rt, &mut driver, &status_inbox, Duration::from_secs(600))
    {
        eprintln!("pp-gpu-node: stage-0 worker did not become ready");
        std::process::exit(1);
    }

    // Resolve pp-stage-1 — stage 1 must have registered by now (or will
    // shortly; SWIM gossip is fast on localhost).
    let next_name = next_stage_name(0, num_stages)
        .expect("stage 0 has a next neighbour in a 2-stage pipeline");
    eprintln!("pp-gpu-node: resolving {next_name}...");
    let (stage1_addr, stage1_node_hex) =
        resolve_name(&mut driver, &next_name, Duration::from_secs(120))
            .unwrap_or_else(|| {
                eprintln!("pp-gpu-node: failed to resolve {next_name} in 120s");
                std::process::exit(1);
            });
    eprintln!("pp-gpu-node: resolved {next_name} -> {stage1_addr:?} on {stage1_node_hex}");

    // Wire a transport route so outbound StageActivations can leave the process.
    match build_route(&driver, &stage1_node_hex) {
        Ok(t) => router.add_route(stage1_addr, t),
        Err(e) => {
            eprintln!("pp-gpu-node: route to stage 1 failed: {e}");
            std::process::exit(1);
        }
    }

    // Inject the real address into the actor and let it land.
    rt.send_to(stage0_actor_addr, Stage0Msg::SetNextStage(stage1_addr))
        .expect("send SetNextStage");
    let msg_pump = ActorMessagePump::new();
    pump(&mut driver, &rt, &codecs, &msg_pump, Duration::from_millis(100));

    // Now register pp-entry — the orchestrator can finally find us. Doing
    // this last avoids any race where an InferenceRequest arrives before
    // the actor knows where to forward the activation.
    register_name(&mut driver, ENTRY_NAME, request_bridge_addr);

    main_pump(driver, rt, codecs, router, status_inbox, msg_pump);
}

fn run_stage_1(
    mut driver: IrohDriver,
    rt: Runtime,
    codecs: Arc<CodecRegistry>,
    router: Arc<TransportRouter>,
    sender: swactor::runtime::ExternalSender,
    status_inbox: swactor::runtime::Inbox<StageActorStatus>,
    stage: u32,
    num_stages: u32,
    max_tokens: u32,
) {
    let placeholder = ActorAddress([0; 32]);
    let stub_mode = std::env::var("PP_WORKER_STUB")
        .map(|v| v.trim() == "1")
        .unwrap_or(false);
    let mut actor = Stage1Actor::new(
        worker_spec(stage, num_stages),
        sender,
        placeholder,  // prev_stage — overwritten by SetNeighbors
        placeholder,  // reply_to   — overwritten by SetNeighbors
        max_tokens,
    )
    .with_status_addr(*status_inbox.addr());
    if !stub_mode {
        actor = actor.with_real_detokenization();
    }
    let stage1_actor_addr = rt.spawn(actor).unwrap();

    let activation_bridge = Stage1ActivationBridge { target: stage1_actor_addr };
    let bridge_addr = rt.spawn(activation_bridge).unwrap();

    // Register pp-stage-{stage} immediately so stage 0 can resolve us.
    // pp-exit is purely informational in the 2-stage MVP — register it
    // once we are fully wired up.
    register_name(&mut driver, &stage_name(stage), bridge_addr);

    if !wait_for_worker_ready(&rt, &mut driver, &status_inbox, Duration::from_secs(600))
    {
        eprintln!("pp-gpu-node: stage-{stage} worker did not become ready");
        std::process::exit(1);
    }

    // Resolve prev neighbour (pp-stage-0) and orchestrator name.
    let prev_name = prev_stage_name(stage)
        .expect("stage 1 has a prev neighbour in a 2-stage pipeline");
    eprintln!("pp-gpu-node: resolving {prev_name}...");
    let (prev_addr, prev_node_hex) =
        resolve_name(&mut driver, &prev_name, Duration::from_secs(120))
            .unwrap_or_else(|| {
                eprintln!("pp-gpu-node: failed to resolve {prev_name} in 120s");
                std::process::exit(1);
            });

    eprintln!("pp-gpu-node: resolving {ORCHESTRATOR_NAME}...");
    let (orch_addr, orch_node_hex) = resolve_name(
        &mut driver,
        ORCHESTRATOR_NAME,
        Duration::from_secs(120),
    )
    .unwrap_or_else(|| {
        eprintln!("pp-gpu-node: failed to resolve {ORCHESTRATOR_NAME} in 120s");
        std::process::exit(1);
    });
    eprintln!(
        "pp-gpu-node: resolved prev={prev_addr:?} on {prev_node_hex}, \
         orch={orch_addr:?} on {orch_node_hex}"
    );

    // Routes for both outbound destinations.
    match build_route(&driver, &prev_node_hex) {
        Ok(t) => router.add_route(prev_addr, t),
        Err(e) => {
            eprintln!("pp-gpu-node: route to stage 0 failed: {e}");
            std::process::exit(1);
        }
    }
    match build_route(&driver, &orch_node_hex) {
        Ok(t) => router.add_route(orch_addr, t),
        Err(e) => {
            eprintln!("pp-gpu-node: route to orchestrator failed: {e}");
            std::process::exit(1);
        }
    }

    rt.send_to(
        stage1_actor_addr,
        Stage1Msg::SetNeighbors {
            prev_stage: prev_addr,
            reply_to: orch_addr,
        },
    )
    .expect("send SetNeighbors");
    let msg_pump = ActorMessagePump::new();
    pump(&mut driver, &rt, &codecs, &msg_pump, Duration::from_millis(100));

    register_name(&mut driver, EXIT_NAME, bridge_addr);

    main_pump(driver, rt, codecs, router, status_inbox, msg_pump);
}

fn main_pump(
    mut driver: IrohDriver,
    rt: Runtime,
    codecs: Arc<CodecRegistry>,
    _router: Arc<TransportRouter>,
    status_inbox: swactor::runtime::Inbox<StageActorStatus>,
    msg_pump: ActorMessagePump,
) {
    eprintln!("pp-gpu-node: entering main pump loop");
    loop {
        driver.recv();
        driver.tick();
        msg_pump.pump(&driver, &codecs, &rt);
        rt.tick();

        if let Some(status) = status_inbox.try_recv() {
            match status {
                StageActorStatus::ProcessExited { status } => {
                    eprintln!("pp-gpu-node: worker exited: {status:?}");
                    eprintln!("pp-gpu-node: keeping SWIM alive for diagnostics");
                }
                other => eprintln!("pp-gpu-node: status: {other:?}"),
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
