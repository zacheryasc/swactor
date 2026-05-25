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
//! * `MAX_TOKENS`    — optional cap for the last stage's decode loop
//!                     (default 64).
//! * `MODEL`         — optional model identifier passed through to the worker.
//! * `WORKER_CMD`    — optional python interpreter (default `python3`).
//! * `WORKER_SCRIPT` — optional worker script path (default `./pp_tinygrad_worker.py`).
//! * `PP_WORKER_STUB`/`PYTHON` — pass-through env to the worker.
//!
//! Lifecycle:
//!
//! 1. Create an iroh driver and join the seed.
//! 2. Wait for SWIM convergence to at least 1 alive peer.
//! 3. Spawn the `StageActor` with placeholder neighbour addresses, plus
//!    the bridges its role needs. Register the stage's per-index SWIM
//!    name immediately so the neighbour stage can resolve it.
//! 4. Resolve neighbour names (and, on the last stage, the orchestrator
//!    name) via SWIM. Send `StageMsg::SetNeighbors` to inject the real
//!    addresses into the actor.
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

use distribution::diagnostics::Role as DiagRole;

use pipeline_parallel_inference::diag;
use pipeline_parallel_inference::iroh_transport::{
    ActorMessagePump, IrohActorTransport, ACTOR_ALPN,
};
use pipeline_parallel_inference::messages::inference_codec_registry;
use pipeline_parallel_inference::stage_actor::{
    ActivationBridge, NextTokenBridge, RequestBridge, StageActor, StageActorStatus,
    StageMsg, StageRole,
};
use pipeline_parallel_inference::topology::{
    next_stage_name, stage_name, ENTRY_NAME, EXIT_NAME,
};
use swactor_process::{ProcessMode, ProcessSpec};

/// SWIM name the orchestrator uses to publish the address of its
/// `InferenceResponse` inbox. The last stage resolves this name to learn
/// where to send the final response. Defined here (and re-declared in
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
///
/// In WAN mode (vast.ai) the bare public key alone is not enough for iroh
/// to reach the peer — its NodeMap may not yet hold the peer's relay URL
/// (SWIM probes populate it eventually but not deterministically for
/// every pair at convergence). We enrich the `EndpointAddr` with whatever
/// relay info we know: SWIM metadata gossip if available, else our own
/// home relay as a fallback. The n0 relay mesh routes by public key, so
/// a dial via any relay reaches the peer as long as both endpoints have
/// a home relay.
fn build_route(
    driver: &IrohDriver,
    node_hex: &str,
) -> Result<Arc<IrohActorTransport>, String> {
    let bytes = parse_hex_node_id(node_hex);
    let key = PublicKey::from_bytes(&bytes)
        .map_err(|e| format!("invalid peer node id {node_hex}: {e}"))?;
    let mut addr = iroh::EndpointAddr::from(key);
    let node_id = distribution::types::NodeId(bytes);
    let relay = driver
        .node()
        .relay_url(&node_id)
        .and_then(|s| s.parse::<iroh::RelayUrl>().ok())
        .or_else(|| driver.home_relay_url());
    if let Some(url) = relay {
        addr = addr.with_relay_url(url);
    }
    Ok(Arc::new(IrohActorTransport::new(
        driver.endpoint().clone(),
        addr,
        driver.tokio_handle(),
    )))
}

fn register_name(driver: &mut IrohDriver, name: &str, addr: ActorAddress, stage: u32) {
    driver.node_mut().register_name(name.into(), addr);
    diag::emit_register_name(driver, name, addr, Some(stage));
    eprintln!("pp-gpu-node: registered {name} -> {addr:?}");
}

fn resolve_or_die(
    driver: &mut IrohDriver,
    name: &str,
    timeout: Duration,
) -> (ActorAddress, String) {
    eprintln!("pp-gpu-node: resolving {name}...");
    resolve_name(driver, name, timeout).unwrap_or_else(|| {
        eprintln!(
            "pp-gpu-node: failed to resolve {name} in {:.0}s",
            timeout.as_secs_f32()
        );
        std::process::exit(1);
    })
}

fn add_route_or_die(
    driver: &IrohDriver,
    router: &TransportRouter,
    addr: ActorAddress,
    node_hex: &str,
    label: &str,
) {
    match build_route(driver, node_hex) {
        Ok(t) => router.add_route(addr, t),
        Err(e) => {
            eprintln!("pp-gpu-node: route to {label} failed: {e}");
            std::process::exit(1);
        }
    }
}

/// Ask the kernel to deliver `SIGTERM` to this process when its parent dies.
/// Without this, a `SIGKILL` to `pp-smoke-run` would orphan its children to
/// pid 1 and leave them running — the orchestrator's `ChainGuard::drop` runs
/// only on graceful exit. With it, each `pp-gpu-node` dies seconds after its
/// orchestrator does, which is the `binary_e2e_orchestrator_sigkilled_*`
/// contract from TEST_SPEC §13.2. Linux-only; other platforms are no-ops.
#[cfg(target_os = "linux")]
fn install_parent_death_signal() {
    // SAFETY: prctl is the standard way to set the parent-death signal on
    // Linux. No memory is read or written via the args, so this is safe to
    // call from any thread state.
    unsafe {
        libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGTERM as libc::c_ulong);
    }
}

#[cfg(not(target_os = "linux"))]
fn install_parent_death_signal() {}

/// Honour the test-only `PP_BOOT_DELAY_STAGE` / `PP_BOOT_DELAY_SECS` pair:
/// if our own stage matches, sleep before doing anything else. Used by the
/// §13.3 boot-order tests to simulate a slow-starting node without needing
/// to rebuild the binary for each scenario.
fn maybe_simulate_boot_delay(stage: u32) {
    let target = std::env::var("PP_BOOT_DELAY_STAGE").ok().and_then(|s| s.trim().parse::<u32>().ok());
    let secs = std::env::var("PP_BOOT_DELAY_SECS").ok().and_then(|s| s.trim().parse::<u64>().ok());
    if let (Some(target), Some(secs)) = (target, secs) {
        if target == stage && secs > 0 {
            eprintln!("pp-gpu-node: simulated boot delay of {secs}s on stage {stage}");
            std::thread::sleep(Duration::from_secs(secs));
        }
    }
}

fn main() {
    let stage = require_u32("STAGE");
    let num_stages = require_u32("NUM_STAGES");
    if num_stages < 2 || stage >= num_stages {
        eprintln!(
            "pp-gpu-node: invalid STAGE={stage} for NUM_STAGES={num_stages} \
             (need NUM_STAGES >= 2 and STAGE < NUM_STAGES; N=1 is not supported)"
        );
        std::process::exit(2);
    }
    install_parent_death_signal();
    maybe_simulate_boot_delay(stage);
    let role = StageRole::for_stage(stage, num_stages);

    let seed_hex = require_env("SEED_ADDR");
    let max_tokens: u32 = std::env::var("MAX_TOKENS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(64);

    // Relay mode: honor `SWACTOR_IROH_RELAY_URL` when set (typically points
    // at an operator-run iroh relay that avoids the canary cluster), else
    // `Default` when a relay URL is provided (vast.ai / WAN), else
    // `Disabled` when only direct addresses are given (localhost).
    let seed_relay_env = std::env::var("SEED_RELAY").ok();
    let custom_relay = pipeline_parallel_inference::relay_config::relay_mode_from_env();
    let relay_mode = match (
        matches!(custom_relay, RelayMode::Custom(_)),
        seed_relay_env.is_some(),
    ) {
        (true, _) => custom_relay,
        (false, true) => RelayMode::Default,
        (false, false) => RelayMode::Disabled,
    };

    let mut driver = IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode,
        node: node_config(),
        peer_auth: None,
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    })
    .expect("failed to create iroh driver");

    // Install diagnostics from SWACTOR_DIAG_* env vars if the collector
    // URL is set. The returned handle is intentionally leaked: a stage
    // process runs until its parent kills it, and finalizing per-stage
    // would race the orchestrator's authoritative finalize record. The
    // background drainer keeps streaming events until SIGKILL.
    let _diag = diag::install_from_env(&mut driver, DiagRole::stage());
    let subprocess_introspect = _diag
        .as_ref()
        .map(|d| d.subprocess_introspect().clone());

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let direct_addrs: Vec<String> = driver
        .direct_addresses()
        .iter()
        .map(|sa| sa.to_string())
        .collect();
    eprintln!(
        "pp-gpu-node: stage {stage}/{num_stages} ({role:?}) started (node_id: {my_hex})"
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
    // endpoint address. When the orchestrator spawns a successor stage it
    // sets these to the predecessor stage's addressing, so the successor's
    // iroh dials the predecessor — that outbound dial causes the
    // predecessor's iroh to learn the successor's source-socket addresses,
    // so both peers know each other for actor-message dials.
    let add_peer_from_env = |targets: &mut Vec<iroh::EndpointAddr>,
                              hex_var: &str,
                              direct_var: &str,
                              label: &str| {
        if let (Ok(peer_hex), Ok(peer_direct)) =
            (std::env::var(hex_var), std::env::var(direct_var))
        {
            let peer_hex = peer_hex.trim();
            let peer_direct = peer_direct.trim();
            if peer_hex.is_empty() || peer_direct.is_empty() {
                return;
            }
            let peer_bytes = parse_hex_node_id(peer_hex);
            let peer_key = PublicKey::from_bytes(&peer_bytes)
                .expect("invalid peer node id");
            let mut peer_addr = iroh::EndpointAddr::from(peer_key);
            for part in peer_direct.split(',') {
                if let Ok(sa) = part.trim().parse::<SocketAddr>() {
                    peer_addr = peer_addr.with_ip_addr(sa);
                }
            }
            eprintln!("pp-gpu-node: also joining {label} {peer_hex}");
            targets.push(peer_addr);
        }
    };

    // Predecessor stage's addressing (set by the orchestrator's
    // spawn_chain). Every non-first stage gets this; dialing the
    // predecessor populates the predecessor's NodeMap with our
    // source-socket reverse path so forward sends i-1 → i work.
    add_peer_from_env(&mut join_targets, "PEER_NODE_ID", "PEER_DIRECT", "peer");
    // Stage 0's addressing. Every non-first stage also gets this so the
    // last → first feedback edge has addressing on both endpoints at
    // N ≥ 3. For stage 1 it duplicates PEER_*, which iroh dedupes.
    add_peer_from_env(
        &mut join_targets,
        "FIRST_PEER_NODE_ID",
        "FIRST_PEER_DIRECT",
        "first-stage peer",
    );

    eprintln!("pp-gpu-node: joining seed {seed_hex}");
    driver.join(&join_targets);

    // If we ended up with a relay (vast.ai / WAN), publish it via SWIM
    // metadata gossip so every other stage learns our relay URL without
    // having to dial us first. `build_route` later reads this through
    // `driver.node().relay_url(...)` to enrich the EndpointAddr with the
    // peer's home relay — without that, an N≥3 vast.ai run can stall on
    // the autoregressive feedback edge (last → first) because SWIM has
    // not yet probed that specific pair.
    if let Some(home) = driver.home_relay_url() {
        eprintln!("pp-gpu-node: publishing home relay {home} to SWIM gossip");
        driver.node_mut().set_relay_url(Some(home.to_string()));
    }

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

    run_stage(
        driver, rt, codecs, router, sender, status_inbox, role, stage, num_stages,
        max_tokens, subprocess_introspect,
    );
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

/// Boot one stage, branching on `role`. The shared boot pieces (worker
/// spawn, name registration, route wiring) are N-generic; only the
/// neighbour-resolution step differs per role, per SPEC §5.1.
#[allow(clippy::too_many_arguments)]
fn run_stage(
    mut driver: IrohDriver,
    rt: Runtime,
    codecs: Arc<CodecRegistry>,
    router: Arc<TransportRouter>,
    sender: swactor::runtime::ExternalSender,
    status_inbox: swactor::runtime::Inbox<StageActorStatus>,
    role: StageRole,
    stage: u32,
    num_stages: u32,
    max_tokens: u32,
    subprocess_introspect: Option<Arc<distribution::diagnostics::subprocess_introspect::SubprocessIntrospect>>,
) {
    // Construct the role-appropriate actor with placeholder routing
    // addresses. SetNeighbors overwrites them once SWIM resolution
    // succeeds.
    let placeholder = ActorAddress([0; 32]);
    let stub_mode = std::env::var("PP_WORKER_STUB")
        .map(|v| v.trim() == "1")
        .unwrap_or(false);

    let diag_emitter = driver.diagnostics().clone();
    let attach_subprocess = |mut a: StageActor| {
        if let Some(intro) = subprocess_introspect.as_ref() {
            a = a.with_subprocess_introspect(intro.clone());
        }
        a
    };
    let actor = match role {
        StageRole::First => {
            let mut a =
                StageActor::first(worker_spec(stage, num_stages), sender, placeholder)
                    .with_status_addr(*status_inbox.addr())
                    .with_diagnostics(diag_emitter.clone())
                    .with_stage_idx(stage);
            if !stub_mode {
                a = a.with_real_tokenization();
            }
            attach_subprocess(a)
        }
        StageRole::Middle => attach_subprocess(
            StageActor::middle(worker_spec(stage, num_stages), sender, placeholder)
                .with_status_addr(*status_inbox.addr())
                .with_diagnostics(diag_emitter.clone())
                .with_stage_idx(stage),
        ),
        StageRole::Last => {
            let mut a = StageActor::last(
                worker_spec(stage, num_stages),
                sender,
                placeholder,
                placeholder,
                max_tokens,
            )
            .with_status_addr(*status_inbox.addr())
            .with_diagnostics(diag_emitter.clone())
            .with_stage_idx(stage);
            if !stub_mode {
                a = a.with_real_detokenization();
            }
            attach_subprocess(a)
        }
    };
    let stage_actor_addr = rt.spawn(actor).unwrap();

    // Bridges are wired per role:
    //   First:        RequestBridge (entry) + NextTokenBridge (per-index).
    //   Middle, Last: ActivationBridge (per-index).
    //   Last also re-uses its ActivationBridge as pp-exit.
    let request_bridge_addr = if role == StageRole::First {
        Some(
            rt.spawn(RequestBridge {
                target: stage_actor_addr,
            })
            .unwrap(),
        )
    } else {
        None
    };
    let next_token_bridge_addr = if role == StageRole::First {
        Some(
            rt.spawn(NextTokenBridge {
                target: stage_actor_addr,
            })
            .unwrap(),
        )
    } else {
        None
    };
    let activation_bridge_addr = if role != StageRole::First {
        Some(
            rt.spawn(ActivationBridge {
                target: stage_actor_addr,
            })
            .unwrap(),
        )
    } else {
        None
    };

    // Register pp-stage-{stage} IMMEDIATELY so neighbouring stages can
    // resolve us. The per-index bridge is the role's *inbound-from-network*
    // adapter:
    //   First:        receives NextTokens from Last  → NextTokenBridge.
    //   Middle, Last: receives StageActivations      → ActivationBridge.
    let per_index_bridge_addr = match role {
        StageRole::First => next_token_bridge_addr.unwrap(),
        StageRole::Middle | StageRole::Last => activation_bridge_addr.unwrap(),
    };
    register_name(&mut driver, &stage_name(stage), per_index_bridge_addr, stage);

    // Worker boot can take time even in stub mode (Python startup +
    // tinygrad import on real mode). Generous timeout.
    if !wait_for_worker_ready(&rt, &mut driver, &status_inbox, Duration::from_secs(600)) {
        eprintln!("pp-gpu-node: stage-{stage} worker did not become ready");
        // The StageActor already emitted Custom("worker_exited") in
        // response to ProcessNotification::Exited. Give the HTTP-sink
        // drainer enough time to flush it before we tear the process
        // down — the bundle is otherwise the only place this signal
        // lands, and on vast.ai the container is destroyed immediately
        // after exit so stderr is unreachable. The sink's default
        // batch interval is 1s, so we wait two batches' worth.
        std::thread::sleep(Duration::from_millis(2_500));
        std::process::exit(1);
    }

    // Resolve neighbours and wire routes per role. Stage 3 keeps the
    // 2-stage resolution targets in place (Last looks up stage 0 as its
    // NextToken sink, which happens to be First in N=2).
    match role {
        StageRole::First => {
            let next_name = next_stage_name(stage, num_stages)
                .expect("first stage has a next neighbour for N>=2");
            let (next_addr, next_hex) =
                resolve_or_die(&mut driver, &next_name, Duration::from_secs(120));
            eprintln!(
                "pp-gpu-node: resolved {next_name} -> {next_addr:?} on {next_hex}"
            );
            add_route_or_die(&driver, &router, next_addr, &next_hex, &next_name);
            rt.send_to(
                stage_actor_addr,
                StageMsg::SetNeighbors {
                    prev_stage: None,
                    next_stage: Some(next_addr),
                    reply_to: None,
                },
            )
            .expect("send SetNeighbors (first)");
        }
        StageRole::Middle => {
            let next_name = next_stage_name(stage, num_stages)
                .expect("middle stage has a next neighbour");
            let (next_addr, next_hex) =
                resolve_or_die(&mut driver, &next_name, Duration::from_secs(120));
            eprintln!(
                "pp-gpu-node: resolved {next_name} -> {next_addr:?} on {next_hex}"
            );
            add_route_or_die(&driver, &router, next_addr, &next_hex, &next_name);
            rt.send_to(
                stage_actor_addr,
                StageMsg::SetNeighbors {
                    prev_stage: None,
                    next_stage: Some(next_addr),
                    reply_to: None,
                },
            )
            .expect("send SetNeighbors (middle)");
        }
        StageRole::Last => {
            // Autoregressive feedback edge: NextToken always returns to
            // stage 0 (the First, the only stage that owns the embed
            // step), regardless of N. At N=2 that is the literal prev
            // neighbour; at N>=3 it skips any Middle stages on the wire.
            let feedback_name = stage_name(0);
            let (feedback_addr, feedback_hex) = resolve_or_die(
                &mut driver,
                &feedback_name,
                Duration::from_secs(120),
            );
            let (orch_addr, orch_hex) =
                resolve_or_die(&mut driver, ORCHESTRATOR_NAME, Duration::from_secs(120));
            eprintln!(
                "pp-gpu-node: resolved {feedback_name}={feedback_addr:?} on \
                 {feedback_hex}, orch={orch_addr:?} on {orch_hex}"
            );
            add_route_or_die(
                &driver,
                &router,
                feedback_addr,
                &feedback_hex,
                &feedback_name,
            );
            add_route_or_die(&driver, &router, orch_addr, &orch_hex, ORCHESTRATOR_NAME);
            rt.send_to(
                stage_actor_addr,
                StageMsg::SetNeighbors {
                    prev_stage: Some(feedback_addr),
                    next_stage: None,
                    reply_to: Some(orch_addr),
                },
            )
            .expect("send SetNeighbors (last)");
        }
    }

    // Let SetNeighbors land before publishing entry / exit names.
    let msg_pump = ActorMessagePump::new();
    pump(&mut driver, &rt, &codecs, &msg_pump, Duration::from_millis(100));

    // Register pp-entry on First (the orchestrator can finally submit
    // requests) and pp-exit on Last (informational). Middle has neither.
    match role {
        StageRole::First => {
            register_name(&mut driver, ENTRY_NAME, request_bridge_addr.unwrap(), stage);
        }
        StageRole::Last => {
            register_name(&mut driver, EXIT_NAME, activation_bridge_addr.unwrap(), stage);
        }
        StageRole::Middle => {}
    }

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
