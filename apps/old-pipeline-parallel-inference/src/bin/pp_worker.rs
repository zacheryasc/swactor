//! pp-worker — pipeline-parallel GPU inference node.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use datastream::DATASTREAM_SINK_NAME;
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode, SecretKey};
use iroh_driver::IrohDriverConfig;

use swactor::actor::ActorAddress;
use swactor::runtime::Runtime;
use swactor_transport::TransportRouter;

use pipeline_parallel_inference::cluster::ClusterNode;
use pipeline_parallel_inference::fleet;

use pipeline_parallel_inference::iroh_transport::{
    ACTOR_ALPN, ActorMessagePump, IrohActorTransport,
};
use pipeline_parallel_inference::messages::inference_codec_registry;
use pipeline_parallel_inference::stage_actor::{
    ActivationBridge, NextTokenBridge, RequestBridge, StageActor, StageActorStatus, StageMsg,
    StageRole,
};
use pipeline_parallel_inference::topology::{ENTRY_NAME, EXIT_NAME, next_stage_name, stage_name};
use swactor_process::{ProcessMode, ProcessSpec};

/// SWIM name the orchestrator uses to publish the address of its
/// `InferenceResponse` inbox. The last stage resolves this name to learn
/// where to send the final response. Defined here (and re-declared in
/// `pp-orchestrator`) so the topology module stays test-shaped; the binary
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
            eprintln!("pp-worker: env {name} is required");
            std::process::exit(2);
        })
        .trim()
        .to_string()
}

fn require_u32(name: &str) -> u32 {
    let raw = require_env(name);
    raw.parse::<u32>().unwrap_or_else(|_| {
        eprintln!("pp-worker: env {name}={raw:?} must be a u32");
        std::process::exit(2);
    })
}

/// A held cluster's stages must keep a STABLE node id across a restart,
/// or the pipeline name registry (pp-entry / pp-stage-N) keeps routing
/// to the dead pre-restart id and the response never returns.
/// `PP_STAGE_SECRET` (64 hex = 32 bytes) pins this stage's keypair; it is
/// injected at instance-create time, so it is re-read from PID 1's env on
/// every restart and the stage id is unchanged. Unset → random identity
/// (fine for a one-shot localhost `--seed` run).
fn stage_secret_from_env() -> Option<SecretKey> {
    let hex = std::env::var("PP_STAGE_SECRET").ok()?;
    let hex = hex.trim();
    if hex.is_empty() {
        return None;
    }
    if hex.len() != 64 {
        eprintln!(
            "pp-worker: PP_STAGE_SECRET must be 64 hex chars, got {}",
            hex.len()
        );
        std::process::exit(2);
    }
    let mut bytes = [0u8; 32];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap_or_else(|_| {
            eprintln!("pp-worker: PP_STAGE_SECRET is not valid hex");
            std::process::exit(2);
        });
    }
    Some(SecretKey::from_bytes(&bytes))
}

fn node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            // Probe one peer every 200 ms. Old config: 10 ticks at the
            // implicit ~20 ms tick = 200 ms.
            probe_interval: Duration::from_millis(200),
            indirect_probes: 2,
            // Periodically reprobe dead peers every ~2 s (old: 100 ticks).
            dead_reprobe_interval: Duration::from_secs(2),
            // Raised above SwimConfig::default() (15 s / 45 s) because
            // relay-mediated iroh paths were declaring peers Dead too eagerly.
            // Doubled to give each probe phase more relay-recovery slack while
            // preserving the 1:3 probe:suspicion ratio. Effective
            // time-to-Dead = 2*probe_timeout + suspicion_timeout = 30 + 30 +
            // 90 = 150 s. Do NOT shrink toward the old 15 / 60 pin (300 ms
            // probe budget on a 200-405 ms relay path) — that was the
            // 1779733878 flap cause.
            probe_timeout: Duration::from_secs(30),
            suspicion_timeout: Duration::from_secs(90),
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn worker_spec(stage: u32, num_stages: u32) -> ProcessSpec {
    let cmd = std::env::var("WORKER_CMD").unwrap_or_else(|_| "python3".into());
    let script =
        std::env::var("WORKER_SCRIPT").unwrap_or_else(|_| "./pp_tinygrad_worker.py".into());

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

/// Wait until SWIM reports at least one alive peer, pumping the cluster
/// in between attempts. Returns true on success, false on timeout.
fn wait_for_cluster(cluster: &mut ClusterNode, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        cluster.pump_once();
        if cluster.alive_count() > 0 {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Resolve a SWIM name, pumping the cluster in between attempts. Returns
/// `(addr, node_id_hex)` on success; the hex node id is what callers
/// turn into an `iroh::PublicKey` for transport routing.
fn resolve_name(
    cluster: &mut ClusterNode,
    name: &str,
    timeout: Duration,
) -> Option<(ActorAddress, String)> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        cluster.pump_once();
        if let Some((addr, node_id)) = cluster.resolve_name(name) {
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
fn build_route(cluster: &ClusterNode, node_hex: &str) -> Result<Arc<IrohActorTransport>, String> {
    let bytes = parse_hex_node_id(node_hex);
    let key = PublicKey::from_bytes(&bytes)
        .map_err(|e| format!("invalid peer node id {node_hex}: {e}"))?;
    let mut addr = iroh::EndpointAddr::from(key);
    let node_id = distribution::types::NodeId(bytes);
    let relay = cluster
        .peer_relay_url(node_id)
        .and_then(|s| s.parse::<iroh::RelayUrl>().ok())
        .or_else(|| cluster.driver.home_relay_url());
    if let Some(url) = relay {
        addr = addr.with_relay_url(url);
    }
    Ok(Arc::new(IrohActorTransport::new(
        cluster.driver.endpoint().clone(),
        addr,
        cluster.driver.tokio_handle(),
    )))
}

fn register_name(cluster: &ClusterNode, name: &str, addr: ActorAddress, _stage: u32) {
    cluster.register_name(name, addr);
    eprintln!("pp-worker: registered {name} -> {addr:?}");
}

fn resolve_or_die(
    cluster: &mut ClusterNode,
    name: &str,
    timeout: Duration,
) -> (ActorAddress, String) {
    eprintln!("pp-worker: resolving {name}...");
    resolve_name(cluster, name, timeout).unwrap_or_else(|| {
        eprintln!(
            "pp-worker: failed to resolve {name} in {:.0}s",
            timeout.as_secs_f32()
        );
        std::process::exit(1);
    })
}

fn add_route_or_die(
    cluster: &ClusterNode,
    router: &TransportRouter,
    addr: ActorAddress,
    node_hex: &str,
    label: &str,
) {
    match build_route(cluster, node_hex) {
        Ok(t) => router.add_route(addr, t),
        Err(e) => {
            eprintln!("pp-worker: route to {label} failed: {e}");
            std::process::exit(1);
        }
    }
}

/// Ask the kernel to deliver `SIGTERM` to this process when its parent dies.
/// Without this, a `SIGKILL` to `pp-orchestrator` would orphan its children to
/// pid 1 and leave them running — the orchestrator's `ChainGuard::drop` runs
/// only on graceful exit. With it, each `pp-worker` dies seconds after its
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

/// Set by the `SIGHUP` handler; polled by the pump loops to drive an
/// in-place worker hot-reload (re-exec the on-disk worker script). An
/// operator pushes a new `pp_tinygrad_worker.py` over the running one and
/// `kill -HUP $(pidof pp-worker)` to pick it up without re-leasing.
static RELOAD_REQUESTED: AtomicBool = AtomicBool::new(false);

#[cfg(target_os = "linux")]
extern "C" fn handle_sighup(_sig: libc::c_int) {
    // Async-signal-safe: the only work done here is a single atomic store.
    RELOAD_REQUESTED.store(true, Ordering::SeqCst);
}

/// Install the `SIGHUP` handler that requests a worker reload. Mirrors
/// `install_parent_death_signal`'s Linux-only pattern; the handler itself
/// only touches an atomic, so it is safe to run in signal context.
#[cfg(target_os = "linux")]
fn install_sighup_handler() {
    // SAFETY: registering a handler that performs only an async-signal-safe
    // atomic store. `signal()` keeps the handler installed across deliveries
    // under glibc (BSD semantics).
    unsafe {
        libc::signal(
            libc::SIGHUP,
            handle_sighup as *const () as libc::sighandler_t,
        );
    }
}

#[cfg(not(target_os = "linux"))]
fn install_sighup_handler() {}

/// If a `SIGHUP` reload was requested, clear the flag and ask the stage
/// actor to swap in a fresh worker. Shared by the startup hold and the main
/// pump so both honour reloads with the same latency.
fn drain_reload_request(rt: &Runtime, stage_actor_addr: ActorAddress) {
    if RELOAD_REQUESTED.swap(false, Ordering::SeqCst) {
        eprintln!("pp-worker: SIGHUP — reloading worker");
        let _ = rt.send_to(stage_actor_addr, StageMsg::ReloadWorker);
    }
}

/// Honour the test-only `PP_BOOT_DELAY_STAGE` / `PP_BOOT_DELAY_SECS` pair:
/// if our own stage matches, sleep before doing anything else. Used by the
/// §13.3 boot-order tests to simulate a slow-starting node without needing
/// to rebuild the binary for each scenario.
fn maybe_simulate_boot_delay(stage: u32) {
    let target = std::env::var("PP_BOOT_DELAY_STAGE")
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok());
    let secs = std::env::var("PP_BOOT_DELAY_SECS")
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok());
    if let (Some(target), Some(secs)) = (target, secs) {
        if target == stage && secs > 0 {
            eprintln!("pp-worker: simulated boot delay of {secs}s on stage {stage}");
            std::thread::sleep(Duration::from_secs(secs));
        }
    }
}

fn main() {
    pipeline_parallel_inference::profile::load_profile();
    let stage = require_u32("STAGE");
    let num_stages = require_u32("NUM_STAGES");
    if num_stages < 2 || stage >= num_stages {
        eprintln!(
            "pp-worker: invalid STAGE={stage} for NUM_STAGES={num_stages} \
             (need NUM_STAGES >= 2 and STAGE < NUM_STAGES; N=1 is not supported)"
        );
        std::process::exit(2);
    }
    install_parent_death_signal();
    install_sighup_handler();
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

    // Per-stage HTTP dashboards are intentionally disabled. Stage telemetry is
    // emitted as datastream frames to the orchestrator, where FleetView folds it
    // into the single dashboard. Legacy PP_STAGE_DASHBOARD* env vars are ignored.

    let mut cluster = ClusterNode::new(
        IrohDriverConfig {
            secret_key: stage_secret_from_env(),
            relay_mode,
            node: node_config(),
            peer_auth: None,
            additional_alpns: vec![ACTOR_ALPN.to_vec()],
        },
        node_config(),
        inference_codec_registry(),
        |_| {},
    )
    .expect("failed to create cluster node");

    // Fleet telemetry is shipped from the main pump loop below over the
    // datastream (see `fleet::FleetEmitter`); there is no separate diagnostics
    // aggregator or in-VM monitor to install here.

    let my_id = cluster.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let direct_addrs: Vec<String> = cluster
        .driver
        .direct_addresses()
        .iter()
        .map(|sa| sa.to_string())
        .collect();
    eprintln!("pp-worker: stage {stage}/{num_stages} ({role:?}) started (node_id: {my_hex})");
    // PP_GPU_NODE_ADDR is printed to stdout (flushed) so a parent process
    // capturing this child's stdout can extract our addressing. The orchestrator
    // uses this to pass each stage's direct addresses to the other stage so
    // peer-to-peer iroh dials work without needing a relay.
    println!("PP_GPU_NODE_ADDR {my_hex} {}", direct_addrs.join(","));
    use std::io::Write;
    let _ = std::io::stdout().flush();

    // Build the seed endpoint and join.
    let seed_bytes = parse_hex_node_id(&seed_hex);
    let seed_key = PublicKey::from_bytes(&seed_bytes).expect("invalid seed public key");
    let mut seed_addr = iroh::EndpointAddr::from(seed_key);
    if let Some(relay) = seed_relay_env.as_deref() {
        if let Ok(relay_url) = relay.trim().parse::<iroh::RelayUrl>() {
            eprintln!("pp-worker: using seed relay {relay}");
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
    let add_peer_from_env =
        |targets: &mut Vec<iroh::EndpointAddr>, hex_var: &str, direct_var: &str, label: &str| {
            if let (Ok(peer_hex), Ok(peer_direct)) =
                (std::env::var(hex_var), std::env::var(direct_var))
            {
                let peer_hex = peer_hex.trim();
                let peer_direct = peer_direct.trim();
                if peer_hex.is_empty() || peer_direct.is_empty() {
                    return;
                }
                let peer_bytes = parse_hex_node_id(peer_hex);
                let peer_key = PublicKey::from_bytes(&peer_bytes).expect("invalid peer node id");
                let mut peer_addr = iroh::EndpointAddr::from(peer_key);
                for part in peer_direct.split(',') {
                    if let Ok(sa) = part.trim().parse::<SocketAddr>() {
                        peer_addr = peer_addr.with_ip_addr(sa);
                    }
                }
                eprintln!("pp-worker: also joining {label} {peer_hex}");
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

    eprintln!("pp-worker: joining seed {seed_hex}");
    cluster.join(&join_targets);

    // If we ended up with a relay (vast.ai / WAN), publish it via SWIM
    // metadata gossip so every other stage learns our relay URL without
    // having to dial us first. `build_route` later reads this through
    // `cluster.peer_relay_url(...)` to enrich the EndpointAddr with the
    // peer's home relay — without that, an N≥3 vast.ai run can stall on
    // the autoregressive feedback edge (last → first) because SWIM has
    // not yet probed that specific pair.
    if let Some(home) = cluster.driver.home_relay_url() {
        eprintln!("pp-worker: publishing home relay {home} to SWIM gossip");
        cluster.set_relay_url(Some(home.to_string()));
    }

    // SWIM convergence gate. The old hardcoded 120s was fatal on vast.ai:
    // every stage's only join target is the seed (the orchestrator), so a
    // stage can only converge once the orchestrator is ticking SWIM and acking
    // its pings. But the orchestrator is blocked in synchronous work for
    // minutes at a time — the vast.ai lease (HTTP polling in lease_chain) —
    // and never acks during those windows.
    // Stages that booted early would hit 120s with no alive peer and exit(1)
    // before the orchestrator ever became responsive (resolve loop), leaving
    // "running" containers with dead processes. The window a stage must outlast
    // is "however long the orchestrator stays busy", so the ceiling defaults
    // high and is tunable via PP_CONVERGE_TIMEOUT_SECS. Convergence is
    // near-instant once the orchestrator starts ticking, so a generous ceiling
    // only costs wall-clock in the genuine no-connectivity case.
    let converge_secs: u64 = std::env::var("PP_CONVERGE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(1200);
    if !wait_for_cluster(&mut cluster, Duration::from_secs(converge_secs)) {
        eprintln!("pp-worker: cluster did not converge in {converge_secs}s");
        std::process::exit(1);
    }
    eprintln!("pp-worker: cluster converged");

    let sender = cluster.rt.create_sender();
    let status_inbox = cluster.rt.new_inbox::<StageActorStatus>().unwrap();

    run_stage(
        cluster,
        sender,
        status_inbox,
        role,
        stage,
        num_stages,
        max_tokens,
    );
}

/// Pump the runtime + driver until the worker reports ready, honouring
/// `SIGHUP`-driven reloads throughout.
///
/// Unlike a timeout-and-exit, this never gives up: if the worker crashes or
/// stalls on startup, the node stays in SWIM (and sshd stays reachable) so an
/// operator can push a fixed `pp_tinygrad_worker.py` and `kill -HUP` to
/// reload in place, recovering the stage without a re-lease. `warn_after`
/// only governs how often the still-waiting line is logged. Returns once a
/// worker (original or reloaded) is ready.
fn hold_until_worker_ready(
    cluster: &mut ClusterNode,
    status_inbox: &swactor::runtime::Inbox<StageActorStatus>,
    stage_actor_addr: ActorAddress,
    stage: u32,
    warn_after: Duration,
) {
    let mut last_warn = Instant::now();
    loop {
        cluster.pump_once();
        drain_reload_request(&cluster.rt, stage_actor_addr);

        if let Some(status) = status_inbox.try_recv() {
            match status {
                StageActorStatus::WorkerReady { pid } => {
                    eprintln!("pp-worker: worker ready (pid: {pid:?})");
                    return;
                }
                StageActorStatus::ProcessStarted => {
                    eprintln!("pp-worker: worker process started");
                }
                StageActorStatus::ProcessExited { status } => {
                    eprintln!(
                        "pp-worker: stage-{stage} worker exited during startup: \
                         {status:?}; holding (SWIM alive) — push a fixed worker.py \
                         and `kill -HUP $(pidof pp-worker)` to reload"
                    );
                    last_warn = Instant::now();
                }
            }
        }

        if last_warn.elapsed() >= warn_after {
            eprintln!(
                "pp-worker: stage-{stage} worker still not ready after {}s; \
                 holding — SIGHUP to reload the worker script",
                warn_after.as_secs()
            );
            last_warn = Instant::now();
        }

        std::thread::sleep(Duration::from_millis(50));
    }
}

fn pump(cluster: &mut ClusterNode, msg_pump: &ActorMessagePump, duration: Duration) {
    let end = Instant::now() + duration;
    while Instant::now() < end {
        cluster.pump_once();
        msg_pump.pump(&cluster.driver, &cluster.codecs, &cluster.rt);
        std::thread::sleep(Duration::from_millis(20));
    }
}

/// Boot one stage, branching on `role`. The shared boot pieces (worker
/// spawn, name registration, route wiring) are N-generic; only the
/// neighbour-resolution step differs per role, per SPEC §5.1.
#[allow(clippy::too_many_arguments)]
fn run_stage(
    mut cluster: ClusterNode,
    sender: swactor::runtime::ExternalSender,
    status_inbox: swactor::runtime::Inbox<StageActorStatus>,
    role: StageRole,
    stage: u32,
    num_stages: u32,
    max_tokens: u32,
) {
    let rt = Arc::clone(&cluster.rt);
    let router = Arc::clone(&cluster.transport_router);
    // Construct the role-appropriate actor with placeholder routing
    // addresses. SetNeighbors overwrites them once SWIM resolution
    // succeeds.
    let placeholder = ActorAddress([0; 32]);
    let stub_mode = std::env::var("PP_WORKER_STUB")
        .map(|v| v.trim() == "1")
        .unwrap_or(false);

    let actor = match role {
        StageRole::First => {
            let mut a = StageActor::first(worker_spec(stage, num_stages), sender, placeholder)
                .with_status_addr(*status_inbox.addr());
            if !stub_mode {
                a = a.with_real_tokenization();
            }
            a
        }
        StageRole::Middle => {
            StageActor::middle(worker_spec(stage, num_stages), sender, placeholder)
                .with_status_addr(*status_inbox.addr())
        }
        StageRole::Last => {
            let mut a = StageActor::last(
                worker_spec(stage, num_stages),
                sender,
                placeholder,
                placeholder,
                max_tokens,
            )
            .with_status_addr(*status_inbox.addr());
            if !stub_mode {
                a = a.with_real_detokenization();
            }
            a
        }
    };
    let stage_actor_addr = rt.spawn(actor).unwrap();

    // Effective worker-ready and neighbor-resolve timeouts. §4.3 couples
    // the neighbor resolve to worker-ready: a stage that has itself
    // become ready MUST be willing to wait for its downstream neighbor
    // for at least as long as it would wait for its own worker. With
    // the per-index registration deferred to post-worker-ready (below),
    // the neighbor's name is genuinely unavailable until the neighbor's
    // worker boots; the resolve must outlast that boot.
    let worker_ready_secs: u64 = std::env::var("PP_WORKER_READY_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(1800);
    let neighbor_resolve_secs: u64 = std::env::var("PP_NEIGHBOR_RESOLVE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(worker_ready_secs);
    let neighbor_resolve_timeout = Duration::from_secs(neighbor_resolve_secs);

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

    // The per-index bridge is the role's *inbound-from-network* adapter:
    //   First:        receives NextTokens from Last  → NextTokenBridge.
    //   Middle, Last: receives StageActivations      → ActivationBridge.
    //
    // Registration is deferred to AFTER the worker is ready (below) so
    // that the SWIM name `pp-stage-{stage}` is a real signal of stage
    // readiness — the orchestrator polls every per-index name to know
    // when to emit `pp_pipeline_wired` (spec §4.6). The per-index actor
    // address itself remains the bridge target.
    let per_index_bridge_addr = match role {
        StageRole::First => next_token_bridge_addr.unwrap(),
        StageRole::Middle | StageRole::Last => activation_bridge_addr.unwrap(),
    };

    // Worker boot can take time even in stub mode (Python startup +
    // tinygrad import on real mode). On a real run the worker also fetches
    // and realizes its model slice: an ~18 GB MoE GGUF (qwen3:30b-a3b) on a
    // cold node can spend many minutes downloading before it reports ready,
    // and the old hardcoded 600s wall would exit(1) a still-loading stage
    // before we ever learn whether the load succeeds. Default high and make
    // it tunable via PP_WORKER_READY_TIMEOUT_SECS; a generous ceiling only
    // costs wall-clock when a worker is genuinely wedged (which the
    // ProcessExited branch below already short-circuits).
    // Worker readiness is non-fatal: hold here (keeping SWIM + sshd alive)
    // until a worker — the original or one swapped in via SIGHUP — reports
    // ready, then proceed to neighbour registration and wiring below.
    hold_until_worker_ready(
        &mut cluster,
        &status_inbox,
        stage_actor_addr,
        stage,
        Duration::from_secs(worker_ready_secs),
    );

    // Now that this worker is ready, publish our per-index name. Spec
    // §4.6: the orchestrator uses each `pp-stage-{K}`'s availability as
    // a per-stage worker-ready signal when deciding to emit
    // `pp_pipeline_wired`. Spec §4.3: neighbour resolves wait on this
    // for as long as worker-ready takes.
    register_name(&cluster, &stage_name(stage), per_index_bridge_addr, stage);

    // Resolve neighbours and wire routes per role. Stage 3 keeps the
    // 2-stage resolution targets in place (Last looks up stage 0 as its
    // NextToken sink, which happens to be First in N=2). Each neighbor
    // resolution uses the §4.3 coupled timeout so a slow-to-boot neighbor
    // cannot break a healthy chain.
    match role {
        StageRole::First => {
            let next_name = next_stage_name(stage, num_stages)
                .expect("first stage has a next neighbour for N>=2");
            let (next_addr, next_hex) =
                resolve_or_die(&mut cluster, &next_name, neighbor_resolve_timeout);
            eprintln!("pp-worker: resolved {next_name} -> {next_addr:?} on {next_hex}");
            add_route_or_die(&cluster, &router, next_addr, &next_hex, &next_name);
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
            let next_name =
                next_stage_name(stage, num_stages).expect("middle stage has a next neighbour");
            let (next_addr, next_hex) =
                resolve_or_die(&mut cluster, &next_name, neighbor_resolve_timeout);
            eprintln!("pp-worker: resolved {next_name} -> {next_addr:?} on {next_hex}");
            add_route_or_die(&cluster, &router, next_addr, &next_hex, &next_name);
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
            let (feedback_addr, feedback_hex) =
                resolve_or_die(&mut cluster, &feedback_name, neighbor_resolve_timeout);
            let (orch_addr, orch_hex) =
                resolve_or_die(&mut cluster, ORCHESTRATOR_NAME, neighbor_resolve_timeout);
            eprintln!(
                "pp-worker: resolved {feedback_name}={feedback_addr:?} on \
                 {feedback_hex}, orch={orch_addr:?} on {orch_hex}"
            );
            add_route_or_die(
                &cluster,
                &router,
                feedback_addr,
                &feedback_hex,
                &feedback_name,
            );
            add_route_or_die(&cluster, &router, orch_addr, &orch_hex, ORCHESTRATOR_NAME);
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
    pump(&mut cluster, &msg_pump, Duration::from_millis(100));

    // Register pp-entry on First (the orchestrator can finally submit
    // requests) and pp-exit on Last (informational). Middle has neither.
    match role {
        StageRole::First => {
            register_name(&cluster, ENTRY_NAME, request_bridge_addr.unwrap(), stage);
        }
        StageRole::Last => {
            register_name(&cluster, EXIT_NAME, activation_bridge_addr.unwrap(), stage);
        }
        StageRole::Middle => {}
    }

    // Fleet telemetry: ship this stage's identity + host.resource (+ runtime /
    // transport / membership) over the swactor cluster transport to the
    // orchestrator's `datastream-sink` actor. The sink address is late-bound —
    // resolved in the main pump once the name converges; until then the
    // ClusterFrameSink drops frames and the bounded mux absorbs the gap.
    let hex: String = cluster
        .node_id()
        .0
        .iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    let name = format!("pp-stage-{stage}");
    let listen = cluster
        .driver
        .direct_addresses()
        .first()
        .map(|a| a.to_string())
        .unwrap_or_default();
    let sink_slot: Arc<OnceLock<ActorAddress>> = Arc::new(OnceLock::new());
    let fleet = fleet::FleetEmitter::new(
        Arc::clone(&cluster.rt),
        Arc::clone(&sink_slot),
        &hex,
        fleet_life(),
        &name,
        &listen,
    );

    main_pump(
        cluster,
        router,
        status_inbox,
        msg_pump,
        stage_actor_addr,
        fleet,
        sink_slot,
    );
}

/// The stream lifetime for this process. The orchestrator injects
/// `PP_STREAM_LIFE` so this stage's boot-phase frames (shipped by the
/// orchestrator over SSH, keyed by this node's pre-derived id) and its own
/// running-phase frames share one `StreamId{node, life}` and fold into a single
/// Fleet row. Falls back to epoch seconds at boot when unset (a standalone
/// run), so a restart still starts a fresh, separately-attributed stream.
fn fleet_life() -> u64 {
    if let Ok(v) = std::env::var("PP_STREAM_LIFE") {
        if let Ok(n) = v.trim().parse::<u64>() {
            return n;
        }
    }
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn main_pump(
    mut cluster: ClusterNode,
    router: Arc<TransportRouter>,
    status_inbox: swactor::runtime::Inbox<StageActorStatus>,
    msg_pump: ActorMessagePump,
    stage_actor_addr: ActorAddress,
    mut fleet: fleet::FleetEmitter,
    sink_slot: Arc<OnceLock<ActorAddress>>,
) {
    eprintln!("pp-worker: entering main pump loop");
    let mut fleet_timer = fleet::FleetTimer::new();
    loop {
        cluster.pump_once();
        msg_pump.pump(&cluster.driver, &cluster.codecs, &cluster.rt);
        drain_reload_request(&cluster.rt, stage_actor_addr);

        // Late-bind the datastream-sink: resolve its cluster name once, add a
        // transport route to the orchestrator, then fill the ClusterFrameSink's
        // destination. Best-effort — until bound, fleet frames are dropped.
        if sink_slot.get().is_none() {
            if let Some((addr, sink_node)) = cluster.resolve_name(DATASTREAM_SINK_NAME) {
                let sink_hex: String = sink_node.0.iter().map(|b| format!("{:02x}", b)).collect();
                match build_route(&cluster, &sink_hex) {
                    Ok(t) => {
                        router.add_route(addr, t);
                        let _ = sink_slot.set(addr);
                        eprintln!("pp-worker: datastream-sink resolved -> {addr:?} on {sink_hex}");
                    }
                    Err(e) => eprintln!("pp-worker: datastream-sink route failed: {e}"),
                }
            }
        }

        if fleet_timer.due() {
            let members = fleet::members_to_pairs(&cluster.members_raw());
            let runtime = fleet::runtime_stats(&cluster.rt);
            let relay_connected = cluster.driver.home_relay_url().is_some();

            // Real membership transitions (with cause) from the SWIM observer —
            // submitted before tick() so they drain this cycle (replaces the
            // emitter's reason-less member-list diff).
            for t in cluster.drain_swim_transitions() {
                fleet.submit_membership(&fleet::membership_transition(&t));
            }
            // Consolidated distribution state: registry + location cache + recent
            // probe targets + directory route count.
            let dist = fleet::build_dist_state(
                &cluster.registry_snapshot(),
                &cluster.location_cache_entries(),
                &cluster.swim_recent_targets(),
                cluster.driver.directory_route_count() as u32,
            );
            fleet.submit_dist_state(&dist);
            // Worker-runtime counters (the deep slice behind runtime.stats).
            fleet.submit_worker_counters(&fleet::worker_counters(&cluster.rt));

            fleet.tick(
                &members,
                runtime,
                relay_connected,
                0,
                cluster.swim_rtt_p50(),
            );
        }

        if let Some(status) = status_inbox.try_recv() {
            match status {
                StageActorStatus::ProcessExited { status } => {
                    eprintln!("pp-worker: worker exited: {status:?}");
                    eprintln!("pp-worker: keeping SWIM alive so the stage stays in the fleet");
                }
                other => eprintln!("pp-worker: status: {other:?}"),
            }
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}
