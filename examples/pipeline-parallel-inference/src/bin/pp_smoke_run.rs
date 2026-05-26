//! pp-smoke-run — orchestrator for the pipeline-parallel smoke test.
//!
//! Two modes:
//!
//! * `--seed`   — fully local. Spawns `N` `pp-gpu-node` child processes
//!   (`STAGE=0..N-1`) talking to a local iroh seed. Uses
//!   `RelayMode::Disabled` since direct addresses suffice on localhost.
//! * `--vastai` — rents `N` GPU instances on vast.ai, deploys the
//!   `pp-gpu-node` image to each, and drives the same orchestrator path
//!   over WAN. Always destroys all rented instances before exit.
//!
//! Stage count is configurable via `--num-stages N` (default 2, any
//! `N >= 2`). The chain logic is identical at every N; the binary's only
//! contribution is the per-process bookkeeping plus driving the request.
//!
//! In both modes the orchestrator:
//!
//! 1. Creates an iroh driver and a swactor runtime with an
//!    `InferenceResponse` inbox.
//! 2. Registers the inbox under the SWIM name `pp-orchestrator` so the
//!    last stage can resolve it and send its final response back.
//! 3. Waits for cluster convergence to `N` alive peers (orchestrator
//!    sees the N stages).
//! 4. Resolves `pp-entry`, sends one `InferenceRequest`, awaits one
//!    `InferenceResponse`, prints it, and exits.
//! 5. Kills any spawned child processes and (on `--vastai`) destroys all
//!    rented instances regardless of success or failure.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode, SecretKey};

use swactor::runtime::{Inbox, Runtime, RuntimeConfig};
use swactor::transport::TransportRouter;

use distribution::diagnostics::Role as DiagRole;

use pipeline_parallel_inference::diag;
use pipeline_parallel_inference::iroh_transport::{
    ActorMessagePump, IrohActorTransport, ACTOR_ALPN,
};
use pipeline_parallel_inference::messages::{
    inference_codec_registry, InferenceRequest, InferenceResponse,
};
use pipeline_parallel_inference::orchestrator::{
    await_convergence, spawn_chain, ChainGuard, StageSpawnCtx,
};
use pipeline_parallel_inference::topology::ENTRY_NAME;

const ORCHESTRATOR_NAME: &str = "pp-orchestrator";

fn node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            probe_interval: 10,
            indirect_probes: 2,
            dead_reprobe_interval: 100,
            // probe_timeout / suspicion_timeout inherit the calibrated
            // SwimConfig::default() (750 / 2250 ticks = 15 s / 45 s; see
            // crates/simulation/SWIM_RETUNE_REPORT.md). Do NOT re-pin them:
            // the old 15 / 60 pin = 300 ms probe budget on a 200-405 ms
            // relay path, the 1779733878 flap cause.
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        republish_interval: 50,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  pp-smoke-run --seed [--num-stages N] [--prompt <text>] [--max-tokens <n>] [--gpu-node <path>] [--worker <path>]");
    eprintln!("  pp-smoke-run --vastai --api-key <key> [--num-stages N] [--gpu \"RTX 3060\"] [--image <name>] [--prompt <text>] [--max-tokens <n>]");
    eprintln!("Cluster lifecycle (--vastai):");
    eprintln!("  (default)   lease N, drive one run, destroy.");
    eprintln!("  --hold      lease N, drive, leave running; writes a cluster-handle file.");
    eprintln!("  --redeploy  scp local binaries onto the held cluster, bounce + drive again.");
    eprintln!("  --teardown  destroy the held cluster and delete the handle file.");
    eprintln!("  --label <s> tag/select the cluster (default pp-<N>-<ts>).");
    eprintln!("  --state <p> cluster-handle file path (default ./.pp-cluster.json).");
    eprintln!("Notes:");
    eprintln!("  --num-stages defaults to 2 and must be >= 2.");
    eprintln!("  --hold/--redeploy need a stable orchestrator identity; it is generated");
    eprintln!("  and stored in the handle file (override with PP_ORCH_SECRET=<64 hex>).");
}

#[derive(Debug)]
struct Args {
    seed: bool,
    vastai: bool,
    num_stages: u32,
    api_key: Option<String>,
    gpu_name: String,
    image: String,
    prompt: String,
    max_tokens: u32,
    gpu_node_path: Option<PathBuf>,
    worker_path: Option<PathBuf>,
    /// Cluster lifecycle mode for --vastai (mutually exclusive):
    ///   default  → lease, drive one run, destroy (the original one-shot).
    ///   hold     → lease, drive, leave the cluster running (no destroy).
    ///   redeploy → skip leasing; scp the local binaries onto every held
    ///              instance (found by --label), bounce them in place,
    ///              drive again, leave running.
    ///   teardown → destroy every instance carrying --label, then exit.
    hold: bool,
    redeploy: bool,
    teardown: bool,
    /// vast.ai instance label used to tag a cluster at lease time and to
    /// rediscover its live SSH endpoints for redeploy/teardown.
    label: Option<String>,
    /// Path to the local cluster-handle file (the orchestrator secret +
    /// the contracts we rented). Defaults to ./.pp-cluster.json.
    state: Option<PathBuf>,
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let mut a = Args {
        seed: false,
        vastai: false,
        num_stages: 2,
        api_key: None,
        // RTX 3060 (12GB) is our default deploy-test class: cheapest GPU class
        // with deep, reliable supply on vast.ai (see fleet notes). Override with
        // --gpu for capacity tests. NOT sized for real model weights.
        gpu_name: "RTX 3060".into(),
        image: "zacheryasc/swactor-pp-gpu:latest".into(),
        prompt: "Say hello".into(),
        max_tokens: 64,
        gpu_node_path: None,
        worker_path: None,
        hold: false,
        redeploy: false,
        teardown: false,
        label: None,
        state: None,
    };
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--seed" => a.seed = true,
            "--vastai" => a.vastai = true,
            "--hold" => a.hold = true,
            "--redeploy" => a.redeploy = true,
            "--teardown" => a.teardown = true,
            "--label" => {
                i += 1;
                a.label = Some(argv[i].clone());
            }
            "--state" => {
                i += 1;
                a.state = Some(PathBuf::from(&argv[i]));
            }
            "--num-stages" => {
                i += 1;
                a.num_stages = argv[i].parse().unwrap_or_else(|_| {
                    eprintln!("--num-stages must be a u32");
                    std::process::exit(2);
                });
            }
            "--api-key" => {
                i += 1;
                a.api_key = Some(argv[i].trim().to_string());
            }
            "--gpu" => {
                i += 1;
                a.gpu_name = argv[i].clone();
            }
            "--image" => {
                i += 1;
                a.image = argv[i].clone();
            }
            "--prompt" => {
                i += 1;
                a.prompt = argv[i].clone();
            }
            "--max-tokens" => {
                i += 1;
                a.max_tokens = argv[i].parse().unwrap_or_else(|_| {
                    eprintln!("--max-tokens must be a u32");
                    std::process::exit(2);
                });
            }
            "--gpu-node" => {
                i += 1;
                a.gpu_node_path = Some(PathBuf::from(&argv[i]));
            }
            "--worker" => {
                i += 1;
                a.worker_path = Some(PathBuf::from(&argv[i]));
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_usage();
                std::process::exit(2);
            }
        }
        i += 1;
    }
    a
}

fn main() {
    let args = parse_args();
    if args.seed == args.vastai {
        eprintln!("exactly one of --seed or --vastai is required");
        print_usage();
        std::process::exit(2);
    }
    if args.num_stages < 2 {
        eprintln!(
            "--num-stages must be >= 2 (got {}); single-node use examples/single-gpu-inference",
            args.num_stages
        );
        std::process::exit(2);
    }
    if args.vastai && args.api_key.is_none() {
        eprintln!("--api-key required with --vastai");
        std::process::exit(2);
    }
    if [args.hold, args.redeploy, args.teardown]
        .iter()
        .filter(|&&f| f)
        .count()
        > 1
    {
        eprintln!("at most one of --hold / --redeploy / --teardown may be set");
        std::process::exit(2);
    }
    if (args.hold || args.redeploy || args.teardown) && !args.vastai {
        eprintln!("--hold / --redeploy / --teardown require --vastai");
        std::process::exit(2);
    }

    let exit_code = if args.seed {
        run_seed(&args)
    } else {
        run_vastai(&args)
    };
    std::process::exit(exit_code);
}

// ─── Seed (localhost) mode ────────────────────────────────────────────

/// Resolve the `pp-gpu-node` binary path. Defaults to a sibling of the
/// current executable.
fn resolve_gpu_node_path(args: &Args) -> PathBuf {
    if let Some(p) = &args.gpu_node_path {
        return p.clone();
    }
    let exe = std::env::current_exe().expect("current_exe failed");
    let parent = exe.parent().expect("current exe has no parent");
    parent.join("pp-gpu-node")
}

/// Resolve the worker script path. Defaults to `pp_tinygrad_worker.py`
/// next to this crate's manifest.
fn resolve_worker_path(args: &Args) -> PathBuf {
    if let Some(p) = &args.worker_path {
        return p.clone();
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pp_tinygrad_worker.py")
}

/// Build a `Command` for a single `pp-gpu-node` child. Captures all the
/// env-var bookkeeping in one place so the spawn-chain closure is short.
fn build_gpu_node_command(
    gpu_node_bin: &PathBuf,
    worker_script: &PathBuf,
    seed_hex: &str,
    seed_direct_csv: &str,
    max_tokens: u32,
    ctx: &StageSpawnCtx,
) -> Command {
    eprintln!(
        "pp-smoke-run: spawning {} STAGE={} NUM_STAGES={}",
        gpu_node_bin.display(),
        ctx.stage,
        ctx.num_stages,
    );
    let mut cmd = Command::new(gpu_node_bin);
    cmd.env("STAGE", ctx.stage.to_string())
        .env("NUM_STAGES", ctx.num_stages.to_string())
        .env("SEED_ADDR", seed_hex)
        .env("SEED_DIRECT", seed_direct_csv)
        .env("MAX_TOKENS", max_tokens.to_string())
        .env("WORKER_SCRIPT", worker_script)
        .stderr(Stdio::inherit());
    // Pass through worker mode / interpreter / model from our own environment.
    // Defaulting WORKER_CMD to python3 keeps the existing manual-invocation
    // ergonomics; everything else opts in.
    for var in [
        "PP_WORKER_STUB",
        "MODEL",
        "PYTHON",
        "CUDA",
        "PP_BOOT_DELAY_STAGE",
        "PP_BOOT_DELAY_SECS",
        "SWACTOR_DIAG_COLLECTOR_URL",
        "SWACTOR_DIAG_RUN_ID",
        "SWACTOR_DIAG_SPOOL_DIR",
        "SWACTOR_DIAG_UDP_ECHO",
    ] {
        if let Ok(v) = std::env::var(var) {
            cmd.env(var, v);
        }
    }
    // The orchestrator knows each child's diagnostic identity. Override
    // the role/index/count rather than letting the child guess from its
    // own env — keeps the bundle's identity blocks authoritative.
    cmd.env("SWACTOR_DIAG_NODE_ROLE", "stage")
        .env("SWACTOR_DIAG_STAGE_INDEX", ctx.stage.to_string())
        .env("SWACTOR_DIAG_STAGE_COUNT", ctx.num_stages.to_string());
    let worker_cmd = std::env::var("WORKER_CMD").unwrap_or_else(|_| "python3".into());
    cmd.env("WORKER_CMD", worker_cmd);
    if let Some(peer) = &ctx.peer {
        cmd.env("PEER_NODE_ID", &peer.hex)
            .env("PEER_DIRECT", &peer.direct);
    }
    // Every stage past the first also gets stage 0's address so it can dial
    // it on boot. That outbound dial seeds Stage 0's iroh NodeMap with the
    // dialer's direct addresses (and vice versa), which is what makes the
    // last → first autoregressive feedback edge work at N ≥ 3 with
    // RelayMode::Disabled — without it the kernel never tells Last where
    // First lives.
    if let Some(first) = &ctx.first_peer {
        cmd.env("FIRST_PEER_NODE_ID", &first.hex)
            .env("FIRST_PEER_DIRECT", &first.direct);
    }
    cmd
}

/// Returns `Err` as soon as any stage child in `guard` is observed to have
/// exited. The orchestrator's various polling loops (convergence,
/// resolve-pp-entry, await-response) all need the same death check to
/// short-circuit instead of waiting out their full timeouts — keeping it in
/// one helper means the third loop can't silently regress past a future
/// refactor.
fn check_child_death(guard: &mut ChainGuard) -> Result<(), String> {
    for spawned in guard.stages_mut() {
        match spawned.child.try_wait() {
            Ok(Some(status)) => {
                return Err(format!(
                    "stage {} child pid {} exited prematurely with {:?}",
                    spawned.stage,
                    spawned.child.id(),
                    status
                ));
            }
            Ok(None) => {}
            Err(e) => {
                return Err(format!(
                    "failed to poll stage {} child pid {}: {e}",
                    spawned.stage,
                    spawned.child.id(),
                ));
            }
        }
    }
    Ok(())
}

fn run_seed(args: &Args) -> i32 {
    let gpu_node_bin = resolve_gpu_node_path(args);
    if !gpu_node_bin.exists() {
        eprintln!(
            "pp-smoke-run: pp-gpu-node binary not found at {} (use --gpu-node to override)",
            gpu_node_bin.display()
        );
        return 1;
    }
    let worker_script = resolve_worker_path(args);
    if !worker_script.exists() {
        eprintln!(
            "pp-smoke-run: worker script not found at {} (use --worker to override)",
            worker_script.display()
        );
        return 1;
    }

    let mut driver = match IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: node_config(),
        peer_auth: None,
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    }) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("pp-smoke-run: failed to create iroh driver: {e}");
            return 1;
        }
    };

    let diag = diag::install_from_env(&mut driver, DiagRole::orchestrator());

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let direct: Vec<SocketAddr> = driver.direct_addresses().to_vec();
    let direct_csv: String = direct
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "pp-smoke-run (--seed --num-stages {n}): orchestrator node {my_hex}, direct={direct:?}",
        n = args.num_stages,
    );

    // Run inside a labelled block so every failure point can `break`
    // with both an exit code and a stable exit-reason string; the
    // diagnostics finalize record then carries that reason into the
    // bundle.
    let (code, exit_reason): (i32, &'static str) = 'run: {
        // Runtime + inbox + transport bookkeeping.
        let mut rt = Runtime::new(RuntimeConfig::default());
        let codecs = Arc::new(inference_codec_registry());
        let router = Arc::new(TransportRouter::new());
        rt.set_codec_registry(codecs.clone());
        rt.set_transport_router(router.clone());

        let response_inbox = match rt.new_inbox::<InferenceResponse>() {
            Ok(i) => i,
            Err(e) => {
                eprintln!("pp-smoke-run: new_inbox failed: {e}");
                break 'run (1, "new_inbox_error");
            }
        };
        let inbox_addr = *response_inbox.addr();

        // Spawn N stage children sequentially. Each non-first child receives
        // its predecessor's announced addressing in `PEER_DIRECT` so the
        // outbound dial populates each peer's NodeMap — SWIM gossip alone
        // propagates membership but not addressing.
        let max_tokens = args.max_tokens;
        let mut guard = match spawn_chain(args.num_stages, Duration::from_secs(60), |ctx| {
            build_gpu_node_command(
                &gpu_node_bin,
                &worker_script,
                &my_hex,
                &direct_csv,
                max_tokens,
                &ctx,
            )
        }) {
            Ok(g) => g,
            Err(e) => {
                eprintln!("pp-smoke-run: {e}");
                break 'run (1, "spawn_chain_error");
            }
        };
        eprintln!("pp-smoke-run: spawned {} stage children", guard.len());
        // The chain spawner read the announcement line from each child's
        // stdout and kept the pipe draining in a background thread. No
        // further stdout pumping needed here.

        // Wait for the cluster (orchestrator + N stages) to converge.
        eprintln!(
            "pp-smoke-run: waiting for cluster convergence ({} alive peers)...",
            args.num_stages,
        );
        let conv_res = await_convergence_or_child_death(
            args.num_stages as usize,
            Duration::from_secs(90),
            Duration::from_millis(100),
            &mut driver,
            &mut guard,
        );

        // Publish pp-orchestrator now that the cluster is non-empty: registering
        // earlier would size the dissemination budget for a one-node cluster, and
        // the entry would exhaust its budget before any child could observe it
        // via SWIM piggyback gossip. Doing it post-convergence gives the registry
        // a budget sized for the real cluster.
        driver
            .node_mut()
            .register_name(ORCHESTRATOR_NAME.into(), inbox_addr);
        diag::emit_register_name(&driver, ORCHESTRATOR_NAME, inbox_addr, None);
        eprintln!("pp-smoke-run: registered {ORCHESTRATOR_NAME} -> {inbox_addr:?}");
        if let Err(e) = conv_res {
            eprintln!("pp-smoke-run: {e}");
            break 'run (1, "convergence_error");
        }
        eprintln!("pp-smoke-run: cluster converged");

        // Resolve pp-entry and wire a route to stage 0. Poll children inside
        // this loop too: a stage that dies between convergence and our resolve
        // breaks pp-entry's gossip propagation, so without the death check we
        // would otherwise wait out the full 60s resolve deadline instead of
        // failing fast.
        eprintln!("pp-smoke-run: resolving {ENTRY_NAME}...");
        let resolve_deadline = Instant::now() + Duration::from_secs(60);
        let (stage0_addr, stage0_node_id) = loop {
            driver.recv();
            driver.tick();
            if let Some((addr, node_id)) = driver.node().resolve_name(ENTRY_NAME) {
                break (addr, node_id);
            }
            if let Err(e) = check_child_death(&mut guard) {
                eprintln!("pp-smoke-run: {e}");
                break 'run (1, "stage_died_pre_resolve");
            }
            if Instant::now() >= resolve_deadline {
                eprintln!("pp-smoke-run: failed to resolve {ENTRY_NAME} in 60s");
                break 'run (1, "resolve_timeout");
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let stage0_hex: String = stage0_node_id
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        eprintln!("pp-smoke-run: {ENTRY_NAME} -> {stage0_addr:?} on {stage0_hex}");

        let key = match PublicKey::from_bytes(&stage0_node_id.0) {
            Ok(k) => k,
            Err(e) => {
                eprintln!("pp-smoke-run: invalid stage-0 node key: {e}");
                break 'run (1, "stage0_key_error");
            }
        };
        let route = Arc::new(IrohActorTransport::new(
            driver.endpoint().clone(),
            iroh::EndpointAddr::from(key),
            driver.tokio_handle(),
        ));
        router.add_route(stage0_addr, route);

        // Submit the request and await the response.
        let request = InferenceRequest {
            reply_to: inbox_addr,
            prompt: args.prompt.clone(),
            max_tokens: args.max_tokens,
        };
        eprintln!(
            "pp-smoke-run: sending InferenceRequest (prompt={:?}, max_tokens={})",
            request.prompt, request.max_tokens
        );
        if let Err(e) = rt.send_to(stage0_addr, request) {
            eprintln!("pp-smoke-run: send_to failed: {e}");
            break 'run (1, "send_to_error");
        }

        let result = await_response(
            &mut driver,
            &rt,
            &codecs,
            &response_inbox,
            Duration::from_secs(180),
            Some(&mut guard),
        );

        match result {
            Ok(text) => {
                println!("=== pipeline-parallel Inference Response ===");
                println!("{text}");
                println!("============================================");
                (0, "ok")
            }
            Err(e) => {
                eprintln!("pp-smoke-run: {e}");
                (1, "response_error")
            }
        }
    };

    if let Some(handles) = diag {
        handles.finalize(exit_reason);
        handles.shutdown();
    }
    driver.shutdown();
    code
    // guard drops here, killing all stage children
}

/// Like `await_convergence` but with an extra failure mode: if any spawned
/// child has already exited, return an error instead of waiting out the
/// timeout. Without this, killing a stage during the convergence window
/// silently parks the orchestrator on a SWIM probe loop until the 90s
/// budget elapses — slow to fail, and the test that drives this scenario
/// (`binary_e2e_*_killed_orchestrator_exits_nonzero`) was the slowest case
/// in the §13.2 suite. Polling children in the same loop tightens that
/// from ~95s down to ~50ms.
fn await_convergence_or_child_death(
    expected: usize,
    timeout: Duration,
    poll_interval: Duration,
    driver: &mut IrohDriver,
    guard: &mut ChainGuard,
) -> Result<(), String> {
    let start = Instant::now();
    let mut last_seen: usize = 0;
    loop {
        driver.recv();
        driver.tick();
        let alive = driver
            .snapshot()
            .members
            .iter()
            .filter(|m| m.state == "alive")
            .count();
        last_seen = last_seen.max(alive);
        if alive >= expected {
            return Ok(());
        }
        check_child_death(guard)?;
        if start.elapsed() >= timeout {
            return Err(format!(
                "cluster did not converge in {:.0}s (expected {} alive peers, last saw {})",
                timeout.as_secs_f32(),
                expected,
                last_seen,
            ));
        }
        std::thread::sleep(poll_interval);
    }
}

fn await_response(
    driver: &mut IrohDriver,
    rt: &Runtime,
    codecs: &Arc<swactor::transport::CodecRegistry>,
    inbox: &Inbox<InferenceResponse>,
    timeout: Duration,
    children: Option<&mut ChainGuard>,
) -> Result<String, String> {
    let msg_pump = ActorMessagePump::new();
    let start = Instant::now();
    let mut last_diag = Instant::now();
    let mut child_guard = children;
    while start.elapsed() < timeout {
        driver.recv();
        driver.tick();
        msg_pump.pump(driver, codecs, rt);
        rt.tick();

        if let Some(response) = inbox.try_recv() {
            if response.text.is_empty() {
                return Err("received empty InferenceResponse".into());
            }
            return Ok(response.text);
        }

        // Surface premature child-process death immediately. The autoregressive
        // loop is single-request, so any stage exiting before the response is
        // an unrecoverable failure — waiting out the SWIM detection window
        // adds latency for no gain.
        if let Some(ref mut guard) = child_guard {
            check_child_death(guard)?;
        }

        if last_diag.elapsed() >= Duration::from_secs(15) {
            let snap = driver.snapshot();
            let members: Vec<_> = snap
                .members
                .iter()
                .map(|m| format!("{}={}", &m.node_id[..8.min(m.node_id.len())], m.state))
                .collect();
            eprintln!(
                "pp-smoke-run: waiting for response ({:.0}s elapsed, members: {:?})",
                start.elapsed().as_secs_f32(),
                members
            );
            last_diag = Instant::now();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "no InferenceResponse within {:.0}s",
        timeout.as_secs_f32()
    ))
}

// ─── vast.ai mode ─────────────────────────────────────────────────────

// ─── vast.ai cluster lifecycle (hold / redeploy / teardown) ───────────

/// Local handle for a held cluster. The orchestrator secret is the one
/// thing vast.ai cannot hand back: held stages seed to the orchestrator's
/// node id (baked into their SEED_ADDR at create time), so re-attaching
/// demands the same keypair. We persist it beside the set of contracts we
/// rented. Volatile facts — live SSH endpoints and liveness — are re-fetched
/// from the vast.ai API at redeploy/teardown, so this file never stores
/// anything that can go stale underneath us.
#[derive(Debug, Serialize, Deserialize)]
struct ClusterState {
    label: String,
    /// 64 hex chars = the 32-byte iroh secret key.
    orchestrator_secret: String,
    num_stages: u32,
    model: String,
    image: String,
    contracts: Vec<ContractRef>,
    created_at: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct ContractRef {
    id: u64,
    stage: u32,
}

impl ClusterState {
    fn load(path: &Path) -> Result<Self, String> {
        let raw = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read cluster state {}: {e}", path.display()))?;
        serde_json::from_str(&raw)
            .map_err(|e| format!("cannot parse cluster state {}: {e}", path.display()))
    }
    fn save(&self, path: &Path) -> Result<(), String> {
        let raw = serde_json::to_string_pretty(self)
            .map_err(|e| format!("cannot serialize cluster state: {e}"))?;
        std::fs::write(path, raw)
            .map_err(|e| format!("cannot write cluster state {}: {e}", path.display()))
    }
}

fn default_state_path() -> PathBuf {
    PathBuf::from(".pp-cluster.json")
}

fn default_label(num_stages: u32) -> String {
    format!("pp-{num_stages}-{}", now_secs())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn secret_from_hex(hex: &str) -> Result<[u8; 32], String> {
    let hex = hex.trim();
    if hex.len() != 64 {
        return Err(format!(
            "orchestrator secret must be 64 hex chars, got {}",
            hex.len()
        ));
    }
    let mut out = [0u8; 32];
    for (i, b) in out.iter_mut().enumerate() {
        *b = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
            .map_err(|_| "orchestrator secret is not valid hex".to_string())?;
    }
    Ok(out)
}

/// 32 bytes from the OS CSPRNG (Linux deploy host) to mint a fresh,
/// persistable orchestrator identity for a held cluster.
fn random_secret() -> Result<[u8; 32], String> {
    use std::io::Read;
    let mut buf = [0u8; 32];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .map_err(|e| format!("cannot read /dev/urandom: {e}"))?;
    Ok(buf)
}

/// SSH private key vast.ai authenticates with (its public half is registered
/// on the account). Override with PP_SSH_KEY.
fn ssh_key_path() -> PathBuf {
    if let Ok(p) = std::env::var("PP_SSH_KEY") {
        return PathBuf::from(p);
    }
    let home = std::env::var("HOME").unwrap_or_else(|_| ".".into());
    PathBuf::from(home).join(".ssh/id_ed25519")
}

/// Push the freshly-built binary + worker onto a held instance over SSH and
/// bounce pp-gpu-node in place. The restart re-execs under PID 1's
/// environment (where vast.ai injected the per-stage env at create time), so
/// STAGE / NUM_STAGES / SEED_ADDR / SEED_RELAY / MODEL survive the bounce
/// without us reconstructing them.
fn redeploy_instance(
    inst: &pipeline_parallel_inference::vastai::LabeledInstance,
    gpu_node_bin: &Path,
    worker_script: &Path,
    ssh_key: &Path,
) -> Result<(), String> {
    let host = if !inst.ssh_host.is_empty() {
        inst.ssh_host.as_str()
    } else {
        inst.public_ipaddr.as_str()
    };
    if host.is_empty() || inst.ssh_port == 0 {
        return Err(format!(
            "contract {} has no SSH endpoint yet (status {})",
            inst.contract_id, inst.actual_status
        ));
    }
    let port = inst.ssh_port.to_string();
    let target = format!("root@{host}");

    let scp = |local: &Path, remote: &str| -> Result<(), String> {
        let out = Command::new("scp")
            .args(["-P", &port])
            .arg("-i")
            .arg(ssh_key)
            .args(["-o", "StrictHostKeyChecking=no"])
            .args(["-o", "UserKnownHostsFile=/dev/null"])
            .args(["-o", "ConnectTimeout=20"])
            .arg(local)
            .arg(format!("{target}:{remote}"))
            .output()
            .map_err(|e| format!("scp spawn failed: {e}"))?;
        if !out.status.success() {
            return Err(format!(
                "scp {} -> {remote} failed: {}",
                local.display(),
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }
        Ok(())
    };

    scp(gpu_node_bin, "/usr/local/bin/pp-gpu-node")?;
    scp(worker_script, "/usr/local/share/pp_tinygrad_worker.py")?;

    // Kill the running stage (a child of vast.ai's PID 1, not PID 1 itself),
    // then re-exec it detached under PID 1's env. Needs `pkill` (procps) and
    // bash in the image.
    let restart = "pkill -f /usr/local/bin/pp-gpu-node || true; sleep 1; chmod +x /usr/local/bin/pp-gpu-node; setsid bash -c 'while IFS= read -r -d \"\" kv; do export \"$kv\"; done < /proc/1/environ; exec /usr/local/bin/pp-gpu-node' >/var/log/pp-redeploy.log 2>&1 </dev/null &";
    let out = Command::new("ssh")
        .arg("-n")
        .args(["-p", &port])
        .arg("-i")
        .arg(ssh_key)
        .args(["-o", "StrictHostKeyChecking=no"])
        .args(["-o", "UserKnownHostsFile=/dev/null"])
        .args(["-o", "ConnectTimeout=20"])
        .arg(&target)
        .arg(restart)
        .output()
        .map_err(|e| format!("ssh spawn failed: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "ssh restart failed: {}",
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    Ok(())
}

struct ResolvedCluster {
    secret: [u8; 32],
    label: String,
    num_stages: u32,
    model: String,
}

/// Resolve the orchestrator identity, label, and stage count for this run.
/// Redeploy adopts them from the on-disk handle (so it re-presents the same
/// node id the held stages seed to); hold/one-shot mint or read them.
/// PP_ORCH_SECRET, when set, always wins.
fn resolve_cluster(args: &Args, state_path: &Path) -> Result<ResolvedCluster, String> {
    let env_secret = match std::env::var("PP_ORCH_SECRET") {
        Ok(h) if !h.trim().is_empty() => Some(secret_from_hex(&h)?),
        _ => None,
    };
    let model = std::env::var("MODEL")
        .ok()
        .filter(|m| !m.trim().is_empty())
        .unwrap_or_else(|| {
            if std::env::var("PP_WORKER_STUB").is_ok() {
                "stub".into()
            } else {
                "unset".into()
            }
        });

    if args.redeploy {
        let st = ClusterState::load(state_path)?;
        let secret = match env_secret {
            Some(s) => s,
            None => secret_from_hex(&st.orchestrator_secret)?,
        };
        return Ok(ResolvedCluster {
            secret,
            label: st.label,
            num_stages: st.num_stages,
            model: st.model,
        });
    }

    // hold or default one-shot: a one-shot's random secret is never
    // persisted (it tears down in the same process), so it is harmless.
    let label = args
        .label
        .clone()
        .unwrap_or_else(|| default_label(args.num_stages));
    let secret = match env_secret {
        Some(s) => s,
        None => random_secret()?,
    };
    Ok(ResolvedCluster {
        secret,
        label,
        num_stages: args.num_stages,
        model,
    })
}

/// Destroy a held cluster and drop its handle. Authority for "is it really
/// gone" is the vast.ai API, not the local file: we destroy by contract id,
/// then re-query the label and only delete the handle once it reports zero.
fn run_teardown(
    tokio_rt: &tokio::runtime::Runtime,
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    state_path: &Path,
) -> i32 {
    use pipeline_parallel_inference::vastai;
    let st = match ClusterState::load(state_path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("pp-smoke-run: {e}");
            eprintln!("  (nothing to tear down at that path)");
            return 1;
        }
    };
    let ids: Vec<u64> = st.contracts.iter().map(|c| c.id).collect();
    eprintln!(
        "pp-smoke-run: tearing down label={} contracts={ids:?}",
        st.label
    );
    let results = tokio_rt.block_on(vastai::destroy_all_instances(http, base_url, api_key, &ids));
    let mut ok = true;
    for (id, r) in ids.iter().zip(results.iter()) {
        if let Err(e) = r {
            ok = false;
            eprintln!("pp-smoke-run: destroy {id} failed: {e}");
        }
    }
    match tokio_rt.block_on(vastai::list_instances_by_label(http, base_url, api_key, &st.label)) {
        Ok(remaining) if remaining.is_empty() => {
            eprintln!(
                "pp-smoke-run: confirmed 0 instances under label {}",
                st.label
            );
            if let Err(e) = std::fs::remove_file(state_path) {
                eprintln!(
                    "pp-smoke-run: note: could not remove {}: {e}",
                    state_path.display()
                );
            }
        }
        Ok(remaining) => {
            ok = false;
            eprintln!(
                "pp-smoke-run: WARNING {} instance(s) still under label {} — keeping handle file",
                remaining.len(),
                st.label
            );
            for r in &remaining {
                eprintln!("  contract {} status={}", r.contract_id, r.actual_status);
            }
        }
        Err(e) => {
            ok = false;
            eprintln!("pp-smoke-run: could not verify teardown via API: {e}");
        }
    }
    if ok {
        0
    } else {
        1
    }
}

fn run_vastai(args: &Args) -> i32 {
    let api_key = args.api_key.clone().expect("--api-key checked earlier");
    let tokio_rt = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("pp-smoke-run: tokio runtime failed: {e}");
            return 1;
        }
    };
    let http = reqwest::Client::new();
    let base_url = "https://cloud.vast.ai";
    let state_path = args.state.clone().unwrap_or_else(default_state_path);

    // Teardown is pure lifecycle — no orchestrator/driver needed.
    if args.teardown {
        return run_teardown(&tokio_rt, &http, base_url, &api_key, &state_path);
    }

    // Resolve identity + shape per mode (redeploy adopts the held cluster's
    // secret/label/N from the handle; hold/one-shot mint or read them).
    let cluster = match resolve_cluster(args, &state_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pp-smoke-run: {e}");
            return 1;
        }
    };
    let num_stages = cluster.num_stages;
    let label = cluster.label.clone();

    let mut driver = match IrohDriver::new(IrohDriverConfig {
        secret_key: Some(SecretKey::from_bytes(&cluster.secret)),
        relay_mode: pipeline_parallel_inference::relay_config::relay_mode_from_env(),
        node: node_config(),
        peer_auth: None,
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    }) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("pp-smoke-run: failed to create iroh driver: {e}");
            return 1;
        }
    };

    // Wire orchestrator-side diagnostics from SWACTOR_DIAG_* env. Mirrors
    // run_seed. When the env vars are unset this returns None and the
    // run proceeds with no diagnostics — same behaviour as before.
    let diag = diag::install_from_env(&mut driver, DiagRole::orchestrator());

    // Rented stage containers learn the same collector URL via env vars
    // injected into their vast.ai create_instance payload below. Reading
    // the values here (rather than from the DiagHandles) means the
    // forwarding works even when the orchestrator's own diagnostics are
    // off (e.g. a quick dry-run that just wants the rented stages to
    // ship into a central collector).
    let diag_env_for_stages = pipeline_parallel_inference::vastai::DiagEnv::from_process_env();

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    eprintln!(
        "pp-smoke-run (--vastai --num-stages {n}): orchestrator node {my_hex}",
        n = num_stages,
    );
    if diag_env_for_stages.is_enabled() {
        eprintln!(
            "pp-smoke-run: forwarding diagnostics to rented stages (collector={})",
            diag_env_for_stages
                .collector_url
                .as_deref()
                .unwrap_or(""),
        );
    }

    // Wait for a relay URL so remote nodes can find us across the internet.
    let relay_url = {
        let start = Instant::now();
        let mut url: Option<String> = None;
        while start.elapsed() < Duration::from_secs(20) {
            driver.recv();
            driver.tick();
            if let Some(u) = driver.home_relay_url() {
                url = Some(u.to_string());
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        url
    };
    if let Some(ref u) = relay_url {
        eprintln!("pp-smoke-run: home relay {u}");
        // Gossip our own home relay through SWIM metadata so the rented
        // stages learn it without having to dial us back first. Mirrors
        // what pp-gpu-node does on its side; together they ensure every
        // pair of nodes can resolve each other's relay URL through
        // metadata gossip alone — the route enrichment in build_route()
        // depends on this.
        driver
            .node_mut()
            .set_relay_url(Some(u.clone()));
    } else {
        eprintln!("pp-smoke-run: no relay URL after 20s — vastai mode usually requires one");
    }

    // ── Acquire the running cluster ──────────────────────────────────
    // Redeploy skips leasing: it rediscovers the held cluster by label and
    // pushes the freshly-built binaries onto each instance in place.
    // Otherwise lease N fresh instances and (on --hold) persist the handle.
    let contract_ids: Vec<u64> = if args.redeploy {
        let insts = match tokio_rt.block_on(
            pipeline_parallel_inference::vastai::list_instances_by_label(
                &http, base_url, &api_key, &label,
            ),
        ) {
            Ok(v) => v,
            Err(e) => {
                eprintln!("pp-smoke-run: cannot list cluster by label {label}: {e}");
                if let Some(handles) = diag {
                    handles.finalize("redeploy_list_error");
                    handles.shutdown();
                }
                return 1;
            }
        };
        if insts.is_empty() {
            eprintln!("pp-smoke-run: no live instances under label {label} — nothing to redeploy");
            return 1;
        }
        if insts.len() != num_stages as usize {
            eprintln!(
                "pp-smoke-run: WARNING handle expects {num_stages} stages but label {label} has {} live",
                insts.len(),
            );
        }
        let gpu_bin = resolve_gpu_node_path(args);
        let worker = resolve_worker_path(args);
        let ssh_key = ssh_key_path();
        eprintln!(
            "pp-smoke-run: redeploying {} onto {} instance(s) (key {})",
            gpu_bin.display(),
            insts.len(),
            ssh_key.display(),
        );
        for inst in &insts {
            eprint!("  contract {} ... ", inst.contract_id);
            match redeploy_instance(inst, &gpu_bin, &worker, &ssh_key) {
                Ok(()) => eprintln!("pushed + bounced"),
                Err(e) => {
                    eprintln!("FAILED: {e}");
                    eprintln!("pp-smoke-run: cluster left running; fix and re-run --redeploy");
                    if let Some(handles) = diag {
                        handles.finalize("redeploy_push_error");
                        handles.shutdown();
                    }
                    return 1;
                }
            }
        }
        insts.iter().map(|i| i.contract_id).collect()
    } else {
        // One call into the lease helper handles find-N-offers, create-N,
        // wait-for-running, and rollback on any partial failure.
        eprintln!(
            "pp-smoke-run: leasing {} {} instances (label {label})...",
            num_stages, args.gpu_name,
        );
        let created = match tokio_rt.block_on(
            pipeline_parallel_inference::vastai::lease_chain(
                &http,
                base_url,
                &api_key,
                &args.gpu_name,
                num_stages,
                &my_hex,
                relay_url.as_deref(),
                &args.image,
                Some(label.as_str()),
                Duration::from_secs(10),
                // Cap per-contract polling at 30 (5 min). A healthy host
                // reaches `running` in ~30-90s; longer means a host
                // mid-failure, which `wait_for_running` already surfaces.
                30,
                Some(&diag_env_for_stages),
            ),
        ) {
            Ok(c) => c,
            Err(e) => {
                eprintln!("pp-smoke-run: lease_chain failed: {e}");
                if let Some(handles) = diag {
                    handles.finalize("lease_chain_error");
                    handles.shutdown();
                }
                return 1;
            }
        };
        let ids: Vec<u64> = created.iter().map(|c| c.contract_id).collect();
        // Persist the handle so --redeploy / --teardown can find this set.
        if args.hold {
            let st = ClusterState {
                label: label.clone(),
                orchestrator_secret: to_hex(&cluster.secret),
                num_stages,
                model: cluster.model.clone(),
                image: args.image.clone(),
                contracts: ids
                    .iter()
                    .enumerate()
                    .map(|(i, &id)| ContractRef {
                        id,
                        stage: i as u32,
                    })
                    .collect(),
                created_at: now_secs(),
            };
            match st.save(&state_path) {
                Ok(()) => eprintln!("pp-smoke-run: wrote cluster handle {}", state_path.display()),
                Err(e) => eprintln!("pp-smoke-run: WARNING could not write cluster handle: {e}"),
            }
        }
        ids
    };
    eprintln!("pp-smoke-run: cluster contracts {contract_ids:?}");

    // Drive the run inside a labelled block returning `(code, reason)` so
    // every failure point can name the reason it bailed; the orchestrator's
    // diagnostics finalize record then carries that reason into the bundle.
    // Mirrors the run_seed pattern.
    let (code, exit_reason): (i32, &'static str) = 'run: {
        // Set up runtime + inbox + orchestrator name, same as seed mode.
        let mut rt = Runtime::new(RuntimeConfig::default());
        let codecs = Arc::new(inference_codec_registry());
        let router = Arc::new(TransportRouter::new());
        rt.set_codec_registry(codecs.clone());
        rt.set_transport_router(router.clone());
        let response_inbox = match rt.new_inbox::<InferenceResponse>() {
            Ok(i) => i,
            Err(e) => {
                eprintln!("pp-smoke-run: new_inbox failed: {e}");
                break 'run (1, "new_inbox_error");
            }
        };
        let inbox_addr = *response_inbox.addr();

        // Wait for cluster convergence (all rented nodes join via the relay).
        // Registering pp-orchestrator must happen *after* convergence so the
        // dissemination budget is sized for the real cluster — see run_seed.
        eprintln!(
            "pp-smoke-run: waiting for SWIM convergence ({} alive peers)...",
            num_stages,
        );
        let conv_res = await_convergence(
            num_stages as usize,
            Duration::from_secs(180),
            Duration::from_millis(200),
            || {
                driver.recv();
                driver.tick();
                driver
                    .snapshot()
                    .members
                    .iter()
                    .filter(|m| m.state == "alive")
                    .count()
            },
        );
        if let Err(e) = conv_res {
            eprintln!("pp-smoke-run: {e}");
            break 'run (1, "convergence_error");
        }

        driver
            .node_mut()
            .register_name(ORCHESTRATOR_NAME.into(), inbox_addr);
        diag::emit_register_name(&driver, ORCHESTRATOR_NAME, inbox_addr, None);
        eprintln!("pp-smoke-run: registered {ORCHESTRATOR_NAME} -> {inbox_addr:?}");

        // Resolve stage 0. Bumped to 300s for vast.ai cold starts: stage 0 only
        // registers pp-entry after every later stage's worker becomes ready,
        // and each worker spends most of its boot fetching the GGUF and
        // realizing the tinygrad model graph on a cold cache.
        let resolve_deadline = Instant::now() + Duration::from_secs(300);
        let (stage0_addr, stage0_node_id) = loop {
            driver.recv();
            driver.tick();
            if let Some((addr, node_id)) = driver.node().resolve_name(ENTRY_NAME) {
                break (addr, node_id);
            }
            if Instant::now() >= resolve_deadline {
                eprintln!("pp-smoke-run: failed to resolve {ENTRY_NAME}");
                break 'run (1, "resolve_timeout");
            }
            std::thread::sleep(Duration::from_millis(200));
        };
        let key = match PublicKey::from_bytes(&stage0_node_id.0) {
            Ok(k) => k,
            Err(e) => {
                eprintln!("pp-smoke-run: invalid stage-0 node key: {e}");
                break 'run (1, "stage0_key_error");
            }
        };
        // Enrich the EndpointAddr with stage 0's relay URL (from SWIM
        // metadata gossip) or our own home relay as a fallback, so iroh has
        // routing info even if it has never dialed stage 0 directly. See
        // pp-gpu-node::build_route for the same rationale on the worker side.
        let mut stage0_endpoint = iroh::EndpointAddr::from(key);
        if let Some(url) = driver
            .node()
            .relay_url(&stage0_node_id)
            .and_then(|s| s.parse::<iroh::RelayUrl>().ok())
            .or_else(|| driver.home_relay_url())
        {
            stage0_endpoint = stage0_endpoint.with_relay_url(url);
        }
        let route = Arc::new(IrohActorTransport::new(
            driver.endpoint().clone(),
            stage0_endpoint,
            driver.tokio_handle(),
        ));
        router.add_route(stage0_addr, route);

        let request = InferenceRequest {
            reply_to: inbox_addr,
            prompt: args.prompt.clone(),
            max_tokens: args.max_tokens,
        };
        if let Err(e) = rt.send_to(stage0_addr, request) {
            eprintln!("pp-smoke-run: send_to failed: {e}");
            break 'run (1, "send_to_error");
        }

        let result = await_response(
            &mut driver,
            &rt,
            &codecs,
            &response_inbox,
            Duration::from_secs(600),
            None,
        );

        match result {
            Ok(text) => {
                println!("=== pipeline-parallel Inference Response ===");
                println!("{text}");
                println!("============================================");
                (0, "ok")
            }
            Err(e) => {
                eprintln!("pp-smoke-run: {e}");
                (1, "response_error")
            }
        }
    };

    // Finalize diagnostics with the run's exit reason before tearing down
    // the driver — finalize triggers the collector to set snapshot_now
    // hints on every reporter, and the spool drainer needs a live driver
    // runtime to flush remaining records.
    if let Some(handles) = diag {
        handles.finalize(exit_reason);
        handles.shutdown();
    }
    driver.shutdown();

    // Always destroy rented instances, even on failure.
    eprintln!("pp-smoke-run: destroying instances {contract_ids:?}");
    let results = tokio_rt.block_on(
        pipeline_parallel_inference::vastai::destroy_all_instances(
            &http,
            base_url,
            &api_key,
            &contract_ids,
        ),
    );
    for (id, r) in contract_ids.iter().zip(results.iter()) {
        if let Err(e) = r {
            eprintln!("pp-smoke-run: destroy {id} failed: {e}");
        }
    }
    code
}
