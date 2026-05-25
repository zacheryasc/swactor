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
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

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

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  pp-smoke-run --seed [--num-stages N] [--prompt <text>] [--max-tokens <n>] [--gpu-node <path>] [--worker <path>]");
    eprintln!("  pp-smoke-run --vastai --api-key <key> [--num-stages N] [--gpu RTX_4090] [--image <name>] [--prompt <text>] [--max-tokens <n>]");
    eprintln!("Notes:");
    eprintln!("  --num-stages defaults to 2 and must be >= 2.");
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
}

fn parse_args() -> Args {
    let argv: Vec<String> = std::env::args().collect();
    let mut a = Args {
        seed: false,
        vastai: false,
        num_stages: 2,
        api_key: None,
        gpu_name: "RTX 4090".into(),
        image: "swactor-pp-gpu:latest".into(),
        prompt: "Say hello".into(),
        max_tokens: 64,
        gpu_node_path: None,
        worker_path: None,
    };
    let mut i = 1;
    while i < argv.len() {
        match argv[i].as_str() {
            "--seed" => a.seed = true,
            "--vastai" => a.vastai = true,
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

fn run_vastai(args: &Args) -> i32 {
    let api_key = args.api_key.clone().expect("--api-key checked earlier");
    let tokio_rt = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("pp-smoke-run: tokio runtime failed: {e}");
            return 1;
        }
    };

    let mut driver = match IrohDriver::new(IrohDriverConfig {
        secret_key: None,
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
        n = args.num_stages,
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

    let base_url = "https://cloud.vast.ai";
    let http = reqwest::Client::new();

    // One call into the lease helper handles find-N-offers, create-N,
    // wait-for-running, and rollback on any partial failure.
    eprintln!(
        "pp-smoke-run: leasing {} {} instances...",
        args.num_stages, args.gpu_name,
    );
    let created = match tokio_rt.block_on(
        pipeline_parallel_inference::vastai::lease_chain(
            &http,
            base_url,
            &api_key,
            &args.gpu_name,
            args.num_stages,
            &my_hex,
            relay_url.as_deref(),
            &args.image,
            Duration::from_secs(10),
            // Cap per-contract polling at 30 (5 min). A healthy 4090 host
            // reaches `running` in ~30-90s; the only cases that take
            // longer are hosts mid-failure (CDI errors, image pull
            // stalls) which `wait_for_running` already surfaces as
            // explicit errors. Keeping the cap tight makes the overall
            // budget predictable.
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
    let contract_ids: Vec<u64> = created.iter().map(|c| c.contract_id).collect();
    eprintln!("pp-smoke-run: rented contracts {contract_ids:?}");

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
            args.num_stages,
        );
        let conv_res = await_convergence(
            args.num_stages as usize,
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
