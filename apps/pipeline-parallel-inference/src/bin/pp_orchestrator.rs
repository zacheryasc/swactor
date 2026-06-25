//! pp-orchestrator — orchestrator for the pipeline-parallel smoke test.
//!
//! Two modes:
//!
//! * `--seed`   — fully local. Spawns `N` `pp-worker` child processes
//!   (`STAGE=0..N-1`) talking to a local iroh seed. Uses
//!   `RelayMode::Disabled` since direct addresses suffice on localhost.
//! * `--vastai` — rents `N` GPU instances on vast.ai, deploys the
//!   `pp-worker` image to each, and drives the same orchestrator path
//!   over WAN. The default one-shot destroys all rented instances before
//!   exit; `--hold` leaves the cluster running (tracked by a local handle
//!   file) so it can be iterated on, and `--teardown` destroys it. See the
//!   cluster-lifecycle usage block.
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use datastream::transport::Delivery;
use datastream::{Consumer, DATASTREAM_SINK_NAME, DatastreamSink};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode, SecretKey};
use iroh_driver::IrohDriverConfig;
use swactor::actor::ActorAddress;
use swactor::runtime::Inbox;

use pipeline_parallel_inference::cluster::ClusterNode;
use pipeline_parallel_inference::iroh_transport::{
    ACTOR_ALPN, ActorMessagePump, IrohActorTransport,
};
use pipeline_parallel_inference::messages::{
    InferenceRequest, InferenceResponse, inference_codec_registry,
};
use pipeline_parallel_inference::orchestrator::{
    ChainGuard, StageSpawnCtx, await_convergence, spawn_chain,
};
use pipeline_parallel_inference::topology::{ENTRY_NAME, stage_name};

const ORCHESTRATOR_NAME: &str = "pp-orchestrator";

fn node_config() -> DistributedNodeConfig {
    DistributedNodeConfig {
        swim: SwimConfig {
            // Probe one peer every 200 ms (old: 10 ticks @ 20 ms tick).
            probe_interval: Duration::from_millis(200),
            indirect_probes: 2,
            // Periodically reprobe dead peers every ~2 s (old: 100 ticks).
            dead_reprobe_interval: Duration::from_secs(2),
            // probe_timeout / suspicion_timeout inherit SwimConfig::default()
            // (15 s / 45 s). Do NOT re-pin them: the old 15 / 60 pin =
            // 300 ms probe budget on a 200-405 ms relay path, the
            // 1779733878 flap cause.
            ..SwimConfig::default()
        },
        cache_capacity: 100,
        registry: RegistryConfig::default(),
        metadata_lambda: 3,
    }
}

fn print_usage() {
    eprintln!("Usage:");
    eprintln!(
        "  pp-orchestrator --seed [--num-stages N] [--prompt <text>] [--max-tokens <n>] [--gpu-node <path>] [--worker <path>]"
    );
    eprintln!(
        "  pp-orchestrator --vastai --api-key <key> [--num-stages N] [--gpu \"RTX 3060\"] [--image <name>] [--prompt <text>] [--max-tokens <n>]"
    );
    eprintln!("Cluster lifecycle (--vastai):");
    eprintln!("  (default)   lease N, drive one run, destroy.");
    eprintln!("  --hold      lease N, drive, leave running; writes a cluster-handle file.");
    eprintln!("  --teardown  destroy the held cluster and delete the handle file.");
    eprintln!("  --label <s> tag/select the cluster (default pp-<N>-<ts>).");
    eprintln!("  --state <p> cluster-handle file path (default ./.pp-cluster.json).");
    eprintln!("Notes:");
    eprintln!("  --num-stages defaults to 2 and must be >= 2.");
    eprintln!("  --hold needs a stable orchestrator identity; it is generated");
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
    ///   teardown → destroy every instance carrying --label, then exit.
    hold: bool,
    teardown: bool,
    /// vast.ai instance label used to tag a cluster at lease time and to
    /// rediscover its live SSH endpoints for teardown.
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
        // GPU model pin: off by default (empty = no model filter). Set PP_GPU in
        // a profile, or --gpu for capacity tests, to restrict to one model. Pair
        // or replace with PP_GPU_MIN_RAM_MB to select by VRAM instead.
        gpu_name: std::env::var("PP_GPU")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .unwrap_or_default(),
        // Image is not baked to a personal registry: it defaults from the
        // PP_IMAGE env (the convention the run scripts already use, e.g.
        // scripts/docker-e2e.sh), falling back to a registry-less tag. Set it
        // in your profile (profiles/*.env) or override with --image.
        image: std::env::var("PP_IMAGE")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "swactor-pp-gpu:latest".into()),
        prompt: "Say hello".into(),
        max_tokens: 64,
        gpu_node_path: None,
        worker_path: None,
        hold: false,
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
    pipeline_parallel_inference::profile::load_profile();
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
    if args.hold && args.teardown {
        eprintln!("at most one of --hold / --teardown may be set");
        std::process::exit(2);
    }
    if (args.hold || args.teardown) && !args.vastai {
        eprintln!("--hold / --teardown require --vastai");
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

/// Resolve the `pp-worker` binary path. Defaults to a sibling of the
/// current executable.
fn resolve_gpu_node_path(args: &Args) -> PathBuf {
    if let Some(p) = &args.gpu_node_path {
        return p.clone();
    }
    let exe = std::env::current_exe().expect("current_exe failed");
    let parent = exe.parent().expect("current exe has no parent");
    parent.join("pp-worker")
}

/// Resolve the worker script path. Defaults to `pp_tinygrad_worker.py`
/// next to this crate's manifest.
fn resolve_worker_path(args: &Args) -> PathBuf {
    if let Some(p) = &args.worker_path {
        return p.clone();
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("pp_tinygrad_worker.py")
}

/// Build a `Command` for a single `pp-worker` child. Captures all the
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
        "pp-orchestrator: spawning {} STAGE={} NUM_STAGES={}",
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
    ] {
        if let Ok(v) = std::env::var(var) {
            cmd.env(var, v);
        }
    }
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

/// Spawn the local datastream consumer and return its actor address. The fold
/// keeps the raw stream in a datastream store without coupling this binary to
/// any presentation layer.
fn wire_datastream_sink(cluster: &ClusterNode) -> Option<ActorAddress> {
    let mut consumer = Consumer::new();
    let sink = DatastreamSink::new(move |stream, frame| {
        let _ = consumer.accept(Delivery { stream, frame });
    });
    match cluster.rt.spawn(sink) {
        Ok(addr) => {
            eprintln!("pp-orchestrator: datastream-sink spawned -> {addr:?}");
            Some(addr)
        }
        Err(e) => {
            eprintln!("pp-orchestrator: failed to spawn datastream-sink: {e}");
            None
        }
    }
}

/// Publish the datastream sink after convergence so the name-dissemination
/// budget is sized for the real cluster, matching the orchestrator inbox.
fn register_datastream_sink_name(cluster: &ClusterNode, addr: Option<ActorAddress>) {
    if let Some(addr) = addr {
        cluster.register_name(DATASTREAM_SINK_NAME, addr);
        eprintln!("pp-orchestrator: datastream-sink registered -> {addr:?}");
    }
}


fn run_seed(args: &Args) -> i32 {
    let gpu_node_bin = resolve_gpu_node_path(args);
    if !gpu_node_bin.exists() {
        eprintln!(
            "pp-orchestrator: pp-worker binary not found at {} (use --gpu-node to override)",
            gpu_node_bin.display()
        );
        return 1;
    }
    let worker_script = resolve_worker_path(args);
    if !worker_script.exists() {
        eprintln!(
            "pp-orchestrator: worker script not found at {} (use --worker to override)",
            worker_script.display()
        );
        return 1;
    }


    let mut cluster = match ClusterNode::new(
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: node_config(),
            peer_auth: None,
            additional_alpns: vec![ACTOR_ALPN.to_vec()],
        },
        node_config(),
        inference_codec_registry(),
        |_| {},
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pp-orchestrator: failed to create cluster node: {e}");
            return 1;
        }
    };


    let my_id = cluster.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let direct: Vec<SocketAddr> = cluster.driver.direct_addresses().to_vec();
    let direct_csv: String = direct
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "pp-orchestrator (--seed --num-stages {n}): orchestrator node {my_hex}, direct={direct:?}",
        n = args.num_stages,
    );
    let seed_datastream_sink_addr = wire_datastream_sink(&cluster);

    // Run inside a labelled block so every failure point can `break`
    // with both an exit code and a stable exit-reason string; the
    // diagnostics finalize record then carries that reason into the
    // bundle.
    let (code, exit_reason): (i32, &'static str) = 'run: {
        let rt = Arc::clone(&cluster.rt);
        let router = Arc::clone(&cluster.transport_router);


        let response_inbox = match rt.new_inbox::<InferenceResponse>() {
            Ok(i) => i,
            Err(e) => {
                eprintln!("pp-orchestrator: new_inbox failed: {e}");
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
                eprintln!("pp-orchestrator: {e}");
                break 'run (1, "spawn_chain_error");
            }
        };
        eprintln!("pp-orchestrator: spawned {} stage children", guard.len());
        // The chain spawner read the announcement line from each child's
        // stdout and kept the pipe draining in a background thread. No
        // further stdout pumping needed here.

        // Wait for the cluster (orchestrator + N stages) to converge.
        eprintln!(
            "pp-orchestrator: waiting for cluster convergence ({} alive peers)...",
            args.num_stages,
        );
        let conv_res = await_convergence_or_child_death(
            args.num_stages as usize,
            Duration::from_secs(90),
            Duration::from_millis(100),
            &mut cluster,
            &mut guard,
        );

        // Publish pp-orchestrator now that the cluster is non-empty: registering
        // earlier would size the dissemination budget for a one-node cluster, and
        // the entry would exhaust its budget before any child could observe it
        // via SWIM piggyback gossip. Doing it post-convergence gives the registry
        // a budget sized for the real cluster.
        cluster.register_name(ORCHESTRATOR_NAME, inbox_addr);
        register_datastream_sink_name(&cluster, seed_datastream_sink_addr);
        eprintln!("pp-orchestrator: registered {ORCHESTRATOR_NAME} -> {inbox_addr:?}");
        if let Err(e) = conv_res {
            eprintln!("pp-orchestrator: {e}");
            break 'run (1, "convergence_error");
        }
        eprintln!("pp-orchestrator: cluster converged");

        // Spec §4.6 + §4.5: gate the request injection on (a) every
        // pp-stage-K resolvable and (b) pp-entry resolvable. The
        // per-index name is published by each stage only after its
        // worker is ready (pp-worker.rs), so resolution of every
        // pp-stage-K is a faithful "all workers ready" signal. The
        // resolve loop polls children too so a stage that dies during
        // wiring fails fast instead of waiting out the timeout.
        let roster_deadline_secs: u64 = std::env::var("PP_PIPELINE_WIRED_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(1800);
        eprintln!("pp-orchestrator: waiting for pipeline-wired (all pp-stage-K + {ENTRY_NAME})...");
        let wire_deadline = Instant::now() + Duration::from_secs(roster_deadline_secs);
        let mut roster_hex: Vec<Option<String>> = vec![None; args.num_stages as usize];
        let (stage0_addr, stage0_node_id) = loop {
            cluster.pump_once();
            for k in 0..args.num_stages {
                if roster_hex[k as usize].is_some() {
                    continue;
                }
                let nm = stage_name(k);
                if let Some((_, nid)) = cluster.resolve_name(&nm) {
                    let hex: String = nid.0.iter().map(|b| format!("{:02x}", b)).collect();
                    roster_hex[k as usize] = Some(hex);
                }
            }
            let entry = cluster.resolve_name(ENTRY_NAME);
            if roster_hex.iter().all(|o| o.is_some()) && entry.is_some() {
                break entry.unwrap();
            }
            if let Err(e) = check_child_death(&mut guard) {
                eprintln!("pp-orchestrator: {e}");
                break 'run (1, "stage_died_pre_resolve");
            }
            if Instant::now() >= wire_deadline {
                let missing: Vec<u32> = roster_hex
                    .iter()
                    .enumerate()
                    .filter_map(|(k, o)| o.is_none().then_some(k as u32))
                    .collect();
                eprintln!(
                    "pp-orchestrator: pipeline did not wire within {roster_deadline_secs}s; \
                     missing pp-stage-K for {missing:?} (entry resolved: {})",
                    entry.is_some(),
                );
                break 'run (1, "pipeline_wired_timeout");
            }
            std::thread::sleep(Duration::from_millis(100));
        };
        let roster: Vec<pipeline_parallel_inference::orchestrator::StageRosterEntry> = roster_hex
            .into_iter()
            .enumerate()
            .map(|(k, hex)| {
                let hex = hex.unwrap();
                let short = hex.chars().take(8).collect::<String>();
                pipeline_parallel_inference::orchestrator::StageRosterEntry {
                    stage_index: k as u32,
                    node_id_hex: hex,
                    node_id_short: short,
                }
            })
            .collect();
        let stage0_hex: String = stage0_node_id
            .0
            .iter()
            .map(|b| format!("{:02x}", b))
            .collect();
        eprintln!("pp-orchestrator: {ENTRY_NAME} -> {stage0_addr:?} on {stage0_hex}");

        // Spec §4.5: emit one pp_stage_roster per drive, before request
        // injection, listing every stage. Seed mode runs a single drive
        // so drive_seq is pinned to 1.
        let drive_seq: u32 = 1;
        // (Diagnostics emission removed: was a driver.emit(Event::Custom {...}).)
        // Spec §4.6: emit exactly one pp_pipeline_wired per drive once
        // every stage is ready, every neighbour is wired (proxied by
        // pp-stage-K registration being post-ready), and the
        // orchestrator has resolved pp-entry.
        // (Diagnostics emission removed: was a driver.emit(Event::Custom {...}).)

        let key = match PublicKey::from_bytes(&stage0_node_id.0) {
            Ok(k) => k,
            Err(e) => {
                eprintln!("pp-orchestrator: invalid stage-0 node key: {e}");
                break 'run (1, "stage0_key_error");
            }
        };
        let route = Arc::new(IrohActorTransport::new(
            cluster.driver.endpoint().clone(),
            iroh::EndpointAddr::from(key),
            cluster.driver.tokio_handle(),
        ));
        router.add_route(stage0_addr, route);

        // Submit the request and await the response.
        let request = InferenceRequest {
            reply_to: inbox_addr,
            prompt: args.prompt.clone(),
            max_tokens: args.max_tokens,
        };
        // Mark this drive's slice of the event stream — seed mode
        // matches the vastai mode emissions so per-drive event slicing
        // applies uniformly.
        // (Diagnostics emission removed: was a driver.emit(Event::Custom {...}).)
        eprintln!(
            "pp-orchestrator: sending InferenceRequest (prompt={:?}, max_tokens={})",
            request.prompt, request.max_tokens
        );
        if let Err(e) = rt.send_to(stage0_addr, request) {
            eprintln!("pp-orchestrator: send_to failed: {e}");
            break 'run (1, "send_to_error");
        }

        let await_secs = await_response_timeout_secs(600);
        let result = await_response(
            &mut cluster,
            &response_inbox,
            Duration::from_secs(await_secs),
            Some(&mut guard),
            &roster,
            drive_seq,
        );
        // (Diagnostics emission removed: was a per-drive boundary marker.)

        match result {
            Ok(text) => {
                println!("=== pipeline-parallel Inference Response ===");
                println!("{text}");
                println!("============================================");
                // The ChainGuard is still in scope here, so the stage
                // containers stay up while we hold.
                if std::env::var_os("PP_HOLD").is_some() {
                    hold_open(&mut cluster);
                }
                (0, "ok")
            }
            Err(e) => {
                eprintln!("pp-orchestrator: {e}");
                (1, e.exit_reason())
            }
        }
    };
    let _ = exit_reason;

    cluster.driver.shutdown();
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
    cluster: &mut ClusterNode,
    guard: &mut ChainGuard,
) -> Result<(), String> {
    let start = Instant::now();
    let mut last_seen: usize = 0;
    // Drain inbound ACTOR_ALPN so stages' fleet telemetry reaches the sink as
    // they join (pump_once only services SWIM/protocol gossip).
    let msg_pump = ActorMessagePump::new();
    loop {
        cluster.pump_once();
        msg_pump.pump(&cluster.driver, &cluster.codecs, &cluster.rt);
        let alive = cluster.alive_count();
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

/// Why [`await_response`] gave up before delivering a response. Used by
/// the caller to pick a stable `exit_reason` string for the drive's
/// finalize record and (spec §4.4) to distinguish dead-member aborts from
/// plain timeouts.
#[derive(Debug)]
enum AwaitError {
    /// Full `PP_AWAIT_RESPONSE_TIMEOUT_SECS` elapsed without a response
    /// AND without any forward-path member declared dead.
    Timeout(Duration),
    /// `InferenceResponse` arrived with an empty `text` field.
    EmptyResponse,
    /// `send_to(stage0, request)` failed before the request left the
    /// orchestrator, so the drive never started.
    SendFailed(String),
    /// Some `pp-worker` child exited locally (seed-mode child guard).
    ChildDied(String),
    /// Spec §4.4: a SWIM member on the forward path (orchestrator +
    /// every stage in the resolved roster) transitioned to `dead` while
    /// the drive was waiting on a response. The diagnostic event
    /// `pp_drive_dead_member` is emitted before this variant is
    /// returned.
    ForwardPathDead {
        stage_index: u32,
        node_id_short: String,
    },
}

impl std::fmt::Display for AwaitError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AwaitError::Timeout(d) => {
                write!(f, "no InferenceResponse within {:.0}s", d.as_secs_f32())
            }
            AwaitError::EmptyResponse => write!(f, "received empty InferenceResponse"),
            AwaitError::SendFailed(s) => write!(f, "send_to failed: {s}"),
            AwaitError::ChildDied(s) => write!(f, "{s}"),
            AwaitError::ForwardPathDead {
                stage_index,
                node_id_short,
            } => write!(
                f,
                "forward-path member dead during drive: stage {stage_index} \
                 (node {node_id_short})"
            ),
        }
    }
}

impl AwaitError {
    /// Distinguishable exit_reason string per failure mode. Spec §4.4
    /// requires the dead-member abort to be distinguishable from a
    /// plain timeout; the orchestrator's finalize record carries this
    /// string into the bundle as `exit_reason`.
    fn exit_reason(&self) -> &'static str {
        match self {
            AwaitError::Timeout(_) => "response_timeout",
            AwaitError::EmptyResponse => "response_empty",
            AwaitError::SendFailed(_) => "send_to_error",
            AwaitError::ChildDied(_) => "stage_died_mid_drive",
            AwaitError::ForwardPathDead { .. } => "forward_path_dead",
        }
    }
}

/// Wait for the response of an injected drive, subject to the
/// [`PP_AWAIT_RESPONSE_TIMEOUT_SECS`] upper bound (spec §4.4).
///
/// In addition to the timeout, the wait aborts early on either:
/// * local child-process death (seed mode only — `children: Some(..)`);
/// * any forward-path SWIM member (resolved roster) transitioning to
///   `dead`. When that happens, a `pp_drive_dead_member` diagnostic
///   event is emitted identifying the stage and the dead member's
///   `node_id_short` before returning [`AwaitError::ForwardPathDead`].
/// Hold the cluster open after a successful drive. The `ChainGuard` is still
/// in scope (containers stay up), and we keep ticking the driver so SWIM stays
/// converged. Returns when the operator presses Enter or closes stdin (Ctrl-D).
fn hold_open(cluster: &mut ClusterNode) {
    eprintln!("pp-orchestrator: holding cluster open. Press Enter (or Ctrl-D) to tear down.");
    // Read stdin on a side thread so the main thread can keep pumping the
    // driver; a blocking read here would freeze SWIM and the live snapshot.
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            let mut line = String::new();
            let _ = std::io::stdin().read_line(&mut line);
            stop.store(true, Ordering::SeqCst);
        });
    }
    let msg_pump = ActorMessagePump::new();
    while !stop.load(Ordering::SeqCst) {
        cluster.pump_once();
        msg_pump.pump(&cluster.driver, &cluster.codecs, &cluster.rt);
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn await_response(
    cluster: &mut ClusterNode,
    inbox: &Inbox<InferenceResponse>,
    timeout: Duration,
    children: Option<&mut ChainGuard>,
    forward_path: &[pipeline_parallel_inference::orchestrator::StageRosterEntry],
    _drive_seq: u32,
) -> Result<String, AwaitError> {
    let msg_pump = ActorMessagePump::new();
    let start = Instant::now();
    let mut last_diag = Instant::now();
    let mut child_guard = children;
    while start.elapsed() < timeout {
        cluster.pump_once();
        msg_pump.pump(&cluster.driver, &cluster.codecs, &cluster.rt);

        if let Some(response) = inbox.try_recv() {
            if response.text.is_empty() {
                return Err(AwaitError::EmptyResponse);
            }
            return Ok(response.text);
        }

        // Surface premature child-process death immediately. The autoregressive
        // loop is single-request, so any stage exiting before the response is
        // an unrecoverable failure — waiting out the SWIM detection window
        // adds latency for no gain.
        if let Some(ref mut guard) = child_guard {
            if let Err(e) = check_child_death(guard) {
                return Err(AwaitError::ChildDied(e));
            }
        }

        // Spec §4.4: subscribe to SWIM membership; abort the wait when
        // any forward-path member transitions to `dead`. The forward
        // path is the orchestrator (self) + every stage in the roster.
        // We only check the roster: the orchestrator's own membership
        // is observable via the surrounding process lifecycle, and
        // SWIM does not declare self `dead`.
        let snap = cluster.snapshot();
        for m in snap.members.iter().filter(|m| m.state == "dead") {
            if let Some(entry) = forward_path.iter().find(|e| e.node_id_hex == m.node_id) {
                // (Diagnostics emission removed: was a driver.emit(Event::Custom {...}).)
                return Err(AwaitError::ForwardPathDead {
                    stage_index: entry.stage_index,
                    node_id_short: entry.node_id_short.clone(),
                });
            }
        }

        if last_diag.elapsed() >= Duration::from_secs(15) {
            let members: Vec<_> = snap
                .members
                .iter()
                .map(|m| format!("{}={}", &m.node_id[..8.min(m.node_id.len())], m.state))
                .collect();
            eprintln!(
                "pp-orchestrator: waiting for response ({:.0}s elapsed, members: {:?})",
                start.elapsed().as_secs_f32(),
                members
            );
            last_diag = Instant::now();
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    Err(AwaitError::Timeout(timeout))
}

/// Read the spec-defined `PP_AWAIT_RESPONSE_TIMEOUT_SECS` upper bound,
/// defaulting to a generous value when unset (spec §4.4: the full
/// timeout MUST still apply if no forward-path member is declared dead
/// and no response arrives).
fn await_response_timeout_secs(default_secs: u64) -> u64 {
    std::env::var("PP_AWAIT_RESPONSE_TIMEOUT_SECS")
        .ok()
        .and_then(|s| s.trim().parse().ok())
        .unwrap_or(default_secs)
}

/// Drive a single inference request through the already-wired pipeline and
/// await its response (vast.ai mode). Self-contained per drive: emits this
/// drive's spec §4.5 `pp_stage_roster` + §4.6 `pp_pipeline_wired` markers and
/// the `pp_drive_start` / `pp_drive_end` boundaries, sends one
/// `InferenceRequest` to stage 0, waits for the response, and prints it on
/// success. The roster and the stage-0 route are resolved once by the caller
/// and reused across drives; the roster is re-emitted each drive so the bundle
/// reader can slice the interleaved event stream per `drive_seq`. Returns the
/// drive's result so the caller can derive an exit code.
#[allow(clippy::too_many_arguments)]
fn drive_once(
    cluster: &mut ClusterNode,
    response_inbox: &Inbox<InferenceResponse>,
    stage0_addr: swactor::actor::ActorAddress,
    roster: &[pipeline_parallel_inference::orchestrator::StageRosterEntry],
    // `num_stages` / `label` were only used in the removed per-drive
    // diagnostics events; kept in the signature so callers stay unchanged.
    _num_stages: u32,
    _label: &str,
    prompt: &str,
    max_tokens: u32,
    drive_seq: u32,
) -> Result<String, AwaitError> {
    let inbox_addr = *response_inbox.addr();

    // Spec §4.5: emit one pp_stage_roster per drive, before any request
    // injection event.
    // (Diagnostics emission removed: was a driver.emit(Event::Custom {...}).)
    // Spec §4.6: emit exactly one pp_pipeline_wired per drive (every stage
    // ready, every neighbour wired — proxied by pp-stage-K registration being
    // post-ready — and pp-entry resolved).
    // (Diagnostics emission removed: was a driver.emit(Event::Custom {...}).)

    let request = InferenceRequest {
        reply_to: inbox_addr,
        prompt: prompt.to_string(),
        max_tokens,
    };
    // Mark this drive's slice of the event stream so a bundle reader can split
    // events by drive.
    // (Diagnostics emission removed: was a driver.emit(Event::Custom {...}).)

    let result = match cluster.rt.send_to(stage0_addr, request) {
        Err(e) => Err(AwaitError::SendFailed(e.to_string())),
        Ok(()) => {
            let await_secs = await_response_timeout_secs(600);
            await_response(
                cluster,
                response_inbox,
                Duration::from_secs(await_secs),
                None,
                roster,
                drive_seq,
            )
        }
    };

    // (Diagnostics emission removed: was a per-drive boundary marker carrying
    // the exit reason / elapsed time.)

    match &result {
        Ok(text) => {
            println!("=== pipeline-parallel Inference Response ===");
            println!("{text}");
            println!("============================================");
        }
        Err(e) => eprintln!("pp-orchestrator: drive {drive_seq} failed: {e}"),
    }
    result
}

/// Live multi-prompt loop for vast.ai `--hold` mode. Keeps the cluster
/// converged while the operator drives more prompts. A stdin-reader side thread
/// feeds prompt lines so the main thread can keep pumping the driver; each
/// non-empty line drives one more inference at the next `drive_seq`. A blank
/// line, `quit`, or EOF (Ctrl-D) ends the loop, after which the caller's
/// finalize + teardown tail runs.
fn prompt_loop(
    cluster: &mut ClusterNode,
    response_inbox: &Inbox<InferenceResponse>,
    stage0_addr: swactor::actor::ActorAddress,
    roster: &[pipeline_parallel_inference::orchestrator::StageRosterEntry],
    num_stages: u32,
    label: &str,
    max_tokens: u32,
    first_drive_seq: u32,
) {
    eprintln!(
        "pp-orchestrator: cluster live. Type a prompt + Enter to drive again; \
         blank line / Ctrl-D / `quit` to tear down."
    );

    // Read prompts on a side thread so the main thread keeps pumping the driver;
    // a blocking stdin read here would freeze SWIM and the live snapshot. The
    // thread forwards each prompt line over a channel and flips `stop` on a
    // blank line, `quit`, or EOF.
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    let stop = Arc::new(AtomicBool::new(false));
    {
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            use std::io::BufRead;
            let stdin = std::io::stdin();
            for line in stdin.lock().lines() {
                let Ok(line) = line else { break };
                let trimmed = line.trim();
                if trimmed.is_empty() || trimmed.eq_ignore_ascii_case("quit") {
                    break;
                }
                if tx.send(trimmed.to_string()).is_err() {
                    break;
                }
            }
            stop.store(true, Ordering::SeqCst);
        });
    }

    // Drain inbound ACTOR_ALPN traffic between drives while the operator is
    // idle at the prompt.
    let msg_pump = ActorMessagePump::new();
    let mut drive_seq = first_drive_seq;
    while !stop.load(Ordering::SeqCst) {
        cluster.pump_once();
        msg_pump.pump(&cluster.driver, &cluster.codecs, &cluster.rt);
        // Drive any prompts that arrived since the last tick. drive_once pumps
        // the driver itself while awaiting each response.
        while let Ok(prompt) = rx.try_recv() {
            drive_seq += 1;
            eprintln!("pp-orchestrator: driving prompt #{drive_seq}: {prompt:?}");
            let _ = drive_once(
                cluster,
                response_inbox,
                stage0_addr,
                roster,
                num_stages,
                label,
                &prompt,
                max_tokens,
                drive_seq,
            );
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

// ─── vast.ai mode ─────────────────────────────────────────────────────

// ─── vast.ai cluster lifecycle (hold / teardown) ──────────────────────

/// Local handle for a held cluster. The orchestrator secret is the one
/// thing vast.ai cannot hand back: held stages seed to the orchestrator's
/// node id (baked into their SEED_ADDR at create time), so re-attaching
/// demands the same keypair. We persist it beside the set of contracts we
/// rented. Volatile facts — live SSH endpoints and liveness — are re-fetched
/// from the vast.ai API at teardown, so this file never stores anything that
/// can go stale underneath us.
#[derive(Debug, Serialize, Deserialize)]
struct ClusterState {
    label: String,
    /// 64 hex chars = the 32-byte iroh secret key.
    orchestrator_secret: String,
    /// Per-stage pinned identities (64 hex each), indexed by stage. Injected
    /// at create so the held cluster keeps stable, resolvable stage node ids
    /// (pp-entry / pp-stage-N) for its whole lifetime.
    #[serde(default)]
    stage_secrets: Vec<String>,
    num_stages: u32,
    model: String,
    image: String,
    contracts: Vec<ContractRef>,
    created_at: u64,
    /// Stable run identifier pinned for the held-cluster lifetime, so a later
    /// `--teardown` reattaches to the same run label without drift from the
    /// operator's current shell env. `None` on handles written before this
    /// field existed.
    #[serde(default)]
    run_id: Option<String>,
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

struct ResolvedCluster {
    secret: [u8; 32],
    label: String,
    num_stages: u32,
    model: String,
    /// Stable identifier for this cluster's lifetime, persisted in the --hold
    /// handle so --teardown reattaches to the same run. `PP_RUN_ID` overrides
    /// the generated default.
    run_id: String,
}

fn resolve_run_id(label: &str) -> String {
    std::env::var("PP_RUN_ID")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| format!("pp-{label}"))
}

/// Resolve the orchestrator identity, label, and stage count for this run.
/// hold/one-shot mint or read them. PP_ORCH_SECRET, when set, always wins.
fn resolve_cluster(args: &Args, _state_path: &Path) -> Result<ResolvedCluster, String> {
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

    // hold or default one-shot: a one-shot's random secret is never
    // persisted (it tears down in the same process), so it is harmless.
    let label = args
        .label
        .clone()
        .unwrap_or_else(|| default_label(args.num_stages));
    let run_id = resolve_run_id(&label);
    let secret = match env_secret {
        Some(s) => s,
        None => random_secret()?,
    };
    Ok(ResolvedCluster {
        secret,
        label,
        num_stages: args.num_stages,
        model,
        run_id,
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
            eprintln!("pp-orchestrator: {e}");
            eprintln!("  (nothing to tear down at that path)");
            return 1;
        }
    };
    let ids: Vec<u64> = st.contracts.iter().map(|c| c.id).collect();
    eprintln!(
        "pp-orchestrator: tearing down label={} contracts={ids:?}",
        st.label
    );
    let results = tokio_rt.block_on(vastai::destroy_all_instances(http, base_url, api_key, &ids));
    let mut ok = true;
    for (id, r) in ids.iter().zip(results.iter()) {
        if let Err(e) = r {
            ok = false;
            eprintln!("pp-orchestrator: destroy {id} failed: {e}");
        }
    }
    match tokio_rt.block_on(vastai::list_instances_by_label(
        http, base_url, api_key, &st.label,
    )) {
        Ok(remaining) if remaining.is_empty() => {
            eprintln!(
                "pp-orchestrator: confirmed 0 instances under label {}",
                st.label
            );
            if let Err(e) = std::fs::remove_file(state_path) {
                eprintln!(
                    "pp-orchestrator: note: could not remove {}: {e}",
                    state_path.display()
                );
            }
        }
        Ok(remaining) => {
            ok = false;
            eprintln!(
                "pp-orchestrator: WARNING {} instance(s) still under label {} — keeping handle file",
                remaining.len(),
                st.label
            );
            for r in &remaining {
                eprintln!("  contract {} status={}", r.contract_id, r.actual_status);
            }
        }
        Err(e) => {
            ok = false;
            eprintln!("pp-orchestrator: could not verify teardown via API: {e}");
        }
    }
    if ok { 0 } else { 1 }
}

fn run_vastai(args: &Args) -> i32 {
    let api_key = args.api_key.clone().expect("--api-key checked earlier");
    let tokio_rt = match tokio::runtime::Runtime::new() {
        Ok(r) => r,
        Err(e) => {
            eprintln!("pp-orchestrator: tokio runtime failed: {e}");
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

    // Resolve identity + shape: hold/one-shot mint or read the secret/label/N.
    let cluster = match resolve_cluster(args, &state_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pp-orchestrator: {e}");
            return 1;
        }
    };
    let num_stages = cluster.num_stages;
    let label = cluster.label.clone();


    let mut cluster_node = match ClusterNode::new(
        IrohDriverConfig {
            secret_key: Some(SecretKey::from_bytes(&cluster.secret)),
            relay_mode: pipeline_parallel_inference::relay_config::relay_mode_from_env(),
            node: node_config(),
            peer_auth: None,
            additional_alpns: vec![ACTOR_ALPN.to_vec()],
        },
        node_config(),
        inference_codec_registry(),
        |_| {},
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pp-orchestrator: failed to create cluster node: {e}");
            return 1;
        }
    };

    // Per-cluster drive counter, emitted on pp_drive_start / pp_drive_end so
    // the bundle reader can slice the interleaved event stream by attempt.
    // One-shot and --hold each drive exactly once per process, so this is 1.
    let drive_seq: u32 = 1;

    // Env injected into each rented stage container: the custom iroh relay so it
    // can join SWIM across the internet, and the orchestrator's deploy SSH key
    // so it can SSH in to drive the boot. Fleet telemetry rides the cluster
    // transport (no env var needed).
    let stage_env = pipeline_parallel_inference::vastai::StageEnv::from_process_env();

    let my_id = cluster_node.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    eprintln!(
        "pp-orchestrator (--vastai --num-stages {n}): orchestrator node {my_hex}",
        n = num_stages,
    );

    // Wait for a relay URL so remote nodes can find us across the internet.
    let relay_url = {
        let start = Instant::now();
        let mut url: Option<String> = None;
        while start.elapsed() < Duration::from_secs(20) {
            cluster_node.pump_once();
            if let Some(u) = cluster_node.driver.home_relay_url() {
                url = Some(u.to_string());
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        url
    };
    if let Some(ref u) = relay_url {
        eprintln!("pp-orchestrator: home relay {u}");
        // Gossip our own home relay through SWIM metadata so the rented
        // stages learn it without having to dial us back first. Mirrors
        // what pp-worker does on its side; together they ensure every
        // pair of nodes can resolve each other's relay URL through
        // metadata gossip alone — the route enrichment in build_route()
        // depends on this.
        cluster_node.set_relay_url(Some(u.clone()));
    } else {
        eprintln!("pp-orchestrator: no relay URL after 20s — vastai mode usually requires one");
    }

    let rt = Arc::clone(&cluster_node.rt);
    let router = Arc::clone(&cluster_node.transport_router);


    // ── Acquire the running cluster ──────────────────────────────────
    // Lease N fresh instances and (on --hold) persist the handle.
    let contract_ids: Vec<u64> = {
        // Pin a stable identity per stage so a held cluster keeps each
        // stage's node id — and thus the pipeline name registry
        // (pp-entry / pp-stage-N) — valid for its whole lifetime. A
        // one-shot tears down immediately, so it uses random ids.
        let stage_secrets: Vec<String> = if args.hold {
            let mut v = Vec::with_capacity(num_stages as usize);
            for _ in 0..num_stages {
                match random_secret() {
                    Ok(b) => v.push(to_hex(&b)),
                    Err(e) => {
                        eprintln!("pp-orchestrator: {e}");
                        return 1;
                    }
                }
            }
            v
        } else {
            Vec::new()
        };

        // Bandwidth-cost deploy default. vast.ai excludes image-pull bandwidth
        // from the per-hour price its search ranks on, so a host that is cheap
        // by the hour can still bill $40/TB on every ~20GB pull. Default to
        // pricing the pull into the offer ranking so true cost drives the pick;
        // an explicit override wins, since set_var only fills an unset/blank
        // var. Set here, before lease_chain spawns any work, so select_offer_pool
        // (which reads it from the env) sees it when ranking the pool.
        let var = "PP_IMAGE_SIZE_GB";
        if std::env::var(var).map_or(true, |v| v.trim().is_empty()) {
            // SAFETY: single-threaded here — no lease/diag worker threads have
            // been spawned yet, so there is no concurrent env access.
            unsafe { std::env::set_var(var, "20") };
            eprintln!("pp-orchestrator: defaulting {var}=20 (price image pull into offer ranking)");
        }

        // One call into the lease helper handles select-the-pool, create-N,
        // wait-for-running, and rollback on any partial failure.
        // Describe the selector accurately: VRAM-filter mode (PP_GPU_MIN_RAM_MB)
        // spans a heterogeneous set of cards, so naming a single model would
        // mislead. lease_chain logs the survivor pool and each stage's pick.
        let vram = std::env::var("PP_GPU_MIN_RAM_MB")
            .ok()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        let selector = match (&vram, args.gpu_name.is_empty()) {
            (Some(mb), true) => format!("any 1-GPU offer with >={mb}MB VRAM"),
            (Some(mb), false) => format!("{} with >={mb}MB VRAM", args.gpu_name),
            (None, false) => args.gpu_name.clone(),
            (None, true) => "any 1-GPU offer".to_string(),
        };
        eprintln!(
            "pp-orchestrator: leasing {} instances [{selector}] (label {label})...",
            num_stages,
        );
        // Run the lease in 200ms slices instead of one blocking call, so the
        // main thread can pump SWIM while instances come up.
        let lease_result = {
            let mut lease_fut = Box::pin(pipeline_parallel_inference::vastai::lease_chain(
                &http,
                base_url,
                &api_key,
                &args.gpu_name,
                num_stages,
                &my_hex,
                relay_url.as_deref(),
                &args.image,
                Some(label.as_str()),
                if stage_secrets.is_empty() {
                    None
                } else {
                    Some(stage_secrets.as_slice())
                },
                Duration::from_secs(10),
                // Cap per-contract polling at 30 (5 min). A healthy host
                // reaches `running` in ~30-90s; longer means a host
                // mid-failure, which `wait_for_running` already surfaces.
                30,
                Some(&stage_env),
            ));
            loop {
                match tokio_rt.block_on(async {
                    tokio::time::timeout(Duration::from_millis(200), &mut lease_fut).await
                }) {
                    Ok(res) => break res,
                    Err(_elapsed) => {
                        cluster_node.pump_once();
                    }
                }
            }
        };
        let created = match lease_result {
            Ok(c) => c,
            Err(e) => {
                eprintln!("pp-orchestrator: lease_chain failed: {e}");
                return 1;
            }
        };
        let ids: Vec<u64> = created.iter().map(|c| c.contract_id).collect();
        // Persist the handle so --teardown can find this set.
        if args.hold {
            let st = ClusterState {
                label: label.clone(),
                orchestrator_secret: to_hex(&cluster.secret),
                stage_secrets: stage_secrets.clone(),
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
                run_id: Some(cluster.run_id.clone()),
            };
            match st.save(&state_path) {
                Ok(()) => eprintln!(
                    "pp-orchestrator: wrote cluster handle {}",
                    state_path.display()
                ),
                Err(e) => eprintln!("pp-orchestrator: WARNING could not write cluster handle: {e}"),
            }
        }
        ids
    };
    eprintln!("pp-orchestrator: cluster contracts {contract_ids:?}");

    let datastream_sink_addr = wire_datastream_sink(&cluster_node);

    // Drive the run inside a labelled block returning `(code, reason)` so
    // every failure point can name the reason it bailed; the orchestrator's
    // diagnostics finalize record then carries that reason into the bundle.
    // Mirrors the run_seed pattern.
    let (code, exit_reason): (i32, &'static str) = 'run: {
        let response_inbox = match rt.new_inbox::<InferenceResponse>() {
            Ok(i) => i,
            Err(e) => {
                eprintln!("pp-orchestrator: new_inbox failed: {e}");
                break 'run (1, "new_inbox_error");
            }
        };
        let inbox_addr = *response_inbox.addr();

        // Wait for cluster convergence (all rented nodes join via the relay).
        // Registering pp-orchestrator must happen *after* convergence so the
        // dissemination budget is sized for the real cluster — see run_seed.
        // The orchestrator only seeds the cluster while it is online, and a
        // bounced N=12 set rejoining over a custom WAN relay can take longer
        // than the old hardcoded 180s to all show "alive" from this side.
        // Env-gate it (default 180 keeps the localhost/small-N behaviour) so a
        // large WAN drive can grant more convergence headroom.
        let orch_converge_secs: u64 = std::env::var("PP_ORCH_CONVERGE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(180);
        eprintln!(
            "pp-orchestrator: waiting for SWIM convergence ({} alive peers, {}s budget)...",
            num_stages, orch_converge_secs,
        );
        // Drain inbound ACTOR_ALPN throughout convergence + pipeline wiring.
        let fleet_pump = ActorMessagePump::new();
        let conv_res = await_convergence(
            num_stages as usize,
            Duration::from_secs(orch_converge_secs),
            Duration::from_millis(200),
            || {
                cluster_node.pump_once();
                fleet_pump.pump(&cluster_node.driver, &cluster_node.codecs, &cluster_node.rt);
                let snap = cluster_node.snapshot();
                snap.members.iter().filter(|m| m.state == "alive").count()
            },
        );
        if let Err(e) = conv_res {
            eprintln!("pp-orchestrator: {e}");
            break 'run (1, "convergence_error");
        }

        cluster_node.register_name(ORCHESTRATOR_NAME, inbox_addr);
        register_datastream_sink_name(&cluster_node, datastream_sink_addr);
        eprintln!("pp-orchestrator: registered {ORCHESTRATOR_NAME} -> {inbox_addr:?}");

        // Spec §4.5 + §4.6: gate the drive on (a) every pp-stage-K
        // resolvable and (b) pp-entry resolvable. Both are proxies for
        // "all stage workers ready and pipeline wired" because the
        // per-index name is published post-worker-ready by pp-worker.
        // 1200s covers an ~18 GB MoE GGUF (e.g. qwen3:30b-a3b)
        // downloading in parallel on N nodes even when some have slow
        // links; smaller models resolve in a fraction of this.
        // Overridable via PP_RESOLVE_TIMEOUT_SECS.
        let resolve_secs = std::env::var("PP_RESOLVE_TIMEOUT_SECS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1200);
        let resolve_deadline = Instant::now() + Duration::from_secs(resolve_secs);
        let mut roster_hex: Vec<Option<String>> = vec![None; num_stages as usize];
        let (stage0_addr, stage0_node_id) = loop {
            cluster_node.pump_once();
            fleet_pump.pump(&cluster_node.driver, &cluster_node.codecs, &cluster_node.rt);
            // Keep app messages flowing while the pipeline wires up.
            for k in 0..num_stages {
                if roster_hex[k as usize].is_some() {
                    continue;
                }
                let nm = stage_name(k);
                if let Some((_, nid)) = cluster_node.resolve_name(&nm) {
                    let hex: String = nid.0.iter().map(|b| format!("{:02x}", b)).collect();
                    roster_hex[k as usize] = Some(hex);
                }
            }
            let entry = cluster_node.resolve_name(ENTRY_NAME);
            if roster_hex.iter().all(|o| o.is_some()) && entry.is_some() {
                break entry.unwrap();
            }
            if Instant::now() >= resolve_deadline {
                let missing: Vec<u32> = roster_hex
                    .iter()
                    .enumerate()
                    .filter_map(|(k, o)| o.is_none().then_some(k as u32))
                    .collect();
                eprintln!(
                    "pp-orchestrator: pipeline did not wire within {resolve_secs}s; \
                     missing pp-stage-K for {missing:?} (entry resolved: {})",
                    entry.is_some(),
                );
                break 'run (1, "resolve_timeout");
            }
            std::thread::sleep(Duration::from_millis(200));
        };
        let roster: Vec<pipeline_parallel_inference::orchestrator::StageRosterEntry> = roster_hex
            .into_iter()
            .enumerate()
            .map(|(k, hex)| {
                let hex = hex.unwrap();
                let short = hex.chars().take(8).collect::<String>();
                pipeline_parallel_inference::orchestrator::StageRosterEntry {
                    stage_index: k as u32,
                    node_id_hex: hex,
                    node_id_short: short,
                }
            })
            .collect();
        let key = match PublicKey::from_bytes(&stage0_node_id.0) {
            Ok(k) => k,
            Err(e) => {
                eprintln!("pp-orchestrator: invalid stage-0 node key: {e}");
                break 'run (1, "stage0_key_error");
            }
        };
        // Enrich the EndpointAddr with stage 0's relay URL (from SWIM
        // metadata gossip) or our own home relay as a fallback, so iroh has
        // routing info even if it has never dialed stage 0 directly. See
        // pp-worker::build_route for the same rationale on the worker side.
        let mut stage0_endpoint = iroh::EndpointAddr::from(key);
        if let Some(url) = cluster_node
            .peer_relay_url(stage0_node_id)
            .and_then(|s| s.parse::<iroh::RelayUrl>().ok())
            .or_else(|| cluster_node.driver.home_relay_url())
        {
            stage0_endpoint = stage0_endpoint.with_relay_url(url);
        }
        let route = Arc::new(IrohActorTransport::new(
            cluster_node.driver.endpoint().clone(),
            stage0_endpoint,
            cluster_node.driver.tokio_handle(),
        ));
        router.add_route(stage0_addr, route);

        // First (canonical) drive: args.prompt at drive_seq 1. drive_once emits
        // this drive's pp_stage_roster / pp_pipeline_wired / pp_drive_start /
        // pp_drive_end markers, sends the request to stage 0, and prints the
        // response. The roster + stage-0 route were resolved once above and are
        // reused for every drive.
        let (code, reason) = match drive_once(
            &mut cluster_node,
            &response_inbox,
            stage0_addr,
            &roster,
            num_stages,
            &label,
            &args.prompt,
            args.max_tokens,
            drive_seq,
        ) {
            Ok(_) => (0, "ok"),
            Err(e) => (1, e.exit_reason()),
        };

        // Live multi-prompt loop for held clusters. A plain one-shot keeps
        // today's single-drive-then-exit behaviour.
        if args.hold {
            prompt_loop(
                &mut cluster_node,
                &response_inbox,
                stage0_addr,
                &roster,
                num_stages,
                &label,
                args.max_tokens,
                drive_seq,
            );
        }

        (code, reason)
    };

    let is_held = args.hold;
    let _ = exit_reason;
    cluster_node.driver.shutdown();

    // Teardown policy: --hold leaves the cluster running so it can be
    // iterated on; only the default one-shot tears down on exit.
    if is_held {
        eprintln!("pp-orchestrator: HOLDING cluster (label={label}, contracts={contract_ids:?})");
        eprintln!(
            "  destroy when done:   pp-orchestrator --vastai --api-key <k> --teardown --state {}",
            state_path.display()
        );
        eprintln!("  inspect:             vastai show instances   (label {label})");
    } else {
        // Default one-shot: always destroy rented instances, even on failure.
        eprintln!("pp-orchestrator: destroying instances {contract_ids:?}");
        let results =
            tokio_rt.block_on(pipeline_parallel_inference::vastai::destroy_all_instances(
                &http,
                base_url,
                &api_key,
                &contract_ids,
            ));
        for (id, r) in contract_ids.iter().zip(results.iter()) {
            if let Err(e) = r {
                eprintln!("pp-orchestrator: destroy {id} failed: {e}");
            }
        }
    }

    code
}
