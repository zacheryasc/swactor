//! pp-smoke-run — orchestrator for the pipeline-parallel smoke test.
//!
//! Two modes:
//!
//! * `--seed`   — fully local. Spawns two `pp-gpu-node` child processes
//!   (`STAGE=0` and `STAGE=1`) talking to a local iroh seed. Uses
//!   `RelayMode::Disabled` since direct addresses suffice on localhost.
//! * `--vastai` — rents two GPU instances on vast.ai, deploys the
//!   `pp-gpu-node` image to each, and drives the same orchestrator path
//!   over WAN. Always destroys both instances before exit.
//!
//! In both modes the orchestrator:
//!
//! 1. Creates an iroh driver and a swactor runtime with an
//!    `InferenceResponse` inbox.
//! 2. Registers the inbox under the SWIM name `pp-orchestrator` so
//!    stage 1 can resolve it and send its final response back.
//! 3. Waits for cluster convergence to 3 alive members (orchestrator +
//!    two stages).
//! 4. Resolves `pp-entry`, sends one `InferenceRequest`, awaits one
//!    `InferenceResponse`, prints it, and exits.
//! 5. Kills any spawned child processes and (on `--vastai`) destroys all
//!    rented instances regardless of success or failure.

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Child, ChildStdout, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::runtime::{Inbox, Runtime, RuntimeConfig};
use swactor::transport::TransportRouter;

use pipeline_parallel_inference::iroh_transport::{
    ActorMessagePump, IrohActorTransport, ACTOR_ALPN,
};
use pipeline_parallel_inference::messages::{
    inference_codec_registry, InferenceRequest, InferenceResponse,
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
    eprintln!("  pp-smoke-run --seed [--prompt <text>] [--max-tokens <n>] [--gpu-node <path>] [--worker <path>]");
    eprintln!("  pp-smoke-run --vastai --api-key <key> [--gpu RTX_4090] [--image <name>] [--prompt <text>] [--max-tokens <n>]");
}

#[derive(Debug)]
struct Args {
    seed: bool,
    vastai: bool,
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

/// RAII guard so that spawned child processes are killed when the
/// orchestrator returns (success or panic).
struct ChildGuard {
    children: Vec<Child>,
}

impl ChildGuard {
    fn new() -> Self {
        Self { children: vec![] }
    }
    fn push(&mut self, child: Child) {
        self.children.push(child);
    }
}

impl Drop for ChildGuard {
    fn drop(&mut self) {
        for child in self.children.iter_mut() {
            let pid = child.id();
            let _ = child.kill();
            let _ = child.wait();
            eprintln!("pp-smoke-run: killed child pid {pid}");
        }
    }
}

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

fn spawn_gpu_node(
    gpu_node_bin: &PathBuf,
    worker_script: &PathBuf,
    stage: u32,
    num_stages: u32,
    seed_hex: &str,
    seed_direct: &[SocketAddr],
    max_tokens: u32,
    peer: Option<(&str, &str)>,
) -> std::io::Result<Child> {
    let direct_str: String = seed_direct
        .iter()
        .map(|a| a.to_string())
        .collect::<Vec<_>>()
        .join(",");
    eprintln!(
        "pp-smoke-run: spawning {} STAGE={stage} NUM_STAGES={num_stages}",
        gpu_node_bin.display()
    );
    let mut cmd = Command::new(gpu_node_bin);
    cmd.env("STAGE", stage.to_string())
        .env("NUM_STAGES", num_stages.to_string())
        .env("SEED_ADDR", seed_hex)
        .env("SEED_DIRECT", direct_str)
        .env("MAX_TOKENS", max_tokens.to_string())
        .env("WORKER_SCRIPT", worker_script)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    // Pass through worker mode / interpreter / model from our own environment.
    // Defaulting WORKER_CMD to python3 keeps the existing manual-invocation
    // ergonomics; everything else opts in.
    for var in ["PP_WORKER_STUB", "MODEL", "PYTHON", "CUDA"] {
        if let Ok(v) = std::env::var(var) {
            cmd.env(var, v);
        }
    }
    let worker_cmd = std::env::var("WORKER_CMD").unwrap_or_else(|_| "python3".into());
    cmd.env("WORKER_CMD", worker_cmd);
    if let Some((peer_hex, peer_direct)) = peer {
        cmd.env("PEER_NODE_ID", peer_hex)
            .env("PEER_DIRECT", peer_direct);
    }
    cmd.spawn()
}

/// Consume a child's stdout: look for the `PP_GPU_NODE_ADDR <hex> <direct>` line
/// and return `(hex, direct_csv)`. All lines are forwarded to our own stdout so
/// the user sees the child's output verbatim. A background thread keeps draining
/// stdout after the announcement so the child does not block on its own pipe.
fn read_stage_address(
    stdout: ChildStdout,
    stage: u32,
    timeout: Duration,
) -> Result<(String, String), String> {
    let (tx, rx) = std::sync::mpsc::channel();
    thread::spawn(move || {
        let mut reader = BufReader::new(stdout);
        let mut line = String::new();
        let mut announced = false;
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    print!("{line}");
                    if !announced {
                        if let Some(rest) = line.trim().strip_prefix("PP_GPU_NODE_ADDR ") {
                            if let Some((hex, direct)) = rest.split_once(' ') {
                                let _ = tx.send((hex.to_string(), direct.to_string()));
                                announced = true;
                            }
                        }
                    }
                }
                Err(_) => break,
            }
        }
    });
    rx.recv_timeout(timeout)
        .map_err(|_| format!("stage {stage}: PP_GPU_NODE_ADDR not seen within {:.0}s", timeout.as_secs_f32()))
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

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    let direct: Vec<SocketAddr> = driver.direct_addresses().to_vec();
    eprintln!(
        "pp-smoke-run (--seed): orchestrator node {my_hex}, direct={:?}",
        direct
    );

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
            return 1;
        }
    };
    let inbox_addr = *response_inbox.addr();

    // Spawn the two stage children serially. Stage 0 starts first so the
    // orchestrator can read its endpoint addressing and pass it to stage 1
    // as a second seed. Stage 1's outbound dial to stage 0 is what populates
    // each peer's iroh NodeMap with the other peer's direct addresses —
    // SWIM gossip alone propagates membership but not addressing on its own.
    let mut guard = ChildGuard::new();
    let mut s0_child = match spawn_gpu_node(
        &gpu_node_bin,
        &worker_script,
        0,
        2,
        &my_hex,
        &direct,
        args.max_tokens,
        None,
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pp-smoke-run: spawn STAGE=0 failed: {e}");
            return 1;
        }
    };
    let s0_stdout = s0_child.stdout.take().expect("stage 0 stdout piped");
    guard.push(s0_child);
    let (s0_hex, s0_direct) = match read_stage_address(s0_stdout, 0, Duration::from_secs(60)) {
        Ok(addr) => addr,
        Err(e) => {
            eprintln!("pp-smoke-run: {e}");
            return 1;
        }
    };
    eprintln!("pp-smoke-run: stage 0 announced node {s0_hex} direct={s0_direct}");

    let mut s1_child = match spawn_gpu_node(
        &gpu_node_bin,
        &worker_script,
        1,
        2,
        &my_hex,
        &direct,
        args.max_tokens,
        Some((&s0_hex, &s0_direct)),
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pp-smoke-run: spawn STAGE=1 failed: {e}");
            return 1;
        }
    };
    let s1_stdout = s1_child.stdout.take().expect("stage 1 stdout piped");
    guard.push(s1_child);
    // Drain stage 1's stdout for forwarding; we do not need its addr now that
    // stage 1 has stage 0 as a peer-seed and the orchestrator already learned
    // both peers via their joins.
    thread::spawn(move || {
        let mut reader = BufReader::new(s1_stdout);
        let mut line = String::new();
        loop {
            line.clear();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => print!("{line}"),
                Err(_) => break,
            }
        }
    });

    // Wait for the cluster (orchestrator + 2 stages) to converge.
    eprintln!("pp-smoke-run: waiting for cluster convergence (3 alive)...");
    let converge_deadline = Instant::now() + Duration::from_secs(90);
    let mut converged = false;
    while Instant::now() < converge_deadline {
        driver.recv();
        driver.tick();
        let snap = driver.snapshot();
        let alive = snap.members.iter().filter(|m| m.state == "alive").count();
        if alive >= 2 {
            // Two alive peers + self = 3-node cluster.
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Publish pp-orchestrator now that the cluster is non-empty: registering
    // earlier would size the dissemination budget for a one-node cluster, and
    // the entry would exhaust its budget before either child could observe it
    // via SWIM piggyback gossip. Doing it post-convergence gives the registry
    // a budget sized for the real cluster.
    driver
        .node_mut()
        .register_name(ORCHESTRATOR_NAME.into(), inbox_addr);
    eprintln!("pp-smoke-run: registered {ORCHESTRATOR_NAME} -> {inbox_addr:?}");
    if !converged {
        eprintln!("pp-smoke-run: cluster did not converge in 90s");
        return 1;
    }
    eprintln!("pp-smoke-run: cluster converged");

    // Resolve pp-entry and wire a route to stage 0.
    eprintln!("pp-smoke-run: resolving {ENTRY_NAME}...");
    let resolve_deadline = Instant::now() + Duration::from_secs(60);
    let (stage0_addr, stage0_node_id) = loop {
        driver.recv();
        driver.tick();
        if let Some((addr, node_id)) = driver.node().resolve_name(ENTRY_NAME) {
            break (addr, node_id);
        }
        if Instant::now() >= resolve_deadline {
            eprintln!("pp-smoke-run: failed to resolve {ENTRY_NAME} in 60s");
            return 1;
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
            return 1;
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
        return 1;
    }

    let result = await_response(
        &mut driver,
        &rt,
        &codecs,
        &response_inbox,
        Duration::from_secs(180),
        Some(&mut guard),
    );

    let code = match result {
        Ok(text) => {
            println!("=== pipeline-parallel Inference Response ===");
            println!("{text}");
            println!("============================================");
            0
        }
        Err(e) => {
            eprintln!("pp-smoke-run: {e}");
            1
        }
    };

    driver.shutdown();
    code
    // guard drops here, killing children
}

fn await_response(
    driver: &mut IrohDriver,
    rt: &Runtime,
    codecs: &Arc<swactor::transport::CodecRegistry>,
    inbox: &Inbox<InferenceResponse>,
    timeout: Duration,
    children: Option<&mut ChildGuard>,
) -> Result<String, String> {
    let msg_pump = ActorMessagePump::new();
    let start = Instant::now();
    let mut last_diag = Instant::now();
    // Re-borrow so we can poll inside the loop without moving the option.
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
            for child in guard.children.iter_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        return Err(format!(
                            "child pid {} exited prematurely with {:?}",
                            child.id(),
                            status
                        ));
                    }
                    Ok(None) => {}
                    Err(e) => {
                        return Err(format!(
                            "failed to poll child pid {}: {e}",
                            child.id()
                        ));
                    }
                }
            }
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
        relay_mode: RelayMode::Default,
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

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    eprintln!("pp-smoke-run (--vastai): orchestrator node {my_hex}");

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
    } else {
        eprintln!("pp-smoke-run: no relay URL after 20s — vastai mode usually requires one");
    }

    let base_url = "https://cloud.vast.ai";
    let http = reqwest::Client::new();

    // Find two distinct offers.
    eprintln!("pp-smoke-run: finding {} offers (x2)...", args.gpu_name);
    let offer_0 = match tokio_rt.block_on(
        pipeline_parallel_inference::vastai::find_offer(
            &http,
            base_url,
            &api_key,
            &args.gpu_name,
            &[],
        ),
    ) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("pp-smoke-run: find_offer #1 failed: {e}");
            return 1;
        }
    };
    let offer_1 = match tokio_rt.block_on(
        pipeline_parallel_inference::vastai::find_offer(
            &http,
            base_url,
            &api_key,
            &args.gpu_name,
            &[offer_0.id],
        ),
    ) {
        Ok(o) => o,
        Err(e) => {
            eprintln!("pp-smoke-run: find_offer #2 failed: {e}");
            return 1;
        }
    };
    eprintln!(
        "pp-smoke-run: offers {}/{} @ ${:.3}/hr + ${:.3}/hr",
        offer_0.id, offer_1.id, offer_0.dph_total, offer_1.dph_total
    );

    // Rent both instances. On error, the helper rolls back any successful one.
    let created = match tokio_rt.block_on(
        pipeline_parallel_inference::vastai::create_pipeline_instances(
            &http,
            base_url,
            &api_key,
            &[offer_0.id, offer_1.id],
            &my_hex,
            relay_url.as_deref(),
            &args.image,
        ),
    ) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("pp-smoke-run: create_pipeline_instances failed: {e}");
            return 1;
        }
    };
    let contract_ids: Vec<u64> = created.iter().map(|c| c.contract_id).collect();
    eprintln!("pp-smoke-run: rented contracts {contract_ids:?}");

    // Helper that always destroys instances on the way out.
    let destroy = |code: i32| -> i32 {
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
    };

    // Wait until both are running.
    let polling = Duration::from_secs(10);
    for c in &contract_ids {
        match tokio_rt.block_on(
            pipeline_parallel_inference::vastai::wait_for_running(
                &http, base_url, &api_key, *c, polling, 60,
            ),
        ) {
            Ok(_) => eprintln!("pp-smoke-run: contract {c} running"),
            Err(e) => {
                eprintln!("pp-smoke-run: contract {c} did not reach running: {e}");
                return destroy(1);
            }
        }
    }

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
            return destroy(1);
        }
    };
    let inbox_addr = *response_inbox.addr();

    // Wait for cluster convergence (both rented nodes join via the relay).
    // Registering pp-orchestrator must happen *after* convergence so the
    // dissemination budget is sized for the real cluster — see run_seed.
    eprintln!("pp-smoke-run: waiting for SWIM convergence...");
    let conv_deadline = Instant::now() + Duration::from_secs(180);
    let mut converged = false;
    while Instant::now() < conv_deadline {
        driver.recv();
        driver.tick();
        let alive = driver
            .snapshot()
            .members
            .iter()
            .filter(|m| m.state == "alive")
            .count();
        if alive >= 2 {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    if !converged {
        eprintln!("pp-smoke-run: cluster did not converge in 180s");
        return destroy(1);
    }

    driver
        .node_mut()
        .register_name(ORCHESTRATOR_NAME.into(), inbox_addr);
    eprintln!("pp-smoke-run: registered {ORCHESTRATOR_NAME} -> {inbox_addr:?}");

    // Resolve stage 0. Bumped to 300s for vast.ai cold starts: stage 0 only
    // registers pp-entry after stage 1's worker becomes ready, and the
    // worker spends most of its boot fetching the GGUF and realizing the
    // tinygrad model graph on a cold cache.
    let resolve_deadline = Instant::now() + Duration::from_secs(300);
    let (stage0_addr, stage0_node_id) = loop {
        driver.recv();
        driver.tick();
        if let Some((addr, node_id)) = driver.node().resolve_name(ENTRY_NAME) {
            break (addr, node_id);
        }
        if Instant::now() >= resolve_deadline {
            eprintln!("pp-smoke-run: failed to resolve {ENTRY_NAME}");
            return destroy(1);
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    let key = match PublicKey::from_bytes(&stage0_node_id.0) {
        Ok(k) => k,
        Err(e) => {
            eprintln!("pp-smoke-run: invalid stage-0 node key: {e}");
            return destroy(1);
        }
    };
    let route = Arc::new(IrohActorTransport::new(
        driver.endpoint().clone(),
        iroh::EndpointAddr::from(key),
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
        return destroy(1);
    }

    let result = await_response(
        &mut driver,
        &rt,
        &codecs,
        &response_inbox,
        Duration::from_secs(600),
        None,
    );

    let code = match result {
        Ok(text) => {
            println!("=== pipeline-parallel Inference Response ===");
            println!("{text}");
            println!("============================================");
            0
        }
        Err(e) => {
            eprintln!("pp-smoke-run: {e}");
            1
        }
    };
    driver.shutdown();
    destroy(code)
}
