//! single-gpu-inference — Local-side orchestrator for distributed tinygrad inference.
//!
//! Starts a local iroh node, waits for the remote gpu-node to join and
//! register its `"inference"` name, then sends an `InferenceRequest` through
//! the full distributed pipeline and prints the response.
//!
//! Usage:
//!   # Localhost mode (default): expects gpu-node already running with SEED_ADDR
//!   single-gpu-inference --seed <hex-node-id> [--seed-direct ip:port,...]
//!
//!   # vast.ai mode: rents a GPU, deploys the Docker image, runs the test
//!   single-gpu-inference --vastai --api-key <key> [--gpu RTX_4090]
//!
//! Environment variables:
//!   PYTHON=1  — passed through to worker (CPU fallback, no GPU/clang)

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use distribution::iroh_driver::{IrohDriver, IrohDriverConfig};
use distribution::node::DistributedNodeConfig;
use distribution::registry::RegistryConfig;
use distribution::swim::probe::SwimConfig;
use iroh::{PublicKey, RelayMode};

use swactor::runtime::{Runtime, RuntimeConfig};
use swactor::transport::TransportRouter;

use single_gpu_inference::iroh_transport::{drain_actor_messages, IrohActorTransport, ACTOR_ALPN};
use single_gpu_inference::messages::{inference_codec_registry, InferenceRequest, InferenceResponse};

fn parse_hex_node_id(s: &str) -> [u8; 32] {
    assert!(
        s.len() == 64,
        "node id must be 64 hex characters (32 bytes)"
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
    eprintln!("  single-gpu-inference --seed <hex-node-id> [--seed-direct ip:port,...]");
    eprintln!("  single-gpu-inference --vastai --api-key <key> [--gpu RTX_4090] [--image ghcr.io/user/swactor-gpu:latest]");
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    let mut seed_hex: Option<String> = None;
    let mut seed_direct: Vec<SocketAddr> = Vec::new();
    let mut vastai = false;
    let mut api_key: Option<String> = None;
    let mut gpu_name = "RTX 4090".to_string();
    let mut image = "swactor-gpu:latest".to_string();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--seed" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--seed requires a value");
                    std::process::exit(1);
                }
                seed_hex = Some(args[i].clone());
            }
            "--seed-direct" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--seed-direct requires a value");
                    std::process::exit(1);
                }
                for part in args[i].split(',') {
                    if let Ok(sa) = part.trim().parse::<SocketAddr>() {
                        seed_direct.push(sa);
                    }
                }
            }
            "--vastai" => {
                vastai = true;
            }
            "--api-key" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--api-key requires a value");
                    std::process::exit(1);
                }
                api_key = Some(args[i].trim().to_string());
            }
            "--gpu" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--gpu requires a value");
                    std::process::exit(1);
                }
                gpu_name = args[i].clone();
            }
            "--image" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("--image requires a value");
                    std::process::exit(1);
                }
                image = args[i].clone();
            }
            "--help" | "-h" => {
                print_usage();
                return;
            }
            other => {
                eprintln!("unknown argument: {other}");
                print_usage();
                std::process::exit(1);
            }
        }
        i += 1;
    }

    if vastai {
        run_vastai(api_key.expect("--api-key required with --vastai"), &gpu_name, &image);
    } else {
        let seed = seed_hex.expect("--seed required in localhost mode");
        run_localhost(&seed, &seed_direct);
    }
}

fn run_localhost(seed_hex: &str, seed_direct: &[SocketAddr]) {
    let seed_bytes = parse_hex_node_id(seed_hex);
    let seed_key =
        iroh::PublicKey::from_bytes(&seed_bytes).expect("invalid seed public key");

    // Build seed endpoint address
    let mut seed_addr = iroh::EndpointAddr::from(seed_key);
    for &sa in seed_direct {
        seed_addr = seed_addr.with_ip_addr(sa);
    }

    // Create local iroh driver
    let mut driver = IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Disabled,
        node: node_config(),
        peer_auth: None,
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    })
    .expect("failed to create iroh driver");

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    eprintln!("single-gpu-inference started (node id: {my_hex})");
    eprintln!("joining seed: {seed_hex}");

    driver.join(&[seed_addr.clone()]);

    // Wait for SWIM convergence — the remote node must appear as alive
    eprintln!("waiting for cluster convergence...");
    let start = Instant::now();
    let mut converged = false;
    while start.elapsed() < Duration::from_secs(30) {
        driver.recv();
        driver.tick();

        let peer_key = PublicKey::from_bytes(&seed_bytes).unwrap();
        let snap = driver.snapshot();
        let peer_hex: String = peer_key.as_bytes().iter().map(|b| format!("{:02x}", b)).collect();
        let alive = snap
            .members
            .iter()
            .any(|m| m.node_id == peer_hex && m.state == "alive");
        if alive {
            eprintln!("cluster converged — remote node is alive");
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    if !converged {
        eprintln!("cluster did not converge within 30s");
        driver.shutdown();
        std::process::exit(1);
    }

    // Wait for name resolution — the remote node registers "inference"
    eprintln!("resolving name 'inference'...");
    let start = Instant::now();
    let mut bridge_addr = None;
    while start.elapsed() < Duration::from_secs(30) {
        driver.recv();
        driver.tick();

        if let Some((addr, _node_id)) = driver.node().resolve_name("inference") {
            eprintln!("resolved 'inference' -> {:?}", addr);
            bridge_addr = Some(addr);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let bridge_addr = bridge_addr.expect("failed to resolve 'inference' name within 30s");

    // Create actor runtime and response inbox
    let mut rt = Runtime::new(RuntimeConfig::default());
    let codecs = Arc::new(inference_codec_registry());
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    // Build transport to the remote node
    let transport_to_remote = Arc::new(IrohActorTransport::new(
        driver.endpoint().clone(),
        seed_addr,
        driver.tokio_handle(),
    ));

    // Wire routes: send to bridge_addr on remote
    let router = TransportRouter::new();
    router.add_route(bridge_addr, transport_to_remote);

    rt.set_codec_registry(codecs.clone());
    rt.set_transport_router(Arc::new(router));

    // Send InferenceRequest
    eprintln!("sending InferenceRequest...");
    rt.send_to(
        bridge_addr,
        InferenceRequest {
            prompt: "Say hello".into(),
            max_tokens: 32,
            temperature: 0.7,
            reply_to: inbox_addr,
        },
    )
    .unwrap();

    // Pump loop: drain messages, tick, check for response
    let start = Instant::now();
    let timeout = Duration::from_secs(300); // generous for model download + inference
    let mut got_response = false;

    while start.elapsed() < timeout {
        std::thread::sleep(Duration::from_millis(100));

        driver.recv();
        driver.tick();
        drain_actor_messages(&driver, &codecs, &rt, Duration::from_millis(100));
        rt.tick();

        if let Some(response) = response_inbox.try_recv() {
            if response.text.is_empty() {
                eprintln!("received empty response (worker may not be ready), retrying...");
                continue;
            }
            println!("=== Inference Response ===");
            println!("{}", response.text);
            println!("==========================");
            got_response = true;
            break;
        }
    }

    if !got_response {
        eprintln!("did not receive InferenceResponse within timeout");
        driver.shutdown();
        std::process::exit(1);
    }

    driver.shutdown();
    eprintln!("single-gpu-inference complete");
}

fn run_vastai(api_key_raw: String, gpu_name: &str, image: &str) {
    let api_key = api_key_raw.trim().to_string();
    // Create a tokio runtime for the async vast.ai client
    let tokio_rt = tokio::runtime::Runtime::new().expect("failed to create tokio runtime");

    // Create local iroh driver first so we know our node id
    // Use RelayMode::Default for WAN NAT traversal to vast.ai instances
    let mut driver = IrohDriver::new(IrohDriverConfig {
        secret_key: None,
        relay_mode: RelayMode::Default,
        node: node_config(),
        peer_auth: None,
        additional_alpns: vec![ACTOR_ALPN.to_vec()],
    })
    .expect("failed to create iroh driver");

    let my_id = driver.node_id();
    let my_hex: String = my_id.0.iter().map(|b| format!("{:02x}", b)).collect();
    eprintln!("single-gpu-inference started (node id: {my_hex})");

    let base_url = "https://cloud.vast.ai";
    let client = reqwest::Client::new();

    // Wait for relay connection so the remote gpu-node can find us over the internet
    eprintln!("waiting for relay connection...");
    let relay_url = {
        let start = Instant::now();
        loop {
            driver.recv();
            driver.tick();
            if let Some(url) = driver.home_relay_url() {
                eprintln!("home relay: {url}");
                break Some(url.to_string());
            }
            if start.elapsed() > Duration::from_secs(15) {
                eprintln!("warning: no relay URL available after 15s");
                break None;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    };

    // Retry loop: find offer → create → wait for running → wait for convergence.
    // Broken hosts (CDI errors, GFW, networking) get excluded on retry.
    let max_retries = 3;
    let mut excluded_offers: Vec<u64> = Vec::new();
    let mut running_contract_id = 0u64;
    let mut converged = false;

    for attempt in 1..=max_retries {
        // 1. Find cheapest offer (excluding previously failed ones)
        eprintln!("finding {gpu_name} offer on vast.ai (attempt {attempt}/{max_retries})...");
        let offer = match tokio_rt.block_on(single_gpu_inference::vastai::find_offer(
            &client, base_url, &api_key, gpu_name, &excluded_offers,
        )) {
            Ok(o) => o,
            Err(e) => {
                eprintln!("no offers available: {e}");
                driver.shutdown();
                std::process::exit(1);
            }
        };
        eprintln!(
            "found offer {} ({}, ${:.3}/hr, geo={:?})",
            offer.id, offer.gpu_name, offer.dph_total, offer.geolocation
        );
        let offer_id = offer.id;

        // 2. Create instance with our node id as SEED_ADDR
        eprintln!("creating instance...");
        let instance = match tokio_rt.block_on(single_gpu_inference::vastai::create_instance(
            &client, base_url, &api_key, offer_id, &my_hex,
            relay_url.as_deref(), image,
        )) {
            Ok(i) => i,
            Err(e) => {
                eprintln!("failed to create instance: {e}");
                excluded_offers.push(offer_id);
                continue;
            }
        };
        let contract_id = instance.contract_id;
        eprintln!("instance created (contract: {contract_id})");

        // 3. Wait for instance to be running
        eprintln!("waiting for instance to start...");
        match tokio_rt.block_on(single_gpu_inference::vastai::wait_for_running(
            &client, base_url, &api_key, contract_id,
            Duration::from_secs(10), 60,
        )) {
            Ok(r) => {
                eprintln!("instance running at {}:{}", r.ip, r.port);
                running_contract_id = contract_id;
            }
            Err(e) => {
                eprintln!("instance failed to start: {e}");
                eprintln!("destroying instance and excluding offer {offer_id}...");
                let _ = tokio_rt.block_on(single_gpu_inference::vastai::destroy_instance(
                    &client, base_url, &api_key, contract_id,
                ));
                excluded_offers.push(offer_id);
                continue;
            }
        }

        // 4. Wait for SWIM convergence (gpu-node must connect back via iroh relay)
        eprintln!("waiting for cluster convergence...");
        let start = Instant::now();
        let mut last_log = Instant::now();
        while start.elapsed() < Duration::from_secs(120) {
            driver.recv();
            driver.tick();

            let snap = driver.snapshot();
            if snap.members.iter().any(|m| m.state == "alive") {
                eprintln!("cluster converged");
                converged = true;
                break;
            }
            if last_log.elapsed() >= Duration::from_secs(15) {
                let elapsed = start.elapsed().as_secs();
                let members: Vec<_> = snap.members.iter()
                    .map(|m| format!("{}={}", &m.node_id[..8], m.state))
                    .collect();
                eprintln!("  convergence: {elapsed}s elapsed, members: {members:?}");
                last_log = Instant::now();
            }
            std::thread::sleep(Duration::from_millis(100));
        }

        if converged {
            break;
        }

        // Convergence failed — fetch logs for debugging, then destroy
        eprintln!("cluster did not converge, fetching instance logs...");
        if let Ok(log_url) = tokio_rt.block_on(single_gpu_inference::vastai::request_logs(
            &client, base_url, &api_key, contract_id,
        )) {
            if let Ok(logs) = tokio_rt.block_on(single_gpu_inference::vastai::fetch_logs(&client, &log_url)) {
                eprintln!("--- instance logs (contract {contract_id}) ---");
                // Print last 40 lines to avoid flooding
                let lines: Vec<&str> = logs.lines().collect();
                let start = if lines.len() > 40 { lines.len() - 40 } else { 0 };
                for line in &lines[start..] {
                    eprintln!("  {line}");
                }
                eprintln!("--- end logs ---");
            }
        }
        eprintln!("destroying instance and excluding offer {offer_id}...");
        let _ = tokio_rt.block_on(single_gpu_inference::vastai::destroy_instance(
            &client, base_url, &api_key, contract_id,
        ));
        excluded_offers.push(offer_id);
    }

    if !converged {
        eprintln!("all {max_retries} attempts failed (no host converged), giving up");
        driver.shutdown();
        std::process::exit(1);
    }

    // Resolve "inference" name
    eprintln!("resolving 'inference' name...");
    let start = Instant::now();
    let mut bridge_addr = None;
    let mut remote_node_id = None;
    while start.elapsed() < Duration::from_secs(60) {
        driver.recv();
        driver.tick();
        if let Some((addr, node_id)) = driver.node().resolve_name("inference") {
            bridge_addr = Some(addr);
            remote_node_id = Some(node_id);
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    let bridge_addr = bridge_addr.expect("failed to resolve 'inference' name");
    let remote_node_id = remote_node_id.unwrap();

    // Build transport to remote
    let remote_key = PublicKey::from_bytes(&remote_node_id.0).expect("invalid remote node key");
    let remote_endpoint_addr = iroh::EndpointAddr::from(remote_key);

    let mut rt = Runtime::new(RuntimeConfig::default());
    let codecs = Arc::new(inference_codec_registry());
    let response_inbox = rt.new_inbox::<InferenceResponse>().unwrap();
    let inbox_addr = *response_inbox.addr();

    let transport_to_remote = Arc::new(IrohActorTransport::new(
        driver.endpoint().clone(),
        remote_endpoint_addr,
        driver.tokio_handle(),
    ));
    let router = TransportRouter::new();
    router.add_route(bridge_addr, transport_to_remote);
    rt.set_codec_registry(codecs.clone());
    rt.set_transport_router(Arc::new(router));

    // Send request
    eprintln!("sending InferenceRequest...");
    rt.send_to(
        bridge_addr,
        InferenceRequest {
            prompt: "Say hello".into(),
            max_tokens: 32,
            temperature: 0.7,
            reply_to: inbox_addr,
        },
    )
    .unwrap();

    // Pump loop
    let start = Instant::now();
    let mut last_diag = Instant::now();
    let mut got_response = false;
    while start.elapsed() < Duration::from_secs(300) {
        std::thread::sleep(Duration::from_millis(100));
        driver.recv();
        driver.tick();
        drain_actor_messages(&driver, &codecs, &rt, Duration::from_millis(50));
        rt.tick();

        // Periodic SWIM health diagnostics
        if last_diag.elapsed() >= Duration::from_secs(15) {
            let snap = driver.snapshot();
            let members: Vec<_> = snap.members.iter()
                .map(|m| format!("{}={}", &m.node_id[..8], m.state))
                .collect();
            let elapsed = start.elapsed().as_secs();
            eprintln!("  inference wait: {elapsed}s elapsed, members: {members:?}");
            last_diag = Instant::now();
        }

        if let Some(response) = response_inbox.try_recv() {
            if response.text.is_empty() {
                continue;
            }
            println!("=== Inference Response ===");
            println!("{}", response.text);
            println!("==========================");
            got_response = true;
            break;
        }
    }

    if !got_response {
        eprintln!("did not receive InferenceResponse within timeout");
        // Fetch logs for debugging before destroying
        eprintln!("fetching instance logs...");
        if let Ok(log_url) = tokio_rt.block_on(single_gpu_inference::vastai::request_logs(
            &client, base_url, &api_key, running_contract_id,
        )) {
            if let Ok(logs) = tokio_rt.block_on(single_gpu_inference::vastai::fetch_logs(&client, &log_url)) {
                eprintln!("--- instance logs (contract {running_contract_id}) ---");
                let lines: Vec<&str> = logs.lines().collect();
                let tail = if lines.len() > 60 { lines.len() - 60 } else { 0 };
                for line in &lines[tail..] {
                    eprintln!("  {line}");
                }
                eprintln!("--- end logs ---");
            }
        }
    }

    // 5. Destroy instance
    eprintln!("destroying instance...");
    let _ = tokio_rt.block_on(single_gpu_inference::vastai::destroy_instance(
        &client,
        base_url,
        &api_key,
        running_contract_id,
    ));

    driver.shutdown();
    if got_response {
        eprintln!("single-gpu-inference complete");
    } else {
        std::process::exit(1);
    }
}
