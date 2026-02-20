//! sim-cluster: multi-process cluster tests using iroh transport.
//!
//! Spawns 5 swactor nodes with iroh transport connected through a local
//! relay server, then runs the same scenarios as `tests/docker/tests/cluster.rs`
//! without Docker.

use std::fs;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use crate::workspace_root;

// ── Constants ───────────────────────────────────────────────────────────

const NODE_COUNT: usize = 5;
const ACTORS_PER_NODE: usize = 2;
const TIMEOUT: Duration = Duration::from_secs(30);

/// Dashboard HTTP ports — high ports to avoid conflicts.
fn dashboard_port(index: usize) -> u16 {
    19091 + index as u16
}

fn all_dashboard_ports(node_count: usize) -> Vec<u16> {
    (0..node_count).map(dashboard_port).collect()
}

// ── Local relay server ──────────────────────────────────────────────────

/// Owns a tokio runtime + iroh-relay server. RAII cleanup on drop.
struct RelayServer {
    _server: iroh_relay::server::Server,
    _rt: tokio::runtime::Runtime,
    port: u16,
}

impl RelayServer {
    fn start() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("failed to create tokio runtime for relay");

        let server = rt.block_on(async {
            iroh_relay::server::Server::spawn(iroh_relay::server::ServerConfig::<(), ()> {
                relay: Some(iroh_relay::server::RelayConfig {
                    http_bind_addr: (Ipv4Addr::LOCALHOST, 0).into(),
                    tls: None,
                    limits: Default::default(),
                    key_cache_capacity: Some(256),
                    access: iroh_relay::server::AccessConfig::Everyone,
                }),
                quic: None,
                metrics_addr: None,
            })
            .await
        })
        .expect("failed to spawn relay server");

        let addr = server.http_addr().expect("relay has no HTTP address");
        let port = addr.port();

        RelayServer {
            _server: server,
            _rt: rt,
            port,
        }
    }
}

// ── Build ───────────────────────────────────────────────────────────────

fn build_swactor(root: &Path) -> PathBuf {
    let status = Command::new("cargo")
        .args(["build", "-p", "swactor-node"])
        .current_dir(root)
        .status()
        .expect("failed to run cargo build");
    if !status.success() {
        eprintln!("cargo build failed");
        std::process::exit(1);
    }

    let binary = root.join("target/debug/swactor");
    if !binary.exists() {
        eprintln!("binary not found at {}", binary.display());
        std::process::exit(1);
    }
    binary
}

// ── Config generation ───────────────────────────────────────────────────

fn write_node_config(
    dir: &Path,
    index: usize,
    relay_port: u16,
    seed_public_key: Option<&str>,
) -> PathBuf {
    let identity_dir = dir.join("identity");
    fs::create_dir_all(&identity_dir).expect("failed to create identity dir");

    let config_path = dir.join("node.toml");
    let dashboard = dashboard_port(index);

    let mut config = format!(
        r#"dashboard_port = {dashboard}
actors = {ACTORS_PER_NODE}
no_datastore = true
identity_dir = "{identity}"
relay = false
relay_port = {relay_port}
relay_hosts = ["127.0.0.1"]
"#,
        identity = identity_dir.display(),
    );

    if let Some(seed_id) = seed_public_key {
        config.push_str(&format!("seed_node_id = \"{seed_id}\"\n"));
    }

    fs::write(&config_path, &config).expect("failed to write node config");
    config_path
}

// ── Seed key discovery ──────────────────────────────────────────────────

fn read_seed_public_key(run_dir: &Path) -> String {
    let key_path = run_dir.join("node-0/identity/node.key.json");
    let deadline = Instant::now() + Duration::from_secs(10);

    loop {
        if Instant::now() > deadline {
            panic!(
                "timed out waiting for seed key file: {}",
                key_path.display()
            );
        }

        if key_path.exists() {
            if let Ok(data) = fs::read_to_string(&key_path) {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&data) {
                    if let Some(pk) = json.get("public_key").and_then(|v| v.as_str()) {
                        return pk.to_string();
                    }
                }
            }
        }

        thread::sleep(Duration::from_millis(100));
    }
}

// ── Dashboard readiness ─────────────────────────────────────────────────

fn wait_for_dashboard(port: u16, timeout: Duration) {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            panic!("timed out waiting for dashboard on port {port}");
        }
        if poll_distribution(port).is_some() {
            return;
        }
        thread::sleep(Duration::from_millis(200));
    }
}

// ── Cluster handle (RAII) ───────────────────────────────────────────────

struct SimCluster {
    children: Vec<(usize, Child)>,
    run_dir: PathBuf,
    binary: PathBuf,
    relay: RelayServer,
    seed_public_key: String,
    _node_count: usize,
}

impl SimCluster {
    fn spawn(binary: &Path, run_dir: &Path, node_count: usize) -> Self {
        fs::create_dir_all(run_dir).expect("failed to create run dir");

        // Phase 0: start relay
        let relay = RelayServer::start();
        println!(
            "    relay at http://127.0.0.1:{}/ (port {})",
            relay.port, relay.port
        );

        let mut children = Vec::new();

        // Phase 1: spawn seed node (index 0) — no seed_node_id
        let child = spawn_node(binary, run_dir, 0, relay.port, None);
        children.push((0, child));

        // Phase 2: wait for seed's key file
        let seed_public_key = read_seed_public_key(run_dir);
        println!("    seed key: {seed_public_key}");

        // Wait for seed's dashboard to be ready before spawning joiners
        wait_for_dashboard(dashboard_port(0), Duration::from_secs(15));

        // Phase 3: spawn remaining nodes with seed_node_id
        for i in 1..node_count {
            let child = spawn_node(binary, run_dir, i, relay.port, Some(&seed_public_key));
            children.push((i, child));
        }

        SimCluster {
            children,
            run_dir: run_dir.to_path_buf(),
            binary: binary.to_path_buf(),
            relay,
            seed_public_key,
            _node_count: node_count,
        }
    }

    fn kill_node(&mut self, index: usize) {
        if let Some(pos) = self.children.iter().position(|(i, _)| *i == index) {
            let (_, mut child) = self.children.remove(pos);
            let _ = signal_term(child.id());
            let _ = child.wait();
        }
    }

    fn restart_node(&mut self, index: usize) {
        // Non-seed nodes need the seed's public key; the seed itself doesn't
        let seed_id = if index != 0 {
            Some(self.seed_public_key.as_str())
        } else {
            None
        };
        let child = spawn_node(
            &self.binary,
            &self.run_dir,
            index,
            self.relay.port,
            seed_id,
        );
        self.children.push((index, child));
    }
}

impl Drop for SimCluster {
    fn drop(&mut self) {
        for (_, child) in &self.children {
            let _ = signal_term(child.id());
        }
        for (_, child) in &mut self.children {
            let _ = child.wait();
        }
        // relay drops automatically via RelayServer Drop
        // NOTE: leaving run_dir for debugging; uncomment below for production
        // let _ = fs::remove_dir_all(&self.run_dir);
    }
}

fn spawn_node(
    binary: &Path,
    run_dir: &Path,
    index: usize,
    relay_port: u16,
    seed_public_key: Option<&str>,
) -> Child {
    let node_dir = run_dir.join(format!("node-{index}"));
    let config_path = write_node_config(&node_dir, index, relay_port, seed_public_key);

    let log_path = node_dir.join("stderr.log");
    let log_file = fs::File::create(&log_path)
        .unwrap_or_else(|e| panic!("failed to create log for node {index}: {e}"));

    Command::new(binary)
        .args(["--config", &config_path.to_string_lossy()])
        .stdout(Stdio::null())
        .stderr(Stdio::from(log_file))
        .spawn()
        .unwrap_or_else(|e| panic!("failed to spawn node {index}: {e}"))
}

fn signal_term(pid: u32) -> std::io::Result<()> {
    unsafe { libc::kill(pid as i32, libc::SIGTERM) };
    Ok(())
}

// ── HTTP polling (decoupled from distribution crate) ────────────────────

fn poll_distribution(port: u16) -> Option<serde_json::Value> {
    let url = format!("http://127.0.0.1:{port}/api/distribution");
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .ok()?;
    let resp = client.get(&url).send().ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let text = resp.text().ok()?;
    if text == "{}" {
        return None;
    }
    serde_json::from_str(&text).ok()
}

fn get_usize(val: &serde_json::Value, key: &str) -> Option<usize> {
    val.get(key).and_then(|v| v.as_u64()).map(|n| n as usize)
}

fn wait_for_convergence(
    ports: &[u16],
    expected_alive: usize,
    timeout: Duration,
) -> Result<Duration, String> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            let mut diag = String::from("Convergence timeout. Last seen: ");
            for &port in ports {
                match poll_distribution(port) {
                    Some(snap) => {
                        let alive = get_usize(&snap, "alive_count").unwrap_or(0);
                        diag.push_str(&format!("port {port}={alive}, "));
                    }
                    None => diag.push_str(&format!("port {port}=unreachable, ")),
                }
            }
            return Err(diag);
        }

        let all_converged = ports.iter().all(|&port| {
            poll_distribution(port)
                .and_then(|snap| get_usize(&snap, "alive_count"))
                .map(|alive| alive >= expected_alive)
                .unwrap_or(false)
        });

        if all_converged {
            return Ok(start.elapsed());
        }

        thread::sleep(Duration::from_secs(1));
    }
}

fn wait_for_death_detection(
    ports: &[u16],
    max_alive: usize,
    timeout: Duration,
) -> Result<Duration, String> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            let mut diag = String::from("Death detection timeout. Last seen: ");
            for &port in ports {
                match poll_distribution(port) {
                    Some(snap) => {
                        let alive = get_usize(&snap, "alive_count").unwrap_or(0);
                        diag.push_str(&format!("port {port}={alive} alive, "));
                    }
                    None => diag.push_str(&format!("port {port}=unreachable, ")),
                }
            }
            return Err(diag);
        }

        let all_detected = ports.iter().all(|&port| {
            poll_distribution(port)
                .and_then(|snap| get_usize(&snap, "alive_count"))
                .map(|alive| alive <= max_alive)
                .unwrap_or(false)
        });

        if all_detected {
            return Ok(start.elapsed());
        }

        thread::sleep(Duration::from_secs(1));
    }
}

fn wait_for_dead_count(ports: &[u16], min_dead: usize, timeout: Duration) -> bool {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }

        let any_sees_dead = ports.iter().any(|&port| {
            poll_distribution(port)
                .and_then(|snap| get_usize(&snap, "dead_count"))
                .map(|dead| dead >= min_dead)
                .unwrap_or(false)
        });

        if any_sees_dead {
            return true;
        }

        thread::sleep(Duration::from_secs(1));
    }
}

// ── Scenarios ───────────────────────────────────────────────────────────

fn scenario_cluster_convergence(binary: &Path, base_dir: &Path) -> Result<(), String> {
    let run_dir = base_dir.join("scenario-1");
    let cluster = SimCluster::spawn(binary, &run_dir, NODE_COUNT);
    let ports = all_dashboard_ports(NODE_COUNT);

    // alive_count excludes self, so each node sees NODE_COUNT - 1 peers
    let expected_alive = NODE_COUNT - 1;
    let elapsed = wait_for_convergence(&ports, expected_alive, TIMEOUT)?;
    println!("    converged in {:.1}s", elapsed.as_secs_f64());

    // Verify each node's snapshot
    for (i, &port) in ports.iter().enumerate() {
        let snap = poll_distribution(port)
            .ok_or_else(|| format!("node {i} (port {port}) unreachable after convergence"))?;
        let alive = get_usize(&snap, "alive_count").unwrap_or(0);
        let routing = get_usize(&snap, "routing_table_size").unwrap_or(0);
        if alive < expected_alive {
            return Err(format!("node {i} sees {alive} alive, expected >= {expected_alive}"));
        }
        if routing < NODE_COUNT - 1 {
            return Err(format!(
                "node {i} has routing_table_size {routing}, expected >= {}",
                NODE_COUNT - 1
            ));
        }
    }

    drop(cluster);
    Ok(())
}

fn scenario_node_death_detection(binary: &Path, base_dir: &Path) -> Result<(), String> {
    let run_dir = base_dir.join("scenario-2");
    let mut cluster = SimCluster::spawn(binary, &run_dir, NODE_COUNT);
    let ports = all_dashboard_ports(NODE_COUNT);

    let expected_alive = NODE_COUNT - 1;
    wait_for_convergence(&ports, expected_alive, TIMEOUT)
        .map_err(|e| format!("pre-kill convergence failed: {e}"))?;

    // Kill node 2
    cluster.kill_node(2);

    // Survivors: all except index 2
    let survivor_ports: Vec<u16> = (0..NODE_COUNT)
        .filter(|&i| i != 2)
        .map(dashboard_port)
        .collect();

    // Wait for alive count to drop (alive_count excludes self, so 5-node cluster
    // sees 4 alive; after killing 1, survivors should see <= 3)
    let elapsed = wait_for_death_detection(&survivor_ports, NODE_COUNT - 2, TIMEOUT)?;
    println!("    detected in {:.1}s", elapsed.as_secs_f64());

    // Poll for dead_count — SWIM transitions suspect→dead with a delay
    let dead_detected = wait_for_dead_count(&survivor_ports, 1, TIMEOUT);
    if !dead_detected {
        return Err("no survivor detected a dead member".into());
    }

    drop(cluster);
    Ok(())
}

fn scenario_killed_node_rejoins(binary: &Path, base_dir: &Path) -> Result<(), String> {
    let run_dir = base_dir.join("scenario-3");
    let mut cluster = SimCluster::spawn(binary, &run_dir, NODE_COUNT);
    let ports = all_dashboard_ports(NODE_COUNT);

    let expected_alive = NODE_COUNT - 1;
    wait_for_convergence(&ports, expected_alive, TIMEOUT)
        .map_err(|e| format!("pre-kill convergence failed: {e}"))?;

    // Kill node 2
    cluster.kill_node(2);

    let survivor_ports: Vec<u16> = (0..NODE_COUNT)
        .filter(|&i| i != 2)
        .map(dashboard_port)
        .collect();
    wait_for_death_detection(&survivor_ports, NODE_COUNT - 2, TIMEOUT)
        .map_err(|e| format!("death detection failed: {e}"))?;

    // Restart node 2 — identity persists, so cluster recognizes it
    cluster.restart_node(2);

    let rejoined_port = dashboard_port(2);
    let elapsed = wait_for_convergence(&[rejoined_port], 1, TIMEOUT)?;
    println!("    rejoined in {:.1}s", elapsed.as_secs_f64());

    let snap = poll_distribution(rejoined_port)
        .ok_or("rejoined node unreachable")?;
    let alive = get_usize(&snap, "alive_count").unwrap_or(0);
    if alive < 1 {
        return Err(format!("rejoined node sees {alive} alive, expected >= 1"));
    }

    drop(cluster);
    Ok(())
}

fn scenario_actors_resolvable(binary: &Path, base_dir: &Path) -> Result<(), String> {
    let run_dir = base_dir.join("scenario-4");
    let cluster = SimCluster::spawn(binary, &run_dir, NODE_COUNT);
    let ports = all_dashboard_ports(NODE_COUNT);

    let expected_alive = NODE_COUNT - 1;
    wait_for_convergence(&ports, expected_alive, TIMEOUT)
        .map_err(|e| format!("convergence failed: {e}"))?;

    let mut total_directory_entries = 0usize;

    for (i, &port) in ports.iter().enumerate() {
        let snap = poll_distribution(port)
            .ok_or_else(|| format!("node {i} unreachable"))?;
        let dir_count = get_usize(&snap, "directory_entry_count").unwrap_or(0);
        if dir_count < ACTORS_PER_NODE {
            return Err(format!(
                "node {i} has {dir_count} directory entries, expected >= {ACTORS_PER_NODE}"
            ));
        }
        total_directory_entries += dir_count;
    }

    let expected_total = NODE_COUNT * ACTORS_PER_NODE;
    if total_directory_entries < expected_total {
        return Err(format!(
            "total directory entries {total_directory_entries}, expected >= {expected_total}"
        ));
    }
    println!("    total: {total_directory_entries} directory entries");

    drop(cluster);
    Ok(())
}

// ── Interactive mode ─────────────────────────────────────────────────────

pub fn run_interactive(node_count: usize) {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Condvar, Mutex};

    let root = workspace_root();

    println!("=== sim-cluster: building swactor (iroh) ===");
    let binary = build_swactor(&root);

    let base_dir = root.join(".sim-cluster");
    let _ = fs::remove_dir_all(&base_dir);

    let run_dir = base_dir.join("interactive");
    println!("=== sim-cluster: spawning {node_count} nodes ===");
    let cluster = SimCluster::spawn(&binary, &run_dir, node_count);

    let ports = all_dashboard_ports(node_count);

    // Wait for all dashboards
    println!("=== sim-cluster: waiting for dashboards ===");
    for &port in &ports {
        wait_for_dashboard(port, Duration::from_secs(30));
    }

    // Wait for convergence
    println!("=== sim-cluster: waiting for convergence ===");
    let expected_alive = node_count - 1;
    let convergence = wait_for_convergence(&ports, expected_alive, TIMEOUT);

    let converged_msg = match &convergence {
        Ok(elapsed) => format!("{node_count} nodes, all converged in {:.1}s", elapsed.as_secs_f64()),
        Err(e) => format!("{node_count} nodes, convergence issue: {e}"),
    };

    // Print summary
    println!();
    println!("=== sim-cluster ready ===");
    println!("  relay:  http://127.0.0.1:{}/", cluster.relay.port);
    for i in 0..node_count {
        let port = dashboard_port(i);
        let label = if i == 0 { "  (seed)" } else { "" };
        println!("  node-{i}: http://127.0.0.1:{port}/{label}");
    }
    println!("  cluster: {converged_msg}");
    println!();
    println!("  Logs: .sim-cluster/interactive/node-N/stderr.log");
    println!("  Press Ctrl-C to shut down.");

    // Block on Ctrl-C
    let shutdown = Arc::new((Mutex::new(false), Condvar::new()));
    let shutdown2 = Arc::clone(&shutdown);
    let flag = Arc::new(AtomicBool::new(false));
    let flag2 = Arc::clone(&flag);

    unsafe {
        let shutdown_ptr = Arc::into_raw(shutdown2) as usize;
        let flag_ptr = Arc::into_raw(flag2) as usize;
        libc::signal(libc::SIGINT, handler as *const () as libc::sighandler_t);
        SHUTDOWN_PTR.store(shutdown_ptr, Ordering::SeqCst);
        FLAG_PTR.store(flag_ptr, Ordering::SeqCst);
    }

    let (lock, cvar) = &*shutdown;
    let mut stopped = lock.lock().unwrap();
    while !*stopped {
        stopped = cvar.wait(stopped).unwrap();
    }

    println!("\n=== sim-cluster: shutting down ===");
    drop(cluster);
    println!("=== sim-cluster: stopped ===");
}

// Signal handler support for run_interactive
use std::sync::atomic::{AtomicUsize, Ordering};

static SHUTDOWN_PTR: AtomicUsize = AtomicUsize::new(0);
static FLAG_PTR: AtomicUsize = AtomicUsize::new(0);

extern "C" fn handler(_sig: libc::c_int) {
    use std::sync::atomic::AtomicBool;
    use std::sync::{Condvar, Mutex};

    let flag_ptr = FLAG_PTR.load(Ordering::SeqCst);
    if flag_ptr != 0 {
        let flag = unsafe { &*(flag_ptr as *const AtomicBool) };
        if flag.swap(true, Ordering::SeqCst) {
            // Second Ctrl-C — force exit
            std::process::exit(1);
        }
    }

    let ptr = SHUTDOWN_PTR.load(Ordering::SeqCst);
    if ptr != 0 {
        let pair = unsafe { &*(ptr as *const (Mutex<bool>, Condvar)) };
        if let Ok(mut stopped) = pair.0.lock() {
            *stopped = true;
            pair.1.notify_one();
        }
    }
}

// ── Entry point (test mode) ─────────────────────────────────────────────

pub fn run() {
    let root = workspace_root();
    let overall_start = Instant::now();

    println!("=== sim-cluster: building swactor (iroh) ===");
    let binary = build_swactor(&root);

    let base_dir = root.join(".sim-cluster");
    // Clean any stale runs
    let _ = fs::remove_dir_all(&base_dir);

    let scenarios: &[(&str, fn(&Path, &Path) -> Result<(), String>)] = &[
        ("cluster convergence", scenario_cluster_convergence),
        ("node death detection", scenario_node_death_detection),
        ("killed node rejoins", scenario_killed_node_rejoins),
        ("actors resolvable", scenario_actors_resolvable),
    ];

    let total = scenarios.len();
    let mut passed = 0usize;

    for (i, (name, func)) in scenarios.iter().enumerate() {
        println!(
            "=== sim-cluster: scenario {}/{total} \u{2014} {name} ===",
            i + 1
        );
        match func(&binary, &base_dir) {
            Ok(()) => passed += 1,
            Err(e) => {
                let elapsed = overall_start.elapsed();
                eprintln!("    FAILED: {e}");
                eprintln!(
                    "\n--- FAILED after {:.1}s ({passed}/{total} passed) ---",
                    elapsed.as_secs_f64()
                );
                // Leave .sim-cluster for debugging
                std::process::exit(1);
            }
        }
    }

    // Clean up base dir
    let _ = fs::remove_dir_all(&base_dir);

    let elapsed = overall_start.elapsed();
    println!(
        "\n--- All {total} scenario(s) passed in {:.1}s ---",
        elapsed.as_secs_f64()
    );
}
