use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use serde::Deserialize;

// ── Config structs ──────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct DeployConfig {
    pub defaults: DeployDefaults,
    pub machines: Vec<MachineConfig>,
}

#[derive(Deserialize, Clone)]
pub struct DeployDefaults {
    #[serde(default)]
    pub image: String,
    #[serde(default)]
    pub container: String,
    pub dashboard_port: u16,
    #[serde(default = "default_relay_port")]
    pub relay_port: u16,
    #[serde(default)]
    pub swactor_flags: Vec<String>,
    #[serde(default)]
    pub relay_hosts: Vec<String>,
    #[serde(default = "default_true")]
    pub introduce_peers: bool,
    #[serde(default = "default_health_timeout")]
    pub health_timeout_secs: u64,
    #[serde(default = "default_convergence_timeout")]
    pub convergence_timeout_secs: u64,
}

fn default_true() -> bool { true }
fn default_relay_port() -> u16 { 3340 }
fn default_health_timeout() -> u64 { 30 }
fn default_convergence_timeout() -> u64 { 60 }

#[derive(Deserialize, Clone)]
pub struct MachineConfig {
    pub name: String,
    /// SSH destination — an alias from ~/.ssh/config (e.g. "thinkpad") or a hostname/IP.
    /// Ignored when `local = true`.
    #[serde(default)]
    pub ssh: String,
    /// When true, run docker commands directly instead of over SSH.
    #[serde(default)]
    pub local: bool,
    pub dashboard_port: Option<u16>,
    pub relay_port: Option<u16>,
    pub container: Option<String>,
    pub swactor_flags: Option<Vec<String>>,
}

impl MachineConfig {
    fn effective_port(&self, defaults: &DeployDefaults) -> u16 {
        self.dashboard_port.unwrap_or(defaults.dashboard_port)
    }

    fn effective_relay_port(&self, defaults: &DeployDefaults) -> u16 {
        self.relay_port.unwrap_or(defaults.relay_port)
    }

    fn effective_container(&self, defaults: &DeployDefaults) -> String {
        self.container.clone().unwrap_or_else(|| defaults.container.clone())
    }

    fn effective_flags(&self, defaults: &DeployDefaults) -> Vec<String> {
        self.swactor_flags.clone().unwrap_or_else(|| defaults.swactor_flags.clone())
    }
}

// ── SSH helpers ─────────────────────────────────────────────────────────────

fn ssh_cmd(machine: &MachineConfig, remote_cmd: &str) -> Command {
    let mut cmd = Command::new("ssh");
    cmd.arg(&machine.ssh);
    cmd.arg(remote_cmd);
    cmd
}

fn transfer_image(machine: &MachineConfig, archive: &Path) -> Result<(), String> {
    if machine.local {
        println!("  Loading image locally on {}...", machine.name);
        let pipe_cmd = format!("gunzip < {} | docker load", archive.display());
        let status = Command::new("sh")
            .arg("-c")
            .arg(&pipe_cmd)
            .status()
            .map_err(|e| format!("failed to run docker load: {e}"))?;
        if !status.success() {
            return Err(format!("local docker load on {} failed", machine.name));
        }
    } else {
        println!("  Copying image to {}...", machine.name);
        let status = Command::new("scp")
            .arg(archive.as_os_str())
            .arg(format!("{}:/tmp/swactor-deploy.tar.gz", machine.ssh))
            .status()
            .map_err(|e| format!("scp failed: {e}"))?;
        if !status.success() {
            return Err(format!("scp to {} failed", machine.name));
        }

        println!("  Loading image on {}...", machine.name);
        let status = Command::new("ssh")
            .arg(&machine.ssh)
            .arg("gunzip < /tmp/swactor-deploy.tar.gz | docker load")
            .status()
            .map_err(|e| format!("docker load on {} failed: {e}", machine.name))?;
        if !status.success() {
            return Err(format!("docker load on {} failed", machine.name));
        }
    }
    println!("  Image loaded on {}", machine.name);
    Ok(())
}

// ── HTTP helpers (via curl, optionally over SSH) ────────────────────────────

/// Build a curl GET url, running locally or over SSH depending on the machine.
fn machine_curl_get(machine: &MachineConfig, defaults: &DeployDefaults, path: &str) -> Option<String> {
    let port = machine.effective_port(defaults);
    let url = format!("http://localhost:{port}{path}");

    let output = if machine.local {
        Command::new("curl")
            .args(["-s", "--max-time", "3", &url])
            .output()
            .ok()?
    } else {
        let curl_cmd = format!("curl -s --max-time 3 '{url}'");
        Command::new("ssh")
            .arg(&machine.ssh)
            .arg(&curl_cmd)
            .output()
            .ok()?
    };

    if !output.status.success() {
        return None;
    }
    let body = String::from_utf8_lossy(&output.stdout).to_string();
    if body.is_empty() || body == "{}" {
        return None;
    }
    Some(body)
}

fn machine_curl_post(machine: &MachineConfig, defaults: &DeployDefaults, path: &str, json_body: &str) -> Result<String, String> {
    let port = machine.effective_port(defaults);
    let url = format!("http://localhost:{port}{path}");

    let output = if machine.local {
        let mut cmd = Command::new("curl");
        cmd.args(["-s", "--max-time", "5", "-X", "POST"]);
        cmd.args(["-H", "Content-Type: application/json"]);
        cmd.arg("-d").arg(json_body);
        cmd.arg(&url);
        cmd.output().map_err(|e| format!("curl failed: {e}"))?
    } else {
        // Escape single quotes in json_body for the shell
        let escaped = json_body.replace('\'', "'\\''");
        let curl_cmd = format!(
            "curl -s --max-time 5 -X POST -H 'Content-Type: application/json' -d '{escaped}' '{url}'"
        );
        Command::new("ssh")
            .arg(&machine.ssh)
            .arg(&curl_cmd)
            .output()
            .map_err(|e| format!("ssh curl failed: {e}"))?
    };

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(format!("POST {url} failed: {stderr}"));
    }
    Ok(String::from_utf8_lossy(&output.stdout).to_string())
}

// ── Deploy orchestration ────────────────────────────────────────────────────

pub fn run_deploy(
    root: &Path,
    config_path: &str,
    skip_build: bool,
    skip_verify: bool,
    skip_peers: bool,
) {
    // Phase 1: Load config
    println!("=== Phase 1: Loading config ===\n");
    let config = load_deploy_config(root, config_path);
    println!("  Loaded {} machine(s): {}", config.machines.len(),
        config.machines.iter().map(|m| m.name.as_str()).collect::<Vec<_>>().join(", "));

    println!();

    // Phase 2: Build image
    let archive = std::env::temp_dir().join("swactor-deploy.tar.gz");
    if !skip_build {
        println!("=== Phase 2: Building Docker image ===\n");
        build_image(root, &config.defaults.image, &archive);
        println!();
    } else {
        println!("=== Phase 2: Skipping build ===\n");
        if !archive.exists() {
            eprintln!("Warning: --skip-build but {} does not exist", archive.display());
            eprintln!("  Run without --skip-build first, or ensure the archive exists.\n");
        }
    }

    // Phase 3: Deploy to each machine
    println!("=== Phase 3: Deploying to machines ===\n");
    for machine in &config.machines {
        deploy_to_machine(machine, &config.defaults, &archive);
    }
    println!();

    // Phase 4: Health check
    if !skip_verify {
        println!("=== Phase 4: Health check ===\n");
        for machine in &config.machines {
            health_check(machine, &config.defaults);
        }
        println!();
    }

    // Phase 4b: Inject relay_hosts into container configs
    if !config.defaults.relay_hosts.is_empty() {
        println!("=== Phase 4b: Injecting relay_hosts config ===\n");
        inject_relay_hosts(&config.machines, &config.defaults);
        println!();
    }

    // Phase 5: Peer introduction + convergence
    if !skip_verify && !skip_peers && config.defaults.introduce_peers {
        println!("=== Phase 5: Peer introduction ===\n");
        let (node_info, seed_index) = introduce_peers(&config.machines, &config.defaults);

        println!("\n=== Phase 5b: Waiting for convergence ===\n");
        wait_for_cluster_convergence(&config.machines, &config.defaults, &node_info, seed_index);
        println!();
    }

    // Phase 6: Report
    println!("=== Phase 6: Cluster status ===\n");
    print_cluster_status(&config.machines, &config.defaults);
}

fn load_deploy_config(root: &Path, config_path: &str) -> DeployConfig {
    let path = if Path::new(config_path).is_absolute() {
        config_path.to_string()
    } else {
        root.join(config_path).to_string_lossy().to_string()
    };

    let content = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        eprintln!("Failed to read config '{}': {e}", path);
        eprintln!("Create a deploy.toml in the workspace root. See the plan for the format.");
        std::process::exit(1);
    });

    let config: DeployConfig = toml::from_str(&content).unwrap_or_else(|e| {
        eprintln!("Failed to parse config '{}': {e}", path);
        std::process::exit(1);
    });

    if config.machines.is_empty() {
        eprintln!("Error: deploy config has no [[machines]] entries");
        std::process::exit(1);
    }

    config
}

fn build_binary(root: &Path) -> std::path::PathBuf {
    println!("  Building swactor-node (musl, static, release)...");
    let status = Command::new("cargo")
        .args(["build", "--release", "-p", "swactor-node", "--target", "x86_64-unknown-linux-musl"])
        .current_dir(root)
        .status()
        .unwrap_or_else(|e| {
            eprintln!("Failed to run cargo build: {e}");
            std::process::exit(1);
        });
    if !status.success() {
        eprintln!("cargo build failed");
        std::process::exit(1);
    }
    root.join("target/x86_64-unknown-linux-musl/release/swactor")
}

fn build_image(root: &Path, image: &str, archive: &Path) {
    if image.is_empty() {
        eprintln!("Error: 'image' must be set in deploy.toml [defaults] for --docker mode");
        std::process::exit(1);
    }
    build_binary(root);

    println!("  Packaging Docker image '{image}'...");
    let status = Command::new("docker")
        .args(["build", "-t", image, "."])
        .current_dir(root)
        .status()
        .unwrap_or_else(|e| {
            eprintln!("Failed to run docker build: {e}");
            std::process::exit(1);
        });
    if !status.success() {
        eprintln!("docker build failed");
        std::process::exit(1);
    }

    println!("  Saving image to {}...", archive.display());
    let pipe_cmd = format!(
        "docker save {} | gzip > {}",
        image,
        archive.display()
    );
    let status = Command::new("sh")
        .arg("-c")
        .arg(&pipe_cmd)
        .status()
        .unwrap_or_else(|e| {
            eprintln!("Failed to save docker image: {e}");
            std::process::exit(1);
        });
    if !status.success() {
        eprintln!("docker save | gzip failed");
        std::process::exit(1);
    }

    let size = std::fs::metadata(archive).map(|m| m.len()).unwrap_or(0);
    println!("  Image archive: {:.1} MB", size as f64 / 1_048_576.0);
}

fn deploy_to_machine(machine: &MachineConfig, defaults: &DeployDefaults, archive: &Path) {
    if defaults.image.is_empty() || defaults.container.is_empty() {
        eprintln!("Error: 'image' and 'container' must be set in deploy.toml [defaults] for --docker mode");
        std::process::exit(1);
    }
    let container = machine.effective_container(defaults);
    let port = machine.effective_port(defaults);
    let relay_port = machine.effective_relay_port(defaults);
    let flags = machine.effective_flags(defaults);
    let image = &defaults.image;

    println!("  Deploying to {}...", machine.name);

    // Step 1: Transfer image
    if let Err(e) = transfer_image(machine, archive) {
        eprintln!("    Error: {e}");
        std::process::exit(1);
    }

    // Step 2: Stop + remove existing containers
    // Kill the named container and any other containers from the same image
    // so stale instances don't hold ports (e.g. relay port 3340).
    let stop_cmd = format!(
        concat!(
            "docker kill {container} 2>/dev/null; docker rm {container} 2>/dev/null; ",
            "for cid in $(docker ps -q --filter ancestor={image} 2>/dev/null); do ",
            "docker kill $cid 2>/dev/null; docker rm $cid 2>/dev/null; ",
            "done; sleep 1; true",
        ),
        container = container,
        image = image,
    );
    if machine.local {
        let _ = Command::new("sh").arg("-c").arg(&stop_cmd).status();
    } else {
        let status = ssh_cmd(machine, &stop_cmd).status();
        if let Err(e) = status {
            eprintln!("    Warning: failed to stop/rm old container: {e}");
        }
    }

    // Step 3: Start new container
    let mut run_parts = vec![
        "docker".to_string(), "run".to_string(), "-d".to_string(),
        "--name".to_string(), container.clone(),
        "--network".to_string(), "host".to_string(),
        "--restart".to_string(), "unless-stopped".to_string(),
        image.clone(),
        "--dashboard-port".to_string(), port.to_string(),
        "--relay-port".to_string(), relay_port.to_string(),
    ];
    run_parts.extend(flags);

    let status = if machine.local {
        Command::new(&run_parts[0])
            .args(&run_parts[1..])
            .status()
            .unwrap_or_else(|e| {
                eprintln!("    Failed to start container on {}: {e}", machine.name);
                std::process::exit(1);
            })
    } else {
        let run_cmd = run_parts.iter()
            .map(|a| if a.contains(' ') { format!("'{a}'") } else { a.clone() })
            .collect::<Vec<_>>()
            .join(" ");
        ssh_cmd(machine, &run_cmd)
            .status()
            .unwrap_or_else(|e| {
                eprintln!("    Failed to start container on {}: {e}", machine.name);
                std::process::exit(1);
            })
    };
    if !status.success() {
        eprintln!("    Container start failed on {}", machine.name);
        std::process::exit(1);
    }

    println!("  {} deployed (container: {container}, dashboard: {port}, relay: {relay_port})", machine.name);
}

fn health_check(machine: &MachineConfig, defaults: &DeployDefaults) {
    let timeout = Duration::from_secs(defaults.health_timeout_secs);
    let start = Instant::now();

    print!("  Checking {}...", machine.name);

    loop {
        if start.elapsed() > timeout {
            println!(" TIMEOUT");
            eprintln!("    Health check failed for {} ({}s timeout)", machine.name, defaults.health_timeout_secs);
            std::process::exit(1);
        }

        if let Some(body) = machine_curl_get(machine, defaults, "/api/stats") {
            if serde_json::from_str::<serde_json::Value>(&body).is_ok() {
                println!(" OK");
                return;
            }
        }

        thread::sleep(Duration::from_secs(2));
    }
}

fn inject_relay_hosts(machines: &[MachineConfig], defaults: &DeployDefaults) {
    // Format the TOML line to append
    let hosts_toml = format!(
        "relay_hosts = [{}]\n",
        defaults.relay_hosts.iter()
            .map(|h| format!("\"{h}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // The container has no shell, so we use docker cp to modify the config:
    //   1. docker cp container:/.swactor/node.toml /tmp/...
    //   2. append relay_hosts line
    //   3. docker cp /tmp/... container:/.swactor/node.toml
    for machine in machines {
        let container = machine.effective_container(defaults);
        let tmp_file = format!("/tmp/swactor-relay-inject-{}.toml", machine.name);

        let result = if machine.local {
            inject_relay_hosts_local(&container, &tmp_file, &hosts_toml)
        } else {
            inject_relay_hosts_remote(machine, &container, &hosts_toml)
        };

        match result {
            Ok(()) => println!("  {} relay_hosts injected", machine.name),
            Err(e) => eprintln!("  Warning: inject on {} failed: {e}", machine.name),
        }
    }

    // Restart containers to pick up the new config
    println!("  Restarting containers...");
    for machine in machines {
        let container = machine.effective_container(defaults);
        let restart_cmd = format!("docker restart {container}");

        let status = if machine.local {
            Command::new("sh").arg("-c").arg(&restart_cmd).status()
        } else {
            ssh_cmd(machine, &restart_cmd).status()
        };

        match status {
            Ok(s) if s.success() => println!("  {} restarted", machine.name),
            Ok(s) => eprintln!("  Warning: restart {} exited {}", machine.name, s),
            Err(e) => eprintln!("  Warning: restart {} failed: {e}", machine.name),
        }
    }

    // Brief pause for containers to come back up
    println!("  Waiting for containers to restart...");
    thread::sleep(Duration::from_secs(5));

    // Re-run health checks after restart
    for machine in machines {
        health_check(machine, defaults);
    }
}

fn inject_relay_hosts_local(container: &str, tmp_file: &str, hosts_toml: &str) -> Result<(), String> {
    // Copy config out of container
    let cp_out = format!("docker cp {container}:/.swactor/node.toml {tmp_file}");
    let status = Command::new("sh").arg("-c").arg(&cp_out)
        .status().map_err(|e| format!("docker cp out: {e}"))?;
    if !status.success() {
        return Err("docker cp out failed".into());
    }

    // Replace or append relay_hosts
    let contents = std::fs::read_to_string(tmp_file)
        .map_err(|e| format!("read tmp: {e}"))?;
    let updated = if contents.contains("relay_hosts") {
        contents.lines()
            .map(|line| {
                if line.trim_start().starts_with("relay_hosts") {
                    hosts_toml.trim_end()
                } else {
                    line
                }
            })
            .collect::<Vec<_>>()
            .join("\n") + "\n"
    } else {
        let mut s = contents;
        if !s.ends_with('\n') && !s.is_empty() { s.push('\n'); }
        s.push_str(hosts_toml);
        s
    };
    std::fs::write(tmp_file, &updated)
        .map_err(|e| format!("write tmp: {e}"))?;

    // Copy config back into container
    let cp_in = format!("docker cp {tmp_file} {container}:/.swactor/node.toml");
    let status = Command::new("sh").arg("-c").arg(&cp_in)
        .status().map_err(|e| format!("docker cp in: {e}"))?;
    if !status.success() {
        return Err("docker cp in failed".into());
    }

    let _ = std::fs::remove_file(tmp_file);
    Ok(())
}

fn inject_relay_hosts_remote(machine: &MachineConfig, container: &str, hosts_toml: &str) -> Result<(), String> {
    // Write the desired line to a local temp file, scp it, then use a simple
    // shell script on the remote to merge it into the container's config.
    let local_tmp = format!("/tmp/swactor-relay-line-{}.txt", machine.name);
    std::fs::write(&local_tmp, hosts_toml)
        .map_err(|e| format!("write local tmp: {e}"))?;

    // scp the line file to the remote
    let scp_status = Command::new("scp")
        .args([&local_tmp, &format!("{}:/tmp/swactor-relay-line.txt", machine.ssh)])
        .status().map_err(|e| format!("scp: {e}"))?;
    if !scp_status.success() {
        let _ = std::fs::remove_file(&local_tmp);
        return Err("scp relay line failed".into());
    }
    let _ = std::fs::remove_file(&local_tmp);

    // On the remote: docker cp out, filter+append, docker cp back
    let remote_cmd = format!(
        concat!(
            "docker cp {container}:/.swactor/node.toml /tmp/swactor-inject.toml && ",
            "grep -v '^relay_hosts' /tmp/swactor-inject.toml > /tmp/swactor-inject2.toml && ",
            "cat /tmp/swactor-relay-line.txt >> /tmp/swactor-inject2.toml && ",
            "docker cp /tmp/swactor-inject2.toml {container}:/.swactor/node.toml && ",
            "rm -f /tmp/swactor-inject.toml /tmp/swactor-inject2.toml /tmp/swactor-relay-line.txt"
        ),
        container = container,
    );
    let status = ssh_cmd(machine, &remote_cmd)
        .status().map_err(|e| format!("ssh: {e}"))?;
    if !status.success() {
        return Err("remote inject failed".into());
    }
    Ok(())
}

/// Collected node info for peer sync.
struct NodeInfo {
    name: String,
    node_id: String,
    relay_url: Option<String>,
}

/// Collect node_id and relay_url from each machine (O(n) GETs).
fn collect_node_info(machines: &[MachineConfig], defaults: &DeployDefaults) -> Vec<NodeInfo> {
    let mut node_info = Vec::new();

    for machine in machines {
        let body = machine_curl_get(machine, defaults, "/api/distribution").unwrap_or_else(|| {
            eprintln!("  Failed to get node_id from {}", machine.name);
            std::process::exit(1);
        });

        let snap: serde_json::Value = serde_json::from_str(&body).unwrap_or_else(|e| {
            eprintln!("  Invalid JSON from {}: {e}", machine.name);
            std::process::exit(1);
        });

        let node_id = snap.get("node_id")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| {
                eprintln!("  No node_id in response from {}", machine.name);
                std::process::exit(1);
            })
            .to_string();

        let relay_url = snap.get("relay_url")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        println!("  {} node_id: {}...{}{}", machine.name, &node_id[..8], &node_id[node_id.len()-8..],
            relay_url.as_ref().map(|u| format!(" relay: {u}")).unwrap_or_default());
        node_info.push(NodeInfo { name: machine.name.clone(), node_id, relay_url });
    }

    node_info
}

/// Pick a seed node index: first machine with a relay_url, or index 0.
fn pick_seed(node_info: &[NodeInfo]) -> usize {
    node_info.iter().position(|n| n.relay_url.is_some()).unwrap_or(0)
}

/// POST /api/peers/sync to each machine with all other peers + join_seed (O(n) POSTs).
fn sync_peers(
    machines: &[MachineConfig],
    defaults: &DeployDefaults,
    node_info: &[NodeInfo],
    seed_index: usize,
) {
    for (i, machine) in machines.iter().enumerate() {
        // Build peers array: all nodes except self
        let peers: Vec<serde_json::Value> = node_info.iter().enumerate()
            .filter(|(j, _)| *j != i)
            .map(|(_, info)| {
                let mut p = serde_json::json!({
                    "node_id": info.node_id,
                    "label": info.name,
                });
                if let Some(url) = &info.relay_url {
                    p["relay_url"] = serde_json::json!(url);
                }
                p
            })
            .collect();

        let payload = serde_json::json!({
            "peers": peers,
            "join_seed": node_info[seed_index].node_id,
        });
        let body = payload.to_string();

        match machine_curl_post(machine, defaults, "/api/peers/sync", &body) {
            Ok(_) => println!("  {} <- synced {} peers (seed: {})", machine.name, peers.len(), node_info[seed_index].name),
            Err(e) => {
                eprintln!("  Warning: failed to sync peers on {}: {e}", machine.name);
            }
        }
    }
}

/// Full introduction flow: collect info, pick seed, sync all peers.
/// Returns (node_info, seed_index) for reuse by convergence retry.
fn introduce_peers(machines: &[MachineConfig], defaults: &DeployDefaults) -> (Vec<NodeInfo>, usize) {
    let node_info = collect_node_info(machines, defaults);
    let seed_index = pick_seed(&node_info);

    println!("  Seed: {} (index {})", node_info[seed_index].name, seed_index);
    println!();

    sync_peers(machines, defaults, &node_info, seed_index);

    (node_info, seed_index)
}

fn wait_for_cluster_convergence(
    machines: &[MachineConfig],
    defaults: &DeployDefaults,
    node_info: &[NodeInfo],
    seed_index: usize,
) {
    let expected_alive = machines.len() - 1;
    let max_attempts = 3;

    for attempt in 1..=max_attempts {
        let timeout = Duration::from_secs(defaults.convergence_timeout_secs);
        let start = Instant::now();

        println!("  Attempt {attempt}/{max_attempts}: waiting for all nodes to see >= {expected_alive} alive peers...");

        loop {
            if start.elapsed() > timeout {
                println!("  TIMEOUT after {}s", defaults.convergence_timeout_secs);
                for machine in machines {
                    match machine_curl_get(machine, defaults, "/api/distribution") {
                        Some(body) => {
                            if let Ok(snap) = serde_json::from_str::<serde_json::Value>(&body) {
                                let alive = snap.get("alive_count")
                                    .and_then(|v| v.as_u64())
                                    .unwrap_or(0);
                                eprintln!("    {}: alive_count={}", machine.name, alive);
                            }
                        }
                        None => eprintln!("    {}: unreachable", machine.name),
                    }
                }

                if attempt < max_attempts {
                    println!("\n  Re-syncing peers (attempt {}/{max_attempts})...\n", attempt + 1);
                    sync_peers(machines, defaults, node_info, seed_index);
                }
                break;
            }

            let all_converged = machines.iter().all(|m| {
                machine_curl_get(m, defaults, "/api/distribution")
                    .and_then(|body| serde_json::from_str::<serde_json::Value>(&body).ok())
                    .and_then(|snap| snap.get("alive_count").and_then(|v| v.as_u64()))
                    .map(|alive| alive as usize >= expected_alive)
                    .unwrap_or(false)
            });

            if all_converged {
                println!("  Converged! All nodes see >= {expected_alive} alive peers.");
                return;
            }

            thread::sleep(Duration::from_secs(3));
        }
    }

    eprintln!("\n  Convergence failed after {max_attempts} attempts.");
    std::process::exit(1);
}

// ── Native (non-Docker) deploy ───────────────────────────────────────────

/// Convert CLI-style swactor_flags into (key, value) pairs for node.toml.
fn parse_swactor_flags(flags: &[String]) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    let mut i = 0;
    while i < flags.len() {
        let flag = &flags[i];
        match flag.as_str() {
            // Boolean flags
            "--auth" => pairs.push(("auth".into(), "true".into())),
            "--no-datastore" => pairs.push(("no_datastore".into(), "true".into())),
            "--no-relay" => pairs.push(("relay".into(), "false".into())),
            // String/numeric value flags
            "--storage-path" | "--identity-dir" | "--auth-dir" | "--peers-file"
            | "--node-name" | "--transport" | "--relay-bind" | "--listen"
            | "--seed" | "--seed-node-id"
            | "--dashboard-port" | "--relay-port" | "--chunk-size"
            | "--gc-interval" | "--disseminate-interval" | "--actors" => {
                let key = flag.trim_start_matches("--").replace('-', "_");
                i += 1;
                let value = flags.get(i).cloned().unwrap_or_default();
                pairs.push((key, value));
            }
            other => {
                eprintln!("  Warning: unknown swactor_flag '{other}', skipping");
            }
        }
        i += 1;
    }
    pairs
}

/// Get the absolute $HOME path on a machine (local or remote).
fn resolve_remote_home(machine: &MachineConfig) -> Result<String, String> {
    let output = if machine.local {
        std::env::var("HOME").map_err(|e| format!("$HOME not set: {e}"))?
    } else {
        let out = Command::new("ssh")
            .arg(&machine.ssh)
            .arg("echo $HOME")
            .output()
            .map_err(|e| format!("ssh failed: {e}"))?;
        if !out.status.success() {
            return Err(format!("ssh echo $HOME failed on {}", machine.name));
        }
        String::from_utf8_lossy(&out.stdout).trim().to_string()
    };
    if output.is_empty() {
        return Err(format!("empty $HOME on {}", machine.name));
    }
    Ok(output)
}

/// Generate node.toml content for a machine.
fn generate_node_toml(machine: &MachineConfig, defaults: &DeployDefaults, home_dir: &str) -> String {
    let swactor_dir = format!("{home_dir}/.swactor");
    let port = machine.effective_port(defaults);
    let relay_port = machine.effective_relay_port(defaults);
    let flags = machine.effective_flags(defaults);
    let overrides = parse_swactor_flags(&flags);

    // Collect all key-value pairs; overrides from flags take precedence.
    let mut kv: Vec<(String, String)> = Vec::new();

    // Base config
    kv.push(("transport".into(), "\"iroh\"".into()));
    kv.push(("dashboard_port".into(), port.to_string()));
    kv.push(("relay_port".into(), relay_port.to_string()));
    kv.push(("relay".into(), "true".into()));
    kv.push(("auth".into(), "true".into()));

    // Absolute paths
    kv.push(("storage_path".into(), format!("\"{swactor_dir}/data\"")));
    kv.push(("identity_dir".into(), format!("\"{swactor_dir}/identity\"")));
    kv.push(("peers_file".into(), format!("\"{swactor_dir}/peers.json\"")));
    kv.push(("auth_dir".into(), format!("\"{swactor_dir}/auth\"")));

    // relay_hosts from defaults
    if !defaults.relay_hosts.is_empty() {
        let hosts = defaults.relay_hosts.iter()
            .map(|h| format!("\"{h}\""))
            .collect::<Vec<_>>()
            .join(", ");
        kv.push(("relay_hosts".into(), format!("[{hosts}]")));
    }

    // Apply flag overrides — replace existing keys or add new ones
    for (key, value) in &overrides {
        let toml_value = match key.as_str() {
            // These are already bare values (true/false/numbers)
            "auth" | "relay" | "no_datastore"
            | "dashboard_port" | "relay_port" | "chunk_size"
            | "gc_interval" | "disseminate_interval" | "actors" => value.clone(),
            // Everything else is a string
            _ => {
                if value.starts_with('"') { value.clone() } else { format!("\"{value}\"") }
            }
        };
        if let Some(existing) = kv.iter_mut().find(|(k, _)| k == key) {
            existing.1 = toml_value;
        } else {
            kv.push((key.clone(), toml_value));
        }
    }

    let mut toml = String::new();
    for (k, v) in &kv {
        toml.push_str(&format!("{k} = {v}\n"));
    }
    toml
}

/// Create ~/.swactor/ and write node.toml on the target machine.
fn write_remote_config(machine: &MachineConfig, toml_content: &str) -> Result<(), String> {
    // Use a heredoc with a unique delimiter to avoid shell expansion
    let cmd = format!(
        "mkdir -p ~/.swactor/identity ~/.swactor/data ~/.swactor/auth && cat > ~/.swactor/node.toml << 'SWACTOR_TOML_EOF'\n{toml_content}SWACTOR_TOML_EOF"
    );

    let status = if machine.local {
        Command::new("sh")
            .arg("-c")
            .arg(&cmd)
            .status()
            .map_err(|e| format!("failed to write config: {e}"))?
    } else {
        Command::new("ssh")
            .arg(&machine.ssh)
            .arg(&cmd)
            .status()
            .map_err(|e| format!("ssh failed: {e}"))?
    };

    if !status.success() {
        return Err(format!("writing node.toml on {} failed", machine.name));
    }
    Ok(())
}

/// Copy the musl binary to /tmp/swactor-deploy on the target machine.
fn transfer_binary(machine: &MachineConfig, binary_path: &Path) -> Result<(), String> {
    if machine.local {
        std::fs::copy(binary_path, "/tmp/swactor-deploy")
            .map_err(|e| format!("copy binary failed: {e}"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions("/tmp/swactor-deploy", std::fs::Permissions::from_mode(0o755))
                .map_err(|e| format!("chmod failed: {e}"))?;
        }
    } else {
        let status = Command::new("scp")
            .arg(binary_path.as_os_str())
            .arg(format!("{}:/tmp/swactor-deploy", machine.ssh))
            .status()
            .map_err(|e| format!("scp failed: {e}"))?;
        if !status.success() {
            return Err(format!("scp to {} failed", machine.name));
        }
    }
    Ok(())
}

/// Deploy to a single machine via native install (no Docker).
fn native_deploy_to_machine(machine: &MachineConfig, defaults: &DeployDefaults, binary_path: &Path) {
    println!("  Deploying to {}...", machine.name);

    // Step 1: Resolve remote $HOME
    let home_dir = resolve_remote_home(machine).unwrap_or_else(|e| {
        eprintln!("    Error resolving HOME on {}: {e}", machine.name);
        std::process::exit(1);
    });
    println!("    home: {home_dir}");

    // Step 2: Transfer binary
    println!("    transferring binary...");
    if let Err(e) = transfer_binary(machine, binary_path) {
        eprintln!("    Error: {e}");
        std::process::exit(1);
    }

    // Step 3: Generate and write config
    let toml_content = generate_node_toml(machine, defaults, &home_dir);
    println!("    writing node.toml...");
    if let Err(e) = write_remote_config(machine, &toml_content) {
        eprintln!("    Error: {e}");
        std::process::exit(1);
    }

    // Step 4: Write empty peers.json if it doesn't exist
    let peers_cmd = format!(
        r#"test -f {home_dir}/.swactor/peers.json || echo '{{"version":1,"peers":[]}}' > {home_dir}/.swactor/peers.json"#
    );
    let _ = if machine.local {
        Command::new("sh").arg("-c").arg(&peers_cmd).status()
    } else {
        ssh_cmd(machine, &peers_cmd).status()
    };

    // Step 5: Run `swactor install` on the target
    println!("    running swactor install...");
    let install_cmd = "/tmp/swactor-deploy install";
    let status = if machine.local {
        Command::new("sh")
            .arg("-c")
            .arg(install_cmd)
            .status()
            .unwrap_or_else(|e| {
                eprintln!("    Failed to run install on {}: {e}", machine.name);
                std::process::exit(1);
            })
    } else {
        ssh_cmd(machine, install_cmd)
            .status()
            .unwrap_or_else(|e| {
                eprintln!("    Failed to run install on {}: {e}", machine.name);
                std::process::exit(1);
            })
    };
    if !status.success() {
        eprintln!("    Install failed on {}", machine.name);
        std::process::exit(1);
    }

    // Step 6: Clean up
    let cleanup_cmd = "rm -f /tmp/swactor-deploy";
    let _ = if machine.local {
        Command::new("sh").arg("-c").arg(cleanup_cmd).status()
    } else {
        ssh_cmd(machine, cleanup_cmd).status()
    };

    let port = machine.effective_port(defaults);
    let relay_port = machine.effective_relay_port(defaults);
    println!("  {} deployed (native, dashboard: {port}, relay: {relay_port})", machine.name);
}

pub fn run_native_deploy(
    root: &Path,
    config_path: &str,
    skip_build: bool,
    skip_verify: bool,
    skip_peers: bool,
) {
    // Phase 1: Load config
    println!("=== Phase 1: Loading config ===\n");
    let config = load_deploy_config(root, config_path);
    println!("  Loaded {} machine(s): {}", config.machines.len(),
        config.machines.iter().map(|m| m.name.as_str()).collect::<Vec<_>>().join(", "));
    println!();

    // Phase 2: Build binary
    let binary_path = if !skip_build {
        println!("=== Phase 2: Building binary ===\n");
        let p = build_binary(root);
        println!("  Binary: {}", p.display());
        println!();
        p
    } else {
        println!("=== Phase 2: Skipping build ===\n");
        let p = root.join("target/x86_64-unknown-linux-musl/release/swactor");
        if !p.exists() {
            eprintln!("Warning: --skip-build but {} does not exist", p.display());
            eprintln!("  Run without --skip-build first, or ensure the binary exists.\n");
        }
        p
    };

    // Phase 3: Deploy to each machine
    println!("=== Phase 3: Deploying to machines ===\n");
    for machine in &config.machines {
        native_deploy_to_machine(machine, &config.defaults, &binary_path);
    }
    println!();

    // Phase 4: Health check
    if !skip_verify {
        println!("=== Phase 4: Health check ===\n");
        for machine in &config.machines {
            health_check(machine, &config.defaults);
        }
        println!();
    }

    // Phase 5: Peer introduction + convergence
    if !skip_verify && !skip_peers && config.defaults.introduce_peers {
        println!("=== Phase 5: Peer introduction ===\n");
        let (node_info, seed_index) = introduce_peers(&config.machines, &config.defaults);

        println!("\n=== Phase 5b: Waiting for convergence ===\n");
        wait_for_cluster_convergence(&config.machines, &config.defaults, &node_info, seed_index);
        println!();
    }

    // Phase 6: Report
    println!("=== Phase 6: Cluster status ===\n");
    print_cluster_status(&config.machines, &config.defaults);
}

fn print_cluster_status(machines: &[MachineConfig], defaults: &DeployDefaults) {
    println!("  {:<12} {:>5} {:>7} {:>5} {:>5}",
        "NAME", "ALIVE", "ROUTING", "DIR", "CACHE");
    println!("  {}", "-".repeat(45));

    for machine in machines {
        match machine_curl_get(machine, defaults, "/api/distribution") {
            Some(body) => {
                if let Ok(snap) = serde_json::from_str::<serde_json::Value>(&body) {
                    let alive = snap.get("alive_count").and_then(|v| v.as_u64()).unwrap_or(0);
                    let routing = snap.get("routing_table_size").and_then(|v| v.as_u64()).unwrap_or(0);
                    let dir = snap.get("directory_entry_count").and_then(|v| v.as_u64()).unwrap_or(0);
                    let cache = snap.get("cache_size").and_then(|v| v.as_u64()).unwrap_or(0);

                    println!("  {:<12} {:>5} {:>7} {:>5} {:>5}",
                        machine.name, alive, routing, dir, cache);
                } else {
                    println!("  {:<12} -- invalid JSON --", machine.name);
                }
            }
            None => {
                println!("  {:<12} -- unreachable --", machine.name);
            }
        }
    }
    println!();
}
