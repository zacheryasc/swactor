//! Test utilities for Docker-based cluster integration tests.

use std::path::PathBuf;
use std::process::Command;
use std::sync::Once;
use std::thread;
use std::time::{Duration, Instant};

use distribution::snapshot::DistributionNodeSnapshot;

/// Dashboard ports mapped to the host for each of the 5 nodes.
pub const DASHBOARD_PORTS: [u16; 5] = [9091, 9092, 9093, 9094, 9095];

/// Service names matching docker-compose.yml.
pub const SERVICE_NAMES: [&str; 5] = ["seed", "node-2", "node-3", "node-4", "node-5"];

/// `CARGO_MANIFEST_DIR` points to `tests/docker/` (the crate root).
const COMPOSE_DIR: &str = env!("CARGO_MANIFEST_DIR");

fn compose_file() -> String {
    let mut p = PathBuf::from(COMPOSE_DIR);
    p.push("docker-compose.yml");
    p.to_string_lossy().into_owned()
}

static BUILD_ONCE: Once = Once::new();

fn build_cluster_images() {
    BUILD_ONCE.call_once(|| {
        let status = Command::new("docker")
            .args(["compose", "-f", &compose_file(), "build"])
            .status()
            .expect("failed to build docker images");
        assert!(status.success(), "docker compose build failed");
    });
}

/// Handle to a running Docker Compose cluster.
/// Stops the cluster on drop.
pub struct ClusterHandle {
    stopped: bool,
}

impl ClusterHandle {
    /// Start the 5-node cluster via docker compose.
    pub fn start() -> Self {
        build_cluster_images();

        let status = Command::new("docker")
            .args(["compose", "-f", &compose_file(), "up", "-d", "--wait"])
            .status()
            .expect("failed to run docker compose");

        if !status.success() {
            let status = Command::new("docker")
                .args(["compose", "-f", &compose_file(), "up", "-d"])
                .status()
                .expect("failed to run docker compose");
            assert!(status.success(), "docker compose up failed");
            thread::sleep(Duration::from_secs(5));
        }

        ClusterHandle { stopped: false }
    }

    /// Stop the cluster.
    pub fn stop(&mut self) {
        if !self.stopped {
            let _ = Command::new("docker")
                .args(["compose", "-f", &compose_file(), "down", "--timeout", "5"])
                .status();
            self.stopped = true;
        }
    }
}

impl Drop for ClusterHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Kill a specific node (simulates crash — container stops).
pub fn kill_node(service: &str) {
    let status = Command::new("docker")
        .args(["compose", "-f", &compose_file(), "stop", service])
        .status()
        .expect("failed to stop node");
    assert!(status.success(), "docker compose stop {service} failed");
}

/// Restart a previously killed node.
pub fn restart_node(service: &str) {
    let status = Command::new("docker")
        .args(["compose", "-f", &compose_file(), "start", service])
        .status()
        .expect("failed to start node");
    assert!(status.success(), "docker compose start {service} failed");
}

/// Fetch the distribution snapshot from a node's dashboard on localhost.
/// Returns None if the node is unreachable or returns empty/error.
pub fn poll_distribution(port: u16) -> Option<DistributionNodeSnapshot> {
    poll_distribution_at("127.0.0.1", port)
}

/// Fetch the distribution snapshot from a node's dashboard at an arbitrary host.
pub fn poll_distribution_at(host: &str, port: u16) -> Option<DistributionNodeSnapshot> {
    let url = format!("http://{host}:{port}/api/distribution");
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

/// Wait until all nodes at the given ports report at least `expected_alive`
/// alive members. Times out after `timeout`.
pub fn wait_for_convergence(
    ports: &[u16],
    expected_alive: usize,
    timeout: Duration,
) -> Result<(), String> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            // Build diagnostic message
            let mut diag = String::from("Convergence timeout. Last seen alive counts: ");
            for &port in ports {
                match poll_distribution(port) {
                    Some(snap) => diag.push_str(&format!("port {}={}, ", port, snap.alive_count)),
                    None => diag.push_str(&format!("port {}=unreachable, ", port)),
                }
            }
            return Err(diag);
        }

        let all_converged = ports.iter().all(|&port| {
            poll_distribution(port)
                .map(|snap| snap.alive_count >= expected_alive)
                .unwrap_or(false)
        });

        if all_converged {
            return Ok(());
        }

        thread::sleep(Duration::from_secs(1));
    }
}

/// Wait until a specific set of ports all report alive_count <= threshold.
pub fn wait_for_death_detection(
    ports: &[u16],
    max_alive: usize,
    timeout: Duration,
) -> Result<(), String> {
    wait_for_death_detection_at(
        &ports.iter().map(|&p| ("127.0.0.1", p)).collect::<Vec<_>>(),
        max_alive,
        timeout,
    )
}

/// Wait until a set of (host, port) endpoints all report alive_count <= threshold.
pub fn wait_for_death_detection_at(
    endpoints: &[(&str, u16)],
    max_alive: usize,
    timeout: Duration,
) -> Result<(), String> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            let mut diag = String::from("Death detection timeout. Last seen: ");
            for &(host, port) in endpoints {
                match poll_distribution_at(host, port) {
                    Some(snap) => diag.push_str(&format!("{host}:{port}={} alive, ", snap.alive_count)),
                    None => diag.push_str(&format!("{host}:{port}=unreachable, ")),
                }
            }
            return Err(diag);
        }

        let all_detected = endpoints.iter().all(|&(host, port)| {
            poll_distribution_at(host, port)
                .map(|snap| snap.alive_count <= max_alive)
                .unwrap_or(false)
        });

        if all_detected {
            return Ok(());
        }

        thread::sleep(Duration::from_secs(1));
    }
}

// ── LAN (cross-machine) cluster support ─────────────────────────────────────

/// Dashboard endpoints for the LAN cluster.
/// hpz (local):     9091, 9092
/// thinkpad (remote): 9093, 9094, 9095
pub const LAN_HPZ_IP: &str = "192.168.1.106";
pub const LAN_THINKPAD_IP: &str = "192.168.1.102";
pub const LAN_THINKPAD_SSH: &str = "thinkpad";
pub const LAN_THINKPAD_REPO: &str = "/home/zach/swactor-distribution-realization";

pub const LAN_ENDPOINTS: [(&str, u16); 5] = [
    ("127.0.0.1", 9091),
    ("127.0.0.1", 9092),
    (LAN_THINKPAD_IP, 9093),
    (LAN_THINKPAD_IP, 9094),
    (LAN_THINKPAD_IP, 9095),
];

pub const LAN_HPZ_COMPOSE: &str = "tests/docker/docker-compose.lan-hpz.yml";
pub const LAN_THINKPAD_COMPOSE: &str = "tests/docker/docker-compose.lan-thinkpad.yml";

static BUILD_LAN_ONCE: Once = Once::new();

fn build_lan_images() {
    BUILD_LAN_ONCE.call_once(|| {
        // Sync repo to thinkpad
        let tar_status = Command::new("bash")
            .args(["-c", &format!(
                "tar czf /tmp/swactor-repo.tar.gz -C {} --exclude=target --exclude=.git . \
                 && scp -q /tmp/swactor-repo.tar.gz {}:/tmp/ \
                 && ssh {} 'mkdir -p {} && tar xzf /tmp/swactor-repo.tar.gz -C {}'",
                COMPOSE_DIR.replace("tests/docker", ""),
                LAN_THINKPAD_SSH, LAN_THINKPAD_SSH, LAN_THINKPAD_REPO, LAN_THINKPAD_REPO,
            )])
            .status()
            .expect("failed to sync repo to thinkpad");
        assert!(tar_status.success(), "repo sync to thinkpad failed");

        // Build hpz images
        let hpz_compose = lan_hpz_compose_path();
        let status = Command::new("docker")
            .args(["compose", "-f", &hpz_compose, "build"])
            .status()
            .expect("failed to build hpz images");
        assert!(status.success(), "docker compose build (hpz) failed");

        // Build thinkpad images
        let status = Command::new("ssh")
            .args([
                LAN_THINKPAD_SSH,
                &format!(
                    "cd {} && docker compose -f {} build",
                    LAN_THINKPAD_REPO, LAN_THINKPAD_COMPOSE,
                ),
            ])
            .status()
            .expect("failed to build thinkpad images");
        assert!(status.success(), "docker compose build (thinkpad) failed");
    });
}

/// Handle to a LAN cluster running across two machines.
pub struct LanClusterHandle {
    stopped: bool,
}

impl LanClusterHandle {
    /// Start the LAN cluster: hpz nodes locally, thinkpad nodes via SSH.
    pub fn start() -> Self {
        build_lan_images();

        // Start hpz side
        let hpz_compose = lan_hpz_compose_path();
        let status = Command::new("docker")
            .args(["compose", "-f", &hpz_compose, "up", "-d"])
            .status()
            .expect("failed to start hpz nodes");
        assert!(status.success(), "docker compose up (hpz) failed");

        // Start thinkpad side
        let status = Command::new("ssh")
            .args([
                LAN_THINKPAD_SSH,
                &format!(
                    "cd {} && docker compose -f {} up -d",
                    LAN_THINKPAD_REPO, LAN_THINKPAD_COMPOSE,
                ),
            ])
            .status()
            .expect("failed to start thinkpad nodes");
        assert!(status.success(), "docker compose up (thinkpad) failed");

        // Give containers a moment to bind
        thread::sleep(Duration::from_secs(3));

        LanClusterHandle { stopped: false }
    }

    /// Stop both sides of the cluster.
    pub fn stop(&mut self) {
        if !self.stopped {
            let hpz_compose = lan_hpz_compose_path();
            let _ = Command::new("docker")
                .args(["compose", "-f", &hpz_compose, "down", "--timeout", "5"])
                .status();
            let _ = Command::new("ssh")
                .args([
                    LAN_THINKPAD_SSH,
                    &format!(
                        "cd {} && docker compose -f {} down --timeout 5",
                        LAN_THINKPAD_REPO, LAN_THINKPAD_COMPOSE,
                    ),
                ])
                .status();
            self.stopped = true;
        }
    }
}

impl Drop for LanClusterHandle {
    fn drop(&mut self) {
        self.stop();
    }
}

/// Kill a node on the thinkpad via SSH.
pub fn kill_remote_node(service: &str) {
    let status = Command::new("ssh")
        .args([
            LAN_THINKPAD_SSH,
            &format!(
                "cd {} && docker compose -f {} stop {}",
                LAN_THINKPAD_REPO, LAN_THINKPAD_COMPOSE, service,
            ),
        ])
        .status()
        .expect("failed to kill remote node");
    assert!(status.success(), "remote docker compose stop {service} failed");
}

/// Restart a node on the thinkpad via SSH.
pub fn restart_remote_node(service: &str) {
    let status = Command::new("ssh")
        .args([
            LAN_THINKPAD_SSH,
            &format!(
                "cd {} && docker compose -f {} start {}",
                LAN_THINKPAD_REPO, LAN_THINKPAD_COMPOSE, service,
            ),
        ])
        .status()
        .expect("failed to restart remote node");
    assert!(status.success(), "remote docker compose start {service} failed");
}

/// Wait until all LAN endpoints report at least `expected_alive` alive members.
pub fn wait_for_lan_convergence(
    endpoints: &[(&str, u16)],
    expected_alive: usize,
    timeout: Duration,
) -> Result<(), String> {
    let start = Instant::now();
    loop {
        if start.elapsed() > timeout {
            let mut diag = String::from("LAN convergence timeout. Last seen: ");
            for &(host, port) in endpoints {
                match poll_distribution_at(host, port) {
                    Some(snap) => diag.push_str(&format!("{host}:{port}={}, ", snap.alive_count)),
                    None => diag.push_str(&format!("{host}:{port}=unreachable, ")),
                }
            }
            return Err(diag);
        }

        let all_converged = endpoints.iter().all(|&(host, port)| {
            poll_distribution_at(host, port)
                .map(|snap| snap.alive_count >= expected_alive)
                .unwrap_or(false)
        });

        if all_converged {
            return Ok(());
        }

        thread::sleep(Duration::from_secs(1));
    }
}

// ── Deploy simulation helpers ────────────────────────────────────────────────

/// Restart a specific container with fresh flags (simulates deploy lifecycle).
pub fn redeploy_node(service: &str) {
    let status = Command::new("docker")
        .args(["compose", "-f", &compose_file(), "up", "-d", "--force-recreate", service])
        .status()
        .expect("failed to redeploy node");
    assert!(status.success(), "docker compose force-recreate {service} failed");
}

/// Fetch the node_id from a node's distribution snapshot.
pub fn get_node_id(port: u16) -> Option<String> {
    poll_distribution(port).map(|snap| snap.node_id)
}

fn lan_hpz_compose_path() -> String {
    let mut p = PathBuf::from(COMPOSE_DIR);
    p.push("docker-compose.lan-hpz.yml");
    p.to_string_lossy().into_owned()
}
