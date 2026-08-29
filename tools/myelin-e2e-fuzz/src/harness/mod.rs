//! Real-binary cluster lifecycle: build, start, provision, health, teardown.

mod control;
mod convergence;

use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::{Value, json};

use crate::resources::{
    TelemetryResourceCensus, build_myelin_binaries, build_workload_image, capture_lines,
    contains_poison, http_json, list_containers, pending_resource_cleanup, remove_container,
    remove_containers, reserve_port, transient_actor_identities,
};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
pub(crate) const GENERATED_LAUNCHER: &str = "/usr/local/bin/myelin-e2e-python";
type CapturedLines = Arc<Mutex<VecDeque<String>>>;
type SpawnedOrchestrator = (Child, CapturedLines, CapturedLines);

#[derive(Clone, Debug)]
pub struct ClusterHarnessConfig {
    pub workspace: PathBuf,
    pub artifacts: PathBuf,
    pub node_count: u8,
    pub seed: u64,
    pub image: Option<String>,
    pub build_image: bool,
    pub deadline: Duration,
}

pub struct ClusterHarness {
    config: ClusterHarnessConfig,
    base_url: String,
    state_dir: PathBuf,
    dashboard_port: u16,
    image: String,
    container_prefix: String,
    telemetry: PathBuf,
    telemetry_census: Mutex<TelemetryResourceCensus>,
    resource_baseline: BTreeSet<String>,
    orchestrator: Child,
    stdout: CapturedLines,
    stderr: CapturedLines,
    node_ids: Vec<u64>,
    orchestrator_stopped: bool,
    pre_stop_containers: Vec<String>,
    torn_down: bool,
}

fn spawn_orchestrator(
    config: &ClusterHarnessConfig,
    dashboard_port: u16,
    state_dir: &Path,
    image: &str,
    container_prefix: &str,
    telemetry: &Path,
    reset_state: bool,
) -> Result<SpawnedOrchestrator, String> {
    let binary = config.workspace.join("target/release/myelin-orchestrator");
    if !binary.is_file() {
        return Err(format!(
            "orchestrator binary is missing: {}",
            binary.display()
        ));
    }
    let mut command = Command::new(binary);
    command
        .current_dir(&config.workspace)
        .args([
            "--provider",
            "vastai",
            "--vastai-provisioning",
            "mock",
            "--image",
            image,
            "--state-dir",
        ])
        .arg(state_dir);
    if reset_state {
        command.arg("--reset-state");
    }
    command
        .arg("--telemetry-frame-log")
        .arg(telemetry)
        .env("MYELIN_DASHBOARD_PORT", dashboard_port.to_string())
        .env("MYELIN_MOCK_VASTAI_CONTAINER_PREFIX", container_prefix)
        .env("MYELIN_DOCKER_GPUS", "")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut orchestrator = command
        .spawn()
        .map_err(|error| format!("start real Myelin orchestrator: {error}"))?;
    let stdout = Arc::new(Mutex::new(VecDeque::new()));
    let stderr = Arc::new(Mutex::new(VecDeque::new()));
    capture_lines(
        orchestrator
            .stdout
            .take()
            .expect("captured orchestrator stdout"),
        Arc::clone(&stdout),
    );
    capture_lines(
        orchestrator
            .stderr
            .take()
            .expect("captured orchestrator stderr"),
        Arc::clone(&stderr),
    );
    Ok((orchestrator, stdout, stderr))
}

impl ClusterHarness {
    pub fn start(config: ClusterHarnessConfig) -> Result<Self, String> {
        if config.node_count < 2 || config.node_count > 3 {
            return Err("cluster harness requires two or three workers".to_owned());
        }
        fs::create_dir_all(&config.artifacts)
            .map_err(|error| format!("create artifact directory: {error}"))?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock precedes epoch: {error}"))?
            .as_millis();
        let state_dir = config
            .artifacts
            .join(format!("state-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&state_dir)
            .map_err(|error| format!("create harness state directory: {error}"))?;
        let image = config
            .image
            .clone()
            .unwrap_or_else(|| format!("myelin-e2e:{}-{nonce}", std::process::id()));
        build_myelin_binaries(&config.workspace)?;
        if config.build_image {
            build_workload_image(&config.workspace, &image)?;
        }
        let port = reserve_port()?;
        let base_url = format!("http://127.0.0.1:{port}");
        let container_prefix = format!("myelin-e2e-{}-{nonce}", std::process::id());
        let telemetry = config.artifacts.join(format!("telemetry-{nonce}.jsonl"));
        let (orchestrator, stdout, stderr) = spawn_orchestrator(
            &config,
            port,
            &state_dir,
            &image,
            &container_prefix,
            &telemetry,
            true,
        )?;
        let mut harness = Self {
            config,
            base_url,
            dashboard_port: port,
            state_dir,
            image,
            container_prefix,
            telemetry,
            telemetry_census: Mutex::new(TelemetryResourceCensus::default()),
            resource_baseline: BTreeSet::new(),
            orchestrator,
            stdout,
            stderr,
            node_ids: Vec::new(),
            orchestrator_stopped: false,
            pre_stop_containers: Vec::new(),
            torn_down: false,
        };
        harness.wait_for_dashboard()?;
        harness.provision_nodes()?;
        let baseline = harness.health_snapshot()?;
        harness.resource_baseline = transient_actor_identities(&baseline);
        Ok(harness)
    }

    pub fn node_ids(&self) -> &[u64] {
        &self.node_ids
    }

    pub fn image(&self) -> &str {
        &self.image
    }

    pub fn state_dir(&self) -> &Path {
        &self.state_dir
    }
}

impl ClusterHarness {
    fn crash_orchestrator(&mut self) -> Result<(), String> {
        self.ensure_orchestrator_live()?;
        self.orchestrator
            .kill()
            .map_err(|error| format!("kill orchestrator for recovery: {error}"))?;
        self.orchestrator
            .wait()
            .map_err(|error| format!("wait for crashed orchestrator: {error}"))?;
        Ok(())
    }

    fn restart_orchestrator(&mut self) -> Result<(), String> {
        let (orchestrator, stdout, stderr) = spawn_orchestrator(
            &self.config,
            self.dashboard_port,
            &self.state_dir,
            &self.image,
            &self.container_prefix,
            &self.telemetry,
            false,
        )?;
        self.orchestrator = orchestrator;
        self.stdout = stdout;
        self.stderr = stderr;
        self.orchestrator_stopped = false;
        self.wait_for_dashboard()
    }

    pub fn verify_running_recovery(&mut self) -> Result<(), String> {
        let expected_containers = list_containers(&self.container_prefix)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let expected_nodes = self.node_ids.clone();
        self.crash_orchestrator()?;
        self.restart_orchestrator()?;
        self.provision_nodes()?;
        let recovered_containers = list_containers(&self.container_prefix)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        if recovered_containers != expected_containers {
            return Err(format!(
                "recovery changed provider resources: before={expected_containers:?}, \
                 after={recovered_containers:?}"
            ));
        }
        if self.node_ids != expected_nodes {
            return Err(format!(
                "recovery changed logical nodes: before={expected_nodes:?}, after={:?}",
                self.node_ids
            ));
        }
        Ok(())
    }

    pub fn verify_missing_resource_recovery(&mut self) -> Result<(), String> {
        if self.orchestrator_stopped {
            return Ok(());
        }
        let mut expected_containers = list_containers(&self.container_prefix)?
            .into_iter()
            .collect::<BTreeSet<_>>();
        let removed = expected_containers
            .pop_last()
            .ok_or_else(|| "missing-resource recovery requires a provider resource".to_owned())?;
        self.crash_orchestrator()?;
        remove_container(&removed)?;
        self.restart_orchestrator()?;

        let deadline = Instant::now() + self.config.deadline;
        loop {
            self.ensure_orchestrator_live()?;
            let containers = list_containers(&self.container_prefix)?
                .into_iter()
                .collect::<BTreeSet<_>>();
            if containers != expected_containers {
                return Err(format!(
                    "missing provider resource was recreated: expected={expected_containers:?}, \
                     observed={containers:?}"
                ));
            }
            let status = http_json(
                "GET",
                &format!("{}/api/control/status", self.base_url),
                None,
            )?;
            let nodes = status
                .pointer("/Status/nodes")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let running = nodes
                .iter()
                .filter(|node| node.get("phase").and_then(Value::as_str) == Some("running"))
                .count();
            let missing_is_terminal = nodes.iter().any(|node| {
                node.get("phase").and_then(Value::as_str) == Some("stopped")
                    && node
                        .get("last_error")
                        .and_then(Value::as_str)
                        .is_some_and(|error| {
                            error.contains("absent") || error.contains("no provider resource")
                        })
            });
            if running == expected_containers.len() && missing_is_terminal {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(self.timeout_evidence(&format!(
                    "missing resource to become terminal without replacement; status={status}"
                )));
            }
            thread::sleep(POLL_INTERVAL);
        }
    }

    pub fn health_snapshot(&self) -> Result<Value, String> {
        let actors = http_json(
            "GET",
            &format!("{}/api/control/actors", self.base_url),
            None,
        )?;
        let fleet = http_json("GET", &format!("{}/api/control/fleet", self.base_url), None)?;
        let running_nodes = fleet
            .pointer("/FleetStatus/nodes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter(|node| node.get("phase").and_then(Value::as_str) == Some("running"))
            .filter_map(|node| node.get("logical_node_id").and_then(Value::as_u64))
            .collect::<Vec<_>>();
        let mut nodes = Vec::new();
        for node in &running_nodes {
            let control_request_id = format!("health-{}-{node}", self.config.seed);
            let live = http_json(
                "GET",
                &format!(
                    "{}/api/control/contextual/nodes/{node}?control_request_id={control_request_id}",
                    self.base_url
                ),
                None,
            )?;
            nodes.push(live);
        }
        let containers = list_containers(&self.container_prefix)?;
        let resources = self
            .telemetry_census
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .update(&self.telemetry)?;
        Ok(json!({
            "actors": actors,
            "fleet": fleet,
            "running_nodes": running_nodes,
            "nodes": nodes,
            "containers": containers,
            "resources": resources,
        }))
    }
    pub fn assert_healthy(&self) -> Result<(), String> {
        if self.orchestrator_stopped {
            let containers = list_containers(&self.container_prefix)?;
            if containers != self.pre_stop_containers {
                return Err(format!(
                    "orchestrator failure changed worker container census: {containers:?}"
                ));
            }
            return self.wait_for_no_workload_processes();
        }
        let deadline = Instant::now() + self.config.deadline;
        loop {
            let health = self.health_snapshot()?;
            if contains_poison(&health) {
                return Err(format!("actor poison or worker panic detected: {health}"));
            }
            let containers = health
                .get("containers")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            let running_nodes = health
                .get("running_nodes")
                .and_then(Value::as_array)
                .map_or(0, Vec::len);
            if containers != running_nodes {
                return Err(format!(
                    "container census mismatch: expected {running_nodes}, observed {containers}; {health}"
                ));
            }
            if let Some(reason) = pending_resource_cleanup(&health, &self.resource_baseline) {
                if Instant::now() >= deadline {
                    return Err(format!("deadline waiting for {reason}; health={health}"));
                }
                thread::sleep(POLL_INTERVAL);
                continue;
            }
            return Ok(());
        }
    }

    fn ensure_orchestrator_live(&mut self) -> Result<(), String> {
        match self
            .orchestrator
            .try_wait()
            .map_err(|error| format!("observe orchestrator process: {error}"))?
        {
            Some(status) => Err(format!(
                "orchestrator exited {status}; stdout={:?}; stderr={:?}",
                self.stdout
                    .lock()
                    .unwrap_or_else(|error| error.into_inner()),
                self.stderr
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
            )),
            None => Ok(()),
        }
    }

    fn timeout_evidence(&self, predicate: &str) -> String {
        format!(
            "deadline waiting for {predicate}; stdout={:?}; stderr={:?}",
            self.stdout
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            self.stderr
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        )
    }

    pub fn teardown(&mut self) -> Result<(), String> {
        if self.torn_down {
            return Ok(());
        }
        let orchestrator_live = self
            .orchestrator
            .try_wait()
            .map_err(|error| format!("observe orchestrator during teardown: {error}"))?
            .is_none();
        let mut recovery_complete = false;
        if orchestrator_live {
            for node in self.node_ids.clone() {
                let _ = self.kill_node(node);
            }
            let deadline = Instant::now() + self.config.deadline;
            while Instant::now() < deadline {
                if self.orchestrator.try_wait().ok().flatten().is_some() {
                    break;
                }
                let status = http_json(
                    "GET",
                    &format!("{}/api/control/status", self.base_url),
                    None,
                );
                if status.as_ref().is_ok_and(|status| {
                    status
                        .pointer("/Status/nodes")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                        .all(|node| {
                            matches!(
                                node.get("phase").and_then(Value::as_str),
                                Some("stopped" | "orphan")
                            )
                        })
                }) {
                    recovery_complete = true;
                    break;
                }
                thread::sleep(POLL_INTERVAL);
            }
        }

        let mut graceful_shutdown = false;
        if self.orchestrator.try_wait().ok().flatten().is_none() {
            let pid = i32::try_from(self.orchestrator.id())
                .map_err(|_| "orchestrator pid exceeds platform range".to_owned())?;
            let signal_result = unsafe { libc::kill(pid, libc::SIGTERM) };
            if signal_result == 0 {
                let deadline = Instant::now() + self.config.deadline;
                while Instant::now() < deadline {
                    if self.orchestrator.try_wait().ok().flatten().is_some() {
                        graceful_shutdown = true;
                        break;
                    }
                    thread::sleep(POLL_INTERVAL);
                }
            }
        }
        if self.orchestrator.try_wait().ok().flatten().is_none() {
            let _ = self.orchestrator.kill();
            let _ = self.orchestrator.wait();
        }
        if graceful_shutdown {
            let checkpoint_exists = self.state_dir.join("cluster.json").exists();
            if recovery_complete && checkpoint_exists {
                return Err("graceful shutdown retained an empty recovery checkpoint".to_owned());
            }
            if !recovery_complete && !checkpoint_exists {
                return Err("graceful shutdown discarded an active recovery checkpoint".to_owned());
            }
        }
        remove_containers(&self.container_prefix)?;
        let remaining = list_containers(&self.container_prefix)?;
        if !remaining.is_empty() {
            return Err(format!(
                "harness containers remained after teardown: {remaining:?}"
            ));
        }
        self.torn_down = true;
        Ok(())
    }
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        let _ = self.teardown();
    }
}
