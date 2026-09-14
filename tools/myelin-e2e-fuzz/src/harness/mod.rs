//! Real-binary cluster lifecycle: build, start, provision, health, teardown.

mod control;
mod convergence;
pub mod raw_fleet;

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::Write;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::budget::Budget;
use crate::ir::BehaviorCase;
use serde_json::{Value, json};
use swactor_vastai::{BlockingVastClient, VastClient};

use crate::resources::{
    BuiltBinaries, TelemetryResourceCensus, build_workload_image, capture_lines, container_census,
    contains_poison, docker_image_identity, http_json_budget, list_containers,
    pending_resource_cleanup, remove_container, remove_containers, reserve_port,
    resolve_myelin_binaries,
};

const POLL_INTERVAL: Duration = Duration::from_millis(100);
const TELEMETRY_REFRESH_INTERVAL: Duration = Duration::from_millis(10);
pub(crate) const GENERATED_LAUNCHER: &str = "/usr/local/bin/myelin-e2e-python";
const BUILTIN_FIXTURE_PATH: &str = "/models/tiny-linear/weights";
type CapturedLines = Arc<Mutex<VecDeque<String>>>;
type SpawnedOrchestrator = (Child, OwnedFd, CapturedLines, CapturedLines);

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HarnessProvider {
    LocalMock,
    VastAiReal {
        ssh_identity: PathBuf,
        api_key_env: String,
        admission: PathBuf,
        /// Optional deployment bundle: when set, the orchestrator refreshes
        /// retained nodes through the SSH artifact bootstrap transaction.
        bundle: Option<PathBuf>,
    },
    StaticSsh {
        manifest: PathBuf,
        identity: PathBuf,
        bundle: PathBuf,
    },
}

#[derive(Clone, Debug)]
pub struct ClusterHarnessConfig {
    pub workspace: PathBuf,
    pub artifacts: PathBuf,
    pub node_count: u8,
    pub seed: u64,
    pub image: Option<String>,
    pub build_image: bool,
    pub deadline: Duration,
    pub provider: HarnessProvider,
    pub selected_offer_ids: Vec<u64>,
    pub state_dir: Option<PathBuf>,
    pub reset_state: bool,
    /// Wait for nothing at startup: adopt the retained fixture's nodes and
    /// let the caller repair them (in-place agent relaunch) before any
    /// running-state wait happens.
    pub adopt_only: bool,
    /// Orchestrator run identity. Derives the provider labels and container
    /// names; a fresh orchestrator state does not persist `cluster.json`
    /// until its first command, so the harness passes the run id explicitly
    /// instead of reading it back from state that may not exist yet.
    pub offer_search_id: Option<u64>,
    pub provision: bool,
    /// Operator-managed iroh relay for providers whose nodes rely on relay
    /// transport (static-ssh fixtures isolate node networks). Mirrors the
    /// remote deployment's relay configuration.
    pub relay_url: Option<String>,
    /// Orchestrator run identity. Derives the provider labels and container
    /// names; a fresh orchestrator state does not persist `cluster.json`
    /// until its first command, so the harness passes the run id explicitly
    /// instead of reading it back from state that may not exist yet.
    pub run_id: u64,
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixturePathObservation {
    pub kind: String,
    pub revision: u64,
    pub active: bool,
    pub length: Option<usize>,
    pub digest: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FixturePathSnapshot {
    pub entries: BTreeMap<String, FixturePathObservation>,
}

/// An unforgeable handle to one still-owned retained attempt, not arbitrary
/// observed actor identities. Every checkpoint resolves its resources afresh.
pub struct RetainedCaseActors {
    attempt: Arc<control::CaseAttempt>,
}

pub struct ClusterHarness {
    config: ClusterHarnessConfig,
    pub(crate) execution_budget: Budget,
    pub(crate) cleanup_budget: Option<Budget>,
    fixture_cleanup_budget: Budget,
    binaries: BuiltBinaries,
    provider_client: Option<BlockingVastClient>,
    lifecycle_started: Instant,
    observation_generation: AtomicU64,
    health_boundary: AtomicU64,
    base_url: String,
    state_dir: PathBuf,
    dashboard_port: u16,
    image: String,
    container_prefix: String,
    telemetry: PathBuf,
    telemetry_census: Mutex<TelemetryResourceCensus>,
    provider_baseline: BTreeSet<String>,
    fixture_mapping: BTreeMap<u64, Value>,
    stopped_nodes: BTreeSet<u64>,
    provider_accounting_baseline: Option<Value>,
    node_generation_baseline: BTreeMap<u64, Value>,
    fixture_blob_baseline: Option<(u64, u64)>,
    fixture_actor_baseline: BTreeSet<String>,
    verified_local_baseline: Option<Value>,
    orchestrator: Child,
    orchestrator_pidfd: OwnedFd,
    stdout: CapturedLines,
    stderr: CapturedLines,
    node_ids: Vec<u64>,
    next_attempt_id: u64,
    last_attempt_id: Option<u64>,
    pending_attempts: BTreeMap<String, control::PendingAttempt>,
    active_attempts: BTreeMap<String, Arc<control::CaseAttempt>>,
    pub(crate) pending_control_processes: Mutex<BTreeSet<String>>,
    last_failure_signature: Option<crate::oracle::FailureSignature>,
    quarantine_reason: Mutex<Option<String>>,
    torn_down: bool,
}

fn spawn_orchestrator(
    config: &ClusterHarnessConfig,
    binary: &Path,
    dashboard_port: u16,
    state_dir: &Path,
    image: &str,
    container_prefix: &str,
    telemetry: &Path,
    budget: &Budget,
    reset_state: bool,
) -> Result<SpawnedOrchestrator, String> {
    budget.check("spawn retained orchestrator process")?;
    if !binary.is_file() {
        return Err(format!(
            "orchestrator binary is missing: {}",
            binary.display()
        ));
    }
    let mut command = Command::new(binary);
    command
        .current_dir(&config.workspace)
        .args(["--provider", "vastai", "--image", image, "--state-dir"])
        .arg(state_dir)
        .arg("--run-id")
        .arg(config.run_id.to_string());
    match &config.provider {
        HarnessProvider::LocalMock => {
            command
                .args(["--vastai-provisioning", "mock"])
                .env("MYELIN_MOCK_VASTAI_CONTAINER_PREFIX", container_prefix)
                .env("MYELIN_DOCKER_GPUS", "");
        }
        HarnessProvider::StaticSsh {
            manifest,
            identity,
            bundle,
        } => {
            if let Some(relay_url) = &config.relay_url {
                command.env("MYELIN_IROH_RELAY_URL", relay_url);
            }
            command
                .args(["--provider", "static-ssh"])
                .arg("--static-ssh-manifest")
                .arg(manifest)
                .arg("--ssh-identity")
                .arg(identity)
                .arg("--deployment-bundle")
                .arg(bundle);
        }
        HarnessProvider::VastAiReal {
            ssh_identity,
            api_key_env,
            bundle,
            admission,
        } => {
            if let Some(bundle) = bundle {
                command.arg("--deployment-bundle").arg(bundle);
            }
            let api_key = std::env::var(api_key_env).map_err(|_| {
                format!("required VastAI credential environment {api_key_env} is unset")
            })?;
            if api_key.trim().is_empty() {
                return Err(format!(
                    "required VastAI credential environment {api_key_env} is empty"
                ));
            }
            command
                .args([
                    "--vastai-provisioning",
                    "real",
                    "--vastai-require-verified",
                    "--vastai-bootstrap-command",
                    "exec /usr/local/bin/myelin-node",
                    "--vastai-ssh-identity",
                ])
                .arg(ssh_identity)
                // The app reads VAST_API_KEY before either legacy alias.
                .env("VAST_API_KEY", api_key)
                .env_remove("MYELIN_VASTAI_API_KEY")
                .env_remove("VASTAI_API_KEY")
                .env("MYELIN_PAID_FIXTURE_ADMISSION", admission);
        }
    }
    let mut monotonic = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    if unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut monotonic) } != 0 {
        return Err(format!(
            "read monotonic subprocess deadline: {}",
            std::io::Error::last_os_error()
        ));
    }
    let now_ms = (monotonic.tv_sec as u128) * 1_000 + (monotonic.tv_nsec as u128) / 1_000_000;
    let deadline_ms = now_ms
        + budget
            .remaining("orchestrator subprocess ownership")?
            .as_millis();
    command.env(
        "MYELIN_E2E_EXECUTION_DEADLINE_MONOTONIC_MS",
        deadline_ms.to_string(),
    );
    if reset_state {
        command.arg("--reset-state");
    }
    command
        .arg("--telemetry-frame-log")
        .arg(telemetry)
        .env("MYELIN_DASHBOARD_PORT", dashboard_port.to_string())
        .env("MYELIN_IROH_BIND_PORT", dashboard_port.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut orchestrator = command
        .spawn()
        .map_err(|error| format!("start real Myelin orchestrator: {error}"))?;
    let pidfd = unsafe { libc::syscall(libc::SYS_pidfd_open, orchestrator.id(), 0) };
    if pidfd < 0 {
        let error = std::io::Error::last_os_error();
        let _ = orchestrator.kill();
        let _ = wait_for_child(
            &mut orchestrator,
            budget,
            "reap child without process identity",
        );
        return Err(format!("open owned orchestrator process identity: {error}"));
    }
    let pidfd = unsafe { OwnedFd::from_raw_fd(pidfd as i32) };
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
    Ok((orchestrator, pidfd, stdout, stderr))
}

impl ClusterHarness {
    pub fn start(config: ClusterHarnessConfig) -> Result<Self, String> {
        let execution = Budget::new(config.deadline);
        let cleanup = Budget::new(
            config.deadline + Duration::from_secs(crate::campaign::CLEANUP_DEADLINE_SECS),
        );
        Self::start_with_budgets(config, execution, cleanup)
    }

    pub fn start_with_budgets(
        config: ClusterHarnessConfig,
        execution: Budget,
        cleanup: Budget,
    ) -> Result<Self, String> {
        Self::start_internal(config, execution, cleanup, true)
    }

    /// Dispatch real deployment while keeping boundary injection on the caller's owner.
    pub fn start_deployment_unobserved(
        config: ClusterHarnessConfig,
        budget: Budget,
        cleanup: Budget,
    ) -> Result<Self, String> {
        Self::start_internal(config, budget, cleanup, false)
    }

    fn start_internal(
        config: ClusterHarnessConfig,
        budget: Budget,
        fixture_cleanup_budget: Budget,
        observe_deployment: bool,
    ) -> Result<Self, String> {
        let lifecycle_started = Instant::now();
        budget.check("fixture construction")?;
        match &config.provider {
            HarnessProvider::LocalMock | HarnessProvider::StaticSsh { .. }
                if !(2..=5).contains(&config.node_count) =>
            {
                return Err("local cluster harness requires two to five workers".to_owned());
            }
            HarnessProvider::VastAiReal { .. }
                if config.node_count != 5
                    && (config.provision
                        || config.reset_state
                        || !(3..=4).contains(&config.node_count)) =>
            {
                return Err(
                    "paid acquisition requires five workers; retained recovery requires three to five"
                        .to_owned(),
                );
            }
            _ => {}
        }
        if config.provision
            && !matches!(&config.provider, HarnessProvider::StaticSsh { .. })
            && config.selected_offer_ids.len() != usize::from(config.node_count)
        {
            return Err("provisioning requires one exact selected offer per worker".to_owned());
        }
        if matches!(&config.provider, HarnessProvider::VastAiReal { .. }) && config.image.is_none()
        {
            return Err(
                "paid VastAI campaign requires an explicit remote runtime image".to_owned(),
            );
        }
        if matches!(&config.provider, HarnessProvider::StaticSsh { .. }) && config.build_image {
            return Err(
                "static-ssh fixtures deploy a bundle; docker image building is not applicable"
                    .to_owned(),
            );
        }
        fs::create_dir_all(&config.artifacts)
            .map_err(|error| format!("create artifact directory: {error}"))?;
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| format!("system clock precedes epoch: {error}"))?
            .as_millis();
        let state_dir = match &config.state_dir {
            Some(path) => path.clone(),
            None => crate::resources::private_fixture_dir(&config.artifacts)?
                .join(format!("state-{}-{nonce}", std::process::id())),
        };
        {
            use std::os::unix::fs::DirBuilderExt;
            fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&state_dir)
                .map_err(|error| format!("create harness state directory: {error}"))?;
        }
        let state_dir = state_dir
            .canonicalize()
            .map_err(|error| format!("resolve harness state directory: {error}"))?;
        let artifact_dir = config
            .artifacts
            .canonicalize()
            .map_err(|error| format!("resolve harness artifact directory: {error}"))?;
        if state_dir.starts_with(&artifact_dir) {
            return Err("runtime identity state cannot reside in publishable artifacts".to_owned());
        }
        let image = config
            .image
            .clone()
            .unwrap_or_else(|| format!("myelin-e2e:{}-{nonce}", std::process::id()));
        let binaries = resolve_myelin_binaries(&config.workspace, &budget)?;
        if config.build_image {
            if !matches!(
                &config.provider,
                HarnessProvider::LocalMock | HarnessProvider::StaticSsh { .. }
            ) {
                return Err(
                    "remote runtime images must be built and published before paid execution"
                        .to_owned(),
                );
            }
            build_workload_image(&config.workspace, &image, &budget)?;
        }
        if !config.build_image && matches!(config.provider, HarnessProvider::LocalMock) {
            docker_image_identity(&image, &budget)?;
        }
        let port = reserve_port()?;
        let base_url = format!("http://127.0.0.1:{port}");
        let container_prefix = format!("myelin-e2e-{}-{nonce}", std::process::id());
        let telemetry = config.artifacts.join(format!("telemetry-{nonce}.jsonl"));
        let provider_client = match &config.provider {
            HarnessProvider::VastAiReal { api_key_env, .. } => {
                let key = std::env::var(api_key_env).map_err(|_| {
                    format!("required VastAI credential environment {api_key_env} is unset")
                })?;
                Some(BlockingVastClient::new(VastClient::new(key))?)
            }
            _ => None,
        };
        let (orchestrator, orchestrator_pidfd, stdout, stderr) = spawn_orchestrator(
            &config,
            &binaries.orchestrator,
            port,
            &state_dir,
            &image,
            &container_prefix,
            &telemetry,
            &fixture_cleanup_budget,
            config.reset_state,
        )?;
        let retain_on_start_error =
            !matches!(config.provider, HarnessProvider::LocalMock) && !config.reset_state;
        let mut harness = Self {
            execution_budget: budget,
            cleanup_budget: None,
            fixture_cleanup_budget,
            binaries,
            provider_client,
            lifecycle_started,
            health_boundary: AtomicU64::new(0),
            observation_generation: AtomicU64::new(0),
            config,
            base_url,
            dashboard_port: port,
            state_dir,
            image,
            container_prefix,
            telemetry,
            telemetry_census: Mutex::new(TelemetryResourceCensus::default()),
            provider_baseline: BTreeSet::new(),
            fixture_mapping: BTreeMap::new(),
            stopped_nodes: BTreeSet::new(),
            provider_accounting_baseline: None,
            node_generation_baseline: BTreeMap::new(),
            fixture_blob_baseline: None,
            fixture_actor_baseline: BTreeSet::new(),
            verified_local_baseline: None,
            orchestrator,
            orchestrator_pidfd,
            stdout,
            stderr,
            node_ids: Vec::new(),
            next_attempt_id: 0,
            active_attempts: BTreeMap::new(),
            pending_control_processes: Mutex::new(BTreeSet::new()),
            last_failure_signature: None,
            last_attempt_id: None,
            pending_attempts: BTreeMap::new(),
            quarantine_reason: Mutex::new(None),
            torn_down: retain_on_start_error,
        };
        if !harness.config.reset_state {
            if let HarnessProvider::VastAiReal { admission, .. } = &harness.config.provider {
                let binding = provisioning::PaidFixtureAdmission::open(admission)
                    .and_then(|ownership| ownership.bind_orchestrator(harness.orchestrator.id()));
                if let Err(error) = binding {
                    let _ = harness.orchestrator.kill();
                    let _ = wait_for_child(
                        &mut harness.orchestrator,
                        &harness.fixture_cleanup_budget,
                        "reap unbound retained orchestrator",
                    );
                    return Err(error);
                }
            }
        }
        harness.wait_for_dashboard()?;
        if !harness.config.reset_state {
            harness.adopt_retained_nodes()?;
        }
        if !observe_deployment {
            if harness.config.provision {
                harness.dispatch_provision()?;
            }
        } else if harness.config.provision {
            harness.complete_deployment_after_dispatch(true)?;
        } else if !harness.config.reset_state && !harness.config.adopt_only {
            harness
                .provision_nodes(false)
                .map_err(|error| format!("{error}; {}", harness.orchestrator_diagnostics()))?;
            harness.record_health_baseline()?;
            // An adopt-only resume snapshots its baseline after in-place
            // repair: the retained agents may be down and their contextual
            // control is restored by the repair itself.
        }
        harness.torn_down = false;
        harness.record_lifecycle("fixture_start", lifecycle_started, &Ok(()))?;
        Ok(harness)
    }

    pub fn set_execution_budget(&mut self, budget: Budget) {
        self.execution_budget = budget;
        self.cleanup_budget = None;
    }

    pub fn execution_budget(&self) -> &Budget {
        &self.execution_budget
    }

    pub fn set_cleanup_budget(&mut self, budget: Budget) {
        self.fixture_cleanup_budget = budget;
    }

    pub(crate) fn operation_budget(&self) -> &Budget {
        self.cleanup_budget
            .as_ref()
            .unwrap_or(&self.execution_budget)
    }

    pub fn complete_deployment(&mut self) -> Result<(), String> {
        self.complete_deployment_after_dispatch(false)
    }

    fn complete_deployment_after_dispatch(&mut self, dispatch: bool) -> Result<(), String> {
        self.provision_nodes(dispatch)?;
        self.wait_contextual_control()?;
        self.record_health_baseline()
    }

    fn record_lifecycle<T: serde::Serialize>(
        &self,
        stage: &str,
        started: Instant,
        result: &Result<T, String>,
    ) -> Result<(), String> {
        let record = json!({
            "schema_version": 2,
            "run_id": self.config.run_id,
            "orchestrator_pid": self.orchestrator.id(),
            "stage": stage,
            "started_monotonic_ns": started.duration_since(self.lifecycle_started).as_nanos(),
            "elapsed_ns": started.elapsed().as_nanos(),
            "pending_predicate": result.as_ref().err(),
            "succeeded": result.is_ok(),
            "observation": result.as_ref().ok(),
        });
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.config.artifacts.join("lifecycle-timings.jsonl"))
            .map_err(|error| format!("open lifecycle evidence: {error}"))?;
        serde_json::to_writer(&mut file, &record)
            .map_err(|error| format!("write lifecycle evidence: {error}"))?;
        writeln!(file).map_err(|error| format!("finish lifecycle evidence: {error}"))
    }

    pub fn is_quarantined(&self) -> bool {
        self.quarantine_reason().is_some()
    }

    fn quarantine_reason(&self) -> Option<String> {
        self.quarantine_reason
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }

    fn quarantine(&self, reason: String) {
        self.quarantine_reason
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get_or_insert(reason);
    }

    pub(super) fn next_observation_generation(&self) -> u64 {
        self.observation_generation.fetch_add(1, Ordering::Relaxed) + 1
    }

    fn bounded_lifecycle(
        &mut self,
        stage: &str,
        run: impl FnOnce(&mut Self) -> Result<(), String>,
    ) -> Result<(), String> {
        let started = Instant::now();
        let budget = self.operation_budget().child(self.config.deadline);
        let previous = self.cleanup_budget.replace(budget);
        let result = run(self);
        self.cleanup_budget = previous;
        self.record_lifecycle(stage, started, &result)?;
        result
    }

    pub(super) fn control_revision(&self, budget: &Budget) -> Result<Value, String> {
        let reply = http_json_budget(
            "POST",
            &format!("{}/api/control/changes", self.base_url),
            Some(json!({"schema_version": 1, "payload": {
                "generation": null, "after_revision": null, "wait_ms": 0
            }})),
            budget,
        )?;
        validate_control_revision(&reply)?;
        Ok(reply["payload"].clone())
    }

    pub(super) fn wait_for_control_change(
        &self,
        cursor: &Value,
        budget: &Budget,
        predicate: &str,
    ) -> Result<(), String> {
        self.ensure_orchestrator_live()?;
        let wait_ms = budget.remaining(predicate)?.as_millis().min(1_000) as u64;
        let reply = http_json_budget(
            "POST",
            &format!("{}/api/control/changes", self.base_url),
            Some(json!({"schema_version": 1, "payload": {
                "generation": cursor["generation"], "after_revision": cursor["revision"], "wait_ms": wait_ms
            }})),
            budget,
        );
        match reply {
            Ok(reply) => validate_control_revision(&reply)?,
            Err(error) if error.contains("504") => {}
            Err(error) => {
                budget.wait(
                    POLL_INTERVAL,
                    &format!("{predicate}; control observation reconnect: {error}"),
                )?;
            }
        }
        budget.check(predicate)?;
        self.ensure_orchestrator_live()
    }

    /// Resolve the exact retained live-node set without initiating provisioning
    /// or accepting a replacement logical identity.
    fn retained_live_node_ids(&self) -> Result<Vec<u64>, String> {
        let mut adopted = self
            .retained_node_specs()?
            .into_iter()
            .map(|(node_id, _, _)| node_id)
            .collect::<Vec<_>>();
        adopted.sort_unstable();
        if adopted.contains(&0)
            || adopted.windows(2).any(|pair| pair[0] == pair[1])
            || adopted.len() != usize::from(self.config.node_count)
        {
            return Err(format!(
                "retained fixture must contain {} exact, unique live logical nodes; observed {adopted:?}",
                self.config.node_count
            ));
        }
        Ok(adopted)
    }
    fn retained_stopped_node_ids(&self) -> Result<BTreeSet<u64>, String> {
        let snapshot: Value = serde_json::from_slice(
            &fs::read(self.state_dir.join("cluster.json"))
                .map_err(|error| format!("read retained node phases: {error}"))?,
        )
        .map_err(|error| format!("parse retained node phases: {error}"))?;
        let mut seen = BTreeSet::new();
        let mut stopped = BTreeSet::new();
        for node in snapshot["nodes"]
            .as_array()
            .ok_or("retained cluster snapshot omitted nodes")?
        {
            let node_id = node["logical_node_id"]
                .as_u64()
                .filter(|node_id| *node_id != 0)
                .ok_or_else(|| format!("retained node omitted valid identity: {node}"))?;
            if !seen.insert(node_id) {
                return Err(format!(
                    "retained cluster duplicated logical node {node_id}"
                ));
            }
            match node["phase"].as_str() {
                Some("stopped") => {
                    stopped.insert(node_id);
                }
                Some("orphan") => {
                    return Err(format!(
                        "retained logical node {node_id} is orphaned and cannot be adopted"
                    ));
                }
                Some(_) => {}
                None => return Err(format!("retained node {node_id} omitted phase")),
            }
        }
        Ok(stopped)
    }

    /// Adopt the retained fixture without waiting for any node state: fill
    /// the live-node list from the durable snapshot so in-place repair can
    /// relaunch agents before running-state waits happen.
    fn adopt_retained_nodes(&mut self) -> Result<(), String> {
        let live = self.retained_live_node_ids()?;
        let stopped = self.retained_stopped_node_ids()?;
        if live.iter().any(|node| stopped.contains(node)) {
            return Err("retained live and stopped logical-node sets overlap".to_owned());
        }
        self.node_ids = live;
        self.stopped_nodes = stopped;
        Ok(())
    }

    /// Captured orchestrator process output for failure diagnostics.
    pub fn orchestrator_diagnostics(&mut self) -> String {
        let exit = match self.orchestrator.try_wait() {
            Ok(Some(status)) => format!("exited: {status}"),
            Ok(None) => "running".to_owned(),
            Err(error) => format!("try_wait failed: {error}"),
        };
        let stdout = self
            .stdout
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        let stderr = self
            .stderr
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n");
        format!("orchestrator {exit}; stdout tail:\n{stdout}\norchestrator stderr tail:\n{stderr}")
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
    pub fn fixture_run_id(&self) -> Result<u64, String> {
        let snapshot = serde_json::from_slice::<Value>(
            &fs::read(self.state_dir.join("cluster.json"))
                .map_err(|error| format!("read cluster run id: {error}"))?,
        )
        .map_err(|error| format!("parse cluster run id: {error}"))?;
        snapshot
            .get("run_id")
            .and_then(Value::as_u64)
            .filter(|run_id| *run_id != 0)
            .ok_or_else(|| "cluster snapshot omitted nonzero run_id".to_owned())
    }
    pub fn search_offers(&self, request: Value) -> Result<Value, String> {
        http_json_budget(
            "POST",
            &format!("{}/api/control/offers", self.base_url),
            Some(request),
            &self.operation_budget().child(self.config.deadline),
        )
    }

    pub fn provision_selected(
        &mut self,
        selected_offer_ids: Vec<u64>,
        search_id: u64,
    ) -> Result<(), String> {
        if !self.node_ids.is_empty() {
            return Err("fixture is already provisioned".to_owned());
        }
        if selected_offer_ids.len() != usize::from(self.config.node_count) {
            return Err("exact offer count differs from fixture node count".to_owned());
        }
        self.config.selected_offer_ids = selected_offer_ids;
        self.config.offer_search_id = Some(search_id);
        self.complete_deployment_after_dispatch(true)
    }

    /// Authorizes the exact five-node paid acquisition this fixture will run.
    pub fn authorize_paid_admission(
        &mut self,
        run_id: u64,
        nodes: Vec<provisioning::PaidNodeAdmission>,
        deadline_unix_ms: u64,
    ) -> Result<(), String> {
        let admission_path = match &self.config.provider {
            HarnessProvider::VastAiReal { admission, .. } => admission.clone(),
            _ => return Err("paid admission requires the real VastAI provider".to_owned()),
        };
        provisioning::PaidFixtureAdmission::create(
            &admission_path,
            run_id,
            nodes,
            deadline_unix_ms,
        )
        .map_err(|error| format!("authorize paid admission: {error}"))?;
        Ok(())
    }

    /// Seals preparation once five exact contracts and bootstrap sessions exist.
    pub fn seal_paid_admission(&self) -> Result<(), String> {
        let admission_path = match &self.config.provider {
            HarnessProvider::VastAiReal { admission, .. } => admission,
            _ => return Err("paid admission requires the real VastAI provider".to_owned()),
        };
        let admission = provisioning::PaidFixtureAdmission::open(admission_path)
            .map_err(|error| format!("open paid admission: {error}"))?;
        admission
            .seal_prepared()
            .map_err(|error| format!("seal paid admission: {error}"))?;
        let snapshot = admission
            .snapshot()
            .map_err(|error| format!("read paid admission: {error}"))?;
        match snapshot.mode {
            provisioning::PaidAdmissionMode::Prepared => Ok(()),
            _ => Err("paid fixture preparation is incomplete or closed".to_owned()),
        }
    }

    fn record_health_baseline(&mut self) -> Result<(), String> {
        self.record_health_baseline_snapshot(true).map(drop)
    }

    fn record_health_baseline_snapshot(
        &mut self,
        require_stable_fixture_sample: bool,
    ) -> Result<Value, String> {
        if !self.config.reset_state
            && matches!(self.config.provider, HarnessProvider::VastAiReal { .. })
        {
            self.stopped_nodes = self
                .paid_admission()?
                .nodes
                .iter()
                .map(|node| node.node_id)
                .filter(|node| !self.node_ids.contains(node))
                .collect();
        }
        let budget = self.operation_budget().child(self.config.deadline);
        let mut prior_fixture_actors = None;
        let (baseline, fixture_actor_baseline) = loop {
            let cursor = self.control_revision(&budget)?;
            let mut health = self.health_snapshot_for_nodes(&self.node_ids, &budget)?;
            let fixture = self.builtin_fixture_blobs(&budget)?;
            let fixture_actors = retained_actor_identities(&health, &fixture)?;
            let retained = self
                .active_attempts
                .values()
                .map(|attempt| RetainedCaseActors {
                    attempt: Arc::clone(attempt),
                })
                .collect::<Vec<_>>();
            let mut resource_baseline = fixture_actors.clone();
            for attempt in &retained {
                resource_baseline.extend(self.retained_blob_exemptions(attempt, &health, &budget)?);
            }
            health["builtin_fixture"] =
                serde_json::to_value(&fixture).map_err(|error| error.to_string())?;
            if let Some(reason) = pending_resource_cleanup(&health, &resource_baseline) {
                self.wait_for_control_change(
                    &cursor,
                    &budget,
                    &format!("clean initial fixture: {reason}"),
                )?;
                continue;
            }
            if require_stable_fixture_sample
                && prior_fixture_actors.as_ref() != Some(&fixture_actors)
            {
                prior_fixture_actors = Some(fixture_actors);
                budget.wait(POLL_INTERVAL, "stable built-in fixture actor identities")?;
                continue;
            }
            let blob = &fixture[BUILTIN_FIXTURE_PATH];
            let observed_blob = (blob.revision, blob.length);
            if self
                .fixture_blob_baseline
                .is_some_and(|expected| expected != observed_blob)
            {
                return Err(format!(
                    "built-in fixture blob baseline changed: expected={:?}, observed={observed_blob:?}",
                    self.fixture_blob_baseline
                ));
            }
            self.fixture_blob_baseline.get_or_insert(observed_blob);
            break (health, fixture_actors);
        };
        self.fixture_actor_baseline = fixture_actor_baseline;
        self.provider_accounting_baseline = baseline
            .get("provider_accounting")
            .filter(|value| !value.is_null())
            .cloned();
        self.provider_baseline = serde_json::from_value(
            baseline
                .get("provider_resources")
                .cloned()
                .ok_or_else(|| "health snapshot omitted provider resources".to_owned())?,
        )
        .map_err(|error| format!("invalid provider resource census: {error}"))?;
        self.fixture_mapping =
            fixture_node_mapping(&baseline, &self.node_ids, &self.stopped_nodes)?;
        validate_fixture_census(
            &baseline,
            &self.provider_baseline,
            &self.node_ids,
            &self.fixture_mapping,
            &self.stopped_nodes,
        )?;
        self.node_generation_baseline = baseline["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .map(|node| {
                let observation = &node["observation"];
                let id = observation["logical_node_id"]
                    .as_u64()
                    .ok_or_else(|| "baseline contextual node omitted identity".to_owned())?;
                Ok((
                    id,
                    resource_runtime_identity(&observation["event"]["resources"]),
                ))
            })
            .collect::<Result<_, String>>()?;
        self.persist_fixture_identity(&baseline)?;
        Ok(baseline)
    }

    pub fn fixture_identity_digest(&self) -> Result<String, String> {
        use sha2::Digest;
        if self.fixture_mapping.is_empty() {
            return Err("fixture identity is unavailable before validated readiness".to_owned());
        }
        let identity = json!({
            "schema_version": 1, "node_contract_hosts": self.fixture_mapping,
            "admission": self.provider_accounting_baseline,
        });
        let bytes = serde_json::to_vec(&identity).map_err(|error| error.to_string())?;
        Ok(format!("{:x}", sha2::Sha256::digest(bytes)))
    }

    fn persist_fixture_identity(&self, health: &Value) -> Result<(), String> {
        let stem = self
            .telemetry
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or("telemetry identity")?;
        let generation = health["generation"]
            .as_u64()
            .ok_or("fixture observation generation")?;
        let proof = json!({
            "schema_version": 1, "fixture_identity_digest": self.fixture_identity_digest()?,
            "node_contract_hosts": self.fixture_mapping, "admission": self.provider_accounting_baseline,
            "running_nodes": self.node_ids, "stopped_nodes": self.stopped_nodes,
            "provider_resources": health["provider_resources"], "provider_hosts": health["provider_hosts"],
            "control_revision": health["control_revision"], "generation": generation,
            "contextual_nodes": health["nodes"], "live_resource_baseline": health["resources"],
            "fixture_actor_baseline": self.fixture_actor_baseline,
            "builtin_fixture": health["builtin_fixture"],
        });
        let path = self
            .config
            .artifacts
            .join(format!("fixture-identity-{stem}.jsonl"));
        let created = !path.exists();
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| format!("open fixture identity evidence: {error}"))?;
        serde_json::to_writer(&mut file, &proof).map_err(|error| error.to_string())?;
        file.write_all(b"\n")
            .map_err(|error| format!("finish fixture identity record: {error}"))?;
        file.sync_data()
            .map_err(|error| format!("sync fixture identity evidence: {error}"))?;
        if created {
            fs::File::open(&self.config.artifacts)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| format!("sync fixture identity directory: {error}"))?;
        }
        Ok(())
    }
    pub fn owned_provider_contracts(&self) -> Result<BTreeSet<u64>, String> {
        match &self.config.provider {
            HarnessProvider::LocalMock => {
                Err("local fixtures do not own VastAI contracts".to_owned())
            }
            HarnessProvider::StaticSsh { .. } => Ok(BTreeSet::new()),
            HarnessProvider::VastAiReal { .. } => self.remote_contracts(),
        }
    }

    pub fn owned_provider_labels(&self) -> Result<BTreeSet<String>, String> {
        match &self.config.provider {
            HarnessProvider::LocalMock => Err("local fixtures do not own VastAI labels".to_owned()),
            HarnessProvider::StaticSsh { .. } => Ok(BTreeSet::new()),
            HarnessProvider::VastAiReal { .. } => self.remote_labels(),
        }
    }

    /// Per-node agent launch facts (node id, environment, argv) from the
    /// retained cluster snapshot, used to relaunch agents in place.
    fn retained_node_specs(
        &self,
    ) -> Result<Vec<(u64, Vec<(String, String)>, Vec<String>)>, String> {
        let snapshot = serde_json::from_slice::<serde_json::Value>(
            &fs::read(self.state_dir.join("cluster.json"))
                .map_err(|error| format!("read cluster snapshot: {error}"))?,
        )
        .map_err(|error| format!("parse cluster snapshot: {error}"))?;
        snapshot
            .get("nodes")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
            .filter(|node| {
                node.get("phase")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|phase| phase != "stopped" && phase != "orphan")
            })
            .map(|node| {
                let node_id = node
                    .get("logical_node_id")
                    .and_then(serde_json::Value::as_u64)
                    .ok_or_else(|| "cluster node omitted logical_node_id".to_owned())?;
                let spec = node
                    .get("spec")
                    .ok_or_else(|| format!("cluster node {node_id} omitted its spec"))?;
                let env = spec
                    .get("env")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .map(|pair| {
                        let pair = pair
                            .as_array()
                            .ok_or_else(|| "node spec env entry is not a pair".to_owned())?;
                        let key = pair
                            .first()
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| "node spec env pair omitted key".to_owned())?;
                        let value = pair
                            .get(1)
                            .and_then(serde_json::Value::as_str)
                            .ok_or_else(|| "node spec env pair omitted value".to_owned())?;
                        Ok((key.to_owned(), value.to_owned()))
                    })
                    .collect::<Result<Vec<_>, String>>()?;
                let args = spec
                    .get("args")
                    .and_then(serde_json::Value::as_array)
                    .into_iter()
                    .flatten()
                    .filter_map(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                Ok((node_id, env, args))
            })
            .collect()
    }
    pub fn retain_remote_fixture(&mut self, development_authorized: bool) -> Result<(), String> {
        if !self.active_attempts.is_empty()
            || !self.pending_attempts.is_empty()
            || !self
                .pending_control_processes
                .lock()
                .map_err(|_| "retained fixture control ownership lock poisoned".to_owned())?
                .is_empty()
        {
            return Err(
                "fixture retention requires quiescent workload and control ownership".to_owned(),
            );
        }
        if matches!(self.config.provider, HarnessProvider::LocalMock) {
            return Err("fixture retention requires a remote-node provider".to_owned());
        }
        if let HarnessProvider::VastAiReal { admission, .. } = &self.config.provider {
            provisioning::PaidFixtureAdmission::open(admission)?
                .retain_development_fixture(development_authorized)?;
        } else if !development_authorized {
            return Err("fixture retention requires explicit development authorization".to_owned());
        }
        self.torn_down = true;
        // Retention releases the local owner, including after injected death
        // or execution expiry. It is cleanup, not a new crash injection.
        let signal = self.orchestrator.kill();
        wait_for_child(
            &mut self.orchestrator,
            &self.fixture_cleanup_budget,
            "retained orchestrator reap",
        )
        .map_err(|error| match signal {
            Ok(()) => error,
            Err(signal) => format!("stop retained orchestrator: {signal}; {error}"),
        })
    }
}

impl ClusterHarness {
    fn vastai_client(&self) -> Result<&BlockingVastClient, String> {
        self.provider_client
            .as_ref()
            .ok_or_else(|| "VastAI client requested for local fixture".to_owned())
    }

    fn provider_host_mapping(
        &self,
        fleet: &Value,
        containers: Option<&BTreeMap<String, String>>,
    ) -> Result<BTreeMap<u64, Value>, String> {
        let nodes = fleet["FleetStatus"]["nodes"]
            .as_array()
            .ok_or("fleet omitted node mapping")?;
        match &self.config.provider {
            HarnessProvider::VastAiReal { .. } => Ok(self
                .paid_admission()?
                .nodes
                .into_iter()
                .map(|node| (node.node_id, json!(node.host_id)))
                .collect()),
            HarnessProvider::StaticSsh { manifest, .. } => {
                let manifest: Value = serde_json::from_slice(
                    &fs::read(manifest)
                        .map_err(|error| format!("read raw fixture host mapping: {error}"))?,
                )
                .map_err(|error| format!("decode raw fixture host mapping: {error}"))?;
                let mut hosts = BTreeMap::new();
                let mut resources = BTreeSet::new();
                for node in manifest["nodes"]
                    .as_array()
                    .ok_or("raw manifest omitted nodes")?
                {
                    let id = node["slot"]
                        .as_u64()
                        .and_then(|slot| slot.checked_add(1))
                        .ok_or("raw slot identity")?;
                    let resource = node["container"]
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .ok_or("raw resource identity")?;
                    let host = node["ssh_host"]
                        .as_str()
                        .filter(|value| !value.is_empty())
                        .ok_or("raw SSH host")?;
                    let port = node["ssh_port"]
                        .as_u64()
                        .filter(|port| (1..=65535).contains(port))
                        .ok_or("raw SSH port")?;
                    if hosts
                        .insert(
                            id,
                            json!({"container": resource, "ssh_host": host, "ssh_port": port}),
                        )
                        .is_some()
                        || !resources.insert(resource)
                    {
                        return Err("duplicate raw node/resource host mapping".to_owned());
                    }
                }
                for node in nodes.iter().filter(|node| node["phase"] == "running") {
                    let id = node["logical_node_id"]
                        .as_u64()
                        .ok_or("raw running node identity")?;
                    let expected = hosts
                        .get(&id)
                        .ok_or_else(|| format!("node {id} has no raw host admission"))?;
                    if node["provider_ref"]
                        != format!(
                            "static-ssh:{}",
                            expected["container"].as_str().ok_or("raw resource")?
                        )
                    {
                        return Err(format!(
                            "raw node {id} is associated with the wrong container/SSH host"
                        ));
                    }
                }
                Ok(hosts)
            }
            HarnessProvider::LocalMock => nodes
                .iter()
                .filter(|node| node["phase"] == "running")
                .map(|node| {
                    let name = node["provider_ref"]
                        .as_str()
                        .ok_or("local provider reference")?;
                    let identity =
                        containers
                            .and_then(|census| census.get(name))
                            .ok_or_else(|| {
                                format!(
                                    "local provider reference {name} is absent from Docker census"
                                )
                            })?;
                    Ok((
                        node["logical_node_id"]
                            .as_u64()
                            .ok_or("local node identity")?,
                        json!({"container": name, "container_id": identity}),
                    ))
                })
                .collect(),
        }
    }

    fn paid_admission(&self) -> Result<provisioning::PaidAdmissionSnapshot, String> {
        let HarnessProvider::VastAiReal { admission, .. } = &self.config.provider else {
            return Err("paid admission requested for local fixture".to_owned());
        };
        provisioning::PaidFixtureAdmission::open(admission)?.snapshot()
    }

    fn remote_labels(&self) -> Result<BTreeSet<String>, String> {
        // Admission reserves labels before create, including responses lost before
        // cluster.json could record a lease. It remains authoritative after stop.
        Ok(self
            .paid_admission()?
            .nodes
            .into_iter()
            .map(|node| node.label)
            .collect())
    }

    fn remote_contracts(&self) -> Result<BTreeSet<u64>, String> {
        self.remote_contracts_budget(&self.operation_budget().child(self.config.deadline))
    }

    fn remote_contracts_budget(&self, budget: &Budget) -> Result<BTreeSet<u64>, String> {
        let started = Instant::now();
        let admission = self.paid_admission()?;
        let labels = admission.cleanup_labels();
        let known = admission.known_contract_ids();
        let census = self.vastai_client()?.owned_census(
            &labels,
            &known,
            started,
            budget.remaining("fresh exact owned provider census")?,
        )?;
        for instance in &census.instances {
            let node = admission
                .contracts
                .iter()
                .find_map(|(node, contract)| (*contract == instance.contract_id).then_some(*node))
                .and_then(|id| admission.nodes.iter().find(|node| node.node_id == id))
                .ok_or_else(|| format!("unadmitted provider contract {}", instance.contract_id))?;
            if instance.host_id != Some(node.host_id) {
                return Err(format!(
                    "node {} contract {} moved hosts: expected {}, observed {:?}",
                    node.node_id, instance.contract_id, node.host_id, instance.host_id
                ));
            }
        }
        self.record_lifecycle("provider_census", started, &Ok(()))?;
        let record = json!({
            "schema_version": 1, "generation": census.generation,
            "elapsed_ns": started.elapsed().as_nanos(),
            "request_started_ns": census.request_started.saturating_duration_since(self.lifecycle_started).as_nanos(),
            "observed_ns": census.observed_at.saturating_duration_since(self.lifecycle_started).as_nanos(),
            "counters": census.counters,
            "contracts": census.instances.iter().map(|instance| instance.contract_id).collect::<BTreeSet<_>>(),
            "missing_contract_ids": census.missing_contract_ids,
        });
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.config.artifacts.join("provider-census.jsonl"))
            .map_err(|error| format!("open scoped provider census evidence: {error}"))?;
        serde_json::to_writer(&mut file, &record)
            .map_err(|error| format!("write scoped provider census evidence: {error}"))?;
        writeln!(file)
            .map_err(|error| format!("finish scoped provider census evidence: {error}"))?;
        Ok(census
            .instances
            .into_iter()
            .map(|instance| instance.contract_id)
            .collect())
    }

    fn provider_resources(&self) -> Result<BTreeSet<String>, String> {
        self.provider_resources_budget(&self.operation_budget().child(self.config.deadline))
    }

    fn provider_resources_budget(&self, budget: &Budget) -> Result<BTreeSet<String>, String> {
        match &self.config.provider {
            HarnessProvider::LocalMock => Ok(list_containers(&self.container_prefix, budget)?
                .into_iter()
                .collect()),
            HarnessProvider::StaticSsh { manifest, .. } => {
                Ok(raw_fleet::read_manifest_containers(manifest, budget)?
                    .into_iter()
                    .collect())
            }
            HarnessProvider::VastAiReal { .. } => Ok(self
                .remote_contracts_budget(budget)?
                .into_iter()
                .map(|contract| format!("vastai:{contract}"))
                .collect()),
        }
    }

    fn crash_orchestrator(&mut self) -> Result<(), String> {
        self.operation_budget()
            .check("orchestrator crash and reap")?;
        self.ensure_orchestrator_live()?;
        self.orchestrator
            .kill()
            .map_err(|error| format!("kill orchestrator for recovery: {error}"))?;
        let budget = self.operation_budget().clone();
        wait_for_child(&mut self.orchestrator, &budget, "reap crashed orchestrator")?;
        let mut census = self
            .telemetry_census
            .lock()
            .map_err(|_| "telemetry census is poisoned".to_owned())?;
        // Drain the stopped producer before resetting its sequence. Otherwise
        // unread pre-crash frames can repopulate the retired incarnation.
        census.update(&self.telemetry, &budget)?;
        census.forget_stream(&format!("myelin-orchestrator#{}", self.config.run_id));
        Ok(())
    }

    fn restart_orchestrator(&mut self) -> Result<(), String> {
        self.operation_budget()
            .check("restart retained orchestrator")?;
        let retained_nodes = self.retained_live_node_ids()?;
        let retained_stopped = self.retained_stopped_node_ids()?;
        if retained_nodes != self.node_ids || retained_stopped != self.stopped_nodes {
            return Err(format!(
                "orchestrator restart must adopt exact logical nodes running={:?}, stopped={:?}; retained snapshot has running={retained_nodes:?}, stopped={retained_stopped:?}",
                self.node_ids, self.stopped_nodes
            ));
        }
        let (orchestrator, orchestrator_pidfd, stdout, stderr) = spawn_orchestrator(
            &self.config,
            &self.binaries.orchestrator,
            self.dashboard_port,
            &self.state_dir,
            &self.image,
            &self.container_prefix,
            &self.telemetry,
            &self.fixture_cleanup_budget,
            false,
        )?;
        self.orchestrator = orchestrator;
        self.orchestrator_pidfd = orchestrator_pidfd;
        self.stdout = stdout;
        self.stderr = stderr;
        if let HarnessProvider::VastAiReal { admission, .. } = &self.config.provider {
            provisioning::PaidFixtureAdmission::open(admission)?
                .bind_orchestrator(self.orchestrator.id())?;
        }
        self.wait_for_dashboard()
    }

    pub fn verify_running_recovery(&mut self) -> Result<(), String> {
        self.bounded_lifecycle("orchestrator_restart_rejoin", |harness| {
            if let Some(reason) = harness.quarantine_reason() {
                return Err(format!("fixture is quarantined: {reason}"));
            }
            harness.restore_owned_baseline(None)?;
            let expected_resources = harness.provider_baseline.clone();
            let expected_nodes = harness.node_ids.clone();
            let expected_mapping = harness.fixture_mapping.clone();
            let previous_pid = harness.orchestrator.id();
            if let HarnessProvider::VastAiReal { admission, .. } = &harness.config.provider {
                provisioning::PaidFixtureAdmission::open(admission)?
                    .begin_orchestrator_recovery()?;
            }
            harness.crash_orchestrator()?;
            harness.restart_orchestrator()?;
            if harness.orchestrator.id() == previous_pid {
                return Err("orchestrator restart did not create a fresh process".to_owned());
            }
            harness.provision_nodes(false)?;
            harness.wait_contextual_control()?;
            // Actor addresses are process-incarnation identities. Re-establish
            // that census after replacement. Contextual readiness already
            // proves the retained fixture is stable, so one exact post-restart
            // snapshot is sufficient and is reused for recovery validation.
            let recovered_health = harness.record_health_baseline_snapshot(false)?;
            validate_fixture_census(
                &recovered_health,
                &expected_resources,
                &expected_nodes,
                &expected_mapping,
                &harness.stopped_nodes,
            )?;
            Ok(())
        })
    }

    pub fn verify_missing_resource_recovery(&mut self) -> Result<(), String> {
        self.bounded_lifecycle("missing_resource_recovery", |harness| {
            if !matches!(harness.config.provider, HarnessProvider::LocalMock) {
                return Err("missing-resource mock recovery requires local Docker ownership".to_owned());
            }
            let mut expected_resources = harness.provider_resources()?;
            let removed_node = *harness.node_ids.last()
                .ok_or_else(|| "missing-resource recovery requires a live node".to_owned())?;
            let removed = harness.resource_for_node(removed_node)?;
            if !expected_resources.remove(&removed) {
                return Err(format!("missing node {removed_node} did not own resource {removed}"));
            }
            let expected_nodes = harness.node_ids.iter().copied()
                .filter(|&node| node != removed_node).collect::<Vec<_>>();
            let mut stopped_nodes = harness.stopped_nodes.clone();
            stopped_nodes.insert(removed_node);
            harness.crash_orchestrator()?;
            remove_container(&removed, harness.operation_budget())?;
            harness.restart_orchestrator()?;
            loop {
                harness.operation_budget().check(&format!("resource {removed} absent and node {removed_node} terminal"))?;
                harness.ensure_orchestrator_live()?;
                let cursor = harness.control_revision(harness.operation_budget())?;
                let resources = harness.provider_resources()?;
                if resources != expected_resources {
                    return Err(format!("missing provider resource was recreated: expected={expected_resources:?}, observed={resources:?}"));
                }
                let status = http_json_budget("GET", &format!("{}/api/control/status", harness.base_url),
                    None, harness.operation_budget())?;
                let nodes = status.pointer("/Status/nodes").and_then(Value::as_array)
                    .ok_or_else(|| format!("recovery status omitted nodes: {status}"))?;
                let running = nodes.iter().filter(|node| node["phase"] == "running")
                    .filter_map(|node| node["logical_node_id"].as_u64()).collect::<BTreeSet<_>>();
                let missing_is_terminal = nodes.iter().find(|node| node["logical_node_id"] == removed_node)
                    .is_some_and(|node| node["phase"] == "stopped"
                        && node["last_error"].as_str().is_some_and(|error|
                            error.contains("absent") || error.contains("no provider resource")));
                if running == expected_nodes.iter().copied().collect() && missing_is_terminal {
                    let health = harness.health_snapshot_for_nodes(&expected_nodes, harness.operation_budget())?;
                    validate_fixture_census(&health, &expected_resources, &expected_nodes,
                        &harness.fixture_mapping, &stopped_nodes)?;
                    harness.node_ids = expected_nodes;
                    harness.provider_baseline = expected_resources;
                    harness.stopped_nodes = stopped_nodes;
                    harness.record_health_baseline()?;
                    return Ok(());
                }
                harness.wait_for_control_change(&cursor, harness.operation_budget(),
                    &format!("missing-resource terminal state; observed={status}"))?;
            }
        })
    }

    pub fn remove_node(&mut self, logical_node_id: u64) -> Result<(), String> {
        self.bounded_lifecycle("destructive_transition", |harness| {
            if let Some(reason) = harness.quarantine_reason() {
                return Err(format!("fixture is quarantined: {reason}"));
            }
            if !harness.active_attempts.is_empty() || !harness.pending_attempts.is_empty() {
                return Err("node removal requires complete attempt cleanup".to_owned());
            }
            harness.assert_healthy()?;
            if !harness.node_ids.contains(&logical_node_id) {
                return Err(format!("logical node {logical_node_id} is not live"));
            }
            if harness.node_ids.len() <= 3 {
                return Err(
                    "survivor campaign cannot remove a node below three survivors".to_owned(),
                );
            }
            let before = harness.health_snapshot()?;
            validate_fixture_census(
                &before,
                &harness.provider_baseline,
                &harness.node_ids,
                &harness.fixture_mapping,
                &harness.stopped_nodes,
            )?;
            let mut expected_resources = harness.provider_resources()?;
            let target = harness.resource_for_node(logical_node_id)?;
            if !expected_resources.remove(&target) {
                return Err(format!(
                    "target node {logical_node_id} resource {target} is absent"
                ));
            }
            let expected_nodes = harness
                .node_ids
                .iter()
                .copied()
                .filter(|node| *node != logical_node_id)
                .collect::<Vec<_>>();
            let mut stopped_nodes = harness.stopped_nodes.clone();
            stopped_nodes.insert(logical_node_id);
            harness.kill_node(logical_node_id)?;
            if matches!(harness.config.provider, HarnessProvider::StaticSsh { .. }) {
                // The static provider stops the exact container but deliberately
                // retains it for deployment gates. A destructive tail owns removal.
                remove_container(&target, harness.operation_budget())?;
            }
            loop {
                harness.operation_budget().check(&format!(
                    "exact survivors {expected_nodes:?} and resources {expected_resources:?}"
                ))?;
                harness.ensure_orchestrator_live()?;
                let cursor = harness.control_revision(harness.operation_budget())?;
                let health = harness
                    .health_snapshot_for_nodes(&expected_nodes, harness.operation_budget())?;
                let stopped = health
                    .pointer("/fleet/FleetStatus/nodes")
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .find(|node| node["logical_node_id"] == logical_node_id)
                    .is_some_and(|node| node["phase"] == "stopped");
                if stopped
                    && validate_fixture_census(
                        &health,
                        &expected_resources,
                        &expected_nodes,
                        &harness.fixture_mapping,
                        &stopped_nodes,
                    )
                    .is_ok()
                {
                    harness.node_ids = expected_nodes;
                    harness.provider_baseline = expected_resources;
                    harness.stopped_nodes = stopped_nodes;
                    harness.record_health_baseline()?;
                    return Ok(());
                }
                harness.wait_for_control_change(
                    &cursor,
                    harness.operation_budget(),
                    &format!("exact survivor census: {health}"),
                )?;
            }
        })
    }

    fn resource_for_node(&self, logical_node_id: u64) -> Result<String, String> {
        if matches!(self.config.provider, HarnessProvider::VastAiReal { .. }) {
            return self
                .paid_admission()?
                .contracts
                .get(&logical_node_id)
                .map(|contract| format!("vastai:{contract}"))
                .ok_or_else(|| {
                    format!("node {logical_node_id} has no admitted provider contract")
                });
        }
        if matches!(self.config.provider, HarnessProvider::LocalMock) {
            return self
                .fixture_mapping
                .get(&logical_node_id)
                .and_then(|identity| identity["provider_host"]["container_id"].as_str())
                .map(str::to_owned)
                .ok_or_else(|| format!("node {logical_node_id} has no validated Docker identity"));
        }
        let snapshot: Value = serde_json::from_slice(
            &fs::read(self.state_dir.join("cluster.json"))
                .map_err(|error| format!("read node resource mapping: {error}"))?,
        )
        .map_err(|error| format!("decode node resource mapping: {error}"))?;
        let resource = snapshot["nodes"]
            .as_array()
            .into_iter()
            .flatten()
            .find(|node| node["logical_node_id"] == logical_node_id)
            .and_then(|node| node["provider_ref"].as_str())
            .ok_or_else(|| format!("node {logical_node_id} has no retained provider identity"))?;
        Ok(resource
            .strip_prefix("static-ssh:")
            .unwrap_or(resource)
            .to_owned())
    }

    pub fn health_snapshot(&self) -> Result<Value, String> {
        self.health_snapshot_for_nodes(
            &self.node_ids,
            &self.operation_budget().child(self.config.deadline),
        )
    }

    pub(crate) fn mark_health_boundary(&self) -> Result<(), String> {
        self.operation_budget()
            .check("post-terminal resource observation boundary")?;
        let generation = self.next_observation_generation();
        self.health_boundary.store(generation, Ordering::Release);
        self.record_lifecycle("post_terminal_resource_boundary", Instant::now(), &Ok(()))
    }

    fn health_snapshot_for_nodes(
        &self,
        expected_nodes: &[u64],
        budget: &Budget,
    ) -> Result<Value, String> {
        let started = Instant::now();
        let result = (|| {
            loop {
                budget.check("fresh consistent actor, execution, arena and provider health")?;
                self.ensure_orchestrator_live()?;
                let cursor = self.control_revision(budget)?;
                let generation = self.next_observation_generation();
                let actor_request_id = format!("health-actors-{}-{generation}", self.config.seed);
                let (fleet, nodes, provider_resources) = thread::scope(|scope| {
                    let fleet = scope.spawn(|| {
                        http_json_budget(
                            "GET",
                            &format!("{}/api/control/fleet", self.base_url),
                            None,
                            budget,
                        )
                    });
                    let nodes = scope.spawn(|| self.query_contextual_nodes(expected_nodes, budget));
                    let provider = scope.spawn(|| {
                        if matches!(self.config.provider, HarnessProvider::LocalMock) {
                            let census = container_census(&self.container_prefix, budget)?;
                            Ok((census.values().cloned().collect(), Some(census)))
                        } else {
                            self.provider_resources_budget(budget)
                                .map(|resources| (resources, None))
                        }
                    });
                    // All bounded operations are joined even if the first result fails.
                    (fleet.join(), nodes.join(), provider.join())
                });
                let fleet = fleet.map_err(|_| "fleet census collector panicked".to_owned())??;
                let nodes =
                    nodes.map_err(|_| "contextual census collector panicked".to_owned())??;
                let (provider_resources, containers) = provider_resources
                    .map_err(|_| "provider census collector panicked".to_owned())??;
                // Query observers must finish before the actor census. They
                // are not fixture actors eligible for a baseline exemption.
                let actors = http_json_budget(
                    "GET",
                    &format!(
                        "{}/api/control/actors/snapshot?request_id={actor_request_id}",
                        self.base_url
                    ),
                    None,
                    budget,
                )?;
                let after = self.control_revision(budget)?;
                if cursor != after {
                    continue;
                }
                if generation <= self.health_boundary.load(Ordering::Acquire) {
                    return Err(
                        "health snapshot crossed a newer terminal/retraction boundary".to_owned(),
                    );
                }
                let running_nodes = fleet
                    .pointer("/FleetStatus/nodes")
                    .and_then(Value::as_array)
                    .ok_or_else(|| format!("health fleet omitted nodes: {fleet}"))?
                    .iter()
                    .filter(|node| node["phase"] == "running")
                    .map(|node| {
                        node["logical_node_id"]
                            .as_u64()
                            .ok_or_else(|| format!("running node omitted identity: {node}"))
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let resources = {
                    let mut census = self
                        .telemetry_census
                        .lock()
                        .map_err(|_| "telemetry census is poisoned".to_owned())?;
                    census.update(&self.telemetry, budget)?;
                    census.record_orchestrator_snapshot(
                        &format!("myelin-orchestrator#{}", self.config.run_id),
                        &actor_request_id,
                        &actors,
                    )?;
                    for node in &nodes {
                        let observation = &node.observation;
                        let node_id = observation.logical_node_id;
                        let myelin_control_contract::ContextualHealthEvent::LiveExecutions {
                            resources,
                            ..
                        } = &observation.event;
                        let snapshot = serde_json::to_value(resources)
                            .map_err(|error| format!("encode typed resource snapshot: {error}"))?;
                        census.record_resource_snapshot(
                            &observation.request_id,
                            node_id,
                            &snapshot,
                        )?;
                        if let Some(expected) = self.node_generation_baseline.get(&node_id) {
                            let observed = resource_runtime_identity(&snapshot);
                            if &observed != expected {
                                return Err(format!(
                                    "retained node {node_id} process/deployment identity changed: expected={expected}, observed={observed}"
                                ));
                            }
                        }
                    }
                    census.snapshot_for_nodes(&expected_nodes.iter().copied().collect())
                };
                let provider_hosts = self.provider_host_mapping(&fleet, containers.as_ref())?;
                let provider_accounting = match &self.config.provider {
                    HarnessProvider::VastAiReal { .. } => {
                        let admission = self.paid_admission()?;
                        let admitted = admission
                            .nodes
                            .iter()
                            .map(|node| node.node_id)
                            .collect::<BTreeSet<_>>();
                        if admitted.len() != 5
                            || admission.contracts.keys().copied().collect::<BTreeSet<_>>()
                                != admitted
                            || admission
                                .create_reservations
                                .keys()
                                .copied()
                                .collect::<BTreeSet<_>>()
                                != admitted
                            || admission
                                .initial_bootstraps
                                .keys()
                                .copied()
                                .collect::<BTreeSet<_>>()
                                != admitted
                            || !admission.accounting_errors.is_empty()
                        {
                            return Err("paid readiness requires five exact observed creations and initial bootstrap admissions".to_owned());
                        }
                        for node in fleet["FleetStatus"]["nodes"]
                            .as_array()
                            .into_iter()
                            .flatten()
                        {
                            if node["phase"] != "running" {
                                continue;
                            }
                            let id = node["logical_node_id"]
                                .as_u64()
                                .ok_or("fleet node identity")?;
                            let specification = admission
                                .nodes
                                .iter()
                                .find(|node| node.node_id == id)
                                .ok_or_else(|| format!("unadmitted running node {id}"))?;
                            let contract = admission.contracts[&id];
                            if node["provider_ref"] != format!("vastai:{contract}")
                                || node["selected_offer_id"].as_u64()
                                    != Some(specification.offer_id)
                                || node["runtime"]["run_id"].as_u64() != Some(admission.run_id)
                                || node["runtime"]["attempt_id"].as_u64()
                                    != Some(admission.create_reservations[&id].attempt_id)
                            {
                                return Err(format!(
                                    "node {id} does not match admitted contract/offer/bootstrap identity"
                                ));
                            }
                        }
                        Some(json!({
                            "nodes": admission.nodes,
                            "contracts": admission.contracts,
                            "create_reservations": admission.create_reservations,
                            "initial_bootstraps": admission.initial_bootstraps,
                        }))
                    }
                    _ => None,
                };
                if self
                    .provider_accounting_baseline
                    .as_ref()
                    .is_some_and(|expected| provider_accounting.as_ref() != Some(expected))
                {
                    return Err(format!(
                        "paid contract/create/bootstrap accounting changed: before={:?}, after={provider_accounting:?}",
                        self.provider_accounting_baseline
                    ));
                }
                return Ok(json!({
                    "schema_version": 2, "generation": generation,
                    "control_revision": cursor, "health_boundary": self.health_boundary.load(Ordering::Acquire),
                    "elapsed_ns": started.elapsed().as_nanos(),
                    "actors": actors, "fleet": fleet, "running_nodes": running_nodes,
                    "nodes": nodes, "provider_resources": provider_resources, "resources": resources,
                    "provider_accounting": provider_accounting,
                    "provider_hosts": provider_hosts,
                }));
            }
        })();
        #[derive(serde::Serialize)]
        struct HealthTiming<'a> {
            control_revision: &'a Value,
            namespace_commits: Option<&'a Value>,
        }
        self.record_lifecycle(
            "fresh_health_snapshot",
            started,
            &result
                .as_ref()
                .map(|health| HealthTiming {
                    control_revision: &health["control_revision"],
                    namespace_commits: health["actors"].get("namespace_commits"),
                })
                .map_err(Clone::clone),
        )?;
        result
    }
    pub fn assert_healthy(&self) -> Result<(), String> {
        self.restore_health(&[]).map(|_| ())
    }

    pub fn assert_healthy_against(&self, retained: &RetainedCaseActors) -> Result<(), String> {
        self.restore_health(std::slice::from_ref(retained))
            .map(|_| ())
    }

    fn restore_owned_baseline(&self, excluding: Option<&str>) -> Result<Value, String> {
        let retained = self
            .active_attempts
            .iter()
            .filter(|(id, _)| Some(id.as_str()) != excluding)
            .map(|(_, attempt)| RetainedCaseActors {
                attempt: Arc::clone(attempt),
            })
            .collect::<Vec<_>>();
        self.restore_health(&retained)
    }

    fn restore_health(&self, retained: &[RetainedCaseActors]) -> Result<Value, String> {
        let started = Instant::now();
        let budget = self.operation_budget().child(self.config.deadline);
        let result = (|| {
            let pending = self
                .pending_control_processes
                .lock()
                .map_err(|_| "owned execution tracking is poisoned".to_owned())?;
            if !pending.is_empty() {
                return Err(format!("owned executions lack terminal proof: {pending:?}"));
            }
            drop(pending);
            loop {
                budget.check("fixture health restoration")?;
                let (health, fixture) = thread::scope(|scope| {
                    let health =
                        scope.spawn(|| self.health_snapshot_for_nodes(&self.node_ids, &budget));
                    let fixture = scope.spawn(|| self.builtin_fixture_blobs(&budget));
                    (health.join(), fixture.join())
                });
                let mut health =
                    health.map_err(|_| "health snapshot collector panicked".to_owned())??;
                let fixture =
                    fixture.map_err(|_| "fixture ownership collector panicked".to_owned())??;
                if contains_poison(&health) {
                    return Err(format!("actor poison or worker panic detected: {health}"));
                }
                validate_fixture_census(
                    &health,
                    &self.provider_baseline,
                    &self.node_ids,
                    &self.fixture_mapping,
                    &self.stopped_nodes,
                )?;
                let fixture_actors = retained_actor_identities(&health, &fixture)?;
                if fixture_actors != self.fixture_actor_baseline {
                    return Err(format!(
                        "built-in fixture actor baseline changed: expected={:?}, observed={fixture_actors:?}",
                        self.fixture_actor_baseline
                    ));
                }
                let mut baseline = fixture_actors;
                health["builtin_fixture"] =
                    serde_json::to_value(&fixture).map_err(|error| error.to_string())?;
                for retained in retained {
                    baseline.extend(self.retained_blob_exemptions(retained, &health, &budget)?);
                }
                if let Some(reason) = pending_resource_cleanup(&health, &baseline) {
                    // This predicate advances when the append-only telemetry
                    // archive changes, not when manual-control state changes.
                    budget.wait(TELEMETRY_REFRESH_INTERVAL, &reason)?;
                    continue;
                }
                self.persist_fixture_identity(&health)?;
                return Ok(health);
            }
        })();
        if let Err(error) = &result {
            self.quarantine(format!("fixture baseline restoration failed: {error}"));
        }
        self.record_lifecycle("health_restoration", started, &result)?;
        result
    }

    pub fn retained_case_actors(&self, case: &BehaviorCase) -> Result<RetainedCaseActors, String> {
        let attempt = self
            .active_attempts
            .get(&case.id)
            .filter(|attempt| attempt.case == *case && attempt.observation.get().is_some())
            .ok_or_else(|| format!("case {} has no verified retained ownership", case.id))?;
        Ok(RetainedCaseActors {
            attempt: Arc::clone(attempt),
        })
    }

    fn retained_blob_exemptions(
        &self,
        retained: &RetainedCaseActors,
        health: &Value,
        budget: &Budget,
    ) -> Result<BTreeSet<String>, String> {
        let attempt = &retained.attempt;
        if self
            .active_attempts
            .get(&attempt.case.id)
            .is_none_or(|active| !Arc::ptr_eq(active, attempt))
        {
            return Err("retained resource exemption expired with modeled case cleanup".to_owned());
        }
        let reply = self.query_retained_blobs(attempt.cleanup_paths.clone(), budget)?;
        attempt.persist_retained_blobs(&reply)?;
        let observation = attempt
            .observation
            .get()
            .ok_or("retained case omitted verified execution evidence")?;
        let blobs = reply
            .blobs
            .iter()
            .map(|(path, blob)| (path.clone(), (blob.revision, blob.length)))
            .collect();
        crate::oracle::BehaviorOracle::verify_retained_blobs_with_budget(
            &attempt.case,
            observation,
            &blobs,
            budget,
        )
        .map_err(|error| format!("retained namespace ownership disagrees with model: {error}"))?;
        retained_actor_identities(health, &reply.blobs)
    }

    fn builtin_fixture_blobs(
        &self,
        budget: &Budget,
    ) -> Result<BTreeMap<String, myelin_control_contract::RetainedBlob>, String> {
        let reply = self.query_retained_blobs(vec![BUILTIN_FIXTURE_PATH.to_owned()], budget)?;
        let blob = reply
            .blobs
            .get(BUILTIN_FIXTURE_PATH)
            .ok_or("built-in read-only fixture is no longer a blob")?;
        if self
            .fixture_blob_baseline
            .is_some_and(|expected| expected != (blob.revision, blob.length))
        {
            return Err("built-in read-only fixture revision or length changed".to_owned());
        }
        Ok(reply.blobs)
    }

    fn query_retained_blobs(
        &self,
        paths: Vec<String>,
        budget: &Budget,
    ) -> Result<myelin_control_contract::RetainedBlobsReply, String> {
        use myelin_control_contract::{RetainedBlobsReply, RetainedBlobsRequest, SCHEMA_VERSION};
        let request_id = format!(
            "retained-{}-{}",
            self.config.seed,
            self.next_observation_generation()
        );
        let request = RetainedBlobsRequest {
            schema_version: SCHEMA_VERSION,
            request_id: request_id.clone(),
            paths,
        };
        let reply: RetainedBlobsReply = serde_json::from_value(http_json_budget(
            "POST",
            &format!("{}/api/control/contextual/retained-blobs", self.base_url),
            Some(serde_json::to_value(&request).map_err(|error| error.to_string())?),
            budget,
        )?)
        .map_err(|error| format!("typed retained ownership proof: {error}"))?;
        if reply.schema_version != SCHEMA_VERSION
            || reply.request_id != request_id
            || reply
                .blobs
                .keys()
                .any(|path| reply.non_blobs.contains_key(path))
            || reply
                .blobs
                .keys()
                .chain(reply.non_blobs.keys())
                .cloned()
                .collect::<BTreeSet<_>>()
                != request.paths.iter().cloned().collect()
        {
            return Err("retained namespace proof omitted or added an owned path".to_owned());
        }
        Ok(reply)
    }

    /// Orchestrator process id, for caller-side child-process assertions.
    pub fn orchestrator_pid(&self) -> u32 {
        self.orchestrator.id()
    }

    /// Raw fleet status from the control plane.
    pub fn fleet_snapshot(&self) -> Result<Value, String> {
        http_json_budget(
            "GET",
            &format!("{}/api/control/fleet", self.base_url),
            None,
            &self.operation_budget().child(self.config.deadline),
        )
    }

    fn ensure_orchestrator_live(&self) -> Result<(), String> {
        let mut poll = libc::pollfd {
            fd: self.orchestrator_pidfd.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        let ready = unsafe { libc::poll(&mut poll, 1, 0) };
        if ready == 0 {
            return Ok(());
        }
        Err(format!(
            "owned orchestrator incarnation exited or cannot be observed (poll={ready}, events={}): stdout={:?}; stderr={:?}",
            poll.revents,
            self.stdout
                .lock()
                .unwrap_or_else(|error| error.into_inner()),
            self.stderr
                .lock()
                .unwrap_or_else(|error| error.into_inner())
        ))
    }
    fn stop_orchestrator_process_for_cleanup(&mut self, budget: &Budget) -> Result<(), String> {
        let mut errors = Vec::new();
        let live = self.orchestrator.try_wait();
        if let Err(error) = &live {
            errors.push(format!("observe paid runner: {error}"));
        }
        if !matches!(live, Ok(Some(_))) {
            if let Err(error) = self.orchestrator.kill() {
                errors.push(format!("stop paid runner: {error}"));
            }
            let reap_budget = budget.child(
                budget
                    .remaining("reserve paid runner reap")
                    .unwrap_or_default()
                    / 4,
            );
            if let Err(error) =
                wait_for_child(&mut self.orchestrator, &reap_budget, "reap paid runner")
            {
                errors.push(error);
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    /// Stop the exact paid orchestrator incarnation before releasing the
    /// independent admission owner to perform provider deletion.
    pub fn stop_paid_orchestrator_for_cleanup(&mut self, budget: &Budget) -> Result<(), String> {
        if !matches!(self.config.provider, HarnessProvider::VastAiReal { .. }) {
            return Err("paid cleanup stop phase requires the real provider".to_owned());
        }
        let started = Instant::now();
        let result = self.stop_orchestrator_process_for_cleanup(budget);
        self.record_lifecycle("paid_orchestrator_stop", started, &result)?;
        result
    }

    pub fn teardown_with_budget(&mut self, budget: Budget) -> Result<(), String> {
        self.fixture_cleanup_budget = budget;
        self.teardown()
    }

    pub fn teardown(&mut self) -> Result<(), String> {
        if self.torn_down {
            return Ok(());
        }
        let started = Instant::now();
        let budget = self.fixture_cleanup_budget.clone();
        let previous = self.cleanup_budget.replace(budget.clone());
        if matches!(&self.config.provider, HarnessProvider::VastAiReal { .. }) {
            let admission = match &self.config.provider {
                HarnessProvider::VastAiReal { admission, .. } => admission.clone(),
                _ => unreachable!("provider checked"),
            };
            let mut errors = Vec::new();
            if let Err(error) = self.stop_orchestrator_process_for_cleanup(&budget) {
                errors.push(error);
            }
            // The local runner is no longer allowed to perform provider work
            // before the independent durable owner is released to delete.
            let admission_result: Result<(), String> = (|| {
                if admission.try_exists().map_err(|error| error.to_string())? {
                    provisioning::PaidFixtureAdmission::open(admission)?.enter_cleanup_only()?;
                }
                Ok(())
            })();
            if let Err(error) = admission_result {
                errors.push(format!("seal paid cleanup ownership: {error}"));
            }
            // Discovery, exact absence and durable accounting remain required
            // even when sealing, signaling or reaping failed.
            let resources = self.cleanup_provider_resources(&budget);
            self.torn_down =
                resources.is_ok() && self.orchestrator.try_wait().ok().flatten().is_some();
            if let Err(error) = resources {
                errors.push(error);
            }
            self.cleanup_budget = previous;
            let result = if errors.is_empty() {
                Ok(())
            } else {
                Err(errors.join("; "))
            };
            self.record_lifecycle("fixture_teardown", started, &result)?;
            return result;
        }
        let mut errors = Vec::new();
        let remaining = budget.remaining("fixture teardown").unwrap_or_default();
        // Stop requests and graceful shutdown must leave time for the independent
        // resource owner and its final absence proof. These are allocations, not
        // scaled product protocol timers.
        let stop_budget = budget.child((remaining / 4).min(self.config.deadline));
        let mut recovery_complete = false;
        let orchestrator_live = self.orchestrator.try_wait().map(|status| status.is_none());
        match orchestrator_live {
            Ok(true) => {
                let stop_nodes = if self.node_ids.is_empty() {
                    (1..=u64::from(self.config.node_count)).collect::<Vec<_>>()
                } else {
                    self.node_ids.clone()
                };
                let stop_errors = thread::scope(|scope| {
                    let requests = stop_nodes
                        .iter()
                        .map(|&node| {
                            let base_url = &self.base_url;
                            let seed = self.config.seed;
                            let request_budget = stop_budget.child(Duration::from_secs(2));
                            scope.spawn(move || {
                                http_json_budget(
                                    "POST",
                                    &format!("{base_url}/api/control/nodes/{node}/kill"),
                                    Some(json!({"command_id": format!("e2e-kill-{seed}-{node}")})),
                                    &request_budget,
                                )
                                .map(|_| ())
                                .map_err(|error| format!("stop node {node}: {error}"))
                            })
                        })
                        .collect::<Vec<_>>();
                    requests
                        .into_iter()
                        .filter_map(|request| match request.join() {
                            Ok(Ok(())) => None,
                            Ok(Err(error)) => Some(error),
                            Err(_) => Some("node stop collector panicked".to_owned()),
                        })
                        .collect::<Vec<_>>()
                });
                errors.extend(stop_errors);
                let stopped = (|| {
                    loop {
                        let cursor = self.control_revision(&stop_budget)?;
                        stop_budget
                            .check("all managed nodes terminal before orchestrator shutdown")?;
                        if self
                            .orchestrator
                            .try_wait()
                            .map_err(|error| format!("observe stop owner: {error}"))?
                            .is_some()
                        {
                            return Err("orchestrator exited before managed-node terminal census"
                                .to_owned());
                        }
                        let status = http_json_budget(
                            "GET",
                            &format!("{}/api/control/status", self.base_url),
                            None,
                            &stop_budget,
                        )?;
                        let nodes = status
                            .pointer("/Status/nodes")
                            .and_then(Value::as_array)
                            .ok_or_else(|| format!("shutdown status omitted nodes: {status}"))?;
                        if nodes.iter().all(|node| {
                            matches!(node["phase"].as_str(), Some("stopped" | "orphan"))
                        }) {
                            return Ok(());
                        }
                        self.wait_for_control_change(
                            &cursor,
                            &stop_budget,
                            &format!("managed-node terminal census: {status}"),
                        )?;
                    }
                })();
                recovery_complete = stopped.is_ok();
                if let Err(error) = stopped {
                    errors.push(error);
                }
            }
            Ok(false) => {}
            Err(error) => errors.push(format!("observe orchestrator during teardown: {error}")),
        }

        let mut graceful_shutdown = false;
        if self.orchestrator.try_wait().ok().flatten().is_none() {
            match i32::try_from(self.orchestrator.id()) {
                Ok(pid) => {
                    if unsafe { libc::kill(pid, libc::SIGTERM) } == 0 {
                        let grace = budget.child(
                            (budget
                                .remaining("orchestrator graceful shutdown")
                                .unwrap_or_default()
                                / 4)
                            .min(Duration::from_secs(10)),
                        );
                        match wait_for_child(
                            &mut self.orchestrator,
                            &grace,
                            "graceful orchestrator reap",
                        ) {
                            Ok(()) => graceful_shutdown = true,
                            Err(error) => errors.push(error),
                        }
                    } else {
                        errors.push(format!(
                            "signal orchestrator shutdown: {}",
                            std::io::Error::last_os_error()
                        ));
                    }
                }
                Err(_) => errors.push("orchestrator pid exceeds platform range".to_owned()),
            }
        }
        if self.orchestrator.try_wait().ok().flatten().is_none() {
            if let Err(error) = self.orchestrator.kill() {
                errors.push(format!("force stop orchestrator: {error}"));
            }
            if let Err(error) =
                wait_for_child(&mut self.orchestrator, &budget, "forced orchestrator reap")
            {
                errors.push(error);
            }
        }
        if graceful_shutdown {
            let checkpoint_exists = self.state_dir.join("cluster.json").exists();
            if recovery_complete && checkpoint_exists {
                errors.push("graceful shutdown retained an empty recovery checkpoint".to_owned());
            } else if !recovery_complete && !checkpoint_exists {
                errors.push("graceful shutdown discarded an active recovery checkpoint".to_owned());
            }
        }

        // Never short-circuit this owner because a stop, reap, or discovery failed.
        let resources = self.cleanup_provider_resources(&budget);
        self.torn_down = resources.is_ok() && self.orchestrator.try_wait().ok().flatten().is_some();
        if let Err(error) = resources {
            errors.push(error);
        }
        self.cleanup_budget = previous;
        let result = if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        };
        self.record_lifecycle("fixture_teardown", started, &result)?;
        result
    }

    fn cleanup_provider_resources(&self, budget: &Budget) -> Result<(), String> {
        match &self.config.provider {
            HarnessProvider::LocalMock => {
                let removal = remove_containers(&self.container_prefix, budget);
                let absence =
                    list_containers(&self.container_prefix, budget).and_then(|remaining| {
                        if remaining.is_empty() {
                            Ok(())
                        } else {
                            Err(format!(
                                "harness containers remained after teardown: {remaining:?}"
                            ))
                        }
                    });
                combine_cleanup_results([removal, absence])
            }
            HarnessProvider::StaticSsh { manifest, .. } => {
                // Read ownership, not a pre-deletion Docker query: one failed
                // inspect must not prevent attempts on any other manifest entry.
                let manifest_value: Value = serde_json::from_slice(
                    &fs::read(manifest)
                        .map_err(|error| format!("read raw ownership manifest: {error}"))?,
                )
                .map_err(|error| format!("decode raw ownership manifest: {error}"))?;
                let containers = manifest_value["nodes"]
                    .as_array()
                    .ok_or_else(|| "raw ownership manifest omitted nodes".to_owned())?
                    .iter()
                    .map(|node| {
                        node["container"]
                            .as_str()
                            .map(str::to_owned)
                            .ok_or_else(|| {
                                "raw ownership manifest node omitted container".to_owned()
                            })
                    })
                    .collect::<Result<BTreeSet<_>, _>>()?;
                let removals = thread::scope(|scope| {
                    let requests = containers
                        .iter()
                        .map(|container| scope.spawn(move || remove_container(container, budget)))
                        .collect::<Vec<_>>();
                    requests
                        .into_iter()
                        .map(|request| {
                            request.join().unwrap_or_else(|_| {
                                Err("raw container removal owner panicked".to_owned())
                            })
                        })
                        .collect::<Vec<_>>()
                });
                let absence =
                    raw_fleet::read_manifest_containers(manifest, budget).and_then(|remaining| {
                        if remaining.is_empty() {
                            Ok(())
                        } else {
                            Err(format!(
                                "raw fleet containers remained after teardown: {remaining:?}"
                            ))
                        }
                    });
                combine_cleanup_results(removals.into_iter().chain([absence]))
            }
            HarnessProvider::VastAiReal {
                admission: admission_path,
                ..
            } => {
                let admission_absent = !admission_path
                    .try_exists()
                    .map_err(|error| format!("inspect paid admission ownership: {error}"))?;
                if admission_absent
                    && !self.config.provision
                    && self.config.selected_offer_ids.is_empty()
                    && self.node_ids.is_empty()
                    && self.provider_baseline.is_empty()
                {
                    let proof = json!({
                        "schema_version": 1, "ownership_unstarted": true,
                        "absent_contract_ids": [], "remaining_contract_ids": [],
                        "discovered_contract_labels": {},
                        "observed_monotonic_ns": self.lifecycle_started.elapsed().as_nanos(),
                    });
                    fs::write(
                        self.config.artifacts.join("provider-absence.json"),
                        serde_json::to_vec(&proof).map_err(|error| {
                            format!("encode unstarted ownership evidence: {error}")
                        })?,
                    )
                    .map_err(|error| format!("persist unstarted ownership evidence: {error}"))?;
                    return Ok(());
                }
                let owner = provisioning::PaidFixtureAdmission::open(admission_path)?;
                let admission = owner.snapshot()?;
                if admission.cleanup.is_none() {
                    return Err("paid cleanup requires the durable independent cleanup owner; recover ownership first".to_owned());
                }
                owner.enter_cleanup_only()?;
                loop {
                    budget.check("independent owner exact provider absence")?;
                    let admission = owner.snapshot()?;
                    let cleanup = admission
                        .cleanup
                        .as_ref()
                        .ok_or("paid cleanup ownership disappeared")?;
                    if cleanup.spending_stopped && !cleanup.complete {
                        return Err(format!(
                            "paid resources absent but accounting evidence rejected: {:?}",
                            admission.accounting_errors
                        ));
                    }
                    if cleanup.complete {
                        let proof = json!({
                            "schema_version": 1, "cleanup_owner": cleanup,
                            "absent_contract_ids": admission.known_contract_ids(),
                            "remaining_contract_ids": [],
                            "observed_monotonic_ns": self.lifecycle_started.elapsed().as_nanos(),
                        });
                        fs::write(
                            self.config.artifacts.join("provider-absence.json"),
                            serde_json::to_vec(&proof).map_err(|error| error.to_string())?,
                        )
                        .map_err(|error| format!("persist owner absence evidence: {error}"))?;
                        return Ok(());
                    }
                    budget.wait(POLL_INTERVAL, "independent owner exact provider absence")?;
                }
            }
        }
    }
}

impl Drop for ClusterHarness {
    fn drop(&mut self) {
        // A retained remote fixture still relinquishes its local process owner.
        // Failed cleanup reuses the original reserve; Drop cannot extend it.
        if self.torn_down {
            let _ = self.orchestrator.kill();
            let _ = wait_for_child(
                &mut self.orchestrator,
                &self.fixture_cleanup_budget,
                "retained orchestrator reap",
            );
        } else {
            let _ = self.teardown();
        }
    }
}

fn validate_control_revision(reply: &Value) -> Result<(), String> {
    if reply["schema_version"] != 1
        || reply.pointer("/payload/type").and_then(Value::as_str) != Some("control_revision")
        || reply
            .pointer("/payload/generation")
            .and_then(Value::as_str)
            .is_none()
        || reply
            .pointer("/payload/revision")
            .and_then(Value::as_u64)
            .is_none()
    {
        return Err(format!("invalid versioned control revision: {reply}"));
    }
    Ok(())
}

fn combine_cleanup_results(
    results: impl IntoIterator<Item = Result<(), String>>,
) -> Result<(), String> {
    let errors = results
        .into_iter()
        .filter_map(Result::err)
        .collect::<Vec<_>>();
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

fn wait_for_child(child: &mut Child, budget: &Budget, predicate: &str) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    let pidfd = {
        use std::os::fd::{FromRawFd, OwnedFd};
        let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0) } as i32;
        if fd >= 0 {
            Some(unsafe { OwnedFd::from_raw_fd(fd) })
        } else {
            None
        }
    };
    loop {
        if child
            .try_wait()
            .map_err(|error| format!("{predicate}: {error}"))?
            .is_some()
        {
            return Ok(());
        }
        let remaining = budget.remaining(predicate)?;
        #[cfg(target_os = "linux")]
        if let Some(fd) = &pidfd {
            use std::os::fd::AsRawFd;
            let mut poll = libc::pollfd {
                fd: fd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let milliseconds = remaining.min(POLL_INTERVAL).as_millis().max(1) as i32;
            if unsafe { libc::poll(&mut poll, 1, milliseconds) } < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted {
                    return Err(format!(
                        "{predicate}: child-exit notification failed: {error}"
                    ));
                }
            }
            continue;
        }
        budget.wait(remaining.min(POLL_INTERVAL), predicate)?;
    }
}

fn resource_runtime_identity(snapshot: &Value) -> Value {
    json!({
        "generation": snapshot["generation"],
        "iroh_node_id": snapshot["iroh_node_id"],
        "artifact_digest": snapshot["artifact_digest"],
        "deployment_generation": snapshot["deployment_generation"],
    })
}

fn validate_fixture_census(
    health: &Value,
    expected_resources: &BTreeSet<String>,
    expected_nodes: &[u64],
    expected_mapping: &BTreeMap<u64, Value>,
    stopped_nodes: &BTreeSet<u64>,
) -> Result<(), String> {
    let resources: BTreeSet<String> = serde_json::from_value(
        health
            .get("provider_resources")
            .cloned()
            .ok_or_else(|| "health snapshot omitted provider resources".to_owned())?,
    )
    .map_err(|error| format!("invalid provider resource census: {error}"))?;
    let nodes: Vec<u64> = serde_json::from_value(
        health
            .get("running_nodes")
            .cloned()
            .ok_or_else(|| "health snapshot omitted running nodes".to_owned())?,
    )
    .map_err(|error| format!("invalid running node census: {error}"))?;
    let expected_nodes: BTreeSet<u64> = expected_nodes.iter().copied().collect();
    let observed_nodes: BTreeSet<u64> = nodes.iter().copied().collect();
    if &resources != expected_resources
        || observed_nodes != expected_nodes
        || nodes.len() != observed_nodes.len()
        || resources.len() != nodes.len()
    {
        return Err(format!(
            "fixture identity changed: expected resources={expected_resources:?}, \
             nodes={expected_nodes:?}; observed resources={resources:?}, nodes={nodes:?}"
        ));
    }
    let mapping = fixture_node_mapping(health, &nodes, stopped_nodes)?;
    for (node, identity) in &mapping {
        if expected_mapping.get(node) != Some(identity) {
            return Err(format!(
                "node {node} contract/host/runtime association changed: expected {:?}, observed {identity}",
                expected_mapping.get(node)
            ));
        }
    }
    let mapped_resources = mapping
        .values()
        .filter_map(|identity| {
            identity["provider_host"]["container_id"]
                .as_str()
                .or_else(|| identity["provider_ref"].as_str())
        })
        .map(|resource| {
            resource
                .strip_prefix("static-ssh:")
                .unwrap_or(resource)
                .to_owned()
        })
        .collect::<BTreeSet<_>>();
    if mapped_resources != resources {
        return Err(format!(
            "fleet mapping does not own exact provider resources: {mapped_resources:?} != {resources:?}"
        ));
    }
    Ok(())
}

fn fixture_node_mapping(
    health: &Value,
    running: &[u64],
    stopped: &BTreeSet<u64>,
) -> Result<BTreeMap<u64, Value>, String> {
    use myelin_control_contract::{NodePhase, NodeStatus};
    let nodes: Vec<NodeStatus> =
        serde_json::from_value(health["fleet"]["FleetStatus"]["nodes"].clone())
            .map_err(|error| format!("invalid typed fleet node census: {error}"))?;
    let mut seen = BTreeSet::new();
    let mut mapping = BTreeMap::new();
    for node in nodes {
        let id = node.logical_node_id;
        if !seen.insert(id) {
            return Err(format!("duplicate fleet node {id}"));
        }
        if stopped.contains(&id) {
            if node.phase != NodePhase::Stopped {
                return Err(format!(
                    "removed node {id} is not stopped: {:?}",
                    node.phase
                ));
            }
        } else if !running.contains(&id) || node.phase != NodePhase::Running {
            return Err(format!(
                "unexpected node or replacement/bootstrap intent for node {id}: {:?}",
                node.phase
            ));
        } else {
            let provider_ref = node
                .provider_ref
                .filter(|value| !value.is_empty())
                .ok_or_else(|| format!("node {id} omitted provider resource"))?;
            let runtime = node
                .runtime
                .ok_or_else(|| format!("node {id} omitted live runtime identity"))?;
            mapping.insert(
                id,
                json!({
                    "provider_ref": provider_ref, "selected_offer_id": node.selected_offer_id,
                    "provider_host": health["provider_hosts"][id.to_string()].clone(),
                    "run_id": runtime.run_id, "attempt_id": runtime.attempt_id,
                    "endpoint": runtime.endpoint, "node_actor": runtime.node_actor,
                    "swim_node_id": runtime.swim_node_id, "readiness_id": runtime.readiness_id,
                }),
            );
        }
    }
    if seen
        != running
            .iter()
            .copied()
            .chain(stopped.iter().copied())
            .collect()
    {
        return Err("fleet omitted an expected running or stopped node".to_owned());
    }
    Ok(mapping)
}

fn retained_actor_identities(
    health: &Value,
    blobs: &BTreeMap<String, myelin_control_contract::RetainedBlob>,
) -> Result<BTreeSet<String>, String> {
    let actors = health
        .pointer("/resources/active_actors")
        .and_then(Value::as_array)
        .ok_or("retained ownership requires a live actor census")?;
    let mut identities = BTreeSet::new();
    for blob in blobs.values() {
        for (address, expected_type) in
            std::iter::once((&blob.source, "data_plane::source::FileBlobSourceActor")).chain(
                blob.binding
                    .as_ref()
                    .map(|binding| (binding, "data_plane::host::HostBlobBindingActor")),
            )
        {
            if address.len() != 64 || !address.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return Err("retained source proof omitted a complete actor identity".to_owned());
            }
            let mut matches = actors.iter().filter(|actor| {
                actor["identity"]
                    .as_str()
                    .and_then(|identity| identity.rsplit_once('/'))
                    .is_some_and(|(_, observed)| observed == address)
            });
            let actor = matches
                .next()
                .ok_or_else(|| format!("retained {expected_type} {address} is absent"))?;
            if matches.next().is_some() || actor["type"] != expected_type {
                return Err(format!(
                    "retained actor {address} is duplicated or has the wrong type"
                ));
            }
            identities.insert(
                actor["identity"]
                    .as_str()
                    .ok_or("retained actor identity")?
                    .to_owned(),
            );
        }
    }
    Ok(identities)
}

#[cfg(test)]
mod fixture_census_tests {
    use super::*;

    #[test]
    fn expired_child_wait_preserves_owner_for_reserved_cleanup() {
        let owner = Budget::new(Duration::from_secs(2));
        let cleanup = owner.child(Duration::from_secs(2));
        let execution = owner.child(Duration::ZERO);
        let mut child = Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("start parked child");
        let expired = wait_for_child(&mut child, &execution, "withheld process exit");
        let killed = child.kill();
        let reaped = wait_for_child(&mut child, &cleanup, "reserved process reap");
        assert!(expired.is_err());
        assert!(killed.is_ok());
        assert!(reaped.is_ok());
        assert!(child.try_wait().expect("observe reaped child").is_some());
    }

    #[test]
    fn control_reconciliation_rejects_unversioned_and_incomplete_cursors() {
        assert!(validate_control_revision(&json!({
            "schema_version": 1, "payload": {"type": "control_revision", "generation": "boot-a", "revision": 7},
        })).is_ok());
        assert!(validate_control_revision(&json!({
            "schema_version": 2, "payload": {"type": "control_revision", "generation": "boot-a", "revision": 7},
        })).is_err());
        assert!(
            validate_control_revision(&json!({
                "schema_version": 1, "payload": {"type": "control_revision", "revision": 7},
            }))
            .is_err()
        );
    }

    fn fleet_node(id: u64, resource: u64, phase: &str) -> Value {
        json!({
            "logical_node_id": id, "provider_ref": format!("vastai:{resource}"),
            "selected_offer_id": id+1000, "phase": phase, "last_error": null, "last_seen_unix_ms": 1,
            "runtime": {"run_id": 9, "attempt_id": 1, "endpoint": format!("host-{id}"),
                "node_actor": vec![id as u8; 32], "swim_node_id": vec![id as u8; 32],
                "stage_index": 0, "readiness_id": id},
        })
    }

    fn original_fleet() -> Value {
        json!({"provider_resources": ["vastai:101", "vastai:102"],
            "running_nodes": [1, 3],
            "fleet": {"FleetStatus": {"nodes": [fleet_node(1, 101, "running"), fleet_node(3, 102, "running")]}}})
    }

    #[test]
    fn docker_census_resolves_names_without_accepting_swaps_or_replacements() {
        let mut original = original_fleet();
        let first = "a".repeat(64);
        let second = "b".repeat(64);
        original["provider_resources"] = json!([first, second]);
        original["fleet"]["FleetStatus"]["nodes"][0]["provider_ref"] = json!("node-one");
        original["fleet"]["FleetStatus"]["nodes"][1]["provider_ref"] = json!("node-three");
        original["provider_hosts"] = json!({
            "1": {"container": "node-one", "container_id": first},
            "3": {"container": "node-three", "container_id": second},
        });
        let resources = BTreeSet::from([first.clone(), second.clone()]);
        let mapping = fixture_node_mapping(&original, &[1, 3], &BTreeSet::new()).unwrap();
        let validate = |health: &Value| {
            validate_fixture_census(health, &resources, &[1, 3], &mapping, &BTreeSet::new())
        };
        assert!(validate(&original).is_ok());
        let mut swapped = original.clone();
        swapped["provider_hosts"]["1"]["container_id"] = json!(second);
        swapped["provider_hosts"]["3"]["container_id"] = json!(first);
        assert!(validate(&swapped).is_err());
        let replacement = "c".repeat(64);
        original["provider_hosts"]["1"]["container_id"] = json!(replacement);
        original["provider_resources"] = json!([replacement, second]);
        assert!(validate(&original).is_err());
    }

    #[test]
    fn retained_fixture_rejects_swapped_mapping_and_new_bootstrap_identity() {
        let original = original_fleet();
        let resources = BTreeSet::from(["vastai:101".to_owned(), "vastai:102".to_owned()]);
        let mapping = fixture_node_mapping(&original, &[1, 3], &BTreeSet::new()).unwrap();
        let validate = |health: &Value| {
            validate_fixture_census(health, &resources, &[1, 3], &mapping, &BTreeSet::new())
        };
        assert!(validate(&original).is_ok());
        let mut swapped = original.clone();
        swapped["fleet"]["FleetStatus"]["nodes"][0]["provider_ref"] = json!("vastai:102");
        swapped["fleet"]["FleetStatus"]["nodes"][1]["provider_ref"] = json!("vastai:101");
        assert!(validate(&swapped).is_err());
        let mut bootstrap = original.clone();
        bootstrap["fleet"]["FleetStatus"]["nodes"][0]["runtime"]["attempt_id"] = json!(2);
        assert!(validate(&bootstrap).is_err());
        let mut replacement = original;
        replacement["fleet"]["FleetStatus"]["nodes"]
            .as_array_mut()
            .unwrap()
            .push(fleet_node(4, 103, "bootstrapping"));
        assert!(validate(&replacement).is_err());
    }

    #[test]
    fn destructive_census_requires_exact_target_stopped_and_survivors_unchanged() {
        let original = original_fleet();
        let mapping = fixture_node_mapping(&original, &[1, 3], &BTreeSet::new()).unwrap();
        let resources = BTreeSet::from(["vastai:102".to_owned()]);
        let stopped = BTreeSet::from([1]);
        let validate =
            |health: &Value| validate_fixture_census(health, &resources, &[3], &mapping, &stopped);
        let mut removed = original;
        removed["provider_resources"] = json!(["vastai:102"]);
        removed["running_nodes"] = json!([3]);
        removed["fleet"]["FleetStatus"]["nodes"][0]["phase"] = json!("stopped");
        assert!(validate(&removed).is_ok());
        let mut wrong = removed.clone();
        wrong["provider_resources"] = json!(["vastai:101"]);
        assert!(validate(&wrong).is_err());
        let mut orphan = removed.clone();
        orphan["fleet"]["FleetStatus"]["nodes"][0]["phase"] = json!("orphan");
        assert!(validate(&orphan).is_err());
        let mut changed_survivor = removed;
        changed_survivor["fleet"]["FleetStatus"]["nodes"][1]["runtime"]["endpoint"] =
            json!("different-host");
        assert!(validate(&changed_survivor).is_err());
    }

    #[test]
    fn retained_blob_exempts_only_exact_owned_source_and_binding() {
        let source = "a".repeat(64);
        let binding = "b".repeat(64);
        let leaked = "c".repeat(64);
        let blobs = BTreeMap::from([(
            "/cases/owned/blob".to_owned(),
            myelin_control_contract::RetainedBlob {
                source: source.clone(),
                binding: Some(binding.clone()),
                source_node: [0; 32],
                length: 3,
                revision: 8,
            },
        )]);
        let mut reply =
            crate::resources::resource_deadline_tests::health_reply("retained", 1, 1, 0);
        reply["observation"]["event"]["resources"]["actors"] = json!([
            {"address": source, "actor_type": "data_plane::source::FileBlobSourceActor", "worker_id": 0, "mailbox_depth": 0, "poisoned": false, "stopping": false},
            {"address": binding, "actor_type": "data_plane::host::HostBlobBindingActor", "worker_id": 0, "mailbox_depth": 0, "poisoned": false, "stopping": false}
        ]);
        let mut census = TelemetryResourceCensus::default();
        census
            .record_resource_snapshot("retained", 1, &reply["observation"]["event"]["resources"])
            .unwrap();
        let mut health = json!({"schema_version": 2, "generation": 2, "health_boundary": 1,
            "running_nodes": [1], "nodes": [reply], "resources": census.snapshot()});
        let exempt = retained_actor_identities(&health, &blobs).unwrap();
        assert_eq!(pending_resource_cleanup(&health, &exempt), None);
        assert!(pending_resource_cleanup(&health, &BTreeSet::new()).is_some());
        health["resources"]["active_actors"].as_array_mut().unwrap().push(json!({
            "identity": format!("1#2/{leaked}"), "type": "data_plane::source::FileBlobSourceActor",
        }));
        let exempt = retained_actor_identities(&health, &blobs).unwrap();
        assert!(pending_resource_cleanup(&health, &exempt).is_some());
    }
}
