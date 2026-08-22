//! Job-runner deployment through the existing Myelin cluster reconciler.
//!
//! SSH remains a provider bootstrap transport owned by `VastAiProvisioningPlugin`;
//! the job workspace, commands, and outputs still travel through the job actors over
//! the iroh actor plane.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use iroh_driver::MVP_IROH_ENDPOINT_ADDR_MASK_ENV;
use provisioning::{
    BootSpec, ClusterShape, DesiredNodeShape, LogicalNodeId, NodeAttemptId, NodeGroupId,
    ProviderKind, RetryPolicy, RoleId, RunId, RunNodeGroupSpec, SwactorId, SwarmJoinTemplate,
};
use swactor::actor::ActorInterface;
use swactor::runtime::{Ctx, ExternalSender};
use swactor_engine::{ActorCompletion, EngineHandle};
use swactor_job_runner::{Job, JobDone};
use swactor_vastai::SelectionPolicy;

use crate::job_deploy::{self, JobRunStateMachine, NodeIdentity};
use crate::orchestration::app::{
    derive_ssh_public_key, ensure_vastai_account_ssh_key, resolve_vastai_ssh_identity,
    ssh_public_key_fingerprint,
};
use crate::orchestration::cluster_reconciler::{ProvisionedClusterGuard, ReconcilerNodeBinding};
use crate::orchestration::provider_adapters::relay::{
    MYELIN_IROH_RELAY_MODE_ENV, MYELIN_IROH_RELAY_URL_ENV, SWACTOR_IROH_RELAY_URL_ENV,
};
use crate::orchestration::provider_adapters::vastai::{
    SshCommandBootstrapLauncher, ToolsVastAiLeaseClient, VastAiProvisioningConfig,
    VastAiProvisioningPlugin,
};
use crate::provisioning::{
    NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink, ProvisionPlugin,
};

const DEFAULT_NODE_ID: u64 = 2;
const DEFAULT_STAGE_INDEX: u32 = 0;
const DEFAULT_REMOTE_WORKER_BIN: &str = "/usr/local/bin/myelin-job-worker";
const DEFAULT_WORKER_WORKDIR: &str = "/root/workspace";
const DEFAULT_ENDPOINT_ADDR_MASK: &str = "relay-only";
const DEFAULT_LABEL_PREFIX: &str = "myelin-job";
const POLL: Duration = Duration::from_millis(100);
const DEFAULT_PROVISION_TIMEOUT: Duration = Duration::from_secs(60 * 30);
const RUNTIME_CONVERGENCE_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub(crate) struct VastAiJobOptions {
    pub api_key: Option<String>,
    pub image: Option<String>,
    pub ssh_identity: Option<PathBuf>,
    pub remote_worker_bin: String,
    pub worker_workdir: String,
    pub run_id: u64,
    pub node_id: u64,
    pub label_prefix: String,
    pub disk_gb: u32,
    pub ssh_user: String,
    pub confirm_lease: bool,
    pub onstart: Option<String>,
    pub relay_mode: Option<String>,
    pub relay_url: Option<String>,
    pub endpoint_addr_mask: String,
    pub gpu_name: Option<String>,
    pub min_gpu_ram_mb: Option<u64>,
    pub min_down_mbps: Option<f64>,
    pub min_up_mbps: Option<f64>,
    pub max_dph_total: Option<f64>,
    pub min_reliability: Option<f64>,
    pub require_verified: Option<bool>,
    pub blacklist_hosts: Vec<u64>,
    pub poll_interval: Option<Duration>,
    pub provision_timeout: Duration,
}

impl Default for VastAiJobOptions {
    fn default() -> Self {
        Self {
            api_key: None,
            image: None,
            ssh_identity: None,
            remote_worker_bin: DEFAULT_REMOTE_WORKER_BIN.to_owned(),
            worker_workdir: DEFAULT_WORKER_WORKDIR.to_owned(),
            run_id: default_run_id(),
            node_id: DEFAULT_NODE_ID,
            label_prefix: DEFAULT_LABEL_PREFIX.to_owned(),
            disk_gb: 80,
            ssh_user: "root".to_owned(),
            confirm_lease: false,
            onstart: None,
            relay_mode: Some("default".to_owned()),
            relay_url: None,
            endpoint_addr_mask: DEFAULT_ENDPOINT_ADDR_MASK.to_owned(),
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_down_mbps: None,
            min_up_mbps: None,
            max_dph_total: None,
            min_reliability: None,
            require_verified: None,
            blacklist_hosts: Vec::new(),
            poll_interval: None,
            provision_timeout: DEFAULT_PROVISION_TIMEOUT,
        }
    }
}

pub(crate) fn run_vastai_job(
    job: Job,
    landing: PathBuf,
    options: VastAiJobOptions,
) -> Result<JobDone, String> {
    let api_key = required_option(
        options.api_key.as_deref(),
        "VAST_API_KEY, MYELIN_VASTAI_API_KEY, VASTAI_API_KEY, or --vastai-api-key",
    )?
    .to_owned();
    let image = required_option(
        options.image.as_deref(),
        "MYELIN_NODE_IMAGE or --image for the job-worker image",
    )?
    .to_owned();
    validate_non_empty("remote worker binary", &options.remote_worker_bin)?;
    validate_non_empty("worker workdir", &options.worker_workdir)?;
    validate_non_empty("endpoint address mask", &options.endpoint_addr_mask)?;

    let session = job_deploy::start_orchestrator(landing)?;
    let orch_json = session.identity_json()?;
    println!("JOB_ORCH_IDENTITY {orch_json}");
    let _ = std::io::Write::flush(&mut std::io::stdout());
    eprintln!(
        "job-reconcile: provisioning VastAI node through reconciler image={image} node_id={} run_id={}",
        options.node_id, options.run_id
    );

    let (sink, observations) = observation_channel();
    let runtime = session.runtime();
    let engine = session.engine_handle();
    let provisioner =
        build_vastai_provisioner(&api_key, &options, runtime.clone(), engine.clone())?;
    let spec = job_node_spec(&options, image, &orch_json)?;
    let cluster = build_cluster(
        &options,
        spec,
        provisioner,
        engine.clone(),
        runtime.clone(),
        sink,
    )?;
    let completion = ActorCompletion::new();
    runtime
        .spawn(ReconciledJobActor {
            cluster,
            observations,
            session: Some(session),
            job: Some(job),
            phase: ReconciledJobPhase::Provisioning {
                deadline: Instant::now() + options.provision_timeout,
            },
            node_id: options.node_id,
            engine,
            sender: runtime.create_sender(),
            completion: completion.clone(),
        })
        .map_err(|error| format!("spawn reconciled job actor: {error}"))?;
    completion.wait()
}

#[derive(Clone)]
struct ReconciledJobTick;

enum ReconciledJobPhase {
    Provisioning {
        deadline: Instant,
    },
    Converging {
        deadline: Instant,
        worker: NodeIdentity,
    },
    Running(Box<JobRunStateMachine>),
    Stopping {
        result: Result<JobDone, String>,
    },
    Finished,
}

struct ReconciledJobActor {
    cluster: ProvisionedClusterGuard,
    observations: mpsc::Receiver<PluginObservation>,
    session: Option<job_deploy::JobOrchestratorSession>,
    job: Option<Job>,
    phase: ReconciledJobPhase,
    node_id: u64,
    engine: EngineHandle,
    sender: ExternalSender,
    completion: ActorCompletion<Result<JobDone, String>>,
}

impl ReconciledJobActor {
    fn schedule(&self, ctx: &Ctx) {
        self.engine.send_after(
            POLL,
            self.sender.clone(),
            ctx.self_addr(),
            ReconciledJobTick,
        );
    }

    fn begin_stop(&mut self, result: Result<JobDone, String>) {
        let result = match self.cluster.begin_shutdown() {
            Ok(()) => result,
            Err(cleanup) => merge_cleanup(result, cleanup),
        };
        self.phase = ReconciledJobPhase::Stopping { result };
    }

    fn complete(
        &mut self,
        ctx: &Ctx,
        result: Result<JobDone, String>,
        cleanup: Result<(), String>,
    ) {
        let result = match cleanup {
            Ok(()) => result,
            Err(cleanup) => merge_cleanup(result, cleanup),
        };
        assert!(
            self.completion.complete(result).is_ok(),
            "reconciled job completed twice"
        );
        self.phase = ReconciledJobPhase::Finished;
        ctx.stop_self();
    }
}

impl ActorInterface for ReconciledJobActor {
    type Incoming = ReconciledJobTick;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), ReconciledJobTick);
    }

    fn handle(&mut self, ctx: &Ctx, _message: Self::Incoming) {
        if let Err(error) = self.cluster.poll(SystemTime::now()) {
            let cleanup = self.cluster.finish_shutdown();
            self.complete(ctx, Err(format!("job reconciler poll: {error}")), cleanup);
            return;
        }

        let phase = std::mem::replace(&mut self.phase, ReconciledJobPhase::Finished);
        match phase {
            ReconciledJobPhase::Provisioning { deadline } => {
                let mut worker = None;
                if let Err(error) = drain_observations(&self.observations, &mut worker) {
                    self.begin_stop(Err(error));
                } else if let Some(worker) = worker {
                    eprintln!("job-reconcile: worker identity observed through reconciler stdout");
                    let result = job_deploy::parse_actor(&worker.actor_hex).and_then(|actor| {
                        let attempt = self.cluster.current_attempt(self.node_id).ok_or_else(|| {
                            format!(
                                "reconciler has no active attempt for node {}",
                                self.node_id
                            )
                        })?;
                        self.cluster
                            .observe_runtime_ready(
                                self.node_id,
                                NodeAttemptId(attempt.0),
                                SwactorId(format!("{actor:?}")),
                                SystemTime::now(),
                            )
                            .then_some(())
                            .ok_or_else(|| {
                                format!(
                                    "reconciler rejected runtime-ready observation for node {} attempt {}",
                                    self.node_id, attempt.0
                                )
                            })
                    });
                    match result {
                        Ok(()) => {
                            self.phase = ReconciledJobPhase::Converging {
                                deadline: Instant::now() + RUNTIME_CONVERGENCE_TIMEOUT,
                                worker,
                            };
                        }
                        Err(error) => self.begin_stop(Err(error)),
                    }
                } else if Instant::now() >= deadline {
                    self.begin_stop(Err(
                        "timed out waiting for reconciled job worker identity".to_string()
                    ));
                } else {
                    self.phase = ReconciledJobPhase::Provisioning { deadline };
                }
            }
            ReconciledJobPhase::Converging { deadline, worker } => {
                let mut ignored = None;
                if let Err(error) = drain_observations(&self.observations, &mut ignored) {
                    self.begin_stop(Err(error));
                } else if self.cluster.is_converged() {
                    eprintln!("job-reconcile: reconciler accepted runtime-ready worker");
                    let machine = self
                        .session
                        .take()
                        .zip(self.job.take())
                        .ok_or_else(|| "reconciled job lost session state".to_owned())
                        .and_then(|(session, job)| JobRunStateMachine::new(session, job, worker));
                    match machine {
                        Ok(mut machine) => {
                            machine.start(Instant::now());
                            self.phase = ReconciledJobPhase::Running(Box::new(machine));
                        }
                        Err(error) => self.begin_stop(Err(error)),
                    }
                } else if Instant::now() >= deadline {
                    self.begin_stop(Err(format!(
                        "timed out after {RUNTIME_CONVERGENCE_TIMEOUT:?} waiting for reconciler convergence"
                    )));
                } else {
                    self.phase = ReconciledJobPhase::Converging { deadline, worker };
                }
            }
            ReconciledJobPhase::Running(mut machine) => {
                let mut ignored = None;
                if let Err(error) = drain_observations(&self.observations, &mut ignored) {
                    self.begin_stop(Err(error));
                } else if let Some(result) = machine.advance(Instant::now()) {
                    self.begin_stop(result);
                } else {
                    self.phase = ReconciledJobPhase::Running(machine);
                }
            }
            ReconciledJobPhase::Stopping { result } => {
                if self.cluster.is_stopped() {
                    let cleanup = self.cluster.finish_shutdown();
                    self.complete(ctx, result, cleanup);
                    return;
                }
                self.phase = ReconciledJobPhase::Stopping { result };
            }
            ReconciledJobPhase::Finished => {
                ctx.stop_self();
                return;
            }
        }
        self.schedule(ctx);
    }
}

fn merge_cleanup(result: Result<JobDone, String>, cleanup: String) -> Result<JobDone, String> {
    match result {
        Ok(_) => Err(format!(
            "job completed but reconciler cleanup failed: {cleanup}"
        )),
        Err(error) => Err(format!("{error}; reconciler cleanup failed: {cleanup}")),
    }
}

fn build_vastai_provisioner(
    api_key: &str,
    options: &VastAiJobOptions,
    runtime: swactor::runtime::Runtime,
    engine: swactor_engine::EngineHandle,
) -> Result<Box<dyn ProvisionPlugin>, String> {
    let identity = resolve_vastai_ssh_identity(options.ssh_identity.clone())?;
    if !identity.is_file() {
        return Err(format!(
            "missing VastAI SSH identity {}; set --vastai-ssh-identity or MYELIN_VASTAI_SSH_IDENTITY",
            identity.display()
        ));
    }
    let public_key = derive_ssh_public_key(&identity)?;
    ensure_vastai_account_ssh_key(api_key, &public_key)?;
    eprintln!(
        "job-reconcile: VastAI SSH identity {} fingerprint {} registered",
        identity.display(),
        ssh_public_key_fingerprint(&public_key)
    );

    let mut config = VastAiProvisioningConfig {
        label_prefix: options.label_prefix.clone(),
        disk_gb: options.disk_gb,
        ssh_user: options.ssh_user.clone(),
        confirm_lease: options.confirm_lease,
        onstart: options.onstart.clone(),
        ssh_public_key: Some(public_key),
        selection: selection_policy(options),
        ..VastAiProvisioningConfig::default()
    };
    if let Some(poll_interval) = options.poll_interval {
        config.lifecycle.poll_interval = poll_interval;
    }

    Ok(Box::new(VastAiProvisioningPlugin::new(
        ToolsVastAiLeaseClient::from_api_key(api_key.to_owned())?
            .with_actor_host(runtime.clone(), engine.clone()),
        SshCommandBootstrapLauncher::new(Some(identity), runtime, engine),
        config,
    )))
}

fn job_node_spec(
    options: &VastAiJobOptions,
    image: String,
    orch_json: &str,
) -> Result<NodeProvisionSpec, String> {
    let mut env = vec![
        ("MYELIN_RUN_ID".to_owned(), options.run_id.to_string()),
        (
            "MYELIN_LOGICAL_NODE_ID".to_owned(),
            options.node_id.to_string(),
        ),
        ("MYELIN_NODE_PROVIDER".to_owned(), "vastai".to_owned()),
        (
            "MYELIN_STAGE_INDEX".to_owned(),
            DEFAULT_STAGE_INDEX.to_string(),
        ),
        (
            MVP_IROH_ENDPOINT_ADDR_MASK_ENV.to_owned(),
            options.endpoint_addr_mask.clone(),
        ),
    ];
    if let Some(mode) = options
        .relay_mode
        .as_ref()
        .filter(|mode| !mode.trim().is_empty())
    {
        env.push((MYELIN_IROH_RELAY_MODE_ENV.to_owned(), mode.clone()));
    }
    if let Some(url) = options
        .relay_url
        .as_ref()
        .filter(|url| !url.trim().is_empty())
    {
        env.push((MYELIN_IROH_RELAY_URL_ENV.to_owned(), url.clone()));
        env.push((SWACTOR_IROH_RELAY_URL_ENV.to_owned(), url.clone()));
    }
    let command = worker_bootstrap_command(options, orch_json, &env);

    Ok(NodeProvisionSpec {
        run_id: options.run_id,
        node_id: options.node_id,
        attempt_id: 0,
        stage_index: Some(DEFAULT_STAGE_INDEX),
        image,
        env,
        args: vec![command],
        mounts: Vec::new(),
    })
}

fn build_cluster(
    options: &VastAiJobOptions,
    spec: NodeProvisionSpec,
    provisioner: Box<dyn ProvisionPlugin>,
    engine: swactor_engine::EngineHandle,
    runtime: swactor::runtime::Runtime,
    sink: PluginSink,
) -> Result<ProvisionedClusterGuard, String> {
    let group_id = NodeGroupId(format!("job-node-{}", spec.node_id));
    let logical_node_id = LogicalNodeId(format!("{}-0", group_id.0));
    let selection = selection_policy(options);
    let desired = ClusterShape {
        run_id: RunId(spec.run_id),
        generation: 1,
        groups: vec![RunNodeGroupSpec {
            run_id: RunId(spec.run_id),
            group_id,
            role: RoleId("job-worker".to_owned()),
            count: 1,
            provider: ProviderKind::new("vastai"),
            shape: DesiredNodeShape {
                image: spec.image.clone(),
                disk_gb: options.disk_gb,
                gpu_name: selection.gpu_name.clone(),
                min_gpu_ram_mb: selection.min_gpu_ram_mb,
                min_down_mbps: Some(selection.min_down_mbps),
                min_up_mbps: selection.min_up_mbps,
                min_reliability: Some(selection.min_reliability),
                require_verified: selection.require_verified,
                provider_labels: BTreeMap::from([(
                    "myelin.job_runner".to_owned(),
                    "reconciled-vastai".to_owned(),
                )]),
            },
            boot: BootSpec {
                ssh_user: options.ssh_user.clone(),
                verify_commands: Vec::new(),
                start_swactor_command: spec.args.join(" "),
                stdout_sources: Vec::new(),
                stderr_sources: Vec::new(),
                env: spec.env.clone(),
                args: spec.args.clone(),
                mounts: Vec::new(),
            },
            swarm_join: SwarmJoinTemplate {
                orch_swactor_addr: sessionless_orchestrator_ref(),
                join_token_ref: "myelin-job-worker-identity".to_owned(),
            },
        }],
    };
    let retry = RetryPolicy {
        operation_timeout: options.provision_timeout,
        ..RetryPolicy::default()
    };
    ProvisionedClusterGuard::new(
        desired,
        vec![ReconcilerNodeBinding {
            logical_node_id,
            provision: spec,
            plugin: provisioner,
        }],
        retry,
        engine,
        runtime,
        sink,
    )
}

fn selection_policy(options: &VastAiJobOptions) -> SelectionPolicy {
    let mut selection = SelectionPolicy::default();
    if let Some(gpu_name) = &options.gpu_name {
        selection.gpu_name = Some(gpu_name.clone());
    }
    if let Some(min_gpu_ram_mb) = options.min_gpu_ram_mb {
        selection.min_gpu_ram_mb = Some(min_gpu_ram_mb);
    }
    if let Some(min_down_mbps) = options.min_down_mbps {
        selection.min_down_mbps = min_down_mbps;
    }
    if let Some(min_up_mbps) = options.min_up_mbps {
        selection.min_up_mbps = Some(min_up_mbps);
    }
    if let Some(max_dph_total) = options.max_dph_total {
        selection.max_dph_total = Some(max_dph_total);
    }
    if let Some(min_reliability) = options.min_reliability {
        selection.min_reliability = min_reliability;
    }
    if let Some(require_verified) = options.require_verified {
        selection.require_verified = require_verified;
    }
    for host_id in &options.blacklist_hosts {
        if !selection.blacklist_hosts.contains(host_id) {
            selection.blacklist_hosts.push(*host_id);
        }
    }
    selection
}

fn worker_bootstrap_command(
    options: &VastAiJobOptions,
    orch_json: &str,
    env: &[(String, String)],
) -> String {
    // The VastAI SSH bootstrap session does not inherit the container's
    // environment, so the relay + endpoint-address-mask env must be exported
    // inline. Without this the worker advertises container-local addresses
    // and is unreachable across the internet.
    let exports = env
        .iter()
        .map(|(key, value)| format!("export {}={};", shell_quote(key), shell_quote(value)))
        .collect::<Vec<_>>()
        .join(" ");
    format!(
        "{exports} exec {} --orch-identity {} --workdir {}",
        shell_quote(&options.remote_worker_bin),
        shell_quote(orch_json),
        shell_quote(&options.worker_workdir),
    )
}

fn drain_observations(
    observations: &mpsc::Receiver<PluginObservation>,
    worker: &mut Option<NodeIdentity>,
) -> Result<(), String> {
    while let Ok(observation) = observations.try_recv() {
        match observation {
            PluginObservation::StdoutLine { line, .. } => {
                eprintln!("job-reconcile worker stdout: {line}");
                if let Some(identity) = parse_worker_identity(&line)? {
                    *worker = Some(identity);
                }
            }
            PluginObservation::StderrLine { line, .. } => {
                eprintln!("job-reconcile worker stderr: {line}");
            }
            PluginObservation::ProviderLine { line, .. } => {
                eprintln!("job-reconcile provider: {line}");
            }
            PluginObservation::TelemetryFrame { channel, .. } => {
                eprintln!("job-reconcile telemetry frame: {channel}");
            }
            PluginObservation::Exited {
                node_id, status, ..
            } => {
                return Err(format!(
                    "reconciled job worker node {node_id} exited before job completion: {status:?}"
                ));
            }
            PluginObservation::Failed {
                node_id, reason, ..
            } => {
                return Err(format!(
                    "reconciled job worker node {node_id} failed before job completion: {reason}"
                ));
            }
        }
    }
    Ok(())
}

fn parse_worker_identity(line: &str) -> Result<Option<NodeIdentity>, String> {
    let Some(json) = line.trim().strip_prefix("JOB_WORKER_IDENTITY ") else {
        return Ok(None);
    };
    serde_json::from_str(json)
        .map(Some)
        .map_err(|e| format!("parse JOB_WORKER_IDENTITY from reconciler stdout: {e}"))
}

fn observation_channel() -> (PluginSink, mpsc::Receiver<PluginObservation>) {
    let (tx, rx) = mpsc::channel();
    (
        PluginSink::new(Arc::new(ChannelObservationSink { tx: Mutex::new(tx) })),
        rx,
    )
}

struct ChannelObservationSink {
    tx: Mutex<mpsc::Sender<PluginObservation>>,
}

impl PluginObservationSink for ChannelObservationSink {
    fn observe(&self, observation: PluginObservation) {
        if let Ok(tx) = self.tx.lock() {
            let _ = tx.send(observation);
        }
    }
}

fn shell_quote(value: &str) -> String {
    if value.is_empty() {
        return "''".to_owned();
    }
    if value.bytes().all(|b| {
        b.is_ascii_alphanumeric()
            || matches!(
                b,
                b'/' | b'.' | b'_' | b'-' | b':' | b'=' | b'@' | b'+' | b','
            )
    }) {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn required_option<'a>(value: Option<&'a str>, label: &str) -> Result<&'a str, String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("missing required {label}"))
}

fn validate_non_empty(label: &str, value: &str) -> Result<(), String> {
    if value.trim().is_empty() {
        Err(format!("missing required {label}"))
    } else {
        Ok(())
    }
}

fn default_run_id() -> u64 {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    millis.try_into().unwrap_or(u64::MAX)
}

fn sessionless_orchestrator_ref() -> String {
    "job-orchestrator-identity-exchanged-out-of-band".to_owned()
}
