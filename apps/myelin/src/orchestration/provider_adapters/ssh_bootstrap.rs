//! Shared SSH bootstrap transport for remote node providers.
//!
//! Two launchers live here:
//!
//! - [`SshCommandBootstrapLauncher`]: the attached-worker transport used by
//!   the Vast.ai provider. The SSH session stays open for the worker's whole
//!   life; the worker's stdio and exit status flow through the session.
//! - [`SshArtifactBootstrapLauncher`]: the deployment-bundle transport. One
//!   SSH session streams a tar bundle over stdin, installs it, and starts the
//!   worker detached. Successful SSH exit means "installation and launch were
//!   dispatched", never "the node exited", so no `PluginObservation::Exited`
//!   is emitted on success. After the receipt, node logs, health, and control
//!   travel exclusively over the data plane.
//!
//! This module is the only place in the orchestrator allowed to invoke `ssh`.

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use serde_json::json;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::EngineHandle;
use telemetry::TelemetryProducer;

use crate::observability::provisioning_logs::BootstrapTelemetryBridge;
use crate::provisioning::{
    DeploymentIdentity, NodeProvisionSpec, PluginSink, execution_owner_deadline,
};
use std::sync::Arc;
use swactor_process::{
    ProcessStream, ProcessStreamObservation, SupervisedChild, SupervisedProcessObservation,
};

use swactor_vastai::LifecyclePolicy;

/// Remote SSH target for one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SshEndpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
}

/// Live orchestrator routing, resolved at bootstrap-attempt time so a
/// restarted orchestrator hands workers its current endpoint.
pub(crate) type BootstrapEnvSource =
    Arc<dyn Fn() -> Result<Vec<(String, String)>, String> + Send + Sync>;

/// Provider hook for starting/stopping one node's SSH bootstrap observation.
pub(crate) trait SshBootstrapLauncher: Send {
    type Handle: Send;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: SshEndpoint,
        sink: PluginSink,
        producer: Option<TelemetryProducer>,
        lifecycle: LifecyclePolicy,
    ) -> Result<Self::Handle, String>;

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SshBootstrapStream {
    Stdout,
    Stderr,
}

#[derive(Clone)]
pub(super) enum SshBootstrapMsg {
    StartAttempt,
    ChildExited {
        attempt: u64,
        result: Result<std::process::ExitStatus, String>,
    },
    OutputLine {
        stream: SshBootstrapStream,
        line: String,
    },
    ReaderError {
        stream: SshBootstrapStream,
        error: String,
    },
    ReaderClosed {
        stream: SshBootstrapStream,
    },
    #[cfg(test)]
    ScriptedAttemptFinished {
        result: Result<i32, String>,
    },
    Stop,
}

struct SshOutputRelay {
    target: ActorAddress,
}

impl ActorInterface for SshOutputRelay {
    type Incoming = SupervisedProcessObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        let message = match observation {
            SupervisedProcessObservation::Exited { operation, result } => {
                SshBootstrapMsg::ChildExited {
                    attempt: operation,
                    result,
                }
            }
            SupervisedProcessObservation::Stream(observation) => match observation {
                ProcessStreamObservation::Line { stream, line } => SshBootstrapMsg::OutputLine {
                    stream: map_process_stream(stream),
                    line,
                },
                ProcessStreamObservation::Error { stream, error } => SshBootstrapMsg::ReaderError {
                    stream: map_process_stream(stream),
                    error,
                },
                ProcessStreamObservation::Closed { stream } => SshBootstrapMsg::ReaderClosed {
                    stream: map_process_stream(stream),
                },
            },
        };
        let _ = ctx.send(self.target, message);
    }
}

#[cfg(test)]
fn pending_test_command() -> Command {
    let mut command = Command::new("sh");
    command.arg("-c").arg("echo pending; sleep 30");
    command
}

fn map_process_stream(stream: ProcessStream) -> SshBootstrapStream {
    match stream {
        ProcessStream::Stdout => SshBootstrapStream::Stdout,
        ProcessStream::Stderr => SshBootstrapStream::Stderr,
    }
}

fn bootstrap_owner_remaining() -> Result<Option<Duration>, String> {
    execution_owner_deadline()
        .map(|deadline| deadline.map(|deadline| deadline.saturating_duration_since(Instant::now())))
}

pub(super) struct SshBootstrapActor {
    bridge: BootstrapTelemetryBridge,
    endpoint: SshEndpoint,
    ssh_identity: Option<PathBuf>,
    sender: ExternalSender,
    live_env: BootstrapEnvSource,
    engine: EngineHandle,
    child: Option<SupervisedChild>,
    reader_relay: Option<ActorAddress>,
    stdout_closed: bool,
    stderr_closed: bool,
    pending_status: Option<std::process::ExitStatus>,
    pending_wait_error: Option<String>,
    attempt: u64,
    attempt_started: Instant,
    backoff: Duration,
    observation_class: Option<&'static str>,
    stopped: bool,
    #[cfg(test)]
    disable_attempt_spawn: bool,
    #[cfg(test)]
    spawn_pending_test_child: bool,
}

impl SshBootstrapActor {
    pub(super) fn new(
        bridge: BootstrapTelemetryBridge,
        endpoint: SshEndpoint,
        ssh_identity: Option<PathBuf>,
        live_env: BootstrapEnvSource,
        sender: ExternalSender,
        engine: EngineHandle,
    ) -> Self {
        Self {
            bridge,
            endpoint,
            live_env,
            ssh_identity,
            sender,
            engine,
            child: None,
            stdout_closed: true,
            reader_relay: None,
            stderr_closed: true,
            pending_status: None,
            pending_wait_error: None,
            attempt: 1,
            attempt_started: Instant::now(),
            backoff: Duration::from_secs(1),
            observation_class: None,
            stopped: false,
            #[cfg(test)]
            disable_attempt_spawn: false,
            #[cfg(test)]
            spawn_pending_test_child: false,
        }
    }

    fn run_id(&self) -> u64 {
        self.bridge.spec().run_id
    }

    fn node_id(&self) -> u64 {
        self.bridge.spec().node_id
    }

    fn schedule(&self, ctx: &Ctx, message: SshBootstrapMsg, delay: Duration) {
        self.engine
            .send_after(delay, self.sender.clone(), ctx.self_addr(), message);
    }

    #[cfg(test)]
    pub(super) fn with_attempt_spawn_disabled(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self
    }

    #[cfg(test)]
    pub(super) fn with_pending_test_child(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self.spawn_pending_test_child = true;
        self
    }

    #[cfg(test)]
    pub(super) fn with_open_test_streams(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self.stdout_closed = false;
        self.stderr_closed = false;
        self
    }

    fn start_attempt(&mut self, ctx: &Ctx) {
        if self.stopped || self.child.is_some() {
            return;
        }
        let deadline = match execution_owner_deadline() {
            Ok(deadline) => deadline,
            Err(error) => {
                self.stopped = true;
                self.bridge
                    .observe_provider_line(format!("SSH owner cancelled: {error}"));
                ctx.stop_self();
                return;
            }
        };
        self.attempt_started = Instant::now();
        self.bridge.observe_provider_line(format!(
            "VastAI SSH bootstrap attempt {} to {}@{}:{}",
            self.attempt, self.endpoint.user, self.endpoint.host, self.endpoint.port
        ));
        #[cfg(test)]
        let attempt = if self.disable_attempt_spawn {
            if self.spawn_pending_test_child {
                Ok(pending_test_command())
            } else {
                return;
            }
        } else {
            ssh_bootstrap_attempt_command(
                self.bridge.spec(),
                &self.live_env,
                &self.endpoint,
                self.ssh_identity.as_deref(),
            )
        };
        #[cfg(not(test))]
        let attempt = ssh_bootstrap_attempt_command(
            self.bridge.spec(),
            &self.live_env,
            &self.endpoint,
            self.ssh_identity.as_deref(),
        );
        let relay = self
            .reader_relay
            .expect("SSH output relay is installed before attempts start");
        let attempt = attempt.and_then(|mut command| {
            SupervisedChild::spawn(
                &mut command,
                None,
                deadline,
                self.sender.clone(),
                relay,
                self.attempt,
            )
            .map_err(|error| format!("start SSH process owner: {error}"))
        });
        match attempt {
            Ok(child) => {
                self.child = Some(child);
                self.stdout_closed = false;
                self.stderr_closed = false;
                self.observation_class = None;
                self.pending_status = None;
                self.pending_wait_error = None;
            }
            Err(error) => {
                self.bridge.observe_provider_line(format!(
                    "spawn VastAI SSH bootstrap attempt {} failed: {error}; retrying",
                    self.attempt
                ));
                self.schedule_retry(ctx);
            }
        }
    }

    fn child_exited(
        &mut self,
        ctx: &Ctx,
        attempt: u64,
        result: Result<std::process::ExitStatus, String>,
    ) {
        if self.stopped || attempt != self.attempt {
            return;
        }
        self.child = None;
        match result {
            Ok(status) => self.pending_status = Some(status),
            Err(error) => self.pending_wait_error = Some(error),
        }
        self.maybe_finish_attempt(ctx);
    }

    fn handle_output_line(&mut self, stream: SshBootstrapStream, line: String) {
        match stream {
            SshBootstrapStream::Stdout => self.bridge.observe_stdout_line(line),
            SshBootstrapStream::Stderr => {
                if let Some(class) = classify_ssh_observation(&line) {
                    self.observation_class = Some(class);
                    self.bridge.observe_provider_line(
                        json!({
                            "type": "VastAiBootstrapObservationClass",
                            "run_id": self.run_id(),
                            "node_id": self.node_id(),
                            "class": class,
                        })
                        .to_string(),
                    );
                }
                self.bridge.observe_stderr_line(line);
            }
        }
    }

    fn handle_reader_error(&self, stream: SshBootstrapStream, error: String) {
        let stream = match stream {
            SshBootstrapStream::Stdout => "stdout",
            SshBootstrapStream::Stderr => "stderr",
        };
        self.bridge
            .observe_provider_line(format!("read VastAI SSH {stream}: {error}"));
    }

    fn handle_reader_closed(&mut self, ctx: &Ctx, stream: SshBootstrapStream) {
        match stream {
            SshBootstrapStream::Stdout => self.stdout_closed = true,
            SshBootstrapStream::Stderr => self.stderr_closed = true,
        }
        self.maybe_finish_attempt(ctx);
    }

    fn finish_completed_attempt(&mut self, ctx: &Ctx, status: String, success: bool) {
        let readiness = if success {
            "exited before runtime ready"
        } else {
            "not ready before runtime ready"
        };
        let observation_class = self.observation_class.unwrap_or("process_exit");
        self.bridge.observe_provider_line(
            json!({
                "type": "VastAiBootstrapAttemptCompleted",
                "run_id": self.run_id(),
                "node_id": self.node_id(),
                "attempt": self.attempt,
                "status": status,
                "elapsed_ms": self.attempt_started.elapsed().as_secs_f64() * 1000.0,
                "class": observation_class,
                "classification": readiness,
            })
            .to_string(),
        );
        self.schedule_retry(ctx);
    }

    fn maybe_finish_attempt(&mut self, ctx: &Ctx) {
        if self.stopped || !self.stdout_closed || !self.stderr_closed {
            return;
        }
        if let Some(error) = self.pending_wait_error.take() {
            self.bridge.observe_provider_line(format!(
                "wait VastAI SSH bootstrap attempt {}: {error}; retrying",
                self.attempt
            ));
            self.schedule_retry(ctx);
            return;
        }
        let Some(status) = self.pending_status.take() else {
            return;
        };
        let success = status.success();
        self.finish_completed_attempt(ctx, status.to_string(), success);
    }

    fn schedule_retry(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        let delay = match bootstrap_owner_remaining() {
            Ok(Some(left)) => self.backoff.min(left),
            Ok(None) => self.backoff,
            Err(error) => {
                self.stopped = true;
                self.bridge
                    .observe_provider_line(format!("SSH owner cancelled: {error}"));
                ctx.stop_self();
                return;
            }
        };
        self.bridge.observe_provider_line(format!(
            "VastAI SSH bootstrap retrying in {}s after attempt {}",
            delay.as_secs(),
            self.attempt
        ));
        self.backoff = std::cmp::min(self.backoff.saturating_mul(2), Duration::from_secs(30));
        self.attempt = self.attempt.saturating_add(1);
        self.schedule(ctx, SshBootstrapMsg::StartAttempt, delay);
    }

    fn stop_child(&mut self) {
        drop(self.child.take());
        self.stdout_closed = true;
        self.stderr_closed = true;
    }

    fn stop_relay(&mut self, ctx: &Ctx) {
        if let Some(reader_relay) = self.reader_relay.take() {
            let _ = ctx.stop_actor(reader_relay);
        }
    }

    fn mark_stopped(&mut self) {
        if self.stopped {
            return;
        }
        self.stopped = true;
        self.bridge.observe_provider_line(
            json!({
                "type": "VastAiBootstrapStopped",
                "run_id": self.run_id(),
                "node_id": self.node_id(),
                "attempt": self.attempt,
                "child_active": self.child.is_some(),
                "classification": "bootstrap_stopped",
            })
            .to_string(),
        );
    }
}

impl ActorInterface for SshBootstrapActor {
    type Incoming = SshBootstrapMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        match ctx.spawn(SshOutputRelay {
            target: ctx.self_addr(),
        }) {
            Ok(reader_relay) => {
                self.reader_relay = Some(reader_relay);
                let _ = ctx.send(ctx.self_addr(), SshBootstrapMsg::StartAttempt);
            }
            Err(error) => {
                self.bridge
                    .observe_provider_line(format!("spawn SSH output relay actor: {error}"));
                ctx.stop_self();
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            SshBootstrapMsg::StartAttempt => self.start_attempt(ctx),
            SshBootstrapMsg::ChildExited { attempt, result } => {
                self.child_exited(ctx, attempt, result)
            }
            SshBootstrapMsg::OutputLine { stream, line } => self.handle_output_line(stream, line),
            SshBootstrapMsg::ReaderError { stream, error } => {
                self.handle_reader_error(stream, error)
            }
            SshBootstrapMsg::ReaderClosed { stream } => self.handle_reader_closed(ctx, stream),
            #[cfg(test)]
            SshBootstrapMsg::ScriptedAttemptFinished { result } => {
                self.stdout_closed = true;
                self.stderr_closed = true;
                match result {
                    Ok(status) => self.finish_completed_attempt(
                        ctx,
                        format!("exit status: {status}"),
                        status == 0,
                    ),
                    Err(error) => {
                        self.stop_child();
                        self.pending_wait_error = Some(error);
                        self.maybe_finish_attempt(ctx);
                    }
                }
            }
            SshBootstrapMsg::Stop => {
                self.mark_stopped();
                self.stop_child();
                self.stop_relay(ctx);
                ctx.stop_self();
            }
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.mark_stopped();
        self.stop_child();
        self.stop_relay(ctx);
    }
}

pub(crate) struct SshBootstrapHandle {
    actor: ActorAddress,
    runtime: Runtime,
}

impl Drop for SshBootstrapHandle {
    fn drop(&mut self) {
        let _ = self.runtime.send_to(self.actor, SshBootstrapMsg::Stop);
    }
}

impl SshCommandBootstrapLauncher {
    pub(crate) fn new(
        ssh_identity: Option<PathBuf>,
        runtime: Runtime,
        live_env: BootstrapEnvSource,
        engine: EngineHandle,
    ) -> Self {
        Self {
            ssh_identity,
            runtime,
            live_env,
            engine,
        }
    }
}

/// Attached-worker SSH bootstrap (Vast.ai shape): the remote command execs
/// the worker under an idempotency lock; worker stdio and exit flow through
/// the session.
pub(crate) struct SshCommandBootstrapLauncher {
    ssh_identity: Option<PathBuf>,
    runtime: Runtime,
    live_env: BootstrapEnvSource,
    engine: EngineHandle,
}

impl SshBootstrapLauncher for SshCommandBootstrapLauncher {
    type Handle = SshBootstrapHandle;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: SshEndpoint,
        sink: PluginSink,
        producer: Option<TelemetryProducer>,
        _lifecycle: LifecyclePolicy,
    ) -> Result<Self::Handle, String> {
        if spec.args.is_empty() {
            return Err(format!(
                "VastAI node {} SSH bootstrap command missing",
                spec.node_id
            ));
        }

        let sender = self.runtime.create_sender();
        let bridge = BootstrapTelemetryBridge::new(spec, sink, producer);
        let actor = self
            .runtime
            .spawn(SshBootstrapActor::new(
                bridge,
                endpoint,
                self.ssh_identity.clone(),
                self.live_env.clone(),
                sender,
                self.engine.clone(),
            ))
            .map_err(|e| format!("spawn VastAI SSH bootstrap actor: {e}"))?;

        Ok(SshBootstrapHandle {
            actor,
            runtime: self.runtime.clone(),
        })
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle) {
        let _ = handle.runtime.send_to(handle.actor, SshBootstrapMsg::Stop);
    }
}

pub(crate) fn classify_ssh_observation(line: &str) -> Option<&'static str> {
    let lower = line.trim_start().to_ascii_lowercase();
    if lower.starts_with("myelin-terminal:invalid_artifact") {
        return Some("invalid_artifact");
    }
    if lower.starts_with("myelin-terminal:substrate_lost") {
        return Some("substrate_lost");
    }
    if lower.starts_with("myelin-retry:") {
        return Some("retryable");
    }
    if lower.contains("permission denied (publickey")
        || lower.contains("publickey denied")
        || lower.contains("public key denied")
        || lower.contains("no supported authentication methods")
    {
        return Some("auth_denied");
    }
    if lower.contains("connection refused")
        || lower.contains("connect to host") && lower.contains("refused")
    {
        return Some("refused");
    }
    if lower.contains("operation timed out")
        || lower.contains("connection timed out")
        || lower.contains("connect timed out")
    {
        return Some("timeout");
    }
    None
}

fn ssh_bootstrap_attempt_command(
    spec: &NodeProvisionSpec,
    live_env: &BootstrapEnvSource,
    endpoint: &SshEndpoint,
    ssh_identity: Option<&Path>,
) -> Result<Command, String> {
    let remote_command = ssh_bootstrap_command_for_attempt(spec, live_env)?;
    let mut command = Command::new("ssh");
    command.args(ssh_bootstrap_args(endpoint, &remote_command, ssh_identity));
    Ok(command)
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub(super) fn ssh_bootstrap_lock(spec: &NodeProvisionSpec) -> String {
    let command = spec.args.join(" ");
    let digest = blake3::hash(command.as_bytes()).to_hex();
    format!(
        "/tmp/myelin-bootstrap-{}-{}-{}-{}",
        spec.run_id,
        spec.node_id,
        spec.attempt_id,
        &digest.as_str()[..16],
    )
}

pub(super) fn ssh_bootstrap_command_for_attempt(
    spec: &NodeProvisionSpec,
    live_env: &BootstrapEnvSource,
) -> Result<String, String> {
    let env = live_env().map_err(|error| {
        format!(
            "locate current orchestrator endpoint for VastAI node {}: {error}",
            spec.node_id
        )
    })?;
    Ok(idempotent_ssh_bootstrap_command(spec, &env))
}

pub(super) fn idempotent_ssh_bootstrap_command(
    spec: &NodeProvisionSpec,
    live_env: &[(String, String)],
) -> String {
    let command = shell_single_quote(&spec.args.join(" "));
    let mut effective_env = spec.env.iter().cloned().collect::<BTreeMap<_, _>>();
    effective_env.extend(live_env.iter().cloned());
    let exports = effective_env
        .iter()
        .map(|(key, value)| shell_single_quote(&format!("{key}={value}")))
        .collect::<Vec<_>>()
        .join(" ");
    let export_command = if exports.is_empty() {
        String::new()
    } else {
        format!("export {exports}; ")
    };
    let lock = ssh_bootstrap_lock(spec);
    format!(
        "lock={lock}; command={command}; while :; do \
         if mkdir \"$lock\" 2>/dev/null; then \
           echo $$ > \"$lock/pid\"; {export_command}sh -lc \"$command\"; status=$?; \
           if [ \"$status\" -eq 0 ]; then touch \"$lock/complete\"; else rm -rf \"$lock\"; fi; \
           exit \"$status\"; \
         fi; \
         if [ -f \"$lock/complete\" ]; then echo 'myelin bootstrap already complete'; exit 0; fi; \
         pid=$(cat \"$lock/pid\" 2>/dev/null || true); \
         if [ -n \"$pid\" ] && kill -0 \"$pid\" 2>/dev/null; then \
           echo 'myelin bootstrap already running'; exit 0; \
         fi; \
         rm -rf \"$lock\"; \
         done"
    )
}

pub(super) fn ssh_bootstrap_args(
    endpoint: &SshEndpoint,
    remote_command: &str,
    ssh_identity: Option<&Path>,
) -> Vec<String> {
    let mut args = vec![
        "-v".to_owned(),
        "-p".to_owned(),
        endpoint.port.to_string(),
        "-o".to_owned(),
        "BatchMode=yes".to_owned(),
        "-o".to_owned(),
        "StrictHostKeyChecking=accept-new".to_owned(),
    ];
    if let Some(identity) = ssh_identity {
        args.push("-i".to_owned());
        args.push(identity.to_string_lossy().into_owned());
        args.push("-o".to_owned());
        args.push("IdentitiesOnly=yes".to_owned());
    }
    args.push(format!("{}@{}", endpoint.user, endpoint.host));
    args.push(remote_command.to_owned());
    args
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DeploymentDescriptor {
    artifact_digest: String,
    deployment_generation: String,
    executable_digest: String,
}

impl DeploymentDescriptor {
    fn deployment_identity(&self) -> DeploymentIdentity {
        DeploymentIdentity {
            artifact_digest: self.artifact_digest.clone(),
            deployment_generation: self.deployment_generation.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct DeploymentReceipt {
    #[serde(rename = "type")]
    kind: String,
    artifact_digest: String,
    deployment_generation: String,
    executable_digest: String,
    pid: u32,
    process_start_ticks: u64,
    incarnation: String,
    worker_count: u32,
}

fn validate_deployment_receipt(
    line: &str,
    identity: &DeploymentIdentity,
    executable_digest: &str,
) -> Result<DeploymentReceipt, String> {
    let receipt = serde_json::from_str::<DeploymentReceipt>(line)
        .map_err(|error| format!("decode deployment receipt: {error}"))?;
    if receipt.kind != "MyelinBootstrapReceipt"
        || receipt.artifact_digest != identity.artifact_digest
        || receipt.deployment_generation != identity.deployment_generation
        || receipt.executable_digest != executable_digest
        || receipt.pid <= 1
        || receipt.process_start_ticks == 0
        || receipt.incarnation
            != format!(
                "{}:{}:{}",
                receipt.pid, receipt.process_start_ticks, receipt.deployment_generation
            )
        || receipt.worker_count != 1
    {
        return Err(format!(
            "deployment receipt does not match the requested identity: {receipt:?}"
        ));
    }
    Ok(receipt)
}

/// Deployment-bundle SSH bootstrap: one session streams the bundle over
/// stdin, installs it, and starts the worker detached.
pub(crate) struct SshArtifactBootstrapLauncher {
    ssh_identity: PathBuf,
    bundle: PathBuf,
    verified_bundle: Option<(Arc<[u8]>, DeploymentDescriptor, String)>,
    runtime: Runtime,
    live_env: BootstrapEnvSource,
    engine: EngineHandle,
}

impl SshArtifactBootstrapLauncher {
    pub(crate) fn new(
        ssh_identity: PathBuf,
        bundle: PathBuf,
        runtime: Runtime,
        live_env: BootstrapEnvSource,
        engine: EngineHandle,
    ) -> Self {
        Self {
            ssh_identity,
            bundle,
            verified_bundle: None,
            runtime,
            live_env,
            engine,
        }
    }
}

impl SshBootstrapLauncher for SshArtifactBootstrapLauncher {
    type Handle = SshBootstrapHandle;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: SshEndpoint,
        sink: PluginSink,
        producer: Option<TelemetryProducer>,
        _lifecycle: LifecyclePolicy,
    ) -> Result<Self::Handle, String> {
        if spec.args.is_empty() {
            return Err(format!(
                "invalid_artifact: node {} artifact bootstrap requires a worker command",
                spec.node_id
            ));
        }
        let identity = spec.deployment.clone().ok_or_else(|| {
            format!(
                "static-ssh node {} bootstrap requires a deployment identity",
                spec.node_id
            )
        })?;
        if self.verified_bundle.is_none() {
            let bytes: Arc<[u8]> = fs::read(&self.bundle)
                .map_err(|error| {
                    format!(
                        "invalid_artifact: read deployment bundle {}: {error}",
                        self.bundle.display()
                    )
                })?
                .into();
            let descriptor = decode_bundle_descriptor(&bytes, &self.bundle.display().to_string())
                .map_err(|error| format!("invalid_artifact: {error}"))?;
            let bundle_digest = format!("sha256:{}", sha256_digest(&bytes));
            self.verified_bundle = Some((bytes, descriptor, bundle_digest));
        }
        let (bytes, descriptor, bundle_digest) = self
            .verified_bundle
            .as_ref()
            .expect("bundle verified above");
        if descriptor.deployment_identity() != identity {
            return Err(format!(
                "invalid_artifact: node {} deployment intent {:?} differs from bundle identity {:?}",
                spec.node_id,
                identity,
                descriptor.deployment_identity(),
            ));
        }
        let sender = self.runtime.create_sender();
        let bridge = BootstrapTelemetryBridge::new(spec, sink, producer);
        let actor = self
            .runtime
            .spawn(ArtifactBootstrapActor::new(
                bridge,
                endpoint,
                self.ssh_identity.clone(),
                Arc::clone(bytes),
                identity,
                descriptor.executable_digest.clone(),
                bundle_digest.clone(),
                self.live_env.clone(),
                sender,
                self.engine.clone(),
            ))
            .map_err(|e| format!("spawn artifact SSH bootstrap actor: {e}"))?;
        Ok(SshBootstrapHandle {
            actor,
            runtime: self.runtime.clone(),
        })
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle) {
        let _ = handle.runtime.send_to(handle.actor, SshBootstrapMsg::Stop);
    }
}

struct ArtifactBootstrapActor {
    bridge: BootstrapTelemetryBridge,
    endpoint: SshEndpoint,
    ssh_identity: PathBuf,
    bundle: Arc<[u8]>,
    bundle_digest: String,
    identity: DeploymentIdentity,
    executable_digest: String,
    sender: ExternalSender,
    live_env: BootstrapEnvSource,
    engine: EngineHandle,
    child: Option<SupervisedChild>,
    reader_relay: Option<ActorAddress>,
    stdout_closed: bool,
    stderr_closed: bool,
    pending_status: Option<std::process::ExitStatus>,
    pending_wait_error: Option<String>,
    receipt: Option<DeploymentReceipt>,
    receipt_error: Option<String>,
    attempt: u64,
    attempt_started: Instant,
    backoff: Duration,
    observation_class: Option<&'static str>,
    stopped: bool,
}

impl ArtifactBootstrapActor {
    fn new(
        bridge: BootstrapTelemetryBridge,
        endpoint: SshEndpoint,
        ssh_identity: PathBuf,
        bundle: Arc<[u8]>,
        identity: DeploymentIdentity,
        executable_digest: String,
        bundle_digest: String,
        live_env: BootstrapEnvSource,
        sender: ExternalSender,
        engine: EngineHandle,
    ) -> Self {
        Self {
            bridge,
            endpoint,
            ssh_identity,
            bundle_digest,
            bundle,
            identity,
            executable_digest,
            live_env,
            sender,
            engine,
            child: None,
            stdout_closed: true,
            reader_relay: None,
            stderr_closed: true,
            pending_status: None,
            pending_wait_error: None,
            receipt: None,
            receipt_error: None,
            attempt: 1,
            attempt_started: Instant::now(),
            backoff: Duration::from_secs(1),
            observation_class: None,
            stopped: false,
        }
    }

    fn run_id(&self) -> u64 {
        self.bridge.spec().run_id
    }

    fn node_id(&self) -> u64 {
        self.bridge.spec().node_id
    }

    fn schedule(&self, ctx: &Ctx, message: SshBootstrapMsg, delay: Duration) {
        self.engine
            .send_after(delay, self.sender.clone(), ctx.self_addr(), message);
    }

    fn start_attempt(&mut self, ctx: &Ctx) {
        if self.stopped || self.child.is_some() {
            return;
        }
        let deadline = match execution_owner_deadline() {
            Ok(deadline) => deadline,
            Err(error) => {
                self.stopped = true;
                self.bridge
                    .observe_provider_line(format!("artifact SSH owner cancelled: {error}"));
                ctx.stop_self();
                return;
            }
        };
        self.attempt_started = Instant::now();
        self.bridge.observe_provider_line(
            json!({
                "type": "MyelinArtifactBootstrapAttempt",
                "run_id": self.run_id(),
                "node_id": self.node_id(),
                "attempt": self.attempt,
                "endpoint": format!("{}@{}:{}", self.endpoint.user, self.endpoint.host, self.endpoint.port),
                "artifact_digest": self.identity.artifact_digest,
                "deployment_generation": self.identity.deployment_generation,
            })
            .to_string(),
        );
        let attempt = artifact_bootstrap_attempt_command(
            self.bridge.spec(),
            &self.live_env,
            &self.endpoint,
            &self.ssh_identity,
            &self.identity,
            &self.executable_digest,
            &self.bundle_digest,
        );
        let relay = self
            .reader_relay
            .expect("SSH output relay is installed before attempts start");
        let attempt = attempt.and_then(|mut command| {
            SupervisedChild::spawn(
                &mut command,
                Some(Arc::clone(&self.bundle)),
                deadline,
                self.sender.clone(),
                relay,
                self.attempt,
            )
            .map_err(|error| format!("start artifact SSH process owner: {error}"))
        });
        match attempt {
            Ok(child) => {
                self.child = Some(child);
                self.stdout_closed = false;
                self.stderr_closed = false;
                self.observation_class = None;
                self.pending_status = None;
                self.pending_wait_error = None;
                self.receipt = None;
                self.receipt_error = None;
            }
            Err(error) => {
                self.bridge.observe_provider_line(format!(
                    "spawn artifact SSH bootstrap attempt {} failed: {error}; retrying",
                    self.attempt
                ));
                self.schedule_retry(ctx);
            }
        }
    }

    fn child_exited(
        &mut self,
        ctx: &Ctx,
        attempt: u64,
        result: Result<std::process::ExitStatus, String>,
    ) {
        if self.stopped || attempt != self.attempt {
            return;
        }
        self.child = None;
        match result {
            Ok(status) => self.pending_status = Some(status),
            Err(error) => self.pending_wait_error = Some(error),
        }
        self.maybe_finish_attempt(ctx);
    }

    fn handle_output_line(&mut self, stream: SshBootstrapStream, line: String) {
        match stream {
            SshBootstrapStream::Stdout => {
                if line.contains("MyelinBootstrapReceipt") {
                    match validate_deployment_receipt(
                        &line,
                        &self.identity,
                        &self.executable_digest,
                    ) {
                        Ok(receipt) => {
                            if self
                                .receipt
                                .as_ref()
                                .is_some_and(|current| current != &receipt)
                            {
                                self.receipt_error =
                                    Some("deployment emitted conflicting receipts".to_owned());
                            } else {
                                self.receipt = Some(receipt);
                            }
                        }
                        Err(error) => self.receipt_error = Some(error),
                    }
                }
                self.bridge.observe_stdout_line(line);
            }
            SshBootstrapStream::Stderr => {
                if let Some(class) = classify_ssh_observation(&line) {
                    if !matches!(
                        self.observation_class,
                        Some("auth_denied" | "invalid_artifact" | "substrate_lost")
                    ) {
                        self.observation_class = Some(class);
                    }
                    self.bridge.observe_provider_line(
                        json!({
                            "type": "MyelinArtifactBootstrapObservationClass",
                            "run_id": self.run_id(),
                            "node_id": self.node_id(),
                            "class": class,
                        })
                        .to_string(),
                    );
                }
                self.bridge.observe_stderr_line(line);
            }
        }
    }

    fn handle_reader_error(&self, stream: SshBootstrapStream, error: String) {
        let stream = match stream {
            SshBootstrapStream::Stdout => "stdout",
            SshBootstrapStream::Stderr => "stderr",
        };
        self.bridge
            .observe_provider_line(format!("read artifact SSH {stream}: {error}"));
    }

    fn handle_reader_closed(&mut self, ctx: &Ctx, stream: SshBootstrapStream) {
        match stream {
            SshBootstrapStream::Stdout => self.stdout_closed = true,
            SshBootstrapStream::Stderr => self.stderr_closed = true,
        }
        self.maybe_finish_attempt(ctx);
    }

    fn finish_dispatched(&mut self, ctx: &Ctx) {
        // Successful handoff: installation and launch were dispatched. This
        // is NOT a node exit; worker lifecycle belongs to the data plane from
        // here on. No retry, no PluginObservation::Exited.
        let receipt = self
            .receipt
            .take()
            .expect("dispatch is accepted only with a validated receipt");
        self.bridge.observe_provider_line(
            json!({
                "type": "MyelinBootstrapDispatched",
                "run_id": self.run_id(),
                "node_id": self.node_id(),
                "attempt": self.attempt,
                "elapsed_ms": self.attempt_started.elapsed().as_secs_f64() * 1000.0,
                "artifact_digest": self.identity.artifact_digest,
                "deployment_generation": self.identity.deployment_generation,
                "receipt": receipt,
            })
            .to_string(),
        );
        self.stopped = true;
        self.stop_relay(ctx);
        ctx.stop_self();
    }

    fn finish_terminal(&mut self, ctx: &Ctx, class: &'static str, reason: String) {
        self.bridge.observe_provider_line(
            json!({
                "type": "MyelinArtifactBootstrapFailed",
                "run_id": self.run_id(),
                "node_id": self.node_id(),
                "attempt": self.attempt,
                "class": class,
            })
            .to_string(),
        );
        self.bridge.observe_failed(reason);
        self.stopped = true;
        self.stop_relay(ctx);
        ctx.stop_self();
    }

    fn maybe_finish_attempt(&mut self, ctx: &Ctx) {
        if self.stopped || !self.stdout_closed || !self.stderr_closed {
            return;
        }
        if self.pending_status.is_none() && self.pending_wait_error.is_none() {
            return;
        }
        if let Some(error) = self.receipt_error.take() {
            self.finish_terminal(
                ctx,
                "invalid_receipt",
                format!(
                    "artifact SSH bootstrap node {} returned an invalid receipt: {error}",
                    self.node_id()
                ),
            );
            return;
        }
        if let Some(class @ ("auth_denied" | "invalid_artifact" | "substrate_lost")) =
            self.observation_class
        {
            self.finish_terminal(
                ctx,
                class,
                format!(
                    "artifact SSH bootstrap node {} failed terminally ({class})",
                    self.node_id()
                ),
            );
            return;
        }
        // A matching receipt proves the remote handoff, not SSH's exit status.
        // Losing the transport after that proof must not restart its worker.
        if self.receipt.is_some() {
            self.finish_dispatched(ctx);
            return;
        }
        if let Some(error) = self.pending_wait_error.take() {
            self.bridge.observe_provider_line(format!(
                "wait artifact SSH bootstrap attempt {}: {error}; retrying",
                self.attempt
            ));
            self.schedule_retry(ctx);
            return;
        }
        let status = self
            .pending_status
            .take()
            .expect("completed transport has an exit status or wait error");
        // A clean SSH exit without the matching receipt is still only an
        // unobserved transaction outcome. Retry the level-triggered remote
        // transaction; never promote the SSH status to deployment success.
        let class = if status.success() {
            "missing_receipt"
        } else {
            self.observation_class.unwrap_or("process_exit")
        };
        self.bridge.observe_provider_line(
            json!({
                "type": "MyelinArtifactBootstrapAttemptFailed",
                "run_id": self.run_id(),
                "node_id": self.node_id(),
                "attempt": self.attempt,
                "status": status.to_string(),
                "elapsed_ms": self.attempt_started.elapsed().as_secs_f64() * 1000.0,
                "class": class,
            })
            .to_string(),
        );
        self.schedule_retry(ctx);
    }

    fn schedule_retry(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        let delay = match bootstrap_owner_remaining() {
            Ok(Some(left)) => self.backoff.min(left),
            Ok(None) => self.backoff,
            Err(error) => {
                self.stopped = true;
                self.bridge
                    .observe_provider_line(format!("artifact SSH owner cancelled: {error}"));
                ctx.stop_self();
                return;
            }
        };
        self.bridge.observe_provider_line(format!(
            "artifact SSH bootstrap retrying in {}s after attempt {}",
            delay.as_secs(),
            self.attempt
        ));
        self.backoff = std::cmp::min(self.backoff.saturating_mul(2), Duration::from_secs(30));
        self.attempt = self.attempt.saturating_add(1);
        self.schedule(ctx, SshBootstrapMsg::StartAttempt, delay);
    }

    fn stop_child(&mut self) {
        drop(self.child.take());
        self.stdout_closed = true;
        self.stderr_closed = true;
    }

    fn stop_relay(&mut self, ctx: &Ctx) {
        if let Some(reader_relay) = self.reader_relay.take() {
            let _ = ctx.stop_actor(reader_relay);
        }
    }
}

impl ActorInterface for ArtifactBootstrapActor {
    type Incoming = SshBootstrapMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        match ctx.spawn(SshOutputRelay {
            target: ctx.self_addr(),
        }) {
            Ok(reader_relay) => {
                self.reader_relay = Some(reader_relay);
                let _ = ctx.send(ctx.self_addr(), SshBootstrapMsg::StartAttempt);
            }
            Err(error) => {
                self.bridge
                    .observe_provider_line(format!("spawn SSH output relay actor: {error}"));
                ctx.stop_self();
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            SshBootstrapMsg::StartAttempt => self.start_attempt(ctx),
            SshBootstrapMsg::ChildExited { attempt, result } => {
                self.child_exited(ctx, attempt, result)
            }
            SshBootstrapMsg::OutputLine { stream, line } => self.handle_output_line(stream, line),
            SshBootstrapMsg::ReaderError { stream, error } => {
                self.handle_reader_error(stream, error)
            }
            SshBootstrapMsg::ReaderClosed { stream } => self.handle_reader_closed(ctx, stream),
            #[cfg(test)]
            SshBootstrapMsg::ScriptedAttemptFinished { result } => {
                self.stdout_closed = true;
                self.stderr_closed = true;
                match result {
                    Ok(0) => self.finish_dispatched(ctx),
                    Ok(status) => {
                        self.bridge.observe_provider_line(format!(
                            "scripted artifact SSH attempt exited {status}"
                        ));
                        self.schedule_retry(ctx);
                    }
                    Err(error) => {
                        self.stop_child();
                        self.pending_wait_error = Some(error);
                        self.maybe_finish_attempt(ctx);
                    }
                }
            }
            SshBootstrapMsg::Stop => {
                if !self.stopped {
                    self.stopped = true;
                    self.bridge.observe_provider_line(
                        json!({
                            "type": "MyelinArtifactBootstrapStopped",
                            "run_id": self.run_id(),
                            "node_id": self.node_id(),
                            "attempt": self.attempt,
                            "child_active": self.child.is_some(),
                        })
                        .to_string(),
                    );
                }
                self.stop_child();
                self.stop_relay(ctx);
                ctx.stop_self();
            }
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        if !self.stopped {
            self.stopped = true;
        }
        self.stop_child();
        self.stop_relay(ctx);
    }
}

fn artifact_bootstrap_attempt_command(
    spec: &NodeProvisionSpec,
    live_env: &BootstrapEnvSource,
    endpoint: &SshEndpoint,
    ssh_identity: &Path,
    identity: &DeploymentIdentity,
    executable_digest: &str,
    bundle_digest: &str,
) -> Result<Command, String> {
    let remote_command =
        artifact_remote_command(spec, live_env, identity, executable_digest, bundle_digest)?;
    let mut command = Command::new("ssh");
    command.args(ssh_bootstrap_args(
        endpoint,
        &remote_command,
        Some(ssh_identity),
    ));
    Ok(command)
}

/// Canonical deployment.json bytes. The harness writes this exact
/// serialization into the bundle; the remote transaction verifies it.
pub(crate) fn deployment_descriptor(
    identity: &DeploymentIdentity,
    executable_digest: &str,
) -> String {
    json!({
        "artifact_digest": identity.artifact_digest,
        "deployment_generation": identity.deployment_generation,
        "executable_digest": executable_digest,
    })
    .to_string()
}

fn artifact_digest_hex(identity: &DeploymentIdentity) -> Result<&str, String> {
    identity
        .artifact_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| {
            format!(
                "artifact digest {:?} must be sha256-prefixed",
                identity.artifact_digest
            )
        })
}

fn validate_deployment_generation(identity: &DeploymentIdentity) -> Result<(), String> {
    if identity.deployment_generation.is_empty()
        || !identity
            .deployment_generation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(format!(
            "deployment generation {:?} is not a safe nonempty token",
            identity.deployment_generation
        ));
    }
    Ok(())
}

// PID files are hints, not authority to signal a process group. Discover live
// workers, pin their incarnations with pidfds, and freeze the complete tree
// (including children forked by non-leader threads) before killing anything.
// Persist captured incarnations before signalling so a crashed fence can resume.
const ARTIFACT_PROCESS_FENCE: &str = r#"
import errno
import json
import os
from pathlib import Path
import select
import signal
import sys
import time

ledger = Path(sys.argv[1]) / 'fenced-processes.json'
captured = {}
started = time.monotonic()
deadline = started + 60

def remaining(predicate):
    left = deadline - time.monotonic()
    if left <= 0:
        raise TimeoutError('%s; pending=%r' % (predicate, list(captured)))
    return left

def cancelled(signum, frame):
    raise TimeoutError('process fence cancelled by signal %s' % signum)

for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
    signal.signal(signum, cancelled)

def stat(pid):
    try:
        text = Path('/proc', str(pid), 'stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    name, fields = text[text.index('(') + 1:].rsplit(')', 1)
    fields = fields.split()
    return (int(fields[19]), fields[0], int(fields[1]), int(fields[2]),
            int(fields[3]), name)

def same(pid, ticks):
    current = stat(pid)
    return current is not None and current[0] == ticks

protected = set()
protected_groups = set()
parent = os.getpid()
while parent > 1 and parent not in protected:
    protected.add(parent)
    current = stat(parent)
    if current is None:
        break
    protected_groups.add(current[3])
    parent = current[2]

def pin(pid, ticks):
    if pid <= 1 or pid in protected or (pid, ticks) in captured:
        return
    try:
        fd = os.pidfd_open(pid)
    except ProcessLookupError:
        return
    if not same(pid, ticks):
        os.close(fd)
        return
    captured[pid, ticks] = fd

def signal_all(sig):
    for fd in captured.values():
        try:
            signal.pidfd_send_signal(fd, sig)
        except ProcessLookupError:
            pass

def save():
    temporary = ledger.with_suffix('.next')
    temporary.write_text(json.dumps(list(captured)))
    os.replace(temporary, ledger)

def snapshot():
    result = {}
    for path in Path('/proc').iterdir():
        if path.name.isdecimal():
            pid = int(path.name)
            current = stat(pid)
            if current is not None:
                result[pid] = current
    return result

def worker(pid, current):
    if pid in protected:
        return False
    if current[5] in ('myelin-worker', 'swactor'):
        return True
    try:
        executable = os.readlink('/proc/%s/exe' % pid).removesuffix(' (deleted)')
        if executable.rsplit('/', 1)[-1] in ('myelin-worker', 'swactor'):
            return True
        environment = Path('/proc', str(pid), 'environ').read_bytes().split(b'\0')
    except OSError as error:
        # Root inside a container cannot ptrace unrelated non-dumpable sshd
        # privilege-separation processes. Named workers are already recognized
        # above; known descendants are pinned separately from this discovery.
        if error.errno in (errno.ENOENT, errno.ESRCH, errno.EACCES, errno.EPERM):
            return False
        raise
    # Detached/reparented descendants retain the deployment's environment.
    return (any(value.startswith(b'MYELIN_ARTIFACT_DIGEST=sha256:') for value in environment)
            and any(value.startswith(b'MYELIN_DEPLOYMENT_GENERATION=')
                    and value != b'MYELIN_DEPLOYMENT_GENERATION=' for value in environment))

def fence():
    if not hasattr(os, 'pidfd_open') or not hasattr(signal, 'pidfd_send_signal'):
        raise RuntimeError('Python/Linux pidfd support is unavailable')
    try:
        previous = json.loads(ledger.read_text())
    except (FileNotFoundError, ValueError):
        previous = []
    if isinstance(previous, list):
        for entry in previous:
            if (isinstance(entry, list) and len(entry) == 2
                    and all(type(value) is int and value > 0 for value in entry)):
                pin(*entry)
    while True:
        remaining('capture stopped process trees')
        before = len(captured)
        processes = snapshot()
        previously_stopped = all(
            pid not in processes or processes[pid][0] != ticks
            or processes[pid][1] in ('T', 't', 'Z', 'X')
            for pid, ticks in captured)
        groups = set()
        for pid, current in processes.items():
            if worker(pid, current):
                pin(pid, current[0])
                if ((pid, current[0]) in captured and current[3] > 1
                        and current[3] not in protected_groups):
                    groups.add((current[3], current[4]))
        # Signal pinned members, never a bare/recycled PGID from a stale pidfile.
        for pid, current in processes.items():
            if (current[3], current[4]) in groups:
                pin(pid, current[0])
        for (pid, ticks) in list(captured):
            if not same(pid, ticks):
                continue
            for path in Path('/proc', str(pid), 'task').glob('*/children'):
                try:
                    children = path.read_text().split()
                except (FileNotFoundError, ProcessLookupError):
                    continue
                for child in children:
                    child = int(child)
                    current = stat(child)
                    if current is not None:
                        pin(child, current[0])
        save()
        signal_all(signal.SIGSTOP)
        # A final pass after every thread is stopped closes fork/setsid races.
        stopped = True
        for pid, ticks in captured:
            current = stat(pid)
            if current is not None and current[0] == ticks and current[1] not in ('T', 't', 'Z', 'X'):
                stopped = False
        if len(captured) == before and previously_stopped and stopped:
            break
        time.sleep(min(.02, remaining('stop captured process trees')))
    # Do not resume stopped processes: a TERM handler could fork another escapee.
    signal_all(signal.SIGKILL)
    pending = set(captured)
    while pending:
        pending = {key for key in pending if same(*key)}
        if not pending:
            break
        left = remaining('reap captured process incarnations')
        poll = select.poll()
        for key in pending:
            poll.register(captured[key], select.POLLIN)
        if poll.poll(min(left, .02) * 1000):
            # Exit readiness precedes init's waitpid: reconcile identity removal,
            # never mistake an unreaped zombie or a recycled PID for absence.
            time.sleep(min(left, .005))
    ledger.unlink(missing_ok=True)

try:
    fence()
    print(json.dumps({'type': 'MyelinProcessFenceCompleted',
                      'elapsed_ms': (time.monotonic() - started) * 1000,
                      'captured_incarnations': len(captured)}), file=sys.stderr)
except TimeoutError as error:
    print('MYELIN-RETRY: process fencing pending: %s' % error, file=sys.stderr)
    sys.exit(75)
except Exception as error:
    print('MYELIN-TERMINAL:SUBSTRATE_LOST: process fencing failed: %s' % error,
          file=sys.stderr)
    sys.exit(69)
finally:
    # Failure/cancellation must not leave a frozen process tree behind. The
    # durable ledger remains until a later level-triggered attempt proves reap.
    signal_all(signal.SIGKILL)
    for fd in captured.values():
        os.close(fd)
"#;

const ARTIFACT_BOUNDARY_WAIT: &str = r#"
import ctypes, os, select, sys, time
from pathlib import Path
path = Path(sys.argv[1])
libc = ctypes.CDLL(None, use_errno=True)
fd = libc.inotify_init1(os.O_CLOEXEC | os.O_NONBLOCK)
if fd < 0 or libc.inotify_add_watch(fd, os.fsencode(path.parent), 0x200 | 0x40) < 0:
    raise OSError(ctypes.get_errno(), 'subscribe boundary release')
try:
    while path.exists():
        if select.select([fd], [], [], 30)[0]:
            os.read(fd, 65536)
finally:
    os.close(fd)
"#;

/// The remote deployment transaction. It verifies immutable bundle bytes,
/// fences every recorded or discoverable stale worker/process group, repairs
/// partial releases, atomically activates the requested release, and emits a
/// receipt only after the live process and executable bytes are verified.
pub(crate) fn artifact_remote_command(
    spec: &NodeProvisionSpec,
    live_env: &BootstrapEnvSource,
    identity: &DeploymentIdentity,
    executable_digest: &str,
    bundle_digest: &str,
) -> Result<String, String> {
    if spec.args.is_empty() {
        return Err(format!(
            "node {} artifact bootstrap requires a worker command",
            spec.node_id
        ));
    }
    let env = live_env().map_err(|error| {
        format!(
            "locate current orchestrator endpoint for node {}: {error}",
            spec.node_id
        )
    })?;
    let digest_hex = artifact_digest_hex(identity)?;
    let executable_hex = executable_digest.strip_prefix("sha256:").ok_or_else(|| {
        format!("executable digest {executable_digest:?} must be sha256-prefixed")
    })?;
    let bundle_hex = bundle_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| format!("bundle digest {bundle_digest:?} must be sha256-prefixed"))?;
    for (name, value) in [
        ("bundle digest", bundle_hex),
        ("artifact digest", digest_hex),
        ("executable digest", executable_hex),
    ] {
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!("{name} {value:?} is not a sha256 hex digest"));
        }
    }
    validate_deployment_generation(identity)?;
    let expected_descriptor =
        shell_single_quote(&deployment_descriptor(identity, executable_digest));
    let command = shell_single_quote(&spec.args.join(" "));
    let mut effective_env = spec.env.iter().cloned().collect::<BTreeMap<_, _>>();
    effective_env.extend(env.iter().cloned());
    effective_env.insert(
        "MYELIN_ARTIFACT_DIGEST".to_owned(),
        identity.artifact_digest.clone(),
    );
    effective_env.insert(
        "MYELIN_DEPLOYMENT_GENERATION".to_owned(),
        identity.deployment_generation.clone(),
    );
    let exports = effective_env
        .iter()
        .map(|(key, value)| shell_single_quote(&format!("{key}={value}")))
        .collect::<Vec<_>>()
        .join(" ");
    let generation = shell_single_quote(&identity.deployment_generation);
    let process_fence = shell_single_quote(ARTIFACT_PROCESS_FENCE);
    let boundary_wait = shell_single_quote(ARTIFACT_BOUNDARY_WAIT);
    let transaction = format!(
        r#"set -eu
root=/opt/myelin; bundle_digest={bundle}; digest={digest}; exec_digest={executable}
gen={generation}; expected={expected_descriptor}
invalid() {{ echo "MYELIN-TERMINAL:INVALID_ARTIFACT: $1" >&2; exit 65; }}
substrate() {{ echo "MYELIN-TERMINAL:SUBSTRATE_LOST: $1" >&2; exit 69; }}
mkdir -p "$root" || substrate 'cannot create deployment root'
for tool in flock python3 tar cmp sha256sum install nohup setsid pgrep; do
    command -v "$tool" >/dev/null 2>&1 || substrate "$tool is unavailable"
done
exec 9>>"$root/deployment.lock" || substrate 'cannot open deployment lock'
flock -x 9 || substrate 'cannot lock deployment'
# Never unlink this lock inode. A lost SSH client does not end the remote
# transaction; foreground descendants retain the lock until their work ends.
stage="$root/staging/$gen.$$"; tx="$root/transactions/$gen"
boundaries="$root/deployment-boundaries"
boundary() {{
    mkdir -p "$boundaries" && touch "$boundaries/$gen.$1.reached" ||
        substrate 'cannot record deployment boundary'
    python3 -c {boundary_wait} "$boundaries/$gen.$1.hold"
}}
process_start() {{
    record=$(cat "/proc/$1/stat" 2>/dev/null) || return 1
    record=${{record##*) }}
    set -- $record
    [ "$1" != Z ] && [ "$1" != X ] || return 1
    printf '%s' "${{20}}"
}}
rm -rf "$root/staging" || substrate 'cannot clear partial transfers'
mkdir -p "$root/releases" "$root/transactions" "$stage" ||
    substrate 'cannot create deployment state'
trap 'rm -rf "$stage"' EXIT
boundary transfer_started
cat > "$stage/bundle.tar" || substrate 'artifact transfer failed'
printf '%s  %s\n' "$bundle_digest" "$stage/bundle.tar" |
    sha256sum -c - >/dev/null ||
    {{ echo 'MYELIN-RETRY: artifact transfer incomplete' >&2; exit 75; }}
tar -xf "$stage/bundle.tar" -C "$stage" || invalid 'bundle extraction failed'
printf '%s' "$expected" | cmp -s - "$stage/deployment.json" ||
    invalid 'deployment descriptor mismatch'
printf '%s  %s\n' "$digest" "$stage/payload.tar" |
    sha256sum -c - >/dev/null || invalid 'payload digest mismatch'
release="$root/releases/$digest"; candidate="$release.staged.$$"
if {{ [ -e "$release" ] || [ -L "$release" ]; }} &&
    ! tar -df "$stage/payload.tar" -C "$release" >/dev/null 2>&1; then
    rm -rf "$release" || substrate 'cannot remove corrupt release'
fi
if [ ! -d "$release" ]; then
    rm -rf "$candidate"; mkdir -p "$candidate" || substrate 'cannot stage release'
    tar -xf "$stage/payload.tar" -C "$candidate" ||
        {{ rm -rf "$candidate"; invalid 'payload extraction failed'; }}
    printf '%s  %s\n' "$exec_digest" "$candidate/bin/myelin-worker" |
        sha256sum -c - >/dev/null ||
        {{ rm -rf "$candidate"; invalid 'worker executable digest mismatch'; }}
    mv "$candidate" "$release" ||
        {{ rm -rf "$candidate"; substrate 'cannot commit release'; }}
fi
find "$root/releases" -maxdepth 1 -type d -name '*.staged.*' -exec rm -rf {{}} + \
    2>/dev/null || true
python3 -m pip --version >/dev/null 2>&1 || substrate 'python pip is unavailable'
python3 -c {process_fence} "$root" ||
    {{ code=$?; [ "$code" -eq 75 ] && exit 75; substrate 'stale process fencing failed'; }}
# Global Python installation must not race a still-running previous generation.
set -- "$release"/python/*.whl
if [ -f "$1" ]; then
    python3 -m pip install --break-system-packages --no-index --force-reinstall \
        "$@" >/dev/null 2>&1 ||
    python3 -m pip install --no-index --force-reinstall "$@" >/dev/null 2>&1 ||
        substrate 'swactor wheel installation failed'
fi
launcher=/usr/local/bin/myelin-e2e-python
install -m 0755 "$release/bin/myelin-e2e-python" "$launcher.next" &&
    mv -Tf "$launcher.next" "$launcher" || substrate 'launcher installation failed'
rm -rf /tmp/myelin-bootstrap-* "$root/runtime" "$root/bootstrap.lock" \
    "$root/bootstrap.sock" "$root/agent.pid" "$root/active-deployment.json" "$tx" ||
    substrate 'cannot clear stale runtime state'
rm -rf /var/cache/myelin-contexts/* 2>/dev/null || true
: > /var/log/myelin-node.log || substrate 'cannot open worker log'
mkdir -p "$root/runtime" "$tx" ||
    substrate 'cannot prepare worker state'
printf '%s' "$expected" > "$tx/deployment.json" || substrate 'cannot record deployment'
next="$root/current.next.$$"
rm -rf "$next"; ln -s "$release" "$next" || substrate 'cannot stage activation'
if [ -L "$root/current" ]; then
    mv -Tf "$next" "$root/current" || substrate 'cannot activate release'
else
    rm -rf "$root/current" && mv "$next" "$root/current" ||
        substrate 'cannot activate release'
fi
boundary before_launch
export {exports}
# Only the detached worker closes the transaction lock, before exec.
nohup setsid sh -lc {command} 9>&- >>/var/log/myelin-node.log 2>&1 </dev/null &
pid=$!
echo "$pid" > "$root/agent.pid" || substrate 'cannot record worker pid'
i=0
while kill -0 "$pid" 2>/dev/null; do
    comm=$(cat "/proc/$pid/comm" 2>/dev/null || true)
    [ "$comm" = myelin-worker ] && break
    i=$((i+1)); [ "$i" -lt 250 ] || exit 75
    sleep 0.02
done
start_ticks=$(process_start "$pid") || exit 75
boundary after_launch
printf '%s' "$expected" > "$root/active-deployment.json" ||
    substrate 'cannot record active deployment'
boundary before_receipt
# Holds and SSH loss may outlive the launched process. Revalidate after them.
workers=$(pgrep -x myelin-worker 2>/dev/null || true); set -- $workers
[ "$#" -eq 1 ] && [ "$1" = "$pid" ] || exit 75
printf '%s  %s\n' "$exec_digest" "/proc/$pid/exe" |
    sha256sum -c - >/dev/null || invalid 'active executable digest mismatch'
[ "$(process_start "$pid")" = "$start_ticks" ] || exit 75
incarnation="$pid:$start_ticks:$gen"
printf '{{"type":"MyelinBootstrapReceipt","artifact_digest":"sha256:%s","deployment_generation":"%s","executable_digest":"sha256:%s","pid":%s,"process_start_ticks":%s,"incarnation":"%s","worker_count":1}}\n' \
    "$digest" "$gen" "$exec_digest" "$pid" "$start_ticks" "$incarnation""#,
        bundle = shell_single_quote(bundle_hex),
        digest = shell_single_quote(digest_hex),
        executable = shell_single_quote(executable_hex),
    );
    // Bound the remote owner, not the retained desired deployment. Timeout is
    // an ordinary retryable SSH attempt outcome, never a terminal node failure.
    let seconds = bootstrap_owner_remaining()?
        .unwrap_or(Duration::from_secs(600))
        .min(Duration::from_secs(600))
        .as_secs_f64();
    Ok(format!(
        "command -v timeout >/dev/null 2>&1 || {{ echo 'MYELIN-TERMINAL:SUBSTRATE_LOST: timeout is unavailable' >&2; exit 69; }}; \
         timeout --signal=TERM --kill-after=5 {seconds} sh -lc {}",
        shell_single_quote(&transaction)
    ))
}

/// Minimal ustar reader for exact-name lookup in harness-produced archives.
fn read_tar_member_bytes<'a>(
    data: &'a [u8],
    source: &str,
    member: &str,
) -> Result<&'a [u8], String> {
    let mut offset = 0_usize;
    while let Some(header_end) = offset.checked_add(512).filter(|end| *end <= data.len()) {
        let header = &data[offset..header_end];
        if header.iter().all(|byte| *byte == 0) {
            break;
        }
        let name_end = header[..100]
            .iter()
            .position(|byte| *byte == 0)
            .unwrap_or(100);
        let name = std::str::from_utf8(&header[..name_end])
            .map_err(|error| format!("decode tar member name in {source}: {error}"))?;
        let size_text = std::str::from_utf8(&header[124..136])
            .map_err(|error| format!("decode tar member size in {source}: {error}"))?
            .trim_matches(|c: char| c == '\0' || c.is_ascii_whitespace());
        let size = u64::from_str_radix(size_text, 8)
            .map_err(|error| format!("decode tar member size {size_text:?}: {error}"))?;
        let size = usize::try_from(size)
            .map_err(|_| format!("tar member {name:?} in {source} is too large"))?;
        offset = header_end;
        let end = offset
            .checked_add(size)
            .ok_or_else(|| format!("tar member {name:?} in {source} is too large"))?;
        let Some(payload) = data.get(offset..end) else {
            return Err(format!("tar member {name:?} in {source} is truncated"));
        };
        if name == member {
            return Ok(payload);
        }
        offset = end
            .checked_add((512 - size % 512) % 512)
            .ok_or_else(|| format!("tar member {name:?} in {source} is too large"))?;
    }
    Err(format!("tar member {member:?} not found in {source}"))
}

fn sha256_digest(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn read_bundle_descriptor(bundle: &Path) -> Result<DeploymentDescriptor, String> {
    let bytes = fs::read(bundle)
        .map_err(|error| format!("read deployment bundle {}: {error}", bundle.display()))?;
    decode_bundle_descriptor(&bytes, &bundle.display().to_string())
}

fn decode_bundle_descriptor(bytes: &[u8], source: &str) -> Result<DeploymentDescriptor, String> {
    let descriptor_bytes = read_tar_member_bytes(bytes, source, "deployment.json")?;
    let descriptor = serde_json::from_slice::<DeploymentDescriptor>(&descriptor_bytes)
        .map_err(|error| format!("decode deployment descriptor: {error}"))?;
    let identity = descriptor.deployment_identity();
    validate_deployment_generation(&identity)?;
    let artifact_hex = artifact_digest_hex(&identity)?;
    let executable_hex = descriptor
        .executable_digest
        .strip_prefix("sha256:")
        .ok_or_else(|| {
            format!(
                "executable digest {:?} must be sha256-prefixed",
                descriptor.executable_digest
            )
        })?;
    for (name, value) in [
        ("artifact digest", artifact_hex),
        ("executable digest", executable_hex),
    ] {
        if value.len() != 64 || !value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!("{name} {value:?} is not a sha256 hex digest"));
        }
    }
    let payload = read_tar_member_bytes(bytes, source, "payload.tar")?;
    let actual_artifact = sha256_digest(&payload);
    if actual_artifact != artifact_hex {
        return Err(format!(
            "deployment bundle digest mismatch: descriptor declares sha256:{artifact_hex} but payload hashes to sha256:{actual_artifact}"
        ));
    }
    let executable = read_tar_member_bytes(&payload, "payload.tar", "bin/myelin-worker")?;
    let actual_executable = sha256_digest(&executable);
    if actual_executable != executable_hex {
        return Err(format!(
            "deployment executable digest mismatch: descriptor declares sha256:{executable_hex} but worker hashes to sha256:{actual_executable}"
        ));
    }
    Ok(descriptor)
}

/// Reads and verifies both the payload and active-executable identity fences.
pub(crate) fn read_bundle_identity(bundle: &Path) -> Result<DeploymentIdentity, String> {
    Ok(read_bundle_descriptor(bundle)?.deployment_identity())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::provisioning::DeploymentIdentity;

    fn identity() -> DeploymentIdentity {
        DeploymentIdentity {
            artifact_digest: format!("sha256:{}", "a".repeat(64)),
            deployment_generation: "deploy-test".to_owned(),
        }
    }
    fn executable_digest() -> String {
        format!("sha256:{}", "b".repeat(64))
    }

    /// Hand-rolled ustar member: 512-byte header + payload + zero padding.
    fn ustar_member(name: &str, payload: &[u8]) -> Vec<u8> {
        let mut header = vec![0_u8; 512];
        header[..name.len()].copy_from_slice(name.as_bytes());
        header[100] = b'0'; // regular file
        header[108..116].copy_from_slice(b"0000000\0"); // mode
        header[116..124].copy_from_slice(b"0000000\0"); // uid
        header[124..136].copy_from_slice(format!("{:011o}\0", payload.len()).as_bytes());
        header[136..148].copy_from_slice(b"00000000000\0"); // mtime
        header[148..156].copy_from_slice(b"       \0"); // checksum placeholder
        let checksum: u32 = header.iter().map(|byte| u32::from(*byte)).sum();
        header[148..156].copy_from_slice(format!("{:06o}\0 ", checksum).as_bytes());
        let mut member = header;
        member.extend_from_slice(payload);
        let padding = 512 - (payload.len() % 512);
        if padding < 512 {
            member.extend(std::iter::repeat_n(0_u8, padding));
        }
        member
    }

    #[test]
    fn tar_reader_extracts_exact_members_and_rejects_absent_ones() {
        let mut archive = ustar_member("deployment.json", b"{\"artifact_digest\":\"x\"}");
        archive.extend(ustar_member("payload.tar", b"payload-bytes"));
        archive.extend([0_u8; 1024]);

        assert_eq!(
            read_tar_member_bytes(&archive, "test bundle", "deployment.json").expect("descriptor"),
            b"{\"artifact_digest\":\"x\"}"
        );
        assert_eq!(
            read_tar_member_bytes(&archive, "test bundle", "payload.tar").expect("payload"),
            b"payload-bytes"
        );
        assert!(read_tar_member_bytes(&archive, "test bundle", "missing.txt").is_err());
    }

    #[test]
    fn tar_reader_rejects_oversized_declared_members() {
        let mut archive = ustar_member("payload.tar", b"partial");
        archive[124..136].copy_from_slice(b"777777777777");
        assert!(read_tar_member_bytes(&archive, "corrupt bundle", "payload.tar").is_err());
    }

    #[test]
    fn deployment_descriptor_is_the_canonical_identity_object() {
        let descriptor = deployment_descriptor(&identity(), &executable_digest());
        assert_eq!(
            descriptor,
            format!(
                "{{\"artifact_digest\":\"sha256:{}\",\"deployment_generation\":\"deploy-test\",\"executable_digest\":\"sha256:{}\"}}",
                "a".repeat(64),
                "b".repeat(64),
            )
        );
    }

    #[test]
    fn deployment_receipt_must_match_every_identity_fence() {
        let line = format!(
            "{{\"type\":\"MyelinBootstrapReceipt\",\"artifact_digest\":\"sha256:{}\",\"deployment_generation\":\"deploy-test\",\"executable_digest\":\"sha256:{}\",\"pid\":42,\"process_start_ticks\":99,\"incarnation\":\"42:99:deploy-test\",\"worker_count\":1}}",
            "a".repeat(64),
            "b".repeat(64),
        );
        let receipt =
            validate_deployment_receipt(&line, &identity(), &executable_digest()).unwrap();
        assert_eq!(receipt.pid, 42);

        let wrong = line.replace("deploy-test", "stale-generation");
        assert!(validate_deployment_receipt(&wrong, &identity(), &executable_digest()).is_err());
        let duplicate = line.replace("\"worker_count\":1", "\"worker_count\":2");
        assert!(
            validate_deployment_receipt(&duplicate, &identity(), &executable_digest()).is_err()
        );
        let unknown = line.replace(
            "\"worker_count\":1",
            "\"worker_count\":1,\"unchecked\":true",
        );
        assert!(validate_deployment_receipt(&unknown, &identity(), &executable_digest()).is_err());
        for incarnation in ["43:99:deploy-test", "42:100:deploy-test", "42:99:stale"] {
            let inconsistent = line.replace("42:99:deploy-test", incarnation);
            assert!(
                validate_deployment_receipt(&inconsistent, &identity(), &executable_digest())
                    .is_err()
            );
        }
    }

    #[test]
    fn non_sha256_digests_are_rejected() {
        let mut wrong = identity();
        wrong.artifact_digest = "blake3:abcdef".to_owned();
        assert!(artifact_digest_hex(&wrong).is_err());
    }

    #[test]
    fn deployment_bundle_rejects_invalid_generation_before_transport() {
        let executable = b"worker";
        let mut payload = ustar_member("bin/myelin-worker", executable);
        payload.extend([0_u8; 1024]);
        let identity = DeploymentIdentity {
            artifact_digest: format!("sha256:{}", sha256_digest(&payload)),
            deployment_generation: "deploy-valid".to_owned(),
        };
        let executable_digest = format!("sha256:{}", sha256_digest(executable));
        let make_bundle = |identity: &DeploymentIdentity| {
            let mut bundle = ustar_member(
                "deployment.json",
                deployment_descriptor(identity, &executable_digest).as_bytes(),
            );
            bundle.extend(ustar_member("payload.tar", &payload));
            bundle.extend([0_u8; 1024]);
            bundle
        };
        assert_eq!(
            decode_bundle_descriptor(&make_bundle(&identity), "valid bundle")
                .unwrap()
                .deployment_identity(),
            identity
        );
        let mut invalid = identity;
        invalid.deployment_generation = "invalid generation".to_owned();
        assert!(decode_bundle_descriptor(&make_bundle(&invalid), "invalid bundle").is_err());
    }

    struct ArtifactCompletionProbe {
        actor: ArtifactBootstrapActor,
        result: Option<Result<std::process::ExitStatus, String>>,
    }

    impl ActorInterface for ArtifactCompletionProbe {
        type Incoming = SshBootstrapMsg;
        type Response = ();

        fn on_start(&mut self, ctx: &Ctx) {
            self.actor.child_exited(ctx, 1, self.result.take().unwrap());
        }

        fn handle(&mut self, ctx: &Ctx, message: Self::Incoming) {
            self.actor.handle(ctx, message);
        }

        fn on_stop(&mut self, ctx: &Ctx) {
            self.actor.on_stop(ctx);
        }
    }

    fn artifact_completion_observations(
        output: &[(SshBootstrapStream, String)],
        result: Result<std::process::ExitStatus, String>,
    ) -> Vec<crate::provisioning::PluginObservation> {
        use crate::provisioning::{PluginObservation, PluginObservationSink};
        use swactor::runtime::{RuntimeConfig, RuntimeParts};
        use swactor_engine::{Engine, SteppingBackend};

        #[derive(Default)]
        struct RecordingSink(parking_lot::Mutex<Vec<PluginObservation>>);
        impl PluginObservationSink for RecordingSink {
            fn observe(&self, observation: PluginObservation) {
                self.0.lock().push(observation);
            }
        }
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let recording = Arc::new(RecordingSink::default());
        let bridge = BootstrapTelemetryBridge::new(
            NodeProvisionSpec {
                deployment: Some(identity()),
                run_id: 7,
                node_id: 1,
                attempt_id: 1,
                stage_index: Some(0),
                image: "raw".to_owned(),
                env: Vec::new(),
                args: vec!["exec /opt/myelin/current/bin/myelin-worker".to_owned()],
                offer_criteria_json: None,
                mounts: Vec::new(),
            },
            PluginSink::new(recording.clone()),
            None,
        );
        let mut actor = ArtifactBootstrapActor::new(
            bridge,
            SshEndpoint {
                host: "scripted.invalid".to_owned(),
                port: 22,
                user: "root".to_owned(),
            },
            PathBuf::new(),
            Arc::<[u8]>::from([]),
            identity(),
            executable_digest(),
            format!("sha256:{}", sha256_digest(&[])),
            Arc::new(|| Ok(Vec::new())),
            runtime.create_sender(),
            engine.handle(),
        );
        for (stream, line) in output {
            actor.handle_output_line(*stream, line.clone());
        }
        let address = runtime
            .spawn(ArtifactCompletionProbe {
                actor,
                result: Some(result),
            })
            .unwrap();
        crate::tests::fuzz_support::drive_steps(&backend, 16);
        let _ = runtime.stop_actor(address);
        crate::tests::fuzz_support::drive_steps(&backend, 16);
        std::mem::take(&mut *recording.0.lock())
    }

    #[test]
    fn deployment_receipt_survives_transport_failure_but_never_hides_terminal_failure() {
        use crate::provisioning::PluginObservation;
        use std::os::unix::process::ExitStatusExt;

        let receipt = json!({
            "type": "MyelinBootstrapReceipt",
            "artifact_digest": identity().artifact_digest,
            "deployment_generation": "deploy-test",
            "executable_digest": executable_digest(),
            "pid": 42,
            "process_start_ticks": 99,
            "incarnation": "42:99:deploy-test",
            "worker_count": 1,
        })
        .to_string();
        let provider_events = |observations: &[PluginObservation]| {
            observations
                .iter()
                .filter_map(|observation| match observation {
                    PluginObservation::ProviderLine { line, .. } => {
                        serde_json::from_str::<serde_json::Value>(line).ok()
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
        };
        for result in [
            Ok(std::process::ExitStatus::from_raw(255 << 8)),
            Err("SSH owner lost after receipt".to_owned()),
        ] {
            let observations = artifact_completion_observations(
                &[(SshBootstrapStream::Stdout, receipt.clone())],
                result,
            );
            assert!(provider_events(&observations).iter().any(|event| {
                event["type"] == "MyelinBootstrapDispatched"
                    && event["receipt"]["incarnation"] == "42:99:deploy-test"
            }));
            assert!(!observations.iter().any(|observation| matches!(
                observation,
                PluginObservation::Failed { .. } | PluginObservation::Exited { .. }
            )));
        }
        let observations =
            artifact_completion_observations(&[], Ok(std::process::ExitStatus::from_raw(0)));
        let events = provider_events(&observations);
        assert!(events.iter().any(|event| {
            event["type"] == "MyelinArtifactBootstrapAttemptFailed"
                && event["class"] == "missing_receipt"
        }));
        assert!(!observations.iter().any(|observation| matches!(
            observation,
            PluginObservation::Failed { .. } | PluginObservation::Exited { .. }
        )));
        for (output, status, expected) in [
            (
                vec![
                    (SshBootstrapStream::Stdout, receipt.clone()),
                    (
                        SshBootstrapStream::Stdout,
                        receipt.replace("42:99:deploy-test", "43:99:deploy-test"),
                    ),
                ],
                255,
                "invalid_receipt",
            ),
            (
                vec![
                    (
                        SshBootstrapStream::Stderr,
                        "MYELIN-TERMINAL:SUBSTRATE_LOST: read only".to_owned(),
                    ),
                    (
                        SshBootstrapStream::Stderr,
                        "ssh: connect timed out".to_owned(),
                    ),
                ],
                255,
                "substrate_lost",
            ),
        ] {
            let observations = artifact_completion_observations(
                &output,
                Ok(std::process::ExitStatus::from_raw(status << 8)),
            );
            let events = provider_events(&observations);
            assert!(events.iter().any(|event| {
                event["type"] == "MyelinArtifactBootstrapFailed" && event["class"] == expected
            }));
            assert!(
                !events
                    .iter()
                    .any(|event| event["type"] == "MyelinBootstrapDispatched")
            );
            assert!(
                observations
                    .iter()
                    .any(|observation| matches!(observation, PluginObservation::Failed { .. }))
            );
        }
    }
    #[test]
    fn terminal_and_transient_ssh_failures_are_typed() {
        assert_eq!(
            classify_ssh_observation("MYELIN-TERMINAL:INVALID_ARTIFACT: bad digest"),
            Some("invalid_artifact")
        );
        assert_eq!(
            classify_ssh_observation("MYELIN-TERMINAL:SUBSTRATE_LOST: read only"),
            Some("substrate_lost")
        );
        assert_eq!(
            classify_ssh_observation("MYELIN-RETRY: artifact transfer incomplete"),
            Some("retryable")
        );
        assert_eq!(
            classify_ssh_observation(
                "debug1: Sending command: echo MYELIN-TERMINAL:INVALID_ARTIFACT"
            ),
            None
        );
        assert_eq!(
            classify_ssh_observation("Permission denied (publickey)."),
            Some("auth_denied")
        );
        assert_eq!(
            classify_ssh_observation("connect to host 127.0.0.1 port 22: Connection refused"),
            Some("refused")
        );
        assert_eq!(
            classify_ssh_observation("ssh: connect timed out"),
            Some("timeout")
        );
    }

    struct ProcessCompletionProbe {
        observations: std::sync::mpsc::Sender<SupervisedProcessObservation>,
    }

    impl ActorInterface for ProcessCompletionProbe {
        type Incoming = SupervisedProcessObservation;
        type Response = ();

        fn handle(&mut self, _ctx: &Ctx, observation: Self::Incoming) {
            let _ = self.observations.send(observation);
        }
    }

    #[test]
    fn withheld_ssh_banner_reaps_transport_and_preserves_sibling_cleanup() {
        use std::io::Read;
        use std::net::TcpListener;
        use swactor::config::RuntimeConfig;
        use swactor::runtime::RuntimeParts;
        use swactor_engine::{Engine, TokioBackend, TokioConfig};

        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("healthy-sibling");
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let _engine =
            Engine::new(parts, TokioBackend::new(TokioConfig::default()).unwrap()).unwrap();
        let baseline_actors = runtime.stats().actors.len();
        let (sender, receiver) = std::sync::mpsc::channel();
        let probe = runtime
            .spawn(ProcessCompletionProbe {
                observations: sender,
            })
            .unwrap();
        let endpoint = SshEndpoint {
            host: "127.0.0.1".to_owned(),
            port,
            user: "unused-before-handshake".to_owned(),
        };
        let mut command = Command::new("ssh");
        // Do not consult user/system SSH configuration, agents or known hosts.
        // The real transport reaches identification but never authentication.
        command.args(["-F", "/dev/null"]);
        command.args(ssh_bootstrap_args(&endpoint, "true", None));
        let started = Instant::now();
        let fuse = started + Duration::from_secs(5);
        let observations = std::thread::scope(|scope| {
            let peer = scope.spawn(move || {
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < fuse, "SSH never reached loopback peer");
                            std::thread::yield_now();
                        }
                        Err(error) => panic!("accept SSH transport: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut received = Vec::new();
                let mut bytes = [0; 256];
                loop {
                    match socket.read(&mut bytes) {
                        Ok(0) => break,
                        Ok(count) => {
                            received.extend_from_slice(&bytes[..count]);
                            assert!(received.len() < 4096, "bounded identification exchange");
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => break,
                        Err(error) => {
                            panic!("SSH owner failed to close withheld banner socket: {error}")
                        }
                    }
                }
                assert!(
                    received.starts_with(b"SSH-2.0-"),
                    "real SSH client never identified itself"
                );
            });
            let transport = SupervisedChild::spawn(
                &mut command,
                Some(Arc::from(vec![0_u8; 1024 * 1024])),
                Some(started + Duration::from_secs(1)),
                runtime.create_sender(),
                probe,
                1,
            )
            .unwrap();
            // A separate real owner must finish even while SSH and its input
            // writer cannot progress past the withheld identification boundary.
            let healthy = SupervisedChild::spawn(
                Command::new("touch").arg(&marker),
                None,
                Some(fuse),
                runtime.create_sender(),
                probe,
                2,
            )
            .unwrap();
            let mut observations = Vec::new();
            while observations
                .iter()
                .filter(|observation| {
                    matches!(observation, SupervisedProcessObservation::Exited { .. })
                })
                .count()
                < 2
            {
                assert!(
                    Instant::now() < fuse,
                    "owned SSH/sibling completion was not bounded"
                );
                observations.push(
                    receiver
                        .recv_timeout(fuse.saturating_duration_since(Instant::now()))
                        .expect("bounded SSH/sibling observation"),
                );
            }
            drop(transport);
            drop(healthy);
            peer.join().unwrap();
            assert!(
                marker.is_file(),
                "withheld SSH canceled independent healthy work"
            );
            // Exercise another owned child after failure, not a mocked cleanup
            // callback. Its exit follows actual removal of the sibling resource.
            let cleanup = SupervisedChild::spawn(
                Command::new("rm").arg(&marker),
                None,
                Some(fuse),
                runtime.create_sender(),
                probe,
                3,
            )
            .unwrap();
            while !observations.iter().any(|observation| {
                matches!(
                    observation,
                    SupervisedProcessObservation::Exited { operation: 3, .. }
                )
            }) {
                assert!(
                    Instant::now() < fuse,
                    "remaining cleanup owner did not finish"
                );
                observations.push(
                    receiver
                        .recv_timeout(fuse.saturating_duration_since(Instant::now()))
                        .expect("bounded cleanup observation"),
                );
            }
            drop(cleanup);
            observations
        });
        runtime.stop_actor(probe).unwrap();
        assert!(
            matches!(
                receiver.recv_timeout(fuse.saturating_duration_since(Instant::now())),
                Err(std::sync::mpsc::RecvTimeoutError::Disconnected)
            ),
            "the observation actor must stop after its last completed owner"
        );
        while runtime.stats().actors.len() != baseline_actors {
            assert!(
                Instant::now() < fuse,
                "observation actor baseline did not restore"
            );
            std::thread::yield_now();
        }
        assert!(started.elapsed() < Duration::from_secs(5));
        assert!(
            !marker.exists(),
            "remaining cleanup was skipped after SSH failure"
        );
        let mut exits = BTreeMap::new();
        let mut closed = 0;
        for observation in observations {
            match observation {
                SupervisedProcessObservation::Exited { operation, result } => {
                    assert!(
                        exits.insert(operation, result).is_none(),
                        "duplicate owner completion"
                    );
                }
                SupervisedProcessObservation::Stream(ProcessStreamObservation::Closed {
                    ..
                }) => {
                    closed += 1;
                }
                _ => {}
            }
        }
        assert!(
            exits[&1]
                .as_ref()
                .unwrap_err()
                .contains("pending child exit")
        );
        assert!(exits[&2].as_ref().unwrap().success());
        assert!(exits[&3].as_ref().unwrap().success());
        assert_eq!(
            closed, 6,
            "every owned stdout/stderr reader must close before completion"
        );
        assert_eq!(
            runtime.stats().actors.len(),
            baseline_actors,
            "process observation actor leaked"
        );
    }
}
