//! Shared SSH bootstrap transport for remote node providers.
//!
//! Two launchers live here:
//!
//! - [`SshCommandBootstrapLauncher`]: the attached-worker transport used by
//!   the Vast.ai provider. The SSH session stays open for the worker's whole
//!   life; the worker's stdio and exit status flow through the session.
//! - [`SshArtifactBootstrapLauncher`]: verifies digest-addressed installed
//!   content before activation, transferring a tar bundle only on a cache
//!   miss. Each generation fences old workers and starts a fresh detached
//!   worker. Only a verified incarnation receipt proves dispatch; SSH success
//!   never means "the node exited", so no `PluginObservation::Exited` is
//!   emitted on success. Logs, health, and control then use the data plane.
//!
//! This module is the only place in the orchestrator allowed to invoke `ssh`.

use std::collections::BTreeMap;
use std::fs;
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::{Deserialize, Serialize};
use serde_json::json;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::{BlockingWorkSender, EngineHandle};
use telemetry::TelemetryProducer;

use crate::orchestration::provisioning_logs::BootstrapTelemetryBridge;
use crate::provisioning::{DeploymentIdentity, NodeProvisionSpec, PluginSink};
use std::sync::Arc;
use swactor_process::{
    ProcessStream, ProcessStreamObservation, SupervisedChild, SupervisedProcessObservation,
};

pub const DEPLOYED_WORKER_BIN_PATH: &str = "/opt/myelin/current/bin/myelin-worker";

/// Remote SSH target for one node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SshEndpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
}

/// Live orchestrator routing, resolved at bootstrap-attempt time so a
/// restarted orchestrator hands workers its current endpoint.
pub type BootstrapEnvSource = Arc<dyn Fn() -> Result<Vec<(String, String)>, String> + Send + Sync>;

/// Provider hook for starting/stopping one node's SSH bootstrap observation.
pub trait SshBootstrapLauncher: Send {
    type Handle: Send;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: SshEndpoint,
        sink: PluginSink,
        producer: Option<TelemetryProducer>,
    ) -> Result<Self::Handle, String>;

    /// Reconcile a retained failed attempt after an external provider event.
    /// Refreshes received while an attempt is active do not start another one.
    fn refresh_bootstrap(&mut self, handle: &mut Self::Handle) -> Result<(), String>;

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle);

    fn complete_bootstrap(&mut self, handle: &mut Self::Handle);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SshBootstrapStream {
    Stdout,
    Stderr,
}

#[derive(Clone)]
pub(super) enum SshBootstrapMsg {
    Refresh,
    ChildExited {
        attempt: u64,
        result: Result<std::process::ExitStatus, String>,
    },
    OutputLine {
        attempt: u64,
        stream: SshBootstrapStream,
        line: String,
    },
    ReaderError {
        attempt: u64,
        stream: SshBootstrapStream,
        error: String,
    },
    ReaderClosed {
        attempt: u64,
        stream: SshBootstrapStream,
    },
    Complete,
    Stop,
}

struct SshOutputRelay {
    target: ActorAddress,
    attempt: u64,
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
                    attempt: self.attempt,
                    stream: map_process_stream(stream),
                    line,
                },
                ProcessStreamObservation::Error { stream, error } => SshBootstrapMsg::ReaderError {
                    attempt: self.attempt,
                    stream: map_process_stream(stream),
                    error,
                },
                ProcessStreamObservation::Closed { stream } => SshBootstrapMsg::ReaderClosed {
                    attempt: self.attempt,
                    stream: map_process_stream(stream),
                },
            },
        };
        let _ = ctx.send(self.target, message);
    }
}

fn map_process_stream(stream: ProcessStream) -> SshBootstrapStream {
    match stream {
        ProcessStream::Stdout => SshBootstrapStream::Stdout,
        ProcessStream::Stderr => SshBootstrapStream::Stderr,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum BootstrapPhase {
    Ready,
    Running,
    Draining,
    AwaitingRefresh,
    Finished,
}

impl BootstrapPhase {
    fn terminal(self) -> bool {
        self == Self::Finished
    }
}

pub(super) struct SshBootstrapActor {
    bridge: BootstrapTelemetryBridge,
    endpoint: SshEndpoint,
    ssh_identity: Option<PathBuf>,
    sender: ExternalSender,
    live_env: BootstrapEnvSource,
    blocking: BlockingWorkSender,
    child: Option<SupervisedChild>,
    reader_relay: Option<ActorAddress>,
    stdout_closed: bool,
    stderr_closed: bool,
    pending_exit: Option<Result<std::process::ExitStatus, String>>,
    attempt: u64,
    observation_class: Option<&'static str>,
    phase: BootstrapPhase,
    bootstrap_completed: bool,
    #[cfg(test)]
    disable_attempt_spawn: bool,
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
            blocking: engine.blocking_work_sender(),
            child: None,
            stdout_closed: true,
            reader_relay: None,
            stderr_closed: true,
            pending_exit: None,
            attempt: 1,
            observation_class: None,
            phase: BootstrapPhase::Ready,
            bootstrap_completed: false,
            #[cfg(test)]
            disable_attempt_spawn: false,
        }
    }

    fn accepts_output(&self, attempt: u64) -> bool {
        attempt == self.attempt
            && matches!(
                self.phase,
                BootstrapPhase::Running | BootstrapPhase::Draining
            )
    }

    fn finish(&mut self, ctx: &Ctx, result: Result<(), &str>) {
        if self.phase.terminal() {
            return;
        }
        if !self.bootstrap_completed
            && let Err(error) = result
        {
            self.bridge.observe_failed(error);
        }
        self.phase = BootstrapPhase::Finished;
        self.stop_child();
        self.stop_relay(ctx);
        ctx.stop_self();
    }

    fn complete(&mut self) {
        if self.phase.terminal() {
            return;
        }
        // Runtime readiness completes bootstrap, not the attached SSH lifetime.
        self.bootstrap_completed = true;
    }

    fn run_id(&self) -> u64 {
        self.bridge.spec().run_id
    }

    fn node_id(&self) -> u64 {
        self.bridge.spec().node_id
    }

    fn refresh(&mut self, ctx: &Ctx) {
        if self.phase != BootstrapPhase::AwaitingRefresh {
            return;
        }
        let Some(attempt) = self.attempt.checked_add(1) else {
            self.finish(ctx, Err("bootstrap attempt identity exhausted"));
            return;
        };
        self.attempt = attempt;
        self.phase = BootstrapPhase::Ready;
        self.start_attempt(ctx, attempt);
    }

    #[cfg(test)]
    pub(super) fn with_attempt_spawn_disabled(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self
    }

    #[cfg(test)]
    pub(super) fn with_open_test_streams(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self.stdout_closed = false;
        self.stderr_closed = false;
        self
    }

    fn start_attempt(&mut self, ctx: &Ctx, attempt: u64) {
        if attempt != self.attempt || self.phase != BootstrapPhase::Ready {
            return;
        }
        self.phase = BootstrapPhase::Running;
        self.pending_exit = None;
        self.observation_class = None;
        match ctx.spawn(SshOutputRelay {
            target: ctx.self_addr(),
            attempt,
        }) {
            Ok(relay) => self.reader_relay = Some(relay),
            Err(_) => {
                self.finish(ctx, Err("bootstrap output relay unavailable"));
                return;
            }
        }
        self.bridge.observe_provider_line(format!(
            "VastAI SSH bootstrap attempt {} to {}@{}:{}",
            self.attempt, self.endpoint.user, self.endpoint.host, self.endpoint.port
        ));
        #[cfg(test)]
        if self.disable_attempt_spawn {
            return;
        }
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
                &self.blocking,
                &mut command,
                None,
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
                self.pending_exit = None;
            }
            Err(error) => {
                self.bridge.observe_provider_line(format!(
                    "spawn VastAI SSH bootstrap attempt {} failed: {error}; awaiting provider refresh",
                    self.attempt
                ));
                self.await_refresh(ctx);
            }
        }
    }

    fn child_exited(
        &mut self,
        ctx: &Ctx,
        attempt: u64,
        result: Result<std::process::ExitStatus, String>,
    ) {
        if attempt != self.attempt || self.phase != BootstrapPhase::Running {
            return;
        }
        self.child = None;
        self.pending_exit = Some(result);
        self.phase = BootstrapPhase::Draining;
        self.maybe_finish_attempt(ctx);
    }

    fn handle_output_line(&mut self, attempt: u64, stream: SshBootstrapStream, line: String) {
        if !self.accepts_output(attempt) {
            return;
        }
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

    fn handle_reader_error(&self, attempt: u64, stream: SshBootstrapStream, error: String) {
        if !self.accepts_output(attempt) {
            return;
        }
        let stream = match stream {
            SshBootstrapStream::Stdout => "stdout",
            SshBootstrapStream::Stderr => "stderr",
        };
        self.bridge
            .observe_provider_line(format!("read VastAI SSH {stream}: {error}"));
    }

    fn handle_reader_closed(&mut self, ctx: &Ctx, attempt: u64, stream: SshBootstrapStream) {
        if !self.accepts_output(attempt) {
            return;
        }
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
                "class": observation_class,
                "classification": readiness,
            })
            .to_string(),
        );
        self.await_refresh(ctx);
    }

    fn maybe_finish_attempt(&mut self, ctx: &Ctx) {
        if self.phase != BootstrapPhase::Draining || !self.stdout_closed || !self.stderr_closed {
            return;
        }
        match self.pending_exit.take() {
            Some(Ok(status)) => {
                self.finish_completed_attempt(ctx, status.to_string(), status.success())
            }
            Some(Err(error)) => {
                self.bridge.observe_provider_line(format!(
                    "wait VastAI SSH bootstrap attempt {}: {error}; awaiting provider refresh",
                    self.attempt
                ));
                self.await_refresh(ctx);
            }
            None => {}
        }
    }

    fn await_refresh(&mut self, ctx: &Ctx) {
        if self.phase.terminal() {
            return;
        }
        self.stop_child();
        self.stop_relay(ctx);
        self.phase = BootstrapPhase::AwaitingRefresh;
        self.bridge.observe_provider_line(format!(
            "VastAI SSH bootstrap awaiting provider refresh after attempt {}",
            self.attempt
        ));
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

impl ActorInterface for SshBootstrapActor {
    type Incoming = SshBootstrapMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.start_attempt(ctx, self.attempt);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            SshBootstrapMsg::Refresh => self.refresh(ctx),
            SshBootstrapMsg::ChildExited { attempt, result } => {
                self.child_exited(ctx, attempt, result)
            }
            SshBootstrapMsg::OutputLine {
                attempt,
                stream,
                line,
            } => self.handle_output_line(attempt, stream, line),
            SshBootstrapMsg::ReaderError {
                attempt,
                stream,
                error,
            } => self.handle_reader_error(attempt, stream, error),
            SshBootstrapMsg::ReaderClosed { attempt, stream } => {
                self.handle_reader_closed(ctx, attempt, stream)
            }
            SshBootstrapMsg::Complete => self.complete(),
            SshBootstrapMsg::Stop => self.finish(ctx, Err("bootstrap cancelled")),
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.finish(ctx, Err("bootstrap owner stopped"));
        self.stop_child();
        self.stop_relay(ctx);
    }
}

pub struct SshBootstrapHandle {
    actor: ActorAddress,
    runtime: Runtime,
}

impl Drop for SshBootstrapHandle {
    fn drop(&mut self) {
        let _ = self.runtime.send_to(self.actor, SshBootstrapMsg::Stop);
    }
}

impl SshCommandBootstrapLauncher {
    pub fn new(
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
pub struct SshCommandBootstrapLauncher {
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

    fn refresh_bootstrap(&mut self, handle: &mut Self::Handle) -> Result<(), String> {
        handle
            .runtime
            .send_to(handle.actor, SshBootstrapMsg::Refresh)
            .map_err(|error| format!("refresh VastAI SSH bootstrap actor: {error}"))
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle) {
        let _ = handle.runtime.send_to(handle.actor, SshBootstrapMsg::Stop);
    }

    fn complete_bootstrap(&mut self, handle: &mut Self::Handle) {
        let _ = handle
            .runtime
            .send_to(handle.actor, SshBootstrapMsg::Complete);
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
    // Keep the lock inode stable. Kernel lock ownership, not a stale/recycled
    // PID or a self-retry loop, decides whether another attachment is active.
    format!(
        "lock={lock}; command={command}; \
         command -v flock >/dev/null 2>&1 || {{ echo 'MYELIN-TERMINAL:SUBSTRATE_LOST: flock is unavailable' >&2; exit 69; }}; \
         if [ -e \"$lock/pid\" ] && [ ! -e \"$lock/owner.lock\" ] && [ ! -f \"$lock/complete\" ]; then \
           echo 'MYELIN-TERMINAL:SUBSTRATE_LOST: legacy bootstrap ownership is unavailable' >&2; exit 69; \
         fi; \
         mkdir -p \"$lock\" || exit 1; \
         exec 9>>\"$lock/owner.lock\" || exit 1; \
         if flock -n -E 75 9; then :; else \
           status=$?; \
           if [ \"$status\" -ne 75 ]; then echo 'MYELIN-TERMINAL:SUBSTRATE_LOST: bootstrap lock failed' >&2; exit 69; fi; \
           echo 'myelin bootstrap already running'; exit 0; \
         fi; \
         if [ -f \"$lock/complete\" ]; then echo 'myelin bootstrap already complete'; exit 0; fi; \
         {export_command}sh -lc \"$command\"; status=$?; \
         if [ \"$status\" -eq 0 ]; then touch \"$lock/complete\" || exit 1; fi; \
         exit \"$status\""
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
        // Host trust belongs to the deployment identity, not the workstation.
        // This preserves key continuity across retained-node redeployments while
        // preventing an unrelated prior endpoint from poisoning a new fixture.
        let mut known_hosts = identity.as_os_str().to_os_string();
        known_hosts.push(".known_hosts");
        args.push("-o".to_owned());
        args.push(format!(
            "UserKnownHostsFile={}",
            PathBuf::from(known_hosts).display()
        ));
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

/// Deployment SSH bootstrap: verify reusable content or transfer/install it,
/// then fence previous incarnations and activate the requested generation.
pub struct SshArtifactBootstrapLauncher {
    ssh_identity: PathBuf,
    bundle: PathBuf,
    verified_bundle: Option<(Arc<[u8]>, DeploymentDescriptor, String)>,
    runtime: Runtime,
    live_env: BootstrapEnvSource,
    engine: EngineHandle,
}

impl SshArtifactBootstrapLauncher {
    pub fn new(
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
    ) -> Result<Self::Handle, String> {
        if spec.args.is_empty() {
            return Err(format!(
                "invalid_artifact: node {} artifact bootstrap requires a worker command",
                spec.node_id
            ));
        }
        let identity = spec.deployment.clone().ok_or_else(|| {
            format!(
                "node {} artifact bootstrap requires a deployment identity",
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

    fn refresh_bootstrap(&mut self, handle: &mut Self::Handle) -> Result<(), String> {
        handle
            .runtime
            .send_to(handle.actor, SshBootstrapMsg::Refresh)
            .map_err(|error| format!("refresh artifact SSH bootstrap actor: {error}"))
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle) {
        let _ = handle.runtime.send_to(handle.actor, SshBootstrapMsg::Stop);
    }

    fn complete_bootstrap(&mut self, handle: &mut Self::Handle) {
        let _ = handle
            .runtime
            .send_to(handle.actor, SshBootstrapMsg::Complete);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArtifactContentPhase {
    Discover,
    Transfer,
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
    blocking: BlockingWorkSender,
    child: Option<SupervisedChild>,
    reader_relay: Option<ActorAddress>,
    stdout_closed: bool,
    stderr_closed: bool,
    pending_exit: Option<Result<std::process::ExitStatus, String>>,
    receipt: Option<DeploymentReceipt>,
    receipt_error: Option<String>,
    transfer_required: bool,
    content_phase: ArtifactContentPhase,
    attempt: u64,
    observation_class: Option<&'static str>,
    phase: BootstrapPhase,
    #[cfg(test)]
    disable_attempt_spawn: bool,
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
            blocking: engine.blocking_work_sender(),
            child: None,
            stdout_closed: true,
            reader_relay: None,
            stderr_closed: true,
            pending_exit: None,
            receipt: None,
            receipt_error: None,
            transfer_required: false,
            content_phase: ArtifactContentPhase::Discover,
            attempt: 1,
            observation_class: None,
            phase: BootstrapPhase::Ready,
            #[cfg(test)]
            disable_attempt_spawn: false,
        }
    }

    fn accepts_output(&self, attempt: u64) -> bool {
        attempt == self.attempt
            && matches!(
                self.phase,
                BootstrapPhase::Running | BootstrapPhase::Draining
            )
    }

    fn finish(&mut self, ctx: &Ctx, result: Result<(), &str>) {
        if self.phase.terminal() {
            return;
        }
        if let Err(error) = result {
            self.bridge.observe_failed(error);
        }
        self.phase = BootstrapPhase::Finished;
        self.stop_child();
        self.stop_relay(ctx);
        ctx.stop_self();
    }

    fn run_id(&self) -> u64 {
        self.bridge.spec().run_id
    }

    fn node_id(&self) -> u64 {
        self.bridge.spec().node_id
    }

    fn refresh(&mut self, ctx: &Ctx) {
        if self.phase == BootstrapPhase::AwaitingRefresh {
            self.start_next_attempt(ctx);
        }
    }

    fn start_next_attempt(&mut self, ctx: &Ctx) {
        let Some(attempt) = self.attempt.checked_add(1) else {
            self.finish(ctx, Err("bootstrap attempt identity exhausted"));
            return;
        };
        self.stop_relay(ctx);
        self.attempt = attempt;
        self.phase = BootstrapPhase::Ready;
        self.start_attempt(ctx, attempt);
    }

    fn start_attempt(&mut self, ctx: &Ctx, attempt: u64) {
        if attempt != self.attempt || self.phase != BootstrapPhase::Ready {
            return;
        }
        self.phase = BootstrapPhase::Running;
        self.pending_exit = None;
        self.observation_class = None;
        self.receipt = None;
        self.receipt_error = None;
        self.transfer_required = false;
        match ctx.spawn(SshOutputRelay {
            target: ctx.self_addr(),
            attempt,
        }) {
            Ok(relay) => self.reader_relay = Some(relay),
            Err(_) => {
                self.finish(ctx, Err("bootstrap output relay unavailable"));
                return;
            }
        }
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
        #[cfg(test)]
        if self.disable_attempt_spawn {
            self.stdout_closed = false;
            self.stderr_closed = false;
            return;
        }
        let transfer = self.content_phase == ArtifactContentPhase::Transfer;
        let attempt = artifact_bootstrap_attempt_command(
            self.bridge.spec(),
            &self.live_env,
            &self.endpoint,
            &self.ssh_identity,
            &self.identity,
            &self.executable_digest,
            &self.bundle_digest,
            transfer,
        );
        let relay = self
            .reader_relay
            .expect("SSH output relay is installed before attempts start");
        let attempt = attempt.and_then(|mut command| {
            SupervisedChild::spawn(
                &self.blocking,
                &mut command,
                transfer.then(|| Arc::clone(&self.bundle)),
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
                self.pending_exit = None;
                self.receipt = None;
                self.receipt_error = None;
            }
            Err(error) => {
                self.bridge.observe_provider_line(format!(
                    "spawn artifact SSH bootstrap attempt {} failed: {error}; awaiting provider refresh",
                    self.attempt
                ));
                self.await_refresh(ctx);
            }
        }
    }

    fn child_exited(
        &mut self,
        ctx: &Ctx,
        attempt: u64,
        result: Result<std::process::ExitStatus, String>,
    ) {
        if attempt != self.attempt || self.phase != BootstrapPhase::Running {
            return;
        }
        self.child = None;
        self.pending_exit = Some(result);
        self.phase = BootstrapPhase::Draining;
        self.maybe_finish_attempt(ctx);
    }

    fn handle_output_line(&mut self, attempt: u64, stream: SshBootstrapStream, line: String) {
        if !self.accepts_output(attempt) {
            return;
        }
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
                if line == "MYELIN-CONTENT:TRANSFER_REQUIRED" {
                    self.transfer_required = true;
                }
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

    fn handle_reader_error(&self, attempt: u64, stream: SshBootstrapStream, error: String) {
        if !self.accepts_output(attempt) {
            return;
        }
        let stream = match stream {
            SshBootstrapStream::Stdout => "stdout",
            SshBootstrapStream::Stderr => "stderr",
        };
        self.bridge
            .observe_provider_line(format!("read artifact SSH {stream}: {error}"));
    }

    fn handle_reader_closed(&mut self, ctx: &Ctx, attempt: u64, stream: SshBootstrapStream) {
        if !self.accepts_output(attempt) {
            return;
        }
        match stream {
            SshBootstrapStream::Stdout => self.stdout_closed = true,
            SshBootstrapStream::Stderr => self.stderr_closed = true,
        }
        self.maybe_finish_attempt(ctx);
    }

    fn finish_dispatched(&mut self, ctx: &Ctx) {
        if self.phase.terminal() {
            return;
        }
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
                "artifact_digest": self.identity.artifact_digest,
                "deployment_generation": self.identity.deployment_generation,
                "receipt": receipt,
            })
            .to_string(),
        );
        self.finish(ctx, Ok(()));
    }

    fn finish_terminal(&mut self, ctx: &Ctx, class: &'static str, reason: String) {
        if self.phase.terminal() {
            return;
        }
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
        self.phase = BootstrapPhase::Finished;
        self.stop_child();
        self.stop_relay(ctx);
        ctx.stop_self();
    }

    fn maybe_finish_attempt(&mut self, ctx: &Ctx) {
        if self.phase != BootstrapPhase::Draining || !self.stdout_closed || !self.stderr_closed {
            return;
        }
        if self.pending_exit.is_none() {
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
        let status = match self
            .pending_exit
            .take()
            .expect("transport completion is retained")
        {
            Ok(status) => status,
            Err(error) => {
                self.bridge.observe_provider_line(format!(
                    "wait artifact SSH bootstrap attempt {}: {error}; awaiting provider refresh",
                    self.attempt
                ));
                self.await_refresh(ctx);
                return;
            }
        };
        if self.transfer_required
            && status.code() == Some(75)
            && self.content_phase == ArtifactContentPhase::Discover
        {
            // Discovery completed: dispatch the transfer phase once. Retain
            // that phase across external refreshes so a failed transfer never
            // returns to cache discovery and creates a content-miss cycle.
            self.content_phase = ArtifactContentPhase::Transfer;
            self.start_next_attempt(ctx);
            return;
        }
        // A clean SSH exit without the matching receipt is still only an
        // unobserved transaction outcome. Await an external event to retry the
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
                "class": class,
            })
            .to_string(),
        );
        self.await_refresh(ctx);
    }

    fn await_refresh(&mut self, ctx: &Ctx) {
        if self.phase.terminal() {
            return;
        }
        self.stop_child();
        self.stop_relay(ctx);
        self.phase = BootstrapPhase::AwaitingRefresh;
        self.bridge.observe_provider_line(format!(
            "artifact SSH bootstrap awaiting provider refresh after attempt {}",
            self.attempt
        ));
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
        self.start_attempt(ctx, self.attempt);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            SshBootstrapMsg::Refresh => self.refresh(ctx),
            SshBootstrapMsg::ChildExited { attempt, result } => {
                self.child_exited(ctx, attempt, result)
            }
            SshBootstrapMsg::OutputLine {
                attempt,
                stream,
                line,
            } => self.handle_output_line(attempt, stream, line),
            SshBootstrapMsg::ReaderError {
                attempt,
                stream,
                error,
            } => self.handle_reader_error(attempt, stream, error),
            SshBootstrapMsg::ReaderClosed { attempt, stream } => {
                self.handle_reader_closed(ctx, attempt, stream)
            }
            SshBootstrapMsg::Complete => self.finish(ctx, Ok(())),
            SshBootstrapMsg::Stop => self.finish(ctx, Err("bootstrap cancelled")),
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.finish(ctx, Err("bootstrap owner stopped"));
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
    transfer: bool,
) -> Result<Command, String> {
    let remote_command = artifact_remote_command(
        spec,
        live_env,
        identity,
        executable_digest,
        bundle_digest,
        transfer,
    )?;
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

// A generation owns a detached Linux subreaper, not just a worker PID. Its
// direct-child stop/exit events and orphan adoption close fork/setsid races.
// Unsupervised legacy trees cannot be adopted retroactively: fail closed rather
// than guessing from a /proc snapshot or requiring ptrace/cgroup privileges.
const ARTIFACT_PROCESS_OWNER: &str = r#"
import ctypes
import errno
import json
import os
from pathlib import Path
import select
import signal
import socket
import struct
import subprocess
import sys

action, directory, *arguments = sys.argv[1:]
root = Path(directory)
ledger = root / 'process-supervisor.json'
address = str(root / 'process-supervisor.sock')

def require(condition, reason):
    if not condition:
        raise RuntimeError(reason)

def stat(pid):
    try:
        text = Path('/proc', str(pid), 'stat').read_text()
    except (FileNotFoundError, ProcessLookupError):
        return None
    name, fields = text[text.index('(') + 1:].rsplit(')', 1)
    fields = fields.split()
    return (int(fields[19]), fields[0], int(fields[1]), name)

def alive(pid, ticks):
    current = stat(pid)
    return current is not None and current[0] == ticks and current[1] not in ('Z', 'X')

def identity(state):
    return {key: state[key] for key in ('pid', 'process_start_ticks', 'deployment_generation')}

def save(state):
    temporary = ledger.with_suffix('.next')
    with temporary.open('w') as stream:
        json.dump(state, stream)
        stream.flush()
        os.fsync(stream.fileno())
    os.replace(temporary, ledger)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)

def read_message(connection):
    data = bytearray()
    while b'\n' not in data:
        part = connection.recv(4096 - len(data))
        if not part:
            raise ConnectionError('process owner disconnected without completion')
        data.extend(part)
        require(len(data) < 4096, 'oversized process owner message')
    return json.loads(data)

def send_message(connection, message):
    connection.sendall(json.dumps(message).encode() + b'\n')

def wait_exit(fd):
    events = select.poll()
    events.register(fd, select.POLLIN)
    for _, flags in events.poll():
        require(flags & (select.POLLIN | select.POLLHUP), 'invalid process exit descriptor')

def discover_legacy():
    protected = set()
    parent = os.getpid()
    while parent > 1 and parent not in protected:
        protected.add(parent)
        current = stat(parent)
        if current is None:
            break
        parent = current[2]
    legacy = set()
    for path in Path('/proc').iterdir():
        if not path.name.isdecimal():
            continue
        pid = int(path.name)
        current = stat(pid)
        if pid in protected or current is None or current[1] in ('Z', 'X'):
            continue
        if current[3] in ('myelin-worker', 'swactor'):
            legacy.add((pid, current[0]))
            continue
        try:
            executable = os.readlink(path / 'exe').removesuffix(' (deleted)')
            if executable.rsplit('/', 1)[-1] in ('myelin-worker', 'swactor'):
                legacy.add((pid, current[0]))
                continue
            environment = (path / 'environ').read_bytes().split(b'\0')
        except OSError as error:
            if error.errno in (errno.ENOENT, errno.ESRCH, errno.EACCES, errno.EPERM):
                continue
            raise
        if (any(value.startswith(b'MYELIN_ARTIFACT_DIGEST=sha256:') for value in environment)
                and any(value.startswith(b'MYELIN_DEPLOYMENT_GENERATION=')
                        and value != b'MYELIN_DEPLOYMENT_GENERATION=' for value in environment)):
            legacy.add((pid, current[0]))
    previous = root / 'fenced-processes.json'
    if previous.exists():
        for pid, ticks in json.loads(previous.read_text()):
            if alive(pid, ticks):
                legacy.add((pid, ticks))
    return legacy

def fence():
    if ledger.exists():
        state = json.loads(ledger.read_text())
        owner = identity(state)
        require(type(owner['pid']) is int and owner['pid'] > 1
                and type(owner['process_start_ticks']) is int
                and owner['process_start_ticks'] > 0
                and isinstance(owner['deployment_generation'], str)
                and owner['deployment_generation'],
                'invalid process supervisor incarnation')
        current = stat(owner['pid'])
        same = current is not None and current[0] == owner['process_start_ticks']
        if state['phase'] != 'fenced' and not (
                same and current[1] not in ('Z', 'X')):
            state = json.loads(ledger.read_text())
            require(identity(state) == owner and state['phase'] == 'fenced',
                    'generation supervisor lost before descendant completion; manual fencing required')
        fd = None
        if same:
            try:
                fd = os.pidfd_open(owner['pid'])
            except ProcessLookupError:
                pass
            if fd is not None and not alive(owner['pid'], owner['process_start_ticks']):
                os.close(fd)
                fd = None
        try:
            if state['phase'] != 'fenced' and fd is None:
                state = json.loads(ledger.read_text())
                require(identity(state) == owner and state['phase'] == 'fenced',
                        'generation supervisor disappeared before fencing')
            if state['phase'] != 'fenced':
                try:
                    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as connection:
                        connection.connect(address)
                        peer = struct.unpack('3i', connection.getsockopt(
                            socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize('3i')))[0]
                        require(peer == owner['pid'], 'process supervisor socket incarnation mismatch')
                        send_message(connection, {'shutdown': owner})
                        require(read_message(connection) == {'fenced': owner},
                                'process supervisor did not prove descendant completion')
                except (FileNotFoundError, ConnectionError):
                    # The worker may have exited naturally before connect. Its
                    # durable completion seal, not socket absence, is authority.
                    completed = json.loads(ledger.read_text())
                    require(identity(completed) == owner and completed['phase'] == 'fenced',
                            'process supervisor unavailable before descendant completion')
            if fd is not None:
                wait_exit(fd)
            completed = json.loads(ledger.read_text())
            require(identity(completed) == owner and completed['phase'] == 'fenced',
                    'generation ended without descendant completion')
        finally:
            if fd is not None:
                os.close(fd)
    else:
        require(not any((root / name).exists() for name in (
                    'agent.pid', 'active-deployment.json', 'fenced-processes.json')),
                'legacy deployment has no descendant ownership proof; manual fencing required')
    legacy = discover_legacy()
    require(not legacy,
            'unsupported unsupervised legacy process incarnations %r; manual fencing required' %
            sorted(legacy))
    # Keep the completion seal across interrupted installation/activation.
    # Only the next owned generation replaces it with a fresh incarnation.
    Path(address).unlink(missing_ok=True)
    (root / 'fenced-processes.json').unlink(missing_ok=True)
    print(json.dumps({'type': 'MyelinProcessFenceCompleted'}), file=sys.stderr)

def shutdown_children():
    # Killing each stopped direct child reparents its complete surviving tree
    # here, including children forked by nonleader threads or detached sessions.
    # Every iteration consumes a real child stop/exit; ECHILD is the final proof.
    children = Path('/proc', str(os.getpid()), 'task', str(os.getpid()), 'children')
    while True:
        pending = [int(pid) for pid in children.read_text().split()]
        if not pending:
            try:
                os.waitpid(-1, 0)
            except ChildProcessError:
                return
            continue
        for pid in pending:
            try:
                fd = os.pidfd_open(pid)
            except ProcessLookupError:
                continue
            try:
                current = stat(pid)
                if current is None or current[2] != os.getpid():
                    continue
                try:
                    signal.pidfd_send_signal(fd, signal.SIGSTOP)
                except ProcessLookupError:
                    pass
                try:
                    _, status = os.waitpid(pid, os.WUNTRACED)
                except ChildProcessError:
                    continue
                if os.WIFSTOPPED(status):
                    try:
                        signal.pidfd_send_signal(fd, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    os.waitpid(pid, 0)
            finally:
                os.close(fd)

class Cancelled(Exception):
    pass

class LaunchIncomplete(Exception):
    pass

def cancelled(signum, frame):
    raise Cancelled('process owner cancelled by signal %s' % signum)

def supervise(parent, generation, command, log_path, lock_fd):
    os.setsid()
    # Only this detached owner drops the foreground transaction lock. The
    # launch client keeps it until the worker's exec acknowledgement arrives.
    os.close(lock_fd)
    with open('/dev/null', 'rb') as stream:
        os.dup2(stream.fileno(), 0)
    with open(log_path, 'ab', buffering=0) as stream:
        os.dup2(stream.fileno(), 1)
        os.dup2(stream.fileno(), 2)
    for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
        signal.signal(signum, cancelled)
    libc = ctypes.CDLL(None, use_errno=True)
    require(libc.prctl(36, 1, 0, 0, 0) == 0,
            'PR_SET_CHILD_SUBREAPER unavailable: errno %s' % ctypes.get_errno())
    state = {'pid': os.getpid(), 'process_start_ticks': stat(os.getpid())[0],
             'deployment_generation': generation, 'phase': 'active'}
    owner = identity(state)
    wake_read, wake_write = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
    signal.signal(signal.SIGCHLD, lambda signum, frame: None)
    signal.set_wakeup_fd(wake_write)
    listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    ready_read = ready_write = None
    response = None
    try:
        listener.bind(address)
        os.chmod(address, 0o600)
        listener.listen()
        save(state)
        ready_read, ready_write = os.pipe2(os.O_CLOEXEC)
        environment = dict(os.environ, MYELIN_BOOTSTRAP_READY_FD=str(ready_write))
        # Popen owns the CLOEXEC exec-error pipe. The inherited worker pipe
        # separately proves final worker exec rather than merely shell exec.
        worker = subprocess.Popen(['sh', '-c', command], stdin=subprocess.DEVNULL,
                                  stdout=1, stderr=2, env=environment,
                                  pass_fds=(ready_write,), start_new_session=True)
        os.close(ready_write)
        ready_write = None
        worker_pid = worker.pid
        current = stat(worker_pid)
        if current is None:
            raise LaunchIncomplete('worker exited before its incarnation was captured')
        expected = {'pid': worker_pid, 'process_start_ticks': current[0],
                    'deployment_generation': generation}
        state['worker'] = expected
        save(state)
        (root / 'agent.pid').write_text(str(worker_pid))
        events = select.poll()
        events.register(ready_read, select.POLLIN | select.POLLHUP)
        events.register(wake_read, select.POLLIN)
        events.register(parent.fileno(), select.POLLIN | select.POLLHUP)
        received = bytearray()
        ready = False
        while not ready:
            for fd, flags in events.poll():
                if fd == parent.fileno():
                    raise Cancelled('launch client disconnected before worker exec proof')
                if fd == wake_read:
                    os.read(wake_read, 4096)
                    if not alive(worker_pid, expected['process_start_ticks']):
                        raise LaunchIncomplete('worker exited before exec acknowledgement')
                if fd == ready_read:
                    part = os.read(ready_read, 4096 - len(received))
                    if not part:
                        raise LaunchIncomplete('worker closed exec acknowledgement without a receipt')
                    received.extend(part)
                    require(len(received) < 4096, 'oversized worker exec acknowledgement')
                    if b'\n' in received:
                        require(json.loads(received) == expected,
                                'worker exec acknowledgement incarnation mismatch')
                        if not alive(worker_pid, expected['process_start_ticks']):
                            raise LaunchIncomplete('worker exited during exec acknowledgement')
                        ready = True
        os.close(ready_read)
        ready_read = None
        send_message(parent, {'ready': expected})
        require(read_message(parent) == {'accepted': expected},
                'launch client did not accept worker incarnation')
        parent.close()
        events = select.poll()
        events.register(listener.fileno(), select.POLLIN)
        events.register(wake_read, select.POLLIN)
        running = True
        while running:
            for fd, flags in events.poll():
                if fd == listener.fileno():
                    connection, _ = listener.accept()
                    try:
                        require(read_message(connection) == {'shutdown': owner},
                                'shutdown requested for a different supervisor incarnation')
                    except BaseException:
                        connection.close()
                        raise
                    response = connection
                    running = False
                    break
                if fd == wake_read:
                    os.read(wake_read, 4096)
                    # Drain only exit events already delivered by SIGCHLD.
                    while True:
                        try:
                            pid, _ = os.waitpid(-1, os.WNOHANG)
                        except ChildProcessError:
                            running = False
                            break
                        if pid == 0:
                            break
                        if pid == worker_pid:
                            running = False
    except BaseException as error:
        try:
            send_message(parent, {'error': str(error),
                                  'retryable': isinstance(error, (Cancelled, LaunchIncomplete))})
        except OSError:
            pass
    finally:
        # Cancellation cannot interrupt the owned drain and leave frozen
        # children. No deadline substitutes for a kernel child completion.
        for signum in (signal.SIGTERM, signal.SIGINT, signal.SIGHUP):
            signal.signal(signum, signal.SIG_IGN)
        shutdown_children()
        state['phase'] = 'fenced'
        save(state)
        if response is not None:
            try:
                send_message(response, {'fenced': owner})
            finally:
                response.close()
        Path(address).unlink(missing_ok=True)
        listener.close()
        parent.close()
        signal.set_wakeup_fd(-1)
        for fd in (ready_read, ready_write, wake_read, wake_write):
            if fd is not None:
                os.close(fd)

def launch(generation, command, log_path, lock_fd):
    lock_fd = int(lock_fd)
    os.fstat(lock_fd)
    if ledger.exists():
        previous = json.loads(ledger.read_text())
        require(previous['phase'] == 'fenced'
                and not alive(previous['pid'], previous['process_start_ticks']),
                'previous generation has not completed fencing')
    require(not Path(address).exists(), 'previous process supervisor socket is still owned')
    client, owner = socket.socketpair()
    pid = os.fork()
    if pid == 0:
        client.close()
        try:
            supervise(owner, generation, command, log_path, lock_fd)
        except BaseException as error:
            try:
                send_message(owner, {'error': str(error)})
            except OSError:
                pass
            os._exit(69)
        os._exit(0)
    owner.close()
    accepted = False
    try:
        message = read_message(client)
        if message.get('retryable'):
            raise LaunchIncomplete(message['error'])
        require('ready' in message, message.get('error', 'process owner omitted worker receipt'))
        worker = message['ready']
        send_message(client, {'accepted': worker})
        accepted = True
        print('%s:%s' % (worker['pid'], worker['process_start_ticks']))
    finally:
        client.close()
        if not accepted:
            os.waitpid(pid, 0)

try:
    require(hasattr(os, 'pidfd_open') and hasattr(signal, 'pidfd_send_signal'),
            'Python/Linux pidfd support is unavailable')
    if action == 'fence':
        fence()
    elif action == 'launch':
        launch(*arguments)
    else:
        raise RuntimeError('unknown process owner action')
except LaunchIncomplete as error:
    print('MYELIN-RETRY: worker launch incomplete: %s' % error, file=sys.stderr)
    sys.exit(75)
except Exception as error:
    print('MYELIN-TERMINAL:SUBSTRATE_LOST: process ownership failed: %s' % error,
          file=sys.stderr)
    sys.exit(69)
"#;

// A completion seal is written only after the source archive and installed wheel
// files have been verified. Every reuse rehashes the archive and the entire tree;
// neither a directory name, an mtime, nor the existence of the seal is evidence.
const ARTIFACT_INSTALLATION: &str = r#"
import hashlib, json, os, stat, sys, tarfile, zipfile
from pathlib import Path, PurePosixPath

action, directory, artifact, executable, *environment = sys.argv[1:]
root = Path(directory)

def require(condition, message):
    if not condition:
        raise ValueError(message)

def digest_file(path):
    require(path.is_file() and not path.is_symlink(), 'not a regular file: ' + str(path))
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()

def safe_name(name):
    path = PurePosixPath(name.rstrip('/'))
    require(str(path) == name.rstrip('/') and not path.is_absolute() and
            all(part not in ('', '.', '..') for part in path.parts), 'ambiguous archive path')
    return path

def tree(directory):
    require(directory.is_dir() and not directory.is_symlink(), 'missing content directory')
    result = {}
    for path in sorted(directory.rglob('*')):
        mode = path.lstat().st_mode
        require(stat.S_ISDIR(mode) or stat.S_ISREG(mode), 'unsupported installed file')
        name = path.relative_to(directory).as_posix()
        result[name] = (['directory', stat.S_IMODE(mode)] if stat.S_ISDIR(mode) else
                        ['file', stat.S_IMODE(mode), digest_file(path)])
    return result

require(root.is_dir() and not root.is_symlink(), 'missing release')
archive = root / '.payload.tar'
require(digest_file(archive) == artifact, 'cached archive digest mismatch')
expected = {}
with tarfile.open(archive, 'r:gz') as source:
    members = source.getmembers()
    for member in members:
        path = safe_name(member.name)
        require(path.parts[0] in ('bin', 'python', 'support'), 'unknown payload root')
        require(member.isdir() or member.isfile(), 'unsupported payload member')
        name = path.as_posix()
        require(name not in expected, 'duplicate payload member')
        if member.isdir():
            expected[name] = ['directory', member.mode & 0o777]
        else:
            with source.extractfile(member) as stream:
                expected[name] = ['file', member.mode & 0o777,
                                  hashlib.file_digest(stream, 'sha256').hexdigest()]
    require('bin/myelin-worker' in expected and
            expected['bin/myelin-worker'][2] == executable, 'worker digest mismatch')
    if action == 'unpack':
        source.extractall(root, members=members, filter='data')
        for member in sorted(members, key=lambda item: len(PurePosixPath(item.name).parts), reverse=True):
            os.chmod(root / member.name, member.mode & 0o777)
        sys.exit(0)

actual = tree(root)
payload = {name: value for name, value in actual.items()
           if name.split('/')[0] in ('bin', 'python', 'support')}
require(payload == expected, 'installed payload differs from verified archive')
require(set(actual) - set(payload) - {'.payload.tar', '.installation.json', 'site-packages'} ==
        {name for name in actual if name.startswith('site-packages/')},
        'unexpected release content')
python_inputs = {
    'schema_version': 2,
    'interpreter_digest': digest_file(Path(sys.executable).resolve()),
    'interpreter_version': sys.version,
    'wheels': {wheel.name: digest_file(wheel) for wheel in sorted((root / 'python').glob('*.whl'))},
}
if action == 'python-key':
    print(hashlib.sha256(json.dumps(python_inputs, sort_keys=True, separators=(',', ':')).encode()).hexdigest())
    sys.exit(0)
python_only = action in ('seal-python', 'verify-python')
state_root = Path(environment[0]) if python_only and len(environment) == 1 else root
require(len(environment) == int(python_only), 'missing or unexpected Python cache path')
if python_only:
    contents = tree(state_root)
    require(set(contents) - {'site-packages', '.installation.json'} ==
            {name for name in contents if name.startswith('site-packages/')},
            'unexpected Python installation content')
installed = tree(state_root / 'site-packages')
# Verify the installation against the original wheel members as well as the
# sealed tree. pip may rewrite RECORD and generate installer metadata, but it
# may not omit or change a package, native extension, or support-data member.
owned = set()
for wheel in sorted((root / 'python').glob('*.whl')):
    with zipfile.ZipFile(wheel) as source:
        for member in source.infolist():
            path = safe_name(member.filename)
            if member.is_dir():
                continue
            require(not stat.S_ISLNK(member.external_attr >> 16), 'wheel symlink')
            parts = path.parts
            if parts[0].endswith('.data'):
                require(len(parts) > 2 and parts[1] in ('purelib', 'platlib'),
                        'ambiguous wheel installation scheme')
                name = PurePosixPath(*parts[2:]).as_posix()
            else:
                name = path.as_posix()
            require(name not in owned, 'overlapping wheel installations')
            owned.add(name)
            require(name in installed and installed[name][0] == 'file',
                    'missing wheel member: ' + name)
            if name.endswith('.dist-info/RECORD'):
                continue
            with source.open(member) as stream:
                digest = hashlib.file_digest(stream, 'sha256').hexdigest()
            require(name in installed and installed[name][0] == 'file' and
                    installed[name][2] == digest, 'incomplete wheel installation: ' + name)

identity = dict(python_inputs, files=installed,
                site_mode=stat.S_IMODE((state_root / 'site-packages').stat().st_mode))
if not python_only:
    identity.update(artifact_digest=artifact, executable_digest=executable)
seal = state_root / '.installation.json'
if action in ('seal', 'seal-python'):
    require(not seal.exists(), 'installation already sealed')
    # Flush installed bytes before the single completion record. An interrupted
    # install cannot become reusable just because its directory was renamed.
    for path in state_root.rglob('*'):
        if path.is_file():
            with path.open('rb') as stream:
                os.fsync(stream.fileno())
    with seal.open('x') as stream:
        json.dump(identity, stream, sort_keys=True, separators=(',', ':'))
        stream.flush()
        os.fsync(stream.fileno())
    for directory in [path for path in state_root.rglob('*') if path.is_dir()] + [state_root]:
        fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
elif action in ('verify', 'verify-python'):
    require(not seal.is_symlink(), 'installation seal is a symlink')
    with seal.open() as stream:
        require(json.load(stream) == identity, 'installation seal mismatch')
else:
    raise ValueError('unknown installation action')
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
    transfer: bool,
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
    let process_owner = shell_single_quote(ARTIFACT_PROCESS_OWNER);
    let installation = shell_single_quote(ARTIFACT_INSTALLATION);
    let transfer = u8::from(transfer);
    let transaction = format!(
        r#"set -eu
root=/opt/myelin; bundle_digest={bundle}; digest={digest}; exec_digest={executable}
gen={generation}; expected={expected_descriptor}; transfer={transfer}
installation={installation}
process_owner={process_owner}
export PYTHONDONTWRITEBYTECODE=1
invalid() {{ echo "MYELIN-TERMINAL:INVALID_ARTIFACT: $1" >&2; exit 65; }}
substrate() {{ echo "MYELIN-TERMINAL:SUBSTRATE_LOST: $1" >&2; exit 69; }}
mkdir -p "$root" || substrate 'cannot create deployment root'
for tool in flock python3 tar gzip cmp sha256sum cp pgrep; do
    command -v "$tool" >/dev/null 2>&1 || substrate "$tool is unavailable"
done
exec 9>>"$root/deployment.lock" || substrate 'cannot open deployment lock'
flock -x 9 || substrate 'cannot lock deployment'
# Never unlink this lock inode. A lost SSH client does not end the remote
# transaction; foreground descendants retain the lock until their work ends.
stage="$root/staging/$gen.$$"; tx="$root/transactions/$gen"
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
release="$root/releases/$digest"; candidate="$stage/release"
if [ "$transfer" -eq 1 ]; then
    cat > "$stage/bundle.tar" || substrate 'artifact transfer failed'
    printf '%s  %s\n' "$bundle_digest" "$stage/bundle.tar" |
        sha256sum -c - >/dev/null ||
        {{ echo 'MYELIN-RETRY: artifact transfer incomplete' >&2; exit 75; }}
    tar -xf "$stage/bundle.tar" -C "$stage" || invalid 'bundle extraction failed'
    printf '%s' "$expected" | cmp -s - "$stage/deployment.json" ||
        invalid 'deployment descriptor mismatch'
    printf '%s  %s\n' "$digest" "$stage/payload.tar" |
        sha256sum -c - >/dev/null || invalid 'payload digest mismatch'
fi
reused=0
if python3 -c "$installation" verify "$release" "$digest" "$exec_digest" >/dev/null 2>&1; then
    reused=1
elif [ "$transfer" -eq 0 ]; then
    echo 'MYELIN-CONTENT:TRANSFER_REQUIRED' >&2
    exit 75
fi
# A cache hit is only content evidence. Every generation still fences and
# launches a fresh process, including interrupted or duplicate activations.
python3 -c "$process_owner" fence "$root" || substrate 'stale process fencing failed'
if [ "$reused" -eq 0 ]; then
    mkdir -p "$candidate" || substrate 'cannot stage release'
    mv "$stage/payload.tar" "$candidate/.payload.tar" ||
        substrate 'cannot retain verified payload'
    python3 -c "$installation" unpack "$candidate" "$digest" "$exec_digest" ||
        invalid 'payload extraction failed'
    python_key=$(python3 -c "$installation" python-key "$candidate" "$digest" "$exec_digest") ||
        invalid 'cannot identify Python environment'
    environment="$root/python-environments/$python_key"
    python_reused=1
    if ! python3 -c "$installation" verify-python "$candidate" "$digest" "$exec_digest" \
        "$environment" >/dev/null 2>&1; then
        python_reused=0
        python_candidate="$stage/python-environment"
        mkdir -p "$python_candidate/site-packages" "$root/python-environments" ||
            substrate 'cannot stage Python installation'
        set -- "$candidate"/python/*.whl
        if [ -f "$1" ]; then
            python3 -m pip install --no-index --no-deps --no-compile \
                --target "$python_candidate/site-packages" "$@" >/dev/null 2>&1 ||
                substrate 'swactor wheel installation failed'
        fi
        python3 -c "$installation" seal-python "$candidate" "$digest" "$exec_digest" \
            "$python_candidate" || invalid 'Python installation verification failed'
        rm -rf "$environment" || substrate 'cannot remove corrupt Python environment'
        mv "$python_candidate" "$environment" || substrate 'cannot commit Python environment'
    fi
    cp -a "$environment/site-packages" "$candidate/site-packages" ||
        substrate 'cannot stage verified Python environment'
    printf '{{"type":"MyelinPythonContent","content_digest":"sha256:%s","reused":%s}}\n' \
        "$python_key" "$python_reused"
    python3 -c "$installation" seal "$candidate" "$digest" "$exec_digest" ||
        invalid 'installed content verification failed'
    rm -rf "$release" || substrate 'cannot remove corrupt release'
    mv "$candidate" "$release" || substrate 'cannot commit verified installation'
fi
find "$root/releases" -maxdepth 1 -type d -name '*.staged.*' -exec rm -rf {{}} + \
    2>/dev/null || true
printf '{{"type":"MyelinDeploymentContent","artifact_digest":"sha256:%s","deployment_generation":"%s","reused":%s,"transferred":%s}}\n' \
    "$digest" "$gen" "$reused" "$transfer"
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
python3 -c "$installation" verify "$release" "$digest" "$exec_digest" ||
    invalid 'installed content changed before activation'
export {exports}
export PYTHONDONTWRITEBYTECODE=1
export PYTHONPATH="$release/site-packages${{PYTHONPATH:+:$PYTHONPATH}}"
export PATH="$release/bin:$PATH"
# Preserve the verified release PATH; login profiles must not replace it.
# The generation subreaper receives a real inherited-descriptor worker exec
# acknowledgement. It closes the transaction lock only in the detached owner.
process_identity=$(python3 -c "$process_owner" launch "$root" "$gen" {command} \
    /var/log/myelin-node.log 9) ||
    {{ code=$?; [ "$code" -eq 75 ] && exit 75; substrate 'worker process ownership failed'; }}
pid=${{process_identity%%:*}}; start_ticks=${{process_identity#*:}}
printf '%s' "$expected" > "$root/active-deployment.json" ||
    substrate 'cannot record active deployment'
# SSH loss may outlive the launched process. Revalidate before emitting the receipt.
workers=0
for candidate in $(pgrep -x myelin-worker 2>/dev/null || true); do
    if process_start "$candidate" >/dev/null; then
        [ "$candidate" = "$pid" ] || exit 75
        workers=$((workers+1))
    fi
done
[ "$workers" -eq 1 ] || exit 75
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
    Ok(format!("sh -lc {}", shell_single_quote(&transaction)))
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
    let mut decoded_payload = Vec::new();
    flate2::read::GzDecoder::new(payload)
        .read_to_end(&mut decoded_payload)
        .map_err(|error| format!("decompress payload.tar: {error}"))?;
    let executable = read_tar_member_bytes(&decoded_payload, "payload.tar", "bin/myelin-worker")?;
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

    #[test]
    fn generation_owner_fences_detached_thread_descendants_and_rejects_legacy_workers() {
        let directory = tempfile::tempdir().unwrap();
        let driver = r#"
import fcntl, json, os, pathlib, select, shlex, signal, subprocess, sys
root = pathlib.Path(sys.argv[1])
owner = sys.argv[2]
generation = 'owned-test'
fixture = root / 'worker.py'
fixture.write_text(r'''
import json, os, pathlib, signal, sys, threading
read_fd, write_fd = os.pipe()
os.set_inheritable(write_fd, True)
descendant = r"""
import json, os, pathlib, signal, sys
fd = int(sys.argv[1])
os.fork()
pid = os.getpid()
ticks = int(pathlib.Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[19])
os.write(fd, json.dumps([pid, ticks]).encode() + b'\n')
os.close(fd)
while True:
    signal.pause()
"""
def fork_from_thread():
    if os.fork() == 0:
        os.setsid()
        # Exec without deployment markers: ancestry ownership, not environment
        # discovery, must account for this detached subtree.
        os.execve(sys.executable, [sys.executable, '-c', descendant, str(write_fd)], {})
thread = threading.Thread(target=fork_from_thread)
thread.start()
thread.join()
os.close(write_fd)
with os.fdopen(read_fd) as records:
    children = [json.loads(records.readline()), json.loads(records.readline())]
pathlib.Path(sys.argv[1]).write_text(json.dumps(children))
pid = os.getpid()
ticks = int(pathlib.Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[19])
ready_fd = int(os.environ['MYELIN_BOOTSTRAP_READY_FD'])
os.write(ready_fd, json.dumps({'pid': pid, 'process_start_ticks': ticks,
         'deployment_generation': os.environ['MYELIN_DEPLOYMENT_GENERATION']}).encode() + b'\n')
os.close(ready_fd)
while True:
    signal.pause()
''')
environment = dict(os.environ, MYELIN_ARTIFACT_DIGEST='sha256:' + 'a' * 64,
                   MYELIN_DEPLOYMENT_GENERATION=generation)
command = 'exec ' + shlex.join([sys.executable, str(fixture), str(root / 'children.json')])
lock = (root / 'deployment.lock').open('a')
fcntl.flock(lock, fcntl.LOCK_EX)
fds = []
try:
    started = subprocess.run([sys.executable, '-c', owner, 'launch', str(root),
                              generation, command, str(root / 'worker.log'), str(lock.fileno())],
                             env=environment, pass_fds=(lock.fileno(),), capture_output=True)
    assert started.returncode == 0, started.stderr.decode()
    worker_pid, worker_ticks = map(int, started.stdout.decode().strip().split(':'))
    state = json.loads((root / 'process-supervisor.json').read_text())
    incarnations = [[state['pid'], state['process_start_ticks']], [worker_pid, worker_ticks]]
    incarnations.extend(json.loads((root / 'children.json').read_text()))
    for pid, ticks in incarnations:
        fd = os.pidfd_open(pid)
        fds.append(fd)
        actual = int(pathlib.Path('/proc', str(pid), 'stat').read_text().rsplit(')', 1)[1].split()[19])
        assert actual == ticks
    fenced = subprocess.run([sys.executable, '-c', owner, 'fence', str(root)], capture_output=True)
    assert fenced.returncode == 0, fenced.stderr.decode()
    exits = select.poll()
    for fd in fds:
        exits.register(fd, select.POLLIN)
    assert {fd for fd, flags in exits.poll() if flags & (select.POLLIN | select.POLLHUP)} == set(fds)
    repeated = subprocess.run([sys.executable, '-c', owner, 'fence', str(root)], capture_output=True)
    assert repeated.returncode == 0, repeated.stderr.decode()
finally:
    for fd in fds:
        try:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
        except ProcessLookupError:
            pass
        os.close(fd)
    lock.close()

# A pre-supervision generation is never "fenced" by killing only a discovered
# leader. Reject it without signalling it or producing a completion receipt.
read_fd, write_fd = os.pipe()
legacy = subprocess.Popen([sys.executable, '-c',
    'import os, signal, sys; os.write(int(sys.argv[1]), b"ready"); signal.pause()',
    str(write_fd)], env=environment, pass_fds=(write_fd,))
os.close(write_fd)
try:
    assert os.read(read_fd, 5) == b'ready'
    rejected = subprocess.run([sys.executable, '-c', owner, 'fence', str(root)], capture_output=True)
    assert rejected.returncode == 69
    assert b'unsupported unsupervised legacy process incarnations' in rejected.stderr
    os.kill(legacy.pid, 0)
finally:
    os.close(read_fd)
    legacy.kill()
    legacy.wait()
"#;
        let output = swactor_process::command_output(
            Command::new("python3")
                .args(["-c", driver])
                .arg(directory.path())
                .arg(ARTIFACT_PROCESS_OWNER),
        )
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn installed_content_reuse_rejects_partial_corrupt_and_ambiguous_trees() {
        let directory = tempfile::tempdir().unwrap();
        let driver = r#"
import hashlib, pathlib, shutil, sys, tarfile, zipfile
base = pathlib.Path(sys.argv[1])
installer = sys.argv[2]
source = base / 'source'
release = base / 'release'
for name in ('bin', 'python', 'support'):
    (source / name).mkdir(parents=True)
release.mkdir()
(source / 'bin/myelin-worker').write_bytes(b'worker')
(source / 'bin/myelin-worker').chmod(0o755)
(source / 'support/runtime.dat').write_bytes(b'support')
wheel = source / 'python/example-1-py3-none-any.whl'
with zipfile.ZipFile(wheel, 'w') as archive:
    archive.writestr('example/__init__.py', b'value = 1\n')
    archive.writestr('example-1.dist-info/RECORD', b'')
archive = release / '.payload.tar'
with tarfile.open(archive, 'w:gz') as output:
    for name in ('bin', 'python', 'support'):
        output.add(source / name, arcname=name)
artifact = hashlib.sha256(archive.read_bytes()).hexdigest()
executable = hashlib.sha256(b'worker').hexdigest()
def invoke(action, environment=None):
    sys.argv = ['installation', action, str(release), artifact, executable]
    if environment is not None:
        sys.argv.append(str(environment))
    try:
        exec(installer, {})
    except SystemExit as error:
        assert error.code == 0
def rejected(action, environment=None):
    try:
        invoke(action, environment)
    except (ValueError, OSError, KeyError):
        return
    raise AssertionError('accepted incomplete or corrupt installation: ' + action)
invoke('unpack')
(release / 'site-packages').mkdir()
with zipfile.ZipFile(wheel) as archive:
    archive.extractall(release / 'site-packages')
package = release / 'site-packages/example/__init__.py'
package.unlink()
rejected('seal')
assert not (release / '.installation.json').exists()
package.write_bytes(b'value = 1\n')
rejected('verify')
invoke('seal')
invoke('verify')
invoke('verify')
for path in [package, release / 'support/runtime.dat', release / 'bin/myelin-worker']:
    original = path.read_bytes()
    path.write_bytes(b'corrupt')
    rejected('verify')
    path.write_bytes(original)
    invoke('verify')
extra = release / 'site-packages/injected.pth'
extra.write_bytes(b'import os')
rejected('verify')
extra.unlink()
seal = release / '.installation.json'
original = seal.read_bytes()
seal.write_bytes(b'{}')
rejected('verify')
seal.write_bytes(original)
invoke('verify')
environment = base / 'python-environment'
shutil.copytree(release / 'site-packages', environment / 'site-packages')
invoke('seal-python', environment)
invoke('verify-python', environment)
# Changing the worker bytes must not invalidate an unchanged Python install.
next_release = base / 'next-release'
shutil.copytree(release, next_release)
release = next_release
(release / '.installation.json').unlink()
(source / 'bin/myelin-worker').write_bytes(b'next-worker')
(release / 'bin/myelin-worker').write_bytes(b'next-worker')
archive = release / '.payload.tar'
with tarfile.open(archive, 'w:gz') as output:
    for name in ('bin', 'python', 'support'):
        output.add(source / name, arcname=name)
artifact = hashlib.sha256(archive.read_bytes()).hexdigest()
executable = hashlib.sha256(b'next-worker').hexdigest()
invoke('verify-python', environment)
invoke('seal')
invoke('verify')
(environment / 'site-packages/example/__init__.py').write_bytes(b'corrupt-cache')
rejected('verify-python', environment)
invoke('verify')
archive = release / '.payload.tar'
archive.write_bytes(b'incomplete')
rejected('verify')
"#;
        let output = swactor_process::command_output(
            Command::new("python3")
                .args(["-c", driver])
                .arg(directory.path())
                .arg(ARTIFACT_INSTALLATION),
        )
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

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
        use std::io::Write as _;

        let executable = b"worker";
        let mut uncompressed_payload = ustar_member("bin/myelin-worker", executable);
        uncompressed_payload.extend([0_u8; 1024]);
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        encoder.write_all(&uncompressed_payload).unwrap();
        let payload = encoder.finish().unwrap();
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

    #[derive(Default)]
    struct BootstrapRecordingSink(parking_lot::Mutex<Vec<crate::provisioning::PluginObservation>>);

    impl crate::provisioning::PluginObservationSink for BootstrapRecordingSink {
        fn observe(&self, observation: crate::provisioning::PluginObservation) {
            self.0.lock().push(observation);
        }
    }

    struct ArtifactHarness {
        runtime: Runtime,
        backend: swactor_engine::SteppingBackend,
        _engine: swactor_engine::Engine,
        actor: ActorAddress,
        recording: Arc<BootstrapRecordingSink>,
    }

    impl ArtifactHarness {
        fn send(&self, message: SshBootstrapMsg) {
            self.runtime.send_to(self.actor, message).unwrap();
            crate::tests::fuzz_support::drive_steps(&self.backend, 8);
        }

        fn observations(&self) -> Vec<crate::provisioning::PluginObservation> {
            self.recording.0.lock().clone()
        }

        fn attempts(&self) -> Vec<u64> {
            self.observations()
                .iter()
                .filter_map(|observation| match observation {
                    crate::provisioning::PluginObservation::ProviderLine { line, .. } => {
                        let event: serde_json::Value = serde_json::from_str(line).ok()?;
                        (event["type"] == "MyelinArtifactBootstrapAttempt")
                            .then(|| event["attempt"].as_u64())
                            .flatten()
                    }
                    _ => None,
                })
                .collect()
        }

        fn close_streams(&self, attempt: u64) {
            for stream in [SshBootstrapStream::Stdout, SshBootstrapStream::Stderr] {
                self.send(SshBootstrapMsg::ReaderClosed { attempt, stream });
            }
        }
    }

    fn artifact_harness() -> ArtifactHarness {
        use swactor::runtime::{RuntimeConfig, RuntimeParts};
        use swactor_engine::{Engine, SteppingBackend};

        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).unwrap();
        let recording = Arc::new(BootstrapRecordingSink::default());
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
        actor.disable_attempt_spawn = true;
        let actor = runtime.spawn(actor).unwrap();
        crate::tests::fuzz_support::drive_steps(&backend, 8);
        ArtifactHarness {
            runtime,
            backend,
            _engine: engine,
            actor,
            recording,
        }
    }

    fn artifact_completion_observations(
        output: &[(SshBootstrapStream, String)],
        result: Result<std::process::ExitStatus, String>,
    ) -> Vec<crate::provisioning::PluginObservation> {
        let harness = artifact_harness();
        for (stream, line) in output {
            harness.send(SshBootstrapMsg::OutputLine {
                attempt: 1,
                stream: *stream,
                line: line.clone(),
            });
        }
        harness.send(SshBootstrapMsg::ChildExited { attempt: 1, result });
        harness.send(SshBootstrapMsg::ReaderClosed {
            attempt: 1,
            stream: SshBootstrapStream::Stdout,
        });
        harness.send(SshBootstrapMsg::ReaderClosed {
            attempt: 1,
            stream: SshBootstrapStream::Stderr,
        });
        let observations = harness.observations();
        let _ = harness.runtime.stop_actor(harness.actor);
        crate::tests::fuzz_support::drive_steps(&harness.backend, 8);
        observations
    }

    fn bootstrap_failures(observations: &[crate::provisioning::PluginObservation]) -> usize {
        observations
            .iter()
            .filter(|observation| {
                matches!(
                    observation,
                    crate::provisioning::PluginObservation::Failed { .. }
                )
            })
            .count()
    }

    #[test]
    fn artifact_stop_and_parent_shutdown_complete_once() {
        for terminal in [Some(SshBootstrapMsg::Stop), None] {
            let harness = artifact_harness();
            // An accepted child exit still owns completion while readers drain.
            harness.send(SshBootstrapMsg::ChildExited {
                attempt: 1,
                result: Err("lost transport".to_owned()),
            });
            assert_eq!(bootstrap_failures(&harness.observations()), 0);
            if let Some(terminal) = terminal {
                harness
                    .runtime
                    .send_to(harness.actor, terminal.clone())
                    .unwrap();
                harness.runtime.send_to(harness.actor, terminal).unwrap();
            } else {
                harness.runtime.stop_actor(harness.actor).unwrap();
            }
            crate::tests::fuzz_support::drive_steps(&harness.backend, 16);
            assert_eq!(bootstrap_failures(&harness.observations()), 1);
            assert!(
                harness
                    .runtime
                    .send_to(harness.actor, SshBootstrapMsg::Stop)
                    .is_err()
            );
        }
    }

    #[test]
    fn artifact_stale_receipt_and_duplicate_exit_cannot_complete_next_attempt() {
        use std::os::unix::process::ExitStatusExt;
        let harness = artifact_harness();
        harness.send(SshBootstrapMsg::ChildExited {
            attempt: 1,
            result: Err("retry transport".to_owned()),
        });
        harness.send(SshBootstrapMsg::ReaderClosed {
            attempt: 1,
            stream: SshBootstrapStream::Stdout,
        });
        harness.send(SshBootstrapMsg::ReaderClosed {
            attempt: 1,
            stream: SshBootstrapStream::Stderr,
        });
        harness.send(SshBootstrapMsg::Refresh);
        harness.send(SshBootstrapMsg::OutputLine {
            attempt: 1,
            stream: SshBootstrapStream::Stdout,
            line: json!({
                "type": "MyelinBootstrapReceipt", "artifact_digest": identity().artifact_digest,
                "deployment_generation": "deploy-test", "executable_digest": executable_digest(),
                "pid": 42, "process_start_ticks": 99, "incarnation": "42:99:deploy-test", "worker_count": 1,
            }).to_string(),
        });
        harness.send(SshBootstrapMsg::ChildExited {
            attempt: 1,
            result: Ok(std::process::ExitStatus::from_raw(0)),
        });
        harness.send(SshBootstrapMsg::ChildExited {
            attempt: 2,
            result: Err("second transport".to_owned()),
        });
        harness.send(SshBootstrapMsg::ChildExited {
            attempt: 2,
            result: Ok(std::process::ExitStatus::from_raw(0)),
        });
        harness.send(SshBootstrapMsg::ReaderClosed {
            attempt: 1,
            stream: SshBootstrapStream::Stdout,
        });
        harness.send(SshBootstrapMsg::ReaderClosed {
            attempt: 1,
            stream: SshBootstrapStream::Stderr,
        });
        assert_eq!(bootstrap_failures(&harness.observations()), 0);
        // Stale EOFs cannot discharge the current attempt; cancellation must still resolve it.
        harness.send(SshBootstrapMsg::Stop);
        let observations = harness.observations();
        assert_eq!(bootstrap_failures(&observations), 1);
        assert!(!observations.iter().any(|observation| matches!(observation,
            crate::provisioning::PluginObservation::ProviderLine { line, .. }
                if serde_json::from_str::<serde_json::Value>(line).ok().is_some_and(|line| line["type"] == "MyelinBootstrapDispatched")
        )));
    }

    #[test]
    fn artifact_failure_requires_external_refresh_and_remains_cancellable() {
        let harness = artifact_harness();
        harness.send(SshBootstrapMsg::Refresh);
        assert_eq!(harness.attempts(), [1]);
        harness.send(SshBootstrapMsg::ChildExited {
            attempt: 1,
            result: Err("lost transport".to_owned()),
        });
        // Refresh during draining cannot discard unread receipts or schedule
        // a retry after the streams close.
        harness.send(SshBootstrapMsg::Refresh);
        harness.close_streams(1);
        assert_eq!(harness.attempts(), [1]);
        assert_eq!(bootstrap_failures(&harness.observations()), 0);

        harness.send(SshBootstrapMsg::Refresh);
        harness.send(SshBootstrapMsg::Refresh);
        assert_eq!(harness.attempts(), [1, 2]);
        harness.send(SshBootstrapMsg::ChildExited {
            attempt: 2,
            result: Err("lost second transport".to_owned()),
        });
        harness.close_streams(2);
        assert_eq!(harness.attempts(), [1, 2]);
        harness.send(SshBootstrapMsg::Stop);
        assert_eq!(bootstrap_failures(&harness.observations()), 1);
        assert!(
            harness
                .runtime
                .send_to(harness.actor, SshBootstrapMsg::Refresh)
                .is_err()
        );
    }

    #[test]
    fn artifact_cache_miss_dispatches_transfer_once_without_a_retry_cycle() {
        use std::os::unix::process::ExitStatusExt;

        let harness = artifact_harness();
        for attempt in [1, 2] {
            harness.send(SshBootstrapMsg::OutputLine {
                attempt,
                stream: SshBootstrapStream::Stderr,
                line: "MYELIN-CONTENT:TRANSFER_REQUIRED".to_owned(),
            });
            harness.send(SshBootstrapMsg::ChildExited {
                attempt,
                result: Ok(std::process::ExitStatus::from_raw(75 << 8)),
            });
            harness.close_streams(attempt);
            // The first miss dispatches transfer immediately; a miss during
            // transfer is a failure, not another discovery-to-transfer edge.
            assert_eq!(harness.attempts(), [1, 2]);
        }
        assert_eq!(bootstrap_failures(&harness.observations()), 0);
        harness.send(SshBootstrapMsg::Refresh);
        assert_eq!(harness.attempts(), [1, 2, 3]);
        harness.send(SshBootstrapMsg::Stop);
        assert_eq!(bootstrap_failures(&harness.observations()), 1);
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
    fn withheld_ssh_banner_disconnect_preserves_sibling_cleanup() {
        use std::io::Read;
        use std::net::TcpListener;
        use swactor::config::RuntimeConfig;
        use swactor::runtime::RuntimeParts;
        use swactor_engine::{Engine, TokioBackend, TokioConfig};

        let directory = tempfile::tempdir().unwrap();
        let marker = directory.path().join("healthy-sibling");
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        });
        let runtime = parts.runtime().clone();
        let engine =
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
        let (identified, identification) = std::sync::mpsc::channel();
        let (disconnect, disconnected) = std::sync::mpsc::channel();
        let peer = engine
            .handle()
            .blocking_work_sender()
            .submit_joinable(move || {
                let (mut socket, _) = listener.accept().expect("accept SSH transport");
                let mut received = Vec::new();
                let mut byte = [0; 1];
                loop {
                    socket
                        .read_exact(&mut byte)
                        .expect("read SSH identification");
                    received.push(byte[0]);
                    assert!(received.len() < 4096, "bounded identification exchange");
                    if byte[0] == b'\n' {
                        break;
                    }
                }
                assert!(
                    received.starts_with(b"SSH-2.0-"),
                    "real SSH client never identified itself"
                );
                identified.send(()).unwrap();
                // The test owns this external event; no clock determines when
                // the withheld identification exchange fails.
                disconnected.recv().expect("explicit peer disconnect");
                drop(socket);
            })
            .expect("submit scripted SSH peer");
        let transport = SupervisedChild::spawn(
            &engine.handle().blocking_work_sender(),
            &mut command,
            Some(Arc::from(vec![0_u8; 1024 * 1024])),
            runtime.create_sender(),
            probe,
            1,
        )
        .unwrap();
        identification.recv().expect("SSH identified itself");
        // An independent child must finish while SSH and its input writer are
        // still blocked on the peer's deliberately withheld banner.
        let healthy = SupervisedChild::spawn(
            &engine.handle().blocking_work_sender(),
            Command::new("touch").arg(&marker),
            None,
            runtime.create_sender(),
            probe,
            2,
        )
        .unwrap();
        let mut observations = Vec::new();
        loop {
            let observation = receiver.recv().expect("sibling process observation");
            let healthy_exited = matches!(
                observation,
                SupervisedProcessObservation::Exited { operation: 2, .. }
            );
            assert!(
                !matches!(
                    observation,
                    SupervisedProcessObservation::Exited { operation: 1, .. }
                ),
                "SSH must remain pending until the peer disconnects"
            );
            observations.push(observation);
            if healthy_exited {
                break;
            }
        }
        drop(healthy);
        assert!(
            marker.is_file(),
            "withheld SSH canceled independent healthy work"
        );
        disconnect.send(()).unwrap();
        peer.join().unwrap();
        loop {
            let observation = receiver.recv().expect("SSH process observation");
            let transport_exited = matches!(
                observation,
                SupervisedProcessObservation::Exited { operation: 1, .. }
            );
            observations.push(observation);
            if transport_exited {
                break;
            }
        }
        drop(transport);
        // Cleanup is another real owned child, not a mocked callback.
        let cleanup = SupervisedChild::spawn(
            &engine.handle().blocking_work_sender(),
            Command::new("rm").arg(&marker),
            None,
            runtime.create_sender(),
            probe,
            3,
        )
        .unwrap();
        loop {
            let observation = receiver.recv().expect("cleanup process observation");
            let cleanup_exited = matches!(
                observation,
                SupervisedProcessObservation::Exited { operation: 3, .. }
            );
            observations.push(observation);
            if cleanup_exited {
                break;
            }
        }
        drop(cleanup);
        let stopped = runtime.admin().stop_actor(probe).unwrap();
        let (stopped_sender, stopped_receiver) = std::sync::mpsc::channel();
        engine.task_spawner().spawn(async move {
            let result = stopped.recv().await;
            drop(stopped);
            let _ = stopped_sender.send(result);
        });
        assert!(stopped_receiver.recv().unwrap().unwrap().applied);
        assert!(
            receiver.recv().is_err(),
            "the observation actor must stop after its last completed owner"
        );
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
                }) => closed += 1,
                _ => {}
            }
        }
        assert!(!exits[&1].as_ref().unwrap().success());
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
