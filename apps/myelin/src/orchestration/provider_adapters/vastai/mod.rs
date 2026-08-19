// VastAI provider adapter: owns private blocking facades and a legacy provider
// monitor thread outside the orchestration engine. The monitor's swactor core
// is driven by an explicit SingleThreadRuntime owned by that thread; the main
// orchestration engine owns all bootstrap actors spawned on its runtime handle.
#![allow(clippy::disallowed_methods)]
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{
    Ctx, ExternalSender, Runtime, RuntimeConfig, RuntimeParts, SingleThreadRuntime,
};
use swactor_vastai::{
    CreateInstanceRequest, LifecyclePolicy, Offer, OfferBrowseCriteria, ProvisionRequest,
    ProvisionedInstance, SelectionPolicy, classify_vastai_error,
};
use telemetry::TelemetryProducer;

use crate::observability::provisioning_logs::{BootstrapTelemetryBridge, node_stream_id};
use crate::provisioning::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginSink,
    ProvisionPlugin,
};

#[derive(Clone, Debug)]
pub(crate) struct VastAiProvisioningConfig {
    pub label_prefix: String,
    pub disk_gb: u32,
    pub ssh_user: String,
    pub selection: SelectionPolicy,
    pub lifecycle: LifecyclePolicy,
    pub confirm_lease: bool,
    pub onstart: Option<String>,
    pub ssh_public_key: Option<String>,
}

impl Default for VastAiProvisioningConfig {
    fn default() -> Self {
        Self {
            label_prefix: "myelin".to_owned(),
            disk_gb: 50,
            ssh_user: "root".to_owned(),
            selection: SelectionPolicy::default(),
            lifecycle: LifecyclePolicy::default(),
            confirm_lease: false,
            onstart: None,
            ssh_public_key: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct VastAiSshEndpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
}

pub(crate) struct VastAiProviderMonitor {
    runtime: Runtime,
    actor: ActorAddress,
    tick_thread: Option<JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
}

impl VastAiProviderMonitor {
    fn new(runtime: Runtime, mut host: SingleThreadRuntime, actor: ActorAddress) -> Self {
        let stop_flag = Arc::new(AtomicBool::new(false));

        let flag = Arc::clone(&stop_flag);
        let tick_thread = thread::spawn(move || {
            while !flag.load(Ordering::Relaxed) || host.has_work() {
                if host.has_work() {
                    host.tick();
                } else {
                    thread::sleep(Duration::from_millis(10));
                }
            }
        });

        Self {
            runtime,
            actor,
            tick_thread: Some(tick_thread),
            stop_flag,
        }
    }

    fn stop(&mut self) {
        let Some(tick_thread) = self.tick_thread.take() else {
            return;
        };
        let _ = self
            .runtime
            .send_to(self.actor, VastAiProviderMonitorMsg::Stop);
        self.stop_flag.store(true, Ordering::Relaxed);
        let _ = tick_thread.join();
    }
}

impl Drop for VastAiProviderMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(crate) trait VastAiLeaseClient: Send {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String>;

    fn provision_exact(
        &mut self,
        _request: ProvisionRequest,
        offer_id: u64,
    ) -> Result<ProvisionedInstance, String> {
        Err(format!(
            "VastAI lease client does not support exact offer {offer_id}"
        ))
    }

    /// Resolves the live contract id carrying `label`, if any.
    fn contract_by_label(&mut self, label: &str) -> Result<Option<u64>, String>;

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        label: &str,
        lifecycle: &LifecyclePolicy,
        ssh_user: &str,
    ) -> Result<VastAiSshEndpoint, String>;
    fn spawn_provider_monitor(
        &mut self,
        _contract_id: u64,
        _label: String,
        _lifecycle: LifecyclePolicy,
        _spec: NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Option<VastAiProviderMonitor> {
        None
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String>;
}

pub(crate) struct ToolsVastAiLeaseClient {
    client: swactor_vastai::VastClient,
    runtime: tokio::runtime::Runtime,
}

impl ToolsVastAiLeaseClient {
    pub(crate) fn new(client: swactor_vastai::VastClient) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("vastai tokio runtime: {e}"))?;
        Ok(Self { client, runtime })
    }

    pub(crate) fn from_api_key(api_key: impl Into<String>) -> Result<Self, String> {
        Self::new(swactor_vastai::VastClient::new(api_key))
    }

    pub(crate) fn browse_offers(
        &mut self,
        criteria: &OfferBrowseCriteria,
    ) -> Result<Vec<Offer>, String> {
        self.runtime.block_on(self.client.browse_offers(criteria))
    }

    fn create_request_for_offer(
        request: &ProvisionRequest,
        offer_id: u64,
    ) -> CreateInstanceRequest {
        let mut env = request.env.clone();
        if let Some(overlay) = request.per_instance_env.first() {
            env.extend(
                overlay
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone())),
            );
        }
        CreateInstanceRequest {
            offer_id,
            image: request.image.clone(),
            disk_gb: request.disk_gb,
            label: request.label.clone(),
            env,
            onstart: request.onstart.clone(),
        }
    }

    fn candidate_pool(&mut self, request: &ProvisionRequest) -> Result<Vec<Offer>, String> {
        self.runtime
            .block_on(self.client.search_offers(&request.selection, 1))
    }

    fn create_from_offer(
        &mut self,
        request: &ProvisionRequest,
        offer: &Offer,
    ) -> Result<ProvisionedInstance, String> {
        let create = Self::create_request_for_offer(request, offer.id);
        let info = self
            .runtime
            .block_on(self.client.create_instance(&create))?;
        Ok(ProvisionedInstance {
            index: 0,
            contract_id: info.contract_id,
            offer_id: offer.id,
            host_id: offer.host_id,
            gpu_name: offer.gpu_name.clone(),
            gpu_ram: offer.gpu_ram,
            dph_total: offer.dph_total,
        })
    }
}

#[derive(Clone)]
enum VastAiProviderMonitorMsg {
    Poll,
    Stop,
}

struct VastAiProviderMonitorActor {
    client: ToolsVastAiLeaseClient,
    contract_id: u64,
    label: String,
    lifecycle: LifecyclePolicy,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    sender: ExternalSender,
    last_state: Option<String>,
    state_since: Instant,
    poll: u64,
    stopped: bool,
}

impl VastAiProviderMonitorActor {
    fn new(
        client: ToolsVastAiLeaseClient,
        contract_id: u64,
        label: String,
        lifecycle: LifecyclePolicy,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        sender: ExternalSender,
    ) -> Self {
        Self {
            client,
            contract_id,
            label,
            lifecycle,
            spec,
            sink,
            sender,
            last_state: None,
            state_since: Instant::now(),
            poll: 0,
            stopped: false,
        }
    }

    fn observe_provider_line(&self, line: impl Into<String>) {
        self.sink.observe(PluginObservation::ProviderLine {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            line: line.into(),
        });
    }

    fn observe_failed(&self, reason: String) {
        self.sink.observe(PluginObservation::Failed {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            reason,
        });
    }

    fn schedule_next_poll(&self, ctx: &Ctx) {
        schedule_provider_monitor_poll(
            self.sender.clone(),
            ctx.self_addr(),
            self.lifecycle.poll_interval,
        );
    }

    fn poll_provider(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        self.poll = self.poll.saturating_add(1);
        let poll = self.poll;
        let status = match self
            .client
            .runtime
            .block_on(self.client.client.instance_status(self.contract_id))
        {
            Ok(status) => status,
            Err(error)
                if error.contains("not found while fetching provider status")
                    || error.contains("parse failed") =>
            {
                let reason = classified_start_error(format!(
                    "vastai provider monitor node {} contract {}: {error}",
                    self.spec.node_id, self.contract_id
                ));
                self.observe_provider_line(
                    serde_json::json!({
                        "type": "VastAiProviderStatusFailure",
                        "run_id": self.spec.run_id,
                        "node_id": self.spec.node_id,
                        "label": &self.label,
                        "contract_id": self.contract_id,
                        "poll": poll,
                        "reason": &reason,
                    })
                    .to_string(),
                );
                self.observe_failed(reason);
                ctx.stop_self();
                return;
            }
            Err(error) => {
                self.observe_provider_line(
                    serde_json::json!({
                        "type": "VastAiProviderStatusPollRetry",
                        "run_id": self.spec.run_id,
                        "node_id": self.spec.node_id,
                        "label": &self.label,
                        "contract_id": self.contract_id,
                        "poll": poll,
                        "reason": error,
                    })
                    .to_string(),
                );
                self.schedule_next_poll(ctx);
                return;
            }
        };

        let actual = status.actual_status.as_str();
        if self.last_state.as_deref() != Some(actual) {
            self.state_since = Instant::now();
            self.last_state = Some(actual.to_owned());
        }
        let in_state_ms = self.state_since.elapsed().as_millis();
        self.observe_provider_line(
            serde_json::json!({
                "type": "VastAiProviderStatusObserved",
                "run_id": self.spec.run_id,
                "node_id": self.spec.node_id,
                "label": &self.label,
                "contract_id": self.contract_id,
                "poll": poll,
                "actual_status": &status.actual_status,
                "intended_status": &status.intended_status,
                "status_msg": &status.status_msg,
                "disk_usage": status.disk_usage,
                "in_state_ms": in_state_ms,
            })
            .to_string(),
        );

        if let Some(error) = provider_terminal_start_error(
            self.contract_id,
            &status.actual_status,
            &status.intended_status,
            status.status_msg.as_deref(),
        ) {
            let reason = classified_start_error(format!(
                "vastai provider monitor node {}: {error}",
                self.spec.node_id
            ));
            self.observe_provider_line(
                serde_json::json!({
                    "type": "VastAiProviderTerminalBeforeRuntimeReady",
                    "run_id": self.spec.run_id,
                    "node_id": self.spec.node_id,
                    "label": &self.label,
                    "contract_id": self.contract_id,
                    "reason": &reason,
                })
                .to_string(),
            );
            self.observe_failed(reason);
            ctx.stop_self();
            return;
        }

        self.schedule_next_poll(ctx);
    }
}

impl ActorInterface for VastAiProviderMonitorActor {
    type Incoming = VastAiProviderMonitorMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), VastAiProviderMonitorMsg::Poll);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            VastAiProviderMonitorMsg::Poll => self.poll_provider(ctx),
            VastAiProviderMonitorMsg::Stop => {
                self.stopped = true;
                ctx.stop_self();
            }
        }
    }
}

fn schedule_provider_monitor_poll(sender: ExternalSender, actor: ActorAddress, delay: Duration) {
    thread::spawn(move || {
        thread::sleep(delay);
        let _ = sender.send_to(actor, VastAiProviderMonitorMsg::Poll);
    });
}

impl Clone for ToolsVastAiLeaseClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            runtime: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("clone VastAI lease client runtime"),
        }
    }
}

fn adopted_instance(contract_id: u64) -> ProvisionedInstance {
    ProvisionedInstance {
        index: 0,
        contract_id,
        offer_id: 0,
        host_id: None,
        gpu_name: "adopted".to_owned(),
        gpu_ram: None,
        dph_total: 0.0,
    }
}

impl VastAiLeaseClient for ToolsVastAiLeaseClient {
    fn contract_by_label(&mut self, label: &str) -> Result<Option<u64>, String> {
        let instances = self.runtime.block_on(self.client.list_by_label(label))?;
        match instances.as_slice() {
            [] => Ok(None),
            [instance] => Ok(Some(instance.contract_id)),
            _ => Err(format!(
                "multiple VastAI contracts share stable label {label}: {}",
                instances
                    .iter()
                    .map(|instance| instance.contract_id.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            )),
        }
    }

    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String> {
        if request.count != 1 {
            return Err(format!(
                "vastai provision_one expected count=1, got {}",
                request.count
            ));
        }
        if let Some(label) = request.label.as_deref()
            && let Some(contract_id) = self.contract_by_label(label)?
        {
            return Ok(adopted_instance(contract_id));
        }

        let pool = self.candidate_pool(&request)?;
        let blocked_hosts = request
            .selection
            .blacklist_hosts
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut failed_hosts = HashSet::new();
        let mut tried_offer_ids = HashSet::new();
        let mut last_error = None;
        for offer in pool {
            if !tried_offer_ids.insert(offer.id) {
                continue;
            }
            if offer.host_id.is_some_and(|host_id| {
                blocked_hosts.contains(&host_id) || failed_hosts.contains(&host_id)
            }) {
                continue;
            }
            match self.create_from_offer(&request, &offer) {
                Ok(instance) => return Ok(instance),
                Err(error) => {
                    if let Some(label) = request.label.as_deref()
                        && let Some(contract_id) = self.contract_by_label(label)?
                    {
                        return Ok(adopted_instance(contract_id));
                    }
                    if classify_vastai_error(&error)
                        != swactor_vastai::VastAiFailureClass::VanishedOffer
                    {
                        return Err(format!("offer {}: {error}", offer.id));
                    }
                    if let Some(host_id) = offer.host_id {
                        failed_hosts.insert(host_id);
                    }
                    last_error = Some(format!("offer {}: {error}", offer.id));
                }
            }
        }

        Err(format!(
            "vastai provision node exhausted eligible offers{}",
            last_error
                .map(|error| format!(" after create failure ({error})"))
                .unwrap_or_default()
        ))
    }

    fn provision_exact(
        &mut self,
        request: ProvisionRequest,
        offer_id: u64,
    ) -> Result<ProvisionedInstance, String> {
        if request.count != 1 {
            return Err(format!(
                "vastai provision_exact expected count=1, got {}",
                request.count
            ));
        }
        if let Some(label) = request.label.as_deref()
            && let Some(contract_id) = self.contract_by_label(label)?
        {
            return Ok(adopted_instance(contract_id));
        }
        let offer = self
            .candidate_pool(&request)?
            .into_iter()
            .find(|offer| offer.id == offer_id)
            .ok_or_else(|| format!("selected offer {offer_id} is unavailable or ineligible"))?;
        match self.create_from_offer(&request, &offer) {
            Ok(instance) => Ok(instance),
            Err(error) => {
                if let Some(label) = request.label.as_deref()
                    && let Some(contract_id) = self.contract_by_label(label)?
                {
                    return Ok(adopted_instance(contract_id));
                }
                Err(format!("selected offer {offer_id}: {error}"))
            }
        }
    }

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        label: &str,
        lifecycle: &LifecyclePolicy,
        ssh_user: &str,
    ) -> Result<VastAiSshEndpoint, String> {
        let endpoint = self.runtime.block_on(self.client.wait_for_ssh_endpoint(
            contract_id,
            label,
            lifecycle,
        ))?;
        let host = endpoint.ip;
        let port = endpoint.port;
        if host.is_empty() || host == "unknown" {
            return Err(format!("vastai contract {contract_id} has no SSH host"));
        }
        if port == 0 {
            return Err(format!("vastai contract {contract_id} has no SSH port"));
        }
        Ok(VastAiSshEndpoint {
            host,
            port,
            user: ssh_user.to_owned(),
        })
    }

    fn spawn_provider_monitor(
        &mut self,
        contract_id: u64,
        label: String,
        lifecycle: LifecyclePolicy,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Option<VastAiProviderMonitor> {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let sender = runtime.create_sender();
        let actor = runtime
            .spawn(VastAiProviderMonitorActor::new(
                self.clone(),
                contract_id,
                label,
                lifecycle,
                spec,
                sink,
                sender,
            ))
            .ok()?;
        Some(VastAiProviderMonitor::new(
            runtime,
            SingleThreadRuntime::new(parts),
            actor,
        ))
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
        self.runtime
            .block_on(self.client.destroy_instance_with_retry(contract_id))
    }
}

fn provider_terminal_start_error(
    contract_id: u64,
    actual: &str,
    intended: &str,
    msg: Option<&str>,
) -> Option<String> {
    if let Some(message) = msg
        && provider_status_message_has_terminal_failure(message)
    {
        return Some(format!("instance {contract_id} error: {message}"));
    }
    if intended == "stopped" && actual != "running" {
        return Some(format!(
            "instance {contract_id} stopped: {}",
            msg.unwrap_or_default()
        ));
    }
    match actual {
        "exited" | "error" | "stopped" => Some(format!(
            "instance {contract_id} reached terminal status: {actual}"
        )),
        _ => None,
    }
}

fn provider_status_message_has_terminal_failure(message: &str) -> bool {
    message
        .split(|ch: char| !ch.is_ascii_alphanumeric())
        .any(|token| {
            token.eq_ignore_ascii_case("error")
                || token.eq_ignore_ascii_case("failed")
                || token.eq_ignore_ascii_case("failure")
                || token.eq_ignore_ascii_case("fatal")
        })
}

pub(crate) trait VastAiBootstrapLauncher: Send {
    type Handle: Send;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: VastAiSshEndpoint,
        sink: PluginSink,
        producer: Option<TelemetryProducer>,
        lifecycle: LifecyclePolicy,
    ) -> Result<Self::Handle, String>;

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SshBootstrapStream {
    Stdout,
    Stderr,
}

#[derive(Clone)]
enum SshBootstrapMsg {
    StartAttempt,
    PollChild,
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
    Stop,
}

struct SshBootstrapActor {
    bridge: BootstrapTelemetryBridge,
    endpoint: VastAiSshEndpoint,
    ssh_identity: Option<PathBuf>,
    sender: ExternalSender,
    child: Option<Child>,
    stdout_reader: Option<JoinHandle<()>>,
    stderr_reader: Option<JoinHandle<()>>,
    stdout_closed: bool,
    stderr_closed: bool,
    pending_status: Option<std::process::ExitStatus>,
    pending_wait_error: Option<String>,
    attempt: u64,
    backoff: Duration,
    observation_class: Option<&'static str>,
    stopped: bool,
}

impl SshBootstrapActor {
    fn new(
        bridge: BootstrapTelemetryBridge,
        endpoint: VastAiSshEndpoint,
        ssh_identity: Option<PathBuf>,
        sender: ExternalSender,
    ) -> Self {
        Self {
            bridge,
            endpoint,
            ssh_identity,
            sender,
            child: None,
            stdout_reader: None,
            stderr_reader: None,
            stdout_closed: true,
            stderr_closed: true,
            pending_status: None,
            pending_wait_error: None,
            attempt: 1,
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

    fn start_attempt(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        self.bridge.observe_provider_line(format!(
            "VastAI SSH bootstrap attempt {} to {}@{}:{}",
            self.attempt, self.endpoint.user, self.endpoint.host, self.endpoint.port
        ));

        match spawn_ssh_bootstrap_attempt(
            self.bridge.spec(),
            &self.endpoint,
            self.ssh_identity.as_deref(),
        ) {
            Ok((child, stdout, stderr)) => {
                self.child = Some(child);
                self.stdout_closed = false;
                self.stderr_closed = false;
                self.observation_class = None;
                self.pending_status = None;
                self.pending_wait_error = None;
                self.stdout_reader = Some(spawn_ssh_output_reader(
                    SshBootstrapStream::Stdout,
                    stdout,
                    self.sender.clone(),
                    ctx.self_addr(),
                ));
                self.stderr_reader = Some(spawn_ssh_output_reader(
                    SshBootstrapStream::Stderr,
                    stderr,
                    self.sender.clone(),
                    ctx.self_addr(),
                ));
                schedule_ssh_message(
                    self.sender.clone(),
                    ctx.self_addr(),
                    SshBootstrapMsg::PollChild,
                    Duration::from_millis(100),
                );
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

    fn poll_child(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        let Some(child) = self.child.as_mut() else {
            return;
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                self.child = None;
                self.pending_status = Some(status);
                self.maybe_finish_attempt(ctx);
            }
            Ok(None) => schedule_ssh_message(
                self.sender.clone(),
                ctx.self_addr(),
                SshBootstrapMsg::PollChild,
                Duration::from_millis(100),
            ),
            Err(error) => {
                self.child = None;
                self.pending_wait_error = Some(error.to_string());
                self.maybe_finish_attempt(ctx);
            }
        }
    }

    fn handle_output_line(&mut self, stream: SshBootstrapStream, line: String) {
        match stream {
            SshBootstrapStream::Stdout => self.bridge.observe_stdout_line(line),
            SshBootstrapStream::Stderr => {
                if let Some(class) = classify_ssh_observation(&line) {
                    self.observation_class = Some(class);
                    self.bridge.observe_provider_line(
                        serde_json::json!({
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

    fn maybe_finish_attempt(&mut self, ctx: &Ctx) {
        if self.stopped || !self.stdout_closed || !self.stderr_closed {
            return;
        }
        if let Some(error) = self.pending_wait_error.take() {
            self.join_readers();
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
        self.join_readers();
        let readiness = if status.success() {
            "exited before runtime ready"
        } else {
            "not ready before runtime ready"
        };
        let observation_class = self.observation_class.unwrap_or("process_exit");
        self.bridge.observe_provider_line(
            serde_json::json!({
                "type": "VastAiBootstrapAttemptCompleted",
                "run_id": self.run_id(),
                "node_id": self.node_id(),
                "attempt": self.attempt,
                "status": status.to_string(),
                "class": observation_class,
                "classification": readiness,
            })
            .to_string(),
        );
        self.schedule_retry(ctx);
    }

    fn schedule_retry(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        let delay = self.backoff;
        self.bridge.observe_provider_line(format!(
            "VastAI SSH bootstrap retrying in {}s after attempt {}",
            delay.as_secs(),
            self.attempt
        ));
        self.backoff = std::cmp::min(self.backoff.saturating_mul(2), Duration::from_secs(30));
        self.attempt = self.attempt.saturating_add(1);
        schedule_ssh_message(
            self.sender.clone(),
            ctx.self_addr(),
            SshBootstrapMsg::StartAttempt,
            delay,
        );
    }

    fn join_readers(&mut self) {
        if let Some(reader) = self.stdout_reader.take() {
            let _ = reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            let _ = reader.join();
        }
        self.stdout_closed = true;
        self.stderr_closed = true;
    }

    fn stop_child(&mut self) {
        stop_ssh_child(&mut self.child);
        self.join_readers();
    }
}

impl ActorInterface for SshBootstrapActor {
    type Incoming = SshBootstrapMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        let _ = ctx.send(ctx.self_addr(), SshBootstrapMsg::StartAttempt);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            SshBootstrapMsg::StartAttempt => self.start_attempt(ctx),
            SshBootstrapMsg::PollChild => self.poll_child(ctx),
            SshBootstrapMsg::OutputLine { stream, line } => self.handle_output_line(stream, line),
            SshBootstrapMsg::ReaderError { stream, error } => {
                self.handle_reader_error(stream, error)
            }
            SshBootstrapMsg::ReaderClosed { stream } => self.handle_reader_closed(ctx, stream),
            SshBootstrapMsg::Stop => {
                self.stopped = true;
                self.stop_child();
                ctx.stop_self();
            }
        }
    }

    fn on_stop(&mut self, _ctx: &Ctx) {
        self.stopped = true;
        self.stop_child();
    }
}

fn stop_ssh_child(child: &mut Option<Child>) {
    let Some(mut child) = child.take() else {
        return;
    };
    let _ = child.kill();
    let _ = child.wait();
}

#[derive(Clone)]
pub(crate) struct SshCommandBootstrapLauncher {
    ssh_identity: Option<PathBuf>,
    runtime: Runtime,
}
pub(crate) struct SshCommandBootstrapHandle {
    actor: ActorAddress,
    runtime: Runtime,
}

impl SshCommandBootstrapLauncher {
    pub(crate) fn new(ssh_identity: Option<PathBuf>, runtime: Runtime) -> Self {
        Self {
            ssh_identity,
            runtime,
        }
    }
}

impl VastAiBootstrapLauncher for SshCommandBootstrapLauncher {
    type Handle = SshCommandBootstrapHandle;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: VastAiSshEndpoint,
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
                sender,
            ))
            .map_err(|e| format!("spawn VastAI SSH bootstrap actor: {e}"))?;

        Ok(SshCommandBootstrapHandle {
            actor,
            runtime: self.runtime.clone(),
        })
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle) {
        let _ = handle.runtime.send_to(handle.actor, SshBootstrapMsg::Stop);
    }
}

fn classify_ssh_observation(line: &str) -> Option<&'static str> {
    let lower = line.to_ascii_lowercase();
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

fn spawn_ssh_output_reader<R>(
    stream: SshBootstrapStream,
    reader: R,
    sender: ExternalSender,
    actor: ActorAddress,
) -> JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let reader = BufReader::new(reader);
        for next in reader.lines() {
            match next {
                Ok(line) => {
                    let _ = sender.send_to(actor, SshBootstrapMsg::OutputLine { stream, line });
                }
                Err(error) => {
                    let _ = sender.send_to(
                        actor,
                        SshBootstrapMsg::ReaderError {
                            stream,
                            error: error.to_string(),
                        },
                    );
                    break;
                }
            }
        }
        let _ = sender.send_to(actor, SshBootstrapMsg::ReaderClosed { stream });
    })
}

fn schedule_ssh_message(
    sender: ExternalSender,
    actor: ActorAddress,
    msg: SshBootstrapMsg,
    delay: Duration,
) {
    thread::spawn(move || {
        thread::sleep(delay);
        let _ = sender.send_to(actor, msg);
    });
}

fn spawn_ssh_bootstrap_attempt(
    spec: &NodeProvisionSpec,
    endpoint: &VastAiSshEndpoint,
    ssh_identity: Option<&Path>,
) -> Result<(Child, std::process::ChildStdout, std::process::ChildStderr), String> {
    let remote_command = idempotent_ssh_bootstrap_command(spec);
    let mut command = Command::new("ssh");
    command
        .args(ssh_bootstrap_args(endpoint, &remote_command, ssh_identity))
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|e| format!("spawn VastAI SSH bootstrap {}: {e}", spec.node_id))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| format!("VastAI node {} SSH stdout missing", spec.node_id))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| format!("VastAI node {} SSH stderr missing", spec.node_id))?;
    Ok((child, stdout, stderr))
}

fn shell_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn idempotent_ssh_bootstrap_command(spec: &NodeProvisionSpec) -> String {
    let command = shell_single_quote(&spec.args.join(" "));
    let lock = format!(
        "/tmp/myelin-bootstrap-{}-{}-{}",
        spec.run_id, spec.node_id, spec.attempt_id
    );
    format!(
        "lock={lock}; command={command}; while :; do \
         if mkdir \"$lock\" 2>/dev/null; then \
           echo $$ > \"$lock/pid\"; sh -lc \"$command\"; status=$?; \
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

fn ssh_bootstrap_args(
    endpoint: &VastAiSshEndpoint,
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

fn emit_node_line(sink: &PluginSink, run_id: u64, node_id: u64, line: impl Into<String>) {
    sink.observe(PluginObservation::ProviderLine {
        run_id,
        node_id,
        line: line.into(),
    });
}

pub(crate) struct VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient,
    B: VastAiBootstrapLauncher,
{
    client: C,
    bootstrap: B,
    config: VastAiProvisioningConfig,
    bootstrap_producer: Option<TelemetryProducer>,
    leased_host_ids: BTreeSet<u64>,
    failed_host_ids: BTreeSet<u64>,
    next_handle_id: u64,
    nodes: BTreeMap<u64, VastAiNode<B::Handle>>,
}

struct VastAiNode<H> {
    contract_id: u64,
    bootstrap: Option<H>,
    provider_monitor: Option<VastAiProviderMonitor>,
    host_id: Option<u64>,
    run_id: u64,
    node_id: u64,
    label: String,
    sink: PluginSink,
    spec: NodeProvisionSpec,
    endpoint: VastAiSshEndpoint,
}

impl<C, B> VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient,
    B: VastAiBootstrapLauncher,
{
    pub(crate) fn new(client: C, bootstrap: B, config: VastAiProvisioningConfig) -> Self {
        Self {
            client,
            bootstrap,
            config,
            bootstrap_producer: None,
            next_handle_id: 1,
            leased_host_ids: BTreeSet::new(),
            failed_host_ids: BTreeSet::new(),
            nodes: BTreeMap::new(),
        }
    }

    fn label_for(&self, spec: &NodeProvisionSpec) -> String {
        format!(
            "{}-{}-{}-attempt-{}",
            self.config.label_prefix, spec.run_id, spec.node_id, spec.attempt_id
        )
    }

    fn build_request(&self, spec: &NodeProvisionSpec, label: String) -> ProvisionRequest {
        let mut env = spec.env.iter().cloned().collect::<BTreeMap<_, _>>();
        if let Some(key) = self
            .config
            .ssh_public_key
            .as_deref()
            .filter(|key| !key.trim().is_empty())
        {
            env.insert("SSH_PUBLIC_KEY".to_owned(), key.to_owned());
        }
        let mut selection = self.config.selection.clone();
        for host_id in self
            .leased_host_ids
            .iter()
            .chain(self.failed_host_ids.iter())
        {
            if !selection.blacklist_hosts.contains(host_id) {
                selection.blacklist_hosts.push(*host_id);
            }
        }

        ProvisionRequest {
            count: 1,
            image: spec.image.clone(),
            label: Some(label),
            disk_gb: self.config.disk_gb,
            env,
            per_instance_env: vec![BTreeMap::new()],
            preferred_offer_id: None,
            onstart: self.config.onstart.clone(),
            selection,
            lifecycle: self.config.lifecycle.clone(),
            confirm_lease: self.config.confirm_lease,
        }
    }

    fn cleanup_contract_after_start_error(&mut self, contract_id: u64, reason: String) -> String {
        match self.client.destroy_contract(contract_id) {
            Ok(()) => reason,
            Err(cleanup) => format!("{reason}; cleanup destroy {contract_id} failed: {cleanup}"),
        }
    }
    fn create_node_with_offer(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        selected_offer_id: Option<u64>,
    ) -> Result<PluginNodeHandle, String> {
        if !spec.mounts.is_empty() {
            return Err("vastai provider does not support host file mounts".to_owned());
        }
        let stream_id = node_stream_id(spec.run_id, spec.node_id);
        let label = self.label_for(&spec);
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            format!("vastai provisioning label={label} stream={stream_id}"),
        );

        let request = self.build_request(&spec, label.clone());
        let instance = match selected_offer_id {
            Some(offer_id) => self.client.provision_exact(request, offer_id),
            None => self.client.provision_one(request),
        }
        .map_err(|error| {
            classified_start_error(format!("vastai provision node {}: {error}", spec.node_id))
        })?;
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            format!(
                "vastai contract {} ready for SSH lookup",
                instance.contract_id
            ),
        );
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            serde_json::json!({
                "type": "VastAiLeaseReady",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "label": &label,
                "image": &spec.image,
                "contract_id": instance.contract_id,
                "offer_id": instance.offer_id,
                "host_id": instance.host_id,
                "gpu_name": &instance.gpu_name,
                "gpu_ram": instance.gpu_ram,
                "dph_total": instance.dph_total,
            })
            .to_string(),
        );
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            serde_json::json!({
                "type": "VastAiSshEndpointDiscoveryStarted",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "contract_id": instance.contract_id,
                "label": &label,
            })
            .to_string(),
        );

        let endpoint = match self.client.ssh_endpoint(
            instance.contract_id,
            &label,
            &self.config.lifecycle,
            &self.config.ssh_user,
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                if let Some(host_id) = instance.host_id {
                    self.failed_host_ids.insert(host_id);
                }
                return Err(self.cleanup_contract_after_start_error(
                    instance.contract_id,
                    classified_start_error(format!(
                        "vastai SSH endpoint node {}: {error}",
                        spec.node_id
                    )),
                ));
            }
        };
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            serde_json::json!({
                "type": "VastAiSshEndpointReady",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "contract_id": instance.contract_id,
                "host": &endpoint.host,
                "port": endpoint.port,
                "user": &endpoint.user,
            })
            .to_string(),
        );

        let host_id = instance.host_id;
        if let Some(host_id) = host_id {
            self.leased_host_ids.insert(host_id);
        }
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: None,
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle.id,
            VastAiNode {
                contract_id: instance.contract_id,
                bootstrap: None,
                provider_monitor: self.client.spawn_provider_monitor(
                    instance.contract_id,
                    label.clone(),
                    self.config.lifecycle.clone(),
                    spec.clone(),
                    sink.clone(),
                ),
                host_id,
                run_id: spec.run_id,
                node_id: spec.node_id,
                label,
                sink,
                spec,
                endpoint,
            },
        );
        Ok(handle)
    }
}

fn classified_start_error(reason: String) -> String {
    let class = classify_vastai_error(&reason).as_str();
    format!("{reason} [class={class}]")
}

impl<C, B> ProvisionPlugin for VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient + Clone + 'static,
    B: VastAiBootstrapLauncher + 'static,
{
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        self.create_node_with_offer(spec, sink, None)
    }

    fn create_node_selected(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        selected_offer_id: Option<u64>,
    ) -> Result<PluginNodeHandle, String> {
        self.create_node_with_offer(spec, sink, selected_offer_id)
    }
    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let node = self
            .nodes
            .get_mut(&handle.id)
            .ok_or_else(|| format!("vastai node handle {} is absent", handle.id))?;
        if node.bootstrap.is_some() {
            return Ok(());
        }
        emit_node_line(
            &node.sink,
            node.run_id,
            node.node_id,
            serde_json::json!({
                "type": "VastAiBootstrapObservationStarted",
                "run_id": node.run_id,
                "node_id": node.node_id,
                "contract_id": node.contract_id,
                "host": &node.endpoint.host,
                "port": node.endpoint.port,
                "user": &node.endpoint.user,
            })
            .to_string(),
        );
        match self.bootstrap.start_bootstrap(
            node.spec.clone(),
            node.endpoint.clone(),
            node.sink.clone(),
            self.bootstrap_producer.clone(),
            self.config.lifecycle.clone(),
        ) {
            Ok(bootstrap) => {
                node.bootstrap = Some(bootstrap);
                Ok(())
            }
            Err(error) => {
                if let Some(host_id) = node.host_id {
                    self.failed_host_ids.insert(host_id);
                }
                Err(classified_start_error(format!(
                    "vastai bootstrap node {}: {error}",
                    node.node_id
                )))
            }
        }
    }

    fn cancel_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.get_mut(&handle.id) else {
            return Ok(());
        };
        if let Some(mut bootstrap) = node.bootstrap.take() {
            self.bootstrap.stop_bootstrap(&mut bootstrap);
        }
        Ok(())
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.get_mut(&handle.id) else {
            return Ok(());
        };
        emit_node_line(
            &node.sink,
            node.run_id,
            node.node_id,
            serde_json::json!({
                "type": "VastAiRuntimeReadyAccepted",
                "run_id": node.run_id,
                "node_id": node.node_id,
                "label": &node.label,
                "contract_id": node.contract_id,
                "classification": "runtime_ready_over_provider_staleness",
            })
            .to_string(),
        );
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(mut node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        if let Some(mut monitor) = node.provider_monitor.take() {
            monitor.stop();
        }
        if let Some(mut bootstrap) = node.bootstrap.take() {
            self.bootstrap.stop_bootstrap(&mut bootstrap);
        }
        let result = self.client.destroy_contract(node.contract_id);
        emit_node_line(
            &node.sink,
            node.run_id,
            node.node_id,
            serde_json::json!({
                "type": "VastAiContractCleanup",
                "run_id": node.run_id,
                "node_id": node.node_id,
                "label": &node.label,
                "contract_id": node.contract_id,
                "result": if result.is_ok() { "ok" } else { "failed" },
                "error": result.as_ref().err(),
            })
            .to_string(),
        );
        match result {
            Ok(()) => {
                if let Some(host_id) = node.host_id {
                    self.leased_host_ids.remove(&host_id);
                }
                Ok(())
            }
            Err(error) => {
                self.nodes.insert(handle.id, node);
                Err(error)
            }
        }
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let label = self.label_for(spec);
        let mut contract = None;
        for attempt in 0..10 {
            contract = self.client.contract_by_label(&label)?;
            if contract.is_some() {
                break;
            }
            if attempt < 9 {
                thread::sleep(self.config.lifecycle.lease_pace);
            }
        }
        if contract.is_none() {
            return Ok(None);
        }
        // Reattach monitoring to the existing lease. The recovery state machine
        // re-enters the idempotent bootstrap only for pre-joining snapshots.
        let handle = self.create_node(spec.clone(), sink)?;
        Ok(Some(AdoptedNode {
            handle,
            provider_ref: label,
        }))
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        self.label_for(spec)
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        let label = self.label_for(spec);
        let Some(contract_id) = self.client.contract_by_label(&label)? else {
            return Ok(false);
        };
        let result = self.client.destroy_contract(contract_id);
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            serde_json::json!({
                "type": "VastAiContractCleanupBySpec",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "label": &label,
                "contract_id": contract_id,
                "result": if result.is_ok() { "ok" } else { "failed" },
                "error": result.as_ref().err(),
            })
            .to_string(),
        );
        result.map(|_| true)
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    #[test]
    fn bootstrap_command_has_stable_remote_idempotency_guard() {
        let spec = NodeProvisionSpec {
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: vec!["printf '%s' \"ready\"".to_owned()],
            mounts: Vec::new(),
        };

        let command = idempotent_ssh_bootstrap_command(&spec);

        assert!(command.contains("/tmp/myelin-bootstrap-5-7-9"));
        assert!(command.contains("kill -0"));
        assert!(command.contains("myelin bootstrap already running"));
        assert!(command.contains("printf"));
    }

    #[test]
    fn provision_one_adopts_stable_label_before_offer_search() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = runtime.block_on(MockServer::start());
        runtime.block_on(async {
            Mock::given(method("GET"))
                .and(path("/api/v0/instances/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "instances": [{
                        "id": 73,
                        "label": "run-5-node-7-attempt-9",
                        "actual_status": "loading",
                        "ssh_host": "",
                        "ssh_port": 0,
                        "public_ipaddr": ""
                    }]
                })))
                .mount(&server)
                .await;
        });
        let client = swactor_vastai::VastClient::with_base_url(server.uri(), "secret");
        let mut client = ToolsVastAiLeaseClient::new(client).unwrap();
        let adopted = client
            .provision_one(ProvisionRequest {
                count: 1,
                image: "node:v1".to_owned(),
                label: Some("run-5-node-7-attempt-9".to_owned()),
                disk_gb: 10,
                env: BTreeMap::new(),
                per_instance_env: vec![BTreeMap::new()],
                preferred_offer_id: None,
                onstart: None,
                selection: SelectionPolicy::default(),
                lifecycle: LifecyclePolicy::default(),
                confirm_lease: false,
            })
            .unwrap();

        assert_eq!(adopted.contract_id, 73);
        assert_eq!(adopted.offer_id, 0);
        let requests = runtime.block_on(server.received_requests()).unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].url.path(), "/api/v0/instances/");
    }

    use std::sync::atomic::AtomicUsize;

    use crate::provisioning::PluginObservationSink;

    struct NullSink;

    impl PluginObservationSink for NullSink {
        fn observe(&self, _observation: PluginObservation) {}
    }

    #[derive(Clone)]
    struct RetryDestroyClient {
        destroy_calls: Arc<AtomicUsize>,
        destroy_failures: Arc<AtomicUsize>,
        existing_contract: Option<u64>,
    }

    impl VastAiLeaseClient for RetryDestroyClient {
        fn contract_by_label(&mut self, _label: &str) -> Result<Option<u64>, String> {
            Ok(self.existing_contract)
        }

        fn provision_one(
            &mut self,
            _request: ProvisionRequest,
        ) -> Result<ProvisionedInstance, String> {
            Ok(ProvisionedInstance {
                index: 0,
                contract_id: 73,
                offer_id: 11,
                host_id: Some(44),
                gpu_name: "test".to_owned(),
                gpu_ram: None,
                dph_total: 0.0,
            })
        }

        fn ssh_endpoint(
            &mut self,
            _contract_id: u64,
            _label: &str,
            _lifecycle: &LifecyclePolicy,
            ssh_user: &str,
        ) -> Result<VastAiSshEndpoint, String> {
            Ok(VastAiSshEndpoint {
                host: "host".to_owned(),
                port: 22,
                user: ssh_user.to_owned(),
            })
        }

        fn destroy_contract(&mut self, _contract_id: u64) -> Result<(), String> {
            self.destroy_calls.fetch_add(1, Ordering::SeqCst);
            if self
                .destroy_failures
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                    (remaining > 0).then(|| remaining - 1)
                })
                .is_ok()
            {
                Err("transient destroy failure".to_owned())
            } else {
                Ok(())
            }
        }
    }

    struct CountingBootstrap {
        starts: Arc<AtomicUsize>,
        stops: Arc<AtomicUsize>,
    }

    impl VastAiBootstrapLauncher for CountingBootstrap {
        type Handle = ();

        fn start_bootstrap(
            &mut self,
            _spec: NodeProvisionSpec,
            _endpoint: VastAiSshEndpoint,
            _sink: PluginSink,
            _producer: Option<TelemetryProducer>,
            _lifecycle: LifecyclePolicy,
        ) -> Result<Self::Handle, String> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn stop_bootstrap(&mut self, _handle: &mut Self::Handle) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn node_creation_defers_bootstrap_and_failed_destroy_remains_retryable() {
        let destroy_calls = Arc::new(AtomicUsize::new(0));
        let destroy_failures = Arc::new(AtomicUsize::new(1));
        let starts = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let mut plugin = VastAiProvisioningPlugin::new(
            RetryDestroyClient {
                destroy_calls: Arc::clone(&destroy_calls),
                destroy_failures,
                existing_contract: None,
            },
            CountingBootstrap {
                starts: Arc::clone(&starts),
                stops: Arc::clone(&stops),
            },
            VastAiProvisioningConfig::default(),
        );
        let spec = NodeProvisionSpec {
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        };
        let handle = plugin
            .create_node(spec, PluginSink::new(Arc::new(NullSink)))
            .unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 0);

        plugin.start_bootstrap(&handle).unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(
            plugin.stop_node(&handle).unwrap_err(),
            "transient destroy failure"
        );
        assert!(plugin.nodes.contains_key(&handle.id));
        assert!(plugin.leased_host_ids.contains(&44));

        plugin.stop_node(&handle).unwrap();
        assert!(!plugin.nodes.contains_key(&handle.id));
        assert!(!plugin.leased_host_ids.contains(&44));
        assert_eq!(destroy_calls.load(Ordering::SeqCst), 2);
        assert_eq!(stops.load(Ordering::SeqCst), 1);
    }
    #[test]
    fn plugin_adopts_existing_contract_without_starting_bootstrap() {
        let starts = Arc::new(AtomicUsize::new(0));
        let mut plugin = VastAiProvisioningPlugin::new(
            RetryDestroyClient {
                destroy_calls: Arc::new(AtomicUsize::new(0)),
                destroy_failures: Arc::new(AtomicUsize::new(0)),
                existing_contract: Some(73),
            },
            CountingBootstrap {
                starts: Arc::clone(&starts),
                stops: Arc::new(AtomicUsize::new(0)),
            },
            VastAiProvisioningConfig::default(),
        );
        let spec = NodeProvisionSpec {
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: vec!["run-worker".to_owned()],
            mounts: Vec::new(),
        };

        let adopted = plugin
            .adopt_by_spec(&spec, PluginSink::new(Arc::new(NullSink)))
            .unwrap()
            .expect("labeled contract must be adopted");

        assert_eq!(adopted.provider_ref, plugin.label_for(&spec));
        assert_eq!(starts.load(Ordering::SeqCst), 0);
    }

    fn exact_request() -> ProvisionRequest {
        ProvisionRequest {
            count: 1,
            image: "node:v1".to_owned(),
            label: Some("exact-node".to_owned()),
            disk_gb: 10,
            env: BTreeMap::new(),
            per_instance_env: vec![BTreeMap::new()],
            preferred_offer_id: None,
            onstart: None,
            selection: SelectionPolicy {
                drop_cheap_frac: 0.0,
                ..SelectionPolicy::default()
            },
            lifecycle: LifecyclePolicy::default(),
            confirm_lease: false,
        }
    }

    #[test]
    fn offer_search_is_read_only() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = runtime.block_on(MockServer::start());
        runtime.block_on(async {
            Mock::given(method("GET"))
                .and(path("/api/v0/bundles/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "offers": [{
                        "id": 41,
                        "gpu_name": "A",
                        "dph_total": 0.2,
                        "host_id": 1,
                        "compute_cap": 800,
                        "reliability2": 0.99,
                        "inet_down": 500.0,
                        "inet_up": 500.0,
                        "geolocation": "US"
                    }]
                })))
                .mount(&server)
                .await;
        });
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .unwrap();

        let offers = client
            .browse_offers(&OfferBrowseCriteria::default())
            .unwrap();

        assert_eq!(
            offers.iter().map(|offer| offer.id).collect::<Vec<_>>(),
            [41]
        );
        let requests = runtime.block_on(server.received_requests()).unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method.as_str(), "GET");
        assert_eq!(requests[0].url.path(), "/api/v0/bundles/");
    }

    #[test]
    fn exact_offer_creation_never_requests_an_alternative() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = runtime.block_on(MockServer::start());
        runtime.block_on(async {
            Mock::given(method("GET"))
                .and(path("/api/v0/instances/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"instances": []})))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v0/bundles/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "offers": [
                        {"id": 41, "gpu_name": "A", "dph_total": 0.2, "host_id": 1,
                         "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                         "geolocation": "US"},
                        {"id": 42, "gpu_name": "B", "dph_total": 0.3, "host_id": 2,
                         "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                         "geolocation": "US"}
                    ]
                })))
                .mount(&server)
                .await;
            Mock::given(method("PUT"))
                .and(path("/api/v0/asks/42/"))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(json!({"new_contract": 700})),
                )
                .mount(&server)
                .await;
        });
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .unwrap();
        let instance = client.provision_exact(exact_request(), 42).unwrap();
        assert_eq!(instance.offer_id, 42);
        assert_eq!(instance.contract_id, 700);
        let requests = runtime.block_on(server.received_requests()).unwrap();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method.as_str() == "PUT")
                .map(|request| request.url.path().to_owned())
                .collect::<Vec<_>>(),
            vec!["/api/v0/asks/42/"]
        );
    }

    #[test]
    fn unavailable_exact_offer_fails_without_any_create_request() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let server = runtime.block_on(MockServer::start());
        runtime.block_on(async {
            Mock::given(method("GET"))
                .and(path("/api/v0/instances/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"instances": []})))
                .mount(&server)
                .await;
            Mock::given(method("GET"))
                .and(path("/api/v0/bundles/"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                    "offers": [{
                        "id": 41, "gpu_name": "A", "dph_total": 0.2, "host_id": 1,
                        "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                        "geolocation": "US"
                    }]
                })))
                .mount(&server)
                .await;
        });
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .unwrap();
        assert!(
            client
                .provision_exact(exact_request(), 42)
                .unwrap_err()
                .contains("selected offer 42")
        );
        let requests = runtime.block_on(server.received_requests()).unwrap();
        assert!(
            requests
                .iter()
                .all(|request| request.method.as_str() != "PUT")
        );
    }
}
