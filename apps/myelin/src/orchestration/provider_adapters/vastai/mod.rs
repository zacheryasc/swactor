// VastAI provider adapter. Actors own provider lifecycle policy; the
// swactor-vastai and swactor-process crates own API and process mechanics.
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::EngineHandle;
use swactor_vastai::{
    BlockingVastClient, CreateInstanceRequest, LifecyclePolicy, Offer, OfferBrowseCriteria,
    ProvisionRequest, ProvisionedInstance, SelectionPolicy, classify_vastai_error,
};
use telemetry::TelemetryProducer;

use crate::observability::provisioning_logs::{BootstrapTelemetryBridge, node_stream_id};
use crate::provisioning::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginSink,
    ProvisionPlugin,
};
use swactor_process::{LineReaderHandle, ProcessStream, ProcessStreamObservation};

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
}

impl VastAiProviderMonitor {
    fn new(runtime: Runtime, actor: ActorAddress) -> Self {
        Self { runtime, actor }
    }

    fn stop(&mut self) {
        let _ = self
            .runtime
            .send_to(self.actor, VastAiProviderMonitorMsg::Stop);
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

    fn contract_by_label_with_retry(
        &mut self,
        label: &str,
        attempts: usize,
        _pace: Duration,
    ) -> Result<Option<u64>, String> {
        for _ in 0..attempts.max(1) {
            let contract = self.contract_by_label(label)?;
            if contract.is_some() {
                return Ok(contract);
            }
        }
        Ok(None)
    }

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
    client: BlockingVastClient,
    actor_host: Option<(Runtime, EngineHandle)>,
}

impl ToolsVastAiLeaseClient {
    pub(crate) fn new(client: swactor_vastai::VastClient) -> Result<Self, String> {
        Ok(Self {
            client: BlockingVastClient::new(client)?,
            actor_host: None,
        })
    }

    pub(crate) fn with_actor_host(mut self, runtime: Runtime, engine: EngineHandle) -> Self {
        self.actor_host = Some((runtime, engine));
        self
    }

    pub(crate) fn from_api_key(api_key: impl Into<String>) -> Result<Self, String> {
        Self::new(swactor_vastai::VastClient::new(api_key))
    }

    pub(crate) fn browse_offers(
        &mut self,
        criteria: &OfferBrowseCriteria,
    ) -> Result<Vec<Offer>, String> {
        self.client.browse_offers(criteria)
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
        self.client.search_offers(&request.selection, 1)
    }

    fn create_from_offer(
        &mut self,
        request: &ProvisionRequest,
        offer: &Offer,
    ) -> Result<ProvisionedInstance, String> {
        let create = Self::create_request_for_offer(request, offer.id);
        let info = self.client.create_instance(&create)?;
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
    engine: EngineHandle,
    last_state: Option<String>,
    state_since: Instant,
    poll: u64,
    stopped: bool,
}

struct VastAiProviderMonitorConfig {
    client: ToolsVastAiLeaseClient,
    contract_id: u64,
    label: String,
    lifecycle: LifecyclePolicy,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    sender: ExternalSender,
    engine: EngineHandle,
}

impl VastAiProviderMonitorActor {
    fn new(config: VastAiProviderMonitorConfig) -> Self {
        let VastAiProviderMonitorConfig {
            client,
            contract_id,
            label,
            lifecycle,
            spec,
            sink,
            sender,
            engine,
        } = config;
        Self {
            client,
            contract_id,
            label,
            lifecycle,
            spec,
            sink,
            sender,
            engine,
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
        self.engine.send_after(
            self.lifecycle.poll_interval,
            self.sender.clone(),
            ctx.self_addr(),
            VastAiProviderMonitorMsg::Poll,
        );
    }

    fn poll_provider(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        self.poll = self.poll.saturating_add(1);
        let poll = self.poll;
        let status = match self.client.client.instance_status(self.contract_id) {
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

impl Clone for ToolsVastAiLeaseClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            actor_host: self.actor_host.clone(),
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
        let instances = self.client.list_by_label(label)?;
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

    fn contract_by_label_with_retry(
        &mut self,
        label: &str,
        attempts: usize,
        pace: Duration,
    ) -> Result<Option<u64>, String> {
        let instances = self
            .client
            .list_by_label_with_retry(label, attempts, pace)?;
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
        let endpoint = self
            .client
            .wait_for_ssh_endpoint(contract_id, label, lifecycle)?;
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
        let (runtime, engine) = self.actor_host.as_ref()?.clone();
        let sender = runtime.create_sender();
        let actor = runtime
            .spawn(VastAiProviderMonitorActor::new(
                VastAiProviderMonitorConfig {
                    client: self.clone(),
                    contract_id,
                    label,
                    lifecycle,
                    spec,
                    sink,
                    sender,
                    engine,
                },
            ))
            .ok()?;
        Some(VastAiProviderMonitor::new(runtime, actor))
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
        self.client.destroy_instance_with_retry(contract_id)
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
    type Incoming = ProcessStreamObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        let message = match observation {
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
struct SshBootstrapActor {
    bridge: BootstrapTelemetryBridge,
    endpoint: VastAiSshEndpoint,
    ssh_identity: Option<PathBuf>,
    sender: ExternalSender,
    engine: EngineHandle,
    child: Option<Child>,
    reader_relay: Option<ActorAddress>,
    stdout_reader: Option<LineReaderHandle>,
    stderr_reader: Option<LineReaderHandle>,
    stdout_closed: bool,
    stderr_closed: bool,
    pending_status: Option<std::process::ExitStatus>,
    pending_wait_error: Option<String>,
    attempt: u64,
    backoff: Duration,
    observation_class: Option<&'static str>,
    stopped: bool,
    #[cfg(test)]
    disable_attempt_spawn: bool,
    #[cfg(test)]
    spawn_pending_test_child: bool,
}

impl SshBootstrapActor {
    fn new(
        bridge: BootstrapTelemetryBridge,
        endpoint: VastAiSshEndpoint,
        ssh_identity: Option<PathBuf>,
        sender: ExternalSender,
        engine: EngineHandle,
    ) -> Self {
        Self {
            bridge,
            endpoint,
            ssh_identity,
            sender,
            engine,
            child: None,
            stdout_reader: None,
            stderr_reader: None,
            stdout_closed: true,
            reader_relay: None,
            stderr_closed: true,
            pending_status: None,
            pending_wait_error: None,
            attempt: 1,
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

    #[cfg(test)]
    fn with_attempt_spawn_disabled(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self
    }

    #[cfg(test)]
    fn with_pending_test_child(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self.spawn_pending_test_child = true;
        self
    }

    #[cfg(test)]
    fn with_open_test_streams(mut self) -> Self {
        self.disable_attempt_spawn = true;
        self.stdout_closed = false;
        self.stderr_closed = false;
        self
    }

    fn schedule(&self, ctx: &Ctx, message: SshBootstrapMsg, delay: Duration) {
        self.engine
            .send_after(delay, self.sender.clone(), ctx.self_addr(), message);
    }

    fn start_attempt(&mut self, ctx: &Ctx) {
        if self.stopped {
            return;
        }
        self.bridge.observe_provider_line(format!(
            "VastAI SSH bootstrap attempt {} to {}@{}:{}",
            self.attempt, self.endpoint.user, self.endpoint.host, self.endpoint.port
        ));

        #[cfg(test)]
        let attempt = if self.disable_attempt_spawn {
            if self.spawn_pending_test_child {
                spawn_pending_test_child()
            } else {
                return;
            }
        } else {
            spawn_ssh_bootstrap_attempt(
                self.bridge.spec(),
                &self.endpoint,
                self.ssh_identity.as_deref(),
            )
        };
        #[cfg(not(test))]
        let attempt = spawn_ssh_bootstrap_attempt(
            self.bridge.spec(),
            &self.endpoint,
            self.ssh_identity.as_deref(),
        );

        match attempt {
            Ok((child, stdout, stderr)) => {
                self.child = Some(child);
                self.stdout_closed = false;
                self.stderr_closed = false;
                self.observation_class = None;
                self.pending_status = None;
                self.pending_wait_error = None;
                let reader_relay = self
                    .reader_relay
                    .expect("SSH output relay is installed before attempts start");
                self.stdout_reader = Some(swactor_process::spawn_line_reader(
                    ProcessStream::Stdout,
                    stdout,
                    self.sender.clone(),
                    reader_relay,
                ));
                self.stderr_reader = Some(swactor_process::spawn_line_reader(
                    ProcessStream::Stderr,
                    stderr,
                    self.sender.clone(),
                    reader_relay,
                ));
                self.schedule(ctx, SshBootstrapMsg::PollChild, Duration::from_millis(100));
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
        match swactor_process::child_try_wait(child) {
            Ok(Some(status)) => {
                self.child = None;
                self.pending_status = Some(status);
                self.maybe_finish_attempt(ctx);
            }
            Ok(None) => {
                self.schedule(ctx, SshBootstrapMsg::PollChild, Duration::from_millis(100));
            }
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

    fn finish_completed_attempt(&mut self, ctx: &Ctx, status: String, success: bool) {
        self.join_readers();
        let readiness = if success {
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
                "status": status,
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
        let success = status.success();
        self.finish_completed_attempt(ctx, status.to_string(), success);
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
        self.schedule(ctx, SshBootstrapMsg::StartAttempt, delay);
    }

    fn join_readers(&mut self) {
        if let Some(reader) = self.stdout_reader.take() {
            reader.join();
        }
        if let Some(reader) = self.stderr_reader.take() {
            reader.join();
        }
        self.stdout_closed = true;
        self.stderr_closed = true;
    }

    fn stop_child(&mut self) {
        stop_ssh_child(&mut self.child);
        self.join_readers();
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
            serde_json::json!({
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
            SshBootstrapMsg::PollChild => self.poll_child(ctx),
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

fn stop_ssh_child(child: &mut Option<Child>) {
    let Some(mut child) = child.take() else {
        return;
    };
    let _ = swactor_process::child_kill(&mut child);
    let _ = swactor_process::child_wait(&mut child);
}

#[cfg(test)]
fn spawn_pending_test_child()
-> Result<(Child, std::process::ChildStdout, std::process::ChildStderr), String> {
    let mut command = Command::new("sh");
    command
        .args(["-c", "read _"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = swactor_process::command_spawn(&mut command)
        .map_err(|error| format!("spawn pending SSH test child: {error}"))?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| "pending SSH test child missing stdout".to_owned())?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| "pending SSH test child missing stderr".to_owned())?;
    Ok((child, stdout, stderr))
}

#[derive(Clone)]
pub(crate) struct SshCommandBootstrapLauncher {
    ssh_identity: Option<PathBuf>,
    runtime: Runtime,
    engine: EngineHandle,
}
pub(crate) struct SshCommandBootstrapHandle {
    actor: ActorAddress,
    runtime: Runtime,
}

impl SshCommandBootstrapLauncher {
    pub(crate) fn new(
        ssh_identity: Option<PathBuf>,
        runtime: Runtime,
        engine: EngineHandle,
    ) -> Self {
        Self {
            ssh_identity,
            runtime,
            engine,
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
                self.engine.clone(),
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

    let mut child = swactor_process::command_spawn(&mut command)
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
        let contract = self.client.contract_by_label_with_retry(
            &label,
            10,
            self.config.lifecycle.lease_pace,
        )?;
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
    use parking_lot::Mutex;
    use proptest::prelude::*;
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use swactor::runtime::{RuntimeConfig, RuntimeParts};
    use swactor_engine::{Engine, SteppingBackend};
    use swactor_vastai::test_http::{TestHttpRoute, TestHttpServer};

    use crate::tests::fuzz_support::{actor_census, advance_and_drive, drive_steps};

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
        let server = TestHttpServer::start(vec![TestHttpRoute::json(
            "GET",
            "/api/v0/instances/",
            200,
            json!({
                "instances": [{
                    "id": 73,
                    "label": "run-5-node-7-attempt-9",
                    "actual_status": "loading",
                    "ssh_host": "",
                    "ssh_port": 0,
                    "public_ipaddr": ""
                }]
            }),
        )])
        .unwrap();
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
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].path, "/api/v0/instances/");
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
        let server = TestHttpServer::start(vec![TestHttpRoute::json(
            "GET",
            "/api/v0/bundles/",
            200,
            json!({
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
            }),
        )])
        .unwrap();
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
        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].method.as_str(), "GET");
        assert_eq!(requests[0].path, "/api/v0/bundles/");
    }

    #[test]
    fn exact_offer_creation_never_requests_an_alternative() {
        let server = TestHttpServer::start(vec![
            TestHttpRoute::json("GET", "/api/v0/instances/", 200, json!({"instances": []})),
            TestHttpRoute::json(
                "GET",
                "/api/v0/bundles/",
                200,
                json!({
                    "offers": [
                        {"id": 41, "gpu_name": "A", "dph_total": 0.2, "host_id": 1,
                         "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                         "geolocation": "US"},
                        {"id": 42, "gpu_name": "B", "dph_total": 0.3, "host_id": 2,
                         "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                         "geolocation": "US"}
                    ]
                }),
            ),
            TestHttpRoute::json("PUT", "/api/v0/asks/42/", 200, json!({"new_contract": 700})),
        ])
        .unwrap();
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .unwrap();
        let instance = client.provision_exact(exact_request(), 42).unwrap();
        assert_eq!(instance.offer_id, 42);
        assert_eq!(instance.contract_id, 700);
        let requests = server.requests();
        assert_eq!(
            requests
                .iter()
                .filter(|request| request.method.as_str() == "PUT")
                .map(|request| request.path.clone())
                .collect::<Vec<_>>(),
            vec!["/api/v0/asks/42/"]
        );
    }

    #[test]
    fn unavailable_exact_offer_fails_without_any_create_request() {
        let server = TestHttpServer::start(vec![
            TestHttpRoute::json("GET", "/api/v0/instances/", 200, json!({"instances": []})),
            TestHttpRoute::json(
                "GET",
                "/api/v0/bundles/",
                200,
                json!({
                    "offers": [{
                        "id": 41, "gpu_name": "A", "dph_total": 0.2, "host_id": 1,
                        "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                        "geolocation": "US"
                    }]
                }),
            ),
        ])
        .unwrap();
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
        let requests = server.requests();
        assert!(
            requests
                .iter()
                .all(|request| request.method.as_str() != "PUT")
        );
    }

    #[derive(Default)]
    struct RecordingSink {
        observations: Mutex<Vec<PluginObservation>>,
    }

    impl PluginObservationSink for RecordingSink {
        fn observe(&self, observation: PluginObservation) {
            self.observations.lock().push(observation);
        }
    }

    fn offer_fixture(id: u64) -> serde_json::Value {
        json!({
            "id": id,
            "gpu_name": "RTX 4090",
            "dph_total": 0.2,
            "gpu_ram": 24_000.0,
            "host_id": id.saturating_add(100),
            "compute_cap": 890,
            "reliability2": 0.99,
            "inet_down": 500.0,
            "inet_up": 250.0,
            "geolocation": "US"
        })
    }

    #[derive(Debug, PartialEq)]
    enum OfferBrowseOutcome {
        Offers(Vec<u64>),
        Rejected(String),
    }

    fn browse_offer_fixture(route: TestHttpRoute) -> (OfferBrowseOutcome, usize) {
        let server = TestHttpServer::start(vec![route]).expect("start offer HTTP fixture");
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .expect("blocking VastAI client");
        let outcome = match client.browse_offers(&OfferBrowseCriteria::default()) {
            Ok(offers) => {
                OfferBrowseOutcome::Offers(offers.into_iter().map(|offer| offer.id).collect())
            }
            Err(error) => OfferBrowseOutcome::Rejected(error),
        };
        (outcome, server.requests().len())
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum TerminalOutcome {
        MonitorRejected {
            run_id: u64,
            node_id: u64,
            reason: String,
        },
        BootstrapStopped {
            run_id: u64,
            node_id: u64,
            attempt: u64,
        },
    }

    #[derive(Debug, PartialEq, Eq)]
    enum TerminalInvariantError {
        MalformedTerminal(String),
        DuplicateTerminal {
            first: TerminalOutcome,
            duplicate: TerminalOutcome,
        },
    }

    fn single_terminal_outcome(
        observations: &[PluginObservation],
    ) -> Result<Option<TerminalOutcome>, TerminalInvariantError> {
        let mut terminal = None;
        for observation in observations {
            let candidate = match observation {
                PluginObservation::Failed {
                    run_id,
                    node_id,
                    reason,
                } => Some(TerminalOutcome::MonitorRejected {
                    run_id: *run_id,
                    node_id: *node_id,
                    reason: reason.clone(),
                }),
                PluginObservation::ProviderLine {
                    run_id: observed_run_id,
                    node_id: observed_node_id,
                    line,
                } => {
                    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
                        continue;
                    };
                    if value.get("type").and_then(serde_json::Value::as_str)
                        != Some("VastAiBootstrapStopped")
                    {
                        continue;
                    }
                    let run_id = value
                        .get("run_id")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| TerminalInvariantError::MalformedTerminal(line.clone()))?;
                    let node_id = value
                        .get("node_id")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| TerminalInvariantError::MalformedTerminal(line.clone()))?;
                    let attempt = value
                        .get("attempt")
                        .and_then(serde_json::Value::as_u64)
                        .ok_or_else(|| TerminalInvariantError::MalformedTerminal(line.clone()))?;
                    if run_id != *observed_run_id || node_id != *observed_node_id {
                        return Err(TerminalInvariantError::MalformedTerminal(line.clone()));
                    }
                    Some(TerminalOutcome::BootstrapStopped {
                        run_id,
                        node_id,
                        attempt,
                    })
                }
                _ => None,
            };
            let Some(candidate) = candidate else {
                continue;
            };
            if let Some(first) = terminal {
                return Err(TerminalInvariantError::DuplicateTerminal {
                    first,
                    duplicate: candidate,
                });
            }
            terminal = Some(candidate);
        }
        Ok(terminal)
    }

    fn observations(recording: &RecordingSink) -> Vec<PluginObservation> {
        recording.observations.lock().clone()
    }

    fn runtime_is_clean(runtime: &Runtime, baseline: usize, actor_limit: usize) -> bool {
        let stats = runtime.stats();
        stats.actors.len() <= baseline.saturating_add(actor_limit)
            && stats.actor_details.iter().all(|actor| !actor.poisoned)
            && stats
                .workers
                .iter()
                .map(|worker| worker.panics)
                .sum::<u64>()
                == 0
    }

    fn monitor_spec(run_id: u64, node_id: u64) -> NodeProvisionSpec {
        NodeProvisionSpec {
            run_id,
            node_id,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        }
    }

    fn monitor_route(contract_id: u64, kind: u8) -> TestHttpRoute {
        let path = format!("/api/v0/instances/{contract_id}/");
        match kind {
            0 => TestHttpRoute::json(
                "GET",
                &path,
                200,
                json!({"instances": {
                    "actual_status": "running",
                    "intended_status": "running",
                    "public_ipaddr": "127.0.0.1",
                    "ssh_port": 22
                }}),
            ),
            1 => TestHttpRoute::json(
                "GET",
                &path,
                200,
                json!({"instances": {
                    "actual_status": "error",
                    "intended_status": "running",
                    "status_msg": "container failed"
                }}),
            ),
            2 => TestHttpRoute::raw("GET", &path, 404, b"missing".to_vec()),
            3 => TestHttpRoute::raw("GET", &path, 200, b"{broken".to_vec()),
            4 => TestHttpRoute::raw("GET", &path, 500, b"retry".to_vec()),
            _ => TestHttpRoute::json(
                "GET",
                &path,
                200,
                json!({"instances": [
                    {"actual_status": "running", "intended_status": "running"},
                    {"actual_status": "error", "intended_status": "running"}
                ]}),
            ),
        }
    }

    struct MonitorHarness {
        server: TestHttpServer,
        runtime: Runtime,
        backend: SteppingBackend,
        _engine: Engine,
        actor: ActorAddress,
        baseline: usize,
        recording: Arc<RecordingSink>,
    }

    fn monitor_harness(contract_id: u64, spec: NodeProvisionSpec, kind: u8) -> MonitorHarness {
        let server = TestHttpServer::start(vec![monitor_route(contract_id, kind)])
            .expect("start monitor HTTP fixture");
        let client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .expect("blocking VastAI client");
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).expect("stepping engine");
        let baseline = runtime.stats().actors.len();
        let recording = Arc::new(RecordingSink::default());
        let actor = runtime
            .spawn(VastAiProviderMonitorActor::new(
                VastAiProviderMonitorConfig {
                    client,
                    contract_id,
                    label: format!(
                        "run-{}-node-{}-attempt-{}",
                        spec.run_id, spec.node_id, spec.attempt_id
                    ),
                    lifecycle: LifecyclePolicy {
                        lease_pace: Duration::ZERO,
                        poll_interval: Duration::from_millis(1),
                        state_timeout: Duration::from_millis(10),
                    },
                    spec,
                    sink: PluginSink::new(recording.clone()),
                    sender: runtime.create_sender(),
                    engine: engine.handle(),
                },
            ))
            .expect("spawn VastAI monitor");
        MonitorHarness {
            server,
            runtime,
            backend,
            _engine: engine,
            actor,
            baseline,
            recording,
        }
    }

    struct SshHarness {
        runtime: Runtime,
        backend: SteppingBackend,
        _engine: Engine,
        actor: ActorAddress,
        baseline: usize,
        recording: Arc<RecordingSink>,
    }

    #[derive(Clone, Copy)]
    enum SshHarnessMode {
        Idle,
        OpenStreams,
        PendingChild,
    }

    fn ssh_harness_with_mode(mode: SshHarnessMode) -> SshHarness {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).expect("stepping engine");
        let baseline = runtime.stats().actors.len();
        let recording = Arc::new(RecordingSink::default());
        let bridge = BootstrapTelemetryBridge::new(
            monitor_spec(5, 7),
            PluginSink::new(recording.clone()),
            None,
        );
        let actor = SshBootstrapActor::new(
            bridge,
            VastAiSshEndpoint {
                host: "scripted.invalid".to_owned(),
                port: 22,
                user: "root".to_owned(),
            },
            None,
            runtime.create_sender(),
            engine.handle(),
        );
        let actor = match mode {
            SshHarnessMode::Idle => actor.with_attempt_spawn_disabled(),
            SshHarnessMode::OpenStreams => actor.with_open_test_streams(),
            SshHarnessMode::PendingChild => actor.with_pending_test_child(),
        };
        let actor = runtime.spawn(actor).expect("spawn SSH bootstrap actor");
        drive_steps(&backend, 8);
        SshHarness {
            runtime,
            backend,
            _engine: engine,
            actor,
            baseline,
            recording,
        }
    }

    fn ssh_harness() -> SshHarness {
        ssh_harness_with_mode(SshHarnessMode::Idle)
    }

    fn malformed_protocol_line(kind: u8, nonce: u16) -> String {
        match kind % 8 {
            0 => "{broken".to_owned(),
            1 => json!({"myelin_stdio_event": 1}).to_string(),
            2 => json!({
                "myelin_stdio_event": 2,
                "kind": "telemetry_frame",
                "channel": "test",
                "payload": {"nonce": nonce}
            })
            .to_string(),
            3 => json!({
                "myelin_stdio_event": 1,
                "kind": "wrong",
                "channel": "test",
                "payload": {"nonce": nonce}
            })
            .to_string(),
            4 => json!({
                "myelin_stdio_event": 1,
                "kind": "telemetry_frame",
                "channel": nonce
            })
            .to_string(),
            5 => json!([1, 2, nonce]).to_string(),
            6 => "null".to_owned(),
            _ => format!("malformed-protocol-{nonce}"),
        }
    }

    #[derive(Clone, Debug)]
    enum SshOutputAction {
        Stdout(String),
        Stderr(String),
        Protocol(u16),
    }

    fn ssh_output_actions() -> impl Strategy<Value = Vec<SshOutputAction>> {
        prop::collection::vec(
            prop_oneof![
                "[ -~]{0,32}".prop_map(SshOutputAction::Stdout),
                "[ -~]{0,32}".prop_map(SshOutputAction::Stderr),
                any::<u16>().prop_map(SshOutputAction::Protocol),
            ],
            0..=32,
        )
    }

    #[derive(Clone, Debug)]
    enum SshOrderingAction {
        Poll,
        Stdout(String),
        ReaderError(bool),
        Advance(u16),
        Stop,
    }

    fn ssh_ordering_actions() -> impl Strategy<Value = Vec<SshOrderingAction>> {
        prop::collection::vec(
            prop_oneof![
                Just(SshOrderingAction::Poll),
                "[ -~]{0,16}".prop_map(SshOrderingAction::Stdout),
                any::<bool>().prop_map(SshOrderingAction::ReaderError),
                (0_u16..=1_000).prop_map(SshOrderingAction::Advance),
                Just(SshOrderingAction::Stop),
            ],
            0..=32,
        )
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn offer_status_classes_are_offers_or_typed_rejections(
            status_index in 0_usize..11,
            empty in any::<bool>(),
        ) {
            let statuses = [200, 201, 206, 204, 300, 302, 400, 404, 429, 500, 503];
            let status = statuses[status_index];
            let expected_ids = if empty { Vec::new() } else { vec![41] };
            let offers = if empty {
                Vec::new()
            } else {
                vec![offer_fixture(41)]
            };
            let (outcome, request_count) = browse_offer_fixture(TestHttpRoute::json(
                "GET",
                "/api/v0/bundles/",
                status,
                json!({"offers": offers}),
            ));
            let should_parse = matches!(status, 200 | 201 | 206);
            prop_assert_eq!(
                matches!(&outcome, OfferBrowseOutcome::Offers(_)),
                should_parse,
                "status={}, outcome={:?}",
                status,
                outcome,
            );
            if should_parse {
                prop_assert_eq!(
                    &outcome,
                    &OfferBrowseOutcome::Offers(expected_ids),
                    "status={}, empty={}, outcome={:?}",
                    status,
                    empty,
                    outcome,
                );
            }
            if let OfferBrowseOutcome::Rejected(error) = &outcome {
                prop_assert!(
                    !error.trim().is_empty(),
                    "status={}, typed rejection was empty: {:?}",
                    status,
                    outcome,
                );
            }
            prop_assert_eq!(
                request_count,
                1,
                "status={}, outcome={:?}",
                status,
                outcome,
            );
        }

        #[test]
        fn malformed_offer_bodies_are_typed_rejections(
            kind in 0_u8..5,
            fuzz in prop::collection::vec(any::<u8>(), 0..=32),
        ) {
            let body = match kind {
                0 => Vec::new(),
                1 => {
                    let mut body = b"{broken".to_vec();
                    body.extend_from_slice(&fuzz);
                    body.push(0xff);
                    body
                }
                2 => {
                    let mut body = b"{\"offers\":[".to_vec();
                    body.extend_from_slice(&fuzz);
                    body.push(0xff);
                    body
                }
                3 => {
                    let mut body = fuzz.clone();
                    body.push(0xff);
                    body
                }
                _ => {
                    let mut body = b"[".to_vec();
                    body.extend_from_slice(&fuzz);
                    body.push(0xff);
                    body
                }
            };
            let (outcome, request_count) = browse_offer_fixture(TestHttpRoute::raw(
                "GET",
                "/api/v0/bundles/",
                200,
                body,
            ));
            prop_assert!(
                matches!(&outcome, OfferBrowseOutcome::Rejected(error) if !error.trim().is_empty()),
                "kind={}, fuzz={:?}, outcome={:?}",
                kind,
                fuzz,
                outcome,
            );
            prop_assert_eq!(
                request_count,
                1,
                "kind={}, fuzz={:?}, outcome={:?}",
                kind,
                fuzz,
                outcome,
            );
        }

        #[test]
        fn wrong_or_missing_offer_fields_are_typed_rejections(kind in 0_u8..8) {
            let body = match kind {
                0 => json!({}),
                1 => json!({"offers": "wrong"}),
                2 => json!({"offers": {}}),
                3 => json!({"offers": [{"gpu_name": "A", "dph_total": 0.2}]}),
                4 => json!({"offers": [{"id": "41", "gpu_name": "A", "dph_total": 0.2}]}),
                5 => json!({"offers": [{"id": 41, "gpu_name": 9, "dph_total": 0.2}]}),
                6 => json!({"offers": [{"id": 41, "gpu_name": "A", "dph_total": "cheap"}]}),
                _ => json!({"offers": null}),
            };
            let (outcome, request_count) = browse_offer_fixture(TestHttpRoute::json(
                "GET",
                "/api/v0/bundles/",
                200,
                body,
            ));
            prop_assert!(
                matches!(&outcome, OfferBrowseOutcome::Rejected(error) if !error.trim().is_empty()),
                "wrong-field kind={}, outcome={:?}",
                kind,
                outcome,
            );
            prop_assert_eq!(
                request_count,
                1,
                "wrong-field kind={}, outcome={:?}",
                kind,
                outcome,
            );
        }

        #[test]
        fn duplicate_offer_records_remain_explicit_values(
            id in 0_u64..=1_000_000,
            repetitions in 2_usize..=32,
        ) {
            let repeated = (0..repetitions).map(|_| offer_fixture(id)).collect::<Vec<_>>();
            let (outcome, request_count) = browse_offer_fixture(TestHttpRoute::json(
                "GET",
                "/api/v0/bundles/",
                200,
                json!({"offers": repeated}),
            ));
            prop_assert_eq!(
                outcome,
                OfferBrowseOutcome::Offers(vec![id; repetitions]),
                "id={}, repetitions={}",
                id,
                repetitions,
            );
            prop_assert_eq!(
                request_count,
                1,
                "id={}, repetitions={}",
                id,
                repetitions,
            );
        }

        #[test]
        fn provider_monitor_preserves_contract_identity_and_cardinality(
            run_id in 1_u64..=10_000,
            node_id in 1_u64..=10_000,
            contract_id in 1_u64..=10_000,
        ) {
            let actions = [
                format!("spawn({contract_id})"),
                "initial_poll".to_owned(),
                "stop".to_owned(),
            ];
            let harness = monitor_harness(contract_id, monitor_spec(run_id, node_id), 0);
            drive_steps(&harness.backend, 8);
            let before_stop = observations(&harness.recording);
            let census = actor_census(&harness.runtime);
            prop_assert!(
                harness.runtime.stats().actors.len() == harness.baseline + 1,
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                before_stop,
                census,
            );
            prop_assert!(
                harness.server.requests().iter().all(|request| {
                    request.path == format!("/api/v0/instances/{contract_id}/")
                }),
                "actions={:?}, outcomes={:?}, requests={:?}, census={}",
                actions,
                before_stop,
                harness.server.requests(),
                census,
            );
            let identities = before_stop
                .iter()
                .filter_map(|observation| match observation {
                    PluginObservation::ProviderLine {
                        run_id: observed_run,
                        node_id: observed_node,
                        line,
                    } => serde_json::from_str::<serde_json::Value>(line)
                        .ok()
                        .filter(|value| {
                            value.get("type").and_then(serde_json::Value::as_str)
                                == Some("VastAiProviderStatusObserved")
                        })
                        .map(|value| (*observed_run, *observed_node, value)),
                    _ => None,
                })
                .collect::<Vec<_>>();
            prop_assert!(
                !identities.is_empty()
                    && identities.iter().all(|(observed_run, observed_node, value)| {
                        *observed_run == run_id
                            && *observed_node == node_id
                            && value.get("run_id").and_then(serde_json::Value::as_u64)
                                == Some(run_id)
                            && value.get("node_id").and_then(serde_json::Value::as_u64)
                                == Some(node_id)
                            && value.get("contract_id").and_then(serde_json::Value::as_u64)
                                == Some(contract_id)
                    }),
                "actions={:?}, identities={:?}, outcomes={:?}, census={}",
                actions,
                identities,
                before_stop,
                census,
            );
            let _ = harness
                .runtime
                .send_to(harness.actor, VastAiProviderMonitorMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(1), 64);
            let final_outcomes = observations(&harness.recording);
            prop_assert!(
                runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                final_outcomes,
                actor_census(&harness.runtime),
            );
        }

        #[test]
        fn provider_monitor_terminal_polling_stops_after_one_typed_outcome(
            terminal_kind in 0_usize..4,
            extra_polls in 0_usize..=32,
        ) {
            let kinds = [1_u8, 2, 3, 5];
            let kind = kinds[terminal_kind];
            let actions = vec!["poll"; extra_polls];
            let harness = monitor_harness(73, monitor_spec(5, 7), kind);
            for _ in 0..extra_polls {
                let _ = harness
                    .runtime
                    .send_to(harness.actor, VastAiProviderMonitorMsg::Poll);
            }
            drive_steps(&harness.backend, 64);
            let first_request_count = harness.server.requests().len();
            advance_and_drive(&harness.backend, Duration::from_secs(1), 64);
            let second_request_count = harness.server.requests().len();
            let outcomes = observations(&harness.recording);
            let terminal = single_terminal_outcome(&outcomes);
            let census = actor_census(&harness.runtime);
            prop_assert_eq!(
                first_request_count,
                second_request_count,
                "kind={}, actions={:?}, outcomes={:?}, census={}",
                kind,
                actions,
                outcomes,
                census,
            );
            prop_assert_eq!(
                first_request_count,
                1,
                "kind={}, actions={:?}, outcomes={:?}, census={}",
                kind,
                actions,
                outcomes,
                census,
            );
            prop_assert!(
                matches!(
                    &terminal,
                    Ok(Some(TerminalOutcome::MonitorRejected { reason, .. }))
                        if reason.contains("[class=")
                ),
                "kind={}, actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                kind,
                actions,
                terminal,
                outcomes,
                census,
            );
            prop_assert!(
                runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "kind={}, actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                kind,
                actions,
                terminal,
                outcomes,
                census,
            );
        }

        #[test]
        fn provider_monitor_poll_stop_orderings_cease_polling(
            retrying in any::<bool>(),
            actions in prop::collection::vec(0_u8..3, 0..=32),
        ) {
            let kind = if retrying { 4 } else { 0 };
            let harness = monitor_harness(73, monitor_spec(5, 7), kind);
            drive_steps(&harness.backend, 8);
            let mut stopped = false;
            let mut stopped_request_count = None;
            for action in &actions {
                match action {
                    0 => {
                        let _ = harness
                            .runtime
                            .send_to(harness.actor, VastAiProviderMonitorMsg::Poll);
                        drive_steps(&harness.backend, 8);
                    }
                    1 => advance_and_drive(
                        &harness.backend,
                        Duration::from_millis(1),
                        8,
                    ),
                    _ => {
                        let _ = harness
                            .runtime
                            .send_to(harness.actor, VastAiProviderMonitorMsg::Stop);
                        drive_steps(&harness.backend, 8);
                        stopped = true;
                        stopped_request_count
                            .get_or_insert_with(|| harness.server.requests().len());
                    }
                }
                let outcomes = observations(&harness.recording);
                prop_assert!(
                    runtime_is_clean(&harness.runtime, harness.baseline, 1),
                    "actions={:?}, stopped={}, outcomes={:?}, census={}",
                    actions,
                    stopped,
                    outcomes,
                    actor_census(&harness.runtime),
                );
                if let Some(count) = stopped_request_count {
                    prop_assert_eq!(
                        harness.server.requests().len(),
                        count,
                        "polling resumed after stop; actions={:?}, outcomes={:?}, census={}",
                        actions,
                        outcomes,
                        actor_census(&harness.runtime),
                    );
                }
            }
            let _ = harness
                .runtime
                .send_to(harness.actor, VastAiProviderMonitorMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(1), 64);
            let settled_requests = harness.server.requests().len();
            advance_and_drive(&harness.backend, Duration::from_secs(1), 64);
            let outcomes = observations(&harness.recording);
            prop_assert_eq!(
                harness.server.requests().len(),
                settled_requests,
                "polling did not cease; actions={:?}, stopped={}, outcomes={:?}, census={}",
                actions,
                stopped,
                outcomes,
                actor_census(&harness.runtime),
            );
            prop_assert!(
                runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, stopped={}, outcomes={:?}, census={}",
                actions,
                stopped,
                outcomes,
                actor_census(&harness.runtime),
            );
        }

        #[test]
        fn duplicate_terminal_detector_rejects_controlled_fault(
            run_id in any::<u64>(),
            node_id in any::<u64>(),
        ) {
            let injected = vec![
                PluginObservation::ProviderLine {
                    run_id,
                    node_id,
                    line: json!({
                        "type": "VastAiBootstrapStopped",
                        "run_id": run_id,
                        "node_id": node_id,
                        "attempt": 1,
                    })
                    .to_string(),
                },
                PluginObservation::ProviderLine {
                    run_id,
                    node_id,
                    line: json!({
                        "type": "VastAiBootstrapStopped",
                        "run_id": run_id,
                        "node_id": node_id,
                        "attempt": 2,
                    })
                    .to_string(),
                },
            ];
            let detected = single_terminal_outcome(&injected);
            prop_assert!(
                matches!(
                    &detected,
                    Err(TerminalInvariantError::DuplicateTerminal { .. })
                ),
                "controlled duplicate terminal escaped detector; actions=[inject_first, inject_duplicate], outcomes={:?}, detected={:?}",
                injected,
                detected,
            );
        }

        #[test]
        fn ssh_bootstrap_output_lines_preserve_stream_and_protocol(actions in ssh_output_actions()) {
            let harness = ssh_harness();
            let initial_census = actor_census(&harness.runtime);
            prop_assert!(
                harness.runtime.stats().actors.len() == harness.baseline + 2,
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                observations(&harness.recording),
                initial_census,
            );
            for action in &actions {
                let message = match action {
                    SshOutputAction::Stdout(line) => SshBootstrapMsg::OutputLine {
                        stream: SshBootstrapStream::Stdout,
                        line: line.clone(),
                    },
                    SshOutputAction::Stderr(line) => SshBootstrapMsg::OutputLine {
                        stream: SshBootstrapStream::Stderr,
                        line: line.clone(),
                    },
                    SshOutputAction::Protocol(nonce) => SshBootstrapMsg::OutputLine {
                        stream: SshBootstrapStream::Stdout,
                        line: json!({
                            "myelin_stdio_event": 1,
                            "kind": "telemetry_frame",
                            "channel": "generated",
                            "payload": {"nonce": nonce}
                        })
                        .to_string(),
                    },
                };
                harness
                    .runtime
                    .send_to(harness.actor, message)
                    .unwrap_or_else(|error| {
                        panic!(
                            "queue SSH output failed: error={error}, action={action:?}, actions={actions:?}, outcomes={:?}, census={}",
                            observations(&harness.recording),
                            actor_census(&harness.runtime),
                        )
                    });
                drive_steps(&harness.backend, 4);
            }
            let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(31), 128);
            let outcomes = observations(&harness.recording);
            let expected_stdout = actions
                .iter()
                .filter(|action| matches!(action, SshOutputAction::Stdout(_)))
                .count();
            let expected_stderr = actions
                .iter()
                .filter(|action| matches!(action, SshOutputAction::Stderr(_)))
                .count();
            let expected_protocol = actions
                .iter()
                .filter(|action| matches!(action, SshOutputAction::Protocol(_)))
                .count();
            prop_assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, PluginObservation::StdoutLine { .. }))
                    .count(),
                expected_stdout,
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                outcomes,
                actor_census(&harness.runtime),
            );
            prop_assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, PluginObservation::StderrLine { .. }))
                    .count(),
                expected_stderr,
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                outcomes,
                actor_census(&harness.runtime),
            );
            prop_assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, PluginObservation::TelemetryFrame { .. }))
                    .count(),
                expected_protocol,
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                outcomes,
                actor_census(&harness.runtime),
            );
            let terminal = single_terminal_outcome(&outcomes);
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::BootstrapStopped { .. }))),
                "actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
            prop_assert!(
                runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
        }

        #[test]
        fn ssh_bootstrap_malformed_protocol_is_data_not_poison(
            actions in prop::collection::vec((0_u8..8, any::<u16>()), 0..=32),
        ) {
            let harness = ssh_harness();
            for (kind, nonce) in &actions {
                harness
                    .runtime
                    .send_to(
                        harness.actor,
                        SshBootstrapMsg::OutputLine {
                            stream: SshBootstrapStream::Stdout,
                            line: malformed_protocol_line(*kind, *nonce),
                        },
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "queue malformed SSH protocol failed: error={error}, action=({kind}, {nonce}), actions={actions:?}, outcomes={:?}, census={}",
                            observations(&harness.recording),
                            actor_census(&harness.runtime),
                        )
                    });
                drive_steps(&harness.backend, 4);
            }
            let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(31), 128);
            let outcomes = observations(&harness.recording);
            prop_assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, PluginObservation::StdoutLine { .. }))
                    .count(),
                actions.len(),
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                outcomes,
                actor_census(&harness.runtime),
            );
            prop_assert_eq!(
                outcomes
                    .iter()
                    .filter(|outcome| matches!(outcome, PluginObservation::TelemetryFrame { .. }))
                    .count(),
                0,
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                outcomes,
                actor_census(&harness.runtime),
            );
            let terminal = single_terminal_outcome(&outcomes);
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::BootstrapStopped { .. })))
                    && runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
        }

        #[test]
        fn ssh_bootstrap_eof_orderings_stop_relay_and_actor(
            generated_actions in prop::collection::vec(any::<bool>(), 0..=30),
        ) {
            let mut actions = generated_actions;
            actions.push(false);
            actions.push(true);
            let harness = ssh_harness_with_mode(SshHarnessMode::OpenStreams);
            for stderr in &actions {
                harness
                    .runtime
                    .send_to(
                        harness.actor,
                        SshBootstrapMsg::ReaderClosed {
                            stream: if *stderr {
                                SshBootstrapStream::Stderr
                            } else {
                                SshBootstrapStream::Stdout
                            },
                        },
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "queue SSH EOF failed: error={error}, action={stderr}, actions={actions:?}, outcomes={:?}, census={}",
                            observations(&harness.recording),
                            actor_census(&harness.runtime),
                        )
                    });
                drive_steps(&harness.backend, 4);
            }
            let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(31), 128);
            let outcomes = observations(&harness.recording);
            let terminal = single_terminal_outcome(&outcomes);
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::BootstrapStopped { .. })))
                    && runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
        }

        #[test]
        fn ssh_bootstrap_child_failures_have_typed_attempt_outcomes(
            actions in prop::collection::vec(1_i32..=255, 1..=32),
        ) {
            let harness = ssh_harness();
            for status in &actions {
                harness
                    .runtime
                    .send_to(
                        harness.actor,
                        SshBootstrapMsg::ScriptedAttemptFinished {
                            result: Ok(*status),
                        },
                    )
                    .unwrap_or_else(|error| {
                        panic!(
                            "queue SSH child failure failed: error={error}, action={status}, actions={actions:?}, outcomes={:?}, census={}",
                            observations(&harness.recording),
                            actor_census(&harness.runtime),
                        )
                    });
                drive_steps(&harness.backend, 4);
            }
            let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(31), 128);
            let outcomes = observations(&harness.recording);
            let completions = outcomes
                .iter()
                .filter_map(|outcome| match outcome {
                    PluginObservation::ProviderLine { line, .. } => {
                        serde_json::from_str::<serde_json::Value>(line).ok()
                    }
                    _ => None,
                })
                .filter(|value| {
                    value.get("type").and_then(serde_json::Value::as_str)
                        == Some("VastAiBootstrapAttemptCompleted")
                        && value
                            .get("classification")
                            .and_then(serde_json::Value::as_str)
                            == Some("not ready before runtime ready")
                })
                .count();
            prop_assert_eq!(
                completions,
                actions.len(),
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                outcomes,
                actor_census(&harness.runtime),
            );
            let terminal = single_terminal_outcome(&outcomes);
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::BootstrapStopped { .. })))
                    && runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
        }

        #[test]
        fn ssh_bootstrap_timeout_is_typed_and_stops_polling(
            advances in prop::collection::vec(0_u16..=1_000, 0..=31),
        ) {
            let harness = ssh_harness_with_mode(SshHarnessMode::PendingChild);
            advance_and_drive(&harness.backend, Duration::from_millis(100), 8);
            let initial_outcomes = observations(&harness.recording);
            prop_assert!(
                harness.runtime.stats().actors.len() == harness.baseline + 2
                    && !initial_outcomes.iter().any(|outcome| matches!(
                        outcome,
                        PluginObservation::ProviderLine { line, .. }
                            if line.contains("spawn VastAI SSH bootstrap attempt")
                                && line.contains("failed")
                    )),
                "actions=[start_pending_child, poll, timeout, {:?}, stop], outcomes={:?}, census={}",
                advances,
                initial_outcomes,
                actor_census(&harness.runtime),
            );
            harness
                .runtime
                .send_to(
                    harness.actor,
                    SshBootstrapMsg::ScriptedAttemptFinished {
                        result: Err("bootstrap timeout".to_owned()),
                    },
                )
                .unwrap_or_else(|error| {
                    panic!(
                        "queue SSH timeout failed: error={error}, actions=[timeout, {advances:?}], outcomes={:?}, census={}",
                        observations(&harness.recording),
                        actor_census(&harness.runtime),
                    )
                });
            drive_steps(&harness.backend, 4);
            for millis in &advances {
                advance_and_drive(
                    &harness.backend,
                    Duration::from_millis(u64::from(*millis)),
                    4,
                );
            }
            let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(31), 128);
            let outcomes = observations(&harness.recording);
            prop_assert!(
                outcomes.iter().any(|outcome| matches!(
                    outcome,
                    PluginObservation::ProviderLine { line, .. }
                        if line.contains("bootstrap timeout") && line.contains("retrying")
                )),
                "actions=[timeout, {:?}, stop], outcomes={:?}, census={}",
                advances,
                outcomes,
                actor_census(&harness.runtime),
            );
            let settled_attempts = outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    PluginObservation::ProviderLine { line, .. }
                        if line.contains("VastAI SSH bootstrap attempt")
                ))
                .count();
            advance_and_drive(&harness.backend, Duration::from_secs(31), 128);
            let final_outcomes = observations(&harness.recording);
            let final_attempts = final_outcomes
                .iter()
                .filter(|outcome| matches!(
                    outcome,
                    PluginObservation::ProviderLine { line, .. }
                        if line.contains("VastAI SSH bootstrap attempt")
                ))
                .count();
            let terminal = single_terminal_outcome(&final_outcomes);
            prop_assert_eq!(
                final_attempts,
                settled_attempts,
                "polling resumed after timeout stop; actions=[timeout, {:?}, stop], terminal={:?}, outcomes={:?}, census={}",
                advances,
                terminal,
                final_outcomes,
                actor_census(&harness.runtime),
            );
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::BootstrapStopped { .. })))
                    && runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions=[timeout, {:?}, stop], terminal={:?}, outcomes={:?}, census={}",
                advances,
                terminal,
                final_outcomes,
                actor_census(&harness.runtime),
            );
        }

        #[test]
        fn ssh_bootstrap_stop_orderings_emit_one_terminal_and_stop_all_actors(
            actions in ssh_ordering_actions(),
        ) {
            let harness = ssh_harness();
            let mut stopped = false;
            for action in &actions {
                match action {
                    SshOrderingAction::Poll => {
                        let _ = harness
                            .runtime
                            .send_to(harness.actor, SshBootstrapMsg::PollChild);
                        drive_steps(&harness.backend, 4);
                    }
                    SshOrderingAction::Stdout(line) => {
                        let _ = harness.runtime.send_to(
                            harness.actor,
                            SshBootstrapMsg::OutputLine {
                                stream: SshBootstrapStream::Stdout,
                                line: line.clone(),
                            },
                        );
                        drive_steps(&harness.backend, 4);
                    }
                    SshOrderingAction::ReaderError(stderr) => {
                        let _ = harness.runtime.send_to(
                            harness.actor,
                            SshBootstrapMsg::ReaderError {
                                stream: if *stderr {
                                    SshBootstrapStream::Stderr
                                } else {
                                    SshBootstrapStream::Stdout
                                },
                                error: "scripted reader failure".to_owned(),
                            },
                        );
                        drive_steps(&harness.backend, 4);
                    }
                    SshOrderingAction::Advance(millis) => advance_and_drive(
                        &harness.backend,
                        Duration::from_millis(u64::from(*millis)),
                        4,
                    ),
                    SshOrderingAction::Stop => {
                        let _ = harness
                            .runtime
                            .send_to(harness.actor, SshBootstrapMsg::Stop);
                        drive_steps(&harness.backend, 4);
                        stopped = true;
                    }
                }
                let outcomes = observations(&harness.recording);
                prop_assert!(
                    runtime_is_clean(&harness.runtime, harness.baseline, 2),
                    "actions={:?}, stopped={}, outcomes={:?}, census={}",
                    actions,
                    stopped,
                    outcomes,
                    actor_census(&harness.runtime),
                );
            }
            let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Stop);
            advance_and_drive(&harness.backend, Duration::from_secs(31), 128);
            let outcomes = observations(&harness.recording);
            let terminal = single_terminal_outcome(&outcomes);
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::BootstrapStopped { .. }))),
                "actions={:?}, stopped={}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                stopped,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
            prop_assert!(
                runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, stopped={}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                stopped,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
        }
    }
}
