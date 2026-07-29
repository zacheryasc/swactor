use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use datastream::DatastreamProducer;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime, RuntimeConfig, RuntimeHandle};
use swactor_vastai::{
    CreateInstanceRequest, LifecyclePolicy, Offer, ProvisionRequest, ProvisionedInstance,
    SelectionPolicy, classify_vastai_error, create_instance,
};

use crate::observability::provisioning_logs::{BootstrapDatastreamBridge, node_stream_id};
use crate::provisioning::{
    NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginSink, ProvisionPlugin,
};

#[derive(Clone, Debug)]
pub struct VastAiProvisioningConfig {
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
            label_prefix: "mvp".to_owned(),
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
pub struct VastAiSshEndpoint {
    pub host: String,
    pub port: u16,
    pub user: String,
}

pub struct VastAiProviderMonitor {
    runtime: Option<RuntimeHandle>,
    actor: ActorAddress,
}

impl VastAiProviderMonitor {
    fn new(runtime: RuntimeHandle, actor: ActorAddress) -> Self {
        Self {
            runtime: Some(runtime),
            actor,
        }
    }

    fn stop(&mut self) {
        let Some(runtime) = self.runtime.take() else {
            return;
        };
        let _ = runtime
            .runtime
            .send_to(self.actor, VastAiProviderMonitorMsg::Stop);
        runtime.shutdown();
        runtime.join();
    }
}

impl Drop for VastAiProviderMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

pub trait VastAiLeaseClient: Send {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String>;
    fn plan_first_wave_offers(
        &mut self,
        requests: &[ProvisionRequest],
    ) -> Result<Vec<Option<u64>>, String> {
        Ok(vec![None; requests.len()])
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

pub struct ToolsVastAiLeaseClient {
    client: swactor_vastai::VastClient,
    runtime: tokio::runtime::Runtime,
    planned_offer_pool: Arc<Mutex<Vec<Offer>>>,
    planned_offer_ids: Arc<Mutex<HashSet<u64>>>,
}

impl ToolsVastAiLeaseClient {
    pub fn new(client: swactor_vastai::VastClient) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("vastai tokio runtime: {e}"))?;
        Ok(Self {
            client,
            runtime,
            planned_offer_pool: Arc::new(Mutex::new(Vec::new())),
            planned_offer_ids: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    pub fn from_api_key(api_key: impl Into<String>) -> Result<Self, String> {
        Self::new(swactor_vastai::VastClient::new(api_key))
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
        let cached = self.planned_offer_pool.lock().clone();
        if request
            .preferred_offer_id
            .is_some_and(|offer_id| cached.iter().any(|offer| offer.id == offer_id))
        {
            return Ok(cached);
        }
        self.runtime
            .block_on(self.client.search_offers(&request.selection, 1))
    }

    fn create_from_offer(
        &mut self,
        request: &ProvisionRequest,
        offer: &Offer,
    ) -> Result<ProvisionedInstance, String> {
        let create = Self::create_request_for_offer(request, offer.id);
        let info = self.runtime.block_on(create_instance(
            self.client.http(),
            self.client.base_url(),
            self.client.api_key(),
            &create,
        ))?;
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
            planned_offer_pool: Arc::clone(&self.planned_offer_pool),
            planned_offer_ids: Arc::clone(&self.planned_offer_ids),
        }
    }
}

impl VastAiLeaseClient for ToolsVastAiLeaseClient {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String> {
        if request.count != 1 {
            return Err(format!(
                "vastai provision_one expected count=1, got {}",
                request.count
            ));
        }

        let pool = self.candidate_pool(&request)?;
        let planned_offer_ids = self.planned_offer_ids.lock().clone();
        let blocked_hosts = request
            .selection
            .blacklist_hosts
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut failed_hosts = HashSet::new();
        let mut tried_offer_ids = HashSet::new();
        let mut ordered = Vec::with_capacity(pool.len());
        if let Some(preferred_offer_id) = request.preferred_offer_id
            && let Some(offer) = pool.iter().find(|offer| offer.id == preferred_offer_id)
        {
            ordered.push(offer.clone());
        }
        ordered.extend(pool.into_iter());

        let mut last_error = None;
        for offer in ordered {
            if !tried_offer_ids.insert(offer.id) {
                continue;
            }
            if request.preferred_offer_id != Some(offer.id) && planned_offer_ids.contains(&offer.id)
            {
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

    fn plan_first_wave_offers(
        &mut self,
        requests: &[ProvisionRequest],
    ) -> Result<Vec<Option<u64>>, String> {
        let Some(first) = requests.first() else {
            self.planned_offer_pool.lock().clear();
            self.planned_offer_ids.lock().clear();
            return Ok(Vec::new());
        };
        let pool = self.runtime.block_on(
            self.client
                .search_offers(&first.selection, requests.len() as u32),
        )?;
        let planned = swactor_vastai::plan_distinct_host_first_wave(
            &pool,
            requests.len() as u32,
            &first.selection.blacklist_hosts,
            &[],
        );
        let planned_ids = planned.iter().map(|offer| offer.id).collect::<HashSet<_>>();
        *self.planned_offer_pool.lock() = pool;
        *self.planned_offer_ids.lock() = planned_ids;
        let mut out = planned
            .into_iter()
            .map(|offer| Some(offer.id))
            .collect::<Vec<_>>();
        out.resize(requests.len(), None);
        Ok(out)
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
        endpoint_from_parts(contract_id, endpoint.ip, endpoint.port, ssh_user)
    }

    fn spawn_provider_monitor(
        &mut self,
        contract_id: u64,
        label: String,
        lifecycle: LifecyclePolicy,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Option<VastAiProviderMonitor> {
        let runtime = Runtime::new(RuntimeConfig::default());
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
        let runtime = runtime.run().ok()?;
        Some(VastAiProviderMonitor::new(runtime, actor))
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
        self.runtime
            .block_on(swactor_vastai::destroy_instance_with_retry(
                self.client.http(),
                self.client.base_url(),
                self.client.api_key(),
                contract_id,
            ))
    }
}

fn endpoint_from_parts(
    contract_id: u64,
    host: String,
    port: u16,
    ssh_user: &str,
) -> Result<VastAiSshEndpoint, String> {
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BootstrapStopReason {
    RuntimeReady,
    NodeStop,
}

pub trait VastAiBootstrapLauncher: Send {
    type Handle: Send;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: VastAiSshEndpoint,
        sink: PluginSink,
        producer: Option<DatastreamProducer>,
        lifecycle: LifecyclePolicy,
    ) -> Result<Self::Handle, String>;

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle, reason: BootstrapStopReason);
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
    bridge: BootstrapDatastreamBridge,
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
    start_on_boot: bool,
}

impl SshBootstrapActor {
    fn new(
        bridge: BootstrapDatastreamBridge,
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
            start_on_boot: true,
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
        self.backoff = next_ssh_backoff(self.backoff);
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
        if self.start_on_boot {
            let _ = ctx.send(ctx.self_addr(), SshBootstrapMsg::StartAttempt);
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
pub struct SshCommandBootstrapLauncher {
    ssh_identity: Option<PathBuf>,
    runtime: Arc<Runtime>,
}
pub struct SshCommandBootstrapHandle {
    actor: ActorAddress,
    runtime: Arc<Runtime>,
}

impl SshCommandBootstrapLauncher {
    pub fn new(ssh_identity: Option<PathBuf>, runtime: Arc<Runtime>) -> Self {
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
        producer: Option<DatastreamProducer>,
        _lifecycle: LifecyclePolicy,
    ) -> Result<Self::Handle, String> {
        if spec.args.is_empty() {
            return Err(format!(
                "VastAI node {} SSH bootstrap command missing",
                spec.node_id
            ));
        }

        let sender = self.runtime.create_sender();
        let bridge = BootstrapDatastreamBridge::new(spec, sink, producer);
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
            runtime: Arc::clone(&self.runtime),
        })
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle, _reason: BootstrapStopReason) {
        let _ = handle.runtime.send_to(handle.actor, SshBootstrapMsg::Stop);
        handle.runtime.tick();
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
    let mut command = Command::new("ssh");
    command
        .args(ssh_bootstrap_args(
            endpoint,
            &spec.args.join(" "),
            ssh_identity,
        ))
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

fn next_ssh_backoff(current: Duration) -> Duration {
    std::cmp::min(current.saturating_mul(2), Duration::from_secs(30))
}

pub struct VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient,
    B: VastAiBootstrapLauncher,
{
    client: C,
    bootstrap: B,
    config: VastAiProvisioningConfig,
    bootstrap_producer: Option<DatastreamProducer>,
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
}

impl<C, B> VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient,
    B: VastAiBootstrapLauncher,
{
    pub fn new(client: C, bootstrap: B, config: VastAiProvisioningConfig) -> Self {
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
            "{}-{}-{}",
            self.config.label_prefix, spec.run_id, spec.node_id
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
}

fn classified_start_error(reason: String) -> String {
    let class = classify_vastai_error(&reason).as_str();
    format!("{reason} [class={class}]")
}

struct VastAiStartedLease {
    label: String,
    instance: ProvisionedInstance,
    endpoint: VastAiSshEndpoint,
}

struct VastAiBatchStartError {
    reason: String,
    failed_host_id: Option<u64>,
}

impl<C, B> ProvisionPlugin for VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient + Clone + 'static,
    B: VastAiBootstrapLauncher + 'static,
{
    fn start_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        if !spec.mounts.is_empty() {
            return Err("vastai provider does not support host file mounts".to_owned());
        }
        let stream_id = node_stream_id(spec.run_id, spec.node_id);
        let label = self.label_for(&spec);
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: format!("vastai provisioning label={label} stream={stream_id}"),
        });

        let request = self.build_request(&spec, label.clone());
        let instance = self.client.provision_one(request).map_err(|e| {
            classified_start_error(format!("vastai provision node {}: {e}", spec.node_id))
        })?;
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: format!(
                "vastai contract {} ready for SSH lookup",
                instance.contract_id
            ),
        });
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: serde_json::json!({
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
        });
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: serde_json::json!({
                "type": "VastAiSshEndpointDiscoveryStarted",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "contract_id": instance.contract_id,
                "label": &label,
            })
            .to_string(),
        });

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
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: serde_json::json!({
                "type": "VastAiSshEndpointReady",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "contract_id": instance.contract_id,
                "host": &endpoint.host,
                "port": endpoint.port,
                "user": &endpoint.user,
            })
            .to_string(),
        });

        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: serde_json::json!({
                "type": "VastAiBootstrapObservationStarted",
                "run_id": spec.run_id,
                "node_id": spec.node_id,
                "contract_id": instance.contract_id,
                "host": &endpoint.host,
                "port": endpoint.port,
                "user": &endpoint.user,
            })
            .to_string(),
        });

        let bootstrap = match self.bootstrap.start_bootstrap(
            spec.clone(),
            endpoint,
            sink.clone(),
            self.bootstrap_producer.clone(),
            self.config.lifecycle.clone(),
        ) {
            Ok(handle) => handle,
            Err(error) => {
                if let Some(host_id) = instance.host_id {
                    self.failed_host_ids.insert(host_id);
                }
                return Err(self.cleanup_contract_after_start_error(
                    instance.contract_id,
                    classified_start_error(format!(
                        "vastai bootstrap node {}: {error}",
                        spec.node_id
                    )),
                ));
            }
        };

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
                bootstrap: Some(bootstrap),
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
            },
        );
        Ok(handle)
    }

    fn start_nodes(
        &mut self,
        specs: Vec<NodeProvisionSpec>,
        sink: PluginSink,
    ) -> Vec<(NodeProvisionSpec, Result<PluginNodeHandle, String>)> {
        if specs.len() <= 1 {
            return specs
                .into_iter()
                .map(|spec| {
                    let result = self.start_node(spec.clone(), sink.clone());
                    (spec, result)
                })
                .collect();
        }

        let mut results = (0..specs.len()).map(|_| None).collect::<Vec<_>>();
        let mut start_inputs = Vec::new();
        for (index, spec) in specs.into_iter().enumerate() {
            if !spec.mounts.is_empty() {
                results[index] = Some((
                    spec,
                    Err("vastai provider does not support host file mounts".to_owned()),
                ));
                continue;
            }
            let stream_id = node_stream_id(spec.run_id, spec.node_id);
            let label = self.label_for(&spec);
            sink.observe(PluginObservation::ProviderLine {
                run_id: spec.run_id,
                node_id: spec.node_id,
                line: format!("vastai provisioning label={label} stream={stream_id}"),
            });
            let request = self.build_request(&spec, label.clone());
            start_inputs.push((index, spec, label, request));
        }

        let request_plan = start_inputs
            .iter()
            .map(|(_, _, _, request)| request.clone())
            .collect::<Vec<_>>();
        let offer_plan = match self.client.plan_first_wave_offers(&request_plan) {
            Ok(plan) => plan,
            Err(error) => {
                for (_, spec, _, _) in &start_inputs {
                    sink.observe(PluginObservation::ProviderLine {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        line: format!(
                            "vastai first-wave offer planning failed; falling back to per-node selection: {error}"
                        ),
                    });
                }
                vec![None; start_inputs.len()]
            }
        };

        let (completion_tx, completion_rx) = mpsc::channel();
        for (plan_index, (index, spec, label, mut request)) in start_inputs.into_iter().enumerate()
        {
            request.preferred_offer_id = offer_plan.get(plan_index).copied().flatten();
            if let Some(offer_id) = request.preferred_offer_id {
                sink.observe(PluginObservation::ProviderLine {
                    run_id: spec.run_id,
                    node_id: spec.node_id,
                    line: serde_json::json!({
                        "type": "VastAiFirstWaveOfferPlanned",
                        "run_id": spec.run_id,
                        "node_id": spec.node_id,
                        "label": &label,
                        "offer_id": offer_id,
                    })
                    .to_string(),
                });
            } else {
                sink.observe(PluginObservation::ProviderLine {
                    run_id: spec.run_id,
                    node_id: spec.node_id,
                    line: serde_json::json!({
                        "type": "VastAiFirstWaveOfferPlanUnavailable",
                        "run_id": spec.run_id,
                        "node_id": spec.node_id,
                        "label": &label,
                    })
                    .to_string(),
                });
            }
            let mut client = self.client.clone();
            let config = self.config.clone();
            let worker_tx = completion_tx.clone();
            let worker_sink = sink.clone();
            std::thread::spawn(move || {
                let started = match client.provision_one(request) {
                    Ok(instance) => {
                        worker_sink.observe(PluginObservation::ProviderLine {
                            run_id: spec.run_id,
                            node_id: spec.node_id,
                            line: serde_json::json!({
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
                        });
                        worker_sink.observe(PluginObservation::ProviderLine {
                            run_id: spec.run_id,
                            node_id: spec.node_id,
                            line: serde_json::json!({
                                "type": "VastAiSshEndpointDiscoveryStarted",
                                "run_id": spec.run_id,
                                "node_id": spec.node_id,
                                "contract_id": instance.contract_id,
                                "label": &label,
                            })
                            .to_string(),
                        });
                        match client.ssh_endpoint(
                            instance.contract_id,
                            &label,
                            &config.lifecycle,
                            &config.ssh_user,
                        ) {
                            Ok(endpoint) => Ok(VastAiStartedLease {
                                label,
                                instance,
                                endpoint,
                            }),
                            Err(error) => {
                                let failed_host_id = instance.host_id;
                                let reason = match client.destroy_contract(instance.contract_id) {
                                    Ok(()) => classified_start_error(format!(
                                        "vastai SSH endpoint node {}: {error}",
                                        spec.node_id
                                    )),
                                    Err(cleanup) => classified_start_error(format!(
                                        "vastai SSH endpoint node {}: {error}; cleanup destroy {} failed: {cleanup}",
                                        spec.node_id, instance.contract_id
                                    )),
                                };
                                Err(VastAiBatchStartError {
                                    reason,
                                    failed_host_id,
                                })
                            }
                        }
                    }
                    Err(error) => Err(VastAiBatchStartError {
                        reason: classified_start_error(format!(
                            "vastai provision node {}: {error}",
                            spec.node_id
                        )),
                        failed_host_id: None,
                    }),
                };
                let _ = worker_tx.send((index, spec, started));
            });
        }
        drop(completion_tx);

        for (index, spec, started) in completion_rx {
            match started {
                Ok(started) => {
                    sink.observe(PluginObservation::ProviderLine {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        line: serde_json::json!({
                            "type": "VastAiSshEndpointReady",
                            "run_id": spec.run_id,
                            "node_id": spec.node_id,
                            "contract_id": started.instance.contract_id,
                            "host": &started.endpoint.host,
                            "port": started.endpoint.port,
                            "user": &started.endpoint.user,
                        })
                        .to_string(),
                    });
                    sink.observe(PluginObservation::ProviderLine {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        line: serde_json::json!({
                            "type": "VastAiBootstrapObservationStarted",
                            "run_id": spec.run_id,
                            "node_id": spec.node_id,
                            "contract_id": started.instance.contract_id,
                            "host": &started.endpoint.host,
                            "port": started.endpoint.port,
                            "user": &started.endpoint.user,
                        })
                        .to_string(),
                    });

                    let bootstrap = match self.bootstrap.start_bootstrap(
                        spec.clone(),
                        started.endpoint,
                        sink.clone(),
                        self.bootstrap_producer.clone(),
                        self.config.lifecycle.clone(),
                    ) {
                        Ok(handle) => handle,
                        Err(error) => {
                            if let Some(host_id) = started.instance.host_id {
                                self.failed_host_ids.insert(host_id);
                            }
                            let node_id = spec.node_id;
                            results[index] = Some((
                                spec,
                                Err(self.cleanup_contract_after_start_error(
                                    started.instance.contract_id,
                                    classified_start_error(format!(
                                        "vastai bootstrap node {node_id}: {error}"
                                    )),
                                )),
                            ));
                            continue;
                        }
                    };

                    let host_id = started.instance.host_id;
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
                            contract_id: started.instance.contract_id,
                            bootstrap: Some(bootstrap),
                            provider_monitor: self.client.spawn_provider_monitor(
                                started.instance.contract_id,
                                started.label.clone(),
                                self.config.lifecycle.clone(),
                                spec.clone(),
                                sink.clone(),
                            ),
                            host_id,
                            run_id: spec.run_id,
                            node_id: spec.node_id,
                            label: started.label,
                            sink: sink.clone(),
                        },
                    );
                    results[index] = Some((spec, Ok(handle)));
                }
                Err(error) => {
                    if let Some(host_id) = error.failed_host_id {
                        self.failed_host_ids.insert(host_id);
                    }
                    results[index] = Some((spec, Err(error.reason)));
                }
            }
        }

        results
            .into_iter()
            .enumerate()
            .map(|(index, result)| {
                result.unwrap_or_else(|| {
                    (
                        NodeProvisionSpec {
                            run_id: 0,
                            node_id: u64::try_from(index).unwrap_or(u64::MAX),
                            stage_index: None,
                            image: String::new(),
                            env: Vec::new(),
                            args: Vec::new(),
                            mounts: Vec::new(),
                        },
                        Err("vastai provision worker panicked".to_owned()),
                    )
                })
            })
            .collect()
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.get_mut(&handle.id) else {
            return Ok(());
        };
        node.sink.observe(PluginObservation::ProviderLine {
            run_id: node.run_id,
            node_id: node.node_id,
            line: serde_json::json!({
                "type": "VastAiRuntimeReadyAccepted",
                "run_id": node.run_id,
                "node_id": node.node_id,
                "label": &node.label,
                "contract_id": node.contract_id,
                "classification": "runtime_ready_over_provider_staleness",
            })
            .to_string(),
        });
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(mut node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        if let Some(mut monitor) = node.provider_monitor.take() {
            monitor.stop();
        }
        if let Some(host_id) = node.host_id {
            self.leased_host_ids.remove(&host_id);
        }
        if let Some(mut bootstrap) = node.bootstrap.take() {
            self.bootstrap
                .stop_bootstrap(&mut bootstrap, BootstrapStopReason::NodeStop);
        }
        let result = self.client.destroy_contract(node.contract_id);
        node.sink.observe(PluginObservation::ProviderLine {
            run_id: node.run_id,
            node_id: node.node_id,
            line: serde_json::json!({
                "type": "VastAiContractCleanup",
                "run_id": node.run_id,
                "node_id": node.node_id,
                "label": &node.label,
                "contract_id": node.contract_id,
                "result": if result.is_ok() { "ok" } else { "failed" },
                "error": result.as_ref().err(),
            })
            .to_string(),
        });
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct NoopLeaseClient;

    impl VastAiLeaseClient for NoopLeaseClient {
        fn provision_one(
            &mut self,
            _request: ProvisionRequest,
        ) -> Result<ProvisionedInstance, String> {
            panic!("build_request tests must not provision a real Vast.ai lease")
        }

        fn ssh_endpoint(
            &mut self,
            _contract_id: u64,
            _label: &str,
            _lifecycle: &LifecyclePolicy,
            _ssh_user: &str,
        ) -> Result<VastAiSshEndpoint, String> {
            panic!("build_request tests must not query a real Vast.ai endpoint")
        }

        fn destroy_contract(&mut self, _contract_id: u64) -> Result<(), String> {
            panic!("build_request tests must not destroy a real Vast.ai lease")
        }
    }

    struct NoopBootstrapLauncher;

    impl VastAiBootstrapLauncher for NoopBootstrapLauncher {
        type Handle = ();

        fn start_bootstrap(
            &mut self,
            _spec: NodeProvisionSpec,
            _endpoint: VastAiSshEndpoint,
            _sink: PluginSink,
            _producer: Option<DatastreamProducer>,
            _lifecycle: LifecyclePolicy,
        ) -> Result<Self::Handle, String> {
            panic!("build_request tests must not start SSH bootstrap")
        }

        fn stop_bootstrap(&mut self, _handle: &mut Self::Handle, _reason: BootstrapStopReason) {
            panic!("build_request tests must not stop SSH bootstrap")
        }
    }

    fn node_spec_with_bootstrap_args() -> NodeProvisionSpec {
        NodeProvisionSpec {
            run_id: 9,
            node_id: 11,
            stage_index: Some(2),
            image: "registry.example.com/mvp-worker:latest".to_owned(),
            env: vec![("EXISTING".to_owned(), "1".to_owned())],
            args: vec!["python".to_owned(), "worker.py".to_owned()],
            mounts: Vec::new(),
        }
    }

    fn plugin_with_onstart(
        onstart: Option<String>,
    ) -> VastAiProvisioningPlugin<NoopLeaseClient, NoopBootstrapLauncher> {
        let config = VastAiProvisioningConfig {
            onstart,
            ..VastAiProvisioningConfig::default()
        };
        VastAiProvisioningPlugin::new(NoopLeaseClient, NoopBootstrapLauncher, config)
    }

    #[derive(Clone, Default)]
    struct ObservationSink {
        observations: Arc<Mutex<Vec<PluginObservation>>>,
    }

    impl crate::provisioning::PluginObservationSink for ObservationSink {
        fn observe(&self, observation: PluginObservation) {
            self.observations.lock().push(observation);
        }
    }

    #[derive(Clone)]
    struct RecordingLeaseClient {
        destroyed_contracts: Arc<Mutex<Vec<u64>>>,
    }

    impl VastAiLeaseClient for RecordingLeaseClient {
        fn provision_one(
            &mut self,
            _request: ProvisionRequest,
        ) -> Result<ProvisionedInstance, String> {
            Ok(ProvisionedInstance {
                index: 0,
                contract_id: 42,
                offer_id: 7,
                host_id: Some(99),
                gpu_name: "RTX 4060".to_owned(),
                gpu_ram: Some(8_192.0),
                dph_total: 0.064,
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
                host: "ssh5.vast.ai".to_owned(),
                port: 22_017,
                user: ssh_user.to_owned(),
            })
        }

        fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
            self.destroyed_contracts.lock().push(contract_id);
            Ok(())
        }
    }

    #[derive(Clone)]
    struct RecordingBootstrapLauncher {
        stop_reasons: Arc<Mutex<Vec<BootstrapStopReason>>>,
    }

    impl VastAiBootstrapLauncher for RecordingBootstrapLauncher {
        type Handle = u64;

        fn start_bootstrap(
            &mut self,
            spec: NodeProvisionSpec,
            _endpoint: VastAiSshEndpoint,
            _sink: PluginSink,
            _producer: Option<DatastreamProducer>,
            _lifecycle: LifecyclePolicy,
        ) -> Result<Self::Handle, String> {
            Ok(spec.node_id)
        }

        fn stop_bootstrap(&mut self, _handle: &mut Self::Handle, reason: BootstrapStopReason) {
            self.stop_reasons.lock().push(reason);
        }
    }

    #[test]
    fn vastai_provisioning_build_request_keeps_bootstrap_args_out_of_onstart() {
        let plugin = plugin_with_onstart(None);
        let request =
            plugin.build_request(&node_spec_with_bootstrap_args(), "test-label".to_owned());

        assert_eq!(request.onstart, None);
        assert_eq!(request.label.as_deref(), Some("test-label"));
        assert_eq!(request.env.get("EXISTING").map(String::as_str), Some("1"));
    }

    #[test]
    fn vastai_provisioning_build_request_uses_explicit_onstart() {
        let plugin = plugin_with_onstart(Some("echo explicit setup".to_owned()));
        let request =
            plugin.build_request(&node_spec_with_bootstrap_args(), "test-label".to_owned());

        assert_eq!(request.onstart.as_deref(), Some("echo explicit setup"));
    }

    #[test]
    fn vastai_complete_bootstrap_keeps_optional_log_tail_until_node_stop() {
        let destroyed_contracts = Arc::new(Mutex::new(Vec::new()));
        let stop_reasons = Arc::new(Mutex::new(Vec::new()));
        let sink = PluginSink::new(Arc::new(ObservationSink::default()));
        let mut plugin = VastAiProvisioningPlugin::new(
            RecordingLeaseClient {
                destroyed_contracts: destroyed_contracts.clone(),
            },
            RecordingBootstrapLauncher {
                stop_reasons: stop_reasons.clone(),
            },
            VastAiProvisioningConfig::default(),
        );

        let handle = plugin
            .start_node(node_spec_with_bootstrap_args(), sink)
            .expect("VastAI node starts");

        plugin
            .complete_bootstrap(&handle)
            .expect("runtime-ready bootstrap completion succeeds");
        assert!(
            stop_reasons.lock().is_empty(),
            "runtime-ready keeps the SSH log tail alive for post-bootstrap diagnostics"
        );

        plugin.stop_node(&handle).expect("VastAI node stops");
        assert_eq!(*stop_reasons.lock(), vec![BootstrapStopReason::NodeStop]);
        assert_eq!(*destroyed_contracts.lock(), vec![42]);
    }

    #[test]
    fn vastai_provisioning_next_ssh_backoff_doubles_until_thirty_second_cap() {
        for (current, expected) in [
            (Duration::from_secs(1), Duration::from_secs(2)),
            (Duration::from_secs(15), Duration::from_secs(30)),
            (Duration::from_secs(20), Duration::from_secs(30)),
            (Duration::from_secs(30), Duration::from_secs(30)),
        ] {
            assert_eq!(
                next_ssh_backoff(current),
                expected,
                "backoff from {current:?}"
            );
        }
    }

    #[test]
    fn ssh_bootstrap_observation_classifies_auth_and_transport_failures() {
        for (line, expected) in [
            ("Permission denied (publickey).", Some("auth_denied")),
            (
                "ssh: connect to host ssh5.vast.ai port 22017: Connection refused",
                Some("refused"),
            ),
            ("ssh: connect timed out", Some("timeout")),
            ("debug1: permanently_set_uid", None),
        ] {
            assert_eq!(classify_ssh_observation(line), expected, "{line}");
        }
    }

    #[test]
    fn ssh_bootstrap_args_include_verbose_flag_and_identity_when_configured() {
        let endpoint = VastAiSshEndpoint {
            host: "ssh5.vast.ai".to_owned(),
            port: 22017,
            user: "ubuntu".to_owned(),
        };

        let args = ssh_bootstrap_args(&endpoint, "python worker.py", Some(Path::new("/tmp/key")));

        assert_eq!(
            args,
            vec![
                "-v",
                "-p",
                "22017",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "-i",
                "/tmp/key",
                "-o",
                "IdentitiesOnly=yes",
                "ubuntu@ssh5.vast.ai",
                "python worker.py",
            ]
        );
        assert_eq!(args.iter().filter(|arg| arg.as_str() == "-v").count(), 1);
    }

    #[test]
    fn ssh_bootstrap_args_keep_identity_absent_when_not_configured() {
        let endpoint = VastAiSshEndpoint {
            host: "ssh5.vast.ai".to_owned(),
            port: 22017,
            user: "ubuntu".to_owned(),
        };

        let args = ssh_bootstrap_args(&endpoint, "python worker.py", None);

        assert_eq!(
            args,
            vec![
                "-v",
                "-p",
                "22017",
                "-o",
                "BatchMode=yes",
                "-o",
                "StrictHostKeyChecking=accept-new",
                "ubuntu@ssh5.vast.ai",
                "python worker.py",
            ]
        );
        assert!(!args.iter().any(|arg| arg == "-i"));
        assert!(!args.iter().any(|arg| arg == "IdentitiesOnly=yes"));
    }

    #[test]
    fn provider_terminal_start_error_ignores_package_names_while_loading() {
        let package_log = "#7 1.745 libevent-core-2.1-7t64 liberror-perl libglib2.0-data";

        assert_eq!(
            provider_terminal_start_error(46132050, "loading", "running", Some(package_log)),
            None
        );
    }

    #[test]
    fn provider_terminal_start_error_reports_standalone_failure_words() {
        assert_eq!(
            provider_terminal_start_error(
                46132050,
                "loading",
                "running",
                Some("ERROR: build failed")
            ),
            Some("instance 46132050 error: ERROR: build failed".to_owned())
        );
    }

    #[test]
    fn ssh_bootstrap_actor_stop_kills_child() {
        let runtime = Arc::new(swactor::runtime::Runtime::new(
            swactor::config::RuntimeConfig::default(),
        ));
        let child = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn sleep child");
        let pid = child.id();
        let bridge = BootstrapDatastreamBridge::new(
            node_spec_with_bootstrap_args(),
            PluginSink::new(Arc::new(ObservationSink::default())),
            None,
        );
        let actor = runtime
            .spawn(SshBootstrapActor {
                bridge,
                endpoint: VastAiSshEndpoint {
                    host: "127.0.0.1".to_owned(),
                    port: 22,
                    user: "ubuntu".to_owned(),
                },
                ssh_identity: None,
                sender: runtime.create_sender(),
                child: Some(child),
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
                start_on_boot: false,
            })
            .expect("spawn ssh bootstrap actor");

        runtime
            .send_to(actor, SshBootstrapMsg::Stop)
            .expect("send stop");
        runtime.tick();
        #[cfg(target_os = "linux")]
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "child process should be reaped"
        );
    }
}
