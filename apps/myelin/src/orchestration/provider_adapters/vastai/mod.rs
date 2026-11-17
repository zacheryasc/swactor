// VastAI provider adapter. Actors own provider lifecycle policy; the
// swactor-vastai and swactor-process crates own API and process mechanics.
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use parking_lot::Mutex;

use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, ExternalSender, Runtime};
use swactor_engine::{BlockingWorkSender, TaskSpawner};
use swactor_vastai::{
    BlockingVastClient, CreateInstanceRequest, Offer, OfferBrowseCriteria, ProviderInstanceStatus,
    ProvisionedInstance, SelectionPolicy, VastClient, VastLogStreamEvent, classify_vastai_error,
    spawn_log_stream_task,
};
use telemetry::TelemetryProducer;

#[cfg(test)]
use super::ssh_bootstrap::{
    BootstrapEnvSource, SshBootstrapActor, SshBootstrapMsg, SshBootstrapStream,
    idempotent_ssh_bootstrap_command, ssh_bootstrap_command_for_attempt, ssh_bootstrap_lock,
};
use super::ssh_bootstrap::{
    SshBootstrapLauncher as VastAiBootstrapLauncher, SshEndpoint as VastAiSshEndpoint,
};
use crate::orchestration::manual_control::{OfferSearchRequest, OfferSearchRequestExt};
#[cfg(test)]
use crate::orchestration::provisioning_logs::BootstrapTelemetryBridge;
use crate::provisioning::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginSink,
    ProvisionPlugin,
};

mod ownership;
pub mod webhooks;
use ownership::{LeaseJournal, LeaseOwnership};
use webhooks::{VastAiWebhookSubscription, VastAiWebhooks};

/// A lease request contains no provider pacing policy.
#[derive(Clone, Debug)]
pub struct ProvisionRequest {
    pub count: u32,
    pub image: String,
    pub label: Option<String>,
    pub disk_gb: u32,
    pub env: BTreeMap<String, String>,
    pub per_instance_env: Vec<BTreeMap<String, String>>,
    pub preferred_offer_id: Option<u64>,
    pub onstart: Option<String>,
    pub selection: SelectionPolicy,
    pub confirm_lease: bool,
}

pub type SnapshotObserver =
    Arc<dyn Fn(u64, Option<ProviderInstanceStatus>) -> Result<bool, String> + Send + Sync>;
pub type CleanupObserver = Arc<dyn Fn(u64) -> Result<(), String> + Send + Sync>;
pub type OwnershipRecorder = Arc<dyn Fn(u64, bool) -> Result<bool, String> + Send + Sync>;

#[derive(Clone, Debug)]
pub struct VastAiProvisioningConfig {
    pub label_prefix: String,
    pub disk_gb: u32,
    pub ssh_user: String,
    pub selection: SelectionPolicy,
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
            confirm_lease: false,
            onstart: None,
            ssh_public_key: None,
        }
    }
}

pub struct VastAiProviderMonitor {
    runtime: Runtime,
    actor: ActorAddress,
    _subscription: Option<VastAiWebhookSubscription>,
    invalidation_queued: Arc<AtomicBool>,
}

impl VastAiProviderMonitor {
    fn new(runtime: Runtime, actor: ActorAddress, invalidation_queued: Arc<AtomicBool>) -> Self {
        Self {
            runtime,
            actor,
            _subscription: None,
            invalidation_queued,
        }
    }

    fn refresh(&self) {
        if !self.invalidation_queued.swap(true, Ordering::AcqRel)
            && self
                .runtime
                .send_to(self.actor, VastAiProviderMonitorMsg::Poll)
                .is_err()
        {
            self.invalidation_queued.store(false, Ordering::Release);
        }
    }

    fn stop(&mut self) {
        let _ = self
            .runtime
            .send_to(self.actor, VastAiProviderMonitorMsg::Stop);
    }

    fn stop_logs(&self) {
        let _ = self
            .runtime
            .send_to(self.actor, VastAiProviderMonitorMsg::StopLogs);
    }
}

impl Drop for VastAiProviderMonitor {
    fn drop(&mut self) {
        self.stop();
    }
}

pub trait VastAiLeaseClient: Send {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String>;

    fn provision_exact(
        &mut self,
        _request: ProvisionRequest,
        offer_id: u64,
        _criteria: OfferBrowseCriteria,
    ) -> Result<ProvisionedInstance, String> {
        Err(format!(
            "VastAI lease client does not support exact offer {offer_id}"
        ))
    }

    /// Acquire or recover a lease for one logical node.
    fn provision_node(
        &mut self,
        request: ProvisionRequest,
        _spec: &NodeProvisionSpec,
        selected_offer_id: Option<u64>,
        criteria: Option<OfferBrowseCriteria>,
    ) -> Result<Option<ProvisionedInstance>, String> {
        match (selected_offer_id, criteria) {
            (Some(offer_id), Some(criteria)) => self.provision_exact(request, offer_id, criteria),
            (None, None) => self.provision_one(request),
            _ => Err("selected offer and search criteria must be paired".to_owned()),
        }
        .map(Some)
    }

    /// Resolves the live contract id carrying `label`, if any.
    fn contract_by_label(&mut self, label: &str) -> Result<Option<u64>, String>;

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        label: &str,
        ssh_user: &str,
    ) -> Result<Option<VastAiSshEndpoint>, String>;
    fn spawn_provider_monitor(
        &mut self,
        _contract_id: Option<u64>,
        _label: String,
        _spec: NodeProvisionSpec,
        _sink: PluginSink,
        _observe_snapshot: SnapshotObserver,
        _cleanup_completed: CleanupObserver,
    ) -> Option<VastAiProviderMonitor> {
        None
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String>;

    fn record_contract(&mut self, _label: &str, _contract_id: u64) -> Result<(), String> {
        Ok(())
    }

    fn cancel_ownership(&mut self, _label: &str) -> Result<(), String> {
        Ok(())
    }

    fn ownership_cancelled(&self, _label: &str) -> Result<bool, String> {
        Ok(false)
    }

    fn ownership_destroyed(&self, _label: &str) -> Result<bool, String> {
        Ok(false)
    }

    fn ensure_event_source(&self) -> Result<(), String> {
        Ok(())
    }

    fn record_destroyed_contract(&mut self, _label: &str, _contract_id: u64) -> Result<(), String> {
        Ok(())
    }

    fn ownership_callbacks(&self, _label: &str) -> (OwnershipRecorder, CleanupObserver) {
        (Arc::new(|_, cancelled| Ok(cancelled)), Arc::new(|_| Ok(())))
    }

    fn bootstrap_environment(&self, _label: &str) -> Result<Vec<(String, String)>, String> {
        Ok(Vec::new())
    }

    fn destroy_owned_contract(&mut self, _label: &str, contract_id: u64) -> Result<(), String> {
        self.destroy_contract(contract_id)
    }
}

pub struct ToolsVastAiLeaseClient {
    client: BlockingVastClient,
    async_client: VastClient,
    actor_host: Option<(Runtime, VastAiTaskPort, Arc<VastAiWebhooks>)>,
    ownership: Option<Arc<Mutex<LeaseJournal>>>,
}

impl ToolsVastAiLeaseClient {
    pub fn new(client: VastClient) -> Result<Self, String> {
        Ok(Self {
            client: BlockingVastClient::new(client.clone())?,
            async_client: client,
            actor_host: None,
            ownership: None,
        })
    }

    pub fn with_actor_host(
        mut self,
        runtime: Runtime,
        tasks: VastAiTaskPort,
        webhooks: Arc<VastAiWebhooks>,
    ) -> Self {
        self.ownership = Some(Arc::new(Mutex::new(LeaseJournal::new(
            webhooks.state_dir(),
        ))));
        self.actor_host = Some((runtime, tasks, webhooks));
        self
    }

    pub fn from_api_key(api_key: impl Into<String>) -> Result<Self, String> {
        Self::new(swactor_vastai::VastClient::new(api_key))
    }

    pub fn browse_offers(&mut self, criteria: &OfferBrowseCriteria) -> Result<Vec<Offer>, String> {
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
        self.create_selected_offer(request, offer.id, offer.host_id, Some(offer))
    }

    fn create_selected_offer(
        &mut self,
        request: &ProvisionRequest,
        offer_id: u64,
        host_id: Option<u64>,
        observed_offer: Option<&Offer>,
    ) -> Result<ProvisionedInstance, String> {
        let create = Self::create_request_for_offer(request, offer_id);
        if let (Some(journal), Some(label)) = (&self.ownership, request.label.as_deref()) {
            journal.lock().reserve_creation(label)?;
        }
        let info = match self.client.create_instance(&create) {
            Ok(info) => info,
            Err(error) => {
                if error.is_definitive_rejection()
                    && let (Some(journal), Some(label)) =
                        (&self.ownership, request.label.as_deref())
                {
                    journal.lock().reject_creation(label)?;
                }
                return Err(error.to_string());
            }
        };
        if let Some(label) = request.label.as_deref() {
            self.record_contract(label, info.contract_id)?;
        }
        let (gpu_name, gpu_ram, dph_total) = observed_offer.map_or_else(
            || ("selected".to_owned(), None, f64::NAN),
            |offer| (offer.gpu_name.clone(), offer.gpu_ram, offer.dph_total),
        );
        Ok(ProvisionedInstance {
            index: 0,
            contract_id: info.contract_id,
            offer_id,
            host_id,
            gpu_name,
            gpu_ram,
            dph_total,
        })
    }
}

const MAX_LOG_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_LOG_MESSAGE_BYTES: usize = 16 * 1024;
const MAX_LOG_LINE_BYTES: usize = 16 * 1024;

/// Actor-facing commands only. Task authority lives in the external owner future.
#[derive(Clone)]
pub struct VastAiTaskPort {
    commands: tokio::sync::mpsc::UnboundedSender<VastAiTaskRequest>,
}

struct VastAiTaskLease(tokio::sync::watch::Sender<bool>);

impl VastAiTaskLease {
    fn cancel(self) {
        let _ = self.0.send(true);
    }
}

struct VastAiTaskReply {
    sender: ExternalSender,
    actor: ActorAddress,
    generation: u64,
    poll: Option<u64>,
    settled: bool,
}

impl VastAiTaskReply {
    fn status(&mut self, result: Result<ProviderInstanceStatus, String>) {
        self.settled = true;
        let _ = self.sender.send_to(
            self.actor,
            VastAiProviderMonitorMsg::StatusCompleted {
                generation: self.generation,
                poll: self.poll.expect("status request carries a poll"),
                result,
            },
        );
    }

    fn cleanup_completed(&mut self) {
        self.settled = true;
        let _ = self.sender.send_to(
            self.actor,
            VastAiProviderMonitorMsg::CleanupCompleted {
                generation: self.generation,
            },
        );
    }

    #[cfg(test)]
    fn resolved_status(&mut self, contract_id: u64, status: ProviderInstanceStatus) {
        let _ = self.sender.send_to(
            self.actor,
            VastAiProviderMonitorMsg::ContractResolved {
                generation: self.generation,
                contract_id,
            },
        );
        self.status(Ok(status));
    }

    fn resolution_pending(&mut self) {
        self.settled = true;
        let _ = self.sender.send_to(
            self.actor,
            VastAiProviderMonitorMsg::ResolutionPending {
                generation: self.generation,
            },
        );
    }

    fn log(&mut self, event: VastLogStreamEvent) -> bool {
        let message = match event {
            VastLogStreamEvent::Chunk(bytes) => VastAiProviderMonitorMsg::LogChunk {
                generation: self.generation,
                bytes,
            },
            VastLogStreamEvent::Finished {
                accepted_bytes,
                truncated,
                error,
            } => {
                self.settled = true;
                VastAiProviderMonitorMsg::LogFinished {
                    generation: self.generation,
                    accepted_bytes,
                    truncated,
                    error,
                }
            }
        };
        self.sender.send_to(self.actor, message).is_ok()
    }
}

impl Drop for VastAiTaskReply {
    fn drop(&mut self) {
        if self.settled {
            return;
        }
        let error = "provider_task_owner_lost".to_owned();
        if self.poll.is_some() {
            self.status(Err(error));
        } else {
            self.log(VastLogStreamEvent::Finished {
                accepted_bytes: 0,
                truncated: false,
                error: Some(error),
            });
        }
    }
}

struct VastAiTaskRequest {
    client: VastClient,
    contract_id: Option<u64>,
    label: String,
    reply: VastAiTaskReply,
    cancelled: tokio::sync::watch::Receiver<bool>,
    callbacks: Option<SnapshotCallbacks>,
}

#[derive(Clone)]
struct SnapshotCallbacks {
    observe: SnapshotObserver,
    cleanup_completed: CleanupObserver,
}

enum SnapshotOutcome {
    Pending,
    Observed(ProviderInstanceStatus),
    Cleaned,
}

async fn blocking_provider_work<T: Send + 'static>(
    blocking: &BlockingWorkSender,
    work: impl FnOnce() -> Result<T, String> + Send + 'static,
) -> Result<T, String> {
    let (reply, completion) = tokio::sync::oneshot::channel();
    blocking
        .submit(Box::new(move || {
            let _ = reply.send(work());
        }))
        .map_err(|_| "provider blocking owner stopped".to_owned())?;
    completion
        .await
        .map_err(|_| "provider blocking owner lost".to_owned())?
}

async fn apply_snapshot(
    blocking: &BlockingWorkSender,
    client: &VastClient,
    callbacks: &Option<SnapshotCallbacks>,
    contract: u64,
    status: Option<ProviderInstanceStatus>,
    cancelled: tokio::sync::watch::Receiver<bool>,
) -> Result<bool, String> {
    let Some(callbacks) = callbacks else {
        return Ok(true);
    };
    let observe = Arc::clone(&callbacks.observe);
    let continue_running = blocking_provider_work(blocking, move || {
        if *cancelled.borrow() || cancelled.has_changed().is_err() {
            return Err("provider observation cancelled".to_owned());
        }
        observe(contract, status)
    })
    .await?;
    if continue_running {
        return Ok(true);
    }
    client.destroy_instance(contract).await?;
    let completed = Arc::clone(&callbacks.cleanup_completed);
    blocking_provider_work(blocking, move || completed(contract)).await?;
    Ok(false)
}

async fn fetch_provider_snapshot(
    client: &VastClient,
    contract_id: u64,
    label: &str,
) -> Result<ProviderInstanceStatus, String> {
    let status = client.instance_status(contract_id).await;
    if status
        .as_ref()
        .is_ok_and(|status| status.ssh_endpoint().is_some())
    {
        return status;
    }
    let instances = client.list_by_label(label).await?;
    if let Some(instance) = instances
        .iter()
        .find(|instance| instance.contract_id == contract_id)
    {
        let host = if instance.ssh_host.trim().is_empty() {
            instance.public_ipaddr.trim()
        } else {
            instance.ssh_host.trim()
        };
        if !host.is_empty() && instance.ssh_port != 0 {
            let mut status = status.unwrap_or_else(|_| ProviderInstanceStatus {
                actual_status: instance.actual_status.clone(),
                intended_status: String::new(),
                status_msg: None,
                public_ipaddr: None,
                ssh_port: None,
                disk_usage: None,
            });
            status.public_ipaddr = Some(host.to_owned());
            status.ssh_port = Some(instance.ssh_port);
            return Ok(status);
        }
    }
    status
}

impl VastAiTaskPort {
    pub fn new(tasks: &TaskSpawner, blocking: BlockingWorkSender) -> Self {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel::<VastAiTaskRequest>();
        let owner_tasks = tasks.clone();
        tasks.spawn(async move {
            while let Some(request) = requests.recv().await {
                let VastAiTaskRequest { client, contract_id, label, mut reply, mut cancelled, callbacks } = request;
                if *cancelled.borrow() || cancelled.has_changed().is_err() { continue; }
                if reply.poll.is_some() {
                    let blocking = blocking.clone();
                    owner_tasks.spawn(async move {
                        let resolution_sender = reply.sender.clone();
                        let resolution_actor = reply.actor;
                        let resolution_generation = reply.generation;
                        let cancellation = cancelled.clone();
                        let snapshot = async {
                            let contract = match contract_id {
                                Some(contract) => contract,
                                None => {
                                    let instances = client.list_by_label(&label).await?;
                                    match instances.as_slice() {
                                        [] => return Ok(SnapshotOutcome::Pending),
                                        [instance] => {
                                            let contract_id = instance.contract_id;
                                            let _ = resolution_sender.send_to(resolution_actor,
                                                VastAiProviderMonitorMsg::ContractResolved {
                                                    generation: resolution_generation, contract_id,
                                                });
                                            contract_id
                                        }
                                        _ => return Err(format!("multiple VastAI contracts share stable label {label}")),
                                    }
                                }
                            };
                            if !apply_snapshot(&blocking, &client, &callbacks, contract, None, cancellation.clone()).await? {
                                return Ok(SnapshotOutcome::Cleaned);
                            }
                            let status = fetch_provider_snapshot(&client, contract, &label).await?;
                            if !apply_snapshot(&blocking, &client, &callbacks, contract, Some(status.clone()), cancellation).await? {
                                return Ok(SnapshotOutcome::Cleaned);
                            }
                            Ok(SnapshotOutcome::Observed(status))
                        };
                        tokio::select! {
                            biased;
                            _ = cancelled.changed() => {}
                            result = snapshot => match result {
                                Ok(SnapshotOutcome::Observed(status)) => reply.status(Ok(status)),
                                Ok(SnapshotOutcome::Pending) => reply.resolution_pending(),
                                Ok(SnapshotOutcome::Cleaned) => reply.cleanup_completed(),
                                Err(error) => reply.status(Err(error)),
                            },
                        }
                    });
                } else {
                    let cancel = spawn_log_stream_task(
                        &owner_tasks, client,
                        contract_id.expect("log requests require a resolved contract"),
                        MAX_LOG_RESPONSE_BYTES, MAX_LOG_MESSAGE_BYTES,
                        move |event| reply.log(event),
                    );
                    owner_tasks.spawn(async move {
                        let _ = cancelled.changed().await;
                        cancel.cancel();
                    });
                }
            }
        });
        Self { commands }
    }

    fn submit(
        &self,
        client: VastClient,
        contract_id: Option<u64>,
        label: String,
        reply: VastAiTaskReply,
        callbacks: Option<SnapshotCallbacks>,
    ) -> VastAiTaskLease {
        let (cancel, cancelled) = tokio::sync::watch::channel(false);
        // A rejected or abandoned request resolves through its reply's Drop.
        let _ = self.commands.send(VastAiTaskRequest {
            client,
            contract_id,
            label,
            reply,
            cancelled,
            callbacks,
        });
        VastAiTaskLease(cancel)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProviderMonitorPhase {
    Ready,
    Polling,
    Closed,
    Failed,
}

impl ProviderMonitorPhase {
    fn terminal(self) -> bool {
        matches!(self, Self::Closed | Self::Failed)
    }
}
#[derive(Clone)]
enum VastAiProviderMonitorMsg {
    StopLogs,
    Poll,
    ContractResolved {
        generation: u64,
        contract_id: u64,
    },
    ResolutionPending {
        generation: u64,
    },
    CleanupCompleted {
        generation: u64,
    },
    StatusCompleted {
        generation: u64,
        poll: u64,
        result: Result<ProviderInstanceStatus, String>,
    },
    LogChunk {
        generation: u64,
        bytes: Vec<u8>,
    },
    LogFinished {
        generation: u64,
        accepted_bytes: usize,
        truncated: bool,
        error: Option<String>,
    },
    Stop,
}

struct VastAiProviderMonitorActor {
    client: VastClient,
    contract_id: Option<u64>,
    label: String,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    sender: ExternalSender,
    tasks: VastAiTaskPort,
    observe_snapshot: Option<SnapshotObserver>,
    cleanup_completed: Option<CleanupObserver>,
    invalidation_queued: Arc<AtomicBool>,
    dirty: bool,
    log_dirty: bool,

    last_reported_status: Option<(String, Option<String>)>,
    log_enabled: bool,
    last_log_snapshot: Vec<u8>,
    current_log_snapshot: Vec<u8>,
    log_emit_cursor: usize,
    log_prefix_matches: bool,
    log_line_buffer: Vec<u8>,
    poll: u64,
    generation: u64,
    status_generation: Option<u64>,
    status_cancel: Option<VastAiTaskLease>,
    log_generation: Option<u64>,
    log_cancel: Option<VastAiTaskLease>,
    #[cfg(test)]
    scripted_status: Option<Result<ProviderInstanceStatus, String>>,
    phase: ProviderMonitorPhase,
}

struct VastAiProviderMonitorConfig {
    client: VastClient,
    contract_id: Option<u64>,
    label: String,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    sender: ExternalSender,
    tasks: VastAiTaskPort,
    observe_snapshot: Option<SnapshotObserver>,
    cleanup_completed: Option<CleanupObserver>,
    invalidation_queued: Arc<AtomicBool>,

    #[cfg(test)]
    scripted_status: Option<Result<ProviderInstanceStatus, String>>,
}

impl VastAiProviderMonitorActor {
    fn new(config: VastAiProviderMonitorConfig) -> Self {
        let VastAiProviderMonitorConfig {
            client,
            contract_id,
            label,
            spec,
            sink,
            sender,
            tasks,
            observe_snapshot,
            cleanup_completed,
            invalidation_queued,

            #[cfg(test)]
            scripted_status,
        } = config;
        Self {
            client,
            contract_id,
            label,
            spec,
            sink,
            sender,
            tasks,
            observe_snapshot,
            cleanup_completed,
            invalidation_queued,
            dirty: false,
            log_dirty: false,

            last_reported_status: None,
            log_enabled: false,
            last_log_snapshot: Vec::new(),
            current_log_snapshot: Vec::new(),
            log_emit_cursor: 0,
            log_prefix_matches: true,
            log_line_buffer: Vec::new(),
            poll: 0,
            generation: 0,
            status_generation: None,
            status_cancel: None,
            log_generation: None,
            log_cancel: None,
            #[cfg(test)]
            scripted_status,
            phase: ProviderMonitorPhase::Ready,
        }
    }

    fn enter_phase(&mut self, next: ProviderMonitorPhase) {
        self.phase = next;
    }

    fn finish(&mut self, ctx: &Ctx, next: ProviderMonitorPhase, failure: Option<String>) {
        if self.phase.terminal() {
            return;
        }
        if let Some(failure) = failure {
            self.observe_failed(failure);
        }
        self.phase = next;
        self.cancel_tasks();
        ctx.stop_self();
    }

    fn with_remote_logs(mut self) -> Self {
        self.log_enabled = true;
        self
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

    fn next_generation(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1).max(1);
        self.generation
    }

    fn invalidate(&mut self, ctx: &Ctx) {
        if self.phase.terminal() {
            return;
        }
        if self.status_generation.is_some() {
            self.dirty = true;
        } else {
            self.poll_provider(ctx);
        }
    }

    fn reconcile_dirty(&mut self, ctx: &Ctx) {
        self.enter_phase(ProviderMonitorPhase::Ready);
        if std::mem::take(&mut self.dirty) {
            self.poll_provider(ctx);
        }
    }

    fn poll_provider(&mut self, ctx: &Ctx) {
        if self.phase.terminal() || self.status_generation.is_some() {
            return;
        }
        self.poll = self.poll.saturating_add(1);
        let poll = self.poll;
        let generation = self.next_generation();
        let sender = self.sender.clone();
        let actor = ctx.self_addr();
        self.status_generation = Some(generation);
        self.enter_phase(ProviderMonitorPhase::Polling);
        #[cfg(test)]
        if let Some(result) = self.scripted_status.clone() {
            let _ = sender.send_to(
                actor,
                VastAiProviderMonitorMsg::StatusCompleted {
                    generation,
                    poll,
                    result,
                },
            );
            return;
        }
        self.status_cancel = Some(
            self.tasks.submit(
                self.client.clone(),
                self.contract_id,
                self.label.clone(),
                VastAiTaskReply {
                    sender,
                    actor,
                    generation,
                    poll: Some(poll),
                    settled: false,
                },
                self.observe_snapshot
                    .as_ref()
                    .zip(self.cleanup_completed.as_ref())
                    .map(|(observe, cleanup)| SnapshotCallbacks {
                        observe: Arc::clone(observe),
                        cleanup_completed: Arc::clone(cleanup),
                    }),
            ),
        );
    }

    fn start_log_stream(&mut self, ctx: &Ctx) {
        if self.phase.terminal() || !self.log_enabled {
            return;
        }
        if self.log_generation.is_some() {
            self.log_dirty = true;
            return;
        }
        let generation = self.next_generation();
        let sender = self.sender.clone();
        let actor = ctx.self_addr();
        self.log_generation = Some(generation);
        self.current_log_snapshot.clear();
        self.log_emit_cursor = self.last_log_snapshot.len();
        self.log_prefix_matches = true;
        self.log_line_buffer.clear();
        self.log_cancel = Some(self.tasks.submit(
            self.client.clone(),
            self.contract_id,
            self.label.clone(),
            VastAiTaskReply {
                sender,
                actor,
                generation,
                poll: None,
                settled: false,
            },
            None,
        ));
    }

    fn emit_log_line(&self, bytes: &[u8], continued: bool) {
        if bytes.is_empty() {
            return;
        }
        let mut line = String::from_utf8_lossy(bytes).into_owned();
        if continued {
            line.push_str(" [continued]");
        }
        self.sink.observe(PluginObservation::StdoutLine {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            line,
        });
    }

    fn observe_log_bytes(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if *byte == b'\n' {
                let line = std::mem::take(&mut self.log_line_buffer);
                self.emit_log_line(&line, false);
            } else {
                self.log_line_buffer.push(*byte);
                if self.log_line_buffer.len() == MAX_LOG_LINE_BYTES {
                    let line = std::mem::take(&mut self.log_line_buffer);
                    self.emit_log_line(&line, true);
                }
            }
        }
    }

    fn handle_log_chunk(&mut self, generation: u64, bytes: Vec<u8>) {
        if self.log_generation != Some(generation) {
            return;
        }
        let start = self.current_log_snapshot.len();
        self.current_log_snapshot.extend_from_slice(&bytes);
        let compared = self
            .current_log_snapshot
            .len()
            .min(self.last_log_snapshot.len());
        if self.log_prefix_matches
            && start < compared
            && self.current_log_snapshot[start..compared] != self.last_log_snapshot[start..compared]
        {
            self.log_prefix_matches = false;
            self.log_emit_cursor = 0;
            self.log_line_buffer.clear();
        }
        if self.current_log_snapshot.len() > self.log_emit_cursor {
            let new_bytes = self.current_log_snapshot[self.log_emit_cursor..].to_vec();
            self.log_emit_cursor = self.current_log_snapshot.len();
            self.observe_log_bytes(&new_bytes);
        }
    }

    fn handle_log_finished(
        &mut self,
        ctx: &Ctx,
        generation: u64,
        accepted_bytes: usize,
        truncated: bool,
        error: Option<String>,
    ) {
        if self.log_generation != Some(generation) {
            return;
        }
        self.log_generation = None;
        self.log_cancel = None;
        if !self.log_line_buffer.is_empty() {
            let line = std::mem::take(&mut self.log_line_buffer);
            self.emit_log_line(&line, false);
        }
        if error.is_none() {
            self.last_log_snapshot = std::mem::take(&mut self.current_log_snapshot);
        } else {
            self.current_log_snapshot.clear();
        }
        if truncated {
            self.observe_provider_line(
                serde_json::json!({
                    "type": "VastAiProviderLogsTruncated",
                    "run_id": self.spec.run_id,
                    "node_id": self.spec.node_id,
                    "contract_id": self.contract_id,
                    "accepted_bytes": accepted_bytes,
                })
                .to_string(),
            );
        } else if let Some(error) = error {
            self.observe_provider_line(
                serde_json::json!({
                    "type": "VastAiProviderLogsReadFailed",
                    "run_id": self.spec.run_id,
                    "node_id": self.spec.node_id,
                    "contract_id": self.contract_id,
                    "reason": error,
                })
                .to_string(),
            );
        }
        if std::mem::take(&mut self.log_dirty) {
            self.start_log_stream(ctx);
        }
    }

    fn handle_status_completed(
        &mut self,
        ctx: &Ctx,
        generation: u64,
        poll: u64,
        result: Result<ProviderInstanceStatus, String>,
    ) {
        if self.status_generation != Some(generation) || self.phase.terminal() {
            return;
        }
        self.status_generation = None;
        self.status_cancel = None;
        let status = match result {
            Ok(status) => status,
            Err(error)
                if error.contains("not found while fetching provider status")
                    || error.contains("parse failed")
                    || error == "provider_task_owner_lost" =>
            {
                let reason = classified_start_error(format!(
                    "vastai provider monitor node {} contract {:?}: {error}",
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
                self.finish(ctx, ProviderMonitorPhase::Failed, Some(reason));
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
                self.reconcile_dirty(ctx);
                return;
            }
        };

        let actual = status.actual_status.as_str();
        let reported = (status.actual_status.clone(), status.status_msg.clone());
        if self.last_reported_status.as_ref() != Some(&reported) {
            let detail = status
                .status_msg
                .as_deref()
                .filter(|message| !message.trim().is_empty())
                .map(|message| format!(": {message}"))
                .unwrap_or_default();
            self.observe_provider_line(format!("Vast.ai {actual}{detail}"));
            self.last_reported_status = Some(reported);
        }
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
            })
            .to_string(),
        );

        let Some(contract_id) = self.contract_id else {
            self.finish(
                ctx,
                ProviderMonitorPhase::Failed,
                Some("provider status has no resolved ownership".to_owned()),
            );
            return;
        };
        if let Some(error) = provider_terminal_start_error(
            contract_id,
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
            self.finish(ctx, ProviderMonitorPhase::Failed, Some(reason));
            return;
        }

        let running = actual.eq_ignore_ascii_case("running");
        if running {
            self.start_log_stream(ctx);
        }
        self.reconcile_dirty(ctx);
    }

    fn stop_logs(&mut self) {
        self.log_enabled = false;
        self.log_dirty = false;
        if let Some(cancel) = self.log_cancel.take() {
            cancel.cancel();
        }
        self.log_generation = None;
        self.last_log_snapshot.clear();
        self.current_log_snapshot.clear();
        self.log_line_buffer.clear();
    }

    fn cancel_tasks(&mut self) {
        if let Some(cancel) = self.status_cancel.take() {
            cancel.cancel();
        }
        self.status_generation = None;
        self.dirty = false;
        self.invalidation_queued.store(false, Ordering::Release);
        self.stop_logs();
    }
}

impl ActorInterface for VastAiProviderMonitorActor {
    type Incoming = VastAiProviderMonitorMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        self.invalidate(ctx);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            VastAiProviderMonitorMsg::Poll => {
                self.invalidation_queued.store(false, Ordering::Release);
                self.invalidate(ctx);
            }
            VastAiProviderMonitorMsg::ContractResolved {
                generation,
                contract_id,
            } => {
                if self.status_generation == Some(generation) && !self.phase.terminal() {
                    if self.contract_id.is_some_and(|known| known != contract_id) {
                        self.finish(
                            ctx,
                            ProviderMonitorPhase::Failed,
                            Some("provider contract changed within one attempt".to_owned()),
                        );
                    } else {
                        self.contract_id = Some(contract_id);
                    }
                }
            }
            VastAiProviderMonitorMsg::CleanupCompleted { generation } => {
                if self.status_generation == Some(generation) && !self.phase.terminal() {
                    self.finish(ctx, ProviderMonitorPhase::Closed, None);
                }
            }
            VastAiProviderMonitorMsg::ResolutionPending { generation } => {
                if self.status_generation == Some(generation) && !self.phase.terminal() {
                    self.status_generation = None;
                    self.status_cancel = None;
                    self.observe_provider_line(
                        "VastAI contract resolution pending; awaiting provider notification",
                    );
                    self.reconcile_dirty(ctx);
                }
            }
            VastAiProviderMonitorMsg::StatusCompleted {
                generation,
                poll,
                result,
            } => self.handle_status_completed(ctx, generation, poll, result),
            VastAiProviderMonitorMsg::LogChunk { generation, bytes } => {
                self.handle_log_chunk(generation, bytes);
            }
            VastAiProviderMonitorMsg::LogFinished {
                generation,
                accepted_bytes,
                truncated,
                error,
            } => self.handle_log_finished(ctx, generation, accepted_bytes, truncated, error),
            VastAiProviderMonitorMsg::StopLogs => self.stop_logs(),
            VastAiProviderMonitorMsg::Stop => self.finish(ctx, ProviderMonitorPhase::Closed, None),
        }
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.finish(
            ctx,
            ProviderMonitorPhase::Failed,
            Some("provider_monitor_parent_stopped".to_owned()),
        );
        self.cancel_tasks();
    }
}

impl Clone for ToolsVastAiLeaseClient {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            async_client: self.async_client.clone(),
            actor_host: self.actor_host.clone(),
            ownership: self.ownership.clone(),
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

fn record_owned_contract(
    journal: &LeaseJournal,
    label: &str,
    contract: u64,
    cancel: bool,
) -> Result<bool, String> {
    let _transaction = journal.acquire_lock()?;
    let mut record = journal.load(label)?.unwrap_or(LeaseOwnership {
        label: label.to_owned(),
        contract_id: None,
        cancelled: false,
        destroyed: false,
    });
    if contract == 0
        || record.contract_id.is_some_and(|known| known != contract)
        || (record.destroyed && record.contract_id != Some(contract))
    {
        return Err(format!(
            "VastAI contract identity conflicts with durable ownership for {label}"
        ));
    }
    if record.contract_id != Some(contract) || (cancel && !record.cancelled) {
        record.contract_id = Some(contract);
        record.cancelled |= cancel;
        journal.store(&record)?;
    }
    Ok(record.cancelled)
}

fn commit_owned_cleanup(
    journal: Option<&Mutex<LeaseJournal>>,
    webhooks: Option<&VastAiWebhooks>,
    label: &str,
    contract: u64,
) -> Result<(), String> {
    if let Some(journal) = journal {
        let journal = journal.lock();
        let _transaction = journal.acquire_lock()?;
        let mut record = journal
            .load(label)?
            .ok_or("cleanup completion has no durable ownership")?;
        if record.contract_id.is_some_and(|known| known != contract) {
            return Err("cleanup completion conflicts with durable contract identity".to_owned());
        }
        record.contract_id = Some(contract);
        record.cancelled = true;
        record.destroyed = true;
        journal.store(&record)?;
    }
    if let Some(webhooks) = webhooks {
        webhooks.revoke_bootstrap(label)?;
    }
    Ok(())
}

impl VastAiLeaseClient for ToolsVastAiLeaseClient {
    fn provision_node(
        &mut self,
        request: ProvisionRequest,
        _spec: &NodeProvisionSpec,
        selected_offer_id: Option<u64>,
        criteria: Option<OfferBrowseCriteria>,
    ) -> Result<Option<ProvisionedInstance>, String> {
        let label = request.label.clone();
        let result = match (selected_offer_id, criteria) {
            (Some(offer), Some(criteria)) => self.provision_exact(request, offer, criteria),
            (None, None) => self.provision_one(request),
            _ => return Err("selected offer and search criteria must be paired".to_owned()),
        };
        match result {
            Ok(instance) => Ok(Some(instance)),
            Err(error) => {
                if let (Some(journal), Some(label)) = (&self.ownership, label)
                    && journal
                        .lock()
                        .load(&label)?
                        .is_some_and(|record| !record.destroyed)
                {
                    return Ok(None);
                }
                Err(error)
            }
        }
    }

    fn record_contract(&mut self, label: &str, contract_id: u64) -> Result<(), String> {
        if let Some(journal) = &self.ownership {
            record_owned_contract(&journal.lock(), label, contract_id, false)?;
        }
        Ok(())
    }

    fn ownership_callbacks(&self, label: &str) -> (OwnershipRecorder, CleanupObserver) {
        let journal = self.ownership.clone();
        let record_label = label.to_owned();
        let recorder: OwnershipRecorder = Arc::new(move |contract, cancelled| {
            journal.as_ref().map_or(Ok(cancelled), |journal| {
                record_owned_contract(&journal.lock(), &record_label, contract, cancelled)
            })
        });
        let journal = self.ownership.clone();
        let webhooks = self
            .actor_host
            .as_ref()
            .map(|(_, _, webhooks)| Arc::clone(webhooks));
        let label = label.to_owned();
        let completed: CleanupObserver = Arc::new(move |contract| {
            commit_owned_cleanup(journal.as_deref(), webhooks.as_deref(), &label, contract)
        });
        (recorder, completed)
    }

    fn record_destroyed_contract(&mut self, label: &str, contract: u64) -> Result<(), String> {
        commit_owned_cleanup(
            self.ownership.as_deref(),
            self.actor_host
                .as_ref()
                .map(|(_, _, webhooks)| webhooks.as_ref()),
            label,
            contract,
        )
    }

    fn cancel_ownership(&mut self, label: &str) -> Result<(), String> {
        if let Some(journal) = &self.ownership {
            let journal = journal.lock();
            let _transaction = journal.acquire_lock()?;
            let mut record = journal.load(label)?.unwrap_or(LeaseOwnership {
                label: label.to_owned(),
                contract_id: None,
                cancelled: false,
                destroyed: false,
            });
            record.cancelled = true;
            journal.store(&record)?;
        }
        Ok(())
    }
    fn ownership_cancelled(&self, label: &str) -> Result<bool, String> {
        self.ownership.as_ref().map_or(Ok(false), |journal| {
            journal
                .lock()
                .load(label)
                .map(|record| record.is_some_and(|record| record.cancelled))
        })
    }

    fn ownership_destroyed(&self, label: &str) -> Result<bool, String> {
        self.ownership.as_ref().map_or(Ok(false), |journal| {
            journal
                .lock()
                .load(label)
                .map(|record| record.is_some_and(|record| record.destroyed))
        })
    }

    fn ensure_event_source(&self) -> Result<(), String> {
        self.actor_host
            .as_ref()
            .map(|_| ())
            .ok_or_else(|| "VastAI lifecycle requires a configured webhook event source".to_owned())
    }

    fn bootstrap_environment(&self, label: &str) -> Result<Vec<(String, String)>, String> {
        self.actor_host.as_ref().map_or_else(
            || Err("VastAI lifecycle requires a configured webhook event source".to_owned()),
            |(_, _, webhooks)| webhooks.bootstrap_environment(label),
        )
    }

    fn destroy_owned_contract(&mut self, label: &str, contract_id: u64) -> Result<(), String> {
        self.cancel_ownership(label)?;
        let already_destroyed = if let Some(journal) = &self.ownership {
            journal
                .lock()
                .load(label)?
                .is_some_and(|record| record.destroyed && record.contract_id == Some(contract_id))
        } else {
            false
        };
        if !already_destroyed {
            self.destroy_contract(contract_id)?;
        }
        self.record_destroyed_contract(label, contract_id)
    }

    fn contract_by_label(&mut self, label: &str) -> Result<Option<u64>, String> {
        if let Some(journal) = &self.ownership
            && let Some(record) = journal.lock().load(label)?
        {
            if record.destroyed || record.contract_id.is_some() {
                return Ok(record.contract_id);
            }
        }
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
        criteria: OfferBrowseCriteria,
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
            .client
            .browse_offers(&criteria)?
            .into_iter()
            .find(|offer| offer.id == offer_id)
            .ok_or_else(|| {
                format!(
                    "selected offer {offer_id} is no longer available; refresh offers and select again"
                )
            })?;
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
        ssh_user: &str,
    ) -> Result<Option<VastAiSshEndpoint>, String> {
        let status = self.client.instance_status(contract_id);
        if let Ok(status) = &status
            && let Some(endpoint) = status.ssh_endpoint()
        {
            return Ok(Some(VastAiSshEndpoint {
                host: endpoint.ip,
                port: endpoint.port,
                user: ssh_user.to_owned(),
            }));
        }
        let instances = self.client.list_by_label(label)?;
        if let Some(instance) = instances
            .iter()
            .find(|instance| instance.contract_id == contract_id)
        {
            let host = if instance.ssh_host.trim().is_empty() {
                instance.public_ipaddr.trim()
            } else {
                instance.ssh_host.trim()
            };
            if !host.is_empty() && instance.ssh_port != 0 {
                return Ok(Some(VastAiSshEndpoint {
                    host: host.to_owned(),
                    port: instance.ssh_port,
                    user: ssh_user.to_owned(),
                }));
            }
        }
        let status = status?;
        if let Some(error) = provider_terminal_start_error(
            contract_id,
            &status.actual_status,
            &status.intended_status,
            status.status_msg.as_deref(),
        ) {
            return Err(error);
        }
        Ok(None)
    }

    fn spawn_provider_monitor(
        &mut self,
        contract_id: Option<u64>,
        label: String,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        observe_snapshot: SnapshotObserver,
        cleanup_completed: CleanupObserver,
    ) -> Option<VastAiProviderMonitor> {
        let (runtime, tasks, webhooks) = self.actor_host.as_ref()?.clone();
        let sender = runtime.create_sender();
        let invalidation_queued = Arc::new(AtomicBool::new(false));
        let actor = match runtime.spawn(
            VastAiProviderMonitorActor::new(VastAiProviderMonitorConfig {
                client: self.async_client.clone(),
                contract_id,
                label: label.clone(),
                spec: spec.clone(),
                sink: sink.clone(),
                sender,
                tasks,
                observe_snapshot: Some(observe_snapshot),
                cleanup_completed: Some(cleanup_completed),
                invalidation_queued: Arc::clone(&invalidation_queued),

                #[cfg(test)]
                scripted_status: None,
            })
            .with_remote_logs(),
        ) {
            Ok(actor) => actor,
            Err(error) => {
                sink.observe(PluginObservation::Failed {
                    run_id: spec.run_id,
                    node_id: spec.node_id,
                    reason: format!("VastAI provider event owner unavailable: {error}"),
                });
                return None;
            }
        };
        let target = runtime.create_sender();
        let queued = Arc::clone(&invalidation_queued);
        let subscription = webhooks.subscribe(
            &label,
            Arc::new(move || {
                if queued.swap(true, Ordering::AcqRel) {
                    return true;
                }
                if target
                    .send_to(actor, VastAiProviderMonitorMsg::Poll)
                    .is_ok()
                {
                    true
                } else {
                    queued.store(false, Ordering::Release);
                    false
                }
            }),
        );
        let mut monitor = VastAiProviderMonitor::new(runtime, actor, invalidation_queued);
        monitor._subscription = Some(subscription);
        Some(monitor)
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

fn emit_node_line(sink: &PluginSink, run_id: u64, node_id: u64, line: impl Into<String>) {
    sink.observe(PluginObservation::ProviderLine {
        run_id,
        node_id,
        line: line.into(),
    });
}

pub struct VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient,
    B: VastAiBootstrapLauncher,
{
    client: C,
    bootstrap: Arc<Mutex<B>>,
    config: VastAiProvisioningConfig,
    bootstrap_producer: Option<TelemetryProducer>,
    leased_host_ids: BTreeSet<u64>,
    failed_host_ids: BTreeSet<u64>,
    next_handle_id: u64,
    nodes: BTreeMap<u64, VastAiNode<B::Handle>>,
}

enum VastAiNodeBootstrap<H> {
    Dormant,
    Pending,
    Running(H),
    Ready(Option<H>),
    Cancelled,
}

struct VastAiNode<H> {
    contract_id: Arc<Mutex<Option<u64>>>,
    bootstrap: Arc<Mutex<VastAiNodeBootstrap<H>>>,
    provider_monitor: Option<VastAiProviderMonitor>,
    host_id: Option<u64>,
    run_id: u64,
    node_id: u64,
    label: String,
    sink: PluginSink,
    spec: NodeProvisionSpec,
}

impl<C, B> VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient + Clone + 'static,
    B: VastAiBootstrapLauncher + 'static,
    B::Handle: 'static,
{
    pub fn new(client: C, bootstrap: B, config: VastAiProvisioningConfig) -> Self {
        Self {
            client,
            bootstrap: Arc::new(Mutex::new(bootstrap)),
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
            confirm_lease: self.config.confirm_lease,
        }
    }

    fn create_node_with_offer(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        selected_offer_id: Option<u64>,
    ) -> Result<PluginNodeHandle, String> {
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            format!(
                "validating selected Vast.ai offer{}",
                selected_offer_id
                    .map(|offer_id| format!(" {offer_id}"))
                    .unwrap_or_default()
            ),
        );
        let result =
            self.create_node_with_offer_inner(spec.clone(), sink.clone(), selected_offer_id);
        if let Err(error) = &result {
            emit_node_line(
                &sink,
                spec.run_id,
                spec.node_id,
                format!("Vast.ai provisioning failed: {error}"),
            );
            sink.observe(PluginObservation::Failed {
                run_id: spec.run_id,
                node_id: spec.node_id,
                reason: error.clone(),
            });
        }
        result
    }

    fn create_node_with_offer_inner(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        selected_offer_id: Option<u64>,
    ) -> Result<PluginNodeHandle, String> {
        if !spec.mounts.is_empty() {
            return Err("vastai provider does not support host file mounts".to_owned());
        }
        let label = self.label_for(&spec);
        let mut request = self.build_request(&spec, label.clone());
        request
            .env
            .extend(self.client.bootstrap_environment(&label)?);
        let exact_criteria = selected_offer_id
            .map(|offer_id| {
                let criteria_json = spec.offer_criteria_json.as_deref().ok_or_else(|| {
                    format!("selected offer {offer_id} has no originating offer-search criteria")
                })?;
                serde_json::from_str::<OfferSearchRequest>(criteria_json)
                    .map_err(|error| format!("decode selected offer criteria: {error}"))
                    .map(|request| request.browse_criteria())
            })
            .transpose()?;
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            "requesting Vast.ai contract".to_owned(),
        );
        let instance = self
            .client
            .provision_node(request, &spec, selected_offer_id, exact_criteria)
            .map_err(|error| {
                classified_start_error(format!("vastai provision node {}: {error}", spec.node_id))
            })?;
        self.register_owned_node(spec, sink, label, instance)
    }

    fn register_owned_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        label: String,
        instance: Option<ProvisionedInstance>,
    ) -> Result<PluginNodeHandle, String> {
        self.client.ensure_event_source()?;
        let ownership_destroyed = self.client.ownership_destroyed(&label)?;
        let contract = instance.as_ref().map(|instance| instance.contract_id);
        let contract_id = Arc::new(Mutex::new(contract));
        emit_node_line(&sink, spec.run_id, spec.node_id, serde_json::json!({
            "type": if ownership_destroyed { "VastAiLeaseReleased" } else if contract.is_some() { "VastAiLeaseReady" } else { "VastAiLeaseResolutionPending" },
            "run_id": spec.run_id, "node_id": spec.node_id, "label": &label,
            "contract_id": contract,
            "offer_id": instance.as_ref().map(|instance| instance.offer_id),
            "host_id": instance.as_ref().and_then(|instance| instance.host_id),
        }).to_string());
        let bootstrap_state = Arc::new(Mutex::new(
            if ownership_destroyed || self.client.ownership_cancelled(&label)? {
                VastAiNodeBootstrap::Cancelled
            } else {
                VastAiNodeBootstrap::Dormant
            },
        ));
        let bootstrap = Arc::clone(&self.bootstrap);
        let state = Arc::clone(&bootstrap_state);
        let bootstrap_spec = spec.clone();
        let bootstrap_sink = sink.clone();
        let producer = self.bootstrap_producer.clone();
        let ssh_user = self.config.ssh_user.clone();
        let resolved_contract = Arc::clone(&contract_id);
        let (record_ownership, commit_cleanup) = self.client.ownership_callbacks(&label);
        let cleanup_sink = sink.clone();
        let cleanup_run = spec.run_id;
        let cleanup_node = spec.node_id;
        let cleanup_completed: CleanupObserver = Arc::new(move |contract| {
            commit_cleanup(contract)?;
            cleanup_sink.observe(PluginObservation::PhaseChanged {
                run_id: cleanup_run,
                node_id: cleanup_node,
                phase: crate::provisioning::ProvisionEventKind::NodeStopped,
            });
            Ok(())
        });
        let observe_snapshot: SnapshotObserver = Arc::new(move |contract, status| {
            let locally_cancelled = matches!(*state.lock(), VastAiNodeBootstrap::Cancelled);
            let cancelled = record_ownership(contract, locally_cancelled)?;
            *resolved_contract.lock() = Some(contract);
            if cancelled {
                if let VastAiNodeBootstrap::Running(mut owned_bootstrap)
                | VastAiNodeBootstrap::Ready(Some(mut owned_bootstrap)) =
                    std::mem::replace(&mut *state.lock(), VastAiNodeBootstrap::Cancelled)
                {
                    bootstrap.lock().stop_bootstrap(&mut owned_bootstrap);
                }
                return Ok(false);
            }
            let Some(status) = status else {
                return Ok(true);
            };
            if provider_terminal_start_error(
                contract,
                &status.actual_status,
                &status.intended_status,
                status.status_msg.as_deref(),
            )
            .is_some()
            {
                return Ok(true);
            }
            let endpoint = status.ssh_endpoint().map(|endpoint| VastAiSshEndpoint {
                host: endpoint.ip,
                port: endpoint.port,
                user: ssh_user.clone(),
            });
            reconcile_bootstrap(
                &bootstrap,
                &state,
                &bootstrap_spec,
                &bootstrap_sink,
                producer.clone(),
                endpoint,
            )?;
            Ok(true)
        });
        let provider_monitor = if ownership_destroyed {
            None
        } else {
            self.client.spawn_provider_monitor(
                contract,
                label.clone(),
                spec.clone(),
                sink.clone(),
                observe_snapshot,
                cleanup_completed,
            )
        };
        emit_node_line(
            &sink,
            spec.run_id,
            spec.node_id,
            "Vast.ai contract ownership ready for persistence".to_owned(),
        );

        let host_id = instance.as_ref().and_then(|instance| instance.host_id);
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
                contract_id,
                bootstrap: bootstrap_state,
                provider_monitor,
                host_id,
                run_id: spec.run_id,
                node_id: spec.node_id,
                label,
                sink,
                spec,
            },
        );
        Ok(handle)
    }
}

fn reconcile_bootstrap<B: VastAiBootstrapLauncher>(
    launcher: &Mutex<B>,
    state: &Mutex<VastAiNodeBootstrap<B::Handle>>,
    spec: &NodeProvisionSpec,
    sink: &PluginSink,
    producer: Option<TelemetryProducer>,
    endpoint: Option<VastAiSshEndpoint>,
) -> Result<(), String> {
    let mut state = state.lock();
    match &mut *state {
        VastAiNodeBootstrap::Dormant
        | VastAiNodeBootstrap::Ready(_)
        | VastAiNodeBootstrap::Cancelled => Ok(()),
        VastAiNodeBootstrap::Running(handle) => launcher.lock().refresh_bootstrap(handle),
        VastAiNodeBootstrap::Pending => {
            let Some(endpoint) = endpoint else {
                emit_node_line(
                    sink,
                    spec.run_id,
                    spec.node_id,
                    "Vast.ai SSH endpoint pending; awaiting provider notification",
                );
                return Ok(());
            };
            emit_node_line(
                sink,
                spec.run_id,
                spec.node_id,
                format!(
                    "Vast.ai SSH endpoint ready: {}:{}",
                    endpoint.host, endpoint.port
                ),
            );
            match launcher
                .lock()
                .start_bootstrap(spec.clone(), endpoint, sink.clone(), producer)
            {
                Ok(handle) => {
                    *state = VastAiNodeBootstrap::Running(handle);
                    Ok(())
                }
                Err(error) => {
                    *state = VastAiNodeBootstrap::Cancelled;
                    let reason = classified_start_error(format!(
                        "vastai bootstrap node {}: {error}",
                        spec.node_id
                    ));
                    sink.observe(PluginObservation::Failed {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        reason: reason.clone(),
                    });
                    Err(reason)
                }
            }
        }
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
    B::Handle: 'static,
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
        {
            let mut state = node.bootstrap.lock();
            match &*state {
                VastAiNodeBootstrap::Running(_) | VastAiNodeBootstrap::Ready(_) => return Ok(()),
                VastAiNodeBootstrap::Cancelled => {
                    return Err("bootstrap_cancelled: VastAI node attempt was retired".to_owned());
                }
                VastAiNodeBootstrap::Dormant | VastAiNodeBootstrap::Pending => {
                    *state = VastAiNodeBootstrap::Pending;
                }
            }
        }
        if let Some(monitor) = &node.provider_monitor {
            monitor.refresh();
            return Ok(());
        }
        let contract_id = *node.contract_id.lock();
        let endpoint = match contract_id {
            Some(contract_id) => {
                self.client
                    .ssh_endpoint(contract_id, &node.label, &self.config.ssh_user)?
            }
            None => None,
        };
        let result = reconcile_bootstrap(
            &self.bootstrap,
            &node.bootstrap,
            &node.spec,
            &node.sink,
            self.bootstrap_producer.clone(),
            endpoint,
        );
        if result.is_err()
            && let Some(host_id) = node.host_id
        {
            self.failed_host_ids.insert(host_id);
        }
        result
    }

    fn cancel_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.get_mut(&handle.id) else {
            return Ok(());
        };
        self.client.cancel_ownership(&node.label)?;
        if let VastAiNodeBootstrap::Running(mut bootstrap)
        | VastAiNodeBootstrap::Ready(Some(mut bootstrap)) =
            std::mem::replace(&mut *node.bootstrap.lock(), VastAiNodeBootstrap::Cancelled)
        {
            self.bootstrap.lock().stop_bootstrap(&mut bootstrap);
        }
        Ok(())
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.get_mut(&handle.id) else {
            return Ok(());
        };
        let mut state = node.bootstrap.lock();
        if matches!(
            *state,
            VastAiNodeBootstrap::Ready(_) | VastAiNodeBootstrap::Cancelled
        ) {
            return Ok(());
        }
        if let VastAiNodeBootstrap::Running(mut bootstrap) =
            std::mem::replace(&mut *state, VastAiNodeBootstrap::Ready(None))
        {
            self.bootstrap.lock().complete_bootstrap(&mut bootstrap);
            *state = VastAiNodeBootstrap::Ready(Some(bootstrap));
        }
        drop(state);
        emit_node_line(
            &node.sink,
            node.run_id,
            node.node_id,
            serde_json::json!({
                "type": "VastAiRuntimeReadyAccepted",
                "run_id": node.run_id,
                "node_id": node.node_id,
                "label": &node.label,
                "contract_id": *node.contract_id.lock(),
                "classification": "runtime_ready_over_provider_staleness",
            })
            .to_string(),
        );
        if let Some(monitor) = node.provider_monitor.as_ref() {
            monitor.stop_logs();
        }
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(mut node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        if let Err(error) = self.client.cancel_ownership(&node.label) {
            self.nodes.insert(handle.id, node);
            return Err(error);
        }
        if let VastAiNodeBootstrap::Running(mut bootstrap)
        | VastAiNodeBootstrap::Ready(Some(mut bootstrap)) =
            std::mem::replace(&mut *node.bootstrap.lock(), VastAiNodeBootstrap::Cancelled)
        {
            self.bootstrap.lock().stop_bootstrap(&mut bootstrap);
        }
        let contract_id = *node.contract_id.lock();
        let result = match contract_id {
            Some(contract_id) => self.client.destroy_owned_contract(&node.label, contract_id),
            None => self.client.ownership_destroyed(&node.label).and_then(|destroyed| {
                if destroyed { Ok(()) }
                else { Err("cleanup_pending: VastAI create outcome is unresolved; cancellation remains owned".to_owned()) }
            }),
        };
        emit_node_line(
            &node.sink,
            node.run_id,
            node.node_id,
            serde_json::json!({
                "type": "VastAiContractCleanup",
                "run_id": node.run_id,
                "node_id": node.node_id,
                "label": &node.label,
                "contract_id": contract_id,
                "result": if result.is_ok() { "ok" } else { "failed" },
                "error": result.as_ref().err(),
            })
            .to_string(),
        );
        match result {
            Ok(()) => {
                if let Some(mut monitor) = node.provider_monitor.take() {
                    monitor.stop();
                }
                if let Some(host_id) = node.host_id {
                    self.leased_host_ids.remove(&host_id);
                }
                Ok(())
            }
            Err(error) => {
                if let Some(monitor) = &node.provider_monitor {
                    monitor.refresh();
                }
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

        let contract = match self.client.contract_by_label(&label) {
            Ok(contract) => contract,
            Err(error) => {
                emit_node_line(
                    &sink,
                    spec.run_id,
                    spec.node_id,
                    format!("VastAI recovery resolution pending: {error}"),
                );
                None
            }
        };
        // Absence from one eventually-consistent listing is not permission to
        // create another rental. The handle retains pending resolution instead.
        let handle = self.register_owned_node(
            spec.clone(),
            sink,
            label.clone(),
            contract.map(adopted_instance),
        )?;
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
        self.client.cancel_ownership(&label)?;
        if self.client.ownership_destroyed(&label)? {
            return Ok(false);
        }
        let Some(contract_id) = self.client.contract_by_label(&label)? else {
            let handle = self.register_owned_node(spec.clone(), sink, label, None)?;
            return self.stop_node(&handle).map(|()| true);
        };
        let result = self.client.destroy_owned_contract(&label, contract_id);
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

    use crate::tests::fuzz_support::{actor_census, drive_steps};

    use super::*;

    #[test]
    fn bootstrap_command_has_stable_remote_idempotency_guard() {
        let directory = tempfile::tempdir().unwrap();
        let result = directory.path().join("result");
        let spec = NodeProvisionSpec {
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: vec![
                ("PLAIN".to_owned(), "value".to_owned()),
                ("QUOTED".to_owned(), "it's ready".to_owned()),
                ("RESULT".to_owned(), result.display().to_string()),
            ],
            args: vec![format!(
                "# {}\nprintf 'ready\\n'; read released; printf '%s\\n' \"$PLAIN\" \"$QUOTED\" >> \"$RESULT\"",
                directory.path().display()
            )],
            offer_criteria_json: None,
            mounts: Vec::new(),
        };

        let command = idempotent_ssh_bootstrap_command(&spec, &[]);

        let driver = r#"
import pathlib, shutil, subprocess, sys
command, lock, result = sys.argv[1:]
first = subprocess.Popen(['sh', '-c', command], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
try:
    assert first.stdout.readline() == b'ready\n'
    duplicate = subprocess.run(['sh', '-c', command], capture_output=True)
    assert duplicate.returncode == 0, duplicate.stderr
    assert not pathlib.Path(result).exists(), 'duplicate activation ran while owner was live'
    stdout, stderr = first.communicate(b'release\n')
    assert first.returncode == 0, stderr
    assert pathlib.Path(result).read_text() == "value\nit's ready\n"
    completed = subprocess.run(['sh', '-c', command], capture_output=True)
    assert completed.returncode == 0, completed.stderr
    assert pathlib.Path(result).read_text() == "value\nit's ready\n", 'completed command ran twice'
finally:
    if first.poll() is None:
        first.kill()
    first.wait()
    shutil.rmtree(lock, ignore_errors=True)
"#;
        let output = swactor_process::command_output(
            std::process::Command::new("python3")
                .args(["-c", driver])
                .arg(command)
                .arg(ssh_bootstrap_lock(&spec))
                .arg(result),
        )
        .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn ssh_bootstrap_late_binds_the_live_orchestrator_endpoint() {
        let spec = NodeProvisionSpec {
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: vec![
                (
                    "MYELIN_COORDINATOR_ENDPOINT".to_owned(),
                    "persisted-relay-a".to_owned(),
                ),
                (
                    "MYELIN_ORCHESTRATOR_ACTOR".to_owned(),
                    "persisted-actor".to_owned(),
                ),
            ],
            args: vec!["exec /usr/local/bin/myelin-node".to_owned()],
            offer_criteria_json: None,
            mounts: Vec::new(),
        };
        let live_env: BootstrapEnvSource = Arc::new(|| {
            Ok(vec![
                (
                    "MYELIN_COORDINATOR_ENDPOINT".to_owned(),
                    "live-relay-b".to_owned(),
                ),
                (
                    "MYELIN_ORCHESTRATOR_ACTOR".to_owned(),
                    "live-actor".to_owned(),
                ),
            ])
        });

        let command = ssh_bootstrap_command_for_attempt(&spec, &live_env).unwrap();

        assert!(
            command.contains("MYELIN_COORDINATOR_ENDPOINT=live-relay-b"),
            "SSH bootstrap could not locate the live orchestrator endpoint: {command}"
        );
        assert!(!command.contains("persisted-relay-a"));
    }

    #[test]
    fn bootstrap_completion_guard_is_scoped_to_the_command() {
        let mut spec = NodeProvisionSpec {
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: vec!["test -x /usr/local/bin/myelin-node".to_owned()],
            offer_criteria_json: None,
            mounts: Vec::new(),
        };
        let old_lock = ssh_bootstrap_lock(&spec);
        assert_eq!(old_lock, ssh_bootstrap_lock(&spec));

        spec.args = vec!["exec /usr/local/bin/myelin-node".to_owned()];

        assert_ne!(old_lock, ssh_bootstrap_lock(&spec));
    }

    #[test]
    fn provision_one_adopts_stable_label_before_offer_search() {
        let engine = http_engine();
        let server = TestHttpServer::start(
            &engine.handle().blocking_work_sender(),
            vec![TestHttpRoute::json(
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
            )],
        )
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
        ssh_endpoint_calls: Arc<AtomicUsize>,
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
            ssh_user: &str,
        ) -> Result<Option<VastAiSshEndpoint>, String> {
            self.ssh_endpoint_calls.fetch_add(1, Ordering::SeqCst);
            Ok(Some(VastAiSshEndpoint {
                host: "host".to_owned(),
                port: 22,
                user: ssh_user.to_owned(),
            }))
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
        ) -> Result<Self::Handle, String> {
            self.starts.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn stop_bootstrap(&mut self, _handle: &mut Self::Handle) {
            self.stops.fetch_add(1, Ordering::SeqCst);
        }

        fn complete_bootstrap(&mut self, _handle: &mut Self::Handle) {}
        fn refresh_bootstrap(&mut self, _handle: &mut Self::Handle) -> Result<(), String> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct PendingLeaseClient {
        observer: Arc<Mutex<Option<SnapshotObserver>>>,
        cleanup_completed: Arc<Mutex<Option<CleanupObserver>>>,
        destroyed: Arc<AtomicUsize>,
    }

    impl VastAiLeaseClient for PendingLeaseClient {
        fn provision_one(
            &mut self,
            _request: ProvisionRequest,
        ) -> Result<ProvisionedInstance, String> {
            panic!("ambiguous recovery must never create another contract")
        }

        fn contract_by_label(&mut self, _label: &str) -> Result<Option<u64>, String> {
            Ok(None)
        }

        fn ssh_endpoint(
            &mut self,
            _contract: u64,
            _label: &str,
            _user: &str,
        ) -> Result<Option<VastAiSshEndpoint>, String> {
            panic!("unresolved ownership has no endpoint to query")
        }

        fn spawn_provider_monitor(
            &mut self,
            _contract: Option<u64>,
            _label: String,
            _spec: NodeProvisionSpec,
            _sink: PluginSink,
            observer: SnapshotObserver,
            cleanup_completed: CleanupObserver,
        ) -> Option<VastAiProviderMonitor> {
            *self.observer.lock() = Some(observer);
            *self.cleanup_completed.lock() = Some(cleanup_completed);
            None
        }

        fn destroy_contract(&mut self, contract: u64) -> Result<(), String> {
            assert_eq!(contract, 73);
            self.destroyed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn cancel_unresolved_recovery_waits_for_cleanup_completion_without_bootstrapping() {
        let observer = Arc::new(Mutex::new(None));
        let cleanup_completed = Arc::new(Mutex::new(None));
        let destroyed = Arc::new(AtomicUsize::new(0));
        let starts = Arc::new(AtomicUsize::new(0));
        let recording = Arc::new(RecordingSink::default());
        let mut plugin = VastAiProvisioningPlugin::new(
            PendingLeaseClient {
                observer: Arc::clone(&observer),
                destroyed: Arc::clone(&destroyed),
                cleanup_completed: Arc::clone(&cleanup_completed),
            },
            CountingBootstrap {
                starts: Arc::clone(&starts),
                stops: Arc::new(AtomicUsize::new(0)),
            },
            VastAiProvisioningConfig::default(),
        );
        let adopted = plugin
            .adopt_by_spec(&monitor_spec(5, 7), PluginSink::new(recording.clone()))
            .unwrap()
            .expect("pending ownership needs a retained handle");
        plugin.start_bootstrap(&adopted.handle).unwrap();
        assert!(
            plugin
                .stop_node(&adopted.handle)
                .unwrap_err()
                .contains("cleanup_pending")
        );
        assert!(plugin.nodes.contains_key(&adopted.handle.id));
        assert_eq!(destroyed.load(Ordering::SeqCst), 0);
        let callback = observer.lock().clone().unwrap();
        assert!(!callback(73, None).unwrap());
        assert_eq!(
            destroyed.load(Ordering::SeqCst),
            0,
            "observation must not issue blocking HTTP"
        );
        assert!(!observations(&recording).iter().any(|observation| matches!(
            observation,
            PluginObservation::PhaseChanged {
                phase: crate::provisioning::ProvisionEventKind::NodeStopped,
                ..
            },
        )));
        plugin.client.destroy_contract(73).unwrap();
        cleanup_completed.lock().as_ref().unwrap()(73).unwrap();
        assert_eq!(destroyed.load(Ordering::SeqCst), 1);
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        assert!(observations(&recording).iter().any(|observation| matches!(
            observation,
            PluginObservation::PhaseChanged {
                run_id: 5,
                node_id: 7,
                phase: crate::provisioning::ProvisionEventKind::NodeStopped
            },
        )));
    }

    #[test]
    fn owned_task_port_resolves_cancelled_lease_and_commits_actual_deletion() {
        struct CompletionSink(std::sync::mpsc::Sender<PluginObservation>);
        impl crate::provisioning::PluginObservationSink for CompletionSink {
            fn observe(&self, observation: PluginObservation) {
                self.0.send(observation).unwrap();
            }
        }

        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let engine = Engine::new(
            parts,
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig::default()).unwrap(),
        )
        .unwrap();
        let tasks = VastAiTaskPort::new(
            &engine.task_spawner(),
            engine.handle().blocking_work_sender(),
        );
        let directory = tempfile::tempdir().unwrap();
        let webhooks = VastAiWebhooks::open(
            webhooks::VastAiWebhookConfig {
                public_url: "https://receiver.example/api/providers/vastai/webhook".to_owned(),
                event_types: vec!["client:instance_started".to_owned()],
                state_dir: directory.path().to_owned(),
                signing_secret: Some("fixture-secret".to_owned()),
                user_id: Some(17),
                register: false,
            },
            "secret",
        )
        .unwrap();
        let label = "run-5-node-7-attempt-9";
        let server = TestHttpServer::start(
            &engine.handle().blocking_work_sender(),
            vec![
                TestHttpRoute::json(
                    "GET",
                    "/api/v0/instances/",
                    200,
                    json!({
                        "instances": [{
                            "id": 73, "label": label, "actual_status": "loading",
                            "ssh_host": "", "ssh_port": 0, "public_ipaddr": ""
                        }]
                    }),
                ),
                TestHttpRoute::json(
                    "DELETE",
                    "/api/v0/instances/73/",
                    200,
                    json!({"success": true}),
                ),
            ],
        )
        .unwrap();
        let mut client =
            ToolsVastAiLeaseClient::new(VastClient::with_base_url(server.uri(), "secret"))
                .unwrap()
                .with_actor_host(runtime, tasks, webhooks);
        client.cancel_ownership(label).unwrap();
        let starts = Arc::new(AtomicUsize::new(0));
        let mut plugin = VastAiProvisioningPlugin::new(
            client,
            CountingBootstrap {
                starts: starts.clone(),
                stops: Arc::new(AtomicUsize::new(0)),
            },
            VastAiProvisioningConfig::default(),
        );
        let (completed, observations) = std::sync::mpsc::channel();
        let _handle = plugin
            .register_owned_node(
                monitor_spec(5, 7),
                PluginSink::new(Arc::new(CompletionSink(completed))),
                label.to_owned(),
                None,
            )
            .unwrap();
        loop {
            match observations.recv().unwrap() {
                PluginObservation::PhaseChanged {
                    run_id: 5,
                    node_id: 7,
                    phase: crate::provisioning::ProvisionEventKind::NodeStopped,
                } => break,
                PluginObservation::PhaseChanged {
                    phase: crate::provisioning::ProvisionEventKind::ProvisionFailed,
                    ..
                } => panic!("owned cleanup failed instead of confirming provider deletion"),
                _ => {}
            }
        }
        let record = LeaseJournal::new(directory.path())
            .load(label)
            .unwrap()
            .unwrap();
        assert_eq!(record.contract_id, Some(73));
        assert!(
            record.cancelled && record.destroyed,
            "stop acknowledgement must follow durable cleanup"
        );
        assert_eq!(
            starts.load(Ordering::SeqCst),
            0,
            "cancelled lease must never bootstrap"
        );
        assert_eq!(
            server
                .requests()
                .iter()
                .filter(|request| request.method == "DELETE")
                .count(),
            1
        );
        drop(plugin);
    }

    #[test]
    fn cancelling_owned_snapshot_closes_blocked_http_without_committing_cleanup() {
        use std::io::{BufRead, Read};
        struct Replies(std::sync::mpsc::Sender<VastAiProviderMonitorMsg>);
        impl ActorInterface for Replies {
            type Incoming = VastAiProviderMonitorMsg;
            type Response = ();
            fn handle(&mut self, _: &Ctx, reply: Self::Incoming) {
                self.0.send(reply).unwrap();
            }
        }
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let engine = Engine::new(
            parts,
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig::default()).unwrap(),
        )
        .unwrap();
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (accepted, request_started) = std::sync::mpsc::channel();
        let (release, released) = std::sync::mpsc::channel();
        let server = engine
            .handle()
            .blocking_work_sender()
            .spawn_owned_thread("provider-http-cancel", move || {
                let (stream, _) = listener.accept().unwrap();
                let mut reader = std::io::BufReader::new(stream);
                let mut line = String::new();
                loop {
                    line.clear();
                    assert_ne!(reader.read_line(&mut line).unwrap(), 0);
                    if line == "\r\n" {
                        break;
                    }
                }
                accepted.send(()).unwrap();
                let _ = released.recv();
                let mut byte = [0];
                reader.read(&mut byte).unwrap()
            })
            .unwrap();
        let (observed, replies) = std::sync::mpsc::channel();
        let actor = runtime.spawn(Replies(observed)).unwrap();
        let port = VastAiTaskPort::new(
            &engine.task_spawner(),
            engine.handle().blocking_work_sender(),
        );
        let callback_count = Arc::new(AtomicUsize::new(0));
        let observed_callbacks = callback_count.clone();
        let cleanup_callbacks = callback_count.clone();
        let lease = port.submit(
            VastClient::with_base_url(format!("http://{address}"), "secret"),
            None,
            "pending".to_owned(),
            VastAiTaskReply {
                sender: runtime.create_sender(),
                actor,
                generation: 7,
                poll: Some(3),
                settled: false,
            },
            Some(SnapshotCallbacks {
                observe: Arc::new(move |_, _| {
                    observed_callbacks.fetch_add(1, Ordering::SeqCst);
                    Ok(false)
                }),
                cleanup_completed: Arc::new(move |_| {
                    cleanup_callbacks.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }),
            }),
        );
        request_started.recv().unwrap();
        lease.cancel();
        assert!(matches!(
            replies.recv().unwrap(),
            VastAiProviderMonitorMsg::StatusCompleted {
                generation: 7,
                poll: 3,
                result: Err(_),
            }
        ));
        release.send(()).unwrap();
        assert_eq!(
            server.join().unwrap(),
            0,
            "cancellation must close the pending HTTP connection"
        );
        assert_eq!(
            callback_count.load(Ordering::SeqCst),
            0,
            "cancelled request cannot commit an observation or cleanup"
        );
    }

    #[test]
    fn durable_create_intent_cannot_be_reserved_twice_and_cancel_survives_resolution() {
        let directory = tempfile::tempdir().unwrap();
        let journal = LeaseJournal::new(directory.path());
        journal.reserve_creation("pending-lease").unwrap();
        assert!(
            LeaseJournal::new(directory.path())
                .reserve_creation("pending-lease")
                .is_err()
        );
        let mut client =
            ToolsVastAiLeaseClient::new(VastClient::with_base_url("http://127.0.0.1:1", "unused"))
                .unwrap();
        client.ownership = Some(Arc::new(Mutex::new(journal)));
        client.cancel_ownership("pending-lease").unwrap();
        client.record_contract("pending-lease", 73).unwrap();
        assert!(client.record_contract("pending-lease", 74).is_err());
        drop(client);
        let restored = LeaseJournal::new(directory.path())
            .load("pending-lease")
            .unwrap()
            .unwrap();
        assert_eq!(restored.contract_id, Some(73));
        assert!(restored.cancelled);
        assert!(!restored.destroyed);
    }

    #[test]
    fn node_creation_defers_bootstrap_and_failed_destroy_remains_retryable() {
        let destroy_calls = Arc::new(AtomicUsize::new(0));
        let destroy_failures = Arc::new(AtomicUsize::new(1));
        let starts = Arc::new(AtomicUsize::new(0));
        let ssh_endpoint_calls = Arc::new(AtomicUsize::new(0));
        let stops = Arc::new(AtomicUsize::new(0));
        let mut plugin = VastAiProvisioningPlugin::new(
            RetryDestroyClient {
                destroy_calls: Arc::clone(&destroy_calls),
                destroy_failures,
                existing_contract: None,
                ssh_endpoint_calls: Arc::clone(&ssh_endpoint_calls),
            },
            CountingBootstrap {
                starts: Arc::clone(&starts),
                stops: Arc::clone(&stops),
            },
            VastAiProvisioningConfig::default(),
        );
        let spec = NodeProvisionSpec {
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            offer_criteria_json: None,
            mounts: Vec::new(),
        };
        let handle = plugin
            .create_node(spec, PluginSink::new(Arc::new(NullSink)))
            .unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 0);
        assert_eq!(ssh_endpoint_calls.load(Ordering::SeqCst), 0);

        plugin.start_bootstrap(&handle).unwrap();
        assert_eq!(starts.load(Ordering::SeqCst), 1);
        assert_eq!(ssh_endpoint_calls.load(Ordering::SeqCst), 1);
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
    fn readiness_before_or_after_launch_keeps_bootstrap_and_cleanup_idempotent() {
        for launch in [false, true] {
            let starts = Arc::new(AtomicUsize::new(0));
            let stops = Arc::new(AtomicUsize::new(0));
            let destroys = Arc::new(AtomicUsize::new(0));
            let mut plugin = VastAiProvisioningPlugin::new(
                RetryDestroyClient {
                    destroy_calls: Arc::clone(&destroys),
                    destroy_failures: Arc::new(AtomicUsize::new(0)),
                    existing_contract: None,
                    ssh_endpoint_calls: Arc::new(AtomicUsize::new(0)),
                },
                CountingBootstrap {
                    starts: Arc::clone(&starts),
                    stops: Arc::clone(&stops),
                },
                VastAiProvisioningConfig::default(),
            );
            let handle = plugin
                .create_node(monitor_spec(5, 7), PluginSink::new(Arc::new(NullSink)))
                .unwrap();
            if launch {
                plugin.start_bootstrap(&handle).unwrap();
            }
            plugin.complete_bootstrap(&handle).unwrap();
            plugin.complete_bootstrap(&handle).unwrap();
            plugin.start_bootstrap(&handle).unwrap();
            assert_eq!(starts.load(Ordering::SeqCst), usize::from(launch));
            assert_eq!(stops.load(Ordering::SeqCst), 0);
            plugin.stop_node(&handle).unwrap();
            plugin.stop_node(&handle).unwrap();
            assert_eq!(stops.load(Ordering::SeqCst), usize::from(launch));
            assert_eq!(destroys.load(Ordering::SeqCst), 1);
        }
    }
    #[test]
    fn plugin_adopts_existing_contract_without_starting_bootstrap() {
        let starts = Arc::new(AtomicUsize::new(0));
        let mut plugin = VastAiProvisioningPlugin::new(
            RetryDestroyClient {
                destroy_calls: Arc::new(AtomicUsize::new(0)),
                destroy_failures: Arc::new(AtomicUsize::new(0)),
                existing_contract: Some(73),
                ssh_endpoint_calls: Arc::new(AtomicUsize::new(0)),
            },
            CountingBootstrap {
                starts: Arc::clone(&starts),
                stops: Arc::new(AtomicUsize::new(0)),
            },
            VastAiProvisioningConfig::default(),
        );
        let spec = NodeProvisionSpec {
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: vec!["run-worker".to_owned()],
            offer_criteria_json: None,
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
            confirm_lease: false,
        }
    }

    #[test]
    fn offer_search_is_read_only() {
        let engine = http_engine();
        let server = TestHttpServer::start(
            &engine.handle().blocking_work_sender(),
            vec![TestHttpRoute::json(
                "GET",
                "/api/v0/bundles/",
                200,
                json!({
                    "offers": [{
                        "id": 41,
                        "gpu_name": "A",
                        "dph_total": 0.2,
                        "internet_down_cost_per_tb": 0.0,
                        "internet_up_cost_per_tb": 0.0,
                        "host_id": 1,
                        "compute_cap": 800,
                        "reliability2": 0.99,
                        "inet_down": 500.0,
                        "inet_up": 500.0,
                        "geolocation": "US"
                    }]
                }),
            )],
        )
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
        let engine = http_engine();
        let server = TestHttpServer::start(
            &engine.handle().blocking_work_sender(),
            vec![
                TestHttpRoute::json("GET", "/api/v0/instances/", 200, json!({"instances": []})),
                TestHttpRoute::json(
                    "GET",
                    "/api/v0/bundles/",
                    200,
                    json!({
                        "offers": [
                            {"id": 41, "gpu_name": "A", "dph_total": 0.2, "host_id": 1,
                             "internet_down_cost_per_tb": 0.0, "internet_up_cost_per_tb": 0.0,
                             "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                             "geolocation": "US"},
                            {"id": 42, "gpu_name": "B", "dph_total": 0.3, "host_id": 2,
                             "internet_down_cost_per_tb": 0.0, "internet_up_cost_per_tb": 0.0,
                             "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                             "geolocation": "US"}
                        ]
                    }),
                ),
                TestHttpRoute::json("PUT", "/api/v0/asks/42/", 200, json!({"new_contract": 700})),
            ],
        )
        .unwrap();
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .unwrap();
        let instance = client
            .provision_exact(exact_request(), 42, OfferBrowseCriteria::default())
            .unwrap();
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
        let engine = http_engine();
        let server = TestHttpServer::start(
            &engine.handle().blocking_work_sender(),
            vec![
                TestHttpRoute::json("GET", "/api/v0/instances/", 200, json!({"instances": []})),
                TestHttpRoute::json(
                    "GET",
                    "/api/v0/bundles/",
                    200,
                    json!({
                        "offers": [{
                            "id": 41, "gpu_name": "A", "dph_total": 0.2, "host_id": 1,
                            "internet_down_cost_per_tb": 0.0, "internet_up_cost_per_tb": 0.0,
                            "compute_cap": 800, "reliability2": 0.99, "inet_down": 500.0,
                            "geolocation": "US"
                        }]
                    }),
                ),
            ],
        )
        .unwrap();
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .unwrap();
        assert!(
            client
                .provision_exact(exact_request(), 42, OfferBrowseCriteria::default())
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

    #[test]
    fn selected_offer_failure_emits_terminal_provider_evidence() {
        let recording = Arc::new(RecordingSink::default());
        let mut plugin = VastAiProvisioningPlugin::new(
            RetryDestroyClient {
                destroy_calls: Arc::new(AtomicUsize::new(0)),
                destroy_failures: Arc::new(AtomicUsize::new(0)),
                existing_contract: None,
                ssh_endpoint_calls: Arc::new(AtomicUsize::new(0)),
            },
            CountingBootstrap {
                starts: Arc::new(AtomicUsize::new(0)),
                stops: Arc::new(AtomicUsize::new(0)),
            },
            VastAiProvisioningConfig::default(),
        );
        let spec = NodeProvisionSpec {
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            offer_criteria_json: Some(
                serde_json::to_string(&OfferSearchRequest::default()).unwrap(),
            ),
            mounts: Vec::new(),
        };

        let error = plugin
            .create_node_selected(spec, PluginSink::new(recording.clone()), Some(42))
            .unwrap_err();
        let observations = recording.observations.lock();
        assert!(error.contains("does not support exact offer 42"));
        assert!(observations.iter().any(|observation| matches!(
            observation,
            PluginObservation::ProviderLine { line, .. }
                if line.contains("validating selected Vast.ai offer 42")
        )));
        assert!(observations.iter().any(|observation| matches!(
            observation,
            PluginObservation::ProviderLine { line, .. }
                if line.contains("Vast.ai provisioning failed")
        )));
        assert!(observations.iter().any(|observation| matches!(
            observation,
            PluginObservation::Failed { reason, .. }
                if reason.contains("does not support exact offer 42")
        )));
    }

    fn offer_fixture(id: u64) -> serde_json::Value {
        json!({
            "id": id,
            "gpu_name": "RTX 4090",
            "dph_total": 0.2,
            "internet_down_cost_per_tb": 0.0,
            "internet_up_cost_per_tb": 0.0,
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

    fn http_engine() -> Engine {
        Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig::default()).unwrap(),
        )
        .unwrap()
    }

    fn browse_offer_fixture(
        blocking: &swactor_engine::BlockingWorkSender,
        route: TestHttpRoute,
    ) -> OfferBrowseOutcome {
        let server =
            TestHttpServer::start(blocking, vec![route]).expect("start offer HTTP fixture");
        let mut client = ToolsVastAiLeaseClient::new(swactor_vastai::VastClient::with_base_url(
            server.uri(),
            "secret",
        ))
        .expect("blocking VastAI client");
        match client.browse_offers(&OfferBrowseCriteria::default()) {
            Ok(offers) => {
                OfferBrowseOutcome::Offers(offers.into_iter().map(|offer| offer.id).collect())
            }
            Err(error) => OfferBrowseOutcome::Rejected(error),
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum TerminalOutcome {
        MonitorRejected {
            run_id: u64,
            node_id: u64,
            reason: String,
        },
    }

    #[derive(Debug, PartialEq, Eq)]
    enum TerminalInvariantError {
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
            deployment: None,
            run_id,
            node_id,
            attempt_id: 9,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            offer_criteria_json: None,
            mounts: Vec::new(),
        }
    }

    fn scripted_monitor_status(kind: u8) -> Result<ProviderInstanceStatus, String> {
        match kind {
            0 => Ok(ProviderInstanceStatus {
                actual_status: "running".to_owned(),
                intended_status: "running".to_owned(),
                status_msg: None,
                public_ipaddr: Some("127.0.0.1".to_owned()),
                ssh_port: Some(22),
                disk_usage: None,
            }),
            1 => Ok(ProviderInstanceStatus {
                actual_status: "error".to_owned(),
                intended_status: "running".to_owned(),
                status_msg: Some("container failed".to_owned()),
                public_ipaddr: None,
                ssh_port: None,
                disk_usage: None,
            }),
            6 => Ok(ProviderInstanceStatus {
                actual_status: "loading".to_owned(),
                intended_status: "running".to_owned(),
                status_msg: Some("Installing packages".to_owned()),
                public_ipaddr: None,
                ssh_port: None,
                disk_usage: None,
            }),
            2 => Err("not found while fetching provider status".to_owned()),
            3 | 5 => Err("provider status parse failed".to_owned()),
            _ => Err("transient provider status failure".to_owned()),
        }
    }

    fn monitor_poll_count(recording: &RecordingSink) -> usize {
        observations(recording)
            .iter()
            .filter(|observation| {
                let PluginObservation::ProviderLine { line, .. } = observation else {
                    return false;
                };
                serde_json::from_str::<serde_json::Value>(line)
                    .ok()
                    .and_then(|value| value.get("type")?.as_str().map(str::to_owned))
                    .is_some_and(|kind| {
                        matches!(
                            kind.as_str(),
                            "VastAiProviderStatusObserved"
                                | "VastAiProviderStatusFailure"
                                | "VastAiProviderStatusPollRetry"
                        )
                    })
            })
            .count()
    }

    fn visible_status_count(recording: &RecordingSink) -> usize {
        observations(recording)
            .iter()
            .filter(|observation| {
                matches!(
                    observation,
                    PluginObservation::ProviderLine { line, .. }
                        if line.starts_with("Vast.ai ")
                )
            })
            .count()
    }

    struct MonitorHarness {
        runtime: Runtime,
        backend: SteppingBackend,
        _engine: Engine,
        actor: ActorAddress,
        baseline: usize,
        recording: Arc<RecordingSink>,
    }

    fn monitor_harness(contract_id: u64, spec: NodeProvisionSpec, kind: u8) -> MonitorHarness {
        monitor_harness_with_port(
            contract_id,
            spec,
            Some(scripted_monitor_status(kind)),
            VastAiTaskPort {
                commands: tokio::sync::mpsc::unbounded_channel().0,
            },
        )
    }

    fn monitor_harness_with_port(
        contract_id: u64,
        spec: NodeProvisionSpec,
        scripted_status: Option<Result<ProviderInstanceStatus, String>>,
        tasks: VastAiTaskPort,
    ) -> MonitorHarness {
        monitor_harness_with_optional_contract(
            Some(contract_id),
            spec,
            scripted_status,
            tasks,
            None,
        )
    }

    fn monitor_harness_with_optional_contract(
        contract_id: Option<u64>,
        spec: NodeProvisionSpec,
        scripted_status: Option<Result<ProviderInstanceStatus, String>>,
        tasks: VastAiTaskPort,
        observe_snapshot: Option<SnapshotObserver>,
    ) -> MonitorHarness {
        let client = VastClient::with_base_url("http://127.0.0.1:1", "secret");
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
                    spec,
                    sink: PluginSink::new(recording.clone()),
                    sender: runtime.create_sender(),
                    tasks,
                    observe_snapshot,
                    cleanup_completed: None,
                    invalidation_queued: Arc::new(AtomicBool::new(false)),

                    scripted_status,
                },
            ))
            .expect("spawn VastAI monitor");
        MonitorHarness {
            runtime,
            backend,
            _engine: engine,
            actor,
            baseline,
            recording,
        }
    }

    #[test]
    fn provider_invalidations_coalesce_while_snapshot_is_in_flight() {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let harness =
            monitor_harness_with_port(73, monitor_spec(5, 7), None, VastAiTaskPort { commands });
        drive_steps(&harness.backend, 8);
        let mut first = requests.try_recv().unwrap();
        for _ in 0..8 {
            harness
                .runtime
                .send_to(harness.actor, VastAiProviderMonitorMsg::Poll)
                .unwrap();
        }
        drive_steps(&harness.backend, 8);
        assert!(
            requests.try_recv().is_err(),
            "in-flight snapshot duplicated"
        );
        first.reply.status(scripted_monitor_status(0));
        drop(first);
        drive_steps(&harness.backend, 8);
        let mut follow_up = requests.try_recv().expect("dirty snapshot must reconcile");
        assert!(
            requests.try_recv().is_err(),
            "dirty events did not coalesce"
        );
        follow_up.reply.status(scripted_monitor_status(0));
        drop(follow_up);
        drive_steps(&harness.backend, 8);
        assert!(
            requests.try_recv().is_err(),
            "completion must not self-poll"
        );
        harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Stop)
            .unwrap();
        drive_steps(&harness.backend, 8);
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
    }

    #[test]
    fn repeated_reconciliation_error_consumes_only_one_real_notification() {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let harness = monitor_harness_with_optional_contract(
            None,
            monitor_spec(5, 7),
            None,
            VastAiTaskPort { commands },
            None,
        );
        drive_steps(&harness.backend, 8);
        let mut first = requests.try_recv().unwrap();
        harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Poll)
            .unwrap();
        harness
            .runtime
            .send_to(
                harness.actor,
                VastAiProviderMonitorMsg::ContractResolved {
                    generation: first.reply.generation,
                    contract_id: 73,
                },
            )
            .unwrap();
        drive_steps(&harness.backend, 8);
        assert!(
            requests.try_recv().is_err(),
            "resolution must not duplicate in-flight work"
        );
        first
            .reply
            .status(Err("ownership journal unavailable".to_owned()));
        drop(first);
        drive_steps(&harness.backend, 8);
        let mut next = requests
            .try_recv()
            .expect("accepted notification must survive resolution failure");
        assert_eq!(next.contract_id, Some(73));
        next.reply
            .status(Err("ownership journal unavailable".to_owned()));
        drop(next);
        drive_steps(&harness.backend, 8);
        assert!(
            requests.try_recv().is_err(),
            "completion must not invent another notification"
        );
        harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Stop)
            .unwrap();
        drive_steps(&harness.backend, 8);
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
    }

    #[test]
    fn unresolved_contract_waits_for_notification_without_fabricating_absence() {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let harness = monitor_harness_with_optional_contract(
            None,
            monitor_spec(5, 7),
            None,
            VastAiTaskPort { commands },
            None,
        );
        drive_steps(&harness.backend, 8);
        let mut first = requests.try_recv().unwrap();
        assert_eq!(first.contract_id, None);
        first.reply.resolution_pending();
        drop(first);
        drive_steps(&harness.backend, 8);
        assert!(requests.try_recv().is_err());
        assert_eq!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(None)
        );
        harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Poll)
            .unwrap();
        drive_steps(&harness.backend, 8);
        let mut resolved = requests.try_recv().unwrap();
        resolved
            .reply
            .resolved_status(73, scripted_monitor_status(0).unwrap());
        drop(resolved);
        drive_steps(&harness.backend, 8);
        assert_eq!(monitor_poll_count(&harness.recording), 1);
        assert_eq!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(None)
        );
        harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Stop)
            .unwrap();
        drive_steps(&harness.backend, 8);
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
    }

    #[test]
    fn provider_owner_loss_resolves_queued_poll_once() {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let harness =
            monitor_harness_with_port(73, monitor_spec(5, 7), None, VastAiTaskPort { commands });
        drive_steps(&harness.backend, 8);
        let request = requests
            .try_recv()
            .expect("accepted provider poll retains its reply");
        assert_eq!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(None)
        );
        let generation = request.reply.generation;
        let poll = request.reply.poll.unwrap();
        drop(request);
        harness
            .runtime
            .send_to(
                harness.actor,
                VastAiProviderMonitorMsg::StatusCompleted {
                    generation,
                    poll,
                    result: Err("duplicate terminal".to_owned()),
                },
            )
            .unwrap();
        drive_steps(&harness.backend, 16);
        assert!(matches!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(Some(TerminalOutcome::MonitorRejected { .. }))
        ));
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
        assert!(requests.try_recv().is_err());
    }

    #[test]
    fn provider_stale_status_cannot_discharge_new_poll() {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let harness =
            monitor_harness_with_port(73, monitor_spec(5, 7), None, VastAiTaskPort { commands });
        drive_steps(&harness.backend, 8);
        let mut first = requests.try_recv().unwrap();
        let generation = first.reply.generation;
        let poll = first.reply.poll.unwrap();
        first.reply.status(scripted_monitor_status(0));
        drop(first);
        drive_steps(&harness.backend, 8);
        harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Poll)
            .unwrap();
        drive_steps(&harness.backend, 8);
        let next = requests.try_recv().expect("next poll accepted");
        harness
            .runtime
            .send_to(
                harness.actor,
                VastAiProviderMonitorMsg::StatusCompleted {
                    generation,
                    poll,
                    result: Err("provider_task_owner_lost".to_owned()),
                },
            )
            .unwrap();
        drive_steps(&harness.backend, 8);
        assert_eq!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(None)
        );
        drop(next);
        drive_steps(&harness.backend, 16);
        assert!(matches!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(Some(TerminalOutcome::MonitorRejected { .. }))
        ));
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
    }

    #[test]
    fn provider_parent_stop_cancels_pending_owner() {
        let (commands, mut requests) = tokio::sync::mpsc::unbounded_channel();
        let harness =
            monitor_harness_with_port(73, monitor_spec(5, 7), None, VastAiTaskPort { commands });
        drive_steps(&harness.backend, 8);
        let request = requests.try_recv().unwrap();
        harness.runtime.stop_actor(harness.actor).unwrap();
        drive_steps(&harness.backend, 16);
        assert!(request.cancelled.has_changed().is_err() || *request.cancelled.borrow());
        drop(request);
        drive_steps(&harness.backend, 8);
        assert!(matches!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(Some(TerminalOutcome::MonitorRejected { .. }))
        ));
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
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
            Arc::new(|| Ok(Vec::new())),
            runtime.create_sender(),
            engine.handle(),
        );
        let actor = match mode {
            SshHarnessMode::Idle => actor.with_attempt_spawn_disabled(),
            SshHarnessMode::OpenStreams => actor.with_open_test_streams(),
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

    #[test]
    fn ssh_completion_keeps_transport_observation_owned_until_stop() {
        let harness = ssh_harness_with_mode(SshHarnessMode::OpenStreams);
        harness
            .runtime
            .send_to(harness.actor, SshBootstrapMsg::Complete)
            .unwrap();
        harness
            .runtime
            .send_to(harness.actor, SshBootstrapMsg::Complete)
            .unwrap();
        drive_steps(&harness.backend, 8);
        harness
            .runtime
            .send_to(
                harness.actor,
                SshBootstrapMsg::OutputLine {
                    attempt: 1,
                    stream: SshBootstrapStream::Stdout,
                    line: "after readiness".to_owned(),
                },
            )
            .unwrap();
        drive_steps(&harness.backend, 8);
        assert!(observations(&harness.recording).iter().any(|event| matches!(event, PluginObservation::StdoutLine { line, .. } if line == "after readiness")));
        harness
            .runtime
            .send_to(harness.actor, SshBootstrapMsg::Stop)
            .unwrap();
        drive_steps(&harness.backend, 16);
        assert_eq!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(None)
        );
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
    }

    #[test]
    fn ssh_duplicate_exit_and_stale_streams_do_not_steal_retry_ownership() {
        use std::os::unix::process::ExitStatusExt;
        let harness = ssh_harness_with_mode(SshHarnessMode::OpenStreams);
        harness
            .runtime
            .send_to(
                harness.actor,
                SshBootstrapMsg::ChildExited {
                    attempt: 1,
                    result: Ok(std::process::ExitStatus::from_raw(1 << 8)),
                },
            )
            .unwrap();
        harness
            .runtime
            .send_to(
                harness.actor,
                SshBootstrapMsg::ChildExited {
                    attempt: 1,
                    result: Err("duplicate".to_owned()),
                },
            )
            .unwrap();
        for stream in [SshBootstrapStream::Stdout, SshBootstrapStream::Stderr] {
            harness
                .runtime
                .send_to(
                    harness.actor,
                    SshBootstrapMsg::ReaderClosed { attempt: 1, stream },
                )
                .unwrap();
        }
        drive_steps(&harness.backend, 8);
        harness
            .runtime
            .send_to(harness.actor, SshBootstrapMsg::Refresh)
            .unwrap();
        drive_steps(&harness.backend, 16);
        for (attempt, line) in [(1, "stale output"), (2, "current output")] {
            harness
                .runtime
                .send_to(
                    harness.actor,
                    SshBootstrapMsg::OutputLine {
                        attempt,
                        stream: SshBootstrapStream::Stdout,
                        line: line.to_owned(),
                    },
                )
                .unwrap();
        }
        drive_steps(&harness.backend, 8);
        let lines = observations(&harness.recording)
            .into_iter()
            .filter_map(|event| match event {
                PluginObservation::StdoutLine { line, .. } => Some(line),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(lines, ["current output"]);
        harness
            .runtime
            .send_to(harness.actor, SshBootstrapMsg::Stop)
            .unwrap();
        drive_steps(&harness.backend, 16);
        assert!(matches!(
            single_terminal_outcome(&observations(&harness.recording)),
            Ok(Some(TerminalOutcome::MonitorRejected { .. }))
        ));
        assert!(runtime_is_clean(&harness.runtime, harness.baseline, 0));
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
        Stdout(String),
        ReaderError(bool),
        Refresh,
        Stop,
    }

    fn ssh_ordering_actions() -> impl Strategy<Value = Vec<SshOrderingAction>> {
        prop::collection::vec(
            prop_oneof![
                "[ -~]{0,16}".prop_map(SshOrderingAction::Stdout),
                any::<bool>().prop_map(SshOrderingAction::ReaderError),
                Just(SshOrderingAction::Refresh),
                Just(SshOrderingAction::Stop),
            ],
            0..=32,
        )
    }

    #[test]
    fn startup_status_refreshes_only_on_invalidation_and_deduplicates_visible_progress() {
        let harness = monitor_harness(73, monitor_spec(5, 7), 6);
        drive_steps(&harness.backend, 8);
        assert_eq!(monitor_poll_count(&harness.recording), 1);
        assert_eq!(visible_status_count(&harness.recording), 1);
        drive_steps(&harness.backend, 8);
        assert_eq!(monitor_poll_count(&harness.recording), 1);
        harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Poll)
            .unwrap();
        drive_steps(&harness.backend, 16);
        assert_eq!(monitor_poll_count(&harness.recording), 2);
        assert_eq!(visible_status_count(&harness.recording), 1);
        let lines = observations(&harness.recording);
        assert!(lines.iter().any(|observation| matches!(
            observation,
            PluginObservation::ProviderLine { line, .. }
                if line == "Vast.ai loading: Installing packages"
        )));
        let _ = harness
            .runtime
            .send_to(harness.actor, VastAiProviderMonitorMsg::Stop);
        drive_steps(&harness.backend, 8);
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
            let engine = http_engine();
            let outcome = browse_offer_fixture(&engine.handle().blocking_work_sender(), TestHttpRoute::json(
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
            let engine = http_engine();
            let outcome = browse_offer_fixture(&engine.handle().blocking_work_sender(), TestHttpRoute::raw(
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
        }

        #[test]
        fn wrong_or_missing_offer_fields_are_typed_rejections(kind in 0_u8..8) {
            let mut body = match kind {
                0 => json!({}),
                1 => json!({"offers": "wrong"}),
                2 => json!({"offers": {}}),
                3 => json!({"offers": [{"gpu_name": "A", "dph_total": 0.2}]}),
                4 => json!({"offers": [{"id": "41", "gpu_name": "A", "dph_total": 0.2}]}),
                5 => json!({"offers": [{"id": 41, "gpu_name": 9, "dph_total": 0.2}]}),
                6 => json!({"offers": [{"id": 41, "gpu_name": "A", "dph_total": "cheap"}]}),
                _ => json!({"offers": null}),
            };
            if let Some(offers) = body.get_mut("offers").and_then(serde_json::Value::as_array_mut) {
                for offer in offers {
                    offer["internet_down_cost_per_tb"] = json!(0.0);
                    offer["internet_up_cost_per_tb"] = json!(0.0);
                }
            }
            let engine = http_engine();
            let outcome = browse_offer_fixture(&engine.handle().blocking_work_sender(), TestHttpRoute::json(
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
        }

        #[test]
        fn duplicate_offer_records_remain_explicit_values(
            id in 0_u64..=1_000_000,
            repetitions in 2_usize..=32,
        ) {
            let repeated = (0..repetitions).map(|_| offer_fixture(id)).collect::<Vec<_>>();
            let engine = http_engine();
            let outcome = browse_offer_fixture(&engine.handle().blocking_work_sender(), TestHttpRoute::json(
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
            prop_assert_eq!(
                monitor_poll_count(&harness.recording),
                1,
                "actions={:?}, outcomes={:?}, census={}",
                actions,
                before_stop,
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
            drive_steps(&harness.backend, 64);
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
            let first_request_count = monitor_poll_count(&harness.recording);
            drive_steps(&harness.backend, 64);
            let second_request_count = monitor_poll_count(&harness.recording);
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
                    1 => drive_steps(&harness.backend, 8),
                    _ => {
                        let _ = harness
                            .runtime
                            .send_to(harness.actor, VastAiProviderMonitorMsg::Stop);
                        drive_steps(&harness.backend, 8);
                        stopped = true;
                        stopped_request_count
                            .get_or_insert_with(|| monitor_poll_count(&harness.recording));
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
                        monitor_poll_count(&harness.recording),
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
            drive_steps(&harness.backend, 64);
            let settled_requests = monitor_poll_count(&harness.recording);
            drive_steps(&harness.backend, 64);
            let outcomes = observations(&harness.recording);
            prop_assert_eq!(
                monitor_poll_count(&harness.recording),
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
                        attempt: 1,
                        stream: SshBootstrapStream::Stdout,
                        line: line.clone(),
                    },
                    SshOutputAction::Stderr(line) => SshBootstrapMsg::OutputLine {
                        attempt: 1,
                        stream: SshBootstrapStream::Stderr,
                        line: line.clone(),
                    },
                    SshOutputAction::Protocol(nonce) => SshBootstrapMsg::OutputLine {
                        attempt: 1,
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
            drive_steps(&harness.backend, 128);
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
                matches!(&terminal, Ok(Some(TerminalOutcome::MonitorRejected { .. }))),
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
                            attempt: 1,
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
            drive_steps(&harness.backend, 128);
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
                matches!(&terminal, Ok(Some(TerminalOutcome::MonitorRejected { .. })))
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
                            attempt: 1,
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
            drive_steps(&harness.backend, 128);
            let outcomes = observations(&harness.recording);
            let terminal = single_terminal_outcome(&outcomes);
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::MonitorRejected { .. })))
                    && runtime_is_clean(&harness.runtime, harness.baseline, 0),
                "actions={:?}, terminal={:?}, outcomes={:?}, census={}",
                actions,
                terminal,
                outcomes,
                actor_census(&harness.runtime),
            );
        }


        #[test]
        fn ssh_bootstrap_failure_waits_for_refresh_and_stop_is_terminal(
            advances in prop::collection::vec(any::<bool>(), 0..=31),
        ) {
            let harness = ssh_harness_with_mode(SshHarnessMode::OpenStreams);
            drive_steps(&harness.backend, 8);
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
                    SshBootstrapMsg::ChildExited {
                        attempt: 1,
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
            for stream in [SshBootstrapStream::Stdout, SshBootstrapStream::Stderr] {
                harness.runtime.send_to(harness.actor, SshBootstrapMsg::ReaderClosed { attempt: 1, stream }).unwrap();
            }
            drive_steps(&harness.backend, 4);
            for refresh in &advances {
                if *refresh {
                    let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Refresh);
                }
                drive_steps(&harness.backend, 4);
            }
            let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Stop);
            drive_steps(&harness.backend, 128);
            let outcomes = observations(&harness.recording);
            prop_assert!(
                outcomes.iter().any(|outcome| matches!(
                    outcome,
                    PluginObservation::ProviderLine { line, .. }
                        if line.contains("bootstrap timeout")
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
            drive_steps(&harness.backend, 128);
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
                matches!(&terminal, Ok(Some(TerminalOutcome::MonitorRejected { .. })))
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
                    SshOrderingAction::Stdout(line) => {
                        let _ = harness.runtime.send_to(
                            harness.actor,
                            SshBootstrapMsg::OutputLine {
                                attempt: 1,
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
                                attempt: 1,
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
                    SshOrderingAction::Refresh => {
                        let _ = harness.runtime.send_to(harness.actor, SshBootstrapMsg::Refresh);
                        drive_steps(&harness.backend, 4);
                    }
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
            drive_steps(&harness.backend, 128);
            let outcomes = observations(&harness.recording);
            let terminal = single_terminal_outcome(&outcomes);
            prop_assert!(
                matches!(&terminal, Ok(Some(TerminalOutcome::MonitorRejected { .. }))),
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
