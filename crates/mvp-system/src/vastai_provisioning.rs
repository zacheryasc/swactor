use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use datastream::DatastreamProducer;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime};
use swactor_vastai::{
    CreateInstanceRequest, LifecyclePolicy, Offer, ProvisionRequest, ProvisionedInstance,
    SelectionPolicy, classify_vastai_error, create_instance,
};

use crate::bootstrap_datastream::{BootstrapDatastreamBridge, node_stream_id};
use crate::node_provisioning::{
    CreateLeaseRequest, CreateLeaseResult, DestroyHandle, LeaseFacts, LogicalNodeSpec,
    ProviderError, ProviderKind, ProviderLeaseId, ProviderPlugin, SshEndpoint,
};
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
    stopping: Arc<AtomicBool>,
    join: Option<JoinHandle<()>>,
}

impl VastAiProviderMonitor {
    fn new(stopping: Arc<AtomicBool>, join: JoinHandle<()>) -> Self {
        Self {
            stopping,
            join: Some(join),
        }
    }

    fn stop(&mut self) {
        self.stopping.store(true, Ordering::SeqCst);
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
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

    pub fn client(&self) -> &swactor_vastai::VastClient {
        &self.client
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

    fn monitor_provider_status(
        &mut self,
        contract_id: u64,
        label: String,
        lifecycle: LifecyclePolicy,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        stopping: Arc<AtomicBool>,
    ) {
        let mut last_state: Option<String> = None;
        let mut state_since = Instant::now();
        let mut poll = 0_u64;
        while !stopping.load(Ordering::SeqCst) {
            poll = poll.saturating_add(1);
            let status = match self
                .runtime
                .block_on(self.client.instance_status(contract_id))
            {
                Ok(status) => status,
                Err(error)
                    if error.contains("not found while fetching provider status")
                        || error.contains("parse failed") =>
                {
                    let reason = classified_start_error(format!(
                        "vastai provider monitor node {} contract {contract_id}: {error}",
                        spec.node_id
                    ));
                    sink.observe(PluginObservation::ProviderLine {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        line: serde_json::json!({
                            "type": "VastAiProviderStatusFailure",
                            "run_id": spec.run_id,
                            "node_id": spec.node_id,
                            "label": &label,
                            "contract_id": contract_id,
                            "poll": poll,
                            "reason": &reason,
                        })
                        .to_string(),
                    });
                    sink.observe(PluginObservation::Failed {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        reason,
                    });
                    return;
                }
                Err(error) => {
                    sink.observe(PluginObservation::ProviderLine {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        line: serde_json::json!({
                            "type": "VastAiProviderStatusPollRetry",
                            "run_id": spec.run_id,
                            "node_id": spec.node_id,
                            "label": &label,
                            "contract_id": contract_id,
                            "poll": poll,
                            "reason": error,
                        })
                        .to_string(),
                    });
                    if !sleep_provider_monitor(lifecycle.poll_interval, &stopping) {
                        return;
                    }
                    continue;
                }
            };

            let actual = status.actual_status.as_str();
            if last_state.as_deref() != Some(actual) {
                state_since = Instant::now();
                last_state = Some(actual.to_owned());
            }
            let in_state_ms = state_since.elapsed().as_millis();
            sink.observe(PluginObservation::ProviderLine {
                run_id: spec.run_id,
                node_id: spec.node_id,
                line: serde_json::json!({
                    "type": "VastAiProviderStatusObserved",
                    "run_id": spec.run_id,
                    "node_id": spec.node_id,
                    "label": &label,
                    "contract_id": contract_id,
                    "poll": poll,
                    "actual_status": &status.actual_status,
                    "intended_status": &status.intended_status,
                    "status_msg": &status.status_msg,
                    "disk_usage": status.disk_usage,
                    "in_state_ms": in_state_ms,
                })
                .to_string(),
            });

            if let Some(error) = provider_terminal_start_error(
                contract_id,
                &status.actual_status,
                &status.intended_status,
                status.status_msg.as_deref(),
            ) {
                let reason = classified_start_error(format!(
                    "vastai provider monitor node {}: {error}",
                    spec.node_id
                ));
                sink.observe(PluginObservation::ProviderLine {
                    run_id: spec.run_id,
                    node_id: spec.node_id,
                    line: serde_json::json!({
                        "type": "VastAiProviderTerminalBeforeRuntimeReady",
                        "run_id": spec.run_id,
                        "node_id": spec.node_id,
                        "label": &label,
                        "contract_id": contract_id,
                        "reason": &reason,
                    })
                    .to_string(),
                });
                sink.observe(PluginObservation::Failed {
                    run_id: spec.run_id,
                    node_id: spec.node_id,
                    reason,
                });
                return;
            }

            if !sleep_provider_monitor(lifecycle.poll_interval, &stopping) {
                return;
            }
        }
    }
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
        let stopping = Arc::new(AtomicBool::new(false));
        let thread_stopping = Arc::clone(&stopping);
        let mut client = self.clone();
        let join = std::thread::spawn(move || {
            client.monitor_provider_status(
                contract_id,
                label,
                lifecycle,
                spec,
                sink,
                thread_stopping,
            );
        });
        Some(VastAiProviderMonitor::new(stopping, join))
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
    if let Some(message) = msg {
        let lower = message.to_ascii_lowercase();
        if lower.contains("error") || lower.contains("failed") {
            return Some(format!("instance {contract_id} error: {message}"));
        }
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

fn sleep_provider_monitor(duration: Duration, stopping: &AtomicBool) -> bool {
    let deadline = Instant::now() + duration;
    while Instant::now() < deadline {
        if stopping.load(Ordering::SeqCst) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        std::thread::sleep(std::cmp::min(remaining, Duration::from_millis(100)));
    }
    !stopping.load(Ordering::SeqCst)
}

#[derive(Clone, Debug)]
pub struct VastAiProviderPlugin<C>
where
    C: VastAiLeaseClient,
{
    client: C,
    config: VastAiProvisioningConfig,
}

impl<C> VastAiProviderPlugin<C>
where
    C: VastAiLeaseClient,
{
    pub fn new(client: C, config: VastAiProvisioningConfig) -> Self {
        Self { client, config }
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    pub fn config(&self) -> &VastAiProvisioningConfig {
        &self.config
    }

    fn label_for(&self, spec: &LogicalNodeSpec) -> String {
        format!(
            "{}-{}-{}",
            self.config.label_prefix, spec.run_id.0, spec.logical_node_id.0
        )
    }

    fn selection_for(&self, spec: &LogicalNodeSpec) -> SelectionPolicy {
        let mut selection = self.config.selection.clone();
        if let Some(gpu_name) = &spec.shape.gpu_name {
            selection.gpu_name = Some(gpu_name.clone());
        }
        if let Some(min_gpu_ram_mb) = spec.shape.min_gpu_ram_mb {
            selection.min_gpu_ram_mb = Some(min_gpu_ram_mb);
        }
        if let Some(min_down_mbps) = spec.shape.min_down_mbps {
            selection.min_down_mbps = min_down_mbps;
        }
        if let Some(min_up_mbps) = spec.shape.min_up_mbps {
            selection.min_up_mbps = Some(min_up_mbps);
        }
        if let Some(min_reliability) = spec.shape.min_reliability {
            selection.min_reliability = min_reliability;
        }
        selection.require_verified = spec.shape.require_verified;
        selection
    }

    fn build_request(&self, spec: &LogicalNodeSpec, label: String) -> ProvisionRequest {
        let mut env = BTreeMap::new();
        env.insert("MVP_RUN_ID".into(), spec.run_id.0.to_string());
        env.insert("MVP_LOGICAL_NODE_ID".into(), spec.logical_node_id.0.clone());
        env.insert(
            "MVP_ORCH_SWACTOR_ADDR".into(),
            spec.swarm_join.orch_swactor_addr.clone(),
        );
        env.insert(
            "MVP_JOIN_TOKEN_REF".into(),
            spec.swarm_join.join_token_ref.clone(),
        );

        ProvisionRequest {
            count: 1,
            image: spec.shape.image.clone(),
            label: Some(label),
            disk_gb: spec.shape.disk_gb,
            env,
            per_instance_env: vec![BTreeMap::new()],
            preferred_offer_id: None,
            onstart: self.config.onstart.clone(),
            selection: self.selection_for(spec),
            lifecycle: self.config.lifecycle.clone(),
            confirm_lease: self.config.confirm_lease,
        }
    }

    fn lease_from_instance(
        spec: &LogicalNodeSpec,
        label: &str,
        instance: ProvisionedInstance,
    ) -> LeaseFacts {
        let contract_id = instance.contract_id.to_string();
        let lease_id = ProviderLeaseId(format!("vastai:{contract_id}"));
        let mut provider_metadata = BTreeMap::new();
        provider_metadata.insert("contract_id".into(), contract_id.clone());
        provider_metadata.insert("label".into(), label.to_owned());
        provider_metadata.insert("image".into(), spec.shape.image.clone());
        provider_metadata.insert("logical_node_id".into(), spec.logical_node_id.0.clone());
        provider_metadata.insert("ssh_user".into(), spec.boot.ssh_user.clone());
        provider_metadata.insert("offer_id".into(), instance.offer_id.to_string());
        provider_metadata.insert("gpu_name".into(), instance.gpu_name);
        provider_metadata.insert("dph_total".into(), instance.dph_total.to_string());
        if let Some(host_id) = instance.host_id {
            provider_metadata.insert("host_id".into(), host_id.to_string());
        }
        if let Some(gpu_ram) = instance.gpu_ram {
            provider_metadata.insert("gpu_ram".into(), gpu_ram.to_string());
        }

        LeaseFacts {
            provider: ProviderKind::VastAi,
            lease_id: lease_id.clone(),
            provider_contract_id: contract_id.clone(),
            offer_id: provider_metadata.get("offer_id").cloned(),
            destroy_handle: DestroyHandle {
                provider: ProviderKind::VastAi,
                lease_id,
                provider_contract_id: contract_id,
            },
            provider_metadata,
        }
    }

    fn contract_id(lease: &LeaseFacts) -> Result<u64, ProviderError> {
        if lease.provider != ProviderKind::VastAi {
            return Err(ProviderError::new(
                "vastai provider received non-vastai lease",
            ));
        }
        lease
            .provider_contract_id
            .parse::<u64>()
            .map_err(|e| ProviderError::new(format!("invalid vastai contract id: {e}")))
    }
}

impl<C> ProviderPlugin for VastAiProviderPlugin<C>
where
    C: VastAiLeaseClient,
{
    fn create_lease(
        &mut self,
        request: CreateLeaseRequest,
    ) -> Result<CreateLeaseResult, ProviderError> {
        if request.spec.provider != ProviderKind::VastAi {
            return Err(ProviderError::new(
                "vastai provider received non-vastai node spec",
            ));
        }
        let label = self.label_for(&request.spec);
        let provision_request = self.build_request(&request.spec, label.clone());
        let instance = self
            .client
            .provision_one(provision_request)
            .map_err(ProviderError::new)?;
        let lease = Self::lease_from_instance(&request.spec, &label, instance);
        Ok(CreateLeaseResult {
            lease,
            endpoint: None,
        })
    }

    fn lookup_endpoint(
        &mut self,
        lease: &LeaseFacts,
    ) -> Result<Option<SshEndpoint>, ProviderError> {
        let contract_id = Self::contract_id(lease)?;
        let label = lease
            .provider_metadata
            .get("label")
            .ok_or_else(|| ProviderError::new("vastai lease missing label"))?;
        let ssh_user = lease
            .provider_metadata
            .get("ssh_user")
            .map(String::as_str)
            .unwrap_or(&self.config.ssh_user);
        let endpoint = self
            .client
            .ssh_endpoint(contract_id, label, &self.config.lifecycle, ssh_user)
            .map_err(ProviderError::new)?;
        Ok(Some(SshEndpoint {
            host: endpoint.host,
            port: endpoint.port,
            user: endpoint.user,
            auth_ref: format!("vastai:{contract_id}:ssh"),
        }))
    }

    fn destroy_lease(&mut self, handle: &DestroyHandle) -> Result<(), ProviderError> {
        if handle.provider != ProviderKind::VastAi {
            return Err(ProviderError::new(
                "vastai provider received non-vastai destroy handle",
            ));
        }
        let contract_id = handle
            .provider_contract_id
            .parse::<u64>()
            .map_err(|e| ProviderError::new(format!("invalid vastai contract id: {e}")))?;
        self.client
            .destroy_contract(contract_id)
            .map_err(ProviderError::new)
    }
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

#[derive(Clone)]
enum SshBootstrapMsg {
    Stop,
}

struct SshBootstrapActor {
    child: Arc<Mutex<Option<Child>>>,
    stopping: Arc<AtomicBool>,
}

impl SshBootstrapActor {
    fn new(child: Arc<Mutex<Option<Child>>>, stopping: Arc<AtomicBool>) -> Self {
        Self { child, stopping }
    }
}

impl ActorInterface for SshBootstrapActor {
    type Incoming = SshBootstrapMsg;
    type Response = ();

    fn handle(&mut self, _ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            SshBootstrapMsg::Stop => {
                self.stopping.store(true, Ordering::SeqCst);
                stop_ssh_child(&self.child);
            }
        }
    }
}

fn stop_ssh_child(child_slot: &Arc<Mutex<Option<Child>>>) {
    let Some(mut child) = child_slot.lock().take() else {
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
        lifecycle: LifecyclePolicy,
    ) -> Result<Self::Handle, String> {
        if spec.args.is_empty() {
            return Err(format!(
                "VastAI node {} SSH bootstrap command missing",
                spec.node_id
            ));
        }

        let child = Arc::new(Mutex::new(None));
        let stopping = Arc::new(AtomicBool::new(false));
        let actor = self
            .runtime
            .spawn(SshBootstrapActor::new(child.clone(), stopping.clone()))
            .map_err(|e| format!("spawn VastAI SSH bootstrap actor: {e}"))?;
        spawn_retrying_ssh_bootstrap(
            spec,
            endpoint,
            sink,
            producer,
            self.ssh_identity.clone(),
            child,
            stopping,
            lifecycle.state_timeout,
        );

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

const POST_GRACE_BOOTSTRAP_FAILURE_LIMIT: u32 = 2;

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

fn post_grace_terminal_bootstrap_class(class: &str) -> bool {
    matches!(class, "auth_denied" | "refused" | "timeout")
}

fn spawn_classifying_stderr_reader<R>(
    stderr: R,
    bridge: BootstrapDatastreamBridge,
    observed_class: Arc<Mutex<Option<&'static str>>>,
) -> JoinHandle<()>
where
    R: Read + Send + 'static,
{
    std::thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for next in reader.lines() {
            match next {
                Ok(line) => {
                    if let Some(class) = classify_ssh_observation(&line) {
                        *observed_class.lock() = Some(class);
                        bridge.observe_provider_line(
                            serde_json::json!({
                                "type": "VastAiBootstrapObservationClass",
                                "run_id": bridge.spec().run_id,
                                "node_id": bridge.spec().node_id,
                                "class": class,
                            })
                            .to_string(),
                        );
                    }
                    bridge.observe_stderr_line(line);
                }
                Err(error) => {
                    bridge.observe_provider_line(format!("read VastAI SSH stderr: {error}"));
                    break;
                }
            }
        }
    })
}
fn spawn_retrying_ssh_bootstrap(
    spec: NodeProvisionSpec,
    endpoint: VastAiSshEndpoint,
    sink: PluginSink,
    producer: Option<DatastreamProducer>,
    ssh_identity: Option<PathBuf>,
    child_slot: Arc<Mutex<Option<Child>>>,
    stopping: Arc<AtomicBool>,
    post_grace_failure_after: Duration,
) {
    std::thread::spawn(move || {
        let run_id = spec.run_id;
        let node_id = spec.node_id;
        let mut attempt = 1u64;
        let mut backoff = Duration::from_secs(1);
        let bootstrap_started = Instant::now();
        let mut post_grace_failures = 0_u32;

        while !stopping.load(Ordering::SeqCst) {
            sink.observe(PluginObservation::ProviderLine {
                run_id,
                node_id,
                line: format!(
                    "VastAI SSH bootstrap attempt {attempt} to {}@{}:{}",
                    endpoint.user, endpoint.host, endpoint.port
                ),
            });

            match spawn_ssh_bootstrap_attempt(&spec, &endpoint, ssh_identity.as_deref()) {
                Ok((child, stdout, stderr)) => {
                    *child_slot.lock() = Some(child);
                    let bridge = BootstrapDatastreamBridge::new(
                        spec.clone(),
                        sink.clone(),
                        producer.clone(),
                    );
                    bridge.spawn_stdout_reader(stdout);
                    let observed_class = Arc::new(Mutex::new(None));
                    let mut stderr_reader = Some(spawn_classifying_stderr_reader(
                        stderr,
                        bridge.clone(),
                        Arc::clone(&observed_class),
                    ));

                    loop {
                        if stopping.load(Ordering::SeqCst) {
                            return;
                        }

                        let wait_result = {
                            let mut guard = child_slot.lock();
                            match guard.as_mut() {
                                Some(child) => match child.try_wait() {
                                    Ok(Some(status)) => {
                                        *guard = None;
                                        Some(Ok(status))
                                    }
                                    Ok(None) => None,
                                    Err(error) => {
                                        *guard = None;
                                        Some(Err(error))
                                    }
                                },
                                None => Some(Err(std::io::Error::new(
                                    std::io::ErrorKind::Other,
                                    "ssh child missing",
                                ))),
                            }
                        };

                        match wait_result {
                            Some(Ok(status)) => {
                                let readiness = if status.success() {
                                    "exited before runtime ready"
                                } else {
                                    "not ready before runtime ready"
                                };
                                if let Some(reader) = stderr_reader.take() {
                                    let _ = reader.join();
                                }
                                let observation_class =
                                    (*observed_class.lock()).unwrap_or("process_exit");
                                sink.observe(PluginObservation::ProviderLine {
                                    run_id,
                                    node_id,
                                    line: serde_json::json!({
                                        "type": "VastAiBootstrapAttemptCompleted",
                                        "run_id": run_id,
                                        "node_id": node_id,
                                        "attempt": attempt,
                                        "status": status.to_string(),
                                        "class": observation_class,
                                        "classification": readiness,
                                    })
                                    .to_string(),
                                });
                                if !status.success()
                                    && !post_grace_failure_after.is_zero()
                                    && bootstrap_started.elapsed() >= post_grace_failure_after
                                    && post_grace_terminal_bootstrap_class(observation_class)
                                {
                                    post_grace_failures = post_grace_failures.saturating_add(1);
                                    if post_grace_failures >= POST_GRACE_BOOTSTRAP_FAILURE_LIMIT {
                                        sink.observe(PluginObservation::Failed {
                                            run_id,
                                            node_id,
                                            reason: format!(
                                                "VastAI SSH bootstrap repeated post-grace {observation_class} failure before runtime ready"
                                            ),
                                        });
                                        return;
                                    }
                                } else {
                                    post_grace_failures = 0;
                                }
                                break;
                            }
                            Some(Err(error)) => {
                                if let Some(reader) = stderr_reader.take() {
                                    let _ = reader.join();
                                }
                                sink.observe(PluginObservation::ProviderLine {
                                    run_id,
                                    node_id,
                                    line: format!(
                                        "wait VastAI SSH bootstrap attempt {attempt}: {error}; retrying"
                                    ),
                                });
                                break;
                            }
                            None => std::thread::sleep(Duration::from_millis(100)),
                        }
                    }
                }
                Err(error) => {
                    sink.observe(PluginObservation::ProviderLine {
                        run_id,
                        node_id,
                        line: format!(
                            "spawn VastAI SSH bootstrap attempt {attempt} failed: {error}; retrying"
                        ),
                    });
                }
            }

            if stopping.load(Ordering::SeqCst) {
                return;
            }
            sink.observe(PluginObservation::ProviderLine {
                run_id,
                node_id,
                line: format!(
                    "VastAI SSH bootstrap retrying in {}s after attempt {attempt}",
                    backoff.as_secs()
                ),
            });
            if !sleep_ssh_backoff(backoff, &stopping) {
                return;
            }
            backoff = next_ssh_backoff(backoff);
            attempt += 1;
        }
    });
}

fn sleep_ssh_backoff(backoff: Duration, stopping: &AtomicBool) -> bool {
    let deadline = std::time::Instant::now() + backoff;
    while std::time::Instant::now() < deadline {
        if stopping.load(Ordering::SeqCst) {
            return false;
        }
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        std::thread::sleep(std::cmp::min(remaining, Duration::from_millis(50)));
    }
    !stopping.load(Ordering::SeqCst)
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

    pub fn with_bootstrap_producer(mut self, producer: DatastreamProducer) -> Self {
        self.bootstrap_producer = Some(producer);
        self
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    pub fn bootstrap(&self) -> &B {
        &self.bootstrap
    }

    pub fn bootstrap_mut(&mut self) -> &mut B {
        &mut self.bootstrap
    }

    pub fn config(&self) -> &VastAiProvisioningConfig {
        &self.config
    }

    pub fn active_contract_count(&self) -> usize {
        self.nodes.len()
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
        if let Some(monitor) = node.provider_monitor.as_mut() {
            monitor.stop();
        }
        node.provider_monitor = None;
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
        if let Some(mut bootstrap) = node.bootstrap.take() {
            self.bootstrap
                .stop_bootstrap(&mut bootstrap, BootstrapStopReason::RuntimeReady);
        }
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
    fn vastai_complete_bootstrap_stops_optional_log_tail_before_node_stop() {
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
        assert_eq!(
            *stop_reasons.lock(),
            vec![BootstrapStopReason::RuntimeReady]
        );

        plugin.stop_node(&handle).expect("VastAI node stops");
        assert_eq!(
            *stop_reasons.lock(),
            vec![BootstrapStopReason::RuntimeReady]
        );
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
    fn ssh_bootstrap_backoff_sleep_observes_stop_without_waiting_full_backoff() {
        let stopping = AtomicBool::new(true);
        let started = std::time::Instant::now();

        assert!(!sleep_ssh_backoff(Duration::from_secs(5), &stopping));
        assert!(
            started.elapsed() < Duration::from_millis(250),
            "stopped bootstrap backoff should not wait for the full retry delay"
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
        let child_slot = Arc::new(Mutex::new(Some(child)));
        let stopping = Arc::new(AtomicBool::new(false));
        let actor = runtime
            .spawn(SshBootstrapActor::new(child_slot.clone(), stopping.clone()))
            .expect("spawn ssh bootstrap actor");

        runtime
            .send_to(actor, SshBootstrapMsg::Stop)
            .expect("send stop");
        runtime.tick();

        assert!(stopping.load(Ordering::SeqCst));
        assert!(child_slot.lock().is_none());
        #[cfg(target_os = "linux")]
        assert!(
            !std::path::Path::new(&format!("/proc/{pid}")).exists(),
            "child process should be reaped"
        );
    }
}
