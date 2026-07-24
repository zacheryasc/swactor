use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use datastream::DatastreamProducer;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime};
use swactor_vastai::{LifecyclePolicy, ProvisionRequest, ProvisionedInstance, SelectionPolicy};

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

pub trait VastAiLeaseClient: Send {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String>;

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        label: &str,
        lifecycle: &LifecyclePolicy,
        ssh_user: &str,
    ) -> Result<VastAiSshEndpoint, String>;

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String>;
}

pub struct ToolsVastAiLeaseClient {
    client: swactor_vastai::VastClient,
    runtime: tokio::runtime::Runtime,
}

impl ToolsVastAiLeaseClient {
    pub fn new(client: swactor_vastai::VastClient) -> Result<Self, String> {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|e| format!("vastai tokio runtime: {e}"))?;
        Ok(Self { client, runtime })
    }

    pub fn from_api_key(api_key: impl Into<String>) -> Result<Self, String> {
        Self::new(swactor_vastai::VastClient::new(api_key))
    }

    pub fn client(&self) -> &swactor_vastai::VastClient {
        &self.client
    }
}

impl VastAiLeaseClient for ToolsVastAiLeaseClient {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String> {
        let fleet = self.runtime.block_on(self.client.provision(request))?;
        let mut instances = fleet.instances;
        if instances.len() != 1 {
            return Err(format!(
                "vastai provision expected one instance, got {}",
                instances.len()
            ));
        }
        Ok(instances.remove(0))
    }

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        label: &str,
        lifecycle: &LifecyclePolicy,
        ssh_user: &str,
    ) -> Result<VastAiSshEndpoint, String> {
        self.runtime.block_on(async {
            let instances = self.client.list_by_label(label).await?;
            if let Some(instance) = instances
                .into_iter()
                .find(|instance| instance.contract_id == contract_id)
            {
                let host = if instance.ssh_host.is_empty() {
                    instance.public_ipaddr
                } else {
                    instance.ssh_host
                };
                return endpoint_from_parts(contract_id, host, instance.ssh_port, ssh_user);
            }

            let running = self.client.wait_for_running(contract_id, lifecycle).await?;
            endpoint_from_parts(contract_id, running.ip, running.port, ssh_user)
        })
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

fn spawn_retrying_ssh_bootstrap(
    spec: NodeProvisionSpec,
    endpoint: VastAiSshEndpoint,
    sink: PluginSink,
    producer: Option<DatastreamProducer>,
    ssh_identity: Option<PathBuf>,
    child_slot: Arc<Mutex<Option<Child>>>,
    stopping: Arc<AtomicBool>,
) {
    std::thread::spawn(move || {
        let run_id = spec.run_id;
        let node_id = spec.node_id;
        let mut attempt = 1u64;
        let mut backoff = Duration::from_secs(1);

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
                    bridge.spawn_stderr_reader(stderr);

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
                                let line = if status.success() {
                                    format!(
                                        "VastAI SSH bootstrap exited before runtime ready: {status}; retrying"
                                    )
                                } else {
                                    format!(
                                        "VastAI SSH bootstrap failed before runtime ready: {status}; retrying"
                                    )
                                };
                                sink.observe(PluginObservation::ProviderLine {
                                    run_id,
                                    node_id,
                                    line,
                                });
                                break;
                            }
                            Some(Err(error)) => {
                                sink.observe(PluginObservation::ProviderLine {
                                    run_id,
                                    node_id,
                                    line: format!("wait VastAI SSH bootstrap: {error}; retrying"),
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
                        line: format!("spawn VastAI SSH bootstrap failed: {error}; retrying"),
                    });
                }
            }

            if stopping.load(Ordering::SeqCst) {
                return;
            }
            sink.observe(PluginObservation::ProviderLine {
                run_id,
                node_id,
                line: format!("VastAI SSH bootstrap retrying in {}s", backoff.as_secs()),
            });
            std::thread::sleep(backoff);
            backoff = next_ssh_backoff(backoff);
            attempt += 1;
        }
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
    next_handle_id: u64,
    nodes: BTreeMap<u64, VastAiNode<B::Handle>>,
}

struct VastAiNode<H> {
    contract_id: u64,
    bootstrap: Option<H>,
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
        ProvisionRequest {
            count: 1,
            image: spec.image.clone(),
            label: Some(label),
            disk_gb: self.config.disk_gb,
            env,
            per_instance_env: vec![BTreeMap::new()],
            onstart: self.config.onstart.clone(),
            selection: self.config.selection.clone(),
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

impl<C, B> ProvisionPlugin for VastAiProvisioningPlugin<C, B>
where
    C: VastAiLeaseClient + 'static,
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
        let instance = self
            .client
            .provision_one(request)
            .map_err(|e| format!("vastai provision node {}: {e}", spec.node_id))?;
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: format!(
                "vastai contract {} ready for SSH lookup",
                instance.contract_id
            ),
        });

        let endpoint = match self.client.ssh_endpoint(
            instance.contract_id,
            &label,
            &self.config.lifecycle,
            &self.config.ssh_user,
        ) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                return Err(self.cleanup_contract_after_start_error(
                    instance.contract_id,
                    format!("vastai SSH endpoint node {}: {error}", spec.node_id),
                ));
            }
        };

        let bootstrap = match self.bootstrap.start_bootstrap(
            spec.clone(),
            endpoint,
            sink,
            self.bootstrap_producer.clone(),
        ) {
            Ok(handle) => handle,
            Err(error) => {
                return Err(self.cleanup_contract_after_start_error(
                    instance.contract_id,
                    format!("vastai bootstrap node {}: {error}", spec.node_id),
                ));
            }
        };

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
            },
        );
        Ok(handle)
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(mut node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        if let Some(mut bootstrap) = node.bootstrap.take() {
            self.bootstrap
                .stop_bootstrap(&mut bootstrap, BootstrapStopReason::NodeStop);
        }
        self.client.destroy_contract(node.contract_id)
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
    fn vastai_complete_bootstrap_keeps_log_tail_until_node_stop() {
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
            "VastAI bootstrap SSH tail must remain alive for post-ready worker logs"
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
