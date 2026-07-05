use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::process::{Child, Command, Stdio};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

use datastream::DatastreamProducer;
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

pub trait VastAiBootstrapLauncher: Send {
    type Handle: Send;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: VastAiSshEndpoint,
        sink: PluginSink,
        producer: Option<DatastreamProducer>,
    ) -> Result<Self::Handle, String>;

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle);
}

#[derive(Default, Debug, Clone, Copy)]
pub struct SshCommandBootstrapLauncher;

pub struct SshCommandBootstrapHandle {
    child: Arc<Mutex<Child>>,
    stopping: Arc<AtomicBool>,
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

        let mut command = Command::new("ssh");
        command
            .arg("-p")
            .arg(endpoint.port.to_string())
            .arg("-o")
            .arg("BatchMode=yes")
            .arg(format!("{}@{}", endpoint.user, endpoint.host))
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        command.arg(spec.args.join(" "));

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

        let run_id = spec.run_id;
        let node_id = spec.node_id;
        let exit_sink = sink.clone();
        let child = Arc::new(Mutex::new(child));
        let stopping = Arc::new(AtomicBool::new(false));
        let bridge = BootstrapDatastreamBridge::new(spec, sink, producer);
        bridge.spawn_stdout_reader(stdout);
        bridge.spawn_stderr_reader(stderr);
        spawn_ssh_exit_watcher(run_id, node_id, child.clone(), stopping.clone(), exit_sink);

        Ok(SshCommandBootstrapHandle { child, stopping })
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle) {
        handle.stopping.store(true, Ordering::SeqCst);
        let mut child = handle.child.lock();
        let _ = child.kill();
        let _ = child.wait();
    }
}

fn spawn_ssh_exit_watcher(
    run_id: u64,
    node_id: u64,
    child: Arc<Mutex<Child>>,
    stopping: Arc<AtomicBool>,
    sink: PluginSink,
) {
    std::thread::spawn(move || {
        loop {
            match child.lock().try_wait() {
                Ok(Some(status)) => {
                    if stopping.load(Ordering::SeqCst) {
                        return;
                    }
                    if status.success() {
                        sink.observe(PluginObservation::Exited {
                            run_id,
                            node_id,
                            status: status.code(),
                        });
                    } else {
                        sink.observe(PluginObservation::Failed {
                            run_id,
                            node_id,
                            reason: format!("VastAI SSH bootstrap exited: {status}"),
                        });
                    }
                    return;
                }
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(error) => {
                    if !stopping.load(Ordering::SeqCst) {
                        sink.observe(PluginObservation::Failed {
                            run_id,
                            node_id,
                            reason: format!("wait VastAI SSH bootstrap: {error}"),
                        });
                    }
                    return;
                }
            }
        }
    });
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
        let env = spec.env.iter().cloned().collect::<BTreeMap<_, _>>();
        ProvisionRequest {
            count: 1,
            image: spec.image.clone(),
            label: Some(label),
            disk_gb: self.config.disk_gb,
            env,
            per_instance_env: vec![BTreeMap::new()],
            onstart: self
                .config
                .onstart
                .clone()
                .or_else(|| (!spec.args.is_empty()).then(|| spec.args.join(" "))),
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

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(mut node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        if let Some(mut bootstrap) = node.bootstrap.take() {
            self.bootstrap.stop_bootstrap(&mut bootstrap);
        }
        self.client.destroy_contract(node.contract_id)
    }
}
