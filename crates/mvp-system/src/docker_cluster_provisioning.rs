//! Docker-backed provider and bootstrap wiring for node provisioning tests.
//!
//! Docker is treated as a concrete provider adapter here: it creates and destroys
//! real Docker container leases through a `DockerCli` boundary. Unit tests use a
//! deterministic CLI implementation, but the provider behavior remains the same
//! provider contract as a remote adapter.

use std::collections::{BTreeMap, BTreeSet};
use std::time::SystemTime;

use crate::node_provisioning::{
    BootstrapDatastreamSink, BootstrapLogRecord, BootstrapLogSource, BootstrapLogStream,
    BootstrapObservation, BootstrapSessionEvent, BootstrapSessionSpec, BootstrapStage,
    CreateLeaseRequest, CreateLeaseResult, DesiredNodeShape, DestroyHandle, LeaseFacts,
    LogicalNodeId, NodeManager, NodeManagerCommand, NodeManagerMsg, NodeRecord, ProviderError,
    ProviderKind, ProviderLeaseId, ProviderPlugin, RunId, RunNodeGroupSpec, SshEndpoint, SwactorId,
    SwarmJoinSpec, expand_node_group,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerCliError {
    pub reason: String,
}

impl DockerCliError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerRunRequest {
    pub container_name: String,
    pub image: String,
    pub run_id: RunId,
    pub logical_node_id: LogicalNodeId,
    pub ssh_user: String,
    pub labels: BTreeMap<String, String>,
    pub env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DockerRunResult {
    pub container_id: String,
}

pub trait DockerCli {
    fn run_container(
        &mut self,
        request: DockerRunRequest,
    ) -> Result<DockerRunResult, DockerCliError>;

    fn inspect_ssh_endpoint(
        &mut self,
        container_id: &str,
    ) -> Result<Option<SshEndpoint>, DockerCliError>;

    fn remove_force(&mut self, container_id: &str) -> Result<(), DockerCliError>;
}

#[derive(Clone, Debug)]
pub struct DockerProvider<C> {
    cli: C,
}

impl<C> DockerProvider<C> {
    pub fn new(cli: C) -> Self {
        Self { cli }
    }

    pub fn cli(&self) -> &C {
        &self.cli
    }

    pub fn cli_mut(&mut self) -> &mut C {
        &mut self.cli
    }

    pub fn into_cli(self) -> C {
        self.cli
    }
}

impl<C: DockerCli> DockerProvider<C> {
    fn build_run_request(request: &CreateLeaseRequest) -> DockerRunRequest {
        let spec = &request.spec;
        let mut labels = spec.shape.provider_labels.clone();
        labels.insert("mvp.provider".into(), "docker".into());
        labels.insert("mvp.run_id".into(), spec.run_id.0.to_string());
        labels.insert("mvp.logical_node_id".into(), spec.logical_node_id.0.clone());

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

        DockerRunRequest {
            container_name: format!("mvp-{}-{}", spec.run_id.0, spec.logical_node_id.0),
            image: spec.shape.image.clone(),
            run_id: spec.run_id.clone(),
            logical_node_id: spec.logical_node_id.clone(),
            ssh_user: spec.boot.ssh_user.clone(),
            labels,
            env,
        }
    }

    fn lease_from_container(
        shape: &DesiredNodeShape,
        logical_node_id: &LogicalNodeId,
        container_id: String,
        endpoint: &Option<SshEndpoint>,
    ) -> LeaseFacts {
        let lease_id = ProviderLeaseId(format!("docker:{container_id}"));
        let mut provider_metadata = BTreeMap::new();
        provider_metadata.insert("container_id".into(), container_id.clone());
        provider_metadata.insert("image".into(), shape.image.clone());
        provider_metadata.insert("logical_node_id".into(), logical_node_id.0.clone());
        if let Some(endpoint) = endpoint {
            provider_metadata.insert("ssh_host".into(), endpoint.host.clone());
            provider_metadata.insert("ssh_port".into(), endpoint.port.to_string());
        }

        LeaseFacts {
            provider: ProviderKind::Docker,
            lease_id: lease_id.clone(),
            provider_contract_id: container_id.clone(),
            offer_id: None,
            destroy_handle: DestroyHandle {
                provider: ProviderKind::Docker,
                lease_id,
                provider_contract_id: container_id,
            },
            provider_metadata,
        }
    }
}

impl<C: DockerCli> ProviderPlugin for DockerProvider<C> {
    fn create_lease(
        &mut self,
        request: CreateLeaseRequest,
    ) -> Result<CreateLeaseResult, ProviderError> {
        if request.spec.provider != ProviderKind::Docker {
            return Err(ProviderError::new(
                "docker provider received non-docker node spec",
            ));
        }
        let run_request = Self::build_run_request(&request);
        let run_result = self
            .cli
            .run_container(run_request)
            .map_err(|error| ProviderError::new(error.reason))?;
        let endpoint = self
            .cli
            .inspect_ssh_endpoint(&run_result.container_id)
            .map_err(|error| ProviderError::new(error.reason))?;
        let lease = Self::lease_from_container(
            &request.spec.shape,
            &request.spec.logical_node_id,
            run_result.container_id,
            &endpoint,
        );
        Ok(CreateLeaseResult { lease, endpoint })
    }

    fn lookup_endpoint(
        &mut self,
        lease: &LeaseFacts,
    ) -> Result<Option<SshEndpoint>, ProviderError> {
        self.cli
            .inspect_ssh_endpoint(&lease.provider_contract_id)
            .map_err(|error| ProviderError::new(error.reason))
    }

    fn destroy_lease(&mut self, handle: &DestroyHandle) -> Result<(), ProviderError> {
        if handle.provider != ProviderKind::Docker {
            return Err(ProviderError::new(
                "docker provider received non-docker destroy handle",
            ));
        }
        self.cli
            .remove_force(&handle.provider_contract_id)
            .map_err(|error| ProviderError::new(error.reason))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BootstrapSshError {
    pub reason: String,
}

impl BootstrapSshError {
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }
}

pub trait BootstrapSshClient {
    fn connect(&mut self, endpoint: &SshEndpoint) -> Result<(), BootstrapSshError>;
    fn probe_stdout(&mut self) -> Result<(), BootstrapSshError>;
    fn read_bootstrap_logs(
        &mut self,
        stdout_sources: &[String],
        stderr_sources: &[String],
    ) -> Result<Vec<(BootstrapLogStream, String)>, BootstrapSshError>;
    fn run_verify_commands(&mut self, commands: &[String]) -> Result<(), BootstrapSshError>;
    fn start_swactor(
        &mut self,
        command: &str,
        join: &SwarmJoinSpec,
    ) -> Result<(), BootstrapSshError>;
    fn close(&mut self);
}

#[derive(Clone, Debug)]
pub struct SshBootstrapSession<C> {
    spec: BootstrapSessionSpec,
    client: C,
    stage: BootstrapStage,
    next_seq: u64,
    last_stdout_seq: Option<u64>,
    last_stderr_seq: Option<u64>,
    closed: bool,
}

impl<C> SshBootstrapSession<C> {
    pub fn new(spec: BootstrapSessionSpec, client: C) -> Self {
        Self {
            spec,
            client,
            stage: BootstrapStage::Created,
            next_seq: 1,
            last_stdout_seq: None,
            last_stderr_seq: None,
            closed: false,
        }
    }

    pub fn client(&self) -> &C {
        &self.client
    }

    pub fn client_mut(&mut self) -> &mut C {
        &mut self.client
    }

    pub fn stage(&self) -> BootstrapStage {
        self.stage
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

impl<C: BootstrapSshClient> SshBootstrapSession<C> {
    pub fn start(&mut self, sink: &mut dyn BootstrapDatastreamSink) -> Vec<BootstrapSessionEvent> {
        let mut events = Vec::new();
        self.stage = BootstrapStage::SshConnecting;
        if let Err(error) = self.client.connect(&self.spec.ssh) {
            self.stage = BootstrapStage::SshTimeout;
            return vec![BootstrapSessionEvent::Failed(format!(
                "ssh connect: {}",
                error.reason
            ))];
        }
        if let Err(error) = self.client.probe_stdout() {
            self.stage = BootstrapStage::SshTimeout;
            return vec![BootstrapSessionEvent::Failed(format!(
                "ssh probe: {}",
                error.reason
            ))];
        }
        self.stage = BootstrapStage::SshReady;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::SshReady),
        ));

        self.stage = BootstrapStage::StdoutStreaming;
        let records = match self.client.read_bootstrap_logs(
            &self.spec.boot.stdout_sources,
            &self.spec.boot.stderr_sources,
        ) {
            Ok(records) => records,
            Err(error) => {
                self.stage = BootstrapStage::StreamError;
                events.push(BootstrapSessionEvent::Failed(format!(
                    "bootstrap log stream: {}",
                    error.reason
                )));
                return events;
            }
        };
        for (stream, line) in records {
            let seq = self.next_seq;
            self.next_seq += 1;
            match stream {
                BootstrapLogStream::Stdout => self.last_stdout_seq = Some(seq),
                BootstrapLogStream::Stderr => self.last_stderr_seq = Some(seq),
            }
            sink.record(BootstrapLogRecord {
                run_id: self.spec.run_id.clone(),
                logical_node_id: self.spec.logical_node_id.clone(),
                lease_id: self.spec.lease_id.clone(),
                source: BootstrapLogSource::SshBootstrap,
                stream,
                seq,
                timestamp: SystemTime::now(),
                line,
            });
        }
        events.push(BootstrapSessionEvent::Observed(BootstrapObservation {
            stage: BootstrapStage::StdoutStreaming,
            last_stdout_seq: self.last_stdout_seq,
            last_stderr_seq: self.last_stderr_seq,
            marker: None,
        }));

        self.stage = BootstrapStage::BootChecking;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::BootChecking),
        ));
        if let Err(error) = self
            .client
            .run_verify_commands(&self.spec.boot.verify_commands)
        {
            self.stage = BootstrapStage::BootCheckFailed;
            events.push(BootstrapSessionEvent::Failed(format!(
                "boot check failed: {}",
                error.reason
            )));
            return events;
        }

        self.stage = BootstrapStage::SwactorStarting;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::SwactorStarting),
        ));
        if let Err(error) = self
            .client
            .start_swactor(&self.spec.boot.start_swactor_command, &self.spec.swarm_join)
        {
            self.stage = BootstrapStage::StartFailed;
            events.push(BootstrapSessionEvent::Failed(format!(
                "swactor start failed: {}",
                error.reason
            )));
            return events;
        }

        self.stage = BootstrapStage::WaitingForSwactorJoin;
        events.push(BootstrapSessionEvent::Observed(
            BootstrapObservation::stage(BootstrapStage::WaitingForSwactorJoin),
        ));
        events
    }

    pub fn convergence_observed(
        &mut self,
        _swactor_id: SwactorId,
        sink: &mut dyn BootstrapDatastreamSink,
    ) -> Vec<BootstrapSessionEvent> {
        self.stage = BootstrapStage::Converged;
        sink.flush();
        self.client.close();
        self.closed = true;
        self.stage = BootstrapStage::Closed;
        vec![
            BootstrapSessionEvent::Observed(BootstrapObservation::stage(BootstrapStage::Converged)),
            BootstrapSessionEvent::Closed,
        ]
    }

    pub fn cancel(&mut self) -> Vec<BootstrapSessionEvent> {
        self.stage = BootstrapStage::Cancelled;
        self.client.close();
        self.closed = true;
        vec![BootstrapSessionEvent::Closed]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SwactorJoinEvent {
    pub logical_node_id: LogicalNodeId,
    pub swactor_id: SwactorId,
}

#[derive(Clone, Debug, Default)]
pub struct SwactorJoinRouter {
    expected_nodes: BTreeSet<LogicalNodeId>,
}

impl SwactorJoinRouter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, logical_node_id: LogicalNodeId) {
        self.expected_nodes.insert(logical_node_id);
    }

    pub fn route(
        &self,
        event: SwactorJoinEvent,
        manager: &mut NodeManager,
    ) -> Result<Vec<NodeManagerCommand>, DockerClusterError> {
        if !self.expected_nodes.contains(&event.logical_node_id) {
            return Err(DockerClusterError::UnknownNode(event.logical_node_id));
        }
        manager
            .handle(NodeManagerMsg::SwactorJoined {
                logical_node_id: event.logical_node_id,
                swactor_id: event.swactor_id,
            })
            .map_err(|error| DockerClusterError::Node(error.reason))
    }
}

pub trait SshBootstrapClientFactory {
    type Client: BootstrapSshClient;

    fn client_for(&mut self, spec: &BootstrapSessionSpec) -> Self::Client;
}

#[derive(Clone, Debug)]
pub struct ManagedDockerNode<C> {
    pub manager: NodeManager,
    pub bootstrap: Option<SshBootstrapSession<C>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DockerClusterError {
    Provider(String),
    Node(String),
    UnknownNode(LogicalNodeId),
    EndpointUnavailable(ProviderLeaseId),
    MissingBootstrap(LogicalNodeId),
}

pub struct DockerClusterHarness<D, F, S>
where
    D: DockerCli,
    F: SshBootstrapClientFactory,
    S: BootstrapDatastreamSink,
{
    provider: DockerProvider<D>,
    client_factory: F,
    datastream: S,
    router: SwactorJoinRouter,
    nodes: BTreeMap<LogicalNodeId, ManagedDockerNode<F::Client>>,
    teardown_complete: bool,
}

impl<D, F, S> DockerClusterHarness<D, F, S>
where
    D: DockerCli,
    F: SshBootstrapClientFactory,
    S: BootstrapDatastreamSink,
{
    pub fn new(provider: DockerProvider<D>, client_factory: F, datastream: S) -> Self {
        Self {
            provider,
            client_factory,
            datastream,
            router: SwactorJoinRouter::new(),
            nodes: BTreeMap::new(),
            teardown_complete: false,
        }
    }

    pub fn provider(&self) -> &DockerProvider<D> {
        &self.provider
    }

    pub fn datastream(&self) -> &S {
        &self.datastream
    }

    pub fn nodes(&self) -> &BTreeMap<LogicalNodeId, ManagedDockerNode<F::Client>> {
        &self.nodes
    }

    pub fn records(&self) -> Vec<NodeRecord> {
        self.nodes
            .values()
            .filter_map(|node| node.manager.record().cloned())
            .collect()
    }

    pub fn all_ready(&self) -> bool {
        !self.nodes.is_empty() && self.nodes.values().all(|node| node.manager.is_ready())
    }

    pub fn start_group(&mut self, group: &RunNodeGroupSpec) -> Result<(), DockerClusterError> {
        for spec in expand_node_group(group) {
            let logical_node_id = spec.logical_node_id.clone();
            self.router.register(logical_node_id.clone());
            let mut manager = NodeManager::new();
            let commands = manager
                .handle(NodeManagerMsg::Start(spec))
                .map_err(|error| DockerClusterError::Node(error.reason))?;
            match self.process_start_commands(&mut manager, commands) {
                Ok(bootstrap) => {
                    self.nodes
                        .insert(logical_node_id, ManagedDockerNode { manager, bootstrap });
                }
                Err(error) => {
                    self.nodes.insert(
                        logical_node_id,
                        ManagedDockerNode {
                            manager,
                            bootstrap: None,
                        },
                    );
                    let _ = self.teardown();
                    return Err(error);
                }
            }
        }
        self.teardown_complete = false;
        Ok(())
    }

    pub fn route_join(&mut self, event: SwactorJoinEvent) -> Result<(), DockerClusterError> {
        let logical_node_id = event.logical_node_id.clone();
        let node = self
            .nodes
            .get_mut(&logical_node_id)
            .ok_or_else(|| DockerClusterError::UnknownNode(logical_node_id.clone()))?;
        let commands = self.router.route(event, &mut node.manager)?;
        for command in commands {
            match command {
                NodeManagerCommand::BootstrapConvergenceObserved { swactor_id, .. } => {
                    let bootstrap = node.bootstrap.as_mut().ok_or_else(|| {
                        DockerClusterError::MissingBootstrap(logical_node_id.clone())
                    })?;
                    let events = bootstrap.convergence_observed(swactor_id, &mut self.datastream);
                    Self::feed_bootstrap_events(&mut node.manager, events)?;
                }
                other => {
                    return Err(DockerClusterError::Node(format!(
                        "unexpected command {other:?}"
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn teardown(&mut self) -> Result<(), DockerClusterError> {
        for node in self.nodes.values_mut() {
            let commands = node
                .manager
                .handle(NodeManagerMsg::Destroy)
                .map_err(|error| DockerClusterError::Node(error.reason))?;
            for command in commands {
                match command {
                    NodeManagerCommand::CancelBootstrap { .. } => {
                        if let Some(bootstrap) = node.bootstrap.as_mut() {
                            let _ = bootstrap.cancel();
                        }
                    }
                    NodeManagerCommand::DestroyLease(handle) => {
                        self.provider
                            .destroy_lease(&handle)
                            .map_err(|error| DockerClusterError::Provider(error.reason))?;
                        node.manager
                            .handle(NodeManagerMsg::LeaseDestroyed)
                            .map_err(|error| DockerClusterError::Node(error.reason))?;
                    }
                    other => {
                        return Err(DockerClusterError::Node(format!(
                            "unexpected command {other:?}"
                        )));
                    }
                }
            }
        }
        self.teardown_complete = true;
        Ok(())
    }

    fn process_start_commands(
        &mut self,
        manager: &mut NodeManager,
        commands: Vec<NodeManagerCommand>,
    ) -> Result<Option<SshBootstrapSession<F::Client>>, DockerClusterError> {
        let mut pending = commands;
        let mut bootstrap = None;
        while let Some(command) = pending.pop() {
            match command {
                NodeManagerCommand::CreateLease(request) => {
                    let result = self
                        .provider
                        .create_lease(request)
                        .map_err(|error| DockerClusterError::Provider(error.reason))?;
                    let more = manager
                        .handle(NodeManagerMsg::LeaseCreated(result))
                        .map_err(|error| DockerClusterError::Node(error.reason))?;
                    pending.extend(more);
                }
                NodeManagerCommand::LookupEndpoint(lease) => {
                    let lease_id = lease.lease_id.clone();
                    let endpoint = match self
                        .provider
                        .lookup_endpoint(&lease)
                        .map_err(|error| DockerClusterError::Provider(error.reason))?
                    {
                        Some(endpoint) => endpoint,
                        None => {
                            let _ = manager.handle(NodeManagerMsg::EndpointFailed(
                                "docker ssh endpoint unavailable".into(),
                            ));
                            return Err(DockerClusterError::EndpointUnavailable(lease_id));
                        }
                    };
                    let more = manager
                        .handle(NodeManagerMsg::EndpointKnown(endpoint))
                        .map_err(|error| DockerClusterError::Node(error.reason))?;
                    pending.extend(more);
                }
                NodeManagerCommand::StartBootstrap(spec) => {
                    let client = self.client_factory.client_for(&spec);
                    let mut session = SshBootstrapSession::new(spec, client);
                    let events = session.start(&mut self.datastream);
                    Self::feed_bootstrap_events(manager, events)?;
                    bootstrap = Some(session);
                }
                other => {
                    return Err(DockerClusterError::Node(format!(
                        "unexpected command {other:?}"
                    )));
                }
            }
        }
        Ok(bootstrap)
    }

    fn feed_bootstrap_events(
        manager: &mut NodeManager,
        events: Vec<BootstrapSessionEvent>,
    ) -> Result<(), DockerClusterError> {
        for event in events {
            match event {
                BootstrapSessionEvent::Observed(observation) => {
                    manager
                        .handle(NodeManagerMsg::BootstrapObserved(observation))
                        .map_err(|error| DockerClusterError::Node(error.reason))?;
                }
                BootstrapSessionEvent::Failed(reason) => {
                    manager
                        .handle(NodeManagerMsg::BootstrapFailed(reason))
                        .map_err(|error| DockerClusterError::Node(error.reason))?;
                }
                BootstrapSessionEvent::Closed => {
                    manager
                        .handle(NodeManagerMsg::BootstrapClosed)
                        .map_err(|error| DockerClusterError::Node(error.reason))?;
                }
            }
        }
        Ok(())
    }
}

impl<D, F, S> Drop for DockerClusterHarness<D, F, S>
where
    D: DockerCli,
    F: SshBootstrapClientFactory,
    S: BootstrapDatastreamSink,
{
    fn drop(&mut self) {
        if !self.teardown_complete {
            let _ = self.teardown();
        }
    }
}
