use std::collections::BTreeMap;

use mvp_system::docker_cluster_provisioning as docker;
use mvp_system::node_provisioning as provision;
use provision::ProviderPlugin;

fn docker_group_spec(count: u32) -> provision::RunNodeGroupSpec {
    provision::RunNodeGroupSpec {
        run_id: provision::RunId(7),
        group_id: provision::NodeGroupId("workers".into()),
        role: provision::RoleId("worker".into()),
        count,
        provider: provision::ProviderKind::Docker,
        shape: provision::DesiredNodeShape {
            image: "mvp-worker-test:latest".into(),
            disk_gb: 32,
            gpu_name: None,
            min_gpu_ram_mb: None,
            min_down_mbps: None,
            min_up_mbps: None,
            min_reliability: None,
            require_verified: false,
            provider_labels: [("suite".into(), "docker-provisioning".into())]
                .into_iter()
                .collect(),
        },
        boot: provision::BootSpec {
            ssh_user: "root".into(),
            verify_commands: vec!["test -x /opt/mvp/swactor".into()],
            start_swactor_command: "/opt/mvp/swactor-node --join ${MVP_ORCH_SWACTOR_ADDR}".into(),
            stdout_sources: vec!["/var/log/mvp/bootstrap.out".into()],
            stderr_sources: vec!["/var/log/mvp/bootstrap.err".into()],
        },
        swarm_join: provision::SwarmJoinTemplate {
            orch_swactor_addr: "quic://orch.local:9443".into(),
            join_token_ref: "secret://run-7-token".into(),
        },
    }
}

fn one_spec() -> provision::LogicalNodeSpec {
    provision::expand_node_group(&docker_group_spec(1))
        .into_iter()
        .next()
        .expect("one logical node")
}

fn endpoint(port: u16) -> provision::SshEndpoint {
    provision::SshEndpoint {
        host: "127.0.0.1".into(),
        port,
        user: "root".into(),
        auth_ref: format!("test-key-{port}"),
    }
}

#[derive(Clone, Debug, Default)]
struct FakeDockerCli {
    next_id: usize,
    run_requests: Vec<docker::DockerRunRequest>,
    inspect_requests: Vec<String>,
    removed: Vec<String>,
    endpoints: BTreeMap<String, Option<provision::SshEndpoint>>,
}

impl FakeDockerCli {
    fn new() -> Self {
        Self {
            next_id: 1,
            ..Self::default()
        }
    }
}

impl docker::DockerCli for FakeDockerCli {
    fn run_container(
        &mut self,
        request: docker::DockerRunRequest,
    ) -> Result<docker::DockerRunResult, docker::DockerCliError> {
        self.run_requests.push(request);
        let container_id = format!("container-{}", self.next_id);
        let ssh_endpoint = endpoint(22000 + self.next_id as u16);
        self.next_id += 1;
        self.endpoints
            .entry(container_id.clone())
            .or_insert_with(|| Some(ssh_endpoint));
        Ok(docker::DockerRunResult { container_id })
    }

    fn inspect_ssh_endpoint(
        &mut self,
        container_id: &str,
    ) -> Result<Option<provision::SshEndpoint>, docker::DockerCliError> {
        self.inspect_requests.push(container_id.into());
        Ok(self.endpoints.get(container_id).cloned().flatten())
    }

    fn remove_force(&mut self, container_id: &str) -> Result<(), docker::DockerCliError> {
        self.removed.push(container_id.into());
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct FakeSshClient {
    logs: Vec<(provision::BootstrapLogStream, String)>,
    connected_to: Option<provision::SshEndpoint>,
    probed: bool,
    stdout_sources: Vec<String>,
    stderr_sources: Vec<String>,
    verify_commands: Vec<String>,
    started_command: Option<String>,
    started_join: Option<provision::SwarmJoinSpec>,
    closed: bool,
}

impl FakeSshClient {
    fn new(logical_node_id: &provision::LogicalNodeId) -> Self {
        Self {
            logs: vec![
                (
                    provision::BootstrapLogStream::Stdout,
                    format!("{} boot entered", logical_node_id.0),
                ),
                (
                    provision::BootstrapLogStream::Stderr,
                    format!("{} stderr ready", logical_node_id.0),
                ),
            ],
            connected_to: None,
            probed: false,
            stdout_sources: Vec::new(),
            stderr_sources: Vec::new(),
            verify_commands: Vec::new(),
            started_command: None,
            started_join: None,
            closed: false,
        }
    }
}

impl docker::BootstrapSshClient for FakeSshClient {
    fn connect(
        &mut self,
        endpoint: &provision::SshEndpoint,
    ) -> Result<(), docker::BootstrapSshError> {
        self.connected_to = Some(endpoint.clone());
        Ok(())
    }

    fn probe_stdout(&mut self) -> Result<(), docker::BootstrapSshError> {
        self.probed = true;
        Ok(())
    }

    fn read_bootstrap_logs(
        &mut self,
        stdout_sources: &[String],
        stderr_sources: &[String],
    ) -> Result<Vec<(provision::BootstrapLogStream, String)>, docker::BootstrapSshError> {
        self.stdout_sources = stdout_sources.to_vec();
        self.stderr_sources = stderr_sources.to_vec();
        Ok(self.logs.clone())
    }

    fn run_verify_commands(
        &mut self,
        commands: &[String],
    ) -> Result<(), docker::BootstrapSshError> {
        self.verify_commands = commands.to_vec();
        Ok(())
    }

    fn start_swactor(
        &mut self,
        command: &str,
        join: &provision::SwarmJoinSpec,
    ) -> Result<(), docker::BootstrapSshError> {
        self.started_command = Some(command.into());
        self.started_join = Some(join.clone());
        Ok(())
    }

    fn close(&mut self) {
        self.closed = true;
    }
}

#[derive(Clone, Debug, Default)]
struct FakeSshFactory {
    created_for: Vec<provision::LogicalNodeId>,
}

impl docker::SshBootstrapClientFactory for FakeSshFactory {
    type Client = FakeSshClient;

    fn client_for(&mut self, spec: &provision::BootstrapSessionSpec) -> Self::Client {
        self.created_for.push(spec.logical_node_id.clone());
        FakeSshClient::new(&spec.logical_node_id)
    }
}

#[test]
fn docker_provider_maps_node_spec_to_container_lease_and_destroy_handle() {
    let spec = one_spec();
    let mut provider = docker::DockerProvider::new(FakeDockerCli::new());

    let result = provider
        .create_lease(provision::CreateLeaseRequest { spec: spec.clone() })
        .expect("docker provider creates lease");

    let cli = provider.cli();
    assert_eq!(cli.run_requests.len(), 1);
    let run = &cli.run_requests[0];
    assert_eq!(run.container_name, "mvp-7-workers-0");
    assert_eq!(run.image, "mvp-worker-test:latest");
    assert_eq!(run.ssh_user, "root");
    assert_eq!(run.labels["mvp.provider"], "docker");
    assert_eq!(run.labels["mvp.logical_node_id"], "workers-0");
    assert_eq!(run.env["MVP_LOGICAL_NODE_ID"], "workers-0");
    assert_eq!(run.env["MVP_ORCH_SWACTOR_ADDR"], "quic://orch.local:9443");
    assert_eq!(cli.inspect_requests, vec!["container-1"]);

    assert_eq!(result.lease.provider, provision::ProviderKind::Docker);
    assert_eq!(
        result.lease.lease_id,
        provision::ProviderLeaseId("docker:container-1".into())
    );
    assert_eq!(result.lease.provider_contract_id, "container-1");
    assert_eq!(result.endpoint, Some(endpoint(22001)));

    provider
        .destroy_lease(&result.lease.destroy_handle)
        .expect("destroy succeeds");
    assert_eq!(provider.cli().removed, vec!["container-1"]);
}

#[test]
fn ssh_bootstrap_session_runs_pre_handoff_steps_and_closes_on_convergence() {
    let spec = one_spec();
    let bootstrap_spec = provision::BootstrapSessionSpec {
        run_id: spec.run_id.clone(),
        logical_node_id: spec.logical_node_id.clone(),
        lease_id: provision::ProviderLeaseId("docker:container-1".into()),
        ssh: endpoint(22001),
        boot: spec.boot.clone(),
        swarm_join: spec.swarm_join.clone(),
        datastream: provision::DatastreamStreamId("run/7/workers-0/bootstrap".into()),
    };
    let mut session = docker::SshBootstrapSession::new(
        bootstrap_spec,
        FakeSshClient::new(&provision::LogicalNodeId("workers-0".into())),
    );
    let mut datastream = provision::InMemoryBootstrapDatastream::default();

    let events = session.start(&mut datastream);

    assert_eq!(
        session.stage(),
        provision::BootstrapStage::WaitingForSwactorJoin
    );
    assert!(events.iter().any(|event| matches!(
        event,
        provision::BootstrapSessionEvent::Observed(obs)
            if obs.stage == provision::BootstrapStage::SshReady
    )));
    assert_eq!(datastream.records().len(), 2);
    assert_eq!(datastream.records()[0].seq, 1);
    assert_eq!(
        datastream.records()[0].stream,
        provision::BootstrapLogStream::Stdout
    );
    assert_eq!(datastream.records()[1].seq, 2);
    assert_eq!(
        datastream.records()[1].stream,
        provision::BootstrapLogStream::Stderr
    );
    assert_eq!(session.client().connected_to, Some(endpoint(22001)));
    assert!(session.client().probed);
    assert_eq!(
        session.client().verify_commands,
        vec!["test -x /opt/mvp/swactor"]
    );
    assert_eq!(
        session.client().started_command.as_deref(),
        Some("/opt/mvp/swactor-node --join ${MVP_ORCH_SWACTOR_ADDR}")
    );

    let events = session.convergence_observed(
        provision::SwactorId("swactor-workers-0".into()),
        &mut datastream,
    );
    assert!(session.is_closed());
    assert!(session.client().closed);
    assert_eq!(datastream.flush_count(), 1);
    assert!(matches!(
        events.as_slice(),
        [_, provision::BootstrapSessionEvent::Closed]
    ));
}

#[test]
fn docker_node_provisioner_rejects_join_for_wrong_logical_node() {
    let spec = one_spec();
    let mut node = docker::DockerNodeProvisioner::new(
        docker::DockerProvider::new(FakeDockerCli::new()),
        FakeSshFactory::default(),
        provision::InMemoryBootstrapDatastream::default(),
    );

    node.start(spec).expect("node starts");
    let mut manager = node.manager().clone();
    let wrong_join = manager.handle(provision::NodeManagerMsg::SwactorJoined {
        logical_node_id: provision::LogicalNodeId("workers-99".into()),
        swactor_id: provision::SwactorId("swactor-wrong".into()),
    });

    assert!(wrong_join.is_err());
    assert_eq!(
        node.record().expect("record exists").stage,
        provision::NodeStage::BootstrapRunning
    );
}

#[test]
fn docker_node_provisioner_wires_provider_bootstrap_join_and_teardown() {
    let provider = docker::DockerProvider::new(FakeDockerCli::new());
    let datastream = provision::InMemoryBootstrapDatastream::default();
    let mut node =
        docker::DockerNodeProvisioner::new(provider, FakeSshFactory::default(), datastream);

    node.start(one_spec()).expect("node starts");

    assert!(!node.is_ready());
    assert_eq!(node.provider().cli().run_requests.len(), 1);
    assert_eq!(node.datastream().records().len(), 2);
    assert_eq!(
        node.record().expect("record exists").stage,
        provision::NodeStage::BootstrapRunning
    );

    node.observe_swactor_join(provision::SwactorId("swactor-workers-0".into()))
        .expect("join completes handoff");

    assert!(node.is_ready());
    assert_eq!(node.datastream().flush_count(), 1);
    let record = node.record().expect("record exists");
    assert_eq!(record.stage, provision::NodeStage::Dormant);
    assert!(record.ready);
    assert_eq!(
        record.lease.as_ref().unwrap().provider,
        provision::ProviderKind::Docker
    );
    assert!(node.bootstrap().expect("bootstrap exists").is_closed());
    assert!(node.bootstrap().unwrap().client().closed);

    node.stop().expect("teardown succeeds");
    assert_eq!(node.provider().cli().removed, vec!["container-1"]);
    let record = node.record().expect("record exists");
    assert_eq!(record.stage, provision::NodeStage::Destroyed);
    assert!(!record.ready);
}

#[test]
fn docker_node_provisioner_teardown_cleans_known_lease_before_join() {
    let provider = docker::DockerProvider::new(FakeDockerCli::new());
    let datastream = provision::InMemoryBootstrapDatastream::default();
    let mut node =
        docker::DockerNodeProvisioner::new(provider, FakeSshFactory::default(), datastream);

    node.start(one_spec()).expect("node starts");
    assert_eq!(
        node.record().unwrap().stage,
        provision::NodeStage::BootstrapRunning
    );

    node.stop().expect("teardown succeeds before join");

    assert_eq!(
        node.record().unwrap().stage,
        provision::NodeStage::Destroyed
    );
    assert!(node.bootstrap().unwrap().is_closed());
    assert!(node.bootstrap().unwrap().client().closed);
    assert_eq!(node.provider().cli().removed, vec!["container-1"]);
}

#[test]
fn docker_node_provisioner_cleans_known_lease_when_endpoint_never_appears() {
    let mut cli = FakeDockerCli::new();
    cli.endpoints.insert("container-1".into(), None);
    let provider = docker::DockerProvider::new(cli);
    let datastream = provision::InMemoryBootstrapDatastream::default();
    let mut node =
        docker::DockerNodeProvisioner::new(provider, FakeSshFactory::default(), datastream);

    let result = node.start(one_spec());

    assert!(matches!(
        result,
        Err(docker::DockerNodeProvisionError::EndpointUnavailable(id))
            if id == provision::ProviderLeaseId("docker:container-1".into())
    ));
    assert_eq!(node.provider().cli().removed, vec!["container-1"]);
    assert_eq!(
        node.record().unwrap().stage,
        provision::NodeStage::Destroyed
    );
}
