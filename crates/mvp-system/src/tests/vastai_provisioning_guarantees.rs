use std::collections::VecDeque;
use std::sync::Arc;

use mvp_system::node_provisioning as provision;
use mvp_system::node_provisioning::ProviderPlugin;
use mvp_system::provisioning::{
    NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink, ProvisionPlugin,
};
use mvp_system::vastai_provisioning::{
    VastAiBootstrapLauncher, VastAiLeaseClient, VastAiProviderPlugin, VastAiProvisioningConfig,
    VastAiProvisioningPlugin, VastAiSshEndpoint,
};
use parking_lot::Mutex;
use swactor_vastai::{LifecyclePolicy, ProvisionRequest, ProvisionedInstance, SelectionPolicy};

#[derive(Default)]
struct RecordingSink {
    observations: Mutex<Vec<PluginObservation>>,
}

impl PluginObservationSink for RecordingSink {
    fn observe(&self, observation: PluginObservation) {
        self.observations.lock().push(observation);
    }
}

fn sink() -> PluginSink {
    PluginSink::new(Arc::new(RecordingSink::default()))
}

#[derive(Default)]
struct FakeLeaseClient {
    requests: Vec<ProvisionRequest>,
    endpoint_lookups: Vec<(u64, String, String)>,
    endpoint_results: VecDeque<Result<VastAiSshEndpoint, String>>,
    destroyed: Vec<u64>,
    destroy_result: Option<Result<(), String>>,
    next_contract_id: u64,
}

impl FakeLeaseClient {
    fn with_contract(mut self, contract_id: u64) -> Self {
        self.next_contract_id = contract_id;
        self
    }
}

impl VastAiLeaseClient for FakeLeaseClient {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String> {
        self.requests.push(request);
        let contract_id = self.next_contract_id;
        self.next_contract_id = self.next_contract_id.wrapping_add(1).max(1);
        Ok(ProvisionedInstance {
            index: 0,
            contract_id,
            offer_id: 55,
            host_id: Some(77),
            gpu_name: "RTX 4090".to_owned(),
            gpu_ram: Some(24_000.0),
            dph_total: 0.42,
        })
    }

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        label: &str,
        _lifecycle: &LifecyclePolicy,
        ssh_user: &str,
    ) -> Result<VastAiSshEndpoint, String> {
        self.endpoint_lookups
            .push((contract_id, label.to_owned(), ssh_user.to_owned()));
        self.endpoint_results.pop_front().unwrap_or_else(|| {
            Ok(VastAiSshEndpoint {
                host: "ssh5.vast.ai".to_owned(),
                port: 22017,
                user: ssh_user.to_owned(),
            })
        })
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
        self.destroyed.push(contract_id);
        self.destroy_result.clone().unwrap_or(Ok(()))
    }
}

#[derive(Default)]
struct FakeBootstrap {
    starts: Vec<(NodeProvisionSpec, VastAiSshEndpoint)>,
    stops: Vec<usize>,
    fail: Option<String>,
    next_handle: usize,
}

impl VastAiBootstrapLauncher for FakeBootstrap {
    type Handle = usize;

    fn start_bootstrap(
        &mut self,
        spec: NodeProvisionSpec,
        endpoint: VastAiSshEndpoint,
        _sink: PluginSink,
        _producer: Option<datastream::DatastreamProducer>,
    ) -> Result<Self::Handle, String> {
        if let Some(reason) = self.fail.clone() {
            return Err(reason);
        }
        self.starts.push((spec, endpoint));
        self.next_handle += 1;
        Ok(self.next_handle)
    }

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle) {
        self.stops.push(*handle);
    }
}

fn spec() -> NodeProvisionSpec {
    NodeProvisionSpec {
        run_id: 9,
        node_id: 11,
        stage_index: Some(2),
        image: "registry.example/mvp-worker:latest".to_owned(),
        env: vec![("EXISTING".to_owned(), "1".to_owned())],
        args: vec!["python".to_owned(), "worker.py".to_owned()],
        mounts: Vec::new(),
    }
}

fn config() -> VastAiProvisioningConfig {
    VastAiProvisioningConfig {
        label_prefix: "test-mvp".to_owned(),
        disk_gb: 80,
        ssh_user: "ubuntu".to_owned(),
        selection: SelectionPolicy::default(),
        lifecycle: LifecyclePolicy::default(),
        confirm_lease: false,
        onstart: None,
        ssh_public_key: Some("ssh-ed25519 AAAATESTKEY test".to_owned()),
    }
}

fn logical_spec() -> provision::LogicalNodeSpec {
    let logical_node_id = provision::LogicalNodeId("workers-0".into());
    provision::LogicalNodeSpec {
        run_id: provision::RunId(9),
        logical_node_id: logical_node_id.clone(),
        group_id: provision::NodeGroupId("workers".into()),
        role: provision::RoleId("worker".into()),
        provider: provision::ProviderKind::VastAi,
        shape: provision::DesiredNodeShape {
            image: "registry.example/mvp-worker:latest".to_owned(),
            disk_gb: 80,
            gpu_name: Some("RTX 4090".to_owned()),
            min_gpu_ram_mb: Some(20_000),
            min_down_mbps: Some(150.0),
            min_up_mbps: Some(25.0),
            min_reliability: Some(0.98),
            require_verified: true,
            provider_labels: [("system".to_owned(), "mvp".to_owned())]
                .into_iter()
                .collect(),
        },
        boot: provision::BootSpec {
            ssh_user: "ubuntu".to_owned(),
            verify_commands: vec!["test -x /opt/mvp/swactor".to_owned()],
            start_swactor_command: "/opt/mvp/swactor-node --join".to_owned(),
            stdout_sources: vec!["/var/log/mvp/stdout.log".to_owned()],
            stderr_sources: vec!["/var/log/mvp/stderr.log".to_owned()],
        },
        swarm_join: provision::SwarmJoinSpec {
            orch_swactor_addr: "quic://orch.example:9443".to_owned(),
            join_token_ref: "secret://join".to_owned(),
            expected_logical_node_id: logical_node_id,
        },
    }
}

#[test]
fn vastai_plugin_builds_one_node_request_and_starts_bootstrap() {
    let mut plugin = VastAiProvisioningPlugin::new(
        FakeLeaseClient::default().with_contract(100),
        FakeBootstrap::default(),
        config(),
    );

    let handle = plugin.start_node(spec(), sink()).unwrap();

    assert_eq!(handle.id, 1);
    assert_eq!(handle.provider_process_id, None);
    assert_eq!(plugin.active_contract_count(), 1);

    let request = &plugin.client().requests[0];
    assert_eq!(request.count, 1);
    assert_eq!(request.image, "registry.example/mvp-worker:latest");
    assert_eq!(request.label.as_deref(), Some("test-mvp-9-11"));
    assert_eq!(request.disk_gb, 80);
    assert_eq!(request.onstart, None);
    assert_eq!(request.env.get("EXISTING").map(String::as_str), Some("1"));
    assert_eq!(
        request.env.get("SSH_PUBLIC_KEY").map(String::as_str),
        Some("ssh-ed25519 AAAATESTKEY test")
    );

    assert_eq!(
        plugin.client().endpoint_lookups,
        vec![(100, "test-mvp-9-11".to_owned(), "ubuntu".to_owned())]
    );
    assert_eq!(plugin.bootstrap().starts.len(), 1);
    assert_eq!(plugin.bootstrap().starts[0].0.env, spec().env);
    assert_eq!(plugin.bootstrap().starts[0].0.args, spec().args);
    assert_eq!(
        plugin.bootstrap().starts[0].1,
        VastAiSshEndpoint {
            host: "ssh5.vast.ai".to_owned(),
            port: 22017,
            user: "ubuntu".to_owned(),
        }
    );
}

#[test]
fn vastai_plugin_omits_ssh_public_key_when_unconfigured() {
    let config = VastAiProvisioningConfig {
        ssh_public_key: None,
        ..config()
    };
    let mut plugin = VastAiProvisioningPlugin::new(
        FakeLeaseClient::default().with_contract(100),
        FakeBootstrap::default(),
        config,
    );

    plugin.start_node(spec(), sink()).unwrap();

    let request = &plugin.client().requests[0];
    assert_eq!(request.env.get("SSH_PUBLIC_KEY"), None);
}

#[test]
fn stop_destroys_known_vastai_contract_exactly_once() {
    let mut plugin = VastAiProvisioningPlugin::new(
        FakeLeaseClient::default().with_contract(100),
        FakeBootstrap::default(),
        config(),
    );
    let handle = plugin.start_node(spec(), sink()).unwrap();

    plugin.stop_node(&handle).unwrap();
    plugin.stop_node(&handle).unwrap();

    assert_eq!(plugin.client().destroyed, vec![100]);
    assert_eq!(plugin.bootstrap().stops, vec![1]);
    assert_eq!(plugin.active_contract_count(), 0);
}

#[test]
fn start_failure_after_contract_creation_destroys_contract_once() {
    let mut client = FakeLeaseClient::default().with_contract(100);
    client
        .endpoint_results
        .push_back(Err("ssh missing".to_owned()));
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());

    let error = plugin.start_node(spec(), sink()).unwrap_err();

    assert!(error.contains("vastai SSH endpoint node 11: ssh missing"));
    assert_eq!(plugin.client().destroyed, vec![100]);
    assert_eq!(plugin.active_contract_count(), 0);
}

#[test]
fn start_failure_reports_original_error_and_cleanup_failure() {
    let mut client = FakeLeaseClient::default().with_contract(100);
    client
        .endpoint_results
        .push_back(Err("ssh missing".to_owned()));
    client.destroy_result = Some(Err("destroy refused".to_owned()));
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());

    let error = plugin.start_node(spec(), sink()).unwrap_err();

    assert!(error.contains("vastai SSH endpoint node 11: ssh missing"));
    assert!(error.contains("cleanup destroy 100 failed: destroy refused"));
    assert_eq!(plugin.client().destroyed, vec![100]);
    assert_eq!(plugin.active_contract_count(), 0);
}

#[test]
fn vastai_provider_plugin_maps_contracts_into_node_manager_lease_model() {
    let mut provider =
        VastAiProviderPlugin::new(FakeLeaseClient::default().with_contract(100), config());
    let spec = logical_spec();

    let result = provider
        .create_lease(provision::CreateLeaseRequest { spec: spec.clone() })
        .expect("vastai lease succeeds");

    assert_eq!(result.endpoint, None);
    assert_eq!(result.lease.provider, provision::ProviderKind::VastAi);
    assert_eq!(
        result.lease.lease_id,
        provision::ProviderLeaseId("vastai:100".into())
    );
    assert_eq!(result.lease.provider_contract_id, "100");
    assert_eq!(
        result.lease.destroy_handle,
        provision::DestroyHandle {
            provider: provision::ProviderKind::VastAi,
            lease_id: provision::ProviderLeaseId("vastai:100".into()),
            provider_contract_id: "100".into(),
        }
    );
    assert_eq!(
        result
            .lease
            .provider_metadata
            .get("label")
            .map(String::as_str),
        Some("test-mvp-9-workers-0")
    );
    assert_eq!(
        result
            .lease
            .provider_metadata
            .get("ssh_user")
            .map(String::as_str),
        Some("ubuntu")
    );

    let request = &provider.client().requests[0];
    assert_eq!(request.count, 1);
    assert_eq!(request.image, spec.shape.image);
    assert_eq!(request.disk_gb, 80);
    assert_eq!(request.label.as_deref(), Some("test-mvp-9-workers-0"));
    assert_eq!(request.env.get("MVP_RUN_ID").map(String::as_str), Some("9"));
    assert_eq!(
        request.env.get("MVP_LOGICAL_NODE_ID").map(String::as_str),
        Some("workers-0")
    );
    assert_eq!(request.selection.gpu_name.as_deref(), Some("RTX 4090"));
    assert_eq!(request.selection.min_gpu_ram_mb, Some(20_000));
    assert_eq!(request.selection.min_down_mbps, 150.0);
    assert_eq!(request.selection.min_up_mbps, Some(25.0));
    assert_eq!(request.selection.min_reliability, 0.98);
    assert!(request.selection.require_verified);

    let endpoint = provider
        .lookup_endpoint(&result.lease)
        .expect("lookup succeeds")
        .expect("vastai lookup yields endpoint");
    assert_eq!(
        endpoint,
        provision::SshEndpoint {
            host: "ssh5.vast.ai".into(),
            port: 22017,
            user: "ubuntu".into(),
            auth_ref: "vastai:100:ssh".into(),
        }
    );
    assert_eq!(
        provider.client().endpoint_lookups,
        vec![(100, "test-mvp-9-workers-0".to_owned(), "ubuntu".to_owned())]
    );

    provider
        .destroy_lease(&result.lease.destroy_handle)
        .expect("destroy succeeds");
    assert_eq!(provider.client().destroyed, vec![100]);
}
