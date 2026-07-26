use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use mvp_system::node_provisioning as provision;
use mvp_system::node_provisioning::ProviderPlugin;
use mvp_system::provisioning::{
    NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink, ProvisionPlugin,
};
use mvp_system::vastai_provisioning::{
    BootstrapStopReason, VastAiBootstrapLauncher, VastAiLeaseClient, VastAiProviderPlugin,
    VastAiProvisioningConfig, VastAiProvisioningPlugin, VastAiSshEndpoint,
};
use parking_lot::{Condvar, Mutex};
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

#[derive(Clone, Default)]
struct FakeLeaseClient {
    requests: Vec<ProvisionRequest>,
    endpoint_lookups: Vec<(u64, String, String)>,
    endpoint_results: VecDeque<Result<VastAiSshEndpoint, String>>,
    destroyed: Vec<u64>,
    destroy_result: Option<Result<(), String>>,
    next_contract_id: u64,
    host_ids: VecDeque<Option<u64>>,
    first_wave_plan: Vec<Option<u64>>,
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
        let host_id = self.host_ids.pop_front().unwrap_or(Some(77));
        Ok(ProvisionedInstance {
            index: 0,
            contract_id,
            offer_id: 55,
            host_id,
            gpu_name: "RTX 4090".to_owned(),
            gpu_ram: Some(24_000.0),
            dph_total: 0.42,
        })
    }

    fn plan_first_wave_offers(
        &mut self,
        requests: &[ProvisionRequest],
    ) -> Result<Vec<Option<u64>>, String> {
        let mut plan = self.first_wave_plan.clone();
        plan.resize(requests.len(), None);
        Ok(plan)
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
    stops: Vec<(usize, BootstrapStopReason)>,
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

    fn stop_bootstrap(&mut self, handle: &mut Self::Handle, reason: BootstrapStopReason) {
        self.stops.push((*handle, reason));
    }
}

#[derive(Clone)]
struct ParallelLeaseClient {
    state: Arc<Mutex<ParallelLeaseState>>,
    gate: Arc<ParallelLeaseGate>,
}

struct ParallelLeaseGate {
    target: usize,
    started: Mutex<usize>,
    all_started: Condvar,
}

#[derive(Default)]
struct ParallelLeaseState {
    requests: Vec<ProvisionRequest>,
    endpoint_lookups: Vec<u64>,
    destroyed: Vec<u64>,
    first_endpoint_request_count: Option<usize>,
    next_contract_id: u64,
    host_ids: VecDeque<Option<u64>>,
    first_wave_plan_requests: usize,
    first_wave_plan: Vec<Option<u64>>,
}

impl ParallelLeaseClient {
    fn new(target: usize) -> Self {
        Self {
            state: Arc::new(Mutex::new(ParallelLeaseState {
                next_contract_id: 100,
                host_ids: (0..target)
                    .map(|index| Some(10_000 + u64::try_from(index).unwrap()))
                    .collect(),
                first_wave_plan: (0..target)
                    .map(|index| Some(9_000 + u64::try_from(index).unwrap()))
                    .collect(),
                ..ParallelLeaseState::default()
            })),
            gate: Arc::new(ParallelLeaseGate {
                target,
                started: Mutex::new(0),
                all_started: Condvar::new(),
            }),
        }
    }
}

impl VastAiLeaseClient for ParallelLeaseClient {
    fn plan_first_wave_offers(
        &mut self,
        requests: &[ProvisionRequest],
    ) -> Result<Vec<Option<u64>>, String> {
        let mut state = self.state.lock();
        state.first_wave_plan_requests = requests.len();
        let mut plan = state.first_wave_plan.clone();
        plan.resize(requests.len(), None);
        Ok(plan)
    }

    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String> {
        {
            self.state.lock().requests.push(request);
        }
        let mut started = self.gate.started.lock();
        *started += 1;
        if *started < self.gate.target {
            let wait = self
                .gate
                .all_started
                .wait_for(&mut started, Duration::from_secs(2));
            assert!(
                !wait.timed_out(),
                "all concurrent Vast.ai lease requests should start before any waits for SSH"
            );
        } else {
            self.gate.all_started.notify_all();
        }
        drop(started);

        let mut state = self.state.lock();
        let contract_id = state.next_contract_id;
        state.next_contract_id = state.next_contract_id.wrapping_add(1).max(1);
        let host_id = state.host_ids.pop_front().unwrap_or(Some(77));
        Ok(ProvisionedInstance {
            index: 0,
            contract_id,
            offer_id: 55,
            host_id,
            gpu_name: "RTX 4090".to_owned(),
            gpu_ram: Some(24_000.0),
            dph_total: 0.42,
        })
    }

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        _label: &str,
        _lifecycle: &LifecyclePolicy,
        ssh_user: &str,
    ) -> Result<VastAiSshEndpoint, String> {
        let mut state = self.state.lock();
        let request_count = state.requests.len();
        state
            .first_endpoint_request_count
            .get_or_insert(request_count);
        state.endpoint_lookups.push(contract_id);
        Ok(VastAiSshEndpoint {
            host: "ssh5.vast.ai".to_owned(),
            port: 22017,
            user: ssh_user.to_owned(),
        })
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
        self.state.lock().destroyed.push(contract_id);
        Ok(())
    }
}

#[derive(Clone)]
struct OutOfOrderLeaseClient {
    state: Arc<Mutex<OutOfOrderLeaseState>>,
}

#[derive(Default)]
struct OutOfOrderLeaseState {
    requests: Vec<u64>,
    endpoint_lookups: Vec<u64>,
    destroyed: Vec<u64>,
    slow_node_ids: Vec<u64>,
    endpoint_fail_node_ids: Vec<u64>,
}

impl OutOfOrderLeaseClient {
    fn new(slow_node_ids: Vec<u64>, endpoint_fail_node_ids: Vec<u64>) -> Self {
        Self {
            state: Arc::new(Mutex::new(OutOfOrderLeaseState {
                slow_node_ids,
                endpoint_fail_node_ids,
                ..OutOfOrderLeaseState::default()
            })),
        }
    }

    fn node_id_from_label(label: Option<&str>) -> u64 {
        label
            .and_then(|label| label.rsplit('-').next())
            .and_then(|node| node.parse::<u64>().ok())
            .expect("test request labels include node id suffix")
    }
}

impl VastAiLeaseClient for OutOfOrderLeaseClient {
    fn provision_one(&mut self, request: ProvisionRequest) -> Result<ProvisionedInstance, String> {
        let node_id = Self::node_id_from_label(request.label.as_deref());
        let should_sleep = {
            let mut state = self.state.lock();
            state.requests.push(node_id);
            state.slow_node_ids.contains(&node_id)
        };
        if should_sleep {
            std::thread::sleep(Duration::from_millis(150));
        }
        Ok(ProvisionedInstance {
            index: 0,
            contract_id: 1_000 + node_id,
            offer_id: 55 + node_id,
            host_id: Some(10_000 + node_id),
            gpu_name: "RTX 4090".to_owned(),
            gpu_ram: Some(24_000.0),
            dph_total: 0.42,
        })
    }

    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        _label: &str,
        _lifecycle: &LifecyclePolicy,
        ssh_user: &str,
    ) -> Result<VastAiSshEndpoint, String> {
        let node_id = contract_id - 1_000;
        let should_fail = {
            let mut state = self.state.lock();
            state.endpoint_lookups.push(node_id);
            state.endpoint_fail_node_ids.contains(&node_id)
        };
        if should_fail {
            return Err("connection refused".to_owned());
        }
        Ok(VastAiSshEndpoint {
            host: "ssh5.vast.ai".to_owned(),
            port: 22017,
            user: ssh_user.to_owned(),
        })
    }

    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
        self.state.lock().destroyed.push(contract_id);
        Ok(())
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
fn pipeline_starts_blacklist_hosts_already_leased_in_run() {
    let mut client = FakeLeaseClient::default().with_contract(100);
    client.host_ids.extend([Some(77), Some(88)]);
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());
    let first = spec();
    let mut second = spec();
    second.node_id = 12;
    second.stage_index = Some(3);

    let first_handle = plugin.start_node(first, sink()).unwrap();
    let second_handle = plugin.start_node(second, sink()).unwrap();

    let requests = &plugin.client().requests;
    assert_eq!(requests.len(), 2);
    assert!(
        !requests[0].selection.blacklist_hosts.contains(&77),
        "first node should not preemptively blacklist the host it has not leased"
    );
    assert!(
        requests[1].selection.blacklist_hosts.contains(&77),
        "second node should avoid the first node's Vast.ai host"
    );
    assert!(
        requests[1].selection.blacklist_hosts.contains(&59017),
        "existing operator blacklist must be preserved"
    );

    plugin.stop_node(&second_handle).unwrap();
    plugin.stop_node(&first_handle).unwrap();
}

#[test]
fn failed_vastai_host_is_blacklisted_for_later_requests() {
    let mut client = FakeLeaseClient::default().with_contract(100);
    client.host_ids.extend([Some(77), Some(88)]);
    client
        .endpoint_results
        .push_back(Err("connection refused".to_owned()));
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());
    let first_error = plugin.start_node(spec(), sink()).unwrap_err();
    assert!(first_error.contains("connection refused"));

    let mut second = spec();
    second.node_id = 12;
    second.stage_index = Some(3);
    let second_handle = plugin.start_node(second, sink()).unwrap();

    assert!(
        plugin.client().requests[1]
            .selection
            .blacklist_hosts
            .contains(&77),
        "host that failed before runtime-ready must be excluded from later Vast.ai requests"
    );
    plugin.stop_node(&second_handle).unwrap();
}

#[test]
fn vastai_start_nodes_starts_lease_requests_concurrently() {
    let client = ParallelLeaseClient::new(4);
    let state = client.state.clone();
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());
    let specs = (0..4)
        .map(|index| {
            let mut spec = spec();
            spec.node_id = 11 + index;
            spec.stage_index = Some(u32::try_from(index).unwrap());
            spec
        })
        .collect::<Vec<_>>();

    let results = plugin.start_nodes(specs, sink());

    assert!(results.iter().all(|(_, result)| result.is_ok()));
    assert_eq!(plugin.active_contract_count(), 4);
    assert_eq!(plugin.bootstrap().starts.len(), 4);
    let state = state.lock();
    assert_eq!(state.requests.len(), 4);
    assert_eq!(state.endpoint_lookups.len(), 4);
    assert_eq!(
        state.first_endpoint_request_count,
        Some(4),
        "SSH lookup must not begin before every lease request has started"
    );
}

#[test]
fn vastai_start_nodes_assigns_shared_first_wave_offer_plan() {
    let client = ParallelLeaseClient::new(3);
    let state = client.state.clone();
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());
    let specs = (0..3)
        .map(|index| {
            let mut spec = spec();
            spec.node_id = 11 + index;
            spec.stage_index = Some(u32::try_from(index).unwrap());
            spec
        })
        .collect::<Vec<_>>();

    let results = plugin.start_nodes(specs, sink());

    assert!(results.iter().all(|(_, result)| result.is_ok()));
    let state = state.lock();
    assert_eq!(state.first_wave_plan_requests, 3);
    let mut assigned = state
        .requests
        .iter()
        .map(|request| {
            (
                request.label.clone().expect("request label"),
                request.preferred_offer_id,
            )
        })
        .collect::<Vec<_>>();
    assigned.sort_by(|left, right| left.0.cmp(&right.0));
    assert_eq!(
        assigned
            .into_iter()
            .map(|(_, preferred)| preferred)
            .collect::<Vec<_>>(),
        vec![Some(9_000), Some(9_001), Some(9_002)],
        "per-node requests should carry the coordinated first-wave offer plan"
    );
}

#[test]
fn vastai_start_nodes_bootstraps_fast_completion_before_earlier_slow_node() {
    let client = OutOfOrderLeaseClient::new(vec![11], Vec::new());
    let state = client.state.clone();
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());
    let mut slow = spec();
    slow.node_id = 11;
    slow.stage_index = Some(0);
    let mut fast = spec();
    fast.node_id = 12;
    fast.stage_index = Some(1);

    let results = plugin.start_nodes(vec![slow, fast], sink());

    assert!(results.iter().all(|(_, result)| result.is_ok()));
    assert_eq!(
        plugin
            .bootstrap()
            .starts
            .iter()
            .map(|(spec, _)| spec.node_id)
            .collect::<Vec<_>>(),
        vec![12, 11],
        "later fast completion should bootstrap before earlier slow completion"
    );
    assert_eq!(state.lock().endpoint_lookups.len(), 2);
}

#[test]
fn vastai_start_nodes_one_failure_does_not_block_completed_node_bootstrap() {
    let client = OutOfOrderLeaseClient::new(vec![11], vec![11]);
    let state = client.state.clone();
    let mut plugin = VastAiProvisioningPlugin::new(client, FakeBootstrap::default(), config());
    let mut slow_failure = spec();
    slow_failure.node_id = 11;
    slow_failure.stage_index = Some(0);
    let mut fast_success = spec();
    fast_success.node_id = 12;
    fast_success.stage_index = Some(1);

    let results = plugin.start_nodes(vec![slow_failure, fast_success], sink());

    assert!(
        results[0]
            .1
            .as_ref()
            .unwrap_err()
            .contains("connection refused")
    );
    assert!(
        results[0]
            .1
            .as_ref()
            .unwrap_err()
            .contains("class=connection_refused")
    );
    assert!(results[1].1.is_ok());
    assert_eq!(
        plugin
            .bootstrap()
            .starts
            .iter()
            .map(|(spec, _)| spec.node_id)
            .collect::<Vec<_>>(),
        vec![12],
        "successful completed node should bootstrap even though another node fails"
    );
    assert_eq!(state.lock().destroyed.as_slice(), &[1_011]);
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
    assert_eq!(
        plugin.bootstrap().stops,
        vec![(1, BootstrapStopReason::NodeStop)]
    );
    assert_eq!(plugin.active_contract_count(), 0);
}

#[test]
fn vastai_complete_bootstrap_stops_optional_log_tail_before_node_stop() {
    let mut plugin = VastAiProvisioningPlugin::new(
        FakeLeaseClient::default().with_contract(100),
        FakeBootstrap::default(),
        config(),
    );
    let handle = plugin.start_node(spec(), sink()).unwrap();

    plugin.complete_bootstrap(&handle).unwrap();

    assert_eq!(
        plugin.bootstrap().stops,
        vec![(1, BootstrapStopReason::RuntimeReady)]
    );
    assert_eq!(plugin.client().destroyed, Vec::<u64>::new());
    assert_eq!(plugin.active_contract_count(), 1);

    plugin.stop_node(&handle).unwrap();

    assert_eq!(plugin.client().destroyed, vec![100]);
    assert_eq!(
        plugin.bootstrap().stops,
        vec![(1, BootstrapStopReason::RuntimeReady)]
    );
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
