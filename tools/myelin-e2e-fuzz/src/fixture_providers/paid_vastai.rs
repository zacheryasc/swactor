//! Harness-owned authorization for paid fixture acquisition and recovery.
use myelin::providers::{
    CleanupObserver, OwnershipRecorder, ProvisionRequest, SnapshotObserver,
    SshEndpoint as VastAiSshEndpoint, ToolsVastAiLeaseClient, VastAiLeaseClient,
    VastAiProviderMonitor,
};
use provisioning::plugin::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginSink, ProvisionPlugin,
};
use provisioning::{
    PaidAdmissionMode, PaidAdmissionSnapshot, PaidFixtureAdmission, PaidNodeAdmission,
};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use swactor_vastai::{
    BlockingVastClient, CreateInstanceRequest, LifecyclePolicy, Offer, OfferBrowseCriteria,
    ProvisionedInstance, VastClient,
};

#[derive(Clone)]
pub(crate) struct PaidVastAiLeaseClient {
    client: BlockingVastClient,
    monitor: ToolsVastAiLeaseClient,
    paid_admission_path: PathBuf,
}
impl PaidVastAiLeaseClient {
    pub(crate) fn new(
        client: VastClient,
        monitor: ToolsVastAiLeaseClient,
        admission: PathBuf,
    ) -> Result<Self, String> {
        Ok(Self {
            client: BlockingVastClient::new(client)?,
            monitor,
            paid_admission_path: admission,
        })
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

    fn create_selected_offer(
        &mut self,
        request: &ProvisionRequest,
        offer_id: u64,
        host_id: Option<u64>,
        observed_offer: Option<&Offer>,
        spec: &NodeProvisionSpec,
    ) -> Result<ProvisionedInstance, String> {
        let admission = PaidFixtureAdmission::open(&self.paid_admission_path)?;
        let snapshot = admission.snapshot()?;
        let expected = paid_node_for_spec(
            &snapshot,
            spec,
            request.label.as_deref().unwrap_or_default(),
            Some(offer_id),
        )?;
        if host_id != Some(expected.host_id) {
            return Err("paid VastAI offer host differs from selected topology".to_owned());
        }
        admission.reserve_create(
            spec.run_id,
            spec.node_id,
            spec.attempt_id,
            offer_id,
            &expected.label,
        )?;
        let create = Self::create_request_for_offer(request, offer_id);
        let info = self.client.create_instance(&create).map_err(|error| {
            if error.is_definitive_rejection() {
                if let Err(record_error) = admission.record_create_rejection(spec.node_id) {
                    return format!("{error}; record definitive create rejection: {record_error}");
                }
            }
            error.to_string()
        })?;
        admission.record_contract(spec.node_id, info.contract_id)?;
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

fn retryable_owned_census_error(error: &str) -> bool {
    error.starts_with("provider census ")
}

fn paid_node_for_spec<'a>(
    snapshot: &'a PaidAdmissionSnapshot,
    spec: &NodeProvisionSpec,
    label: &str,
    selected_offer_id: Option<u64>,
) -> Result<&'a PaidNodeAdmission, String> {
    // The orchestrator provisions the initial attempt with attempt_id 0
    // (admission labels reserve `...-attempt-0`); zero is a valid identity
    // and only a run mismatch invalidates the spec.
    if snapshot.run_id != spec.run_id {
        return Err("paid VastAI run identity mismatch".to_owned());
    }
    let expected = snapshot
        .nodes
        .iter()
        .find(|node| node.node_id == spec.node_id)
        .ok_or_else(|| "paid VastAI node was not selected".to_owned())?;
    if expected.label != label
        || selected_offer_id.is_some_and(|offer_id| offer_id != expected.offer_id)
        || snapshot
            .create_reservations
            .get(&spec.node_id)
            .is_some_and(|reservation| reservation.attempt_id != spec.attempt_id)
    {
        return Err("paid VastAI node identity differs from selected topology".to_owned());
    }
    Ok(expected)
}
impl VastAiLeaseClient for PaidVastAiLeaseClient {
    fn provision_one(&mut self, _request: ProvisionRequest) -> Result<ProvisionedInstance, String> {
        Err("paid VastAI acquisition requires run-scoped admission".to_owned())
    }
    fn provision_exact(
        &mut self,
        _request: ProvisionRequest,
        _offer_id: u64,
        _criteria: OfferBrowseCriteria,
    ) -> Result<ProvisionedInstance, String> {
        Err("paid VastAI acquisition requires run-scoped admission".to_owned())
    }
    fn contract_by_label(&mut self, label: &str) -> Result<Option<u64>, String> {
        let instances: Vec<_> = {
            let path = &self.paid_admission_path;
            let snapshot = PaidFixtureAdmission::open(path)?.snapshot()?;
            let census = self.client.owned_census(
                &snapshot.cleanup_labels(),
                &snapshot.known_contract_ids(),
                Instant::now(),
                VastClient::REQUEST_TIMEOUT * 4 + Duration::from_secs(180),
            )?;
            census
                .instances
                .into_iter()
                .filter(|instance| {
                    census
                        .contract_labels
                        .get(&instance.contract_id)
                        .is_some_and(|owned| owned == label)
                })
                .collect()
        };
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
    fn provision_node(
        &mut self,
        request: ProvisionRequest,
        spec: &NodeProvisionSpec,
        selected_offer_id: Option<u64>,
        criteria: Option<OfferBrowseCriteria>,
    ) -> Result<Option<ProvisionedInstance>, String> {
        if request.count != 1 {
            return Err("paid VastAI provisioning requires exactly one logical node".to_owned());
        }
        let path = &self.paid_admission_path;
        let admission = PaidFixtureAdmission::open(path)?;
        let snapshot = admission.snapshot()?;
        let label = request.label.as_deref().unwrap_or_default();
        let expected = paid_node_for_spec(&snapshot, spec, label, selected_offer_id)?;
        if snapshot.mode == PaidAdmissionMode::CleanupOnly {
            return Err("paid VastAI fixture is cleanup-only".to_owned());
        }
        let existing = loop {
            match self.contract_by_label(label) {
                Ok(existing) => break existing,
                Err(error) if retryable_owned_census_error(&error) => {
                    if admission.snapshot()?.mode == PaidAdmissionMode::CleanupOnly {
                        return Err(format!(
                            "paid fixture stopped while establishing label absence: {error}"
                        ));
                    }
                    eprintln!(
                        "  paid label {label} observation failed before create: {error} (retrying)"
                    );
                    self.client
                        .pace_owned_reconciliation(LifecyclePolicy::default().poll_interval);
                }
                Err(error) => return Err(error),
            }
        };
        if let Some(&contract_id) = snapshot.contracts.get(&spec.node_id) {
            if existing != Some(contract_id) {
                return Err("paid VastAI retained contract disappeared or changed".to_owned());
            }
            return Ok(Some(ProvisionedInstance {
                offer_id: expected.offer_id,
                host_id: Some(expected.host_id),
                ..adopted_instance(contract_id)
            }));
        }
        if snapshot.create_reservations.contains_key(&spec.node_id) {
            if let Some(contract_id) = existing {
                admission.record_contract(spec.node_id, contract_id)?;
            }
            return Err(
                "paid VastAI create slot was already consumed; cleanup is required".to_owned(),
            );
        }
        if existing.is_some() {
            return Err("paid VastAI label exists without a create reservation".to_owned());
        }
        criteria
            .filter(|_| selected_offer_id == Some(expected.offer_id))
            .ok_or_else(|| {
                "paid VastAI acquisition requires the exact selected offer and criteria".to_owned()
            })?;
        // The search receipt and durable admission bind this immutable offer ID
        // and host. Re-browsing the eventually consistent marketplace creates a
        // false TOCTOU rejection; the create endpoint is authoritative.
        match self.create_selected_offer(
            &request,
            expected.offer_id,
            Some(expected.host_id),
            None,
            spec,
        ) {
            Ok(instance) => Ok(Some(instance)),
            Err(create_error) => {
                // The create slot is consumed before the request. An uncertain
                // response is reconciled against that stable label until the
                // same contract appears, the provider definitively rejected
                // the create, or the fixture owner closes acquisition.
                loop {
                    let current = admission.snapshot()?;
                    if current.rejected_creates.contains(&spec.node_id) {
                        return Err(create_error);
                    }
                    if current.mode == PaidAdmissionMode::CleanupOnly {
                        return Err(format!(
                            "{create_error}; paid fixture stopped while reconciling ambiguous create for {label}"
                        ));
                    }
                    match self.contract_by_label(label) {
                        Ok(Some(contract_id)) => {
                            admission.record_contract(spec.node_id, contract_id)?;
                            return Ok(Some(ProvisionedInstance {
                                offer_id: expected.offer_id,
                                host_id: Some(expected.host_id),
                                ..adopted_instance(contract_id)
                            }));
                        }
                        Ok(None) => {
                            eprintln!(
                                "  paid create for label {label} remains unobserved; reconciling"
                            );
                        }
                        Err(error) if retryable_owned_census_error(&error) => {
                            eprintln!(
                                "  paid create for label {label} has incomplete census: {error} (retrying)"
                            );
                        }
                        Err(error) => {
                            return Err(format!(
                                "{create_error}; reconcile ambiguous create for {label}: {error}"
                            ));
                        }
                    }
                    self.client
                        .pace_owned_reconciliation(LifecyclePolicy::default().poll_interval);
                }
            }
        }
    }
    fn ssh_endpoint(
        &mut self,
        contract_id: u64,
        _label: &str,
        ssh_user: &str,
    ) -> Result<Option<VastAiSshEndpoint>, String> {
        let labels = PaidFixtureAdmission::open(&self.paid_admission_path)?
            .snapshot()?
            .cleanup_labels();
        let endpoint =
            self.client
                .wait_for_ssh_endpoint(contract_id, &labels, &LifecyclePolicy::default())?;
        let host = endpoint.ip;
        let port = endpoint.port;
        if host.is_empty() || host == "unknown" {
            return Err(format!("vastai contract {contract_id} has no SSH host"));
        }
        if port == 0 {
            return Err(format!("vastai contract {contract_id} has no SSH port"));
        }
        Ok(Some(VastAiSshEndpoint {
            host,
            port,
            user: ssh_user.to_owned(),
        }))
    }
    fn destroy_contract(&mut self, contract_id: u64) -> Result<(), String> {
        self.client.destroy_instance_with_retry(contract_id)
    }

    fn record_contract(&mut self, label: &str, contract_id: u64) -> Result<(), String> {
        self.monitor.record_contract(label, contract_id)
    }

    fn cancel_ownership(&mut self, label: &str) -> Result<(), String> {
        self.monitor.cancel_ownership(label)
    }

    fn ownership_cancelled(&self, label: &str) -> Result<bool, String> {
        self.monitor.ownership_cancelled(label)
    }

    fn ownership_destroyed(&self, label: &str) -> Result<bool, String> {
        self.monitor.ownership_destroyed(label)
    }

    fn ownership_callbacks(&self, label: &str) -> (OwnershipRecorder, CleanupObserver) {
        self.monitor.ownership_callbacks(label)
    }

    fn ensure_event_source(&self) -> Result<(), String> {
        self.monitor.ensure_event_source()
    }

    fn bootstrap_environment(&self, label: &str) -> Result<Vec<(String, String)>, String> {
        self.monitor.bootstrap_environment(label)
    }

    fn destroy_owned_contract(&mut self, label: &str, contract_id: u64) -> Result<(), String> {
        self.monitor.destroy_owned_contract(label, contract_id)
    }
    fn spawn_provider_monitor(
        &mut self,
        contract_id: Option<u64>,
        label: String,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        observer: SnapshotObserver,
        cleanup_completed: CleanupObserver,
    ) -> Option<VastAiProviderMonitor> {
        self.monitor.spawn_provider_monitor(
            contract_id,
            label,
            spec,
            sink,
            observer,
            cleanup_completed,
        )
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BootstrapState {
    Pending,
    Active,
    Completed,
    Cancelled,
}

struct PaidNode {
    spec: NodeProvisionSpec,
    contract_id: u64,
    bootstrap: BootstrapState,
}

/// Keeps fixture admission at the harness boundary while the real adapter owns
/// provider monitoring, SSH bootstrap and resource cleanup.
pub(crate) struct PaidProvisioningPlugin {
    inner: Box<dyn ProvisionPlugin>,
    admission_path: PathBuf,
    nodes: BTreeMap<u64, PaidNode>,
}

impl PaidProvisioningPlugin {
    pub(crate) fn new(inner: Box<dyn ProvisionPlugin>, admission_path: PathBuf) -> Self {
        Self {
            inner,
            admission_path,
            nodes: BTreeMap::new(),
        }
    }

    fn track(&mut self, handle: &PluginNodeHandle, spec: NodeProvisionSpec) -> Result<(), String> {
        let snapshot = PaidFixtureAdmission::open(&self.admission_path)?.snapshot()?;
        let contract_id = *snapshot
            .contracts
            .get(&spec.node_id)
            .ok_or("paid VastAI lease returned without durable contract ownership")?;
        self.nodes.insert(
            handle.id,
            PaidNode {
                spec,
                contract_id,
                bootstrap: BootstrapState::Pending,
            },
        );
        Ok(())
    }
}

impl ProvisionPlugin for PaidProvisioningPlugin {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        self.create_node_selected(spec, sink, None)
    }

    fn create_node_selected(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        selected_offer_id: Option<u64>,
    ) -> Result<PluginNodeHandle, String> {
        let label = self.inner.provider_ref_for(&spec);
        let snapshot = PaidFixtureAdmission::open(&self.admission_path)?.snapshot()?;
        paid_node_for_spec(&snapshot, &spec, &label, selected_offer_id)?;
        let handle = self
            .inner
            .create_node_selected(spec.clone(), sink, selected_offer_id)?;
        self.track(&handle, spec)?;
        Ok(handle)
    }

    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let node = self
            .nodes
            .get_mut(&handle.id)
            .ok_or("paid VastAI bootstrap handle is absent")?;
        match node.bootstrap {
            BootstrapState::Active | BootstrapState::Completed => return Ok(()),
            BootstrapState::Cancelled => {
                return Err("bootstrap_cancelled: paid node attempt was retired".to_owned());
            }
            BootstrapState::Pending => {}
        }
        let admission = PaidFixtureAdmission::open(&self.admission_path)?;
        let snapshot = admission.snapshot()?;
        let label = self.inner.provider_ref_for(&node.spec);
        paid_node_for_spec(&snapshot, &node.spec, &label, None)?;
        if snapshot.contracts.get(&node.spec.node_id) != Some(&node.contract_id) {
            return Err("paid VastAI bootstrap contract identity mismatch".to_owned());
        }
        if snapshot.mode == PaidAdmissionMode::Prepared {
            let deployment =
                node.spec.deployment.as_ref().ok_or(
                    "prepared paid fixture denies bootstrap without an explicit deployment",
                )?;
            admission.reserve_retained_bootstrap(
                node.spec.run_id,
                node.spec.node_id,
                node.contract_id,
                deployment,
            )?;
        } else {
            admission.reserve_bootstrap(node.spec.run_id, node.spec.node_id)?;
        }
        self.inner.start_bootstrap(handle)?;
        node.bootstrap = BootstrapState::Active;
        Ok(())
    }

    fn cancel_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.inner.cancel_bootstrap(handle)?;
        if let Some(node) = self.nodes.get_mut(&handle.id) {
            node.bootstrap = BootstrapState::Cancelled;
        }
        Ok(())
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.inner.complete_bootstrap(handle)?;
        if let Some(node) = self.nodes.get_mut(&handle.id) {
            if node.bootstrap != BootstrapState::Cancelled {
                node.bootstrap = BootstrapState::Completed;
            }
        }
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.inner.stop_node(handle)?;
        self.nodes.remove(&handle.id);
        Ok(())
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let snapshot = PaidFixtureAdmission::open(&self.admission_path)?.snapshot()?;
        let label = self.inner.provider_ref_for(spec);
        paid_node_for_spec(&snapshot, spec, &label, None)?;
        if snapshot.mode == PaidAdmissionMode::CleanupOnly {
            return Err("paid VastAI fixture is cleanup-only".to_owned());
        }
        if !snapshot.contracts.contains_key(&spec.node_id)
            && !snapshot.create_reservations.contains_key(&spec.node_id)
        {
            return Ok(None);
        }
        // The lease implementation reconciles the immutable label exactly once;
        // ambiguous reservations cannot create replacement contracts.
        let handle = self.create_node(spec.clone(), sink)?;
        Ok(Some(AdoptedNode {
            handle,
            provider_ref: label,
        }))
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        self.inner.provider_ref_for(spec)
    }
    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        self.inner.list_managed_refs()
    }
    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        self.inner.stop_by_spec(spec, sink)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use swactor::runtime::{RuntimeConfig, RuntimeParts};
    use swactor_engine::Engine;
    use swactor_vastai::SelectionPolicy;
    use swactor_vastai::test_http::{TestHttpRoute, TestHttpServer};
    fn http_engine() -> Engine {
        Engine::new(
            RuntimeParts::new(RuntimeConfig::default()),
            swactor_engine::TokioBackend::new(swactor_engine::TokioConfig::default()).unwrap(),
        )
        .unwrap()
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

    fn paid_test_admission() -> (
        tempfile::TempDir,
        PaidFixtureAdmission,
        provisioning::PaidCleanupOwner,
    ) {
        use std::os::unix::fs::PermissionsExt;
        let directory = tempfile::tempdir().unwrap();
        std::fs::set_permissions(directory.path(), std::fs::Permissions::from_mode(0o700)).unwrap();
        let nodes = (1..=5)
            .map(|node_id| PaidNodeAdmission {
                node_id,
                offer_id: 40 + node_id,
                host_id: node_id,
                label: format!("myelin-5-{node_id}-attempt-9"),
            })
            .collect();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap();
        let admission = PaidFixtureAdmission::create(
            directory.path().join("admission.json"),
            5,
            nodes,
            u64::try_from(now.as_millis()).unwrap()
                + provisioning::paid_admission::PAID_CLEANUP_RESERVE_MS
                + 60_000,
        )
        .unwrap();
        admission
            .install_cleanup_ownership(
                provisioning::PaidProcessIdentity::current().unwrap(),
                provisioning::PaidCleanupLimits {
                    maximum_cost_microusd: 1_000_000,
                    hourly_price_microusd: 1_000_000,
                },
            )
            .unwrap();
        let owner = admission.claim_cleanup_owner().unwrap();
        admission.bind_orchestrator(std::process::id()).unwrap();
        (directory, admission, owner)
    }

    fn paid_test_request() -> ProvisionRequest {
        ProvisionRequest {
            label: Some("myelin-5-2-attempt-9".to_owned()),
            ..exact_request()
        }
    }

    fn paid_test_routes(status: u16, response: serde_json::Value) -> Vec<TestHttpRoute> {
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
            TestHttpRoute::json("PUT", "/api/v0/asks/42/", status, response),
        ]
    }

    #[test]
    fn configured_missing_admission_never_reaches_provider_acquisition() {
        let engine = http_engine();
        let directory = tempfile::tempdir().unwrap();
        let server =
            TestHttpServer::start(&engine.handle().blocking_work_sender(), Vec::new()).unwrap();
        let api = VastClient::with_base_url(server.uri(), "secret");
        let mut client = PaidVastAiLeaseClient::new(
            api.clone(),
            ToolsVastAiLeaseClient::new(api).unwrap(),
            directory.path().join("not-authorized.json"),
        )
        .unwrap();
        assert!(client.provision_one(paid_test_request()).is_err());
        assert!(
            client
                .provision_exact(paid_test_request(), 42, OfferBrowseCriteria::default())
                .is_err()
        );
        assert!(
            client
                .provision_node(
                    paid_test_request(),
                    &monitor_spec(5, 2),
                    Some(42),
                    Some(OfferBrowseCriteria::default())
                )
                .is_err()
        );
        assert!(server.requests().is_empty());
    }

    #[test]
    fn paid_create_is_durable_before_return_and_missing_contract_cannot_be_replaced() {
        let engine = http_engine();
        let (directory, admission, _owner) = paid_test_admission();
        let server = TestHttpServer::start(
            &engine.handle().blocking_work_sender(),
            paid_test_routes(200, json!({"new_contract": 700})),
        )
        .unwrap();
        let api = VastClient::with_base_url(server.uri(), "secret");
        let mut client = PaidVastAiLeaseClient::new(
            api.clone(),
            ToolsVastAiLeaseClient::new(api).unwrap(),
            directory.path().join("admission.json"),
        )
        .unwrap();
        let instance = client
            .provision_node(
                paid_test_request(),
                &monitor_spec(5, 2),
                Some(42),
                Some(OfferBrowseCriteria::default()),
            )
            .unwrap()
            .expect("paid fixture resolved its contract");
        assert_eq!(instance.contract_id, 700);
        assert_eq!(
            admission.snapshot().unwrap().contracts,
            BTreeMap::from([(2, 700)])
        );
        // The provider now reports absence. Even a fresh client must not replace it.
        let mut reopened = client.clone();
        assert!(
            reopened
                .provision_node(
                    paid_test_request(),
                    &monitor_spec(5, 2),
                    Some(42),
                    Some(OfferBrowseCriteria::default())
                )
                .is_err()
        );
        let creates = server
            .requests()
            .into_iter()
            .filter(|request| request.method == "PUT")
            .map(|request| request.path)
            .collect::<Vec<_>>();
        assert_eq!(creates, ["/api/v0/asks/42/"]);
    }

    #[test]
    fn paid_ambiguous_rate_limit_reconciles_exact_contract_without_recreating() {
        let engine = http_engine();
        let (directory, admission, _owner) = paid_test_admission();
        let mut routes = paid_test_routes(429, json!({"error": "rate limited"}));
        routes.push(
            TestHttpRoute::json(
                "GET",
                "/api/v0/instances/",
                200,
                json!({
                    "instances": [{
                        "id": 700,
                        "label": "myelin-5-2-attempt-9",
                        "actual_status": "loading",
                        "ssh_host": "",
                        "ssh_port": 0,
                        "public_ipaddr": ""
                    }]
                }),
            )
            .starting_with_request(2),
        );
        let server =
            TestHttpServer::start(&engine.handle().blocking_work_sender(), routes).unwrap();
        let api = VastClient::with_base_url(server.uri(), "secret");
        let mut client = PaidVastAiLeaseClient::new(
            api.clone(),
            ToolsVastAiLeaseClient::new(api).unwrap(),
            directory.path().join("admission.json"),
        )
        .unwrap();

        let instance = client
            .provision_node(
                paid_test_request(),
                &monitor_spec(5, 2),
                Some(42),
                Some(OfferBrowseCriteria::default()),
            )
            .unwrap()
            .expect("paid fixture resolved its contract");

        assert_eq!(instance.contract_id, 700);
        let creates = server
            .requests()
            .into_iter()
            .filter(|request| request.method == "PUT")
            .map(|request| request.path)
            .collect::<Vec<_>>();
        assert_eq!(creates, ["/api/v0/asks/42/"]);
        let snapshot = admission.snapshot().unwrap();
        assert_eq!(snapshot.contracts.get(&2), Some(&700));
        assert_eq!(
            snapshot
                .create_reservations
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [2]
        );
    }

    #[test]
    fn paid_offer_host_mismatch_is_rejected_before_create() {
        let engine = http_engine();
        let (directory, admission, _owner) = paid_test_admission();
        let server =
            TestHttpServer::start(&engine.handle().blocking_work_sender(), Vec::new()).unwrap();
        let api = VastClient::with_base_url(server.uri(), "secret");
        let mut client = PaidVastAiLeaseClient::new(
            api.clone(),
            ToolsVastAiLeaseClient::new(api).unwrap(),
            directory.path().join("admission.json"),
        )
        .unwrap();
        let wrong_host: Offer = serde_json::from_value(json!({
            "id": 42, "host_id": 999, "gpu_name": "B", "dph_total": 0.3,
            "internet_down_cost_per_tb": 0.0, "internet_up_cost_per_tb": 0.0,
        }))
        .unwrap();
        assert!(
            client
                .create_selected_offer(
                    &paid_test_request(),
                    wrong_host.id,
                    wrong_host.host_id,
                    Some(&wrong_host),
                    &monitor_spec(5, 2)
                )
                .is_err()
        );
        assert!(server.requests().is_empty());
        assert!(admission.snapshot().unwrap().create_reservations.is_empty());
    }

    struct BootstrapFixture(std::sync::Arc<std::sync::atomic::AtomicUsize>);
    impl ProvisionPlugin for BootstrapFixture {
        fn create_node(
            &mut self,
            _spec: NodeProvisionSpec,
            _sink: PluginSink,
        ) -> Result<PluginNodeHandle, String> {
            Ok(PluginNodeHandle {
                id: 1,
                provider_process_id: None,
            })
        }
        fn start_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
        fn cancel_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            Ok(())
        }
        fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            Ok(())
        }
        fn stop_node(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
            Ok(())
        }
        fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
            format!(
                "myelin-{}-{}-attempt-{}",
                spec.run_id, spec.node_id, spec.attempt_id
            )
        }
    }
    struct NullSink;
    impl provisioning::plugin::PluginObservationSink for NullSink {
        fn observe(&self, _observation: provisioning::plugin::PluginObservation) {}
    }
    #[test]
    fn active_bootstrap_is_idempotent_but_cancel_does_not_reauthorize() {
        let (directory, admission, _owner) = paid_test_admission();
        admission
            .reserve_create(5, 2, 9, 42, "myelin-5-2-attempt-9")
            .unwrap();
        admission.record_contract(2, 73).unwrap();
        let starts = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut plugin = PaidProvisioningPlugin::new(
            Box::new(BootstrapFixture(starts.clone())),
            directory.path().join("admission.json"),
        );
        let handle = plugin
            .create_node(
                monitor_spec(5, 2),
                PluginSink::new(std::sync::Arc::new(NullSink)),
            )
            .unwrap();
        plugin.start_bootstrap(&handle).unwrap();
        plugin.start_bootstrap(&handle).unwrap();
        plugin.cancel_bootstrap(&handle).unwrap();
        assert!(plugin.start_bootstrap(&handle).is_err());
        assert_eq!(starts.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            admission
                .snapshot()
                .unwrap()
                .initial_bootstraps
                .keys()
                .copied()
                .collect::<Vec<_>>(),
            [2]
        );
    }
}
