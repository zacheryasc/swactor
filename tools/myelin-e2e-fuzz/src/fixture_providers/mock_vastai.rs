use myelin::providers::LocalDockerPlugin;
#[cfg(test)]
use myelin::providers::LocalProcessPlugin;
use provisioning::plugin::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginSink,
    ProvisionPlugin,
};
use std::collections::BTreeMap;
#[cfg(test)]
use std::path::PathBuf;
use swactor::runtime::Runtime;
use swactor_engine::BlockingWorkSender;
const SELECTED_OFFER_ID_ENV: &str = "MYELIN_SELECTED_OFFER_ID";

pub(crate) struct MockVastAiPlugin {
    inner: Box<dyn ProvisionPlugin>,
    contracts: BTreeMap<u64, MockVastAiContract>,
    selected_offers: BTreeMap<u64, u64>,
}

#[derive(Clone)]
struct MockVastAiContract {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    provider_ref: String,
    selected_offer_id: Option<u64>,
}

impl MockVastAiPlugin {
    fn with_inner(inner: Box<dyn ProvisionPlugin>) -> Self {
        Self {
            inner,
            contracts: BTreeMap::new(),
            selected_offers: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_registry(
        blocking: BlockingWorkSender,
        program: impl Into<PathBuf>,
        registry_path: impl Into<PathBuf>,
        runtime: Runtime,
    ) -> Self {
        Self::with_inner(Box::new(LocalProcessPlugin::with_registry(
            blocking,
            program,
            registry_path,
            "mock-vastai",
            runtime,
        )))
    }

    pub(crate) fn with_docker(
        blocking: BlockingWorkSender,
        container_name_prefix: impl Into<String>,
        runtime: Runtime,
    ) -> Self {
        Self::with_inner(Box::new(LocalDockerPlugin::new(
            blocking,
            container_name_prefix,
            runtime,
        )))
    }
}

fn selected_offer_from_spec(spec: &NodeProvisionSpec) -> Option<u64> {
    spec.env
        .iter()
        .find(|(key, _)| key == SELECTED_OFFER_ID_ENV)
        .and_then(|(_, value)| value.parse().ok())
}

fn observe_mock_vastai_contract(
    sink: &PluginSink,
    event_type: &str,
    spec: &NodeProvisionSpec,
    provider_ref: &str,
    selected_offer_id: Option<u64>,
) {
    sink.observe(PluginObservation::ProviderLine {
        run_id: spec.run_id,
        node_id: spec.node_id,
        line: serde_json::json!({
            "type": event_type,
            "simulated": true,
            "provider_ref": provider_ref,
            "selected_offer_id": selected_offer_id,
        })
        .to_string(),
    });
}

impl ProvisionPlugin for MockVastAiPlugin {
    fn create_node(
        &mut self,
        _spec: NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        Err("mock Vast.ai provisioning requires an exact selected offer".to_owned())
    }

    fn create_node_selected(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        selected_offer_id: Option<u64>,
    ) -> Result<PluginNodeHandle, String> {
        let offer_id = selected_offer_id
            .filter(|offer_id| *offer_id > 0)
            .ok_or_else(|| {
                "mock Vast.ai provisioning requires an exact selected offer".to_owned()
            })?;
        let provider_ref = self.inner.provider_ref_for(&spec);
        let handle = self.inner.create_node(spec.clone(), sink.clone())?;
        self.selected_offers.insert(handle.id, offer_id);
        self.contracts.insert(
            handle.id,
            MockVastAiContract {
                spec: spec.clone(),
                sink: sink.clone(),
                provider_ref: provider_ref.clone(),
                selected_offer_id: Some(offer_id),
            },
        );
        observe_mock_vastai_contract(
            &sink,
            "MockVastAiContractCreated",
            &spec,
            &provider_ref,
            Some(offer_id),
        );
        Ok(handle)
    }

    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.inner.start_bootstrap(handle)
    }

    fn cancel_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.inner.cancel_bootstrap(handle)
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.inner.complete_bootstrap(handle)
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let contract = self.contracts.get(&handle.id).cloned();
        self.inner.stop_node(handle)?;
        self.selected_offers.remove(&handle.id);
        self.contracts.remove(&handle.id);
        if let Some(contract) = contract {
            observe_mock_vastai_contract(
                &contract.sink,
                "MockVastAiContractDestroyed",
                &contract.spec,
                &contract.provider_ref,
                contract.selected_offer_id,
            );
        }
        Ok(())
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let adopted = self.inner.adopt_by_spec(spec, sink.clone())?;
        if let Some(adopted) = &adopted {
            let offer_id = selected_offer_from_spec(spec);
            if let Some(offer_id) = offer_id {
                self.selected_offers.insert(adopted.handle.id, offer_id);
            }
            self.contracts.insert(
                adopted.handle.id,
                MockVastAiContract {
                    spec: spec.clone(),
                    sink: sink.clone(),
                    provider_ref: adopted.provider_ref.clone(),
                    selected_offer_id: offer_id,
                },
            );
            observe_mock_vastai_contract(
                &sink,
                "MockVastAiContractAdopted",
                spec,
                &adopted.provider_ref,
                offer_id,
            );
        }
        Ok(adopted)
    }

    fn prepare_missing_bootstrap(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let prepared = self.inner.prepare_missing_bootstrap(spec, sink.clone())?;
        if let Some(prepared) = &prepared {
            let offer_id = selected_offer_from_spec(spec);
            if let Some(offer_id) = offer_id {
                self.selected_offers.insert(prepared.handle.id, offer_id);
            }
            self.contracts.insert(
                prepared.handle.id,
                MockVastAiContract {
                    spec: spec.clone(),
                    sink: sink.clone(),
                    provider_ref: prepared.provider_ref.clone(),
                    selected_offer_id: offer_id,
                },
            );
            observe_mock_vastai_contract(
                &sink,
                "MockVastAiContractRecreated",
                spec,
                &prepared.provider_ref,
                offer_id,
            );
        }
        Ok(prepared)
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        self.inner.provider_ref_for(spec)
    }

    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        self.inner.list_managed_refs()
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        let provider_ref = self.inner.provider_ref_for(spec);
        let stopped = self.inner.stop_by_spec(spec, sink.clone())?;
        if stopped {
            let matching_handles = self
                .contracts
                .iter()
                .filter_map(|(handle, contract)| {
                    (contract.spec.run_id == spec.run_id
                        && contract.spec.node_id == spec.node_id
                        && contract.spec.attempt_id == spec.attempt_id)
                        .then_some(*handle)
                })
                .collect::<Vec<_>>();
            for handle in matching_handles {
                self.contracts.remove(&handle);
                self.selected_offers.remove(&handle);
            }
            observe_mock_vastai_contract(
                &sink,
                "MockVastAiContractDestroyed",
                spec,
                &provider_ref,
                selected_offer_from_spec(spec),
            );
        }
        Ok(stopped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, mpsc};
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, TokioBackend, TokioConfig};
    struct ChannelSink(mpsc::Sender<PluginObservation>);
    impl provisioning::plugin::PluginObservationSink for ChannelSink {
        fn observe(&self, observation: PluginObservation) {
            let _ = self.0.send(observation);
        }
    }
    fn test_runtime() -> (Engine, Runtime) {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = TokioBackend::new(TokioConfig::default()).expect("test Tokio backend");
        let engine = Engine::new(parts, backend).expect("test actor engine");
        (engine, runtime)
    }
    fn test_spec() -> NodeProvisionSpec {
        NodeProvisionSpec {
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 11,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            offer_criteria_json: None,
            mounts: Vec::new(),
        }
    }
    #[test]
    fn exact_offer_is_required_before_any_worker_is_started() {
        let (engine, runtime) = test_runtime();
        let directory = tempfile::tempdir().unwrap();
        let (tx, rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut plugin = MockVastAiPlugin::with_registry(
            engine.handle().blocking_work_sender(),
            "/not/executed",
            directory.path().join("registry.json"),
            runtime,
        );
        let spec = test_spec();
        assert!(
            plugin
                .create_node_selected(spec.clone(), sink.clone(), None)
                .is_err()
        );
        assert!(
            plugin
                .create_node_selected(spec.clone(), sink.clone(), Some(0))
                .is_err()
        );
        let handle = plugin
            .create_node_selected(spec.clone(), sink, Some(42))
            .unwrap();
        assert_eq!(handle.provider_process_id, None);
        let event = rx
            .try_iter()
            .find_map(|observation| match observation {
                PluginObservation::ProviderLine { line, .. } => Some(
                    serde_json::from_str::<serde_json::Value>(&line).expect("provider event JSON"),
                ),
                _ => None,
            })
            .expect("provider event was not observed");
        assert_eq!(event["selected_offer_id"], 42);
        assert_eq!(event["provider_ref"], plugin.provider_ref_for(&spec));
        plugin.stop_node(&handle).unwrap();
    }

    #[test]
    fn retained_mock_worker_is_adopted_without_replacement() {
        let (engine, runtime) = test_runtime();
        let directory = tempfile::tempdir().unwrap();
        let registry = directory.path().join("registry.json");
        let (tx, _rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut spec = test_spec();
        spec.env = vec![
            ("MYELIN_RUN_ID".to_owned(), spec.run_id.to_string()),
            (
                "MYELIN_LOGICAL_NODE_ID".to_owned(),
                spec.node_id.to_string(),
            ),
            (SELECTED_OFFER_ID_ENV.to_owned(), "42".to_owned()),
        ];
        spec.args = vec![
            "-c".to_owned(),
            "trap 'exit 0' TERM; while :; do sleep 1; done".to_owned(),
        ];
        let original = {
            let mut plugin = MockVastAiPlugin::with_registry(
                engine.handle().blocking_work_sender(),
                "/bin/sh",
                &registry,
                runtime.clone(),
            );
            let handle = plugin
                .create_node_selected(spec.clone(), sink.clone(), Some(42))
                .unwrap();
            plugin.start_bootstrap(&handle).unwrap();
            plugin
                .adopt_by_spec(&spec, sink.clone())
                .unwrap()
                .unwrap()
                .handle
                .provider_process_id
                .unwrap()
        };
        let mut recovered = MockVastAiPlugin::with_registry(
            engine.handle().blocking_work_sender(),
            "/bin/sh",
            &registry,
            runtime,
        );
        let adopted = recovered
            .adopt_by_spec(&spec, sink.clone())
            .unwrap()
            .expect("retained fixture must be adoptable");
        assert_eq!(adopted.handle.provider_process_id, Some(original));
        recovered.stop_node(&adopted.handle).unwrap();
        assert!(recovered.adopt_by_spec(&spec, sink).unwrap().is_none());
    }
}
