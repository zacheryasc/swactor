use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeProvisionSpec {
    pub run_id: u64,
    pub node_id: u64,
    /// Concrete attempt identity. Zero is reserved for an unbound template.
    #[serde(default)]
    pub attempt_id: u64,
    pub stage_index: Option<u32>,
    pub image: String,
    pub env: Vec<(String, String)>,
    pub args: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub mounts: Vec<ProviderMount>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderMount {
    pub host_path: String,
    pub container_path: String,
    pub readonly: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionEvent {
    pub run_id: u64,
    pub node_id: u64,
    pub kind: ProvisionEventKind,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    pub message: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProvisionEventKind {
    ProvisionStart,
    NodeLive,
    ProvisionFailed,
    NodeStopped,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionLogLine {
    pub run_id: u64,
    pub node_id: u64,
    pub stream: ProvisionLogStream,
    pub line: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProvisionLogStream {
    Stdout,
    Stderr,
    Provider,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PluginObservation {
    StdoutLine {
        run_id: u64,
        node_id: u64,
        line: String,
    },
    StderrLine {
        run_id: u64,
        node_id: u64,
        line: String,
    },
    TelemetryFrame {
        run_id: u64,
        node_id: u64,
        channel: String,
        payload: String,
    },
    ProviderLine {
        run_id: u64,
        node_id: u64,
        line: String,
    },
    Exited {
        run_id: u64,
        node_id: u64,
        status: Option<i32>,
    },
    Failed {
        run_id: u64,
        node_id: u64,
        reason: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PluginNodeHandle {
    pub id: u64,
    pub provider_process_id: Option<u32>,
}

/// Result of a successful spec-addressed adoption.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdoptedNode {
    pub handle: PluginNodeHandle,
    /// Stable provider-side address of the resource (docker container name,
    /// vastai lease label, ...) for snapshot bookkeeping.
    pub provider_ref: String,
}

pub trait PluginObservationSink: Send + Sync {
    fn observe(&self, observation: PluginObservation);
}

#[derive(Clone)]
pub struct PluginSink {
    inner: Arc<dyn PluginObservationSink>,
}

impl PluginSink {
    pub fn new(inner: Arc<dyn PluginObservationSink>) -> Self {
        Self { inner }
    }

    pub fn observe(&self, observation: PluginObservation) {
        self.inner.observe(observation);
    }
}

pub trait ProvisionPlugin: Send {
    /// Acquires or adopts the provider resource for one concrete node attempt.
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String>;

    /// Creates one concrete node. A selected offer is authoritative: providers
    /// must either create exactly it or fail without substitution.
    fn create_node_selected(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
        selected_offer_id: Option<u64>,
    ) -> Result<PluginNodeHandle, String> {
        if let Some(offer_id) = selected_offer_id {
            return Err(format!(
                "provider does not support exact offer selection {offer_id}"
            ));
        }
        self.create_node(spec, sink)
    }

    /// Starts or adopts bootstrap work on an already-created provider resource.
    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String>;

    fn cancel_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String>;

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String>;
    fn adopt_by_spec(
        &mut self,
        _spec: &NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        Ok(None)
    }
    /// Recreates only the provider-local bootstrap handle when durable intent
    /// was persisted before the provider resource was started. This must not
    /// allocate, rent, or otherwise create a remote resource.
    fn prepare_missing_bootstrap(
        &mut self,
        _spec: &NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        Ok(None)
    }

    /// Stable provider-side address for a spec (docker container name, vastai
    /// lease label, ...). Derivable without provider state.
    fn provider_ref_for(&self, _spec: &NodeProvisionSpec) -> String {
        format!("node-{}-{}", _spec.run_id, _spec.node_id)
    }

    /// Lists provider resources carrying this daemon's label. Used for orphan
    /// detection; providers without a label sweep return an empty list.
    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        Ok(Vec::new())
    }

    /// Stops a provider resource addressed by its provision spec. Returns
    /// `Ok(false)` when nothing matching exists (already stopped is success).
    fn stop_by_spec(
        &mut self,
        _spec: &NodeProvisionSpec,
        _sink: PluginSink,
    ) -> Result<bool, String> {
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        observations: Mutex<Vec<PluginObservation>>,
    }

    impl PluginObservationSink for RecordingSink {
        fn observe(&self, observation: PluginObservation) {
            self.observations.lock().unwrap().push(observation);
        }
    }

    #[test]
    fn mounted_node_specs_round_trip_without_losing_readonly_intent() {
        let spec = NodeProvisionSpec {
            run_id: 17,
            node_id: 23,
            attempt_id: 7,
            stage_index: Some(2),
            image: "runtime:latest".to_owned(),
            env: vec![("A".to_owned(), "B".to_owned())],
            args: vec!["--join".to_owned()],
            mounts: vec![ProviderMount {
                host_path: "/cache/model.gguf".to_owned(),
                container_path: "/models/model.gguf".to_owned(),
                readonly: true,
            }],
        };

        let json = serde_json::to_string(&spec).expect("spec serializes");
        let decoded: NodeProvisionSpec = serde_json::from_str(&json).expect("spec decodes");

        assert_eq!(decoded, spec);
    }

    #[test]
    fn missing_mounts_decode_as_empty_for_older_specs() {
        let decoded: NodeProvisionSpec = serde_json::from_value(serde_json::json!({
            "run_id": 17,
            "node_id": 23,
            "stage_index": 2,
            "image": "runtime:latest",
            "env": [],
            "args": []
        }))
        .expect("legacy spec decodes");

        assert!(decoded.mounts.is_empty());
        assert_eq!(decoded.attempt_id, 0);
    }

    #[test]
    fn plugin_sink_fans_out_typed_observations() {
        let recorder = Arc::new(RecordingSink::default());
        let sink = PluginSink::new(recorder.clone());

        sink.observe(PluginObservation::ProviderLine {
            run_id: 1,
            node_id: 2,
            line: "booting".to_owned(),
        });

        assert_eq!(
            recorder.observations.lock().unwrap().as_slice(),
            &[PluginObservation::ProviderLine {
                run_id: 1,
                node_id: 2,
                line: "booting".to_owned(),
            }]
        );
    }
}
