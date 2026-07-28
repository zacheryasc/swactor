use std::sync::Arc;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeProvisionSpec {
    pub run_id: u64,
    pub node_id: u64,
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
    DatastreamFrame {
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
    fn start_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String>;

    fn start_nodes(
        &mut self,
        specs: Vec<NodeProvisionSpec>,
        sink: PluginSink,
    ) -> Vec<(NodeProvisionSpec, Result<PluginNodeHandle, String>)> {
        specs
            .into_iter()
            .map(|spec| {
                let result = self.start_node(spec.clone(), sink.clone());
                (spec, result)
            })
            .collect()
    }

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String>;

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String>;
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
