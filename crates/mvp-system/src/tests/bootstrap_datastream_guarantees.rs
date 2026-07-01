use std::sync::Arc;

use datastream::{DatastreamEndpoint, Record};
use iroh::{EndpointAddr, SecretKey};
use mvp_system::bootstrap_datastream::{BootstrapDatastreamBridge, node_stream_id};
use mvp_system::provisioning::{
    NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink, ProvisionLogStream,
};
use mvp_system::telemetry::MvpProvisionLogRecord;
use parking_lot::Mutex;
use serde_json::json;
use swactor::actor::ActorAddress;

#[derive(Default)]
struct RecordingSink {
    observations: Mutex<Vec<PluginObservation>>,
}

impl RecordingSink {
    fn observations(&self) -> Vec<PluginObservation> {
        self.observations.lock().clone()
    }
}

impl PluginObservationSink for RecordingSink {
    fn observe(&self, observation: PluginObservation) {
        self.observations.lock().push(observation);
    }
}

fn recording_sink() -> (Arc<RecordingSink>, PluginSink) {
    let recording = Arc::new(RecordingSink::default());
    (recording.clone(), PluginSink::new(recording))
}

fn spec() -> NodeProvisionSpec {
    NodeProvisionSpec {
        run_id: 7,
        node_id: 42,
        stage_index: Some(3),
        image: "worker:latest".to_owned(),
        env: Vec::new(),
        args: Vec::new(),
    }
}

#[test]
fn bootstrap_bridge_writes_node_stream_and_forwards_plugin_observations() {
    let endpoint = DatastreamEndpoint::new(node_stream_id(7, 42));
    let subscription = endpoint.subscribe_all("test");
    let (recording, sink) = recording_sink();
    let bridge = BootstrapDatastreamBridge::new(spec(), sink, Some(endpoint.producer()));

    bridge.observe_stdout_line("boot entered");
    bridge.observe_stderr_line("warning");
    endpoint.tick();

    let observations = recording.observations();
    assert_eq!(
        observations,
        vec![
            PluginObservation::StdoutLine {
                run_id: 7,
                node_id: 42,
                line: "boot entered".to_owned(),
            },
            PluginObservation::StderrLine {
                run_id: 7,
                node_id: 42,
                line: "warning".to_owned(),
            },
        ]
    );

    let deliveries = subscription.drain_available();
    assert_eq!(deliveries.len(), 2);
    assert_eq!(deliveries[0].stream, node_stream_id(7, 42));
    assert_eq!(deliveries[1].stream, node_stream_id(7, 42));

    let stdout = MvpProvisionLogRecord::decode(&deliveries[0].frame.payload).unwrap();
    let stderr = MvpProvisionLogRecord::decode(&deliveries[1].frame.payload).unwrap();
    assert_eq!(stdout.line.stream, ProvisionLogStream::Stdout);
    assert_eq!(stdout.line.line, "boot entered");
    assert_eq!(stderr.line.stream, ProvisionLogStream::Stderr);
    assert_eq!(stderr.line.line, "warning");
}

#[test]
fn ready_json_on_stdout_emits_runtime_ready_through_plugin_sink() {
    let (recording, sink) = recording_sink();
    let bridge = BootstrapDatastreamBridge::new(spec(), sink, None);
    let endpoint = EndpointAddr::new(SecretKey::from_bytes(&[7; 32]).public());
    let node_actor = ActorAddress::new_random();
    let line = serde_json::to_string(&json!({
        "type": "ready",
        "endpoint": endpoint,
        "node_actor": node_actor,
        "logical_node_id": 42,
        "stage_index": 3,
    }))
    .unwrap();

    bridge.observe_stdout_line(line);

    assert!(recording.observations().iter().any(|observation| matches!(
        observation,
        PluginObservation::RuntimeReady {
            run_id: 7,
            node_id: 42,
            stage_index: Some(3),
            endpoint: observed_endpoint,
            node_actor: observed_actor,
        } if observed_endpoint == &endpoint && observed_actor == &node_actor
    )));
}
