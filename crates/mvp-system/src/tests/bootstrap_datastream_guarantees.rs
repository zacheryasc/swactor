use std::sync::Arc;

use datastream::{DatastreamEndpoint, Record};
use iroh::{EndpointAddr, SecretKey};
use mvp_system::observability::provisioning_logs::{BootstrapDatastreamBridge, node_stream_id};
use mvp_system::observability::telemetry::MvpProvisionLogRecord;
use mvp_system::orchestration::provisioning::{
    NodeProvisionSpec, PluginObservation, PluginObservationSink, PluginSink, ProvisionLogStream,
};
use parking_lot::Mutex;
use serde_json::json;

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
        mounts: Vec::new(),
    }
}

#[test]
fn bootstrap_bridge_writes_node_stream_and_forwards_plugin_observations() {
    let endpoint = DatastreamEndpoint::new(node_stream_id(7, 42));
    let subscription = endpoint.subscribe_all("test");
    let (recording, sink) = recording_sink();
    let bridge = BootstrapDatastreamBridge::new(spec(), sink, Some(endpoint.producer()));

    bridge.observe_stdout_line("ssh stdout diagnostic");
    bridge.observe_stderr_line("debug1: ssh stderr diagnostic");
    endpoint.tick();

    let observations = recording.observations();
    assert_eq!(
        observations,
        vec![
            PluginObservation::StdoutLine {
                run_id: 7,
                node_id: 42,
                line: "ssh stdout diagnostic".to_owned(),
            },
            PluginObservation::StderrLine {
                run_id: 7,
                node_id: 42,
                line: "debug1: ssh stderr diagnostic".to_owned(),
            },
        ]
    );

    let deliveries = subscription.drain_available();
    let logs = deliveries
        .iter()
        .filter_map(|event| match event {
            datastream::DatastreamEvent::Frame(delivery)
                if delivery.channel.stream == node_stream_id(7, 42) =>
            {
                MvpProvisionLogRecord::decode(&delivery.payload).ok()
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(logs.len(), 2, "{deliveries:?}");

    let stdout = &logs[0];
    let stderr = &logs[1];
    assert_eq!(stdout.line.stream, ProvisionLogStream::Stdout);
    assert_eq!(stdout.line.line, "ssh stdout diagnostic");
    assert_eq!(stderr.line.stream, ProvisionLogStream::Stderr);
    assert_eq!(stderr.line.line, "debug1: ssh stderr diagnostic");
}

#[test]
fn stdout_ready_json_is_log_only() {
    let (recording, sink) = recording_sink();
    let bridge = BootstrapDatastreamBridge::new(spec(), sink, None);
    let line = serde_json::to_string(&json!({
        "type": "ready",
        "endpoint": EndpointAddr::new(SecretKey::from_bytes(&[7; 32]).public()),
        "node_actor": "ignored-by-stdout-bridge",
        "logical_node_id": 42,
        "stage_index": 3,
    }))
    .unwrap();

    bridge.observe_stdout_line(line.clone());

    assert_eq!(
        recording.observations(),
        vec![PluginObservation::StdoutLine {
            run_id: 7,
            node_id: 42,
            line,
        }]
    );
}
