#![allow(dead_code)]

use std::io::{BufRead, BufReader, Read};
use std::thread::{self, JoinHandle};

use datastream::{ChannelContent, DatastreamProducer, Lifetime, NodeId, StreamId};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::observability::telemetry::{
    MVP_PROVISIONING_LOGS, MvpProvisionLogRecord, mvp_provision_log_channel,
};
use crate::provisioning::{
    NodeProvisionSpec, PluginObservation, PluginSink, ProvisionLogLine, ProvisionLogStream,
};

pub fn node_datastream_id(node_id: u64) -> String {
    node_id.to_string()
}

pub fn node_stream_id(run_id: u64, node_id: u64) -> StreamId {
    StreamId::new(NodeId::new(&node_datastream_id(node_id)), Lifetime(run_id))
}

#[derive(Clone)]
pub struct BootstrapDatastreamBridge {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    producer: Option<DatastreamProducer>,
}

impl BootstrapDatastreamBridge {
    pub fn new(
        spec: NodeProvisionSpec,
        sink: PluginSink,
        producer: Option<DatastreamProducer>,
    ) -> Self {
        Self {
            spec,
            sink,
            producer,
        }
    }

    pub fn spec(&self) -> &NodeProvisionSpec {
        &self.spec
    }

    pub fn stream_id(&self) -> StreamId {
        node_stream_id(self.spec.run_id, self.spec.node_id)
    }

    pub fn observe_stdout_line(&self, line: impl Into<String>) {
        let line = line.into();
        if let Some(frame) = parse_stdio_datastream_frame(&self.spec, &line) {
            self.sink.observe(frame);
            return;
        }
        self.submit_log(ProvisionLogStream::Stdout, &line);
        self.sink.observe(PluginObservation::StdoutLine {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            line: line.clone(),
        });
    }

    pub fn observe_stderr_line(&self, line: impl Into<String>) {
        let line = line.into();
        self.submit_log(ProvisionLogStream::Stderr, &line);
        self.sink.observe(PluginObservation::StderrLine {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            line,
        });
    }

    pub fn observe_provider_line(&self, line: impl Into<String>) {
        let line = line.into();
        self.submit_log(ProvisionLogStream::Provider, &line);
        self.sink.observe(PluginObservation::ProviderLine {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            line,
        });
    }

    pub fn spawn_stdout_reader<R>(&self, stdout: R) -> JoinHandle<()>
    where
        R: Read + Send + 'static,
    {
        let bridge = self.clone();
        thread::spawn(move || bridge.read_stdout(stdout))
    }

    pub fn spawn_stderr_reader<R>(&self, stderr: R) -> JoinHandle<()>
    where
        R: Read + Send + 'static,
    {
        let bridge = self.clone();
        thread::spawn(move || bridge.read_stderr(stderr))
    }

    fn read_stdout<R>(&self, stdout: R)
    where
        R: Read,
    {
        let reader = BufReader::new(stdout);
        for next in reader.lines() {
            match next {
                Ok(line) => self.observe_stdout_line(line),
                Err(error) => {
                    self.sink.observe(PluginObservation::Failed {
                        run_id: self.spec.run_id,
                        node_id: self.spec.node_id,
                        reason: format!("read stdout: {error}"),
                    });
                    break;
                }
            }
        }
    }

    fn read_stderr<R>(&self, stderr: R)
    where
        R: Read,
    {
        let reader = BufReader::new(stderr);
        for next in reader.lines() {
            match next {
                Ok(line) => self.observe_stderr_line(line),
                Err(error) => {
                    self.sink.observe(PluginObservation::Failed {
                        run_id: self.spec.run_id,
                        node_id: self.spec.node_id,
                        reason: format!("read stderr: {error}"),
                    });
                    break;
                }
            }
        }
    }

    fn submit_log(&self, stream: ProvisionLogStream, line: &str) {
        let Some(producer) = &self.producer else {
            return;
        };
        let record = MvpProvisionLogRecord::new(ProvisionLogLine {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            stream,
            line: line.to_owned(),
        });
        let channel = producer.register_channel(
            mvp_provision_log_channel(self.spec.node_id, stream),
            ChannelContent::JsonRecord {
                schema: Some(MVP_PROVISIONING_LOGS.to_owned()),
            },
        );
        let payload = serde_json::to_vec(&record).expect("serialize bootstrap log record");
        producer.submit_bytes(channel, payload);
    }
}

#[derive(Deserialize, Serialize)]
struct StdioDatastreamFrame {
    mvp_stdio_event: u32,
    kind: String,
    channel: String,
    payload: Value,
}

pub fn parse_stdio_datastream_frame(
    spec: &NodeProvisionSpec,
    line: &str,
) -> Option<PluginObservation> {
    let frame = serde_json::from_str::<StdioDatastreamFrame>(line).ok()?;
    if frame.mvp_stdio_event != 1 || frame.kind != "datastream_frame" {
        return None;
    }
    Some(PluginObservation::DatastreamFrame {
        run_id: spec.run_id,
        node_id: spec.node_id,
        channel: frame.channel,
        payload: frame.payload.to_string(),
    })
}

pub fn bootstrap_log_channel(node_id: u64, stream: ProvisionLogStream) -> String {
    mvp_provision_log_channel(node_id, stream)
}
