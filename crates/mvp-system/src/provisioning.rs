//! Minimal provisioning model and local Docker plugin for the MVP system.
//!
//! Swactor actors own provisioning control. Plugins only perform concrete I/O and
//! report observations back to the provisioner actor through [`PluginSink`].

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::process::{ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::thread;

use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeProvisionSpec {
    pub run_id: u64,
    pub node_id: u64,
    pub stage_index: Option<u32>,
    pub image: String,
    pub env: Vec<(String, String)>,
    pub args: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProvisionEvent {
    pub run_id: u64,
    pub node_id: u64,
    pub kind: ProvisionEventKind,
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
    ProviderLine {
        run_id: u64,
        node_id: u64,
        line: String,
    },
    RuntimeReady {
        run_id: u64,
        node_id: u64,
        stage_index: Option<u32>,
        endpoint: EndpointAddr,
        node_actor: ActorAddress,
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

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String>;
}

pub struct LocalDockerPlugin {
    container_name_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalDockerNode>,
}

struct LocalDockerNode {
    container_name: String,
    stdin: ChildStdin,
}

impl LocalDockerPlugin {
    pub fn new(container_name_prefix: impl Into<String>) -> Self {
        Self {
            container_name_prefix: container_name_prefix.into(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
        }
    }
}

impl ProvisionPlugin for LocalDockerPlugin {
    fn start_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let container_name = format!(
            "{}-{}-{}",
            self.container_name_prefix, spec.run_id, spec.node_id
        );
        let mut command = Command::new("docker");
        command
            .arg("run")
            .arg("--rm")
            .arg("--add-host")
            .arg("host.docker.internal:host-gateway")
            .arg("--name")
            .arg(&container_name)
            .arg("-i");
        for (key, value) in &spec.env {
            command.arg("-e").arg(format!("{key}={value}"));
        }
        command.arg(&spec.image);
        for arg in &spec.args {
            command.arg(arg);
        }
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|e| format!("spawn Docker node {}: {e}", spec.node_id))?;

        let provider_process_id = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| format!("Docker node {} stdin missing", spec.node_id))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| format!("Docker node {} stdout missing", spec.node_id))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| format!("Docker node {} stderr missing", spec.node_id))?;

        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: Some(provider_process_id),
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle.id,
            LocalDockerNode {
                container_name: container_name.clone(),
                stdin,
            },
        );

        spawn_stdout_reader(spec.clone(), sink.clone(), stdout);
        spawn_stderr_reader(spec.clone(), sink.clone(), stderr);
        thread::spawn(move || match child.wait() {
            Ok(status) => sink.observe(PluginObservation::Exited {
                run_id: spec.run_id,
                node_id: spec.node_id,
                status: status.code(),
            }),
            Err(error) => sink.observe(PluginObservation::Failed {
                run_id: spec.run_id,
                node_id: spec.node_id,
                reason: format!("wait Docker node: {error}"),
            }),
        });

        Ok(handle)
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(mut node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        let _ = writeln!(node.stdin, "shutdown");
        let _ = node.stdin.flush();
        let status = Command::new("docker")
            .arg("stop")
            .arg("-t")
            .arg("2")
            .arg(&node.container_name)
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|e| format!("docker stop {}: {e}", node.container_name))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!(
                "docker stop {} exited with {status}",
                node.container_name
            ))
        }
    }
}

#[derive(Deserialize)]
struct RuntimeReadyLine {
    #[serde(rename = "type")]
    kind: String,
    endpoint: EndpointAddr,
    node_actor: ActorAddress,
    logical_node_id: u64,
    stage_index: u32,
}

fn spawn_stdout_reader(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stdout: impl std::io::Read + Send + 'static,
) {
    thread::spawn(move || {
        let reader = BufReader::new(stdout);
        for next in reader.lines() {
            match next {
                Ok(line) => {
                    sink.observe(PluginObservation::StdoutLine {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        line: line.clone(),
                    });
                    if let Ok(ready) = serde_json::from_str::<RuntimeReadyLine>(&line) {
                        if ready.kind == "ready" && ready.logical_node_id == spec.node_id {
                            sink.observe(PluginObservation::RuntimeReady {
                                run_id: spec.run_id,
                                node_id: spec.node_id,
                                stage_index: Some(ready.stage_index),
                                endpoint: ready.endpoint,
                                node_actor: ready.node_actor,
                            });
                        }
                    }
                }
                Err(error) => {
                    sink.observe(PluginObservation::Failed {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        reason: format!("read stdout: {error}"),
                    });
                    break;
                }
            }
        }
    });
}

fn spawn_stderr_reader(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stderr: impl std::io::Read + Send + 'static,
) {
    thread::spawn(move || {
        let reader = BufReader::new(stderr);
        for next in reader.lines() {
            match next {
                Ok(line) => sink.observe(PluginObservation::StderrLine {
                    run_id: spec.run_id,
                    node_id: spec.node_id,
                    line,
                }),
                Err(error) => {
                    sink.observe(PluginObservation::Failed {
                        run_id: spec.run_id,
                        node_id: spec.node_id,
                        reason: format!("read stderr: {error}"),
                    });
                    break;
                }
            }
        }
    });
}
