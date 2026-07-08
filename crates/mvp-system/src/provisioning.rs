//! Minimal provisioning model and local Docker plugin for the MVP system.
//!
//! Swactor actors own provisioning control. Plugins only perform concrete I/O and
//! report observations back to the provisioner actor through [`PluginSink`].

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::Path;
use std::process::{ChildStdin, Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::UNIX_EPOCH;

use serde::{Deserialize, Serialize};

use crate::bootstrap_datastream::BootstrapDatastreamBridge;

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

    fn complete_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String>;

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

fn docker_mount_arg(mount: &ProviderMount) -> String {
    let mut arg = format!(
        "type=bind,src={},dst={}",
        mount.host_path, mount.container_path
    );
    if mount.readonly {
        arg.push_str(",readonly");
    }
    arg
}

fn docker_runtime_mount_arg(image: &str, mount: &ProviderMount) -> Result<String, String> {
    let host_path = Path::new(&mount.host_path);
    if host_path.is_file() {
        return prepare_docker_file_volume(image, mount, host_path);
    }
    Ok(docker_mount_arg(mount))
}

fn prepare_docker_file_volume(
    image: &str,
    mount: &ProviderMount,
    host_path: &Path,
) -> Result<String, String> {
    let container_path = Path::new(&mount.container_path);
    let container_dir = container_path
        .parent()
        .and_then(|path| path.to_str())
        .filter(|path| !path.is_empty())
        .ok_or_else(|| {
            format!(
                "cached model container path has no parent: {}",
                mount.container_path
            )
        })?;
    let file_name = container_path
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .ok_or_else(|| {
            format!(
                "cached model container path has no file name: {}",
                mount.container_path
            )
        })?;
    let volume = docker_file_cache_volume_name(host_path)?;
    docker_status(
        &["volume", "create", &volume],
        "create cached model docker volume",
    )?;
    let loader_name = format!("mvp-cache-load-{}-{volume}", std::process::id());
    let _ = Command::new("docker")
        .args(["rm", "-f", &loader_name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    docker_status_vec(
        vec![
            "create".to_owned(),
            "--name".to_owned(),
            loader_name.clone(),
            "--mount".to_owned(),
            format!("type=volume,src={volume},dst={container_dir}"),
            "--entrypoint".to_owned(),
            "/bin/sh".to_owned(),
            image.to_owned(),
            "-c".to_owned(),
            "true".to_owned(),
        ],
        "create cached model loader container",
    )?;
    let copy_result = docker_status_vec(
        vec![
            "cp".to_owned(),
            host_path.to_string_lossy().to_string(),
            format!("{loader_name}:{container_dir}/{file_name}"),
        ],
        "copy cached model into docker volume",
    );
    let _ = Command::new("docker")
        .args(["rm", &loader_name])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    copy_result?;

    // tinygrad opens GGUF files read-write even when it does not intend to mutate
    // the original artifact. The Docker volume is a copied cache, so keeping the
    // runtime mount writable preserves the host cache while satisfying the loader.
    Ok(format!("type=volume,src={volume},dst={container_dir}"))
}

fn docker_file_cache_volume_name(path: &Path) -> Result<String, String> {
    let metadata = fs::metadata(path).map_err(|e| format!("stat {}: {e}", path.display()))?;
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or(0);
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(safe_docker_volume_component)
        .unwrap_or_else(|| "model".to_owned());
    Ok(format!("mvp-cache-{file}-{}-{modified}", metadata.len()))
}

fn safe_docker_volume_component(value: &str) -> String {
    let mut out = String::with_capacity(value.len().min(40));
    for ch in value.chars() {
        if out.len() >= 40 {
            break;
        }
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
        } else if !out.ends_with('-') {
            out.push('-');
        }
    }
    let trimmed = out.trim_matches('-');
    if trimmed.is_empty() {
        "model".to_owned()
    } else {
        trimmed.to_owned()
    }
}

fn docker_status(args: &[&str], label: &str) -> Result<(), String> {
    let status = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("{label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with {status}"))
    }
}

fn docker_status_vec(args: Vec<String>, label: &str) -> Result<(), String> {
    let status = Command::new("docker")
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("{label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with {status}"))
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
        let docker_gpus = spec
            .env
            .iter()
            .find(|(key, _)| key == "MVP_DOCKER_GPUS")
            .map(|(_, value)| value.clone())
            .or_else(|| std::env::var("MVP_DOCKER_GPUS").ok())
            .filter(|value| !value.trim().is_empty());
        sink.observe(PluginObservation::DatastreamFrame {
            run_id: spec.run_id,
            node_id: spec.node_id,
            channel: "mvp.node.bootstrap".to_owned(),
            payload: serde_json::json!({
                "type":"DockerGpuConfigResolved",
                "provider":"Docker",
                "gpus_arg":docker_gpus.as_deref(),
                "has_gpus_arg":docker_gpus.is_some(),
            })
            .to_string(),
        });
        if let Some(gpus) = docker_gpus.as_deref() {
            command.arg("--gpus").arg(gpus);
        }
        for mount in &spec.mounts {
            command
                .arg("--mount")
                .arg(docker_runtime_mount_arg(&spec.image, mount)?);
        }
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

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
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

fn spawn_stdout_reader(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stdout: impl std::io::Read + Send + 'static,
) {
    BootstrapDatastreamBridge::new(spec, sink, None).spawn_stdout_reader(stdout);
}

fn spawn_stderr_reader(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stderr: impl std::io::Read + Send + 'static,
) {
    BootstrapDatastreamBridge::new(spec, sink, None).spawn_stderr_reader(stderr);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec_with_mounts(mounts: Vec<ProviderMount>) -> NodeProvisionSpec {
        NodeProvisionSpec {
            run_id: 17,
            node_id: 23,
            stage_index: Some(2),
            image: "swactor-mvp-node:test".to_owned(),
            env: vec![("MVP_RUN_ID".to_owned(), "17".to_owned())],
            args: vec!["--serve".to_owned()],
            mounts,
        }
    }

    #[test]
    fn node_provision_spec_serde_preserves_readonly_mounts() {
        let spec = spec_with_mounts(vec![ProviderMount {
            host_path: "/cache/models/model.gguf".to_owned(),
            container_path: "/models/cached/model.gguf".to_owned(),
            readonly: true,
        }]);

        let json = serde_json::to_string(&spec).expect("serialize mounted node spec");
        let decoded: NodeProvisionSpec =
            serde_json::from_str(&json).expect("deserialize mounted node spec");

        assert_eq!(decoded, spec);
    }

    #[test]
    fn node_provision_spec_serde_omits_empty_mounts_and_accepts_missing_mounts() {
        let spec = spec_with_mounts(Vec::new());

        let json = serde_json::to_value(&spec).expect("serialize unmounted node spec");
        assert_eq!(json.get("mounts"), None);

        let decoded: NodeProvisionSpec = serde_json::from_value(serde_json::json!({
            "run_id": 17,
            "node_id": 23,
            "stage_index": 2,
            "image": "swactor-mvp-node:test",
            "env": [["MVP_RUN_ID", "17"]],
            "args": ["--serve"]
        }))
        .expect("deserialize node spec written before mounts existed");

        assert!(decoded.mounts.is_empty());
    }

    #[test]
    fn docker_mount_arg_uses_bind_src_dst_and_readonly_flag() {
        let mount = ProviderMount {
            host_path: "/cache/models/model.gguf".to_owned(),
            container_path: "/models/cached/model.gguf".to_owned(),
            readonly: true,
        };

        assert_eq!(
            docker_mount_arg(&mount),
            "type=bind,src=/cache/models/model.gguf,dst=/models/cached/model.gguf,readonly"
        );

        let writable_mount = ProviderMount {
            readonly: false,
            ..mount
        };
        assert_eq!(
            docker_mount_arg(&writable_mount),
            "type=bind,src=/cache/models/model.gguf,dst=/models/cached/model.gguf"
        );
    }
}
