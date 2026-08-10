//! Myelin-local provisioning provider plugins.
//!
//! Provider-neutral lifecycle contracts live in the reusable `provisioning`
//! crate. This module keeps Myelin-local process/Docker plugin implementations
//! that know about bootstrap datastream plumbing and local runtime execution.

use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, UNIX_EPOCH};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

pub use ::provisioning::plugin::{
    NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginObservationSink, PluginSink,
    ProviderMount, ProvisionEvent, ProvisionEventKind, ProvisionLogLine, ProvisionLogStream,
    ProvisionPlugin,
};

use crate::observability::provisioning_logs::BootstrapDatastreamBridge;

pub(crate) struct LocalDockerPlugin {
    container_name_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalDockerNode>,
}

struct LocalDockerNode {
    container_name: String,
    stdin: ChildStdin,
}

pub(crate) struct LocalProcessPlugin {
    program: PathBuf,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalProcessNode>,
}

struct LocalProcessNode {
    stdin: ChildStdin,
    child: Arc<Mutex<Option<Child>>>,
    spec: NodeProvisionSpec,
    sink: PluginSink,
}

impl LocalProcessPlugin {
    pub(crate) fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
        }
    }
}

impl LocalDockerPlugin {
    pub(crate) fn new(container_name_prefix: impl Into<String>) -> Self {
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
    let loader_name = format!("myelin-cache-load-{}-{volume}", std::process::id());
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
    Ok(format!("myelin-cache-{file}-{}-{modified}", metadata.len()))
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

fn lock_process_child(
    child: &Arc<Mutex<Option<Child>>>,
) -> std::sync::MutexGuard<'_, Option<Child>> {
    child
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl ProvisionPlugin for LocalProcessPlugin {
    // provider process supervision/lifecycle is out of scope (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn start_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let mut command = Command::new(&self.program);
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        command.args(&spec.args);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(target_os = "linux")]
        unsafe {
            command.pre_exec(|| {
                if libc::setpgid(0, 0) == 0 {
                    Ok(())
                } else {
                    Err(std::io::Error::last_os_error())
                }
            });
        }

        let mut child = command.spawn().map_err(|e| {
            format!(
                "spawn local process node {} with {}: {e}",
                spec.node_id,
                self.program.display()
            )
        })?;
        let provider_process_id = child.id();
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| format!("local process node {} stdin missing", spec.node_id))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| format!("local process node {} stdout missing", spec.node_id))?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| format!("local process node {} stderr missing", spec.node_id))?;

        let child = Arc::new(Mutex::new(Some(child)));
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: Some(provider_process_id),
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle.id,
            LocalProcessNode {
                stdin,
                child: Arc::clone(&child),
                spec: spec.clone(),
                sink: sink.clone(),
            },
        );

        spawn_stdout_reader(spec.clone(), sink.clone(), stdout);
        spawn_stderr_reader(spec.clone(), sink.clone(), stderr);
        thread::spawn(move || {
            loop {
                let observation = {
                    let mut slot = lock_process_child(&child);
                    let Some(child) = slot.as_mut() else {
                        return;
                    };
                    match child.try_wait() {
                        Ok(Some(status)) => {
                            *slot = None;
                            Some(PluginObservation::Exited {
                                run_id: spec.run_id,
                                node_id: spec.node_id,
                                status: status.code(),
                            })
                        }
                        Ok(None) => None,
                        Err(error) => {
                            *slot = None;
                            Some(PluginObservation::Failed {
                                run_id: spec.run_id,
                                node_id: spec.node_id,
                                reason: format!("wait local process node: {error}"),
                            })
                        }
                    }
                };
                if let Some(observation) = observation {
                    sink.observe(observation);
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        });

        Ok(handle)
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    // provider process supervision/lifecycle is out of scope (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(mut node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        let _ = node.stdin.write_all(b"shutdown\n");
        let _ = node.stdin.flush();

        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            let observation = {
                let mut slot = lock_process_child(&node.child);
                let Some(child) = slot.as_mut() else {
                    return Ok(());
                };
                match child.try_wait() {
                    Ok(Some(status)) => {
                        *slot = None;
                        Some(PluginObservation::Exited {
                            run_id: node.spec.run_id,
                            node_id: node.spec.node_id,
                            status: status.code(),
                        })
                    }
                    Ok(None) => None,
                    Err(error) => {
                        *slot = None;
                        Some(PluginObservation::Failed {
                            run_id: node.spec.run_id,
                            node_id: node.spec.node_id,
                            reason: format!("wait local process node: {error}"),
                        })
                    }
                }
            };
            if let Some(observation) = observation {
                node.sink.observe(observation);
                return Ok(());
            }
            thread::sleep(Duration::from_millis(50));
        }

        let observation = {
            let mut slot = lock_process_child(&node.child);
            let Some(child) = slot.as_mut() else {
                return Ok(());
            };
            #[cfg(target_os = "linux")]
            unsafe {
                let _ = libc::kill(-(child.id() as i32), libc::SIGKILL);
            }
            let _ = child.kill();
            match child.wait() {
                Ok(status) => {
                    *slot = None;
                    PluginObservation::Exited {
                        run_id: node.spec.run_id,
                        node_id: node.spec.node_id,
                        status: status.code(),
                    }
                }
                Err(error) => {
                    *slot = None;
                    PluginObservation::Failed {
                        run_id: node.spec.run_id,
                        node_id: node.spec.node_id,
                        reason: format!("kill local process node: {error}"),
                    }
                }
            }
        };
        node.sink.observe(observation);
        Ok(())
    }
}

impl Drop for LocalProcessPlugin {
    fn drop(&mut self) {
        let handles = self
            .nodes
            .keys()
            .copied()
            .map(|id| PluginNodeHandle {
                id,
                provider_process_id: None,
            })
            .collect::<Vec<_>>();
        for handle in handles {
            let _ = self.stop_node(&handle);
        }
    }
}

impl ProvisionPlugin for LocalDockerPlugin {
    // provider process supervision/lifecycle is out of scope (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
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
            .find(|(key, _)| key == "MYELIN_DOCKER_GPUS")
            .map(|(_, value)| value.clone())
            .or_else(|| std::env::var("MYELIN_DOCKER_GPUS").ok())
            .filter(|value| !value.trim().is_empty());
        sink.observe(PluginObservation::DatastreamFrame {
            run_id: spec.run_id,
            node_id: spec.node_id,
            channel: "myelin.provisioning.events".to_owned(),
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
