//! MVP-local provisioning provider plugins.
//!
//! Provider-neutral lifecycle contracts live in the reusable `provisioning`
//! crate. This module keeps MVP-local process/Docker plugin implementations
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

pub struct LocalDockerPlugin {
    container_name_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalDockerNode>,
}

struct LocalDockerNode {
    container_name: String,
    stdin: ChildStdin,
}

pub struct LocalProcessPlugin {
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
    pub fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
        }
    }
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

fn lock_process_child(
    child: &Arc<Mutex<Option<Child>>>,
) -> std::sync::MutexGuard<'_, Option<Child>> {
    child
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

impl ProvisionPlugin for LocalProcessPlugin {
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
    use std::sync::Mutex;
    use std::time::{Duration, Instant};

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    #[derive(Default)]
    struct RecordingSink {
        observations: Mutex<Vec<PluginObservation>>,
    }

    impl RecordingSink {
        fn observations(&self) -> Vec<PluginObservation> {
            self.observations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }
    }

    impl PluginObservationSink for RecordingSink {
        fn observe(&self, observation: PluginObservation) {
            self.observations
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(observation);
        }
    }

    fn recording_sink() -> (Arc<RecordingSink>, PluginSink) {
        let recorder = Arc::new(RecordingSink::default());
        (Arc::clone(&recorder), PluginSink::new(recorder))
    }

    fn wait_for_observation(
        recorder: &RecordingSink,
        predicate: impl Fn(&PluginObservation) -> bool,
    ) -> PluginObservation {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(observation) = recorder
                .observations()
                .into_iter()
                .find(|observation| predicate(observation))
            {
                return observation;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for observation; observed: {:?}",
                recorder.observations()
            );
            thread::sleep(Duration::from_millis(20));
        }
    }

    fn process_spec(node_id: u64, args: Vec<String>) -> NodeProvisionSpec {
        NodeProvisionSpec {
            run_id: 17,
            node_id,
            stage_index: Some(2),
            image: "unused-for-local-process".to_owned(),
            env: vec![("MVP_RUN_ID".to_owned(), "17".to_owned())],
            args,
            mounts: Vec::new(),
        }
    }

    #[cfg(unix)]
    struct TempScript {
        root: PathBuf,
        path: PathBuf,
    }

    #[cfg(unix)]
    impl TempScript {
        fn new(name: &str, content: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "mvp-local-process-plugin-{name}-{}-{}",
                std::process::id(),
                thread::current().name().unwrap_or("unnamed")
            ));
            let _ = fs::remove_dir_all(&root);
            fs::create_dir_all(&root).expect("create temp script directory");
            let path = root.join("helper.sh");
            fs::write(&path, content).expect("write local process helper");
            let mut permissions = fs::metadata(&path)
                .expect("stat local process helper")
                .permissions();
            permissions.set_mode(0o755);
            fs::set_permissions(&path, permissions).expect("chmod local process helper");
            Self { root, path }
        }
    }

    #[cfg(unix)]
    impl Drop for TempScript {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    #[cfg(unix)]
    fn process_alive(pid: u32) -> bool {
        unsafe {
            if libc::kill(pid as i32, 0) == 0 {
                true
            } else {
                std::io::Error::last_os_error().raw_os_error() != Some(libc::ESRCH)
            }
        }
    }

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

    #[cfg(unix)]
    #[test]
    fn local_process_plugin_observes_stdio_frames_and_graceful_shutdown() {
        let helper = TempScript::new(
            "graceful",
            r#"#!/bin/sh
printf '%s\n' '{"mvp_stdio_event":1,"kind":"datastream_frame","channel":"mvp.node.bootstrap","payload":{"type":"TestFrame","status":"ready"}}'
printf '%s\n' 'local-process-stderr' >&2
while IFS= read -r line; do
    if [ "$line" = shutdown ]; then
        exit 0
    fi
done
exit 0
"#,
        );
        let mut plugin = LocalProcessPlugin::new("/bin/sh");
        let (recorder, sink) = recording_sink();
        let spec = process_spec(23, vec![helper.path.to_string_lossy().to_string()]);

        let handle = plugin
            .start_node(spec, sink)
            .expect("start local process node");

        assert!(handle.provider_process_id.is_some());
        wait_for_observation(&recorder, |observation| {
            matches!(
                observation,
                PluginObservation::DatastreamFrame {
                    channel,
                    payload,
                    ..
                } if channel == "mvp.node.bootstrap"
                    && payload.contains("\"TestFrame\"")
                    && payload.contains("\"ready\"")
            )
        });
        wait_for_observation(&recorder, |observation| {
            matches!(
                observation,
                PluginObservation::StderrLine { line, .. } if line == "local-process-stderr"
            )
        });

        plugin.stop_node(&handle).expect("stop local process node");

        wait_for_observation(&recorder, |observation| {
            matches!(
                observation,
                PluginObservation::Exited {
                    node_id: 23,
                    status: Some(0),
                    ..
                }
            )
        });
    }

    #[cfg(unix)]
    #[test]
    fn local_process_plugin_kills_and_reaps_unresponsive_child() {
        let helper = TempScript::new(
            "unresponsive",
            r#"#!/bin/sh
while :; do
    sleep 1
done
"#,
        );
        let mut plugin = LocalProcessPlugin::new("/bin/sh");
        let (recorder, sink) = recording_sink();
        let spec = process_spec(24, vec![helper.path.to_string_lossy().to_string()]);
        let handle = plugin
            .start_node(spec, sink)
            .expect("start unresponsive local process node");
        let pid = handle
            .provider_process_id
            .expect("local process handle exposes child pid");

        plugin
            .stop_node(&handle)
            .expect("stop unresponsive local process node");

        wait_for_observation(&recorder, |observation| {
            matches!(
                observation,
                PluginObservation::Exited {
                    node_id: 24,
                    status,
                    ..
                } if *status != Some(0)
            )
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while process_alive(pid) && Instant::now() < deadline {
            thread::sleep(Duration::from_millis(20));
        }
        assert!(
            !process_alive(pid),
            "local process child {pid} is still live"
        );
    }
}
