//! Myelin-local provisioning provider plugins.
//!
//! Provider-neutral lifecycle contracts live in the reusable `provisioning`
//! crate. This module keeps Myelin-local process/Docker plugin implementations
//! that know about bootstrap telemetry plumbing and local runtime execution.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

pub use ::provisioning::plugin::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginObservationSink,
    PluginSink, ProviderMount, ProvisionEvent, ProvisionEventKind, ProvisionLogLine,
    ProvisionLogStream, ProvisionPlugin,
};
use iroh::EndpointAddr;
use swactor::actor::ActorAddress;

use crate::node::worker_node_runtime::request_debug_join;
use crate::observability::provisioning_logs::BootstrapTelemetryBridge;

pub(crate) struct LocalDockerPlugin {
    container_name_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalDockerNode>,
}

struct LocalDockerNode {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    container_name: String,
}

pub(crate) struct LocalProcessPlugin {
    program: PathBuf,
    registry_path: Option<PathBuf>,
    provider_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalProcessNode>,
}

/// Safe Vast.ai provisioning simulator. Marketplace selection remains real;
/// the selected offer is recorded here and a local worker stands in for the
/// rented machine.
pub(crate) struct MockVastAiPlugin {
    inner: LocalProcessPlugin,
    selected_offers: BTreeMap<u64, u64>,
}

struct LocalProcessNode {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    pid: Option<u32>,
    runtime: Option<LocalProcessRuntime>,
}

struct LocalProcessRuntime {
    stdin: ChildStdin,
    child: Arc<Mutex<Option<Child>>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
struct LocalProcessRecord {
    pid: u32,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
    run_id: u64,
    node_id: u64,
    attempt_id: u64,
}

static PROCESS_REGISTRY_LOCK: Mutex<()> = Mutex::new(());

impl LocalProcessPlugin {
    #[cfg(test)]
    pub(crate) fn new(program: impl Into<PathBuf>) -> Self {
        Self {
            program: program.into(),
            registry_path: None,
            provider_prefix: "process".to_owned(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
        }
    }

    pub(crate) fn with_registry(
        program: impl Into<PathBuf>,
        registry_path: impl Into<PathBuf>,
        provider_prefix: impl Into<String>,
    ) -> Self {
        Self {
            program: program.into(),
            registry_path: Some(registry_path.into()),
            provider_prefix: provider_prefix.into(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
        }
    }
}

impl MockVastAiPlugin {
    #[cfg(test)]
    pub(crate) fn new(program: impl Into<PathBuf>) -> Self {
        let mut inner = LocalProcessPlugin::new(program);
        inner.provider_prefix = "mock-vastai".to_owned();
        Self {
            inner,
            selected_offers: BTreeMap::new(),
        }
    }

    pub(crate) fn with_registry(
        program: impl Into<PathBuf>,
        registry_path: impl Into<PathBuf>,
    ) -> Self {
        Self {
            inner: LocalProcessPlugin::with_registry(program, registry_path, "mock-vastai"),
            selected_offers: BTreeMap::new(),
        }
    }
}
fn local_process_provider_ref(prefix: &str, spec: &NodeProvisionSpec) -> String {
    format!(
        "{prefix}-{}-{}-attempt-{}",
        spec.run_id, spec.node_id, spec.attempt_id
    )
}

fn process_output_paths(registry_path: Option<&Path>, provider_ref: &str) -> (PathBuf, PathBuf) {
    let output_root = registry_path
        .and_then(Path::parent)
        .map(|parent| parent.join("process-output"))
        .unwrap_or_else(|| {
            std::env::temp_dir().join(format!("myelin-process-output-{}", std::process::id()))
        });
    (
        output_root.join(format!("{provider_ref}.stdout.log")),
        output_root.join(format!("{provider_ref}.stderr.log")),
    )
}

fn read_process_registry(path: &Path) -> Result<BTreeMap<String, LocalProcessRecord>, String> {
    match fs::read_to_string(path) {
        Ok(content) => serde_json::from_str(&content)
            .map_err(|error| format!("parse process registry {}: {error}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeMap::new()),
        Err(error) => Err(format!("read process registry {}: {error}", path.display())),
    }
}

fn write_process_registry(
    path: &Path,
    registry: &BTreeMap<String, LocalProcessRecord>,
) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            format!("create process registry dir {}: {error}", parent.display())
        })?;
    }
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    let tmp = path.with_extension(format!("tmp-{}-{nonce}", std::process::id()));
    let bytes = serde_json::to_vec_pretty(registry)
        .map_err(|error| format!("serialize process registry: {error}"))?;
    fs::write(&tmp, bytes)
        .map_err(|error| format!("write process registry temp {}: {error}", tmp.display()))?;
    fs::rename(&tmp, path).map_err(|error| {
        let _ = fs::remove_file(&tmp);
        format!(
            "replace process registry {} from {}: {error}",
            path.display(),
            tmp.display()
        )
    })
}

fn update_process_registry<T>(
    path: &Path,
    update: impl FnOnce(&mut BTreeMap<String, LocalProcessRecord>) -> T,
) -> Result<T, String> {
    let _guard = PROCESS_REGISTRY_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut registry = read_process_registry(path)?;
    let result = update(&mut registry);
    write_process_registry(path, &registry)?;
    Ok(result)
}

fn process_record_matches(record: &LocalProcessRecord) -> bool {
    #[cfg(target_os = "linux")]
    {
        let Ok(stat) = fs::read_to_string(format!("/proc/{}/stat", record.pid)) else {
            return false;
        };
        let Some((_, tail)) = stat.rsplit_once(')') else {
            return false;
        };
        let mut fields = tail.split_whitespace();
        let state = fields.next();
        let _parent_pid = fields.next();
        let process_group = fields.next().and_then(|value| value.parse::<u32>().ok());
        if state == Some("Z") || process_group != Some(record.pid) {
            return false;
        }
        let Ok(environ) = fs::read(format!("/proc/{}/environ", record.pid)) else {
            return false;
        };
        let expected = [
            ("MYELIN_RUN_ID", record.run_id.to_string()),
            ("MYELIN_LOGICAL_NODE_ID", record.node_id.to_string()),
        ];
        return expected.iter().all(|(key, value)| {
            environ.split(|byte| *byte == 0).any(|entry| {
                entry
                    .strip_prefix(format!("{key}=").as_bytes())
                    .is_some_and(|actual| actual == value.as_bytes())
            })
        });
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = record;
        false
    }
}

struct FollowProcessFile {
    file: File,
    record: LocalProcessRecord,
}

impl Read for FollowProcessFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        loop {
            let count = self.file.read(buffer)?;
            if count > 0 || !process_record_matches(&self.record) {
                return Ok(count);
            }
            thread::sleep(Duration::from_millis(50));
        }
    }
}
fn discover_process_record(
    spec: &NodeProvisionSpec,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
) -> Result<Option<LocalProcessRecord>, String> {
    #[cfg(target_os = "linux")]
    {
        let entries =
            fs::read_dir("/proc").map_err(|error| format!("scan /proc for worker: {error}"))?;
        let mut matches = Vec::new();
        for entry in entries.flatten() {
            let Some(pid) = entry
                .file_name()
                .to_str()
                .and_then(|name| name.parse::<u32>().ok())
            else {
                continue;
            };
            let record = LocalProcessRecord {
                pid,
                stdout_path: stdout_path.clone(),
                stderr_path: stderr_path.clone(),
                run_id: spec.run_id,
                node_id: spec.node_id,
                attempt_id: spec.attempt_id,
            };
            if process_record_matches(&record) {
                matches.push(record);
            }
        }
        return match matches.len() {
            0 => Ok(None),
            1 => Ok(matches.pop()),
            count => Err(format!(
                "{count} local worker processes match run {} node {}",
                spec.run_id, spec.node_id
            )),
        };
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = (spec, stdout_path, stderr_path);
        Ok(None)
    }
}

fn request_process_rejoin(spec: &NodeProvisionSpec) -> Result<(), String> {
    let Some(endpoint_json) = spec
        .env
        .iter()
        .find_map(|(key, value)| (key == "MYELIN_COORDINATOR_ENDPOINT").then_some(value.as_str()))
    else {
        return Ok(());
    };
    let endpoint = serde_json::from_str::<EndpointAddr>(endpoint_json)
        .map_err(|error| format!("parse recovery coordinator endpoint: {error}"))?;
    let actor_json = spec
        .env
        .iter()
        .find_map(|(key, value)| (key == "MYELIN_ORCHESTRATOR_ACTOR").then_some(value.as_str()))
        .ok_or_else(|| "recovery spec has no orchestrator actor".to_owned())?;
    let orchestrator_actor = serde_json::from_str::<ActorAddress>(actor_json)
        .map_err(|error| format!("parse recovery orchestrator actor: {error}"))?;
    let socket = spec
        .env
        .iter()
        .find_map(|(key, value)| {
            (key == "MYELIN_DEBUG_JOIN_SOCKET").then_some(PathBuf::from(value))
        })
        .unwrap_or_else(|| {
            PathBuf::from(format!(
                "/tmp/myelin-node-debug-join-{}-{}.sock",
                spec.run_id, spec.node_id
            ))
        });
    let mut last_error = None;
    for _ in 0..40 {
        match request_debug_join(&socket, endpoint.clone(), orchestrator_actor) {
            Ok(()) => return Ok(()),
            Err(error) => last_error = Some(error),
        }
        thread::sleep(Duration::from_millis(50));
    }
    Err(format!(
        "rejoin local worker through {}: {}",
        socket.display(),
        last_error.unwrap_or_else(|| "unknown error".to_owned())
    ))
}

fn observe_process_files(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    record: LocalProcessRecord,
) -> Result<(), String> {
    let stdout = File::open(&record.stdout_path).map_err(|error| {
        format!(
            "open local process stdout {}: {error}",
            record.stdout_path.display()
        )
    })?;
    let stderr = File::open(&record.stderr_path).map_err(|error| {
        format!(
            "open local process stderr {}: {error}",
            record.stderr_path.display()
        )
    })?;
    spawn_stdout_reader(
        spec.clone(),
        sink.clone(),
        FollowProcessFile {
            file: stdout,
            record: record.clone(),
        },
    );
    spawn_stderr_reader(
        spec,
        sink,
        FollowProcessFile {
            file: stderr,
            record,
        },
    );
    Ok(())
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

fn docker_container_name(prefix: &str, spec: &NodeProvisionSpec) -> String {
    format!(
        "{prefix}-{}-{}-attempt-{}",
        spec.run_id, spec.node_id, spec.attempt_id
    )
}

/// Docker labels identifying every container this daemon owns. Orphan sweeps
/// (`docker ps -a --filter label=myelin.daemon=<prefix>`) rely on these; the
/// daemon label is the stable identity across restarts.
fn docker_container_labels(prefix: &str, spec: &NodeProvisionSpec) -> Vec<String> {
    vec![
        format!("myelin.daemon={prefix}"),
        format!("myelin.run={}", spec.run_id),
        format!("myelin.node={}", spec.node_id),
        format!("myelin.attempt={}", spec.attempt_id),
    ]
}

fn docker_inspect_error_is_absent(stderr: &str) -> bool {
    let stderr = stderr.to_ascii_lowercase();
    stderr.contains("no such object") || stderr.contains("no such container")
}

#[allow(clippy::disallowed_methods)]
fn docker_container_is_absent(name: &str) -> Result<bool, String> {
    let output = Command::new("docker")
        .arg("inspect")
        .arg(name)
        .output()
        .map_err(|error| format!("inspect Docker container {name}: {error}"))?;
    if output.status.success() {
        return Ok(false);
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    if docker_inspect_error_is_absent(&stderr) {
        Ok(true)
    } else {
        Err(format!(
            "inspect Docker container {name} exited with {}: {}",
            output.status,
            stderr.trim()
        ))
    }
}

#[allow(clippy::disallowed_methods)]
fn docker_container_is_running(name: &str) -> Result<bool, String> {
    let output = Command::new("docker")
        .args(["inspect", "-f", "{{.State.Running}}"])
        .arg(name)
        .output()
        .map_err(|error| format!("inspect Docker container {name} state: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "inspect Docker container {name} state exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
}

/// Lists container names carrying this daemon's label, running or not.
#[allow(clippy::disallowed_methods)]
fn docker_labeled_containers(prefix: &str) -> Result<Vec<String>, String> {
    let output = Command::new("docker")
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("label=myelin.daemon={prefix}"),
            "--format",
            "{{.Names}}",
        ])
        .output()
        .map_err(|error| format!("list labeled Docker containers: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "list labeled Docker containers exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn docker_containers_for_spec(
    prefix: &str,
    spec: &NodeProvisionSpec,
) -> Result<Vec<String>, String> {
    let output = Command::new("docker")
        .args(["ps", "-a"])
        .arg("--filter")
        .arg(format!("label=myelin.daemon={prefix}"))
        .arg("--filter")
        .arg(format!("label=myelin.run={}", spec.run_id))
        .arg("--filter")
        .arg(format!("label=myelin.node={}", spec.node_id))
        .args(["--format", "{{.Names}}"])
        .output()
        .map_err(|error| format!("list Docker containers for node {}: {error}", spec.node_id))?;
    if !output.status.success() {
        return Err(format!(
            "list Docker containers for node {} exited with {}: {}",
            spec.node_id,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(str::to_owned)
        .collect())
}

fn docker_container_for_spec(
    prefix: &str,
    spec: &NodeProvisionSpec,
) -> Result<Option<String>, String> {
    let expected = docker_container_name(prefix, spec);
    if !docker_container_is_absent(&expected)? {
        return Ok(Some(expected));
    }
    let matches = docker_containers_for_spec(prefix, spec)?;
    match matches.as_slice() {
        [] => Ok(None),
        [container] => Ok(Some(container.clone())),
        _ => Err(format!(
            "multiple Docker containers match run {} node {}: {}",
            spec.run_id,
            spec.node_id,
            matches.join(", ")
        )),
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
fn stop_owned_process(runtime: &mut LocalProcessRuntime) -> Result<Option<i32>, String> {
    let _ = runtime.stdin.write_all(b"shutdown\n");
    let _ = runtime.stdin.flush();
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        let mut slot = lock_process_child(&runtime.child);
        let Some(child) = slot.as_mut() else {
            return Ok(None);
        };
        match child.try_wait() {
            Ok(Some(status)) => {
                *slot = None;
                return Ok(status.code());
            }
            Ok(None) => {
                drop(slot);
                thread::sleep(Duration::from_millis(50));
            }
            Err(error) => return Err(format!("wait local process node: {error}")),
        }
    }

    let mut slot = lock_process_child(&runtime.child);
    let Some(child) = slot.as_mut() else {
        return Ok(None);
    };
    #[cfg(target_os = "linux")]
    unsafe {
        let _ = libc::kill(-(child.id() as i32), libc::SIGKILL);
    }
    let kill_error = child.kill().err();
    match child.wait() {
        Ok(status) => {
            *slot = None;
            Ok(status.code())
        }
        Err(error) => Err(match kill_error {
            Some(kill_error) => {
                format!("kill local process node: {kill_error}; wait failed: {error}")
            }
            None => format!("wait for killed local process node: {error}"),
        }),
    }
}

fn stop_adopted_process(record: &LocalProcessRecord) -> Result<(), String> {
    if !process_record_matches(record) {
        return Ok(());
    }
    #[cfg(target_os = "linux")]
    unsafe {
        if libc::kill(-(record.pid as i32), libc::SIGTERM) != 0 {
            return Err(format!(
                "terminate adopted local process {}: {}",
                record.pid,
                std::io::Error::last_os_error()
            ));
        }
    }
    let deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if !process_record_matches(record) {
            return Ok(());
        }
        thread::sleep(Duration::from_millis(50));
    }
    #[cfg(target_os = "linux")]
    unsafe {
        if libc::kill(-(record.pid as i32), libc::SIGKILL) != 0 && process_record_matches(record) {
            return Err(format!(
                "kill adopted local process {}: {}",
                record.pid,
                std::io::Error::last_os_error()
            ));
        }
    }
    Ok(())
}

fn observe_adopted_process(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    registry_path: PathBuf,
    provider_ref: String,
    record: LocalProcessRecord,
) {
    thread::spawn(move || {
        while process_record_matches(&record) {
            thread::sleep(Duration::from_millis(100));
        }
        let _ = update_process_registry(&registry_path, |registry| {
            registry.remove(&provider_ref);
        });
        sink.observe(PluginObservation::Exited {
            run_id: spec.run_id,
            node_id: spec.node_id,
            status: None,
        });
    });
}

impl ProvisionPlugin for LocalProcessPlugin {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: None,
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle.id,
            LocalProcessNode {
                spec,
                sink,
                pid: None,
                runtime: None,
            },
        );
        Ok(handle)
    }

    // provider process supervision/lifecycle is out of scope (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let registry_path = self.registry_path.clone();
        let provider_prefix = self.provider_prefix.clone();
        let program = self.program.clone();
        let node = self
            .nodes
            .get_mut(&handle.id)
            .ok_or_else(|| format!("local process node handle {} is absent", handle.id))?;
        if node.pid.is_some() {
            return Ok(());
        }
        let spec = node.spec.clone();
        let sink = node.sink.clone();
        let provider_ref = local_process_provider_ref(&provider_prefix, &spec);
        let (stdout_path, stderr_path) =
            process_output_paths(registry_path.as_deref(), &provider_ref);
        let output_root = stdout_path
            .parent()
            .expect("local process output path has a parent");
        fs::create_dir_all(output_root).map_err(|error| {
            format!(
                "create local process output dir {}: {error}",
                output_root.display()
            )
        })?;
        let stdout = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&stdout_path)
            .map_err(|error| {
                format!(
                    "create local process stdout {}: {error}",
                    stdout_path.display()
                )
            })?;
        let stderr = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&stderr_path)
            .map_err(|error| {
                format!(
                    "create local process stderr {}: {error}",
                    stderr_path.display()
                )
            })?;
        let mut command = Command::new(&program);
        for (key, value) in &spec.env {
            command.env(key, value);
        }
        command.args(&spec.args);
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr));
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

        let mut child = command.spawn().map_err(|error| {
            format!(
                "spawn local process node {} with {}: {error}",
                spec.node_id,
                program.display()
            )
        })?;
        let pid = child.id();
        let Some(stdin) = child.stdin.take() else {
            let _ = child.kill();
            let _ = child.wait();
            return Err(format!(
                "local process node {} did not expose piped stdin",
                spec.node_id
            ));
        };
        let record = LocalProcessRecord {
            pid,
            stdout_path,
            stderr_path,
            run_id: spec.run_id,
            node_id: spec.node_id,
            attempt_id: spec.attempt_id,
        };
        if let Some(path) = registry_path.as_deref()
            && let Err(error) = update_process_registry(path, |registry| {
                registry.insert(provider_ref.clone(), record.clone());
            })
        {
            #[cfg(target_os = "linux")]
            unsafe {
                let _ = libc::kill(-(pid as i32), libc::SIGKILL);
            }
            let _ = child.kill();
            let _ = child.wait();
            return Err(error);
        }

        let child = Arc::new(Mutex::new(Some(child)));
        node.pid = Some(pid);
        node.runtime = Some(LocalProcessRuntime {
            stdin,
            child: Arc::clone(&child),
        });
        observe_process_files(spec.clone(), sink.clone(), record)?;
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
                        Err(error) => Some(PluginObservation::Failed {
                            run_id: spec.run_id,
                            node_id: spec.node_id,
                            reason: format!("wait local process node: {error}"),
                        }),
                    }
                };
                if let Some(observation) = observation {
                    if let Some(path) = registry_path.as_deref() {
                        let _ = update_process_registry(path, |registry| {
                            registry.remove(&provider_ref);
                        });
                    }
                    sink.observe(observation);
                    return;
                }
                thread::sleep(Duration::from_millis(100));
            }
        });
        Ok(())
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
        let provider_ref = local_process_provider_ref(&self.provider_prefix, &node.spec);
        let record = self
            .registry_path
            .as_deref()
            .map(read_process_registry)
            .transpose()?
            .and_then(|registry| registry.get(&provider_ref).cloned());
        let result = match node.runtime.as_mut() {
            Some(runtime) => stop_owned_process(runtime),
            None => match record.as_ref() {
                Some(record) => stop_adopted_process(record).map(|()| None),
                None => Ok(None),
            },
        };
        match result {
            Ok(status) => {
                if let Some(path) = self.registry_path.as_deref() {
                    update_process_registry(path, |registry| {
                        registry.remove(&provider_ref);
                    })?;
                }
                node.sink.observe(PluginObservation::Exited {
                    run_id: node.spec.run_id,
                    node_id: node.spec.node_id,
                    status,
                });
                Ok(())
            }
            Err(reason) => {
                node.sink.observe(PluginObservation::Failed {
                    run_id: node.spec.run_id,
                    node_id: node.spec.node_id,
                    reason: reason.clone(),
                });
                self.nodes.insert(handle.id, node);
                Err(reason)
            }
        }
    }

    fn prepare_missing_bootstrap(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let handle = self.create_node(spec.clone(), sink)?;
        Ok(Some(AdoptedNode {
            handle,
            provider_ref: self.provider_ref_for(spec),
        }))
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let Some(registry_path) = self.registry_path.clone() else {
            return Ok(None);
        };
        let provider_ref = local_process_provider_ref(&self.provider_prefix, spec);
        let mut record = read_process_registry(&registry_path)?
            .get(&provider_ref)
            .cloned();
        if record
            .as_ref()
            .is_some_and(|record| !process_record_matches(record))
        {
            update_process_registry(&registry_path, |registry| {
                registry.remove(&provider_ref);
            })?;
            record = None;
        }
        if record.is_none() {
            let (stdout_path, stderr_path) =
                process_output_paths(Some(&registry_path), &provider_ref);
            for _ in 0..40 {
                record = discover_process_record(spec, stdout_path.clone(), stderr_path.clone())?;
                if record.is_some() {
                    break;
                }
                thread::sleep(Duration::from_millis(50));
            }
            if let Some(discovered) = record.as_ref() {
                update_process_registry(&registry_path, |registry| {
                    registry.insert(provider_ref.clone(), discovered.clone());
                })?;
            }
        }
        let Some(record) = record else {
            return Ok(None);
        };
        request_process_rejoin(spec)?;
        observe_process_files(spec.clone(), sink.clone(), record.clone())?;
        observe_adopted_process(
            spec.clone(),
            sink.clone(),
            registry_path,
            provider_ref.clone(),
            record.clone(),
        );
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: Some(record.pid),
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle.id,
            LocalProcessNode {
                spec: spec.clone(),
                sink,
                pid: Some(record.pid),
                runtime: None,
            },
        );
        Ok(Some(AdoptedNode {
            handle,
            provider_ref,
        }))
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        local_process_provider_ref(&self.provider_prefix, spec)
    }

    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        let Some(path) = self.registry_path.as_deref() else {
            return Ok(Vec::new());
        };
        update_process_registry(path, |registry| {
            registry.retain(|_, record| process_record_matches(record));
            registry.keys().cloned().collect()
        })
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        let Some(path) = self.registry_path.as_deref() else {
            return Ok(false);
        };
        let provider_ref = local_process_provider_ref(&self.provider_prefix, spec);
        let record = read_process_registry(path)?.get(&provider_ref).cloned();
        let Some(record) = record else {
            return Ok(false);
        };
        stop_adopted_process(&record)?;
        update_process_registry(path, |registry| {
            registry.remove(&provider_ref);
        })?;
        sink.observe(PluginObservation::Exited {
            run_id: spec.run_id,
            node_id: spec.node_id,
            status: None,
        });
        Ok(true)
    }
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
        sink.observe(PluginObservation::ProviderLine {
            run_id: spec.run_id,
            node_id: spec.node_id,
            line: serde_json::json!({
                "type": "MockVastAiContractCreated",
                "simulated": true,
                "selected_offer_id": offer_id,
            })
            .to_string(),
        });
        let handle = self.inner.create_node(spec, sink)?;
        self.selected_offers.insert(handle.id, offer_id);
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
        self.inner.stop_node(handle)?;
        self.selected_offers.remove(&handle.id);
        Ok(())
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        self.inner.adopt_by_spec(spec, sink)
    }

    fn prepare_missing_bootstrap(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        self.inner.prepare_missing_bootstrap(spec, sink)
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        self.inner.provider_ref_for(spec)
    }

    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        self.inner.list_managed_refs()
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        self.inner.stop_by_spec(spec, sink)
    }
}

impl ProvisionPlugin for LocalDockerPlugin {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: None,
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle.id,
            LocalDockerNode {
                container_name: docker_container_name(&self.container_name_prefix, &spec),
                spec,
                sink,
            },
        );
        Ok(handle)
    }

    // provider process supervision/lifecycle is out of scope (ENGINE_SPEC.md §2)
    #[allow(clippy::disallowed_methods)]
    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let node = self
            .nodes
            .get(&handle.id)
            .ok_or_else(|| format!("Docker node handle {} is absent", handle.id))?;
        if !docker_container_is_absent(&node.container_name)? {
            return if docker_container_is_running(&node.container_name)? {
                Ok(())
            } else {
                Err(format!(
                    "Docker container {} exists but is not running",
                    node.container_name
                ))
            };
        }
        let spec = node.spec.clone();
        let sink = node.sink.clone();
        let container_name = node.container_name.clone();
        let mut command = Command::new("docker");
        command
            .arg("run")
            .arg("-d")
            .arg("--add-host")
            .arg("host.docker.internal:host-gateway")
            .arg("--name")
            .arg(&container_name);
        for label in docker_container_labels(&self.container_name_prefix, &spec) {
            command.arg("--label").arg(label);
        }
        let docker_gpus = spec
            .env
            .iter()
            .find(|(key, _)| key == "MYELIN_DOCKER_GPUS")
            .map(|(_, value)| value.clone())
            .or_else(|| std::env::var("MYELIN_DOCKER_GPUS").ok())
            .filter(|value| !value.trim().is_empty());
        sink.observe(PluginObservation::TelemetryFrame {
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
        let output = command
            .output()
            .map_err(|error| format!("run Docker node {}: {error}", spec.node_id))?;
        if !output.status.success() {
            return Err(format!(
                "run Docker node {} exited with {}: {}",
                spec.node_id,
                output.status,
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        observe_docker_container(spec, sink, container_name, "all")
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        let result = remove_docker_container(&node.container_name);
        if result.is_err() {
            self.nodes.insert(handle.id, node);
        }
        result
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let Some(container_name) = docker_container_for_spec(&self.container_name_prefix, spec)?
        else {
            return Ok(None);
        };
        let running = docker_container_is_running(&container_name)?;
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: None,
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        if running {
            // Replay this container's bootstrap log into the fresh daemon,
            // then follow new output. The replay supplies runtime facts when
            // the prior daemon died before persisting readiness.
            observe_docker_container(spec.clone(), sink.clone(), container_name.clone(), "all")?;
        }
        let adopted_name = container_name.clone();
        self.nodes.insert(
            handle.id,
            LocalDockerNode {
                spec: spec.clone(),
                sink: sink.clone(),
                container_name,
            },
        );
        sink.observe(PluginObservation::TelemetryFrame {
            run_id: spec.run_id,
            node_id: spec.node_id,
            channel: "myelin.provisioning.events".to_owned(),
            payload: serde_json::json!({
                "type":"DockerContainerAdopted",
                "provider":"Docker",
                "container":adopted_name.clone(),
                "running":running,
            })
            .to_string(),
        });
        Ok(Some(AdoptedNode {
            handle,
            provider_ref: adopted_name,
        }))
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        docker_container_name(&self.container_name_prefix, spec)
    }
    fn prepare_missing_bootstrap(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let handle = self.create_node(spec.clone(), sink)?;
        Ok(Some(AdoptedNode {
            handle,
            provider_ref: self.provider_ref_for(spec),
        }))
    }

    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        docker_labeled_containers(&self.container_name_prefix)
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        let Some(container_name) = docker_container_for_spec(&self.container_name_prefix, spec)?
        else {
            return Ok(false);
        };
        remove_docker_container(&container_name)?;
        sink.observe(PluginObservation::TelemetryFrame {
            run_id: spec.run_id,
            node_id: spec.node_id,
            channel: "myelin.provisioning.events".to_owned(),
            payload: serde_json::json!({
                "type":"DockerContainerRemoved",
                "provider":"Docker",
                "container":container_name,
                "removed":true,
                "exit_ok":true,
            })
            .to_string(),
        });
        Ok(true)
    }
}

fn remove_docker_container(container_name: &str) -> Result<(), String> {
    let status = Command::new("docker")
        .arg("rm")
        .arg("-f")
        .arg(container_name)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map_err(|error| format!("docker rm {container_name}: {error}"))?;
    if status.success() || matches!(docker_container_is_absent(container_name), Ok(true)) {
        Ok(())
    } else {
        Err(format!("docker rm {container_name} exited with {status}"))
    }
}

fn observe_docker_container(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    container_name: String,
    tail: &str,
) -> Result<(), String> {
    let mut logs = Command::new("docker");
    let mut logs = logs
        .arg("logs")
        .arg("--follow")
        .arg("--tail")
        .arg(tail)
        .arg(&container_name)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("follow Docker logs for {container_name}: {error}"))?;
    let stdout = logs
        .stdout
        .take()
        .ok_or_else(|| format!("Docker logs for {container_name} has no stdout"))?;
    let stderr = logs
        .stderr
        .take()
        .ok_or_else(|| format!("Docker logs for {container_name} has no stderr"))?;
    spawn_stdout_reader(spec.clone(), sink.clone(), stdout);
    spawn_stderr_reader(spec.clone(), sink.clone(), stderr);
    thread::spawn(move || {
        let _ = logs.wait();
    });

    let wait_name = container_name;
    thread::spawn(move || {
        let status = Command::new("docker").arg("wait").arg(&wait_name).output();
        let code = status.ok().and_then(|output| {
            String::from_utf8_lossy(&output.stdout)
                .trim()
                .parse::<i32>()
                .ok()
        });
        sink.observe(PluginObservation::Exited {
            run_id: spec.run_id,
            node_id: spec.node_id,
            status: code,
        });
    });
    Ok(())
}

fn spawn_stdout_reader(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stdout: impl std::io::Read + Send + 'static,
) {
    BootstrapTelemetryBridge::new(spec, sink, None).spawn_stdout_reader(stdout);
}

fn spawn_stderr_reader(
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stderr: impl std::io::Read + Send + 'static,
) {
    BootstrapTelemetryBridge::new(spec, sink, None).spawn_stderr_reader(stderr);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;

    #[test]
    fn docker_absence_detection_is_case_insensitive() {
        assert!(docker_inspect_error_is_absent(
            "Error: No such object: missing"
        ));
        assert!(docker_inspect_error_is_absent(
            "error: no such container: missing"
        ));
        assert!(!docker_inspect_error_is_absent("permission denied"));
    }

    struct ChannelSink(mpsc::Sender<PluginObservation>);

    impl PluginObservationSink for ChannelSink {
        fn observe(&self, observation: PluginObservation) {
            let _ = self.0.send(observation);
        }
    }

    fn test_spec() -> NodeProvisionSpec {
        NodeProvisionSpec {
            run_id: 5,
            node_id: 7,
            attempt_id: 11,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            mounts: Vec::new(),
        }
    }

    #[test]
    fn mock_vastai_preserves_exact_offer_identity_without_starting_a_lease() {
        let (tx, rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut plugin = MockVastAiPlugin::new("/not/executed");
        let spec = test_spec();

        let handle = plugin
            .create_node_selected(spec.clone(), sink, Some(8_675_309))
            .unwrap();

        assert_eq!(plugin.provider_ref_for(&spec), "mock-vastai-5-7-attempt-11");
        assert_eq!(plugin.selected_offers.get(&handle.id), Some(&8_675_309));
        assert_eq!(plugin.inner.nodes.len(), 1);
        let PluginObservation::ProviderLine { line, .. } = rx.try_recv().unwrap() else {
            panic!("mock lease must emit a provider event");
        };
        let event: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(event["type"], "MockVastAiContractCreated");
        assert_eq!(event["simulated"], true);
        assert_eq!(event["selected_offer_id"], 8_675_309);

        plugin.stop_node(&handle).unwrap();
        assert!(plugin.selected_offers.is_empty());
        assert!(plugin.inner.nodes.is_empty());
    }

    #[test]
    fn mock_vastai_rejects_create_without_an_exact_offer() {
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut plugin = MockVastAiPlugin::new("/not/executed");

        let error = plugin.create_node(test_spec(), sink).unwrap_err();

        assert!(error.contains("exact selected offer"));
        assert!(plugin.selected_offers.is_empty());
        assert!(plugin.inner.nodes.is_empty());
    }

    proptest::proptest! {
        #[test]
        fn mock_vastai_handle_state_survives_random_create_and_stop_sequences(
            operations in proptest::collection::vec(
                (
                    proptest::prelude::any::<u64>(),
                    proptest::prelude::any::<bool>(),
                ),
                1..128,
            )
        ) {
            let (tx, _) = mpsc::channel();
            let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
            let mut plugin = MockVastAiPlugin::new("/not/executed");
            let mut handles = Vec::new();

            for (offer_id, should_stop) in operations {
                let mut spec = test_spec();
                spec.node_id = handles.len() as u64 + 1;
                let handle = plugin
                    .create_node_selected(spec, sink.clone(), Some(offer_id))
                    .unwrap();
                handles.push(handle.clone());
                if should_stop {
                    plugin.stop_node(&handle).unwrap();
                } else {
                    plugin.complete_bootstrap(&handle).unwrap();
                }
                proptest::prop_assert_eq!(
                    plugin.selected_offers.keys().copied().collect::<Vec<_>>(),
                    plugin.inner.nodes.keys().copied().collect::<Vec<_>>()
                );
            }

            for handle in handles {
                plugin.stop_node(&handle).unwrap();
            }
            proptest::prop_assert!(plugin.selected_offers.is_empty());
            proptest::prop_assert!(plugin.inner.nodes.is_empty());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_process_creation_does_not_start_bootstrap() {
        let (tx, rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut spec = test_spec();
        spec.args = vec![
            "-c".to_owned(),
            "printf 'started\\n'; IFS= read -r line".to_owned(),
        ];
        let mut plugin = LocalProcessPlugin::new("/bin/sh");

        let handle = plugin.create_node(spec, sink).unwrap();
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        plugin.start_bootstrap(&handle).unwrap();
        let deadline = Instant::now() + Duration::from_secs(2);
        let line = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let observation = rx.recv_timeout(remaining).unwrap();
            if let PluginObservation::StdoutLine { line, .. } = observation {
                break line;
            }
        };
        assert_eq!(line, "started");
        plugin.stop_node(&handle).unwrap();
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn persistent_process_is_adopted_after_plugin_drop() {
        let registry_path = std::env::temp_dir().join(format!(
            "myelin-process-registry-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut spec = test_spec();
        spec.env
            .push(("MYELIN_RUN_ID".to_owned(), spec.run_id.to_string()));
        spec.env.push((
            "MYELIN_LOGICAL_NODE_ID".to_owned(),
            spec.node_id.to_string(),
        ));
        spec.args = vec![
            "-c".to_owned(),
            "trap 'exit 0' TERM; while :; do sleep 1; done".to_owned(),
        ];
        let provider_ref = local_process_provider_ref("process", &spec);

        let record = {
            let mut plugin =
                LocalProcessPlugin::with_registry("/bin/sh", &registry_path, "process");
            let handle = plugin.create_node(spec.clone(), sink.clone()).unwrap();
            plugin.start_bootstrap(&handle).unwrap();
            read_process_registry(&registry_path)
                .unwrap()
                .get(&provider_ref)
                .cloned()
                .unwrap()
        };
        assert!(process_record_matches(&record));
        // Model a crash after spawn but before the parent commits its registry update.
        fs::remove_file(&registry_path).unwrap();

        let mut restarted = LocalProcessPlugin::with_registry("/bin/sh", &registry_path, "process");
        let adopted = restarted
            .adopt_by_spec(&spec, sink)
            .unwrap()
            .expect("running process must be adopted");
        assert_eq!(adopted.provider_ref, provider_ref);
        assert_eq!(adopted.handle.provider_process_id, Some(record.pid));
        restarted.stop_node(&adopted.handle).unwrap();
        assert!(!process_record_matches(&record));
        let _ = fs::remove_file(registry_path);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn mock_vastai_process_is_adopted_after_plugin_drop() {
        let registry_path = std::env::temp_dir().join(format!(
            "myelin-mock-registry-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut spec = test_spec();
        spec.env
            .push(("MYELIN_RUN_ID".to_owned(), spec.run_id.to_string()));
        spec.env.push((
            "MYELIN_LOGICAL_NODE_ID".to_owned(),
            spec.node_id.to_string(),
        ));
        spec.args = vec![
            "-c".to_owned(),
            "trap 'exit 0' TERM; while :; do sleep 1; done".to_owned(),
        ];

        let record = {
            let mut plugin = MockVastAiPlugin::with_registry("/bin/sh", &registry_path);
            let handle = plugin
                .create_node_selected(spec.clone(), sink.clone(), Some(42))
                .unwrap();
            plugin.start_bootstrap(&handle).unwrap();
            read_process_registry(&registry_path)
                .unwrap()
                .values()
                .next()
                .cloned()
                .unwrap()
        };
        let mut restarted = MockVastAiPlugin::with_registry("/bin/sh", &registry_path);
        let adopted = restarted
            .adopt_by_spec(&spec, sink)
            .unwrap()
            .expect("mock worker must be adopted");
        assert_eq!(adopted.handle.provider_process_id, Some(record.pid));
        restarted.stop_node(&adopted.handle).unwrap();
        assert!(!process_record_matches(&record));
        let _ = fs::remove_file(registry_path);
    }
    #[cfg(target_os = "linux")]
    #[test]
    fn mixed_mock_processes_survive_kill_provision_and_restart() {
        let registry_path = std::env::temp_dir().join(format!(
            "myelin-mixed-mock-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let make_spec = |node_id: u64| {
            let mut spec = test_spec();
            spec.node_id = node_id;
            spec.env
                .push(("MYELIN_RUN_ID".to_owned(), spec.run_id.to_string()));
            spec.env.push((
                "MYELIN_LOGICAL_NODE_ID".to_owned(),
                spec.node_id.to_string(),
            ));
            spec.args = vec![
                "-c".to_owned(),
                "trap 'exit 0' TERM; while :; do sleep 1; done".to_owned(),
            ];
            spec
        };

        {
            let mut plugin = MockVastAiPlugin::with_registry("/bin/sh", &registry_path);
            for node_id in 1..=3 {
                let handle = plugin
                    .create_node_selected(make_spec(node_id), sink.clone(), Some(node_id))
                    .unwrap();
                plugin.start_bootstrap(&handle).unwrap();
            }
        }
        {
            let mut restarted = MockVastAiPlugin::with_registry("/bin/sh", &registry_path);
            let killed = restarted
                .adopt_by_spec(&make_spec(2), sink.clone())
                .unwrap()
                .expect("node 2");
            restarted.stop_node(&killed.handle).unwrap();
            let new_node = restarted
                .create_node_selected(make_spec(4), sink.clone(), Some(4))
                .unwrap();
            restarted.start_bootstrap(&new_node).unwrap();
        }

        let mut final_restart = MockVastAiPlugin::with_registry("/bin/sh", &registry_path);
        assert_eq!(
            final_restart.list_managed_refs().unwrap(),
            [
                "mock-vastai-5-1-attempt-11",
                "mock-vastai-5-3-attempt-11",
                "mock-vastai-5-4-attempt-11",
            ]
        );
        for node_id in [1, 3, 4] {
            let adopted = final_restart
                .adopt_by_spec(&make_spec(node_id), sink.clone())
                .unwrap()
                .expect("surviving mock process");
            final_restart.stop_node(&adopted.handle).unwrap();
        }
        let _ = fs::remove_file(registry_path);
    }

    #[test]
    fn missing_local_bootstrap_recreates_only_the_provider_handle() {
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let spec = test_spec();
        let mut process = LocalProcessPlugin::new("/not/executed");
        let process_node = process
            .prepare_missing_bootstrap(&spec, sink.clone())
            .unwrap()
            .expect("process bootstrap handle");
        assert_eq!(process_node.provider_ref, "process-5-7-attempt-11");
        assert_eq!(
            process.nodes.get(&process_node.handle.id).unwrap().pid,
            None
        );

        let mut docker = LocalDockerPlugin::new("myelin");
        let docker_node = docker
            .prepare_missing_bootstrap(&spec, sink)
            .unwrap()
            .expect("docker bootstrap handle");
        assert_eq!(docker_node.provider_ref, "myelin-5-7-attempt-11");
        assert_eq!(docker.nodes.len(), 1);
    }

    #[test]
    fn docker_container_identity_distinguishes_node_attempts() {
        let mut spec = test_spec();

        assert_eq!(
            docker_container_name("myelin", &spec),
            "myelin-5-7-attempt-11"
        );
        spec.attempt_id = 12;
        assert_eq!(
            docker_container_name("myelin", &spec),
            "myelin-5-7-attempt-12"
        );
    }
}
