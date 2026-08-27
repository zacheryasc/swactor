//! Myelin-local provisioning provider plugins.
//!
//! Provider-neutral lifecycle contracts live in the reusable `provisioning`
//! crate. This module keeps Myelin-local process/Docker plugin implementations
//! that know about bootstrap telemetry plumbing and local runtime execution.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

pub use ::provisioning::plugin::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginObservationSink,
    PluginSink, ProviderMount, ProvisionEvent, ProvisionEventKind, ProvisionLogLine,
    ProvisionLogStream, ProvisionPlugin,
};
use iroh::EndpointAddr;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime};

use crate::node::worker_node_runtime::request_debug_join;
use crate::observability::provisioning_logs::BootstrapTelemetryBridge;
use crate::orchestration::manual_control::SELECTED_OFFER_ID_ENV;

pub(crate) struct LocalDockerPlugin {
    container_name_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalDockerNode>,
    runtime: Runtime,
    backend: Arc<dyn DockerLifecycleBackend>,
}

struct LocalDockerNode {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    container_name: String,
}

trait DockerLifecycleBackend: Send + Sync {
    fn container_state(&self, name: &str) -> Result<Option<bool>, String>;
    fn start(&self, prefix: &str, runtime: &Runtime, node: &LocalDockerNode) -> Result<(), String>;
    fn remove(&self, name: &str) -> Result<(), String>;
    fn find(
        &self,
        prefix: &str,
        spec: &NodeProvisionSpec,
    ) -> Result<Option<(String, bool)>, String>;
    fn observe(&self, runtime: &Runtime, node: &LocalDockerNode, tail: &str) -> Result<(), String>;
    fn list_managed(&self, prefix: &str) -> Result<Vec<String>, String>;
}

struct SystemDockerLifecycle;

pub(crate) struct LocalProcessPlugin {
    program: PathBuf,
    registry_path: Option<PathBuf>,
    provider_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalProcessNode>,
    runtime: Runtime,
}

/// Safe Vast.ai provisioning simulator. Marketplace selection and contract
/// semantics remain VastAI-shaped while the selected image is realized by a
/// delegated local provider.
pub(crate) struct MockVastAiPlugin {
    inner: Box<dyn ProvisionPlugin>,
    contracts: BTreeMap<u64, MockVastAiContract>,
    selected_offers: BTreeMap<u64, u64>,
}

#[derive(Clone)]
struct MockVastAiContract {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    provider_ref: String,
    selected_offer_id: Option<u64>,
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
    exit_actor: Option<ActorAddress>,
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

impl LocalProcessRecord {
    fn identity(&self) -> swactor_process::ProcessIdentity {
        swactor_process::ProcessIdentity {
            pid: self.pid,
            environment: vec![
                ("MYELIN_RUN_ID".to_owned(), self.run_id.to_string()),
                (
                    "MYELIN_LOGICAL_NODE_ID".to_owned(),
                    self.node_id.to_string(),
                ),
            ],
            process_group_leader: true,
        }
    }
}

struct BootstrapOutputActor {
    bridge: BootstrapTelemetryBridge,
    closed: u8,
}

impl ActorInterface for BootstrapOutputActor {
    type Incoming = swactor_process::ProcessStreamObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        match observation {
            swactor_process::ProcessStreamObservation::Line { stream, line } => match stream {
                swactor_process::ProcessStream::Stdout => self.bridge.observe_stdout_line(line),
                swactor_process::ProcessStream::Stderr => self.bridge.observe_stderr_line(line),
            },
            swactor_process::ProcessStreamObservation::Error { stream, error } => {
                self.bridge.observe_provider_line(format!(
                    "read {}: {error}",
                    match stream {
                        swactor_process::ProcessStream::Stdout => "stdout",
                        swactor_process::ProcessStream::Stderr => "stderr",
                    }
                ));
            }
            swactor_process::ProcessStreamObservation::Closed { .. } => {
                self.closed = self.closed.saturating_add(1);
                if self.closed == 2 {
                    ctx.stop_self();
                }
            }
        }
    }
}

struct ProcessExitActor {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    registry: Option<(PathBuf, String)>,
}

impl ActorInterface for ProcessExitActor {
    type Incoming = swactor_process::ProcessExitObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        if let Some((path, provider_ref)) = &self.registry {
            let _ = update_process_registry(path, |registry| {
                registry.remove(provider_ref);
            });
        }
        let cleanup_error = remove_debug_join_socket(&self.spec).err();
        let event = match (observation.error, cleanup_error) {
            (Some(reason), Some(cleanup_error)) => PluginObservation::Failed {
                run_id: self.spec.run_id,
                node_id: self.spec.node_id,
                reason: format!("wait local process node: {reason}; {cleanup_error}"),
            },
            (Some(reason), None) => PluginObservation::Failed {
                run_id: self.spec.run_id,
                node_id: self.spec.node_id,
                reason: format!("wait local process node: {reason}"),
            },
            (None, Some(reason)) => PluginObservation::Failed {
                run_id: self.spec.run_id,
                node_id: self.spec.node_id,
                reason,
            },
            (None, None) => PluginObservation::Exited {
                run_id: self.spec.run_id,
                node_id: self.spec.node_id,
                status: observation.status,
            },
        };
        self.sink.observe(event);
        ctx.stop_self();
    }
}

struct DockerWaitActor {
    spec: NodeProvisionSpec,
    sink: PluginSink,
}

impl ActorInterface for DockerWaitActor {
    type Incoming = swactor_process::CommandOutputObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        let status = observation
            .error
            .is_none()
            .then(|| {
                String::from_utf8_lossy(&observation.stdout)
                    .trim()
                    .parse::<i32>()
                    .ok()
            })
            .flatten();
        self.sink.observe(PluginObservation::Exited {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            status,
        });
        ctx.stop_self();
    }
}

struct DiscardProcessExit;

impl ActorInterface for DiscardProcessExit {
    type Incoming = swactor_process::ProcessExitObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, _observation: Self::Incoming) {
        ctx.stop_self();
    }
}

static PROCESS_REGISTRY_LOCK: Mutex<()> = Mutex::new(());

impl LocalProcessPlugin {
    #[cfg(test)]
    pub(crate) fn new(program: impl Into<PathBuf>, runtime: Runtime) -> Self {
        Self {
            program: program.into(),
            registry_path: None,
            provider_prefix: "process".to_owned(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
            runtime,
        }
    }

    pub(crate) fn with_registry(
        program: impl Into<PathBuf>,
        registry_path: impl Into<PathBuf>,
        provider_prefix: impl Into<String>,
        runtime: Runtime,
    ) -> Self {
        Self {
            program: program.into(),
            registry_path: Some(registry_path.into()),
            provider_prefix: provider_prefix.into(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
            runtime,
        }
    }
}

impl MockVastAiPlugin {
    fn with_inner(inner: Box<dyn ProvisionPlugin>) -> Self {
        Self {
            inner,
            contracts: BTreeMap::new(),
            selected_offers: BTreeMap::new(),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_registry(
        program: impl Into<PathBuf>,
        registry_path: impl Into<PathBuf>,
        runtime: Runtime,
    ) -> Self {
        Self::with_inner(Box::new(LocalProcessPlugin::with_registry(
            program,
            registry_path,
            "mock-vastai",
            runtime,
        )))
    }

    pub(crate) fn with_docker(container_name_prefix: impl Into<String>, runtime: Runtime) -> Self {
        Self::with_inner(Box::new(LocalDockerPlugin::new(
            container_name_prefix,
            runtime,
        )))
    }
}
fn selected_offer_from_spec(spec: &NodeProvisionSpec) -> Option<u64> {
    spec.env
        .iter()
        .find(|(key, _)| key == SELECTED_OFFER_ID_ENV)
        .and_then(|(_, value)| value.parse().ok())
}

fn observe_mock_vastai_contract(
    sink: &PluginSink,
    event_type: &str,
    spec: &NodeProvisionSpec,
    provider_ref: &str,
    selected_offer_id: Option<u64>,
) {
    sink.observe(PluginObservation::ProviderLine {
        run_id: spec.run_id,
        node_id: spec.node_id,
        line: serde_json::json!({
            "type": event_type,
            "simulated": true,
            "provider_ref": provider_ref,
            "selected_offer_id": selected_offer_id,
        })
        .to_string(),
    });
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
    record.identity().matches()
}
fn discover_process_record(
    spec: &NodeProvisionSpec,
    stdout_path: PathBuf,
    stderr_path: PathBuf,
) -> Result<Option<LocalProcessRecord>, String> {
    let environment = vec![
        ("MYELIN_RUN_ID".to_owned(), spec.run_id.to_string()),
        (
            "MYELIN_LOGICAL_NODE_ID".to_owned(),
            spec.node_id.to_string(),
        ),
    ];
    let mut matches = swactor_process::find_process_identities_with_retry(
        &environment,
        true,
        40,
        Duration::from_millis(50),
    )
    .map_err(|error| format!("scan processes for worker: {error}"))?;
    match matches.len() {
        0 => Ok(None),
        1 => {
            let identity = matches.pop().expect("one process identity");
            Ok(Some(LocalProcessRecord {
                pid: identity.pid,
                stdout_path,
                stderr_path,
                run_id: spec.run_id,
                node_id: spec.node_id,
                attempt_id: spec.attempt_id,
            }))
        }
        count => Err(format!(
            "{count} local worker processes match run {} node {}",
            spec.run_id, spec.node_id
        )),
    }
}

fn debug_join_socket_path(spec: &NodeProvisionSpec) -> PathBuf {
    spec.env
        .iter()
        .find_map(|(key, value)| {
            (key == "MYELIN_DEBUG_JOIN_SOCKET").then_some(PathBuf::from(value))
        })
        .unwrap_or_else(|| {
            PathBuf::from(format!(
                "/tmp/myelin-node-debug-join-{}-{}.sock",
                spec.run_id, spec.node_id
            ))
        })
}

fn remove_debug_join_socket(spec: &NodeProvisionSpec) -> Result<(), String> {
    let socket = debug_join_socket_path(spec);
    match fs::remove_file(&socket) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(format!(
            "remove debug join socket {}: {error}",
            socket.display()
        )),
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
    let socket = debug_join_socket_path(spec);
    if !swactor_process::wait_for_path(&socket, 40, Duration::from_millis(50)) {
        return Err(format!(
            "rejoin socket {} did not become ready",
            socket.display()
        ));
    }
    request_debug_join(&socket, endpoint, orchestrator_actor)
        .map_err(|error| format!("rejoin local worker through {}: {error}", socket.display()))
}

fn observe_process_files(
    runtime: &Runtime,
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
    observe_output_streams(
        runtime,
        spec,
        sink,
        swactor_process::FollowProcessFile::new(
            stdout,
            record.identity(),
            Duration::from_millis(50),
        ),
        swactor_process::FollowProcessFile::new(
            stderr,
            record.identity(),
            Duration::from_millis(50),
        ),
    )
}

fn observe_output_streams(
    runtime: &Runtime,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stdout: impl std::io::Read + Send + 'static,
    stderr: impl std::io::Read + Send + 'static,
) -> Result<(), String> {
    let actor = runtime
        .spawn(BootstrapOutputActor {
            bridge: BootstrapTelemetryBridge::new(spec, sink, None),
            closed: 0,
        })
        .map_err(|error| format!("spawn bootstrap output actor: {error}"))?;
    let sender = runtime.create_sender();
    swactor_process::spawn_line_reader(
        swactor_process::ProcessStream::Stdout,
        stdout,
        sender.clone(),
        actor,
    );
    swactor_process::spawn_line_reader(
        swactor_process::ProcessStream::Stderr,
        stderr,
        sender,
        actor,
    );
    Ok(())
}

impl LocalDockerPlugin {
    pub(crate) fn new(container_name_prefix: impl Into<String>, runtime: Runtime) -> Self {
        Self::with_backend(
            container_name_prefix,
            runtime,
            Arc::new(SystemDockerLifecycle),
        )
    }

    fn with_backend(
        container_name_prefix: impl Into<String>,
        runtime: Runtime,
        backend: Arc<dyn DockerLifecycleBackend>,
    ) -> Self {
        Self {
            container_name_prefix: container_name_prefix.into(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
            runtime,
            backend,
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

fn docker_container_is_absent(name: &str) -> Result<bool, String> {
    let output = swactor_process::command_output(Command::new("docker").arg("inspect").arg(name))
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

fn docker_container_is_running(name: &str) -> Result<bool, String> {
    let output = swactor_process::command_output(
        Command::new("docker")
            .args(["inspect", "-f", "{{.State.Running}}"])
            .arg(name),
    )
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
fn docker_labeled_containers(prefix: &str) -> Result<Vec<String>, String> {
    let output = swactor_process::command_output(Command::new("docker").args([
        "ps",
        "-a",
        "--filter",
        &format!("label=myelin.daemon={prefix}"),
        "--format",
        "{{.Names}}",
    ]))
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
    let output = swactor_process::command_output(
        Command::new("docker")
            .args(["ps", "-a"])
            .arg("--filter")
            .arg(format!("label=myelin.daemon={prefix}"))
            .arg("--filter")
            .arg(format!("label=myelin.run={}", spec.run_id))
            .arg("--filter")
            .arg(format!("label=myelin.node={}", spec.node_id))
            .args(["--format", "{{.Names}}"]),
    )
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
    let _ = swactor_process::command_status(
        Command::new("docker")
            .args(["rm", "-f", &loader_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
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
    let _ = swactor_process::command_status(
        Command::new("docker")
            .args(["rm", &loader_name])
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    );
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
    let status = swactor_process::command_status(
        Command::new("docker")
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit()),
    )
    .map_err(|e| format!("{label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with {status}"))
    }
}

fn docker_status_vec(args: Vec<String>, label: &str) -> Result<(), String> {
    let status = swactor_process::command_status(
        Command::new("docker")
            .args(&args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit()),
    )
    .map_err(|e| format!("{label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with {status}"))
    }
}

fn stop_owned_process(runtime: &mut LocalProcessRuntime) -> Result<Option<i32>, String> {
    let _ = runtime.stdin.write_all(b"shutdown\n");
    let _ = runtime.stdin.flush();
    swactor_process::wait_shared_child_or_kill(
        &runtime.child,
        Duration::from_secs(2),
        true,
        Duration::from_millis(50),
    )
    .map(|status| status.and_then(|status| status.code()))
    .map_err(|error| format!("stop local process node: {error}"))
}

fn stop_adopted_process(record: &LocalProcessRecord) -> Result<(), String> {
    #[cfg(target_os = "linux")]
    {
        swactor_process::terminate_process_group(
            &record.identity(),
            Duration::from_secs(2),
            Duration::from_millis(50),
        )
    }
    #[cfg(not(target_os = "linux"))]
    Ok(())
}

fn observe_adopted_process(
    runtime: &Runtime,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    registry_path: PathBuf,
    provider_ref: String,
    record: LocalProcessRecord,
) -> Result<(), String> {
    let actor = runtime
        .spawn(ProcessExitActor {
            spec,
            sink,
            registry: Some((registry_path, provider_ref)),
        })
        .map_err(|error| format!("spawn adopted process observer actor: {error}"))?;
    swactor_process::spawn_identity_exit_wait(
        record.identity(),
        Duration::from_millis(100),
        runtime.create_sender(),
        actor,
    );
    Ok(())
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

        let mut child = swactor_process::command_spawn(&mut command).map_err(|error| {
            format!(
                "spawn local process node {} with {}: {error}",
                spec.node_id,
                program.display()
            )
        })?;
        let pid = child.id();
        let Some(stdin) = child.stdin.take() else {
            let _ = swactor_process::child_kill(&mut child);
            let _ = swactor_process::child_wait(&mut child);
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
            let _ = swactor_process::child_kill(&mut child);
            let _ = swactor_process::child_wait(&mut child);
            return Err(error);
        }

        let child = Arc::new(Mutex::new(Some(child)));
        node.pid = Some(pid);
        node.runtime = Some(LocalProcessRuntime {
            stdin,
            child: Arc::clone(&child),
            exit_actor: None,
        });
        observe_process_files(&self.runtime, spec.clone(), sink.clone(), record)?;
        let exit_actor = self
            .runtime
            .spawn(ProcessExitActor {
                spec,
                sink,
                registry: registry_path.map(|path| (path, provider_ref)),
            })
            .map_err(|error| format!("spawn local process observer actor: {error}"))?;
        node.runtime
            .as_mut()
            .expect("local process runtime was installed before its exit observer")
            .exit_actor = Some(exit_actor);
        swactor_process::spawn_shared_child_wait(
            child,
            Duration::from_millis(100),
            self.runtime.create_sender(),
            exit_actor,
        );
        Ok(())
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

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
            Some(runtime) => {
                let result = stop_owned_process(runtime);
                if result.is_ok()
                    && let Some(exit_actor) = runtime.exit_actor.take()
                {
                    let _ = self.runtime.stop_actor(exit_actor);
                }
                result
            }
            None => match record.as_ref() {
                Some(record) => stop_adopted_process(record).map(|()| None),
                None => Ok(None),
            },
        };
        let result = result.and_then(|status| {
            remove_debug_join_socket(&node.spec)?;
            Ok(status)
        });
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
            record = discover_process_record(spec, stdout_path, stderr_path)?;
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
        observe_process_files(&self.runtime, spec.clone(), sink.clone(), record.clone())?;
        observe_adopted_process(
            &self.runtime,
            spec.clone(),
            sink.clone(),
            registry_path,
            provider_ref.clone(),
            record.clone(),
        )?;
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
        let provider_ref = self.inner.provider_ref_for(&spec);
        let handle = self.inner.create_node(spec.clone(), sink.clone())?;
        self.selected_offers.insert(handle.id, offer_id);
        self.contracts.insert(
            handle.id,
            MockVastAiContract {
                spec: spec.clone(),
                sink: sink.clone(),
                provider_ref: provider_ref.clone(),
                selected_offer_id: Some(offer_id),
            },
        );
        observe_mock_vastai_contract(
            &sink,
            "MockVastAiContractCreated",
            &spec,
            &provider_ref,
            Some(offer_id),
        );
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
        let contract = self.contracts.get(&handle.id).cloned();
        self.inner.stop_node(handle)?;
        self.selected_offers.remove(&handle.id);
        self.contracts.remove(&handle.id);
        if let Some(contract) = contract {
            observe_mock_vastai_contract(
                &contract.sink,
                "MockVastAiContractDestroyed",
                &contract.spec,
                &contract.provider_ref,
                contract.selected_offer_id,
            );
        }
        Ok(())
    }

    fn adopt_by_spec(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let adopted = self.inner.adopt_by_spec(spec, sink.clone())?;
        if let Some(adopted) = &adopted {
            let offer_id = selected_offer_from_spec(spec);
            if let Some(offer_id) = offer_id {
                self.selected_offers.insert(adopted.handle.id, offer_id);
            }
            self.contracts.insert(
                adopted.handle.id,
                MockVastAiContract {
                    spec: spec.clone(),
                    sink: sink.clone(),
                    provider_ref: adopted.provider_ref.clone(),
                    selected_offer_id: offer_id,
                },
            );
            observe_mock_vastai_contract(
                &sink,
                "MockVastAiContractAdopted",
                spec,
                &adopted.provider_ref,
                offer_id,
            );
        }
        Ok(adopted)
    }

    fn prepare_missing_bootstrap(
        &mut self,
        spec: &NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<Option<AdoptedNode>, String> {
        let prepared = self.inner.prepare_missing_bootstrap(spec, sink.clone())?;
        if let Some(prepared) = &prepared {
            let offer_id = selected_offer_from_spec(spec);
            if let Some(offer_id) = offer_id {
                self.selected_offers.insert(prepared.handle.id, offer_id);
            }
            self.contracts.insert(
                prepared.handle.id,
                MockVastAiContract {
                    spec: spec.clone(),
                    sink: sink.clone(),
                    provider_ref: prepared.provider_ref.clone(),
                    selected_offer_id: offer_id,
                },
            );
            observe_mock_vastai_contract(
                &sink,
                "MockVastAiContractRecreated",
                spec,
                &prepared.provider_ref,
                offer_id,
            );
        }
        Ok(prepared)
    }

    fn provider_ref_for(&self, spec: &NodeProvisionSpec) -> String {
        self.inner.provider_ref_for(spec)
    }

    fn list_managed_refs(&self) -> Result<Vec<String>, String> {
        self.inner.list_managed_refs()
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        let provider_ref = self.inner.provider_ref_for(spec);
        let stopped = self.inner.stop_by_spec(spec, sink.clone())?;
        if stopped {
            let matching_handles = self
                .contracts
                .iter()
                .filter_map(|(handle, contract)| {
                    (contract.spec.run_id == spec.run_id
                        && contract.spec.node_id == spec.node_id
                        && contract.spec.attempt_id == spec.attempt_id)
                        .then_some(*handle)
                })
                .collect::<Vec<_>>();
            for handle in matching_handles {
                self.contracts.remove(&handle);
                self.selected_offers.remove(&handle);
            }
            observe_mock_vastai_contract(
                &sink,
                "MockVastAiContractDestroyed",
                spec,
                &provider_ref,
                selected_offer_from_spec(spec),
            );
        }
        Ok(stopped)
    }
}

impl DockerLifecycleBackend for SystemDockerLifecycle {
    fn container_state(&self, name: &str) -> Result<Option<bool>, String> {
        if docker_container_is_absent(name)? {
            Ok(None)
        } else {
            docker_container_is_running(name).map(Some)
        }
    }

    fn start(&self, prefix: &str, runtime: &Runtime, node: &LocalDockerNode) -> Result<(), String> {
        start_system_docker_container(prefix, runtime, node)
    }

    fn remove(&self, name: &str) -> Result<(), String> {
        remove_docker_container(name)
    }

    fn find(
        &self,
        prefix: &str,
        spec: &NodeProvisionSpec,
    ) -> Result<Option<(String, bool)>, String> {
        let Some(name) = docker_container_for_spec(prefix, spec)? else {
            return Ok(None);
        };
        let running = docker_container_is_running(&name)?;
        Ok(Some((name, running)))
    }

    fn observe(&self, runtime: &Runtime, node: &LocalDockerNode, tail: &str) -> Result<(), String> {
        observe_docker_container(
            runtime,
            node.spec.clone(),
            node.sink.clone(),
            node.container_name.clone(),
            tail,
        )
    }

    fn list_managed(&self, prefix: &str) -> Result<Vec<String>, String> {
        docker_labeled_containers(prefix)
    }
}

fn start_system_docker_container(
    prefix: &str,
    runtime: &Runtime,
    node: &LocalDockerNode,
) -> Result<(), String> {
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
    for label in docker_container_labels(prefix, &spec) {
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
    let output = swactor_process::command_output(&mut command)
        .map_err(|error| format!("run Docker node {}: {error}", spec.node_id))?;
    if !output.status.success() {
        return Err(format!(
            "run Docker node {} exited with {}: {}",
            spec.node_id,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    observe_docker_container(runtime, spec, sink, container_name, "all")
}

impl ProvisionPlugin for LocalDockerPlugin {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        let container_name = docker_container_name(&self.container_name_prefix, &spec);
        if self
            .nodes
            .values()
            .any(|node| node.container_name == container_name)
        {
            return Err(format!(
                "Docker attempt {} is already registered",
                container_name
            ));
        }
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: None,
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        self.nodes.insert(
            handle.id,
            LocalDockerNode {
                container_name,
                spec,
                sink,
            },
        );
        Ok(handle)
    }

    fn start_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let node = self
            .nodes
            .get(&handle.id)
            .ok_or_else(|| format!("Docker node handle {} is absent", handle.id))?;
        match self.backend.container_state(&node.container_name)? {
            Some(true) => Ok(()),
            Some(false) => Err(format!(
                "Docker container {} exists but is not running",
                node.container_name
            )),
            None => self
                .backend
                .start(&self.container_name_prefix, &self.runtime, node),
        }
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        let Some(node) = self.nodes.remove(&handle.id) else {
            return Ok(());
        };
        let result = self.backend.remove(&node.container_name);
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
        if let Some((&id, node)) = self.nodes.iter_mut().find(|(_, node)| {
            node.container_name == docker_container_name(&self.container_name_prefix, spec)
        }) {
            node.sink = sink;
            return Ok(Some(AdoptedNode {
                handle: PluginNodeHandle {
                    id,
                    provider_process_id: None,
                },
                provider_ref: node.container_name.clone(),
            }));
        }
        let Some((container_name, running)) =
            self.backend.find(&self.container_name_prefix, spec)?
        else {
            return Ok(None);
        };
        let handle = PluginNodeHandle {
            id: self.next_handle_id,
            provider_process_id: None,
        };
        self.next_handle_id = self.next_handle_id.wrapping_add(1).max(1);
        let node = LocalDockerNode {
            spec: spec.clone(),
            sink: sink.clone(),
            container_name: container_name.clone(),
        };
        if running {
            // Replay this container's bootstrap log into the fresh daemon,
            // then follow new output. The replay supplies runtime facts when
            // the prior daemon died before persisting readiness.
            self.backend.observe(&self.runtime, &node, "all")?;
        }
        let adopted_name = container_name;
        self.nodes.insert(handle.id, node);
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
        self.backend.list_managed(&self.container_name_prefix)
    }

    fn stop_by_spec(&mut self, spec: &NodeProvisionSpec, sink: PluginSink) -> Result<bool, String> {
        let Some((container_name, _running)) =
            self.backend.find(&self.container_name_prefix, spec)?
        else {
            return Ok(false);
        };
        self.backend.remove(&container_name)?;
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
    let status = swactor_process::command_status(
        Command::new("docker")
            .arg("rm")
            .arg("-f")
            .arg(container_name)
            .stdout(Stdio::null())
            .stderr(Stdio::null()),
    )
    .map_err(|error| format!("docker rm {container_name}: {error}"))?;
    if status.success() || matches!(docker_container_is_absent(container_name), Ok(true)) {
        Ok(())
    } else {
        Err(format!("docker rm {container_name} exited with {status}"))
    }
}

fn observe_docker_container(
    runtime: &Runtime,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    container_name: String,
    tail: &str,
) -> Result<(), String> {
    let mut logs = Command::new("docker");
    let mut logs = swactor_process::command_spawn(
        logs.arg("logs")
            .arg("--follow")
            .arg("--tail")
            .arg(tail)
            .arg(&container_name)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    )
    .map_err(|error| format!("follow Docker logs for {container_name}: {error}"))?;
    let stdout = logs
        .stdout
        .take()
        .ok_or_else(|| format!("Docker logs for {container_name} has no stdout"))?;
    let stderr = logs
        .stderr
        .take()
        .ok_or_else(|| format!("Docker logs for {container_name} has no stderr"))?;
    observe_output_streams(runtime, spec.clone(), sink.clone(), stdout, stderr)?;

    let logs_waiter = runtime
        .spawn(DiscardProcessExit)
        .map_err(|error| format!("spawn Docker log waiter actor: {error}"))?;
    swactor_process::spawn_child_wait(logs, runtime.create_sender(), logs_waiter);

    let wait_actor = runtime
        .spawn(DockerWaitActor { spec, sink })
        .map_err(|error| format!("spawn Docker waiter actor: {error}"))?;
    let mut wait = Command::new("docker");
    wait.arg("wait").arg(container_name);
    swactor_process::spawn_command_output(wait, runtime.create_sender(), wait_actor);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use parking_lot::Mutex as ParkingMutex;
    use proptest::prelude::*;
    use std::collections::BTreeSet;
    use std::sync::mpsc;
    use swactor::runtime::RuntimeParts;
    use swactor_engine::{Engine, SteppingBackend, TokioBackend, TokioConfig};

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

    fn test_runtime() -> (Engine, Runtime) {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = TokioBackend::new(TokioConfig::default()).expect("test Tokio backend");
        let engine = Engine::new(parts, backend).expect("test actor engine");
        (engine, runtime)
    }

    fn mock_docker_plugin(runtime: Runtime) -> (MockVastAiPlugin, Arc<ScriptedDockerBackend>) {
        let backend = Arc::new(ScriptedDockerBackend::default());
        let inner = LocalDockerPlugin::with_backend("mock-vastai", runtime, backend.clone());
        (MockVastAiPlugin::with_inner(Box::new(inner)), backend)
    }

    fn recv_provider_event(rx: &mpsc::Receiver<PluginObservation>) -> serde_json::Value {
        for _ in 0..32 {
            let observation = rx
                .recv_timeout(Duration::from_secs(1))
                .expect("provider event");
            if let PluginObservation::ProviderLine { line, .. } = observation {
                return serde_json::from_str(&line).expect("provider event JSON");
            }
        }
        panic!("provider event was not observed");
    }

    #[test]
    fn mock_vastai_preserves_exact_offer_identity_without_starting_a_lease() {
        let (_engine, runtime) = test_runtime();
        let (tx, rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let (mut plugin, backend) = mock_docker_plugin(runtime);
        let spec = test_spec();

        let handle = plugin
            .create_node_selected(spec.clone(), sink, Some(8_675_309))
            .unwrap();

        assert_eq!(plugin.provider_ref_for(&spec), "mock-vastai-5-7-attempt-11");
        assert_eq!(plugin.selected_offers.get(&handle.id), Some(&8_675_309));
        assert!(backend.state.lock().resources.is_empty());
        assert_eq!(plugin.contracts.len(), 1);
        let event = recv_provider_event(&rx);
        assert_eq!(event["type"], "MockVastAiContractCreated");
        assert_eq!(event["simulated"], true);
        assert_eq!(event["selected_offer_id"], 8_675_309);
        assert_eq!(event["provider_ref"], "mock-vastai-5-7-attempt-11");

        plugin.stop_node(&handle).unwrap();
        assert!(plugin.selected_offers.is_empty());
        assert!(plugin.contracts.is_empty());
        let event = recv_provider_event(&rx);
        assert_eq!(event["type"], "MockVastAiContractDestroyed");
        assert_eq!(event["provider_ref"], "mock-vastai-5-7-attempt-11");
        assert_eq!(event["selected_offer_id"], 8_675_309);
    }

    #[test]
    fn mock_vastai_rejects_create_without_an_exact_offer() {
        let (_engine, runtime) = test_runtime();
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let (mut plugin, _) = mock_docker_plugin(runtime);

        let error = plugin.create_node(test_spec(), sink).unwrap_err();

        assert!(error.contains("exact selected offer"));
        assert!(plugin.selected_offers.is_empty());
        assert!(plugin.contracts.is_empty());
    }

    proptest::proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]
        #[test]
        fn mock_vastai_handle_state_survives_random_create_and_stop_sequences(
            operations in proptest::collection::vec(
                (
                    proptest::prelude::any::<u64>(),
                    proptest::prelude::any::<bool>(),
                ),
                0..=32,
            )
        ) {
            let (_engine, runtime) = test_runtime();
            let (tx, _) = mpsc::channel();
            let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
            let (mut plugin, _) = mock_docker_plugin(runtime);
            let mut handles = Vec::new();

            for (offer_id, should_stop) in operations {
                let mut spec = test_spec();
                spec.node_id = handles.len() as u64 + 1;
                let handle = plugin
                    .create_node_selected(spec, sink.clone(), Some(offer_id.max(1)))
                    .unwrap();
                handles.push(handle.clone());
                if should_stop {
                    plugin.stop_node(&handle).unwrap();
                } else {
                    plugin.complete_bootstrap(&handle).unwrap();
                }
                proptest::prop_assert_eq!(
                    plugin.selected_offers.keys().copied().collect::<Vec<_>>(),
                    plugin.contracts.keys().copied().collect::<Vec<_>>()
                );
            }

            for handle in handles {
                plugin.stop_node(&handle).unwrap();
            }
            proptest::prop_assert!(plugin.selected_offers.is_empty());
            proptest::prop_assert!(plugin.contracts.is_empty());
        }
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn local_process_stop_releases_all_observer_actors() {
        let (_engine, runtime) = test_runtime();
        let baseline_actors = runtime.stats().actors.len();
        let baseline_fds = fs::read_dir("/proc/self/fd").unwrap().count();
        let (tx, rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut spec = test_spec();
        let socket_path = std::env::temp_dir().join(format!(
            "myelin-local-process-socket-test-{}-{}.sock",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::write(&socket_path, []).unwrap();
        spec.env.push((
            "MYELIN_DEBUG_JOIN_SOCKET".to_owned(),
            socket_path.display().to_string(),
        ));
        spec.args = vec![
            "-c".to_owned(),
            "printf 'started\n'; IFS= read -r line".to_owned(),
        ];
        let mut plugin = LocalProcessPlugin::new("/bin/sh", runtime.clone());

        let handle = plugin.create_node(spec, sink).unwrap();
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        plugin.start_bootstrap(&handle).unwrap();
        let node = plugin
            .nodes
            .get(&handle.id)
            .expect("registered local process");
        assert!(node.pid.is_some(), "bootstrap did not start its process");
        assert!(
            node.runtime.is_some(),
            "bootstrap did not retain its process runtime"
        );
        plugin.stop_node(&handle).unwrap();
        assert!(
            !socket_path.exists(),
            "local process debug socket survived explicit stop"
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while (runtime.stats().actors.len() != baseline_actors
            || fs::read_dir("/proc/self/fd").unwrap().count() > baseline_fds)
            && std::time::Instant::now() < deadline
        {
            std::thread::sleep(Duration::from_millis(10));
        }
        assert_eq!(
            runtime.stats().actors.len(),
            baseline_actors,
            "local process observers survived explicit process stop"
        );
        let final_fds = fs::read_dir("/proc/self/fd").unwrap().count();
        assert!(
            final_fds <= baseline_fds,
            "local process stop leaked file descriptors: baseline={baseline_fds}, final={final_fds}"
        );
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn persistent_process_is_adopted_after_plugin_drop() {
        let (_engine, runtime) = test_runtime();
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
            let mut plugin = LocalProcessPlugin::with_registry(
                "/bin/sh",
                &registry_path,
                "process",
                runtime.clone(),
            );
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

        let mut restarted =
            LocalProcessPlugin::with_registry("/bin/sh", &registry_path, "process", runtime);
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
        let (_engine, runtime) = test_runtime();
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
        spec.run_id = 6;
        spec.node_id = 8;
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
            let mut plugin =
                MockVastAiPlugin::with_registry("/bin/sh", &registry_path, runtime.clone());
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
        let mut restarted = MockVastAiPlugin::with_registry("/bin/sh", &registry_path, runtime);
        let adopted = restarted
            .adopt_by_spec(&spec, sink)
            .unwrap()
            .expect("mock worker must be adopted");
        assert_eq!(adopted.handle.provider_process_id, Some(record.pid));
        restarted.stop_node(&adopted.handle).unwrap();
        assert!(!process_record_matches(&record));
        let _ = fs::remove_file(registry_path);
    }

    #[test]
    fn missing_local_bootstrap_recreates_only_the_provider_handle() {
        let (_engine, runtime) = test_runtime();
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let spec = test_spec();
        let mut process = LocalProcessPlugin::new("/not/executed", runtime.clone());
        let process_node = process
            .prepare_missing_bootstrap(&spec, sink.clone())
            .unwrap()
            .expect("process bootstrap handle");
        assert_eq!(process_node.provider_ref, "process-5-7-attempt-11");
        assert_eq!(
            process.nodes.get(&process_node.handle.id).unwrap().pid,
            None
        );

        let mut docker = LocalDockerPlugin::new("myelin", runtime);
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

    #[derive(Clone, Debug)]
    struct ScriptedDockerResource {
        id: u64,
        attempt: String,
        running: bool,
    }

    #[derive(Default, Debug)]
    struct ScriptedDockerState {
        resources: Vec<ScriptedDockerResource>,
        waiters: BTreeMap<String, ActorAddress>,
        next_resource_id: u64,
        fail_next_start: bool,
        fail_next_remove: bool,
        started_specs: Vec<NodeProvisionSpec>,
    }

    #[derive(Default)]
    struct ScriptedDockerBackend {
        state: ParkingMutex<ScriptedDockerState>,
    }

    #[derive(Clone, Copy, Debug)]
    enum ScriptedDockerTerminal {
        Exit(i32),
        CommandFailure,
        MalformedOutput,
    }

    impl ScriptedDockerTerminal {
        fn observation(self) -> swactor_process::CommandOutputObservation {
            match self {
                Self::Exit(status) => swactor_process::CommandOutputObservation {
                    status: Some(0),
                    stdout: format!("{status}\n").into_bytes(),
                    stderr: Vec::new(),
                    error: None,
                },
                Self::CommandFailure => swactor_process::CommandOutputObservation {
                    status: None,
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                    error: Some("scripted docker wait command failure".to_owned()),
                },
                Self::MalformedOutput => swactor_process::CommandOutputObservation {
                    status: Some(0),
                    stdout: b"not-an-exit-status\n".to_vec(),
                    stderr: Vec::new(),
                    error: None,
                },
            }
        }
    }

    impl ScriptedDockerBackend {
        fn fail_start(&self) {
            self.state.lock().fail_next_start = true;
        }

        fn fail_remove(&self) {
            self.state.lock().fail_next_remove = true;
        }

        fn clear_failures(&self) {
            let mut state = self.state.lock();
            state.fail_next_start = false;
            state.fail_next_remove = false;
        }

        fn seed_orphan(&self, name: &str, running: bool) -> Result<(), String> {
            let mut state = self.state.lock();
            if state
                .resources
                .iter()
                .any(|resource| resource.attempt == name)
            {
                return Err(format!("scripted Docker resource {name} already exists"));
            }
            state.next_resource_id = state.next_resource_id.wrapping_add(1).max(1);
            let id = state.next_resource_id;
            state.resources.push(ScriptedDockerResource {
                id,
                attempt: name.to_owned(),
                running,
            });
            Ok(())
        }

        fn inject_duplicate_resource(&self, name: &str) -> Result<(), String> {
            let mut state = self.state.lock();
            let running = state
                .resources
                .iter()
                .find(|resource| resource.attempt == name)
                .ok_or_else(|| format!("cannot duplicate absent Docker resource {name}"))?
                .running;
            state.next_resource_id = state.next_resource_id.wrapping_add(1).max(1);
            let id = state.next_resource_id;
            state.resources.push(ScriptedDockerResource {
                id,
                attempt: name.to_owned(),
                running,
            });
            Ok(())
        }

        fn has_waiter(&self, name: &str) -> bool {
            self.state.lock().waiters.contains_key(name)
        }

        fn finish(
            &self,
            runtime: &Runtime,
            name: &str,
            terminal: ScriptedDockerTerminal,
        ) -> Result<bool, String> {
            let waiter = {
                let mut state = self.state.lock();
                for resource in &mut state.resources {
                    if resource.attempt == name {
                        resource.running = false;
                    }
                }
                state.waiters.remove(name)
            };
            let Some(waiter) = waiter else {
                return Ok(false);
            };
            runtime
                .send_to(waiter, terminal.observation())
                .map_err(|error| {
                    format!("deliver scripted Docker terminal observation: {error}")
                })?;
            Ok(true)
        }

        fn finish_all(&self, runtime: &Runtime) -> Result<(), String> {
            let waiters = {
                let mut state = self.state.lock();
                std::mem::take(&mut state.waiters)
            };
            for (_, waiter) in waiters {
                runtime
                    .send_to(waiter, ScriptedDockerTerminal::CommandFailure.observation())
                    .map_err(|error| {
                        format!("deliver scripted Docker cleanup observation: {error}")
                    })?;
            }
            Ok(())
        }

        fn observe_node(&self, runtime: &Runtime, node: &LocalDockerNode) -> Result<(), String> {
            {
                let state = self.state.lock();
                if state.waiters.contains_key(&node.container_name) {
                    return Ok(());
                }
            }
            let waiter = runtime
                .spawn(DockerWaitActor {
                    spec: node.spec.clone(),
                    sink: node.sink.clone(),
                })
                .map_err(|error| format!("spawn scripted Docker waiter: {error}"))?;
            self.state
                .lock()
                .waiters
                .insert(node.container_name.clone(), waiter);
            Ok(())
        }
    }

    impl DockerLifecycleBackend for ScriptedDockerBackend {
        fn container_state(&self, name: &str) -> Result<Option<bool>, String> {
            let state = self.state.lock();
            let matches = state
                .resources
                .iter()
                .filter(|resource| resource.attempt == name)
                .collect::<Vec<_>>();
            match matches.as_slice() {
                [] => Ok(None),
                [resource] => Ok(Some(resource.running)),
                _ => Err(format!(
                    "multiple scripted Docker resources match {name}: {matches:?}"
                )),
            }
        }

        fn start(
            &self,
            _prefix: &str,
            runtime: &Runtime,
            node: &LocalDockerNode,
        ) -> Result<(), String> {
            {
                let mut state = self.state.lock();
                if std::mem::take(&mut state.fail_next_start) {
                    return Err("scripted Docker start command failure".to_owned());
                }
                if state
                    .resources
                    .iter()
                    .any(|resource| resource.attempt == node.container_name)
                {
                    return Err(format!(
                        "duplicate scripted Docker container {}",
                        node.container_name
                    ));
                }
                state.started_specs.push(node.spec.clone());
                state.next_resource_id = state.next_resource_id.wrapping_add(1).max(1);
                let id = state.next_resource_id;
                state.resources.push(ScriptedDockerResource {
                    id,
                    attempt: node.container_name.clone(),
                    running: true,
                });
            }
            self.observe_node(runtime, node)
        }

        fn remove(&self, name: &str) -> Result<(), String> {
            let mut state = self.state.lock();
            if std::mem::take(&mut state.fail_next_remove) {
                return Err("scripted Docker remove command failure".to_owned());
            }
            state.resources.retain(|resource| resource.attempt != name);
            Ok(())
        }

        fn find(
            &self,
            prefix: &str,
            spec: &NodeProvisionSpec,
        ) -> Result<Option<(String, bool)>, String> {
            let name = docker_container_name(prefix, spec);
            Ok(self.container_state(&name)?.map(|running| (name, running)))
        }

        fn observe(
            &self,
            runtime: &Runtime,
            node: &LocalDockerNode,
            _tail: &str,
        ) -> Result<(), String> {
            self.observe_node(runtime, node)
        }

        fn list_managed(&self, _prefix: &str) -> Result<Vec<String>, String> {
            Ok(self
                .state
                .lock()
                .resources
                .iter()
                .map(|resource| resource.attempt.clone())
                .collect())
        }
    }

    #[derive(Default)]
    struct RecordingDockerSink {
        observations: ParkingMutex<Vec<PluginObservation>>,
    }

    impl RecordingDockerSink {
        fn snapshot(&self) -> Vec<PluginObservation> {
            self.observations.lock().clone()
        }
    }

    impl PluginObservationSink for RecordingDockerSink {
        fn observe(&self, observation: PluginObservation) {
            self.observations.lock().push(observation);
        }
    }

    #[test]
    fn mock_vastai_realizes_selected_image_in_one_docker_resource() {
        let (_engine, runtime) = test_runtime();
        let (tx, rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let (mut plugin, backend) = mock_docker_plugin(runtime.clone());
        let mut spec = test_spec();
        spec.image = "myelin-tinygrad:test".to_owned();
        spec.env
            .push(("MYELIN_DOCKER_GPUS".to_owned(), "device=0".to_owned()));

        let handle = plugin
            .create_node_selected(spec.clone(), sink, Some(91))
            .unwrap();
        assert!(backend.state.lock().resources.is_empty());
        plugin.start_bootstrap(&handle).unwrap();

        let state = backend.state.lock();
        assert_eq!(state.resources.len(), 1);
        assert_eq!(state.started_specs, [spec.clone()]);
        drop(state);
        let created = recv_provider_event(&rx);
        assert_eq!(created["type"], "MockVastAiContractCreated");
        assert_eq!(created["provider_ref"], "mock-vastai-5-7-attempt-11");

        plugin.complete_bootstrap(&handle).unwrap();
        plugin.stop_node(&handle).unwrap();
        plugin.stop_node(&handle).unwrap();
        assert!(
            backend
                .finish(
                    &runtime,
                    "mock-vastai-5-7-attempt-11",
                    ScriptedDockerTerminal::Exit(137),
                )
                .unwrap()
        );
        assert!(plugin.contracts.is_empty());
        assert!(plugin.selected_offers.is_empty());
        assert!(backend.state.lock().resources.is_empty());
        let destroyed = recv_provider_event(&rx);
        assert_eq!(destroyed["type"], "MockVastAiContractDestroyed");
    }

    #[test]
    fn mock_vastai_can_stop_before_start_and_reprovision_new_attempt() {
        let (_engine, runtime) = test_runtime();
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let (mut plugin, backend) = mock_docker_plugin(runtime);
        let first = test_spec();
        let first_handle = plugin
            .create_node_selected(first.clone(), sink.clone(), Some(1))
            .unwrap();

        plugin.cancel_bootstrap(&first_handle).unwrap();
        plugin.stop_node(&first_handle).unwrap();
        plugin.stop_node(&first_handle).unwrap();
        assert!(backend.state.lock().resources.is_empty());

        let mut second = first;
        second.attempt_id += 1;
        let second_handle = plugin
            .create_node_selected(second.clone(), sink, Some(2))
            .unwrap();
        plugin.start_bootstrap(&second_handle).unwrap();
        assert_eq!(
            backend
                .state
                .lock()
                .resources
                .iter()
                .map(|resource| resource.attempt.as_str())
                .collect::<Vec<_>>(),
            ["mock-vastai-5-7-attempt-12"]
        );
        plugin.stop_node(&second_handle).unwrap();
        assert!(backend.state.lock().resources.is_empty());
    }

    #[test]
    fn mock_vastai_worker_death_cleanup_allows_reprovision() {
        let (_engine, runtime) = test_runtime();
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let (mut plugin, backend) = mock_docker_plugin(runtime.clone());
        let first = test_spec();
        let first_handle = plugin
            .create_node_selected(first.clone(), sink.clone(), Some(11))
            .unwrap();
        plugin.start_bootstrap(&first_handle).unwrap();
        assert!(
            backend
                .finish(
                    &runtime,
                    "mock-vastai-5-7-attempt-11",
                    ScriptedDockerTerminal::Exit(23),
                )
                .unwrap()
        );
        assert!(plugin.stop_by_spec(&first, sink.clone()).unwrap());
        assert!(plugin.contracts.is_empty());
        assert!(backend.state.lock().resources.is_empty());

        let mut replacement = first;
        replacement.attempt_id += 1;
        let replacement_handle = plugin
            .create_node_selected(replacement, sink, Some(12))
            .unwrap();
        plugin.start_bootstrap(&replacement_handle).unwrap();
        assert_eq!(backend.state.lock().resources.len(), 1);
        plugin.stop_node(&replacement_handle).unwrap();
        assert!(backend.state.lock().resources.is_empty());
    }

    #[derive(Clone, Debug)]
    enum DockerAction {
        Create(u8),
        StartBootstrap(u8),
        Inspect(u8),
        Stop(u8),
        ExitBeforeReadiness(u8),
        CommandFailure(u8),
        MalformedCommandOutput(u8),
        StopWhilePending(u8),
        FailStart(u8),
        FailStop(u8),
        AdoptOrReject(u8),
        StopBySpec(u8),
    }

    fn docker_actions() -> impl Strategy<Value = Vec<DockerAction>> {
        prop::collection::vec(
            prop_oneof![
                3 => any::<u8>().prop_map(DockerAction::Create),
                3 => any::<u8>().prop_map(DockerAction::StartBootstrap),
                2 => any::<u8>().prop_map(DockerAction::Inspect),
                2 => any::<u8>().prop_map(DockerAction::Stop),
                1 => any::<u8>().prop_map(DockerAction::ExitBeforeReadiness),
                1 => any::<u8>().prop_map(DockerAction::CommandFailure),
                1 => any::<u8>().prop_map(DockerAction::MalformedCommandOutput),
                1 => any::<u8>().prop_map(DockerAction::StopWhilePending),
                1 => any::<u8>().prop_map(DockerAction::FailStart),
                1 => any::<u8>().prop_map(DockerAction::FailStop),
                2 => any::<u8>().prop_map(DockerAction::AdoptOrReject),
                2 => any::<u8>().prop_map(DockerAction::StopBySpec),
            ],
            0..=32,
        )
    }

    fn generated_docker_spec(selector: u8) -> NodeProvisionSpec {
        let mut spec = test_spec();
        spec.node_id = u64::from(selector % 4) + 1;
        spec.attempt_id = u64::from((selector / 4) % 3) + 1;
        spec
    }

    fn registered_docker_handle(
        plugin: &LocalDockerPlugin,
        spec: &NodeProvisionSpec,
    ) -> Option<PluginNodeHandle> {
        let name = docker_container_name(&plugin.container_name_prefix, spec);
        plugin.nodes.iter().find_map(|(&id, node)| {
            (node.container_name == name).then_some(PluginNodeHandle {
                id,
                provider_process_id: None,
            })
        })
    }

    fn ensure_registered_docker_attempt(
        plugin: &mut LocalDockerPlugin,
        spec: &NodeProvisionSpec,
        sink: &PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        if let Some(handle) = registered_docker_handle(plugin, spec) {
            return Ok(handle);
        }
        plugin.create_node(spec.clone(), sink.clone())
    }

    const DOCKER_STEP_BUDGET: usize = 32;
    const DOCKER_TIME_BUDGET: Duration = Duration::from_millis(1);

    fn settle_docker_actors(stepping: &SteppingBackend) {
        crate::tests::fuzz_support::advance_and_drive(
            stepping,
            DOCKER_TIME_BUDGET,
            DOCKER_STEP_BUDGET,
        );
    }

    fn reset_scripted_attempt(
        plugin: &mut LocalDockerPlugin,
        backend: &ScriptedDockerBackend,
        runtime: &Runtime,
        stepping: &SteppingBackend,
        spec: &NodeProvisionSpec,
    ) -> Result<(), String> {
        backend.clear_failures();
        if let Some(handle) = registered_docker_handle(plugin, spec) {
            plugin.stop_node(&handle)?;
            plugin.stop_node(&handle)?;
        }
        let name = docker_container_name(&plugin.container_name_prefix, spec);
        backend.remove(&name)?;
        let _ = backend.finish(runtime, &name, ScriptedDockerTerminal::CommandFailure)?;
        settle_docker_actors(stepping);
        Ok(())
    }

    fn prepare_pending_docker_attempt(
        plugin: &mut LocalDockerPlugin,
        backend: &ScriptedDockerBackend,
        runtime: &Runtime,
        stepping: &SteppingBackend,
        spec: &NodeProvisionSpec,
        sink: &PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        reset_scripted_attempt(plugin, backend, runtime, stepping, spec)?;
        let handle = plugin.create_node(spec.clone(), sink.clone())?;
        plugin.start_bootstrap(&handle)?;
        let name = docker_container_name(&plugin.container_name_prefix, spec);
        if !backend.has_waiter(&name) {
            return Err(format!(
                "scripted Docker attempt {name} started without a terminal observer"
            ));
        }
        Ok(handle)
    }

    fn docker_lifecycle_invariant(
        plugin: &LocalDockerPlugin,
        backend: &ScriptedDockerBackend,
        runtime: &Runtime,
        baseline_actors: usize,
        actions: &[DockerAction],
        replies: &[PluginObservation],
    ) -> Result<(), String> {
        let state = backend.state.lock();
        let resources = format!("{:?}", state.resources);
        let waiters = state.waiters.keys().cloned().collect::<Vec<_>>();
        let evidence = || {
            format!(
                "actions={actions:?}; resources={resources}; waiters={waiters:?}; replies={replies:?}; census=\n{}",
                crate::tests::fuzz_support::actor_census(runtime),
            )
        };

        let registered_attempts = plugin
            .nodes
            .values()
            .map(|node| node.container_name.as_str())
            .collect::<BTreeSet<_>>();
        if registered_attempts.len() != plugin.nodes.len() {
            return Err(format!(
                "duplicate Docker attempt handles were registered; {}",
                evidence()
            ));
        }

        let mut resource_counts = BTreeMap::<&str, usize>::new();
        let mut resource_ids = BTreeSet::new();
        for resource in &state.resources {
            *resource_counts
                .entry(resource.attempt.as_str())
                .or_default() += 1;
            if !resource_ids.insert(resource.id) {
                return Err(format!(
                    "scripted Docker resource id {} was reused; {}",
                    resource.id,
                    evidence()
                ));
            }
        }
        if let Some((attempt, count)) = resource_counts.iter().find(|(_, count)| **count > 1) {
            return Err(format!(
                "Docker attempt {attempt} owns {count} external resources; {}",
                evidence()
            ));
        }

        let stats = runtime.stats();
        let poisoned = stats
            .actor_details
            .iter()
            .filter(|actor| actor.poisoned)
            .collect::<Vec<_>>();
        let worker_panics = stats
            .workers
            .iter()
            .map(|worker| worker.panics)
            .sum::<u64>();
        if !poisoned.is_empty() || worker_panics != 0 {
            return Err(format!(
                "Docker lifecycle poisoned actors: poisoned={poisoned:?}, worker_panics={worker_panics}; {}",
                evidence()
            ));
        }
        let actor_ceiling = baseline_actors.saturating_add(state.waiters.len());
        if stats.actors.len() > actor_ceiling {
            return Err(format!(
                "Docker lifecycle actor count {} exceeds baseline {baseline_actors} plus {} pending terminal observers; {}",
                stats.actors.len(),
                state.waiters.len(),
                evidence()
            ));
        }
        Ok(())
    }

    proptest! {
        #![proptest_config(ProptestConfig {
            cases: 128,
            max_shrink_iters: 2_000,
            ..ProptestConfig::default()
        })]

        #[test]
        fn docker_generated_attempt_lifecycles_are_idempotent_and_bounded(
            actions in docker_actions()
        ) {
            let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
            let runtime = parts.runtime().clone();
            let stepping = SteppingBackend::new();
            let _engine = Engine::new(parts, stepping.clone()).expect("stepping engine");
            let baseline_actors = runtime.stats().actors.len();
            let recording = Arc::new(RecordingDockerSink::default());
            let sink = PluginSink::new(recording.clone());
            let backend = Arc::new(ScriptedDockerBackend::default());
            let mut plugin = LocalDockerPlugin::with_backend(
                "myelin",
                runtime.clone(),
                backend.clone(),
            );

            for action in &actions {
                let selector = match *action {
                    DockerAction::Create(selector)
                    | DockerAction::StartBootstrap(selector)
                    | DockerAction::Inspect(selector)
                    | DockerAction::Stop(selector)
                    | DockerAction::ExitBeforeReadiness(selector)
                    | DockerAction::CommandFailure(selector)
                    | DockerAction::MalformedCommandOutput(selector)
                    | DockerAction::StopWhilePending(selector)
                    | DockerAction::FailStart(selector)
                    | DockerAction::FailStop(selector)
                    | DockerAction::AdoptOrReject(selector)
                    | DockerAction::StopBySpec(selector) => selector,
                };
                let spec = generated_docker_spec(selector);
                let name = docker_container_name("myelin", &spec);

                match *action {
                    DockerAction::Create(_) => {
                        let existing = registered_docker_handle(&plugin, &spec);
                        let first = plugin.create_node(spec.clone(), sink.clone());
                        if existing.is_some() {
                            prop_assert!(
                                first.is_err(),
                                "repeated create unexpectedly replaced {}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                                name,
                                actions,
                                backend.state.lock().resources,
                                recording.snapshot(),
                                crate::tests::fuzz_support::actor_census(&runtime),
                            );
                        } else {
                            prop_assert!(
                                first.is_ok(),
                                "first create rejected {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                                name,
                                first,
                                actions,
                                backend.state.lock().resources,
                                recording.snapshot(),
                                crate::tests::fuzz_support::actor_census(&runtime),
                            );
                        }
                        let registered = registered_docker_handle(&plugin, &spec);
                        let repeated = plugin.create_node(spec.clone(), sink.clone());
                        prop_assert!(
                            repeated.is_err() && registered_docker_handle(&plugin, &spec) == registered,
                            "duplicate create was not deterministically rejected for {}: repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            repeated,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                    }
                    DockerAction::StartBootstrap(_) => {
                        let handle = ensure_registered_docker_attempt(&mut plugin, &spec, &sink);
                        prop_assert!(
                            handle.is_ok(),
                            "register before start failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            handle,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let handle = handle.unwrap();
                        let first = plugin.start_bootstrap(&handle);
                        let repeated = plugin.start_bootstrap(&handle);
                        prop_assert_eq!(
                            &repeated,
                            &first,
                            "repeated Docker start was non-deterministic for {}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        if first.is_ok() {
                            let complete = plugin.complete_bootstrap(&handle);
                            let repeated_complete = plugin.complete_bootstrap(&handle);
                            prop_assert!(
                                complete.is_ok() && repeated_complete.is_ok(),
                                "repeated Docker bootstrap completion failed for {}: first={:?}, repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                                name,
                                complete,
                                repeated_complete,
                                actions,
                                backend.state.lock().resources,
                                recording.snapshot(),
                                crate::tests::fuzz_support::actor_census(&runtime),
                            );
                        }
                    }
                    DockerAction::Inspect(_) => {
                        let first = backend.container_state(&name);
                        let repeated = backend.container_state(&name);
                        prop_assert_eq!(
                            &repeated,
                            &first,
                            "repeated Docker inspect was non-deterministic for {}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                    }
                    DockerAction::Stop(_) => {
                        let handle = ensure_registered_docker_attempt(&mut plugin, &spec, &sink);
                        prop_assert!(
                            handle.is_ok(),
                            "register before stop failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            handle,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let handle = handle.unwrap();
                        let first = plugin.stop_node(&handle);
                        let repeated = plugin.stop_node(&handle);
                        prop_assert!(
                            first.is_ok() && repeated.is_ok(),
                            "Docker stop was not idempotent for {}: first={:?}, repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            first,
                            repeated,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let terminal = backend.finish(
                            &runtime,
                            &name,
                            ScriptedDockerTerminal::Exit(137),
                        );
                        prop_assert!(
                            terminal.is_ok(),
                            "stopped Docker terminal delivery failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            terminal,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        settle_docker_actors(&stepping);
                    }
                    DockerAction::ExitBeforeReadiness(_)
                    | DockerAction::CommandFailure(_)
                    | DockerAction::MalformedCommandOutput(_)
                    | DockerAction::StopWhilePending(_) => {
                        let handle = prepare_pending_docker_attempt(
                            &mut plugin,
                            &backend,
                            &runtime,
                            &stepping,
                            &spec,
                            &sink,
                        );
                        prop_assert!(
                            handle.is_ok(),
                            "prepare pending Docker operation failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            handle,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let handle = handle.unwrap();
                        let before_replies = recording.snapshot().len();
                        let terminal = match *action {
                            DockerAction::ExitBeforeReadiness(_) => {
                                ScriptedDockerTerminal::Exit(23)
                            }
                            DockerAction::CommandFailure(_) => {
                                ScriptedDockerTerminal::CommandFailure
                            }
                            DockerAction::MalformedCommandOutput(_) => {
                                ScriptedDockerTerminal::MalformedOutput
                            }
                            DockerAction::StopWhilePending(_) => {
                                let first = plugin.stop_node(&handle);
                                let repeated = plugin.stop_node(&handle);
                                prop_assert!(
                                    first.is_ok() && repeated.is_ok(),
                                    "stop while Docker observation was pending was not idempotent for {}: first={:?}, repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                                    name,
                                    first,
                                    repeated,
                                    actions,
                                    backend.state.lock().resources,
                                    recording.snapshot(),
                                    crate::tests::fuzz_support::actor_census(&runtime),
                                );
                                ScriptedDockerTerminal::Exit(137)
                            }
                            _ => unreachable!(),
                        };
                        let delivered = backend.finish(&runtime, &name, terminal);
                        prop_assert!(
                            matches!(&delivered, Ok(true)),
                            "Docker terminal observation was not delivered for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            delivered,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        settle_docker_actors(&stepping);
                        let observations = recording.snapshot();
                        let expected_status = match terminal {
                            ScriptedDockerTerminal::Exit(status) => Some(status),
                            ScriptedDockerTerminal::CommandFailure
                            | ScriptedDockerTerminal::MalformedOutput => None,
                        };
                        prop_assert!(
                            observations[before_replies..].iter().any(|observation| {
                                matches!(
                                    observation,
                                    PluginObservation::Exited { status, .. }
                                        if *status == expected_status
                                )
                            }),
                            "Docker terminal reply was missing for {}: expected_status={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            expected_status,
                            actions,
                            backend.state.lock().resources,
                            observations,
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                    }
                    DockerAction::FailStart(_) => {
                        let reset = reset_scripted_attempt(
                            &mut plugin,
                            &backend,
                            &runtime,
                            &stepping,
                            &spec,
                        );
                        prop_assert!(
                            reset.is_ok(),
                            "reset before Docker start failure failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            reset,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let handle = plugin.create_node(spec.clone(), sink.clone());
                        prop_assert!(
                            handle.is_ok(),
                            "create before Docker start failure failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            handle,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let handle = handle.unwrap();
                        backend.fail_start();
                        let failed = plugin.start_bootstrap(&handle);
                        prop_assert!(
                            matches!(&failed, Err(error) if error.contains("start command failure")),
                            "scripted Docker start command failure was not propagated for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            failed,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let retry = plugin.start_bootstrap(&handle);
                        prop_assert!(
                            retry.is_ok(),
                            "Docker start did not recover deterministically after one command failure for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            retry,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                    }
                    DockerAction::FailStop(_) => {
                        let handle = prepare_pending_docker_attempt(
                            &mut plugin,
                            &backend,
                            &runtime,
                            &stepping,
                            &spec,
                            &sink,
                        );
                        prop_assert!(
                            handle.is_ok(),
                            "prepare before Docker stop failure failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            handle,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let handle = handle.unwrap();
                        backend.fail_remove();
                        let failed = plugin.stop_node(&handle);
                        prop_assert!(
                            matches!(&failed, Err(error) if error.contains("remove command failure"))
                                && registered_docker_handle(&plugin, &spec) == Some(handle.clone()),
                            "Docker stop failure did not retain its registered attempt for retry {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            failed,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let retry = plugin.stop_node(&handle);
                        let repeated = plugin.stop_node(&handle);
                        prop_assert!(
                            retry.is_ok() && repeated.is_ok(),
                            "Docker stop retry was not idempotent for {}: retry={:?}, repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            retry,
                            repeated,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let terminal = backend.finish(
                            &runtime,
                            &name,
                            ScriptedDockerTerminal::CommandFailure,
                        );
                        prop_assert!(
                            matches!(&terminal, Ok(true)),
                            "Docker stop failure cleanup did not close its observer for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            terminal,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        settle_docker_actors(&stepping);
                    }
                    DockerAction::AdoptOrReject(_) => {
                        let reset = reset_scripted_attempt(
                            &mut plugin,
                            &backend,
                            &runtime,
                            &stepping,
                            &spec,
                        );
                        prop_assert!(
                            reset.is_ok(),
                            "reset before Docker adoption failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            reset,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let should_exist = selector % 2 == 0;
                        if should_exist {
                            let seeded = backend.seed_orphan(&name, selector & 2 != 0);
                            prop_assert!(
                                seeded.is_ok(),
                                "seed Docker orphan failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                                name,
                                seeded,
                                actions,
                                backend.state.lock().resources,
                                recording.snapshot(),
                                crate::tests::fuzz_support::actor_census(&runtime),
                            );
                        }
                        let before_resources = backend.state.lock().resources.len();
                        let first = plugin.adopt_by_spec(&spec, sink.clone());
                        let repeated = plugin.adopt_by_spec(&spec, sink.clone());
                        prop_assert!(
                            first.is_ok() && repeated.is_ok(),
                            "Docker adoption errored for {}: first={:?}, repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            first,
                            repeated,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let first = first.unwrap();
                        let repeated = repeated.unwrap();
                        prop_assert_eq!(
                            first.as_ref().map(|node| &node.handle),
                            repeated.as_ref().map(|node| &node.handle),
                            "Docker adoption was non-deterministic for {}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        prop_assert_eq!(
                            first.is_some(),
                            should_exist,
                            "Docker adoption did not deterministically adopt/reject {}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let after_state = backend.state.lock();
                        let after_resource_count = after_state.resources.len();
                        let after_resources = after_state.resources.clone();
                        drop(after_state);
                        prop_assert_eq!(
                            after_resource_count,
                            before_resources,
                            "Docker adoption multiplied external resources for {}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            actions,
                            after_resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                    }
                    DockerAction::StopBySpec(_) => {
                        let first = plugin.stop_by_spec(&spec, sink.clone());
                        let repeated = plugin.stop_by_spec(&spec, sink.clone());
                        prop_assert!(
                            first.is_ok()
                                && repeated.is_ok()
                                && matches!(&repeated, Ok(false)),
                            "Docker spec-addressed stop was not idempotent for {}: first={:?}, repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            first,
                            repeated,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        let terminal = backend.finish(
                            &runtime,
                            &name,
                            ScriptedDockerTerminal::Exit(137),
                        );
                        prop_assert!(
                            terminal.is_ok(),
                            "Docker spec-addressed stop failed to close its observer for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                            name,
                            terminal,
                            actions,
                            backend.state.lock().resources,
                            recording.snapshot(),
                            crate::tests::fuzz_support::actor_census(&runtime),
                        );
                        settle_docker_actors(&stepping);
                    }
                }
                settle_docker_actors(&stepping);


                let observations = recording.snapshot();
                let invariant = docker_lifecycle_invariant(
                    &plugin,
                    &backend,
                    &runtime,
                    baseline_actors,
                    &actions,
                    &observations,
                );
                prop_assert!(invariant.is_ok(), "{}", invariant.unwrap_err());
            }

            backend.clear_failures();
            let handles = plugin
                .nodes
                .keys()
                .copied()
                .map(|id| PluginNodeHandle {
                    id,
                    provider_process_id: None,
                })
                .collect::<Vec<_>>();
            for handle in handles {
                let first = plugin.stop_node(&handle);
                let repeated = plugin.stop_node(&handle);
                prop_assert!(
                    first.is_ok() && repeated.is_ok(),
                    "final Docker stop was not idempotent: first={:?}, repeated={:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                    first,
                    repeated,
                    actions,
                    backend.state.lock().resources,
                    recording.snapshot(),
                    crate::tests::fuzz_support::actor_census(&runtime),
                );
            }
            let finished = backend.finish_all(&runtime);
            prop_assert!(
                finished.is_ok(),
                "final Docker terminal-observer cleanup failed: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                finished,
                actions,
                backend.state.lock().resources,
                recording.snapshot(),
                crate::tests::fuzz_support::actor_census(&runtime),
            );
            settle_docker_actors(&stepping);

            let remaining = backend
                .state
                .lock()
                .resources
                .iter()
                .map(|resource| resource.attempt.clone())
                .collect::<BTreeSet<_>>();
            for name in remaining {
                let removed = backend.remove(&name);
                prop_assert!(
                    removed.is_ok(),
                    "final scripted Docker resource cleanup failed for {}: {:?}; actions={:?}; resources={:?}; replies={:?}; census=\n{}",
                    name,
                    removed,
                    actions,
                    backend.state.lock().resources,
                    recording.snapshot(),
                    crate::tests::fuzz_support::actor_census(&runtime),
                );
            }
            let observations = recording.snapshot();
            let invariant = docker_lifecycle_invariant(
                &plugin,
                &backend,
                &runtime,
                baseline_actors,
                &actions,
                &observations,
            );
            prop_assert!(invariant.is_ok(), "{}", invariant.unwrap_err());
            let final_state = backend.state.lock();
            let resources_empty = final_state.resources.is_empty();
            let waiters_empty = final_state.waiters.is_empty();
            let final_resources = final_state.resources.clone();
            let final_waiters = final_state.waiters.keys().cloned().collect::<Vec<_>>();
            drop(final_state);
            prop_assert!(
                resources_empty
                    && waiters_empty
                    && plugin.nodes.is_empty()
                    && runtime.stats().actors.len() == baseline_actors,
                "Docker lifecycle did not return to actor/resource baseline; actions={:?}; resources={:?}; waiters={:?}; replies={:?}; census=\n{}",
                actions,
                final_resources,
                final_waiters,
                observations,
                crate::tests::fuzz_support::actor_census(&runtime),
            );
        }
    }

    #[test]
    fn docker_duplicate_resource_detector_rejects_controlled_fault() {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let stepping = SteppingBackend::new();
        let _engine = Engine::new(parts, stepping.clone()).expect("stepping engine");
        let baseline_actors = runtime.stats().actors.len();
        let recording = Arc::new(RecordingDockerSink::default());
        let sink = PluginSink::new(recording.clone());
        let backend = Arc::new(ScriptedDockerBackend::default());
        let mut plugin =
            LocalDockerPlugin::with_backend("myelin", runtime.clone(), backend.clone());
        let spec = generated_docker_spec(0);
        let name = docker_container_name("myelin", &spec);
        let handle = plugin
            .create_node(spec, sink)
            .expect("register Docker attempt");
        plugin
            .start_bootstrap(&handle)
            .expect("start one Docker resource");
        let actions = vec![DockerAction::Create(0), DockerAction::StartBootstrap(0)];
        let observations = recording.snapshot();
        docker_lifecycle_invariant(
            &plugin,
            &backend,
            &runtime,
            baseline_actors,
            &actions,
            &observations,
        )
        .expect("valid single-resource lifecycle");

        backend
            .inject_duplicate_resource(&name)
            .expect("inject controlled duplicate resource");
        let error = docker_lifecycle_invariant(
            &plugin,
            &backend,
            &runtime,
            baseline_actors,
            &actions,
            &recording.snapshot(),
        )
        .expect_err("duplicate-resource invariant must reject controlled fault");
        assert!(
            error.contains("owns 2 external resources"),
            "wrong duplicate-resource failure: {error}"
        );

        backend.clear_failures();
        plugin
            .stop_node(&handle)
            .expect("remove controlled duplicate resources");
        backend
            .finish_all(&runtime)
            .expect("finish controlled fault observer");
        settle_docker_actors(&stepping);
        let observations = recording.snapshot();
        docker_lifecycle_invariant(
            &plugin,
            &backend,
            &runtime,
            baseline_actors,
            &actions,
            &observations,
        )
        .expect("controlled fault cleanup");
        assert_eq!(
            runtime.stats().actors.len(),
            baseline_actors,
            "controlled fault leaked actors; actions={actions:?}; resources={:?}; replies={observations:?}; census=\n{}",
            backend.state.lock().resources,
            crate::tests::fuzz_support::actor_census(&runtime),
        );
    }
}
