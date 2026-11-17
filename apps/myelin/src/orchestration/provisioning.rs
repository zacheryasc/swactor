//! Myelin-local provisioning provider plugins.
//!
//! Provider-neutral lifecycle contracts live in the reusable `provisioning`
//! crate. This module keeps Myelin-local process/Docker plugin implementations
//! that know about bootstrap telemetry plumbing and local runtime execution.

use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Read;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

pub use ::provisioning::plugin::{
    AdoptedNode, NodeProvisionSpec, PluginNodeHandle, PluginObservation, PluginObservationSink,
    PluginSink, ProviderMount, ProvisionEvent, ProvisionEventKind, ProvisionLogLine,
    ProvisionLogStream, ProvisionPlugin,
};
use iroh::EndpointAddr;
pub use myelin_control_contract::DeploymentIdentity;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::{Ctx, Runtime};
use swactor_engine::BlockingWorkSender;
use swactor_process::ProcessWatch;

use crate::node::worker_node_runtime::request_debug_join;
use crate::orchestration::provisioning_logs::BootstrapTelemetryBridge;

pub struct LocalDockerPlugin {
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
    fn rejoin(&self, node: &LocalDockerNode) -> Result<(), String>;
    fn observe(&self, runtime: &Runtime, node: &LocalDockerNode, tail: &str) -> Result<(), String>;
    fn list_managed(&self, prefix: &str) -> Result<Vec<String>, String>;
}

struct SystemDockerLifecycle {
    blocking: BlockingWorkSender,
}

pub struct LocalProcessPlugin {
    program: PathBuf,
    registry_path: Option<PathBuf>,
    provider_prefix: String,
    next_handle_id: u64,
    nodes: BTreeMap<u64, LocalProcessNode>,
    runtime: Runtime,
    blocking: BlockingWorkSender,
}

struct LocalProcessNode {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    resource: LocalProcessResource,
    exit_actor: Option<ActorAddress>,
    observer: Option<ProcessWatch>,
    output: Option<ProcessOutput>,
}

enum LocalProcessResource {
    Prepared,
    Owned {
        record: LocalProcessRecord,
        runtime: LocalProcessRuntime,
    },
    Adopted(LocalProcessRecord),
}

struct LocalProcessRuntime {
    stdin: ChildStdin,
    child: Arc<Mutex<Option<Child>>>,
}

impl LocalProcessResource {
    fn record(&self) -> Option<&LocalProcessRecord> {
        match self {
            Self::Prepared => None,
            Self::Owned { record, .. } | Self::Adopted(record) => Some(record),
        }
    }
}

fn process_exit_fd(pid: u32) -> std::io::Result<OwnedFd> {
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) as i32 };
    if fd < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(unsafe { OwnedFd::from_raw_fd(fd) })
    }
}

fn file_change_fd(path: &Path) -> std::io::Result<OwnedFd> {
    let path = std::ffi::CString::new(path.as_os_str().as_bytes())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
    let raw = unsafe { libc::inotify_init1(libc::IN_CLOEXEC | libc::IN_NONBLOCK) };
    if raw < 0 {
        return Err(std::io::Error::last_os_error());
    }
    let fd = unsafe { OwnedFd::from_raw_fd(raw) };
    let mask = libc::IN_MODIFY
        | libc::IN_CLOSE_WRITE
        | libc::IN_CREATE
        | libc::IN_MOVED_TO
        | libc::IN_DELETE_SELF
        | libc::IN_MOVE_SELF;
    if unsafe { libc::inotify_add_watch(fd.as_raw_fd(), path.as_ptr(), mask) } < 0 {
        Err(std::io::Error::last_os_error())
    } else {
        Ok(fd)
    }
}

fn drain_change(fd: &OwnedFd) -> std::io::Result<()> {
    let mut bytes = [0_u8; 4096];
    loop {
        let count = unsafe { libc::read(fd.as_raw_fd(), bytes.as_mut_ptr().cast(), bytes.len()) };
        if count < 0 {
            let error = std::io::Error::last_os_error();
            match error.kind() {
                std::io::ErrorKind::WouldBlock => return Ok(()),
                std::io::ErrorKind::Interrupted => continue,
                _ => return Err(error),
            }
        }
        if count == 0 {
            return Err(std::io::Error::other("file observation closed"));
        }
        let mut offset = 0;
        while offset + std::mem::size_of::<libc::inotify_event>() <= count as usize {
            let event = unsafe {
                std::ptr::read_unaligned(bytes.as_ptr().add(offset).cast::<libc::inotify_event>())
            };
            if event.mask & libc::IN_IGNORED != 0 {
                return Err(std::io::Error::other("file observation was removed"));
            }
            offset += std::mem::size_of::<libc::inotify_event>() + event.len as usize;
        }
    }
}

struct ObserverCancellation {
    fd: OwnedFd,
    requested: AtomicBool,
}

fn cancellation_fd() -> Result<Arc<ObserverCancellation>, String> {
    let fd = unsafe { libc::eventfd(0, libc::EFD_CLOEXEC | libc::EFD_NONBLOCK) };
    if fd < 0 {
        return Err(format!(
            "create process observer cancellation: {}",
            std::io::Error::last_os_error()
        ));
    }
    Ok(Arc::new(ObserverCancellation {
        fd: unsafe { OwnedFd::from_raw_fd(fd) },
        requested: AtomicBool::new(false),
    }))
}

fn cancel_observer(cancel: &ObserverCancellation) {
    cancel.requested.store(true, Ordering::Release);
    let value = 1_u64;
    loop {
        let result = unsafe {
            libc::write(
                cancel.fd.as_raw_fd(),
                (&value as *const u64).cast(),
                std::mem::size_of::<u64>(),
            )
        };
        if result >= 0 || std::io::Error::last_os_error().kind() != std::io::ErrorKind::Interrupted
        {
            break;
        }
    }
}

struct FollowLocalFile {
    file: File,
    changed: OwnedFd,
    exited: Option<OwnedFd>,
    cancel: Arc<ObserverCancellation>,
}

impl FollowLocalFile {
    fn new(
        file: File,
        identity: swactor_process::ProcessIdentity,
        cancel: Arc<ObserverCancellation>,
    ) -> std::io::Result<Self> {
        // Watch the actual open inode, not a path that may be replaced.
        let changed = file_change_fd(Path::new(&format!("/proc/self/fd/{}", file.as_raw_fd())))?;
        let exited = match process_exit_fd(identity.pid) {
            Ok(fd) if identity.matches() => Some(fd),
            Ok(_) => None,
            Err(_) if !identity.matches() => None,
            Err(error) => return Err(error),
        };
        Ok(Self {
            file,
            changed,
            exited,
            cancel,
        })
    }
}

impl Read for FollowLocalFile {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        if buffer.is_empty() {
            return Ok(0);
        }
        loop {
            if self.cancel.requested.load(Ordering::Acquire) {
                return Ok(0);
            }
            let count = self.file.read(buffer)?;
            if count != 0 || self.exited.is_none() {
                return Ok(count);
            }
            let ready = swactor_process::wait_readable(&[
                Some(&self.changed),
                self.exited.as_ref(),
                Some(&self.cancel.fd),
            ])?;
            if ready[2] {
                return Ok(0);
            }
            if ready[1] {
                self.exited = None;
            }
            if ready[0] {
                drain_change(&self.changed)?;
            }
        }
    }
}

struct ProcessOutput {
    cancel: Arc<ObserverCancellation>,
    readers: Vec<swactor_engine::OwnedIoThread<()>>,
    actor: ActorAddress,
    runtime: Runtime,
}

impl Drop for ProcessOutput {
    fn drop(&mut self) {
        cancel_observer(&self.cancel);
        for reader in self.readers.drain(..) {
            let _ = reader.join();
        }
        let _ = self.runtime.stop_actor(self.actor);
    }
}

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum OutputPhase {
    Following,
    StdoutClosed,
    StderrClosed,
    Closed,
}

struct BootstrapOutputActor {
    bridge: BootstrapTelemetryBridge,
    phase: OutputPhase,
}

impl BootstrapOutputActor {
    fn stream_is_open(&self, stream: swactor_process::ProcessStream) -> bool {
        matches!(
            (self.phase, stream),
            (OutputPhase::Following, _)
                | (
                    OutputPhase::StdoutClosed,
                    swactor_process::ProcessStream::Stderr
                )
                | (
                    OutputPhase::StderrClosed,
                    swactor_process::ProcessStream::Stdout
                )
        )
    }

    fn line(&mut self, stream: swactor_process::ProcessStream, line: String) {
        if !self.stream_is_open(stream) {
            return;
        }
        match stream {
            swactor_process::ProcessStream::Stdout => self.bridge.observe_stdout_line(line),
            swactor_process::ProcessStream::Stderr => self.bridge.observe_stderr_line(line),
        }
    }

    fn stream_error(&mut self, stream: swactor_process::ProcessStream, error: String) {
        if self.stream_is_open(stream) {
            self.bridge
                .observe_provider_line(format!("read {stream:?}: {error}"));
        }
    }

    fn stream_closed(&mut self, ctx: &Ctx, stream: swactor_process::ProcessStream) {
        if !self.stream_is_open(stream) {
            return;
        }
        let next = match (self.phase, stream) {
            (OutputPhase::Following, swactor_process::ProcessStream::Stdout) => {
                OutputPhase::StdoutClosed
            }
            (OutputPhase::Following, swactor_process::ProcessStream::Stderr) => {
                OutputPhase::StderrClosed
            }
            _ => OutputPhase::Closed,
        };
        self.phase = next;
        if next == OutputPhase::Closed {
            ctx.stop_self();
        }
    }
}

impl ActorInterface for BootstrapOutputActor {
    type Incoming = swactor_process::ProcessStreamObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        match observation {
            swactor_process::ProcessStreamObservation::Line { stream, line } => {
                self.line(stream, line)
            }
            swactor_process::ProcessStreamObservation::Error { stream, error } => {
                self.stream_error(stream, error)
            }
            swactor_process::ProcessStreamObservation::Closed { stream } => {
                self.stream_closed(ctx, stream)
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExitPhase {
    Awaiting,
    Completed,
    Failed,
}

struct ProcessExitActor {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    record: LocalProcessRecord,
    registry: Option<(PathBuf, String)>,
    phase: ExitPhase,
}

impl ProcessExitActor {
    fn failed(&mut self, ctx: &Ctx, reason: String) {
        if self.phase != ExitPhase::Awaiting {
            return;
        }
        self.phase = ExitPhase::Failed;
        self.sink.observe(PluginObservation::Failed {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            reason,
        });
        ctx.stop_self();
    }

    fn exited(&mut self, ctx: &Ctx, observation: swactor_process::ProcessExitObservation) {
        if self.phase != ExitPhase::Awaiting {
            return;
        }
        if let Some(error) = observation.error {
            // A broken observer is not evidence that the process exited.
            self.failed(ctx, format!("wait local process node: {error}"));
            return;
        }
        let cleanup = match &self.registry {
            Some((path, provider_ref)) => update_process_registry(path, |registry| {
                if registry
                    .get(provider_ref)
                    .is_some_and(|record| record != &self.record)
                {
                    return Ok(false);
                }
                remove_debug_join_socket(&self.spec)?;
                registry.remove(provider_ref);
                Ok(true)
            })
            .and_then(|result| result),
            None => remove_debug_join_socket(&self.spec).map(|()| true),
        };
        match cleanup {
            Err(error) => self.failed(ctx, error),
            Ok(false) => self.failed(
                ctx,
                "local process observation belongs to a replaced attempt".to_owned(),
            ),
            Ok(true) => {
                self.phase = ExitPhase::Completed;
                self.sink.observe(PluginObservation::Exited {
                    run_id: self.spec.run_id,
                    node_id: self.spec.node_id,
                    status: observation.status,
                });
                ctx.stop_self();
            }
        }
    }
}

impl ActorInterface for ProcessExitActor {
    type Incoming = swactor_process::ProcessExitObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        self.exited(ctx, observation);
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.failed(
            ctx,
            "local process observer stopped before exit was established".to_owned(),
        );
    }
}

struct DockerWaitActor {
    spec: NodeProvisionSpec,
    sink: PluginSink,
    phase: ExitPhase,
}

impl DockerWaitActor {
    fn failed(&mut self, ctx: &Ctx, reason: String) {
        if self.phase != ExitPhase::Awaiting {
            return;
        }
        self.phase = ExitPhase::Failed;
        self.sink.observe(PluginObservation::Failed {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            reason,
        });
        ctx.stop_self();
    }

    fn command_finished(
        &mut self,
        ctx: &Ctx,
        observation: swactor_process::CommandOutputObservation,
    ) {
        if self.phase != ExitPhase::Awaiting {
            return;
        }
        if let Some(error) = observation.error {
            self.failed(ctx, format!("wait Docker container: {error}"));
            return;
        }
        if observation.status != Some(0) {
            self.failed(
                ctx,
                format!(
                    "docker wait command failed: status={:?}",
                    observation.status
                ),
            );
            return;
        }
        let Ok(status) = String::from_utf8_lossy(&observation.stdout)
            .trim()
            .parse::<i32>()
        else {
            self.failed(
                ctx,
                "docker wait returned an invalid container exit status".to_owned(),
            );
            return;
        };
        self.phase = ExitPhase::Completed;
        self.sink.observe(PluginObservation::Exited {
            run_id: self.spec.run_id,
            node_id: self.spec.node_id,
            status: Some(status),
        });
        ctx.stop_self();
    }
}

impl ActorInterface for DockerWaitActor {
    type Incoming = swactor_process::CommandOutputObservation;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, observation: Self::Incoming) {
        self.command_finished(ctx, observation);
    }

    fn on_stop(&mut self, ctx: &Ctx) {
        self.failed(
            ctx,
            "Docker observer stopped before exit was established".to_owned(),
        );
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
    pub(crate) fn new(
        blocking: BlockingWorkSender,
        program: impl Into<PathBuf>,
        runtime: Runtime,
    ) -> Self {
        Self {
            program: program.into(),
            registry_path: None,
            provider_prefix: "process".to_owned(),
            next_handle_id: 1,
            nodes: BTreeMap::new(),
            runtime,
            blocking,
        }
    }

    pub fn with_registry(
        blocking: BlockingWorkSender,
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
            blocking,
        }
    }
    fn stop_node_inner(&mut self, handle: &PluginNodeHandle, cancel: bool) -> Result<(), String> {
        let Some(node) = self.nodes.get_mut(&handle.id) else {
            return Ok(());
        };
        let provider_ref = local_process_provider_ref(&self.provider_prefix, &node.spec);
        let status = match &mut node.resource {
            LocalProcessResource::Prepared => None,
            LocalProcessResource::Owned { runtime, .. } if cancel => {
                swactor_process::cancel_shared_child(&runtime.child)
                    .map_err(|error| format!("cancel local process: {error}"))?
                    .and_then(|status| status.code())
            }
            LocalProcessResource::Adopted(record) if cancel => {
                swactor_process::cancel_process_group(&record.identity())?;
                None
            }
            LocalProcessResource::Owned { runtime, .. } => stop_owned_process(runtime)?,
            LocalProcessResource::Adopted(record) => {
                stop_adopted_process(record)?;
                None
            }
        };
        // All fallible cleanup precedes removing the handle. In particular,
        // registry I/O failure must leave the process identity available to retry.
        if let Some(path) = self.registry_path.as_deref() {
            update_process_registry(path, |registry| {
                if registry
                    .get(&provider_ref)
                    .is_some_and(|record| node.resource.record() != Some(record))
                {
                    return Ok(());
                }
                remove_debug_join_socket(&node.spec)?;
                registry.remove(&provider_ref);
                Ok(())
            })
            .and_then(|result| result)?;
        } else {
            remove_debug_join_socket(&node.spec)?;
        }
        if let Some(exit_actor) = node.exit_actor {
            // The terminal observer owns the sole completion, including when
            // its OS observation was already queued ahead of this stop.
            let _ = self.runtime.send_to(
                exit_actor,
                swactor_process::ProcessExitObservation {
                    status,
                    error: None,
                },
            );
        } else {
            node.sink.observe(PluginObservation::Exited {
                run_id: node.spec.run_id,
                node_id: node.spec.node_id,
                status,
            });
        }
        self.nodes.remove(&handle.id);
        Ok(())
    }
}

impl Drop for LocalProcessPlugin {
    fn drop(&mut self) {
        // A durable process is intentionally adoptable after the adapter goes
        // away, but its old caller and observer actor must not stay pending.
        for node in self.nodes.values_mut() {
            if let Some(actor) = node.exit_actor.take() {
                let _ = self.runtime.stop_actor(actor);
            }
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
    let nonce = ActorAddress::new_random();
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
    // Recovery reconciles the current OS inventory once. Only a concrete
    // worker identity can establish ownership of subsequent readiness events.
    let mut matches = swactor_process::find_process_identities_by_environment(&environment, true)
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

fn recovery_route_env(spec: &NodeProvisionSpec) -> Result<Option<(&str, &str)>, String> {
    let Some(endpoint_json) = spec
        .env
        .iter()
        .find_map(|(key, value)| (key == "MYELIN_COORDINATOR_ENDPOINT").then_some(value.as_str()))
    else {
        return Ok(None);
    };
    let actor_json = spec
        .env
        .iter()
        .find_map(|(key, value)| (key == "MYELIN_ORCHESTRATOR_ACTOR").then_some(value.as_str()))
        .ok_or_else(|| "recovery spec has no orchestrator actor".to_owned())?;
    Ok(Some((endpoint_json, actor_json)))
}

fn request_process_rejoin(
    spec: &NodeProvisionSpec,
    record: &LocalProcessRecord,
) -> Result<(), String> {
    let Some((endpoint_json, actor_json)) = recovery_route_env(spec)? else {
        return Ok(());
    };
    let endpoint = serde_json::from_str::<EndpointAddr>(endpoint_json)
        .map_err(|error| format!("parse recovery coordinator endpoint: {error}"))?;
    let orchestrator_actor = serde_json::from_str::<ActorAddress>(actor_json)
        .map_err(|error| format!("parse recovery orchestrator actor: {error}"))?;
    let socket = debug_join_socket_path(spec);
    let parent = socket
        .parent()
        .ok_or_else(|| format!("rejoin socket {} has no parent directory", socket.display()))?;
    let change = file_change_fd(parent)
        .map_err(|error| format!("observe rejoin socket {}: {error}", socket.display()))?;
    let exited = process_exit_fd(record.pid)
        .map_err(|error| format!("observe rejoining worker {}: {error}", record.pid))?;
    if !record.identity().matches() {
        return Err(format!(
            "rejoining worker {} is no longer owned",
            record.pid
        ));
    }
    while !socket
        .try_exists()
        .map_err(|error| format!("inspect rejoin socket {}: {error}", socket.display()))?
    {
        let ready = swactor_process::wait_readable(&[Some(&change), Some(&exited)])
            .map_err(|error| format!("wait rejoin socket {}: {error}", socket.display()))?;
        if ready[1] {
            return Err(format!(
                "worker {} exited before its rejoin socket was ready",
                record.pid
            ));
        }
        drain_change(&change)
            .map_err(|error| format!("observe rejoin socket {}: {error}", socket.display()))?;
    }
    request_debug_join(&socket, endpoint, orchestrator_actor)
        .map_err(|error| format!("rejoin local worker through {}: {error}", socket.display()))
}

fn observe_process_files(
    blocking: &BlockingWorkSender,
    runtime: &Runtime,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    record: LocalProcessRecord,
) -> Result<ProcessOutput, String> {
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
    let cancel = cancellation_fd()?;
    let stdout = FollowLocalFile::new(stdout, record.identity(), Arc::clone(&cancel))
        .map_err(|error| format!("observe local process stdout: {error}"))?;
    let stderr = FollowLocalFile::new(stderr, record.identity(), Arc::clone(&cancel))
        .map_err(|error| format!("observe local process stderr: {error}"))?;
    let actor = runtime
        .spawn(BootstrapOutputActor {
            bridge: BootstrapTelemetryBridge::new(spec, sink, None),
            phase: OutputPhase::Following,
        })
        .map_err(|error| format!("spawn local process output actor: {error}"))?;
    let sender = runtime.create_sender();
    let mut output = ProcessOutput {
        cancel,
        readers: Vec::with_capacity(2),
        actor,
        runtime: runtime.clone(),
    };
    output.readers.push(
        swactor_process::spawn_owned_line_reader(
            blocking,
            swactor_process::ProcessStream::Stdout,
            stdout,
            sender.clone(),
            actor,
        )
        .map_err(|error| format!("start local process stdout observer: {error}"))?,
    );
    output.readers.push(
        swactor_process::spawn_owned_line_reader(
            blocking,
            swactor_process::ProcessStream::Stderr,
            stderr,
            sender,
            actor,
        )
        .map_err(|error| format!("start local process stderr observer: {error}"))?,
    );
    Ok(output)
}

fn observe_output_streams(
    blocking: &BlockingWorkSender,
    runtime: &Runtime,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    stdout: impl std::io::Read + Send + 'static,
    stderr: impl std::io::Read + Send + 'static,
) -> Result<(), String> {
    let actor = runtime
        .spawn(BootstrapOutputActor {
            bridge: BootstrapTelemetryBridge::new(spec, sink, None),
            phase: OutputPhase::Following,
        })
        .map_err(|error| format!("spawn bootstrap output actor: {error}"))?;
    let sender = runtime.create_sender();
    swactor_process::spawn_line_reader(
        blocking,
        swactor_process::ProcessStream::Stdout,
        stdout,
        sender.clone(),
        actor,
    );
    swactor_process::spawn_line_reader(
        blocking,
        swactor_process::ProcessStream::Stderr,
        stderr,
        sender,
        actor,
    );
    Ok(())
}

impl LocalDockerPlugin {
    pub fn new(
        blocking: BlockingWorkSender,
        container_name_prefix: impl Into<String>,
        runtime: Runtime,
    ) -> Self {
        Self::with_backend(
            container_name_prefix,
            runtime,
            Arc::new(SystemDockerLifecycle { blocking }),
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

pub fn docker_container_is_absent(name: &str) -> Result<bool, String> {
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

pub fn docker_container_is_running(name: &str) -> Result<bool, String> {
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
    let mut source =
        File::open(path).map_err(|error| format!("open {}: {error}", path.display()))?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = [0_u8; 64 * 1024];
    loop {
        let count = source
            .read(&mut bytes)
            .map_err(|error| format!("read {}: {error}", path.display()))?;
        if count == 0 {
            break;
        }
        hasher.update(&bytes[..count]);
    }
    let file = path
        .file_name()
        .and_then(|name| name.to_str())
        .map(safe_docker_volume_component)
        .unwrap_or_else(|| "model".to_owned());
    Ok(format!(
        "myelin-cache-{file}-{}",
        hasher.finalize().to_hex()
    ))
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
    swactor_process::stop_shared_child_with_input(&runtime.child, &mut runtime.stdin, b"shutdown\n")
        .map(|status| status.and_then(|status| status.code()))
        .map_err(|error| format!("stop local process: {error}"))
}

fn stop_adopted_process(record: &LocalProcessRecord) -> Result<(), String> {
    swactor_process::terminate_process_group(&record.identity())
        .map_err(|error| format!("stop adopted node {}: {error}", record.pid))
}

fn observe_adopted_process(
    blocking: &BlockingWorkSender,
    runtime: &Runtime,
    spec: NodeProvisionSpec,
    sink: PluginSink,
    registry_path: PathBuf,
    provider_ref: String,
    record: LocalProcessRecord,
) -> Result<(ActorAddress, ProcessWatch), String> {
    let actor = runtime
        .spawn(ProcessExitActor {
            spec,
            sink,
            record: record.clone(),
            phase: ExitPhase::Awaiting,

            registry: Some((registry_path, provider_ref)),
        })
        .map_err(|error| format!("spawn adopted process observer actor: {error}"))?;
    match swactor_process::watch_process(
        blocking,
        record.identity(),
        None,
        runtime.create_sender(),
        actor,
    ) {
        Ok(watch) => Ok((actor, watch)),
        Err(error) => {
            let _ = runtime.stop_actor(actor);
            Err(format!("watch adopted process: {error}"))
        }
    }
}

impl ProvisionPlugin for LocalProcessPlugin {
    fn create_node(
        &mut self,
        spec: NodeProvisionSpec,
        sink: PluginSink,
    ) -> Result<PluginNodeHandle, String> {
        if self.nodes.values().any(|node| {
            node.spec.run_id == spec.run_id
                && node.spec.node_id == spec.node_id
                && node.spec.attempt_id == spec.attempt_id
        }) {
            return Err("local process attempt is already registered".to_owned());
        }
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
                resource: LocalProcessResource::Prepared,
                exit_actor: None,
                observer: None,
                output: None,
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
        if node.resource.record().is_some() {
            return if node.observer.is_some() && node.output.is_some() {
                Ok(())
            } else {
                Err("local process is retained after incomplete bootstrap; stop or recover it before retrying".to_owned())
            };
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
        // Install ownership before any fallible registry or observer work.
        let child = Arc::new(Mutex::new(Some(child)));
        node.resource = LocalProcessResource::Owned {
            record: record.clone(),
            runtime: LocalProcessRuntime {
                stdin,
                child: Arc::clone(&child),
            },
        };
        if let Some(path) = registry_path.as_deref() {
            update_process_registry(path, |registry| {
                registry.insert(provider_ref.clone(), record.clone());
            })?;
        }
        node.output = Some(observe_process_files(
            &self.blocking,
            &self.runtime,
            spec.clone(),
            sink.clone(),
            record.clone(),
        )?);
        let exit_actor = self
            .runtime
            .spawn(ProcessExitActor {
                spec,
                sink,
                record: record.clone(),
                phase: ExitPhase::Awaiting,
                registry: registry_path.map(|path| (path, provider_ref)),
            })
            .map_err(|error| format!("spawn local process observer actor: {error}"))?;
        node.exit_actor = Some(exit_actor);
        node.observer = Some(
            swactor_process::watch_process(
                &self.blocking,
                record.identity(),
                Some(child),
                self.runtime.create_sender(),
                exit_actor,
            )
            .map_err(|error| {
                let _ = self.runtime.stop_actor(exit_actor);
                format!("watch local process: {error}")
            })?,
        );
        Ok(())
    }

    fn complete_bootstrap(&mut self, _handle: &PluginNodeHandle) -> Result<(), String> {
        Ok(())
    }

    fn stop_node(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.stop_node_inner(handle, false)
    }

    fn cancel_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.stop_node_inner(handle, true)
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
        if let Some((&id, node)) = self.nodes.iter().find(|(_, node)| {
            node.spec.run_id == spec.run_id
                && node.spec.node_id == spec.node_id
                && node.spec.attempt_id == spec.attempt_id
        }) {
            if let Some(record) = node.resource.record() {
                request_process_rejoin(spec, record)?;
                if node.observer.is_none() || node.output.is_none() {
                    return Err(
                        "retained local process has incomplete observers; cleanup is required"
                            .to_owned(),
                    );
                }
                return Ok(Some(AdoptedNode {
                    handle: PluginNodeHandle {
                        id,
                        provider_process_id: Some(record.pid),
                    },
                    provider_ref,
                }));
            }
        }
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
        request_process_rejoin(spec, &record)?;
        let output = observe_process_files(
            &self.blocking,
            &self.runtime,
            spec.clone(),
            sink.clone(),
            record.clone(),
        )?;
        let (exit_actor, observer) = observe_adopted_process(
            &self.blocking,
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
                resource: LocalProcessResource::Adopted(record),
                exit_actor: Some(exit_actor),
                observer: Some(observer),
                output: Some(output),
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
        if let Some((&id, _)) = self.nodes.iter().find(|(_, node)| {
            node.spec.run_id == spec.run_id
                && node.spec.node_id == spec.node_id
                && node.spec.attempt_id == spec.attempt_id
        }) {
            self.stop_node(&PluginNodeHandle {
                id,
                provider_process_id: None,
            })?;
            return Ok(true);
        }
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

impl DockerLifecycleBackend for SystemDockerLifecycle {
    fn container_state(&self, name: &str) -> Result<Option<bool>, String> {
        if docker_container_is_absent(name)? {
            Ok(None)
        } else {
            docker_container_is_running(name).map(Some)
        }
    }

    fn start(&self, prefix: &str, runtime: &Runtime, node: &LocalDockerNode) -> Result<(), String> {
        start_system_docker_container(&self.blocking, prefix, runtime, node)
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

    fn rejoin(&self, node: &LocalDockerNode) -> Result<(), String> {
        let Some((endpoint_json, actor_json)) = recovery_route_env(&node.spec)? else {
            return Ok(());
        };
        // Docker keeps the original worker environment on adoption. Send the
        // live endpoint and control actor through its existing debug socket;
        // replaying old readiness logs cannot reconnect either control route.
        let mut command = Command::new("docker");
        command.arg("exec");

        command
            .arg(&node.container_name)
            .args(["/usr/local/bin/myelin-node", "debug-join", "--socket"])
            .arg(debug_join_socket_path(&node.spec))
            .arg("--endpoint-json")
            .arg(endpoint_json)
            .arg("--orchestrator-actor-json")
            .arg(actor_json);
        let output = swactor_process::command_output_owned(&self.blocking, &mut command)
            .map_err(|error| format!("rejoin Docker worker {}: {error}", node.container_name))?;
        if !output.status.success() {
            return Err(format!(
                "rejoin Docker worker {} exited with {}: stdout={}, stderr={}",
                node.container_name,
                output.status,
                String::from_utf8_lossy(&output.stdout).trim(),
                String::from_utf8_lossy(&output.stderr).trim()
            ));
        }
        Ok(())
    }

    fn observe(&self, runtime: &Runtime, node: &LocalDockerNode, tail: &str) -> Result<(), String> {
        observe_docker_container(
            &self.blocking,
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
    blocking: &BlockingWorkSender,
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
    observe_docker_container(blocking, runtime, spec, sink, container_name, "all")
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

    fn cancel_bootstrap(&mut self, handle: &PluginNodeHandle) -> Result<(), String> {
        self.stop_node(handle)
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
            node.spec = spec.clone();
            if self.backend.container_state(&node.container_name)? == Some(true) {
                self.backend.rejoin(node)?;
            }
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
            self.backend.rejoin(&node)?;
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
    blocking: &BlockingWorkSender,
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
    let observers = (|| {
        let stdout = logs
            .stdout
            .take()
            .ok_or_else(|| format!("Docker logs for {container_name} has no stdout"))?;
        let stderr = logs
            .stderr
            .take()
            .ok_or_else(|| format!("Docker logs for {container_name} has no stderr"))?;
        let logs_waiter = runtime
            .spawn(DiscardProcessExit)
            .map_err(|error| format!("spawn Docker log waiter actor: {error}"))?;
        let wait_actor = match runtime.spawn(DockerWaitActor {
            spec: spec.clone(),
            sink: sink.clone(),
            phase: ExitPhase::Awaiting,
        }) {
            Ok(actor) => actor,
            Err(error) => {
                let _ = runtime.stop_actor(logs_waiter);
                return Err(format!("spawn Docker waiter actor: {error}"));
            }
        };
        if let Err(error) = observe_output_streams(blocking, runtime, spec, sink, stdout, stderr) {
            let _ = runtime.stop_actor(logs_waiter);
            let _ = runtime.stop_actor(wait_actor);
            return Err(error);
        }
        Ok((logs_waiter, wait_actor))
    })();
    let (logs_waiter, wait_actor) = match observers {
        Ok(actors) => actors,
        Err(error) => {
            let _ = swactor_process::child_kill(&mut logs);
            let _ = swactor_process::child_wait(&mut logs);
            return Err(error);
        }
    };
    if let Err(mut logs) =
        swactor_process::spawn_child_wait(blocking, logs, runtime.create_sender(), logs_waiter)
    {
        let _ = runtime.stop_actor(wait_actor);
        let _ = swactor_process::child_kill(&mut logs);
        swactor_process::child_wait(&mut logs)
            .map_err(|error| format!("reap rejected Docker logs observer: {error}"))?;
        return Err("engine rejected Docker logs observer".to_owned());
    }
    let mut wait = Command::new("docker");
    wait.arg("wait").arg(container_name);
    swactor_process::spawn_command_output(blocking, wait, runtime.create_sender(), wait_actor);
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
            deployment: None,
            run_id: 5,
            node_id: 7,
            attempt_id: 11,
            stage_index: Some(0),
            image: "node:v1".to_owned(),
            env: Vec::new(),
            args: Vec::new(),
            offer_criteria_json: None,
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

    #[cfg(target_os = "linux")]
    #[test]
    fn local_process_stop_releases_all_observer_actors() {
        // /proc/self/fd is process-wide; sibling tests must not contribute
        // descriptors to this resource-ownership assertion.
        const CHILD_ENV: &str = "MYELIN_LOCAL_PROCESS_FD_TEST_CHILD";
        if std::env::var_os(CHILD_ENV).is_none() {
            let (_engine, _) = test_runtime();
            let output = swactor_process::command_output_owned(
                &_engine.handle().blocking_work_sender(),
                std::process::Command::new(std::env::current_exe().unwrap())
                    .args([
                        "--exact",
                        "provisioning::tests::local_process_stop_releases_all_observer_actors",
                        "--nocapture",
                    ])
                    .env(CHILD_ENV, "1"),
            )
            .expect("complete isolated local-process resource check");
            assert!(
                output.status.success(),
                "isolated local-process resource check failed:\n{}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr),
            );
            return;
        }
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let stepping = SteppingBackend::new();
        let _engine = Engine::new(parts, stepping.clone()).expect("stepping engine");
        let baseline_actors = runtime.stats().actors.len();
        let baseline_fds = fs::read_dir("/proc/self/fd").unwrap().count();
        let (tx, rx) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut spec = test_spec();
        spec.env
            .push(("MYELIN_RUN_ID".to_owned(), spec.run_id.to_string()));
        spec.env.push((
            "MYELIN_LOGICAL_NODE_ID".to_owned(),
            spec.node_id.to_string(),
        ));
        let root = tempfile::tempdir().unwrap();
        let socket_path = root.path().join("worker.sock");
        fs::write(&socket_path, []).unwrap();
        spec.env.push((
            "MYELIN_DEBUG_JOIN_SOCKET".to_owned(),
            socket_path.display().to_string(),
        ));
        spec.args = vec![
            "-c".to_owned(),
            "printf 'started\n'; IFS= read -r line".to_owned(),
        ];
        let mut plugin = LocalProcessPlugin::new(
            _engine.handle().blocking_work_sender(),
            "/bin/sh",
            runtime.clone(),
        );

        let handle = plugin.create_node(spec, sink).unwrap();
        assert!(matches!(rx.try_recv(), Err(mpsc::TryRecvError::Empty)));

        plugin.start_bootstrap(&handle).unwrap();
        plugin.stop_node(&handle).unwrap();
        assert!(
            !socket_path.exists(),
            "local process debug socket survived explicit stop"
        );

        stepping
            .join_blocking()
            .expect("join stopped process observers");
        settle_docker_actors(&stepping);
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
        let root = tempfile::tempdir().unwrap();
        let registry_path = root.path().join("registry.json");
        let fifo = root.path().join("worker-input");
        let fifo_path = std::ffi::CString::new(fifo.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) }, 0);
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let mut spec = test_spec();
        spec.env
            .push(("MYELIN_RUN_ID".to_owned(), spec.run_id.to_string()));
        spec.env.push((
            "MYELIN_LOGICAL_NODE_ID".to_owned(),
            spec.node_id.to_string(),
        ));
        spec.env
            .push(("MYELIN_TEST_FIFO".to_owned(), fifo.display().to_string()));
        spec.args = vec![
            "-c".to_owned(),
            "trap 'exit 0' TERM; exec 3<>\"$MYELIN_TEST_FIFO\"; IFS= read -r line <&3".to_owned(),
        ];
        let provider_ref = local_process_provider_ref("process", &spec);

        let record = {
            let mut plugin = LocalProcessPlugin::with_registry(
                _engine.handle().blocking_work_sender(),
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

        let mut restarted = LocalProcessPlugin::with_registry(
            _engine.handle().blocking_work_sender(),
            "/bin/sh",
            &registry_path,
            "process",
            runtime,
        );
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

    #[test]
    fn missing_local_bootstrap_recreates_only_the_provider_handle() {
        let (_engine, runtime) = test_runtime();
        let (tx, _) = mpsc::channel();
        let sink = PluginSink::new(Arc::new(ChannelSink(tx)));
        let spec = test_spec();
        let mut process = LocalProcessPlugin::new(
            _engine.handle().blocking_work_sender(),
            "/not/executed",
            runtime.clone(),
        );
        let process_node = process
            .prepare_missing_bootstrap(&spec, sink.clone())
            .unwrap()
            .expect("process bootstrap handle");
        assert_eq!(process_node.provider_ref, "process-5-7-attempt-11");
        assert_eq!(process_node.handle.provider_process_id, None);

        let mut docker =
            LocalDockerPlugin::new(_engine.handle().blocking_work_sender(), "myelin", runtime);
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
        fail_next_rejoin: bool,
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
                    phase: ExitPhase::Awaiting,
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

        fn rejoin(&self, _node: &LocalDockerNode) -> Result<(), String> {
            if std::mem::take(&mut self.state.lock().fail_next_rejoin) {
                return Err("scripted Docker worker rejected rejoin".to_owned());
            }
            Ok(())
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
    fn docker_adoption_requires_rejoin_without_replacing_retained_resource() {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let stepping = SteppingBackend::new();
        let _engine = Engine::new(parts, stepping.clone()).expect("stepping engine");
        let baseline_actors = runtime.stats().actors.len();
        let sink = PluginSink::new(Arc::new(RecordingDockerSink::default()));
        let backend = Arc::new(ScriptedDockerBackend::default());
        let mut plugin =
            LocalDockerPlugin::with_backend("myelin", runtime.clone(), backend.clone());
        let spec = test_spec();
        let name = docker_container_name("myelin", &spec);
        backend.seed_orphan(&name, true).unwrap();
        let retained_id = backend.state.lock().resources[0].id;
        backend.state.lock().fail_next_rejoin = true;

        // Existing bootstrap logs are not proof of a current control binding.
        // A rejected rejoin must not report a successful adoption or dispose
        // of the live worker, so recovery can retry the same external resource.
        assert!(plugin.adopt_by_spec(&spec, sink.clone()).is_err());
        assert!(plugin.nodes.is_empty());
        assert_eq!(plugin.list_managed_refs().unwrap(), [name.clone()]);
        assert_eq!(backend.container_state(&name).unwrap(), Some(true));
        assert_eq!(backend.state.lock().resources[0].id, retained_id);
        assert_eq!(runtime.stats().actors.len(), baseline_actors);

        let adopted = plugin
            .adopt_by_spec(&spec, sink.clone())
            .unwrap()
            .expect("retained container adopted after rejoin");
        assert_eq!(adopted.provider_ref, name);
        assert_eq!(backend.state.lock().resources[0].id, retained_id);

        // An already registered handle also has to reconnect on recovery;
        // failure retains its ownership rather than silently accepting it.
        backend.state.lock().fail_next_rejoin = true;
        assert!(plugin.adopt_by_spec(&spec, sink.clone()).is_err());
        let retried = plugin
            .adopt_by_spec(&spec, sink)
            .unwrap()
            .expect("registered container rejoined");
        assert_eq!(retried.handle.id, adopted.handle.id);
        assert_eq!(plugin.list_managed_refs().unwrap(), [name.clone()]);
        assert_eq!(backend.state.lock().resources[0].id, retained_id);

        plugin.stop_node(&retried.handle).unwrap();
        backend.finish_all(&runtime).unwrap();
        settle_docker_actors(&stepping);
        assert!(plugin.list_managed_refs().unwrap().is_empty());
        assert_eq!(runtime.stats().actors.len(), baseline_actors);
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

    fn settle_docker_actors(stepping: &SteppingBackend) {
        crate::tests::fuzz_support::drive_steps(stepping, DOCKER_STEP_BUDGET);
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
                                match (terminal, observation) {
                                    (ScriptedDockerTerminal::Exit(expected), PluginObservation::Exited { status, .. }) => *status == Some(expected),
                                    (ScriptedDockerTerminal::CommandFailure | ScriptedDockerTerminal::MalformedOutput, PluginObservation::Failed { .. }) => true,
                                    _ => false,
                                }
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

    #[test]
    fn duplicate_stdout_close_keeps_stderr_delivery_owned() {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let stepping = SteppingBackend::new();
        let _engine = Engine::new(parts, stepping.clone()).unwrap();
        let baseline = runtime.stats().actors.len();
        let recording = Arc::new(RecordingDockerSink::default());
        let actor = runtime
            .spawn(BootstrapOutputActor {
                bridge: BootstrapTelemetryBridge::new(
                    test_spec(),
                    PluginSink::new(recording.clone()),
                    None,
                ),
                phase: OutputPhase::Following,
            })
            .unwrap();
        for _ in 0..2 {
            runtime
                .send_to(
                    actor,
                    swactor_process::ProcessStreamObservation::Closed {
                        stream: swactor_process::ProcessStream::Stdout,
                    },
                )
                .unwrap();
        }
        runtime
            .send_to(
                actor,
                swactor_process::ProcessStreamObservation::Line {
                    stream: swactor_process::ProcessStream::Stdout,
                    line: "stale stdout".to_owned(),
                },
            )
            .unwrap();
        runtime
            .send_to(
                actor,
                swactor_process::ProcessStreamObservation::Line {
                    stream: swactor_process::ProcessStream::Stderr,
                    line: "still owned".to_owned(),
                },
            )
            .unwrap();
        settle_docker_actors(&stepping);
        assert_eq!(
            recording.snapshot(),
            [PluginObservation::StderrLine {
                run_id: 5,
                node_id: 7,
                line: "still owned".to_owned(),
            }]
        );
        assert_eq!(runtime.stats().actors.len(), baseline + 1);

        runtime
            .send_to(
                actor,
                swactor_process::ProcessStreamObservation::Closed {
                    stream: swactor_process::ProcessStream::Stderr,
                },
            )
            .unwrap();
        settle_docker_actors(&stepping);
        assert_eq!(runtime.stats().actors.len(), baseline);
    }

    fn retained_test_process(spec: &NodeProvisionSpec, root: &Path) -> LocalProcessRecord {
        LocalProcessRecord {
            // This cannot identify a live Linux PID; no process is acquired.
            pid: u32::MAX,
            stdout_path: root.join("stdout"),
            stderr_path: root.join("stderr"),
            run_id: spec.run_id,
            node_id: spec.node_id,
            attempt_id: spec.attempt_id,
        }
    }

    #[test]
    fn process_observer_failure_or_parent_stop_resolves_once_without_erasing_ownership() {
        for parent_stop in [false, true] {
            let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
            let runtime = parts.runtime().clone();
            let stepping = SteppingBackend::new();
            let _engine = Engine::new(parts, stepping.clone()).unwrap();
            let baseline = runtime.stats().actors.len();
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("registry.json");
            let socket = root.path().join("worker.sock");
            fs::write(&socket, []).unwrap();
            let mut spec = test_spec();
            spec.env.push((
                "MYELIN_DEBUG_JOIN_SOCKET".to_owned(),
                socket.display().to_string(),
            ));
            let record = retained_test_process(&spec, root.path());
            let provider_ref = local_process_provider_ref("process", &spec);
            write_process_registry(
                &path,
                &BTreeMap::from([(provider_ref.clone(), record.clone())]),
            )
            .unwrap();
            let recording = Arc::new(RecordingDockerSink::default());
            let actor = runtime
                .spawn(ProcessExitActor {
                    spec: spec.clone(),
                    sink: PluginSink::new(recording.clone()),
                    record: record.clone(),
                    registry: Some((path.clone(), provider_ref.clone())),
                    phase: ExitPhase::Awaiting,
                })
                .unwrap();
            settle_docker_actors(&stepping);
            if parent_stop {
                let mut plugin = LocalProcessPlugin::with_registry(
                    _engine.handle().blocking_work_sender(),
                    "/not/executed",
                    &path,
                    "process",
                    runtime.clone(),
                );
                let handle = plugin
                    .create_node(spec, PluginSink::new(recording.clone()))
                    .unwrap();
                let node = plugin.nodes.get_mut(&handle.id).unwrap();
                node.resource = LocalProcessResource::Adopted(record.clone());
                node.exit_actor = Some(actor);
                drop(plugin);
            } else {
                runtime
                    .send_to(
                        actor,
                        swactor_process::ProcessExitObservation {
                            status: None,
                            error: Some("observer unavailable".to_owned()),
                        },
                    )
                    .unwrap();
                runtime
                    .send_to(
                        actor,
                        swactor_process::ProcessExitObservation {
                            status: Some(0),
                            error: None,
                        },
                    )
                    .unwrap();
            }
            settle_docker_actors(&stepping);
            assert!(matches!(
                recording.snapshot().as_slice(),
                [PluginObservation::Failed { .. }]
            ));
            assert_eq!(
                read_process_registry(&path).unwrap().get(&provider_ref),
                Some(&record)
            );
            assert!(socket.exists());
            assert_eq!(runtime.stats().actors.len(), baseline);
        }
    }

    #[test]
    fn process_exit_cleans_once_and_stale_exit_preserves_replacement() {
        for replaced in [false, true] {
            let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
            let runtime = parts.runtime().clone();
            let stepping = SteppingBackend::new();
            let _engine = Engine::new(parts, stepping.clone()).unwrap();
            let root = tempfile::tempdir().unwrap();
            let path = root.path().join("registry.json");
            let socket = root.path().join("worker.sock");
            fs::write(&socket, []).unwrap();
            let mut spec = test_spec();
            spec.env.push((
                "MYELIN_DEBUG_JOIN_SOCKET".to_owned(),
                socket.display().to_string(),
            ));
            let record = retained_test_process(&spec, root.path());
            let mut registered = record.clone();
            if replaced {
                registered.attempt_id += 1;
                registered.pid -= 1;
            }
            let provider_ref = local_process_provider_ref("process", &spec);
            write_process_registry(
                &path,
                &BTreeMap::from([(provider_ref.clone(), registered.clone())]),
            )
            .unwrap();
            let recording = Arc::new(RecordingDockerSink::default());
            let actor = runtime
                .spawn(ProcessExitActor {
                    spec,
                    sink: PluginSink::new(recording.clone()),
                    record,
                    registry: Some((path.clone(), provider_ref.clone())),
                    phase: ExitPhase::Awaiting,
                })
                .unwrap();
            for _ in 0..2 {
                runtime
                    .send_to(
                        actor,
                        swactor_process::ProcessExitObservation {
                            status: Some(17),
                            error: None,
                        },
                    )
                    .unwrap();
            }
            settle_docker_actors(&stepping);
            if replaced {
                assert!(matches!(
                    recording.snapshot().as_slice(),
                    [PluginObservation::Failed { .. }]
                ));
                assert_eq!(
                    read_process_registry(&path).unwrap().get(&provider_ref),
                    Some(&registered)
                );
                assert!(socket.exists());
            } else {
                assert_eq!(
                    recording.snapshot(),
                    [PluginObservation::Exited {
                        run_id: 5,
                        node_id: 7,
                        status: Some(17),
                    }]
                );
                assert!(read_process_registry(&path).unwrap().is_empty());
                assert!(!socket.exists());
            }
            assert!(runtime.stats().actors.is_empty());
        }
    }

    #[test]
    fn failed_process_stop_retains_identity_until_cleanup_retry() {
        let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let stepping = SteppingBackend::new();
        let engine = Engine::new(parts, stepping.clone()).unwrap();
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("registry.json");
        let spec = test_spec();
        let record = retained_test_process(&spec, root.path());
        let recording = Arc::new(RecordingDockerSink::default());
        let sink = PluginSink::new(recording.clone());
        let mut plugin = LocalProcessPlugin::with_registry(
            engine.handle().blocking_work_sender(),
            "/not/executed",
            &path,
            "process",
            runtime,
        );
        let handle = plugin.create_node(spec.clone(), sink.clone()).unwrap();
        plugin.nodes.get_mut(&handle.id).unwrap().resource =
            LocalProcessResource::Adopted(record.clone());
        fs::write(&path, b"invalid registry").unwrap();
        assert!(plugin.stop_node(&handle).is_err());
        assert_eq!(
            plugin.nodes.get(&handle.id).unwrap().resource.record(),
            Some(&record)
        );
        assert!(plugin.create_node(spec.clone(), sink).is_err());
        assert!(recording.snapshot().is_empty());

        write_process_registry(
            &path,
            &BTreeMap::from([(local_process_provider_ref("process", &spec), record)]),
        )
        .unwrap();
        plugin.cancel_bootstrap(&handle).unwrap();
        plugin.stop_node(&handle).unwrap();
        assert!(plugin.nodes.is_empty());
        assert!(read_process_registry(&path).unwrap().is_empty());
        assert_eq!(
            recording.snapshot(),
            [PluginObservation::Exited {
                run_id: 5,
                node_id: 7,
                status: None,
            }]
        );
        settle_docker_actors(&stepping);
    }

    #[test]
    fn docker_terminal_and_parent_stop_release_observers_once() {
        for parent_stop in [false, true] {
            let parts = RuntimeParts::new(swactor::config::RuntimeConfig::default());
            let runtime = parts.runtime().clone();
            let stepping = SteppingBackend::new();
            let _engine = Engine::new(parts, stepping.clone()).unwrap();
            let recording = Arc::new(RecordingDockerSink::default());
            let backend = Arc::new(ScriptedDockerBackend::default());
            let mut plugin =
                LocalDockerPlugin::with_backend("myelin", runtime.clone(), backend.clone());
            let spec = test_spec();
            let name = docker_container_name("myelin", &spec);
            let handle = plugin
                .create_node(spec.clone(), PluginSink::new(recording.clone()))
                .unwrap();
            plugin.start_bootstrap(&handle).unwrap();
            settle_docker_actors(&stepping);
            let (actor, resource_id) = {
                let state = backend.state.lock();
                (state.waiters[&name], state.resources[0].id)
            };
            if parent_stop {
                runtime.stop_actor(actor).unwrap();
                backend.state.lock().waiters.remove(&name);
            } else {
                for _ in 0..2 {
                    runtime
                        .send_to(actor, ScriptedDockerTerminal::Exit(0).observation())
                        .unwrap();
                }
                backend.state.lock().waiters.remove(&name);
            }
            settle_docker_actors(&stepping);
            if parent_stop {
                assert!(matches!(
                    recording.snapshot().as_slice(),
                    [PluginObservation::Failed { .. }]
                ));
            } else {
                assert_eq!(
                    recording.snapshot(),
                    [PluginObservation::Exited {
                        run_id: 5,
                        node_id: 7,
                        status: Some(0),
                    }]
                );
            }
            // Observer shutdown never relinquishes the provider resource.
            assert_eq!(backend.state.lock().resources[0].id, resource_id);
            plugin.cancel_bootstrap(&handle).unwrap();
            plugin.stop_node(&handle).unwrap();
            assert!(backend.state.lock().resources.is_empty());
            assert!(runtime.stats().actors.is_empty());
        }
    }
}
