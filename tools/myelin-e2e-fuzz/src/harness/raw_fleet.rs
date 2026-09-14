//! Raw Docker fleet fixture for the deployment E2E.
//!
//! Blank nodes: each container is created from the production base image
//! (`apps/myelin/node-image/Dockerfile.base`) with no Myelin binaries, no
//! Swactor wheel, and no E2E launcher. Each node lives on its own Docker
//! bridge, so no shared Docker LAN exists between nodes and cross-bridge
//! forwarding is blocked by Docker's inter-network isolation. The only way
//! in is the published per-node SSH port on loopback.
//!
//! The fixture owns node lifecycle: `ensure` creates or rediscovers the
//! containers (stable fixture/slot labels survive harness crashes),
//! `teardown` removes them. Refresh means destroy-and-recreate, never
//! incremental repair of half-booted state. The durable ownership manifest is
//! authoritative across runner restarts; names alone never establish ownership.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Seek};
use std::net::TcpStream;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::budget::{Budget, record_execution_stage};

pub(crate) const BASE_IMAGE: &str = "myelin-node-base:cuda12.6";

const SSH_BANNER_TIMEOUT: Duration = Duration::from_secs(30);
const SSH_BANNER_POLL: Duration = Duration::from_millis(200);

#[derive(Clone, Debug)]
pub struct RawNode {
    pub slot: u32,
    pub container: String,
    pub network: String,
    pub ssh_host: String,
    pub ssh_port: u16,
    container_claim: RawResourceClaim,
    network_claim: RawResourceClaim,
}

pub struct RawDockerFleet {
    pub nodes: Vec<RawNode>,
    pub identity: PathBuf,
    pub prefix: String,
    prior_processes: Mutex<BTreeMap<u32, Vec<RawProcessFact>>>,
    budget: Budget,
    cleanup_budget: Budget,
    manifest: RawManifestStore,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawFixtureNodeIdentity {
    pub logical_node_id: u64,
    pub slot: u32,
    pub container: String,
    pub container_id: String,
    pub network: String,
    pub network_id: String,
    pub ssh_host: String,
    pub ssh_port: u16,
}

#[derive(Clone, Debug, Default)]
struct RawResourceClaim {
    // A create reservation permits label-based reconciliation, not name-based
    // deletion. Once observed, the immutable provider ID can never be replaced.
    creation: Option<String>,
    id: OnceLock<String>,
}

#[derive(Clone, Copy, Debug)]
enum RawResourceKind {
    Container,
    Network,
}

#[derive(Debug)]
struct RawResourceObservation {
    id: String,
    name: String,
    labels: BTreeMap<String, String>,
}

impl RawResourceClaim {
    fn reserve_create(&mut self) -> Result<&str, String> {
        let mut nonce = [0_u8; 16];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut file| file.read_exact(&mut nonce))
            .map_err(|error| format!("reserve raw resource creation identity: {error}"))?;
        self.creation = Some(format!("{:032x}", u128::from_ne_bytes(nonce)));
        Ok(self
            .creation
            .as_deref()
            .expect("creation identity just reserved"))
    }

    fn verify(
        &self,
        observed: &RawResourceObservation,
        fixture: &str,
        slot: u32,
    ) -> Result<(), String> {
        if observed.id.len() != 64 || !observed.id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(format!(
                "raw resource {} has an invalid immutable ID",
                observed.name
            ));
        }
        if observed
            .labels
            .get("myelin.raw-fixture")
            .map(String::as_str)
            != Some(fixture)
            || observed.labels.get("myelin.raw-slot") != Some(&slot.to_string())
            || self.creation.as_ref().is_some_and(|creation| {
                observed.labels.get("myelin.raw-creation") != Some(creation)
            })
        {
            return Err(format!(
                "raw resource {} ({}) does not belong to fixture {fixture} slot {slot}",
                observed.name, observed.id
            ));
        }
        if self.id.get().is_some_and(|id| id != &observed.id) {
            return Err(format!(
                "raw resource {} was replaced: retained {:?}, observed {}",
                observed.name,
                self.id.get(),
                observed.id
            ));
        }
        Ok(())
    }

    fn adopt(
        &self,
        observed: &RawResourceObservation,
        fixture: &str,
        slot: u32,
    ) -> Result<&str, String> {
        self.verify(observed, fixture, slot)?;
        // Cleanup and construction may both reconcile a reserved creation.
        // Never allow a losing observation to replace the first accepted ID.
        self.id.get_or_init(|| observed.id.clone());
        self.verify(observed, fixture, slot)?;
        self.retained_id()
    }

    fn retained_id(&self) -> Result<&str, String> {
        self.id
            .get()
            .map(String::as_str)
            .ok_or_else(|| "raw resource has no verified immutable identity".to_owned())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawFleetSnapshot {
    pub nodes: Vec<RawFixtureNodeIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawProcessFact {
    pub pid: u32,
    pub start_ticks: u64,
    pub parent_pid: u32,
    pub process_group: u32,
    pub command: String,
    pub executable_digest: Option<String>,
    pub artifact_digest: Option<String>,
    pub deployment_generation: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RawNodeCensus {
    pub logical_node_id: u64,
    pub container: String,
    pub active_deployment_raw: Option<String>,
    pub active_link_target: Option<String>,
    pub active_executable_digest: Option<String>,
    pub agent_pid: Option<u32>,
    pub worker_processes: Vec<RawProcessFact>,
    pub worker_group_processes: Vec<RawProcessFact>,
    pub prior_processes: Vec<RawProcessFact>,
    pub surviving_prior_processes: Vec<RawProcessFact>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RawPriorState {
    NoWorkerOrTrustworthyMetadata,
    HealthyStaleWorker,
    DeadWorkerWithStaleMetadata,
    MultipleStaleWorkersAndDescendants,
    InterruptedTransferAndPartialInstall,
    CorruptActiveBinary,
    CorruptActivationPointer,
    CorruptDeploymentDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentBoundary {
    TransferStarted,
    BeforeLaunch,
    AfterLaunch,
    BeforeReceipt,
}

impl DeploymentBoundary {
    pub fn as_remote_name(self) -> &'static str {
        match self {
            Self::TransferStarted => "transfer_started",
            Self::BeforeLaunch => "before_launch",
            Self::AfterLaunch => "after_launch",
            Self::BeforeReceipt => "before_receipt",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestClaim {
    creation: String,
    attempted: bool,
    id: Option<String>,
}

impl ManifestClaim {
    fn new() -> Result<Self, String> {
        let mut claim = RawResourceClaim::default();
        Ok(Self {
            creation: claim.reserve_create()?.to_owned(),
            attempted: false,
            id: None,
        })
    }

    fn retained(&self) -> RawResourceClaim {
        RawResourceClaim {
            creation: self.attempted.then(|| self.creation.clone()),
            id: self.id.clone().map(OnceLock::from).unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestNode {
    slot: u32,
    container: String,
    network: String,
    ssh_host: String,
    ssh_port: u16,
    container_claim: ManifestClaim,
    network_claim: ManifestClaim,
}

impl ManifestNode {
    fn retained(&self) -> RawNode {
        RawNode {
            slot: self.slot,
            container: self.container.clone(),
            network: self.network.clone(),
            ssh_host: self.ssh_host.clone(),
            ssh_port: self.ssh_port,
            container_claim: self.container_claim.retained(),
            network_claim: self.network_claim.retained(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    fixture: String,
    base_image: String,
    image_id: String,
    public_key: String,
    ready: bool,
    retired: bool,
    nodes: Vec<ManifestNode>,
}

impl Manifest {
    fn fresh(node_count: u32, image_id: String, public_key: String) -> Result<Self, String> {
        let fixture = format!("myelin-e2e-raw-{}", ManifestClaim::new()?.creation);
        let nodes = (0..node_count)
            .map(|slot| {
                Ok(ManifestNode {
                    slot,
                    container: format!("{fixture}-node-{slot}"),
                    network: format!("{fixture}-net-{slot}"),
                    ssh_host: "127.0.0.1".to_owned(),
                    ssh_port: 0,
                    container_claim: ManifestClaim::new()?,
                    network_claim: ManifestClaim::new()?,
                })
            })
            .collect::<Result<_, String>>()?;
        let manifest = Self {
            schema_version: 1,
            fixture,
            base_image: BASE_IMAGE.to_owned(),
            image_id,
            public_key,
            ready: false,
            retired: false,
            nodes,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    fn load(path: &Path) -> Result<Option<Self>, String> {
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(format!(
                    "read raw-fleet manifest {}: {error}",
                    path.display()
                ));
            }
        };
        let manifest: Self = serde_json::from_str(&text).map_err(|error| format!(
            "unsupported or ambiguous raw-fleet manifest {}; retained without replacement: {error}",
            path.display(),
        ))?;
        manifest.validate()?;
        Ok(Some(manifest))
    }

    fn validate(&self) -> Result<(), String> {
        let hex = |value: &str, len| {
            value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit())
        };
        if self.schema_version != 1
            || !self
                .fixture
                .strip_prefix("myelin-e2e-raw-")
                .is_some_and(|nonce| hex(nonce, 32))
            || self.base_image != BASE_IMAGE
            || !self
                .image_id
                .strip_prefix("sha256:")
                .is_some_and(|id| hex(id, 64))
            || self.public_key.is_empty()
            || self.nodes.is_empty()
        {
            return Err(
                "unsupported raw-fleet identity/configuration; manifest retained".to_owned(),
            );
        }
        let mut nonces = BTreeSet::new();
        let mut ids = BTreeSet::new();
        for (slot, node) in self.nodes.iter().enumerate() {
            if node.slot as usize != slot
                || node.container != format!("{}-node-{slot}", self.fixture)
                || node.network != format!("{}-net-{slot}", self.fixture)
                || node.ssh_host != "127.0.0.1"
                || (self.ready && node.ssh_port == 0)
                || (node.ssh_port != 0 && node.container_claim.id.is_none())
            {
                return Err(
                    "raw-fleet slot/endpoint configuration changed; manifest retained".to_owned(),
                );
            }
            for claim in [&node.container_claim, &node.network_claim] {
                if !hex(&claim.creation, 32)
                    || !nonces.insert(&claim.creation)
                    || (claim.id.is_some() && !claim.attempted)
                    || (self.ready && claim.id.is_none())
                    || claim
                        .id
                        .as_ref()
                        .is_some_and(|id| !hex(id, 64) || !ids.insert(id))
                {
                    return Err(
                        "ambiguous raw-fleet resource ownership; manifest retained".to_owned()
                    );
                }
            }
        }
        Ok(())
    }

    fn require_configuration(&self, node_count: u32, public_key: &str) -> Result<(), String> {
        if self.nodes.len() != node_count as usize || self.public_key != public_key {
            return Err(
                "retained raw-fleet node count or SSH identity differs; manifest retained"
                    .to_owned(),
            );
        }
        Ok(())
    }

    fn persist(&self, path: &Path) -> Result<(), String> {
        let parent = path
            .parent()
            .ok_or_else(|| "raw-fleet manifest has no parent".to_owned())?;
        let mut temporary = tempfile::NamedTempFile::new_in(parent)
            .map_err(|error| format!("create raw-fleet manifest temporary file: {error}"))?;
        serde_json::to_writer_pretty(temporary.as_file_mut(), self)
            .map_err(|error| format!("serialize raw-fleet manifest: {error}"))?;
        temporary
            .as_file()
            .sync_all()
            .map_err(|error| format!("sync raw-fleet manifest: {error}"))?;
        temporary
            .persist(path)
            .map_err(|error| format!("commit raw-fleet manifest: {error}"))?;
        std::fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| format!("sync raw-fleet manifest directory: {error}"))
    }
}

struct RawManifestStore {
    path: PathBuf,
    state: Mutex<Manifest>,
    // The open description owns the flock until this fleet is dropped.
    _lock: std::fs::File,
}

impl RawManifestStore {
    fn lock(artifacts: &Path) -> Result<std::fs::File, String> {
        let lock = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(artifacts.join("raw-fleet.lock"))
            .map_err(|error| format!("open raw-fleet ownership lock: {error}"))?;
        if unsafe { libc::flock(lock.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(format!(
                "raw-fleet artifacts already owned or unavailable: {}",
                std::io::Error::last_os_error()
            ));
        }
        Ok(lock)
    }

    fn update(
        &self,
        change: impl FnOnce(&mut Manifest) -> Result<(), String>,
    ) -> Result<(), String> {
        let mut manifest = self
            .state
            .lock()
            .map_err(|_| "raw-fleet manifest lock poisoned".to_owned())?;
        change(&mut manifest)?;
        manifest.validate()?;
        manifest.persist(&self.path)
    }

    fn begin_creation(&self, slot: u32, kind: RawResourceKind) -> Result<String, String> {
        let mut nonce = String::new();
        self.update(|manifest| {
            let node = &mut manifest.nodes[slot as usize];
            let claim = match kind {
                RawResourceKind::Container => &mut node.container_claim,
                RawResourceKind::Network => &mut node.network_claim,
            };
            if claim.attempted || claim.id.is_some() {
                return Err("refusing to replace a previously attempted raw resource".to_owned());
            }
            claim.attempted = true;
            nonce.clone_from(&claim.creation);
            Ok(())
        })?;
        Ok(nonce)
    }

    fn record(&self, node: &RawNode) -> Result<(), String> {
        self.update(|manifest| {
            let retained = &mut manifest.nodes[node.slot as usize];
            for (saved, claim) in [
                (&mut retained.container_claim, &node.container_claim),
                (&mut retained.network_claim, &node.network_claim),
            ] {
                if let Some(id) = claim.id.get() {
                    if !saved.attempted
                        || claim.creation.as_ref() != Some(&saved.creation)
                        || saved.id.as_ref().is_some_and(|saved| saved != id)
                    {
                        return Err(
                            "refusing to overwrite retained raw resource ownership".to_owned()
                        );
                    }
                    saved.id = Some(id.clone());
                }
            }
            retained.ssh_port = node.ssh_port;
            Ok(())
        })
    }
}

/// The owner retains the child until it has been killed and reaped. Output
/// goes to anonymous files, so a descendant cannot strand a pipe-reader join.
fn command_output(
    command: &mut Command,
    budget: &Budget,
    predicate: &str,
) -> Result<Output, String> {
    let started = Instant::now();
    budget.check(predicate)?;
    let mut stdout = tempfile::tempfile().map_err(|error| format!("{predicate}: {error}"))?;
    let mut stderr = tempfile::tempfile().map_err(|error| format!("{predicate}: {error}"))?;
    command.process_group(0);
    command.stdout(Stdio::from(
        stdout.try_clone().map_err(|error| error.to_string())?,
    ));
    command.stderr(Stdio::from(
        stderr.try_clone().map_err(|error| error.to_string())?,
    ));
    let mut child = command
        .spawn()
        .map_err(|error| format!("{predicate}: {error}"))?;
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, child.id(), 0) as i32 };
    let exit = (fd >= 0).then(|| unsafe { OwnedFd::from_raw_fd(fd) });
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) => {}
            Err(error) => break Err(format!("{predicate}: wait: {error}")),
        }
        let remaining = match budget.remaining(predicate) {
            Ok(remaining) => remaining,
            Err(error) => break Err(error),
        };
        let delay = remaining.min(Duration::from_millis(50));
        if let Some(exit) = &exit {
            let mut descriptor = libc::pollfd {
                fd: exit.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            unsafe {
                libc::poll(&mut descriptor, 1, delay.as_millis().max(1) as i32);
            }
        } else if let Err(error) = budget.wait(delay, predicate) {
            break Err(error);
        }
    };
    if status.is_err() {
        unsafe {
            libc::kill(-(child.id() as i32), libc::SIGKILL);
        }
        let _ = child.kill();
        child
            .wait()
            .map_err(|error| format!("{predicate}: reap: {error}"))?;
    }
    if status.is_err() {
        record_execution_stage(predicate, started.elapsed(), 0, 1);
    }
    let status = status?;
    stdout.rewind().map_err(|error| error.to_string())?;
    stderr.rewind().map_err(|error| error.to_string())?;
    let mut output = Output {
        status,
        stdout: Vec::new(),
        stderr: Vec::new(),
    };
    stdout
        .read_to_end(&mut output.stdout)
        .map_err(|error| error.to_string())?;
    stderr
        .read_to_end(&mut output.stderr)
        .map_err(|error| error.to_string())?;
    record_execution_stage(
        predicate,
        started.elapsed(),
        (output.stdout.len() + output.stderr.len()) as u64,
        1,
    );
    budget.check(predicate)?;
    Ok(output)
}

fn run_checked(command: &mut Command, predicate: &str, budget: &Budget) -> Result<(), String> {
    let output = command_output(command, budget, predicate)?;
    if output.status.success() {
        Ok(())
    } else {
        Err(format!(
            "{predicate}: {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        ))
    }
}

fn docker(args: &[&str], budget: &Budget) -> Result<String, String> {
    let output = command_output(
        Command::new("docker").args(args),
        budget,
        &format!("raw.docker.{}", args.first().copied().unwrap_or("command")),
    )?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).into_owned())
    } else {
        Err(format!(
            "docker {:?} exited {}: {}",
            args,
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn docker_shell(container: &str, script: &str, budget: &Budget) -> Result<String, String> {
    // Docker killing its client does not terminate an exec inside a container.
    // The remote timeout therefore owns every helper independently as well.
    let seconds = budget
        .remaining("raw Docker exec")?
        .as_secs_f64()
        .to_string();
    docker(
        &[
            "exec", container, "timeout", "-s", "KILL", &seconds, "sh", "-lc", script,
        ],
        budget,
    )
    .map_err(|error| format!("raw node {container}: {error}"))
}

fn parse_optional_u32(value: &str, description: &str) -> Result<Option<u32>, String> {
    let value = value.trim();
    if value.is_empty() {
        return Ok(None);
    }
    value
        .parse::<u32>()
        .map(Some)
        .map_err(|error| format!("parse {description} {value:?}: {error}"))
}

fn validate_remote_token(value: &str, description: &str) -> Result<(), String> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(format!("{description} {value:?} is not a safe token"));
    }
    Ok(())
}

fn stop_worker_trees_script() -> &'static str {
    r#"python3 - <<'PY' || exit $?
import os, select, signal, time
from pathlib import Path
def stat(pid):
    try:
        text = Path('/proc', str(pid), 'stat').read_text()
        name, fields = text[text.index('(') + 1:].rsplit(')', 1)
        fields = fields.split()
        return int(fields[19]), int(fields[1]), int(fields[2]), name, fields[0]
    except (FileNotFoundError, ProcessLookupError):
        return None
captured = {}
deadline = time.monotonic() + 10
try:
    while True:
        before = len(captured)
        processes = {int(p.name): stat(int(p.name)) for p in Path('/proc').iterdir() if p.name.isdecimal()}
        groups = {v[2] for v in processes.values() if v and v[3] == 'myelin-worker'}
        for pid, value in processes.items():
            if not value or pid <= 1 or pid == os.getpid():
                continue
            if value[3] == 'myelin-worker' or value[2] in groups or value[1] in {p for p, _ in captured}:
                key = pid, value[0]
                if key not in captured:
                    try:
                        fd = os.pidfd_open(pid)
                    except ProcessLookupError:
                        continue
                    current = stat(pid)
                    if current is None or current[0] != value[0]:
                        os.close(fd)
                        continue
                    captured[key] = fd
                    try:
                        signal.pidfd_send_signal(fd, signal.SIGSTOP)
                    except ProcessLookupError:
                        pass
        stopped = all((current := stat(pid)) is None or current[0] != ticks
                      or current[4] in ('T', 't', 'Z', 'X') for pid, ticks in captured)
        if len(captured) == before and stopped:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError('stale worker tree capture')
    for fd in captured.values():
        try:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
        except ProcessLookupError:
            pass
    pending = set(captured)
    while pending:
        pending = {key for key in pending if (value := stat(key[0])) and value[0] == key[1]}
        if not pending:
            break
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('stale process incarnations remain: %r' % sorted(pending))
        # pidfds wake on exit; /proc reconciliation additionally proves reaping.
        poll = select.poll()
        for key in pending:
            poll.register(captured[key], select.POLLIN)
        if poll.poll(min(remaining, .05) * 1000):
            time.sleep(min(remaining, .005))
finally:
    for fd in captured.values():
        try:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
        except ProcessLookupError:
            pass
        os.close(fd)
PY
:"#
}

fn process_census(container: &str, budget: &Budget) -> Result<Vec<RawProcessFact>, String> {
    let output = docker_shell(
        container,
        r#"python3 - <<'PY'
import hashlib
import json
from pathlib import Path

processes = []
for directory in Path("/proc").iterdir():
    if not directory.name.isdecimal():
        continue
    try:
        stat = (directory / "stat").read_text()
        fields = stat.rsplit(")", 1)[1].split()
        command = stat.split("(", 1)[1].rsplit(")", 1)[0]
        executable_digest = None
        artifact_digest = None
        deployment_generation = None
        if command == "myelin-worker":
            try:
                with (directory / "exe").open("rb") as executable:
                    executable_digest = "sha256:" + hashlib.file_digest(executable, "sha256").hexdigest()
            except (FileNotFoundError, ProcessLookupError):
                # A zombie still has an incarnation even though exe is gone.
                pass
        try:
            for entry in (directory / "environ").read_bytes().split(b'\0'):
                if entry.startswith(b'MYELIN_ARTIFACT_DIGEST=sha256:') and len(entry) > 30:
                    artifact_digest = entry.split(b'=', 1)[1].decode('utf-8', errors='replace')
                elif entry.startswith(b'MYELIN_DEPLOYMENT_GENERATION=') and len(entry) > 29:
                    deployment_generation = entry.split(b'=', 1)[1].decode('utf-8', errors='replace')
        except (FileNotFoundError, ProcessLookupError, PermissionError):
            pass
        current = (directory / "stat").read_text().rsplit(")", 1)[1].split()
        if fields[19] != current[19]:
            continue
        processes.append({
            "pid": int(directory.name),
            "start_ticks": int(fields[19]),
            "parent_pid": int(fields[1]),
            "process_group": int(fields[2]),
            "command": command,
            "executable_digest": executable_digest,
            "artifact_digest": artifact_digest,
            "deployment_generation": deployment_generation,
        })
    except (FileNotFoundError, ProcessLookupError):
        continue
print(json.dumps(processes))
PY"#,
        budget,
    )?;
    serde_json::from_str(&output)
        .map_err(|error| format!("parse raw process census for {container}: {error}"))
}

fn related_worker_processes(
    processes: &[RawProcessFact],
    prior: &[RawProcessFact],
) -> Vec<RawProcessFact> {
    let mut groups = processes
        .iter()
        .filter(|process| {
            matches!(process.command.as_str(), "myelin-worker" | "swactor")
                || (process.artifact_digest.is_some() && process.deployment_generation.is_some())
        })
        .map(|process| process.process_group)
        .collect::<BTreeSet<_>>();
    for old in prior {
        let leader = processes
            .iter()
            .find(|process| process.pid == old.process_group);
        if leader.is_none_or(|leader| {
            prior.iter().any(|original| {
                original.pid == leader.pid && original.start_ticks == leader.start_ticks
            })
        }) {
            groups.insert(old.process_group);
        }
    }
    let mut related = processes
        .iter()
        .filter(|process| {
            groups.contains(&process.process_group)
                || prior
                    .iter()
                    .any(|old| old.pid == process.pid && old.start_ticks == process.start_ticks)
        })
        .map(|process| process.pid)
        .collect::<BTreeSet<_>>();
    loop {
        let mut changed = false;
        for process in processes {
            if related.contains(&process.parent_pid) && related.insert(process.pid) {
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    let mut related = processes
        .iter()
        .filter(|process| related.contains(&process.pid))
        .cloned()
        .collect::<Vec<_>>();
    related.sort_by_key(|process| (process.pid, process.start_ticks));
    related
}

fn container_running(container: &str, budget: &Budget) -> Result<bool, String> {
    let output = command_output(
        Command::new("docker").args(["inspect", "-f", "{{.State.Running}}", container]),
        budget,
        "inspect raw container running",
    )?;
    if !output.status.success() {
        return Err(format!(
            "docker inspect {container} exited {}",
            output.status
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim() == "true")
}

fn container_has_init(container: &str, budget: &Budget) -> Result<bool, String> {
    docker(
        &["inspect", "-f", "{{.HostConfig.Init}}", container],
        budget,
    )
    .map(|value| value.trim() == "true")
}

fn published_ssh_port(container: &str, budget: &Budget) -> Result<u16, String> {
    let text = docker(&["port", container, "22"], budget)?;
    let mut endpoints = text.lines();
    let port = endpoints
        .next()
        .and_then(|line| line.strip_prefix("127.0.0.1:"))
        .and_then(|port| port.parse::<u16>().ok())
        .filter(|port| *port != 0);
    match (port, endpoints.next()) {
        (Some(port), None) => Ok(port),
        _ => Err(format!(
            "container {container} has no exclusive loopback SSH endpoint"
        )),
    }
}

fn resource_census(
    kind: RawResourceKind,
    creation: Option<&str>,
    budget: &Budget,
) -> Result<Vec<(String, String)>, String> {
    let mut args = match kind {
        RawResourceKind::Container => vec!["ps", "-a", "--no-trunc"],
        RawResourceKind::Network => vec!["network", "ls", "--no-trunc"],
    };
    let filter = creation.map(|nonce| format!("label=myelin.raw-creation={nonce}"));
    if let Some(filter) = &filter {
        args.extend(["--filter", filter]);
    }
    args.extend([
        "--format",
        match kind {
            RawResourceKind::Container => "{{.ID}}\t{{.Names}}",
            RawResourceKind::Network => "{{.ID}}\t{{.Name}}",
        },
    ]);
    docker(&args, budget)?
        .lines()
        .map(|line| {
            let (id, name) = line
                .split_once('\t')
                .ok_or_else(|| format!("invalid raw {kind:?} census row: {line:?}"))?;
            if id.len() != 64 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) || name.is_empty()
            {
                return Err(format!("invalid raw {kind:?} census identity: {line:?}"));
            }
            Ok((id.to_owned(), name.to_owned()))
        })
        .collect()
}

fn inspect_resource(
    kind: RawResourceKind,
    reference: &str,
    budget: &Budget,
) -> Result<Option<RawResourceObservation>, String> {
    let output = match kind {
        RawResourceKind::Container => docker(&["container", "inspect", reference], budget),
        RawResourceKind::Network => docker(&["network", "inspect", reference], budget),
    };
    let output = match output {
        Ok(output) => output,
        Err(error) => {
            // Only a successful exact-ID/name census proves absence. In
            // particular, a timeout or daemon error must not erase ownership.
            return match resource_census(kind, None, budget) {
                Ok(resources)
                    if !resources
                        .iter()
                        .any(|(id, name)| id == reference || name == reference) =>
                {
                    Ok(None)
                }
                Ok(_) => Err(error),
                Err(census) => Err(format!("{error}; {census}")),
            };
        }
    };
    let mut values: Vec<serde_json::Value> = serde_json::from_str(&output)
        .map_err(|error| format!("decode raw {kind:?} identity: {error}"))?;
    if values.len() != 1 {
        return Err(format!(
            "raw {kind:?} {reference} has ambiguous inspection results"
        ));
    }
    let value = values.pop().expect("exactly one inspected resource");
    let id = value["Id"]
        .as_str()
        .ok_or_else(|| format!("raw {kind:?} {reference} has no provider ID"))?
        .to_owned();
    let name = value["Name"]
        .as_str()
        .ok_or_else(|| format!("raw {kind:?} {reference} has no provider name"))?
        .trim_start_matches('/')
        .to_owned();
    if reference != id && reference != name {
        return Err(format!(
            "raw {kind:?} {reference} resolved to a different resource {id} ({name})"
        ));
    }
    let labels = match kind {
        RawResourceKind::Container => &value["Config"]["Labels"],
        RawResourceKind::Network => &value["Labels"],
    };
    let labels = serde_json::from_value::<Option<BTreeMap<String, String>>>(labels.clone())
        .map_err(|error| format!("decode raw {kind:?} ownership labels: {error}"))?
        .unwrap_or_default();
    Ok(Some(RawResourceObservation { id, name, labels }))
}

fn owned_resource(
    kind: RawResourceKind,
    claim: &RawResourceClaim,
    fixture: &str,
    slot: u32,
    budget: &Budget,
) -> Result<Option<RawResourceObservation>, String> {
    let observed = if let Some(id) = claim.id.get() {
        inspect_resource(kind, id, budget)?
    } else if let Some(creation) = &claim.creation {
        let candidates = resource_census(kind, Some(creation), budget)?;
        // Another cleanup owner may have reconciled and removed the resource
        // while this label census was running. Its pinned ID remains decisive.
        if let Some(id) = claim.id.get() {
            inspect_resource(kind, id, budget)?
        } else if candidates.len() == 1 {
            inspect_resource(kind, &candidates[0].0, budget)?
        } else {
            return Err(format!(
                "unresolved raw {kind:?} creation {creation} for {fixture} slot {slot}: \
                 expected one owned identity, found {}",
                candidates.len()
            ));
        }
    } else {
        // A planned name that failed adoption is not a cleanup capability.
        return Ok(None);
    };
    if let Some(observed) = &observed {
        claim.adopt(observed, fixture, slot)?;
    } else if claim.id.get().is_none() {
        return Err(format!(
            "unresolved raw {kind:?} creation {:?} for {fixture} slot {slot}",
            claim.creation
        ));
    }
    Ok(observed)
}

fn remove_resource(kind: RawResourceKind, id: &str, budget: &Budget) -> Result<(), String> {
    let removal = match kind {
        RawResourceKind::Container => docker(&["rm", "-f", id], budget),
        RawResourceKind::Network => docker(&["network", "rm", id], budget),
    };
    if let Err(error) = removal {
        match resource_census(kind, None, budget) {
            Ok(resources) if !resources.iter().any(|(observed, _)| observed == id) => {}
            Ok(_) => return Err(error),
            Err(census) => return Err(format!("{error}; {census}")),
        }
    }
    Ok(())
}

fn container_ip_on_network(container: &str, budget: &Budget) -> Result<String, String> {
    let text = docker(
        &[
            "inspect",
            "-f",
            "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",
            container,
        ],
        budget,
    )?;
    let ip = text.trim().to_owned();
    if ip.is_empty() {
        return Err(format!("container {container} has no network address"));
    }
    Ok(ip)
}

fn ensure_base_image(workspace: &Path, budget: &Budget) -> Result<(), String> {
    // Always present the build to Docker. Layer caching keeps this cheap and
    // avoids silently reusing a tag built from an older production base.
    run_checked(
        Command::new("docker").current_dir(workspace).args([
            "build",
            "-f",
            "apps/myelin/node-image/Dockerfile.base",
            "-t",
            BASE_IMAGE,
            ".",
        ]),
        "build raw-fleet base image",
        budget,
    )
}

fn ensure_identity(artifacts: &Path, budget: &Budget, required: bool) -> Result<PathBuf, String> {
    let identity = crate::resources::private_fixture_dir(artifacts)?.join("raw-fleet-id_ed25519");
    if identity.is_file() {
        return Ok(identity);
    }
    if required {
        return Err(format!(
            "retained raw-fleet SSH identity {} is missing; manifest retained",
            identity.display()
        ));
    }
    run_checked(
        Command::new("ssh-keygen")
            .args(["-t", "ed25519", "-N", "", "-C", "myelin-raw-fleet", "-f"])
            .arg(&identity),
        "generate raw-fleet SSH identity",
        budget,
    )?;
    Ok(identity)
}

fn public_key(identity: &Path) -> Result<String, String> {
    let text = std::fs::read_to_string(identity.with_extension("pub"))
        .map_err(|error| format!("read raw-fleet public key: {error}"))?;
    Ok(text.trim().to_owned())
}

/// Read the SSH identification banner over a raw TCP connection. The fixture
/// deliberately avoids invoking `ssh` itself: the only SSH client in the
/// deployment E2E belongs to the orchestrator's bootstrap transport.
fn wait_for_ssh_banner(host: &str, port: u16, budget: &Budget) -> Result<(), String> {
    let budget = budget.child(SSH_BANNER_TIMEOUT);
    let predicate = format!("SSH banner at {host}:{port}");
    let address = format!("{host}:{port}")
        .parse()
        .map_err(|error| format!("{predicate}: invalid address: {error}"))?;
    loop {
        let remaining = budget.remaining(&predicate)?;
        if let Ok(mut stream) = TcpStream::connect_timeout(&address, remaining.min(SSH_BANNER_POLL))
        {
            stream
                .set_read_timeout(Some(budget.remaining(&predicate)?.min(SSH_BANNER_POLL)))
                .map_err(|error| format!("{predicate}: {error}"))?;
            let mut banner = [0_u8; 64];
            if let Ok(count) = stream.read(&mut banner) {
                let text = String::from_utf8_lossy(&banner[..count]);
                if text.starts_with("SSH-2.0-") || text.starts_with("SSH-1.99-") {
                    return Ok(());
                }
            }
        }
        budget.wait(SSH_BANNER_POLL, &predicate)?;
    }
}

fn reserve_port() -> Result<u16, String> {
    let listener = std::net::TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("reserve raw-fleet SSH port: {error}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| format!("read reserved raw-fleet SSH port: {error}"))
}

/// Verified owned containers still present; legacy name-only manifests cannot
/// establish either ownership or absence.
pub(crate) fn read_manifest_containers(
    manifest: &Path,
    budget: &Budget,
) -> Result<Vec<String>, String> {
    let parsed = Manifest::load(manifest)?
        .ok_or_else(|| format!("raw-fleet manifest {} is absent", manifest.display()))?;
    let mut existing = Vec::new();
    for node in &parsed.nodes {
        if let Some(observed) = owned_resource(
            RawResourceKind::Container,
            &node.container_claim.retained(),
            &parsed.fixture,
            node.slot,
            budget,
        )? {
            existing.push(observed.name);
        }
    }
    Ok(existing)
}

fn recover_resource(
    kind: RawResourceKind,
    claim: &RawResourceClaim,
    name: &str,
    fixture: &str,
    slot: u32,
    budget: &Budget,
) -> Result<Option<RawResourceObservation>, String> {
    if claim.creation.is_some() || claim.id.get().is_some() {
        let observed = owned_resource(kind, claim, fixture, slot, budget)?.ok_or_else(|| {
            format!("retained raw {kind:?} {name} disappeared; refusing replacement")
        })?;
        if observed.name != name {
            return Err(format!(
                "retained raw {kind:?} {name} was renamed to {}; manifest retained",
                observed.name
            ));
        }
        Ok(Some(observed))
    } else if inspect_resource(kind, name, budget)?.is_some() {
        Err(format!(
            "unclaimed raw {kind:?} name collision {name}; refusing adoption"
        ))
    } else {
        Ok(None)
    }
}

impl RawDockerFleet {
    /// Creates or rediscovers `node_count` blank nodes and writes the static
    /// fleet manifest. Newly created nodes are asserted to carry no Myelin
    /// artifacts.
    pub fn ensure(
        workspace: &Path,
        artifacts: &Path,
        node_count: u32,
        build_image: bool,
        budget: &Budget,
        cleanup_budget: &Budget,
    ) -> Result<Self, String> {
        std::fs::create_dir_all(artifacts)
            .map_err(|error| format!("create raw-fleet artifacts dir: {error}"))?;
        let lock = RawManifestStore::lock(artifacts)?;
        let path = Self::manifest_path(artifacts);
        let previous = Manifest::load(&path)?;
        if let Some(previous) = previous.as_ref().filter(|manifest| manifest.retired) {
            // A completed cleanup is a tombstone, not permission to forget
            // ownership without rechecking the original immutable identities.
            for node in &previous.nodes {
                for (kind, claim) in [
                    (RawResourceKind::Container, &node.container_claim),
                    (RawResourceKind::Network, &node.network_claim),
                ] {
                    if owned_resource(
                        kind,
                        &claim.retained(),
                        &previous.fixture,
                        node.slot,
                        budget,
                    )?
                    .is_some()
                    {
                        return Err(
                            "retired raw-fleet still owns resources; manifest retained".to_owned()
                        );
                    }
                }
            }
        }
        let recovered = previous.as_ref().is_some_and(|manifest| !manifest.retired);
        if let Some(previous) = previous.as_ref().filter(|_| recovered) {
            if previous.nodes.len() != node_count as usize {
                return Err("retained raw-fleet node count differs; manifest retained".to_owned());
            }
        } else if build_image {
            ensure_base_image(workspace, budget)?;
        }
        let identity = ensure_identity(artifacts, budget, recovered)?;
        let public_key = public_key(&identity)?;
        let manifest = if let Some(previous) = previous.filter(|_| recovered) {
            previous.require_configuration(node_count, &public_key)?;
            previous
        } else {
            let image_id = docker(&["image", "inspect", "-f", "{{.Id}}", BASE_IMAGE], budget)?;
            let manifest =
                Manifest::fresh(node_count, image_id.trim().to_owned(), public_key.clone())?;
            // All names and nonce reservations become durable before a single
            // network/container creation can be issued.
            manifest.persist(&path)?;
            manifest
        };
        let image_id = manifest.image_id.clone();
        let mut fleet = Self {
            nodes: manifest.nodes.iter().map(ManifestNode::retained).collect(),
            identity,
            prefix: manifest.fixture.clone(),
            prior_processes: Mutex::new(BTreeMap::new()),
            budget: budget.clone(),
            cleanup_budget: cleanup_budget.clone(),
            manifest: RawManifestStore {
                path,
                state: Mutex::new(manifest),
                _lock: lock,
            },
        };
        let prepared = (|| {
            let prefix = fleet.prefix.as_str();
            let manifest = &fleet.manifest;
            std::thread::scope(|scope| {
                // Five construction owners at most, irrespective of manifest size.
                // Each owner is cancellable; scope cannot strand a Docker child.
                let mut errors = Vec::new();
                for batch in fleet.nodes.chunks_mut(5) {
                    let mut pending = Vec::new();
                    for node in batch {
                        let public_key = &public_key;
                        let image_id = &image_id;
                        pending.push(scope.spawn(move || {
                            let started = Instant::now();
                            let fixture_budget = budget;
                            let node_budget = budget.child(Duration::from_secs(60));
                            let budget = &node_budget;
                            let result = (|| {
                                let slot = node.slot;
                                let container = &node.container;
                                let network = &node.network;
                                let fixture_label = format!("myelin.raw-fixture={prefix}");
                                let slot_label = format!("myelin.raw-slot={slot}");
                                let existing_network = recover_resource(
                                    RawResourceKind::Network, &node.network_claim,
                                    network, prefix, slot, budget,
                                )?;
                                if let Some(observed) = existing_network {
                                    node.network_claim.adopt(&observed, prefix, slot)?;
                                } else {
                                    let nonce = manifest.begin_creation(slot, RawResourceKind::Network)?;
                                    node.network_claim.creation = Some(nonce.clone());
                                    let creation_label = format!("myelin.raw-creation={nonce}");
                                    let id = docker(&[
                                        "network", "create",
                                        "--label", &fixture_label,
                                        "--label", &slot_label,
                                        "--label", &creation_label,
                                        network,
                                    ], budget)?;
                                    let observed = inspect_resource(
                                        RawResourceKind::Network, id.trim(), budget,
                                    )?.ok_or_else(|| format!("created raw network {network} disappeared"))?;
                                    node.network_claim.adopt(&observed, prefix, slot)?;
                                }
                                manifest.record(node)?;
                                let network_id = node.network_claim.retained_id()?;
                                let existing_container = recover_resource(
                                    RawResourceKind::Container, &node.container_claim,
                                    container, prefix, slot, budget,
                                )?;
                                let created_now = existing_container.is_none();
                                if let Some(observed) = existing_container {
                                    node.container_claim.adopt(&observed, prefix, slot)?;
                                } else {
                                    let port = reserve_port()?;
                                    let nonce = manifest.begin_creation(slot, RawResourceKind::Container)?;
                                    node.container_claim.creation = Some(nonce.clone());
                                    let creation_label = format!("myelin.raw-creation={nonce}");
                                    let id = docker(&[
                                        "run", "-d", "--init",
                                        "--name", container,
                                        "--label", &fixture_label,
                                        "--label", &slot_label,
                                        "--label", &creation_label,
                                        "--network", network_id,
                                        "--publish", &format!("127.0.0.1:{port}:22"),
                                        "--env", &format!("PUBLIC_KEY={public_key}"),
                                        image_id,
                                    ], budget)?;
                                    let observed = inspect_resource(
                                        RawResourceKind::Container, id.trim(), budget,
                                    )?.ok_or_else(|| format!("created raw node {container} disappeared"))?;
                                    node.container_claim.adopt(&observed, prefix, slot)?;
                                }
                                manifest.record(node)?;
                                let container_id = node.container_claim.retained_id()?;
                                let configuration = docker(&[
                                    "inspect", "-f", "{{json .}}", container_id,
                                ], budget)?;
                                let configuration: serde_json::Value = serde_json::from_str(&configuration)
                                    .map_err(|error| format!("decode retained raw node configuration: {error}"))?;
                                let expected_key = format!("PUBLIC_KEY={public_key}");
                                if configuration["Image"].as_str() != Some(image_id.as_str())
                                    || !configuration["Config"]["Env"].as_array().is_some_and(|env| {
                                        env.iter().filter_map(serde_json::Value::as_str)
                                            .filter(|value| value.starts_with("PUBLIC_KEY="))
                                            .eq(std::iter::once(expected_key.as_str()))
                                    })
                                {
                                    return Err(format!("retained raw node {container} image or SSH key changed"));
                                }
                                if created_now {
                                    // Blank-node guarantee: the base image must not carry
                                    // Myelin deployment state.
                                    docker(&["exec", container_id, "test", "!", "-e", "/opt/myelin"], budget)
                                        .map_err(|error| format!("fresh raw node {container} is not blank: {error}"))?;
                                    docker(&[
                                        "exec", container_id, "test", "!", "-e",
                                        "/usr/local/bin/myelin-node",
                                    ], budget).map_err(|error| {
                                        format!("fresh raw node {container} ships myelin-node: {error}")
                                    })?;
                                } else if !container_running(container_id, budget)? {
                                    docker(&["start", container_id], budget)?;
                                }
                                if !container_has_init(container_id, budget)? {
                                    return Err(format!(
                                        "raw node {container} has no init reaper; refresh the fixture"
                                    ));
                                }
                                let attached = docker(&[
                                    "inspect", "-f",
                                    "{{range .NetworkSettings.Networks}}{{.NetworkID}}{{end}}",
                                    container_id,
                                ], budget)?;
                                if attached.trim() != network_id {
                                    return Err(format!(
                                        "raw node {container} is not attached exclusively to retained network {network_id}"
                                    ));
                                }
                                let ssh_port = published_ssh_port(container_id, budget)?;
                                if node.ssh_port != 0 && node.ssh_port != ssh_port {
                                    return Err(format!("retained raw node {container} SSH endpoint changed"));
                                }
                                wait_for_ssh_banner("127.0.0.1", ssh_port, budget)?;
                                node.ssh_port = ssh_port;
                                manifest.record(node)?;
                                Ok::<(), String>(())
                            })();
                            record_execution_stage("raw_fleet.construct_node", started.elapsed(), 0, 1);
                            if result.is_err() {
                                fixture_budget.cancel();
                            }
                            result
                        }));
                    }
                    for owner in pending {
                        match owner.join() {
                            Ok(Ok(())) => {}
                            Ok(Err(error)) => errors.push(error),
                            Err(_) => {
                                budget.cancel();
                                errors.push("raw construction owner panicked".to_owned());
                            }
                        }
                    }
                    if !errors.is_empty() {
                        break;
                    }
                }
                if errors.is_empty() {
                    Ok(())
                } else {
                    Err(errors.join("; "))
                }
            })?;
            fleet.assert_isolated()?;
            fleet.snapshot()?;
            fleet.manifest.update(|manifest| {
                manifest.ready = true;
                Ok(())
            })?;
            Ok::<(), String>(())
        })();
        if let Err(error) = prepared {
            if recovered {
                return Err(format!(
                    "recover raw fixture: {error}; original ownership manifest retained at {}",
                    fleet.manifest.path.display(),
                ));
            }
            let teardown = fleet.teardown();
            let remaining = fleet.remaining_resources_with_budget(cleanup_budget);
            return Err(format!(
                "prepare raw fixture: {error}; teardown={teardown:?}; remaining={remaining:?}"
            ));
        }
        Ok(fleet)
    }

    pub fn set_execution_budget(&mut self, budget: Budget) {
        self.budget = budget;
    }

    /// Gateway address of node 0's bridge: a host IP reachable from every
    /// node's isolated network and from the host itself.
    pub fn host_gateway(&self) -> Result<String, String> {
        let budget = &self.budget;
        let Some(node) = self.nodes.first() else {
            return Err("raw fleet has no nodes".to_owned());
        };
        let gateway = docker(
            &[
                "inspect",
                "-f",
                "{{range .NetworkSettings.Networks}}{{.Gateway}}{{end}}",
                node.container_claim.retained_id()?,
            ],
            budget,
        )?;
        let gateway = gateway.trim().to_owned();
        if gateway.is_empty() {
            return Err(format!(
                "raw fleet network for {} has no gateway",
                node.container
            ));
        }
        Ok(gateway)
    }

    pub fn manifest_path(artifacts: &Path) -> PathBuf {
        artifacts.join("raw-fleet.json")
    }

    fn node(&self, slot: u32) -> Result<&RawNode, String> {
        self.nodes
            .iter()
            .find(|node| node.slot == slot)
            .ok_or_else(|| format!("raw fleet has no slot {slot}"))
    }

    /// Immutable provider-resource and logical mapping evidence.
    pub fn snapshot(&self) -> Result<RawFleetSnapshot, String> {
        let budget = &self.budget;
        let mut nodes = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let container = inspect_resource(RawResourceKind::Container, &node.container, budget)?
                .ok_or_else(|| format!("retained raw container {} disappeared", node.container))?;
            let network = inspect_resource(RawResourceKind::Network, &node.network, budget)?
                .ok_or_else(|| format!("retained raw network {} disappeared", node.network))?;
            // Snapshot must compare to acquisition identities, not establish a
            // new baseline after a same-name resource has been replaced.
            node.container_claim
                .verify(&container, &self.prefix, node.slot)?;
            node.network_claim
                .verify(&network, &self.prefix, node.slot)?;
            let container_id = node.container_claim.retained_id()?.to_owned();
            let network_id = node.network_claim.retained_id()?.to_owned();
            nodes.push(RawFixtureNodeIdentity {
                logical_node_id: u64::from(node.slot) + 1,
                slot: node.slot,
                container: node.container.clone(),
                container_id,
                network: node.network.clone(),
                network_id,
                ssh_host: node.ssh_host.clone(),
                ssh_port: published_ssh_port(node.container_claim.retained_id()?, budget)?,
            });
        }
        nodes.sort_by_key(|node| node.slot);
        Ok(RawFleetSnapshot { nodes })
    }

    pub fn assert_unchanged(&self, expected: &RawFleetSnapshot) -> Result<(), String> {
        let observed = self.snapshot()?;
        if observed == *expected {
            Ok(())
        } else {
            Err(format!(
                "raw fixture identity changed: expected {expected:?}, observed {observed:?}"
            ))
        }
    }

    /// Retains process identities on the host before a new generation starts.
    /// Node reset cannot erase this evidence by removing deployment metadata.
    pub fn record_prior_processes(&self) -> Result<(), String> {
        for node in &self.nodes {
            self.record_node_processes(node)?;
        }
        Ok(())
    }

    fn record_node_processes(&self, node: &RawNode) -> Result<(), String> {
        let budget = &self.budget;
        let processes = process_census(node.container_claim.retained_id()?, budget)?;
        let mut prior = self
            .prior_processes
            .lock()
            .map_err(|_| "raw prior-process census lock poisoned".to_owned())?;
        let recorded = prior.entry(node.slot).or_default();
        let related = related_worker_processes(&processes, recorded);
        for process in related {
            if !recorded
                .iter()
                .any(|old| old.pid == process.pid && old.start_ticks == process.start_ticks)
            {
                recorded.push(process);
            }
        }
        recorded.sort_by_key(|process| (process.pid, process.start_ticks));
        Ok(())
    }

    /// Docker is used only as a census mechanism. It does not launch or
    /// install the tested worker.
    pub fn census(&self) -> Result<Vec<RawNodeCensus>, String> {
        let budget = &self.budget;
        let mut census = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let processes = process_census(node.container_claim.retained_id()?, budget)?;
            let prior_processes = self
                .prior_processes
                .lock()
                .map_err(|_| "raw prior-process census lock poisoned".to_owned())?
                .get(&node.slot)
                .cloned()
                .unwrap_or_default();
            let surviving_prior_processes = processes
                .iter()
                .filter(|process| {
                    prior_processes.iter().any(|prior| {
                        prior.pid == process.pid && prior.start_ticks == process.start_ticks
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut worker_processes = processes
                .iter()
                .filter(|process| process.command == "myelin-worker")
                .cloned()
                .collect::<Vec<_>>();
            worker_processes.sort_by_key(|process| process.pid);
            let worker_group_processes = related_worker_processes(&processes, &prior_processes);
            let agent_pid = parse_optional_u32(
                &docker_shell(
                    node.container_claim.retained_id()?,
                    "cat /opt/myelin/agent.pid 2>/dev/null || true",
                    budget,
                )?,
                "raw node agent pid",
            )?;
            let active_link_target = docker_shell(
                node.container_claim.retained_id()?,
                "readlink /opt/myelin/current 2>/dev/null || true",
                budget,
            )?;
            let active_link_target = (!active_link_target.trim().is_empty())
                .then(|| active_link_target.trim().to_owned());
            let active_digest = docker_shell(
                node.container_claim.retained_id()?,
                "sha256sum /opt/myelin/current/bin/myelin-worker 2>/dev/null || true",
                budget,
            )?;
            let active_executable_digest = active_digest
                .split_whitespace()
                .next()
                .filter(|digest| digest.len() == 64)
                .map(|digest| format!("sha256:{digest}"));
            let active_deployment = docker_shell(
                node.container_claim.retained_id()?,
                "cat /opt/myelin/active-deployment.json 2>/dev/null || true",
                budget,
            )?;
            let active_deployment_raw =
                (!active_deployment.trim().is_empty()).then(|| active_deployment.trim().to_owned());
            census.push(RawNodeCensus {
                logical_node_id: u64::from(node.slot) + 1,
                container: node.container.clone(),
                active_deployment_raw,
                agent_pid,
                active_link_target,
                active_executable_digest,
                worker_processes,
                worker_group_processes,
                prior_processes,
                surviving_prior_processes,
            });
        }
        Ok(census)
    }

    /// Constructs a prior state without installing or launching the tested
    /// binaries. The production SSH deployment transaction remains unaware
    /// of which state was selected.
    pub fn inject_prior_state(&self, slot: u32, state: RawPriorState) -> Result<(), String> {
        let budget = &self.budget;
        let node = self.node(slot)?;
        let script = match state {
            RawPriorState::NoWorkerOrTrustworthyMetadata => format!(
                "{}; rm -rf /opt/myelin/agent.pid /opt/myelin/runtime \
             /opt/myelin/bootstrap.lock /opt/myelin/bootstrap.sock \
             /opt/myelin/current /opt/myelin/active-deployment.json \
             /opt/myelin/transactions",
                stop_worker_trees_script()
            ),
            RawPriorState::HealthyStaleWorker => ":".to_owned(),
            RawPriorState::DeadWorkerWithStaleMetadata => format!(
                "{}; mkdir -p /opt/myelin/runtime /opt/myelin/bootstrap.lock; \
             printf '999999\\n' > /opt/myelin/agent.pid; \
             printf '999998\\n' > /opt/myelin/runtime/stale.pid; \
             printf '999997\\n' > /opt/myelin/bootstrap.lock/pid; \
             touch /opt/myelin/bootstrap.lock/complete /opt/myelin/bootstrap.sock",
                stop_worker_trees_script()
            ),
            RawPriorState::MultipleStaleWorkersAndDescendants => r#"python3 - <<'PY'
import os, select, shutil, signal, subprocess, time
shutil.copy('/bin/sh', '/tmp/myelin-worker')
os.chmod('/tmp/myelin-worker', 0o755)
read, write = os.pipe()
workers = []
try:
    for index in range(3):
        with open('/tmp/myelin-stale-%s.log' % index, 'wb') as log:
            workers.append(subprocess.Popen(
                ['/tmp/myelin-worker', '-c',
                 '(trap "" TERM; printf r >&%s; exec sleep 600) & wait' % write],
                stdin=subprocess.DEVNULL, stdout=log, stderr=log,
                pass_fds=(write,), start_new_session=True))
    os.close(write)
    write = None
    deadline = time.monotonic() + 2.5
    ready = b''
    while len(ready) < 3:
        remaining = deadline - time.monotonic()
        if remaining <= 0 or not select.select([read], [], [], remaining)[0]:
            raise TimeoutError('TERM-resistant descendants did not acknowledge readiness')
        data = os.read(read, 3 - len(ready))
        if not data:
            raise RuntimeError('stale worker exited before its descendant was ready')
        ready += data
except BaseException:
    for worker in workers:
        try:
            os.killpg(worker.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        worker.wait()
    raise
finally:
    os.close(read)
    if write is not None:
        os.close(write)
PY"#
            .to_owned(),
            RawPriorState::InterruptedTransferAndPartialInstall => {
                "mkdir -p /opt/myelin/staging/interrupted \
               /opt/myelin/releases/interrupted.staged.1/bin \
               /opt/myelin/transactions/interrupted; \
             printf 'partial-bundle' > /opt/myelin/staging/interrupted/bundle.tar; \
             printf 'partial-worker' > \
               /opt/myelin/releases/interrupted.staged.1/bin/myelin-worker; \
             printf '{\"state\":\"interrupted\"}' > \
               /opt/myelin/transactions/interrupted/deployment.json"
                    .to_owned()
            }
            RawPriorState::CorruptActiveBinary => {
                "worker=/opt/myelin/current/bin/myelin-worker; test -e \"$worker\"; \
             corrupt=$(mktemp /opt/myelin/current/bin/.myelin-worker.corrupt.XXXXXX); \
             printf 'corrupt-worker' > \"$corrupt\"; chmod 0755 \"$corrupt\"; \
             mv -f \"$corrupt\" \"$worker\""
                    .to_owned()
            }
            RawPriorState::CorruptActivationPointer => "rm -rf /opt/myelin/current; \
             ln -s /opt/myelin/releases/missing /opt/myelin/current"
                .to_owned(),
            RawPriorState::CorruptDeploymentDescriptor => {
                "mkdir -p /opt/myelin/transactions/corrupt; \
             printf 'not-json' > /opt/myelin/active-deployment.json; \
             printf '{\"artifact_digest\":\"stale\"}' > \
               /opt/myelin/transactions/corrupt/deployment.json"
                    .to_owned()
            }
        };
        docker_shell(node.container_claim.retained_id()?, &script, budget)?;
        self.record_node_processes(node)
    }

    pub fn hold_boundary(
        &self,
        generation: &str,
        boundary: DeploymentBoundary,
    ) -> Result<(), String> {
        let budget = &self.budget;
        validate_remote_token(generation, "deployment generation")?;
        let boundary = boundary.as_remote_name();
        for node in &self.nodes {
            docker_shell(
                node.container_claim.retained_id()?,
                &format!(
                    "mkdir -p /opt/myelin/deployment-boundaries; \
             touch /opt/myelin/deployment-boundaries/{generation}.{boundary}.hold"
                ),
                budget,
            )?;
        }
        Ok(())
    }

    pub fn release_boundary(
        &self,
        generation: &str,
        boundary: DeploymentBoundary,
    ) -> Result<(), String> {
        self.release_boundary_with_budget(generation, boundary, &self.budget)
    }

    pub fn release_boundary_with_budget(
        &self,
        generation: &str,
        boundary: DeploymentBoundary,
        budget: &Budget,
    ) -> Result<(), String> {
        validate_remote_token(generation, "deployment generation")?;
        let boundary = boundary.as_remote_name();
        let mut errors = Vec::new();
        for node in &self.nodes {
            if let Err(error) = node.container_claim.retained_id().and_then(|container| {
                docker_shell(
                    container,
                    &format!(
                        "rm -f /opt/myelin/deployment-boundaries/{generation}.{boundary}.hold"
                    ),
                    budget,
                )
            }) {
                errors.push(format!("release boundary on {}: {error}", node.container));
            }
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    pub fn wait_for_boundary(
        &self,
        slot: u32,
        generation: &str,
        boundary: DeploymentBoundary,
        timeout: Duration,
    ) -> Result<(), String> {
        validate_remote_token(generation, "deployment generation")?;
        let node = self.node(slot)?;
        let marker = format!(
            "/opt/myelin/deployment-boundaries/{}.{}.reached",
            generation,
            boundary.as_remote_name()
        );
        let budget = self.budget.child(timeout);
        let seconds = budget
            .remaining(&format!("deployment boundary {marker}"))?
            .as_secs_f64();
        // Subscribe before checking the marker: neither an early boundary nor
        // a boundary created between check and sleep can be missed.
        let script = format!(
            r#"python3 - {marker} {seconds} <<'PY'
import ctypes, os, select, sys, time
from pathlib import Path
path = Path(sys.argv[1])
deadline = time.monotonic() + float(sys.argv[2])
path.parent.mkdir(parents=True, exist_ok=True)
libc = ctypes.CDLL(None, use_errno=True)
fd = libc.inotify_init1(os.O_CLOEXEC | os.O_NONBLOCK)
if fd < 0 or libc.inotify_add_watch(fd, os.fsencode(path.parent), 0x100 | 0x80 | 0x8) < 0:
    raise OSError(ctypes.get_errno(), 'subscribe deployment boundary')
try:
    while not path.exists():
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise TimeoutError('pending deployment boundary: ' + str(path))
        if select.select([fd], [], [], remaining)[0]:
            os.read(fd, 65536)
finally:
    os.close(fd)
PY"#
        );
        docker_shell(node.container_claim.retained_id()?, &script, &budget)
            .map(|_| ())
            .map_err(|error| {
                format!(
                    "pending deployment boundary {marker} on {}: {error}",
                    node.container
                )
            })
    }

    pub fn set_node_reachable(&self, slot: u32, reachable: bool) -> Result<(), String> {
        self.set_node_reachable_with_budget(slot, reachable, &self.budget)
    }

    pub fn set_node_reachable_with_budget(
        &self,
        slot: u32,
        reachable: bool,
        budget: &Budget,
    ) -> Result<(), String> {
        let node = self.node(slot)?;
        if reachable {
            docker(&["unpause", node.container_claim.retained_id()?], budget)?;
            wait_for_ssh_banner(&node.ssh_host, node.ssh_port, budget)
        } else {
            docker(&["pause", node.container_claim.retained_id()?], budget).map(|_| ())
        }
    }

    /// Nodes must sit on exactly one network each, and no node may open a TCP
    /// connection to another node's address.
    pub fn assert_isolated(&self) -> Result<(), String> {
        let budget = &self.budget;
        let started = Instant::now();
        #[derive(Deserialize)]
        #[serde(rename_all = "snake_case")]
        enum Outcome {
            Unreachable,
            Reachable,
            Error,
        }
        #[derive(Deserialize)]
        struct Observation {
            address: String,
            outcome: Outcome,
            error: Option<String>,
        }
        const PROBE: &str = r#"
import concurrent.futures, errno, json, socket, sys
def probe(address):
    try:
        with socket.socket() as stream:
            stream.settimeout(2.8)
            stream.connect((address, 22))
    except TimeoutError:
        return dict(address=address, outcome="unreachable")
    except OSError as error:
        if error.errno in (errno.ECONNREFUSED, errno.EHOSTUNREACH, errno.ENETUNREACH):
            return dict(address=address, outcome="unreachable")
        return dict(address=address, outcome="error", error=str(error))
    return dict(address=address, outcome="reachable")
with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(sys.argv) - 1)) as workers:
    print(json.dumps(list(workers.map(probe, sys.argv[1:]))))
"#;
        let mut addresses = Vec::with_capacity(self.nodes.len());
        for node in &self.nodes {
            let networks = docker(
                &[
                    "inspect",
                    "-f",
                    "{{len .NetworkSettings.Networks}}",
                    node.container_claim.retained_id()?,
                ],
                budget,
            )?;
            if networks.trim() != "1" {
                return Err(format!(
                    "raw node {} is attached to {} networks; expected exactly 1",
                    node.container,
                    networks.trim()
                ));
            }
            addresses.push(container_ip_on_network(
                node.container_claim.retained_id()?,
                budget,
            )?);
        }
        if addresses
            .iter()
            .enumerate()
            .any(|(index, address)| addresses[..index].contains(address))
        {
            return Err("raw fixture peers share an isolation probe address".to_owned());
        }
        let mut errors = Vec::new();
        // One interpreter per source, with four independent socket probes.
        // All 20 directions remain concurrent for five nodes. Startup is inside
        // the command budget, not a competing 3s fuse around a 2.8s socket wait.
        for batch in self.nodes.chunks(5) {
            let results = std::thread::scope(|scope| {
                let pending = batch
                    .iter()
                    .map(|source| {
                        let targets = self
                            .nodes
                            .iter()
                            .zip(&addresses)
                            .filter(|(target, _)| target.slot != source.slot)
                            .map(|(target, address)| (address.as_str(), target))
                            .collect::<BTreeMap<_, _>>();
                        scope.spawn(move || {
                            if targets.is_empty() {
                                return Ok(());
                            }
                            let probe = command_output(
                                Command::new("docker")
                                    .args(["exec", source.container_claim.retained_id()?, "python3", "-c", PROBE])
                                    .args(targets.keys()),
                                &budget.child(Duration::from_secs(10)),
                                "directed raw isolation probes",
                            )?;
                            if !probe.status.success() {
                                return Err(format!(
                                    "isolation probes from {} failed without absence proof: {}: {}",
                                    source.container,
                                    probe.status,
                                    String::from_utf8_lossy(&probe.stderr),
                                ));
                            }
                            let observations: Vec<Observation> =
                                serde_json::from_slice(&probe.stdout).map_err(|error| {
                                    format!("decode isolation probes from {}: {error}", source.container)
                                })?;
                            if observations.len() != targets.len() {
                                return Err(format!("incomplete isolation probes from {}", source.container));
                            }
                            let mut pending = targets;
                            let mut failures = Vec::new();
                            for observation in observations {
                                let target = pending.remove(observation.address.as_str()).ok_or_else(|| {
                                    format!("unexpected or duplicate isolation address {:?} from {}",
                                        observation.address, source.container)
                                })?;
                                match observation.outcome {
                                    Outcome::Unreachable => {}
                                    Outcome::Reachable => failures.push(format!(
                                        "raw node {} reached {} at {}:22",
                                        source.container, target.container, observation.address,
                                    )),
                                    Outcome::Error => failures.push(format!(
                                        "isolation probe {} -> {} failed without absence proof: {}",
                                        source.container, target.container,
                                        observation.error.as_deref().unwrap_or("missing probe error"),
                                    )),
                                }
                            }
                            if failures.is_empty() { Ok(()) } else { Err(failures.join("; ")) }
                        })
                    })
                    .collect::<Vec<_>>();
                pending
                    .into_iter()
                    .map(|owner| {
                        owner
                            .join()
                            .unwrap_or_else(|_| Err("raw isolation owner panicked".to_owned()))
                    })
                    .collect::<Vec<_>>()
            });
            errors.extend(results.into_iter().filter_map(Result::err));
        }
        record_execution_stage(
            "raw_fleet.isolation",
            started.elapsed(),
            0,
            (self.nodes.len() * self.nodes.len().saturating_sub(1)) as u64,
        );
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }

    pub fn remaining_resources(&self) -> Result<Vec<String>, String> {
        self.remaining_resources_with_budget(&self.budget)
    }

    pub fn remaining_resources_with_budget(&self, budget: &Budget) -> Result<Vec<String>, String> {
        let results = std::thread::scope(|scope| {
            self.nodes
                .iter()
                .flat_map(|node| {
                    [
                        (RawResourceKind::Container, &node.container_claim),
                        (RawResourceKind::Network, &node.network_claim),
                    ]
                    .map(|(kind, claim)| {
                        scope.spawn(move || {
                            owned_resource(kind, claim, &self.prefix, node.slot, budget).map(
                                |resource| {
                                    resource.map(|resource| {
                                        let kind = match kind {
                                            RawResourceKind::Container => "container",
                                            RawResourceKind::Network => "network",
                                        };
                                        format!("{kind}:{}:{}", resource.name, resource.id)
                                    })
                                },
                            )
                        })
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|owner| {
                    owner
                        .join()
                        .unwrap_or_else(|_| Err("raw ownership census owner panicked".to_owned()))
                })
                .collect::<Vec<_>>()
        });
        let mut remaining = Vec::new();
        let mut errors = Vec::new();
        for result in results {
            match result {
                Ok(Some(resource)) => remaining.push(resource),
                Ok(None) => {}
                Err(error) => errors.push(error),
            }
        }
        if !errors.is_empty() {
            return Err(errors.join("; "));
        }
        remaining.sort();
        Ok(remaining)
    }

    /// Preserve bounded worker failure evidence before destroying the local fixture.
    pub fn failure_diagnostics(&self, budget: &Budget) -> serde_json::Value {
        let nodes = std::thread::scope(|scope| {
            self.nodes
                .iter()
                .map(|node| {
                    scope.spawn(move || {
                        let log = node
                            .container_claim
                            .retained_id()
                            .and_then(|container| {
                                docker_shell(
                                    container,
                                    r#"python3 -c 'import json, pathlib
p = pathlib.Path("/var/log/myelin-node.log")
with p.open("rb") as f:
    f.seek(0, 2)
    size = f.tell()
    f.seek(max(0, size - 65536))
    print(json.dumps({"log_bytes": size, "log_tail": f.read().decode("utf-8", errors="replace")}))
'"#,
                                    budget,
                                )
                            })
                            .and_then(|text| {
                                serde_json::from_str::<serde_json::Value>(&text)
                                    .map_err(|error| format!("decode worker diagnostics: {error}"))
                            });
                        serde_json::json!({
                            "slot": node.slot, "container": node.container,
                            "worker_log": log.as_ref().ok(),
                            "error": log.as_ref().err(),
                        })
                    })
                })
                .collect::<Vec<_>>()
                .into_iter()
                .map(|owner| {
                    owner.join().unwrap_or_else(
                        |_| serde_json::json!({"error": "worker diagnostic collector panicked"}),
                    )
                })
                .collect::<Vec<_>>()
        });
        serde_json::json!({"schema_version": 1, "nodes": nodes})
    }

    /// Removes every container and network owned by this fixture.
    pub fn teardown(&self) -> Result<(), String> {
        self.teardown_with_budget(&self.cleanup_budget)
    }

    pub fn teardown_with_budget(&self, budget: &Budget) -> Result<(), String> {
        let mut errors = std::thread::scope(|scope| {
            let pending = self
                .nodes
                .iter()
                .flat_map(|node| {
                    let container = scope.spawn(move || {
                        let result = owned_resource(
                            RawResourceKind::Container,
                            &node.container_claim,
                            &self.prefix,
                            node.slot,
                            budget,
                        )
                        .and_then(|resource| match resource {
                            Some(resource) => {
                                remove_resource(RawResourceKind::Container, &resource.id, budget)
                            }
                            None => Ok(()),
                        });
                        result.err().into_iter().collect::<Vec<_>>()
                    });
                    let network = scope.spawn(move || {
                        // Endpoint detachment, not container process reaping, is
                        // the network removal dependency. Own it independently
                        // so a slow rm cannot consume the network's whole reserve.
                        // Resolve each endpoint to a verified immutable identity;
                        // an unrelated same-name container is never detached.
                        let result = (|| {
                            let Some(network) = owned_resource(
                                RawResourceKind::Network,
                                &node.network_claim,
                                &self.prefix,
                                node.slot,
                                budget,
                            )?
                            else {
                                return Ok(());
                            };
                            let disconnect = owned_resource(
                                RawResourceKind::Container,
                                &node.container_claim,
                                &self.prefix,
                                node.slot,
                                budget,
                            )
                            .and_then(|container| match container {
                                Some(container) => docker(
                                    &["network", "disconnect", "-f", &network.id, &container.id],
                                    budget,
                                )
                                .map(|_| ()),
                                None => Ok(()),
                            });
                            if let Err(error) =
                                remove_resource(RawResourceKind::Network, &network.id, budget)
                            {
                                return Err(match disconnect {
                                    Ok(()) => error,
                                    Err(disconnect) => format!("{error}; {disconnect}"),
                                });
                            }
                            // Successful removal also proves endpoint absence
                            // when concurrent container removal won the race.
                            Ok::<(), String>(())
                        })();
                        result.err().into_iter().collect::<Vec<_>>()
                    });
                    [container, network]
                })
                .collect::<Vec<_>>();
            pending
                .into_iter()
                .flat_map(|owner| {
                    owner
                        .join()
                        .unwrap_or_else(|_| vec!["raw teardown owner panicked".to_owned()])
                })
                .collect::<Vec<_>>()
        });
        match self.remaining_resources_with_budget(budget) {
            Ok(remaining) if remaining.is_empty() => {
                // Preserve IDs recovered during teardown before recording the
                // terminal state. Never replace this manifest merely because a
                // new runner has a different PID.
                for node in &self.nodes {
                    self.manifest.record(node)?;
                }
                self.manifest.update(|manifest| {
                    manifest.retired = true;
                    Ok(())
                })?;
                for path in [&self.identity, &self.identity.with_extension("pub")] {
                    if let Err(error) = std::fs::remove_file(path) {
                        if error.kind() != std::io::ErrorKind::NotFound {
                            errors.push(format!(
                                "remove private fixture identity {}: {error}",
                                path.display()
                            ));
                        }
                    }
                }
            }
            Ok(remaining) => {
                errors.push(format!("raw fixture resources remain: {remaining:?}"));
            }
            Err(error) => errors.push(format!("prove raw fixture absence: {error}")),
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors.join("; "))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn observation(id: char, fixture: &str, slot: u32) -> RawResourceObservation {
        RawResourceObservation {
            id: id.to_string().repeat(64),
            name: format!("fixture-node-{slot}"),
            labels: BTreeMap::from([
                ("myelin.raw-fixture".to_owned(), fixture.to_owned()),
                ("myelin.raw-slot".to_owned(), slot.to_string()),
            ]),
        }
    }

    #[test]
    fn unrelated_name_collisions_never_become_cleanup_capabilities() {
        let mut unlabeled = observation('a', "fixture", 0);
        unlabeled.labels.clear();
        for observed in [
            unlabeled,
            observation('a', "another-fixture", 0),
            observation('a', "fixture", 1),
        ] {
            let claim = RawResourceClaim::default();
            assert!(claim.adopt(&observed, "fixture", 0).is_err());
            // Even with no command budget, unwind of a rejected adoption
            // succeeds without inspecting, detaching or deleting the name.
            for kind in [RawResourceKind::Container, RawResourceKind::Network] {
                assert!(
                    owned_resource(kind, &claim, "fixture", 0, &Budget::new(Duration::ZERO),)
                        .unwrap()
                        .is_none()
                );
            }
        }
    }

    #[test]
    fn retained_resource_identity_survives_rename_but_rejects_replacement() {
        let claim = RawResourceClaim::default();
        let mut retained = observation('a', "fixture", 0);
        assert_eq!(claim.adopt(&retained, "fixture", 0).unwrap(), retained.id);
        retained.name = "renamed-owned-resource".to_owned();
        assert_eq!(claim.adopt(&retained, "fixture", 0).unwrap(), retained.id);
        let replacement = observation('b', "fixture", 0);
        assert!(claim.adopt(&replacement, "fixture", 0).is_err());
        assert_eq!(claim.retained_id().unwrap(), retained.id);
    }

    #[test]
    fn ambiguous_creation_requires_its_exact_nonce_then_pins_one_id() {
        let claim = RawResourceClaim {
            creation: Some("attempt-a".to_owned()),
            ..RawResourceClaim::default()
        };
        let mut partial = observation('a', "fixture", 0);
        assert!(claim.adopt(&partial, "fixture", 0).is_err());
        partial
            .labels
            .insert("myelin.raw-creation".to_owned(), "attempt-b".to_owned());
        assert!(claim.adopt(&partial, "fixture", 0).is_err());
        partial
            .labels
            .insert("myelin.raw-creation".to_owned(), "attempt-a".to_owned());
        assert_eq!(claim.adopt(&partial, "fixture", 0).unwrap(), partial.id);
        partial.id = "b".repeat(64);
        assert!(claim.adopt(&partial, "fixture", 0).is_err());
        assert_eq!(claim.retained_id().unwrap(), "a".repeat(64));
    }

    #[test]
    fn interrupted_manifest_recovers_exact_fixture_and_creation_ownership() {
        let directory = tempfile::tempdir().unwrap();
        let path = RawDockerFleet::manifest_path(directory.path());
        let manifest = Manifest::fresh(
            1,
            format!("sha256:{}", "a".repeat(64)),
            "ssh-ed25519 fixture-key".to_owned(),
        )
        .unwrap();
        let fixture = manifest.fixture.clone();
        manifest.persist(&path).unwrap();
        let store = RawManifestStore {
            path: path.clone(),
            state: Mutex::new(manifest),
            _lock: RawManifestStore::lock(directory.path()).unwrap(),
        };
        let nonce = store.begin_creation(0, RawResourceKind::Network).unwrap();
        // Simulate runner loss after Docker accepted creation, before its ID
        // reached the journal. A different runner must use this exact nonce.
        drop(store);
        let recovered = Manifest::load(&path).unwrap().unwrap();
        assert_eq!(recovered.fixture, fixture);
        let node = recovered.nodes[0].retained();
        let mut observed = observation('b', &fixture, 0);
        observed.name.clone_from(&node.network);
        observed
            .labels
            .insert("myelin.raw-creation".to_owned(), nonce.clone());
        assert_eq!(
            node.network_claim.adopt(&observed, &fixture, 0).unwrap(),
            observed.id
        );
        let store = RawManifestStore {
            path: path.clone(),
            state: Mutex::new(recovered),
            _lock: RawManifestStore::lock(directory.path()).unwrap(),
        };
        store.record(&node).unwrap();
        assert!(store.begin_creation(0, RawResourceKind::Network).is_err());
        drop(store);
        let retained = Manifest::load(&path).unwrap().unwrap();
        assert_eq!(retained.fixture, fixture);
        assert_eq!(retained.nodes[0].network_claim.creation, nonce);
        let claim = retained.nodes[0].network_claim.retained();
        assert_eq!(claim.retained_id().unwrap(), observed.id);
        observed.id = "c".repeat(64);
        assert!(claim.adopt(&observed, &fixture, 0).is_err());
        observed.id = "b".repeat(64);
        observed
            .labels
            .insert("myelin.raw-creation".to_owned(), "d".repeat(32));
        assert!(claim.adopt(&observed, &fixture, 0).is_err());
    }

    #[test]
    fn legacy_or_changed_fixture_configuration_is_retained_without_execution() {
        let directory = tempfile::tempdir().unwrap();
        let path = RawDockerFleet::manifest_path(directory.path());
        let legacy = r#"{"nodes":[{"slot":0,"container":"old-pid-node-0","ssh_host":"127.0.0.1","ssh_port":32000}]}"#;
        std::fs::write(&path, legacy).unwrap();
        let budget = Budget::new(Duration::ZERO);
        assert!(
            RawDockerFleet::ensure(
                directory.path(),
                directory.path(),
                1,
                false,
                &budget,
                &budget,
            )
            .is_err()
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), legacy);
        let manifest = Manifest::fresh(
            1,
            format!("sha256:{}", "a".repeat(64)),
            "ssh-ed25519 fixture-key".to_owned(),
        )
        .unwrap();
        manifest.persist(&path).unwrap();
        let original = std::fs::read(&path).unwrap();
        assert!(
            RawDockerFleet::ensure(
                directory.path(),
                directory.path(),
                2,
                false,
                &budget,
                &budget,
            )
            .is_err()
        );
        assert!(
            manifest
                .require_configuration(1, "ssh-ed25519 another-key")
                .is_err()
        );
        assert_eq!(std::fs::read(&path).unwrap(), original);
    }
}
