//! Typed intermediate representation of generated behavioral programs.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionClass {
    PublishBlob,
    ReadBlob,
    StreamWrite,
    StreamRead,
    Lookup,
    Rename,
    Unlink,
    Descriptor,
}

impl ActionClass {
    pub const ALL: [Self; 8] = [
        Self::PublishBlob,
        Self::ReadBlob,
        Self::StreamWrite,
        Self::StreamRead,
        Self::Lookup,
        Self::Rename,
        Self::Unlink,
        Self::Descriptor,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PublishBlob => "publish_blob",
            Self::ReadBlob => "read_blob",
            Self::StreamWrite => "stream_write",
            Self::StreamRead => "stream_read",
            Self::Lookup => "lookup",
            Self::Rename => "rename",
            Self::Unlink => "unlink",
            Self::Descriptor => "descriptor",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorWriteMethod {
    Write,
    WriteFrom,
    Mapping,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorReadMethod {
    Read,
    ReadInto,
    Mapping,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DescriptorFinish {
    Close,
    Abort,
    Drop,
    CloseTwice,
    AbortTwice,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PythonException {
    BufferError,
    ValueError,
    StreamError,
}

impl PythonException {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::BufferError => "BufferError",
            Self::ValueError => "ValueError",
            Self::StreamError => "StreamError",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActionOp {
    PublishBlob {
        path: String,
        bytes: Vec<u8>,
    },
    ReadBlob {
        path: String,
        expected: Vec<u8>,
    },
    StreamWrite {
        path: String,
        chunks: Vec<Vec<u8>>,
        replace: bool,
    },
    StreamRoundTrip {
        path: String,
        chunks: Vec<Vec<u8>>,
    },
    GatedStreamWrite {
        path: String,
        #[serde(default)]
        replace: bool,
        frames: Vec<Vec<u8>>,
        release_path: String,
    },
    StreamRead {
        path: String,
        expected: Vec<u8>,
    },
    /// A stream read that keeps retrying its attach while the stream is
    /// mid-replacement (duplicate role, stale incarnation or absence are
    /// transient) until it attaches to the live generation.
    StreamReadWithRetry {
        path: String,
        expected: Vec<u8>,
    },
    GatedStreamRead {
        path: String,
        expected: Vec<u8>,
        observed_path: String,
        #[serde(default)]
        retry_attach: bool,
        #[serde(default)]
        park_after_first_frame: bool,
    },
    StreamReadInto {
        path: String,
        expected: Vec<u8>,
        buffer_sizes: Vec<usize>,
    },
    Lookup {
        path: String,
        expected_kind: String,
    },
    AwaitEntry {
        path: String,
        expected_kind: String,
    },
    WaitForQuiescent {
        path: String,
    },
    Rename {
        source: String,
        destination: String,
        replace: bool,
    },
    Unlink {
        path: String,
    },
    DescriptorWrite {
        path: String,
        flags: i32,
        length: Option<u64>,
        bytes: Vec<u8>,
        method: DescriptorWriteMethod,
        finish: DescriptorFinish,
    },
    DescriptorRead {
        path: String,
        flags: i32,
        expected: Vec<u8>,
        method: DescriptorReadMethod,
        offset: u64,
        finish: DescriptorFinish,
    },
    MappingExportClose {
        path: String,
        length: u64,
    },
}

impl ActionOp {
    pub const fn class(&self) -> ActionClass {
        match self {
            Self::PublishBlob { .. } => ActionClass::PublishBlob,
            Self::ReadBlob { .. } => ActionClass::ReadBlob,
            Self::StreamWrite { .. }
            | Self::StreamRoundTrip { .. }
            | Self::GatedStreamWrite { .. } => ActionClass::StreamWrite,
            Self::StreamRead { .. }
            | Self::StreamReadWithRetry { .. }
            | Self::GatedStreamRead { .. }
            | Self::StreamReadInto { .. } => ActionClass::StreamRead,
            Self::Lookup { .. } | Self::AwaitEntry { .. } | Self::WaitForQuiescent { .. } => {
                ActionClass::Lookup
            }
            Self::Rename { .. } => ActionClass::Rename,
            Self::Unlink { .. } => ActionClass::Unlink,
            Self::DescriptorWrite { .. }
            | Self::DescriptorRead { .. }
            | Self::MappingExportClose { .. } => ActionClass::Descriptor,
        }
    }

    pub fn path(&self) -> &str {
        match self {
            Self::PublishBlob { path, .. }
            | Self::ReadBlob { path, .. }
            | Self::StreamWrite { path, .. }
            | Self::StreamRoundTrip { path, .. }
            | Self::GatedStreamWrite { path, .. }
            | Self::StreamRead { path, .. }
            | Self::StreamReadWithRetry { path, .. }
            | Self::GatedStreamRead { path, .. }
            | Self::StreamReadInto { path, .. }
            | Self::Lookup { path, .. }
            | Self::AwaitEntry { path, .. }
            | Self::WaitForQuiescent { path }
            | Self::Unlink { path }
            | Self::DescriptorWrite { path, .. }
            | Self::DescriptorRead { path, .. }
            | Self::MappingExportClose { path, .. } => path,
            Self::Rename { source, .. } => source,
        }
    }
}
impl ActionOp {
    pub(crate) fn map_paths(&mut self, mut map: impl FnMut(&mut String)) {
        match self {
            Self::Rename {
                source,
                destination,
                ..
            } => {
                map(source);
                map(destination);
            }
            Self::GatedStreamWrite {
                path, release_path, ..
            } => {
                map(path);
                map(release_path);
            }
            Self::GatedStreamRead {
                path,
                observed_path,
                ..
            } => {
                map(path);
                map(observed_path);
            }
            operation => map(operation.path_mut()),
        }
    }

    pub(crate) fn paths(&self) -> [Option<&str>; 2] {
        let extra = match self {
            Self::Rename { destination, .. } => Some(destination.as_str()),
            Self::GatedStreamWrite { release_path, .. } => Some(release_path.as_str()),
            Self::GatedStreamRead { observed_path, .. } => Some(observed_path.as_str()),
            _ => None,
        };
        [Some(self.path()), extra]
    }

    pub(crate) fn mutating_paths(&self) -> [Option<&str>; 2] {
        match self {
            Self::ReadBlob { .. }
            | Self::Lookup { .. }
            | Self::AwaitEntry { .. }
            | Self::WaitForQuiescent { .. }
            | Self::MappingExportClose { .. } => [None, None],
            Self::DescriptorRead { flags, path, .. } => [
                (matches!(*flags & libc::O_ACCMODE, libc::O_WRONLY | libc::O_RDWR)
                    || *flags & (libc::O_CREAT | libc::O_TRUNC) != 0)
                    .then_some(path.as_str()),
                None,
            ],
            Self::GatedStreamWrite { path, .. } => [Some(path), None],
            Self::PublishBlob { .. }
            | Self::StreamWrite { .. }
            | Self::StreamRoundTrip { .. }
            | Self::StreamRead { .. }
            | Self::StreamReadWithRetry { .. }
            | Self::GatedStreamRead { .. }
            | Self::StreamReadInto { .. }
            | Self::Rename { .. }
            | Self::Unlink { .. }
            | Self::DescriptorWrite { .. } => self.paths(),
        }
    }

    fn path_mut(&mut self) -> &mut String {
        match self {
            Self::PublishBlob { path, .. }
            | Self::ReadBlob { path, .. }
            | Self::StreamRoundTrip { path, .. }
            | Self::StreamWrite { path, .. }
            | Self::GatedStreamWrite { path, .. }
            | Self::StreamRead { path, .. }
            | Self::StreamReadWithRetry { path, .. }
            | Self::GatedStreamRead { path, .. }
            | Self::StreamReadInto { path, .. }
            | Self::Lookup { path, .. }
            | Self::AwaitEntry { path, .. }
            | Self::WaitForQuiescent { path }
            | Self::Unlink { path }
            | Self::DescriptorWrite { path, .. }
            | Self::DescriptorRead { path, .. }
            | Self::MappingExportClose { path, .. } => path,
            Self::Rename { source, .. } => source,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum ExpectedOutcome {
    Ok,
    Error(i32),
    Exception(PythonException),
    Linearized {
        group: u32,
        successes: u8,
        error: i32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Action {
    pub operation: ActionOp,
    pub expected: ExpectedOutcome,
    /// Opaque corpus-authored evidence labels threaded into rendered emits.
    ///
    /// Codegen never interprets hint semantics; it only forwards the known
    /// keys ([`EVIDENCE_HINT_TOKEN`], [`EVIDENCE_HINT_LAP`],
    /// [`EVIDENCE_HINT_EDGE_INDEX`], [`EVIDENCE_HINT_BARRIER`]) into the
    /// generated Python emit calls so scenarios can label token-ring traffic
    /// without codegen parsing corpus semantics.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub evidence_hints: BTreeMap<String, String>,
}

impl Action {
    pub fn ok(operation: ActionOp) -> Self {
        Self {
            operation,
            expected: ExpectedOutcome::Ok,
            evidence_hints: BTreeMap::new(),
        }
    }

    pub fn error(operation: ActionOp, errno: i32) -> Self {
        Self {
            operation,
            expected: ExpectedOutcome::Error(errno),
            evidence_hints: BTreeMap::new(),
        }
    }

    pub fn exception(operation: ActionOp, exception: PythonException) -> Self {
        Self {
            operation,
            expected: ExpectedOutcome::Exception(exception),
            evidence_hints: BTreeMap::new(),
        }
    }

    pub fn linearized(operation: ActionOp, group: u32, successes: u8, error: i32) -> Self {
        Self {
            operation,
            expected: ExpectedOutcome::Linearized {
                group,
                successes,
                error,
            },
            evidence_hints: BTreeMap::new(),
        }
    }

    /// Attaches opaque evidence labels to this action.
    pub fn with_evidence_hints(mut self, evidence_hints: BTreeMap<String, String>) -> Self {
        self.evidence_hints = evidence_hints;
        self
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessSpec {
    pub execution_id: String,
    pub read_prefixes: Vec<String>,
    pub write_prefixes: Vec<String>,
}

impl AccessSpec {
    pub fn unrestricted(execution_id: impl Into<String>) -> Self {
        Self {
            execution_id: execution_id.into(),
            read_prefixes: vec![
                "/models".to_owned(),
                "/cases".to_owned(),
                "/runs".to_owned(),
            ],
            write_prefixes: vec!["/cases".to_owned(), "/runs".to_owned()],
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessProgram {
    pub id: String,
    pub logical_node_id: u64,
    pub access: AccessSpec,
    #[serde(default)]
    pub depends_on: Vec<String>,
    pub actions: Vec<Action>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProcessStopPhase {
    AfterSpawn,
    DuringBootstrap,
    AfterContextReady,
    AfterStreamFirstFrame,
    AfterSiblingStreamFirstFrame,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaunchFailureKind {
    EmptyCommand,
    MissingExecutable,
    MalformedExecutionIdentity,
    PythonSyntax,
    PythonRuntime,
}

pub const CASE_SCHEMA_VERSION: u32 = 1;
pub const GENERATOR_VERSION: u32 = 6;

/// Hard admission cap, including zero-length logical frames.
pub const MAX_STREAM_FRAMES: usize = 65_536;

// The public data-plane host opens every stream endpoint with this capacity
// (crates/data-plane/src/host.rs::STREAM_RING_CAPACITY). Python read() also
// allocates a capacity-sized Rust Vec and copies it into Python bytes.
const STREAM_ENDPOINT_CAPACITY: u64 = 256 * 1024;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TopologyFamily {
    #[default]
    Fixed,
    Chain,
    RingWalk,
    FanOut,
    FanIn,
    Diamond,
    RandomDag,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CoverageScenario {
    ConcurrentStartup,
    RingCompletion,
    HotColdFairness,
    ProcessChurn,
    ActiveMutation,
    QuiescentMutation,
    ActiveStreamWriterAbort,
    ActiveStreamReaderStop,
    FailureIsolation,
    Authorization,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataKind {
    Blob,
    Stream,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataEdge {
    pub source: u64,
    pub destination: u64,
    /// Process identities owning the two logical vertices, independent of
    /// physical placement so a route may revisit a live node.
    pub source_role: String,
    pub destination_role: String,
    pub path: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DataRoute {
    pub id: String,
    pub kind: DataKind,
    pub edges: Vec<DataEdge>,
    #[serde(default)]
    pub join_inputs: Vec<String>,
}

/// Checked aggregate admission charges, not measured RSS. Allocation bytes sum
/// every action's requested data/mapping capacity, endpoint ring and public
/// binding buffers, conservatively charging sequential actions as concurrently live.
/// Observation count is an upper bound including action milestones and hints.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CaseResources {
    pub process_count: u64,
    pub action_count: u64,
    pub payload_bytes: u64,
    pub allocation_bytes: u64,
    pub observation_count: u64,
}

impl CaseResources {
    pub(crate) fn checked_add(self, other: Self) -> Result<Self, String> {
        let add = |left: u64, right: u64| {
            left.checked_add(right)
                .ok_or_else(|| "case resource count overflowed".to_owned())
        };
        Ok(Self {
            process_count: add(self.process_count, other.process_count)?,
            action_count: add(self.action_count, other.action_count)?,
            payload_bytes: add(self.payload_bytes, other.payload_bytes)?,
            allocation_bytes: add(self.allocation_bytes, other.allocation_bytes)?,
            observation_count: add(self.observation_count, other.observation_count)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaseResourceBounds {
    pub max_actions: u32,
    pub max_processes: u8,
    pub max_payload_bytes: u64,
    #[serde(default = "default_max_allocation_bytes")]
    pub max_allocation_bytes: u64,
    pub max_race_states: u32,
}

fn default_max_allocation_bytes() -> u64 {
    64 * 1024 * 1024
}

impl Default for CaseResourceBounds {
    fn default() -> Self {
        Self {
            max_actions: 64,
            max_processes: 20,
            max_payload_bytes: 8 * 1024 * 1024,
            max_allocation_bytes: default_max_allocation_bytes(),
            max_race_states: 256,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum FailureInjection {
    #[default]
    None,
    StopProcess {
        process: String,
        phase: ProcessStopPhase,
        kill_after_ms: Option<u64>,
    },
    LaunchFailure {
        process: String,
        kind: LaunchFailureKind,
    },
    /// Ordering-only fault. Explicit ordinary actions publish the park marker
    /// and await release; the namespace oracle judges their causal history.
    /// Legacy `delay_ms` cases are rejected by the required marker fields.
    SlowProcess {
        process: String,
        parked_path: String,
        release_path: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorCase {
    pub schema_version: u32,
    pub generator_version: u32,
    pub id: String,
    pub seed: u64,
    pub live_nodes: BTreeSet<u64>,
    #[serde(default)]
    pub topology: TopologyFamily,
    #[serde(default)]
    pub scenarios: BTreeSet<CoverageScenario>,
    #[serde(default)]
    pub routes: Vec<DataRoute>,
    #[serde(default)]
    pub read_only_fixture_paths: BTreeSet<String>,
    #[serde(default)]
    pub resource_bounds: CaseResourceBounds,
    pub processes: Vec<ProcessProgram>,
    #[serde(default)]
    pub failure: FailureInjection,
}

pub(crate) fn contiguous_nodes(node_count: u8) -> BTreeSet<u64> {
    (1..=u64::from(node_count)).collect()
}

fn path_is_within(path: &str, prefix: &str) -> bool {
    path == prefix
        || path
            .strip_prefix(prefix)
            .is_some_and(|suffix| suffix.starts_with('/'))
}

pub(crate) fn validate_artifact_id(id: &str, kind: &str) -> Result<(), String> {
    if !id.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        || !id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(format!(
            "{kind} ID {id:?} must be a safe single ASCII artifact component"
        ));
    }
    Ok(())
}

fn validate_data_path(path: &str) -> Result<(), String> {
    if !path.starts_with('/')
        || path[1..].split('/').any(|part| {
            part.is_empty() || matches!(part, "." | "..") || part.contains(['\0', '\\'])
        })
    {
        return Err(format!("noncanonical data path {path:?}"));
    }
    Ok(())
}

/// A single immutable namespace transformation, shared by recovery phases.
/// Fixtures owned by another phase move with that phase; external fixtures do not.
pub(crate) struct AttemptPaths {
    attempt: u64,
    read_only: BTreeSet<String>,
    owners: BTreeSet<String>,
}

impl AttemptPaths {
    pub(crate) fn new(cases: &[&BehaviorCase], attempt: u64) -> Self {
        let owned = cases
            .iter()
            .flat_map(|case| case.owned_paths())
            .collect::<BTreeSet<_>>();
        let read_only = cases
            .iter()
            .flat_map(|case| &case.read_only_fixture_paths)
            .filter(|path| !owned.contains(*path))
            .cloned()
            .collect();
        let owners = owned
            .iter()
            .filter_map(|path| path.strip_prefix("/cases/"))
            .map(|suffix| suffix.split('/').next().expect("nonempty owner").to_owned())
            .collect();
        Self {
            attempt,
            read_only,
            owners,
        }
    }

    pub(crate) fn map(&self, path: &str) -> String {
        if self.read_only.contains(path) {
            return path.to_owned();
        }
        let Some(suffix) = path.strip_prefix("/cases/") else {
            return path.to_owned();
        };
        let (owner, rest) = suffix.split_once('/').map_or((suffix, ""), |parts| parts);
        if rest.is_empty() {
            format!("/cases/{owner}/attempt-{}", self.attempt)
        } else {
            format!("/cases/{owner}/attempt-{}/{rest}", self.attempt)
        }
    }

    fn grants(&self, prefixes: &[String], write: bool) -> Vec<String> {
        let mut grants = BTreeSet::new();
        for prefix in prefixes {
            if prefix == "/cases" {
                grants.extend(
                    self.owners
                        .iter()
                        .map(|owner| self.map(&format!("/cases/{owner}"))),
                );
            } else if !self.read_only.contains(prefix) {
                grants.insert(self.map(prefix));
            }
            if !write {
                grants.extend(
                    self.read_only
                        .iter()
                        .filter(|path| path_is_within(path, prefix))
                        .cloned(),
                );
            }
        }
        grants.into_iter().collect()
    }
}

impl BehaviorCase {
    pub fn node_count(&self) -> u8 {
        u8::try_from(self.live_nodes.len()).unwrap_or(u8::MAX)
    }

    /// Bind observations to an exact scoped execution without delimiter ambiguity.
    pub fn execution_request_id(&self, process: &ProcessProgram) -> String {
        use sha2::{Digest, Sha256};

        let mut digest = Sha256::new();
        digest.update(b"myelin-e2e-execution-v1\0");
        for field in [&self.id, &process.id, &process.access.execution_id] {
            digest.update((field.len() as u64).to_le_bytes());
            digest.update(field.as_bytes());
        }
        digest.update(self.seed.to_le_bytes());
        digest.update(process.logical_node_id.to_le_bytes());
        format!("e2e-{:x}", digest.finalize())
    }

    pub(crate) fn for_attempt(&self, attempt: u64, isolate_paths: bool) -> Self {
        let paths = isolate_paths.then(|| AttemptPaths::new(&[self], attempt));
        self.with_attempt_paths(attempt, paths.as_ref())
    }

    pub(crate) fn with_attempt_paths(&self, attempt: u64, paths: Option<&AttemptPaths>) -> Self {
        let mut scoped = self.clone();
        for process in &mut scoped.processes {
            let old_execution = format!("/runs/{}", process.access.execution_id);
            process.access.execution_id =
                format!("{}-attempt-{attempt}", process.access.execution_id);
            let new_execution = format!("/runs/{}", process.access.execution_id);
            if let Some(paths) = paths {
                process.access.read_prefixes = paths.grants(&process.access.read_prefixes, false);
                process.access.write_prefixes = paths.grants(&process.access.write_prefixes, true);
                for action in &mut process.actions {
                    action.operation.map_paths(|path| *path = paths.map(path));
                }
            }
            // Grants are evaluated against resolved paths, not the /runs/self alias.
            for prefix in process
                .access
                .read_prefixes
                .iter_mut()
                .chain(&mut process.access.write_prefixes)
            {
                if prefix == "/runs" {
                    *prefix = new_execution.clone();
                } else if path_is_within(prefix, "/runs/self") {
                    *prefix = format!("{new_execution}{}", &prefix["/runs/self".len()..]);
                } else if path_is_within(prefix, &old_execution) {
                    *prefix = format!("{new_execution}{}", &prefix[old_execution.len()..]);
                }
            }
        }
        if let Some(paths) = paths {
            scoped.read_only_fixture_paths = scoped
                .read_only_fixture_paths
                .iter()
                .map(|path| paths.map(path))
                .collect();
            if let FailureInjection::SlowProcess {
                parked_path,
                release_path,
                ..
            } = &mut scoped.failure
            {
                *parked_path = paths.map(parked_path);
                *release_path = paths.map(release_path);
            }
            for route in &mut scoped.routes {
                for edge in &mut route.edges {
                    edge.path = paths.map(&edge.path);
                }
                for path in &mut route.join_inputs {
                    *path = paths.map(path);
                }
            }
        }
        scoped
    }

    pub(crate) fn owned_paths(&self) -> BTreeSet<String> {
        let mut owned = BTreeSet::new();
        for process in &self.processes {
            for path in process
                .actions
                .iter()
                .flat_map(|action| action.operation.paths())
                .flatten()
            {
                if self.read_only_fixture_paths.contains(path) {
                    continue;
                }
                if path.starts_with("/cases/") {
                    owned.insert(path.to_owned());
                } else if path_is_within(path, "/runs/self") {
                    owned.insert(format!(
                        "/runs/{}{}",
                        process.access.execution_id,
                        &path["/runs/self".len()..]
                    ));
                }
            }
        }
        owned
    }

    pub(crate) fn stream_frame_limit(&self) -> usize {
        self.processes
            .iter()
            .flat_map(|process| &process.actions)
            .filter_map(|action| match &action.operation {
                ActionOp::StreamWrite { chunks, .. } | ActionOp::StreamRoundTrip { chunks, .. } => {
                    Some(chunks.len())
                }
                ActionOp::GatedStreamWrite { frames, .. } => Some(frames.len()),
                _ => None,
            })
            .max()
            .unwrap_or(0)
            .max(1)
    }

    pub fn resource_summary(&self) -> Result<CaseResources, String> {
        let size = |value: usize| {
            u64::try_from(value).map_err(|_| "case resource size overflowed".to_owned())
        };
        let chunks_size = |chunks: &[Vec<u8>]| {
            if chunks.len() > MAX_STREAM_FRAMES {
                return Err("stream logical frame count exceeds hard admission bound".to_owned());
            }
            chunks.iter().try_fold(0_u64, |sum, chunk| {
                sum.checked_add(size(chunk.len())?)
                    .ok_or_else(|| "case chunk byte count overflowed".to_owned())
            })
        };
        let mut total = CaseResources {
            process_count: size(self.processes.len())?,
            ..Default::default()
        };
        let read_frames = size(self.stream_frame_limit())?;
        let buffered_allocation = |capacity: u64, payload: u64, copies: u64| {
            payload
                .checked_mul(copies)
                .and_then(|bytes| capacity.checked_add(bytes))
                .ok_or_else(|| "data buffer allocation overflowed".to_owned())
        };
        let framed_allocation = |length: u64, copies: u64, headers: u64, rings: u64| {
            length
                .checked_mul(copies)
                .and_then(|bytes| bytes.checked_add(headers * 16))
                .and_then(|bytes| bytes.checked_add(rings * STREAM_ENDPOINT_CAPACITY))
                .ok_or_else(|| "framed stream allocation overflowed".to_owned())
        };
        for action in self.processes.iter().flat_map(|process| &process.actions) {
            let (payload_bytes, allocation_bytes, observations) = match &action.operation {
                ActionOp::PublishBlob { bytes, .. }
                | ActionOp::ReadBlob {
                    expected: bytes, ..
                } => {
                    let length = size(bytes.len())?;
                    (length, buffered_allocation(length, length, 1)?, 1)
                }
                ActionOp::StreamWrite {
                    chunks, replace, ..
                } => {
                    let length = chunks_size(chunks)?;
                    (
                        length,
                        framed_allocation(length, 2, 2, 1)?,
                        2 + u64::from(*replace) + size(chunks.len())?,
                    )
                }
                ActionOp::StreamRoundTrip { chunks, .. } => {
                    let length = chunks_size(chunks)?;
                    (
                        length,
                        framed_allocation(length, 3, 2, 4)?,
                        5 + size(chunks.len())? * 2,
                    )
                }
                ActionOp::GatedStreamWrite { frames, .. } => {
                    if frames.is_empty() {
                        return Err("gated stream writer requires a first logical frame".to_owned());
                    }
                    let length = chunks_size(frames)?;
                    (
                        length,
                        framed_allocation(length, 2, 2, 1)?,
                        3 + size(frames.len())?,
                    )
                }
                ActionOp::StreamRead { expected, .. }
                | ActionOp::StreamReadWithRetry { expected, .. }
                | ActionOp::GatedStreamRead { expected, .. } => {
                    let length = size(expected.len())?;
                    (length, framed_allocation(length, 2, 1, 3)?, 4 + read_frames)
                }
                ActionOp::StreamReadInto {
                    expected,
                    buffer_sizes,
                    ..
                } => {
                    if buffer_sizes.is_empty() || buffer_sizes.contains(&0) {
                        return Err(
                            "stream readinto requires nonempty, nonzero buffer sizes".to_owned()
                        );
                    }
                    let buffer = size(*buffer_sizes.iter().max().expect("nonempty buffers"))?;
                    let length = size(expected.len())?;
                    (
                        length,
                        framed_allocation(length, 2, 1, 1)?
                            .checked_add(buffer)
                            .ok_or("readinto allocation overflowed")?,
                        4 + read_frames,
                    )
                }
                ActionOp::DescriptorWrite {
                    bytes,
                    length,
                    method,
                    ..
                } => {
                    let payload = size(bytes.len())?;
                    // The reservation and Python payload coexist. write()
                    // copies into Rust; writefrom() and map() borrow the bytes.
                    let copies = 1 + u64::from(*method == DescriptorWriteMethod::Write);
                    (
                        payload,
                        buffered_allocation(
                            length.unwrap_or(payload).max(payload),
                            payload,
                            copies,
                        )?,
                        1,
                    )
                }
                ActionOp::DescriptorRead {
                    expected,
                    offset,
                    method,
                    ..
                } => {
                    let length = size(expected.len())?;
                    if *method == DescriptorReadMethod::Mapping {
                        offset
                            .checked_add(length)
                            .ok_or("descriptor mapping range overflowed")?;
                    }
                    let copies = if *method == DescriptorReadMethod::Read {
                        2
                    } else {
                        1
                    };
                    (length, buffered_allocation(0, length, copies)?, 1)
                }
                ActionOp::MappingExportClose { length, .. } => (0, *length, 1),
                ActionOp::Lookup { .. }
                | ActionOp::AwaitEntry { .. }
                | ActionOp::WaitForQuiescent { .. } => (0, 0, 1),
                ActionOp::Rename { .. } | ActionOp::Unlink { .. } => (0, 0, 2),
            };
            total = total.checked_add(CaseResources {
                process_count: 0,
                action_count: 1,
                payload_bytes,
                allocation_bytes,
                observation_count: observations
                    + u64::from(action.evidence_hints.contains_key(EVIDENCE_HINT_BARRIER)),
            })?;
        }
        Ok(total)
    }

    pub fn validate(&self) -> Result<(), String> {
        validate_artifact_id(&self.id, "case")?;
        if self.schema_version != CASE_SCHEMA_VERSION {
            return Err(format!(
                "case schema {} is incompatible with supported schema {CASE_SCHEMA_VERSION}",
                self.schema_version
            ));
        }
        if self.generator_version == 0 || self.generator_version > GENERATOR_VERSION {
            return Err(format!(
                "case generator {} is incompatible with supported generator {GENERATOR_VERSION}",
                self.generator_version
            ));
        }
        if !self.routes.is_empty() && self.generator_version != GENERATOR_VERSION {
            return Err(
                "route case generator predates validated concurrent relay semantics".to_owned(),
            );
        }
        if self.live_nodes.len() < 2 || self.live_nodes.len() > 5 {
            return Err(
                "behavior case topology must contain two to five exact live nodes".to_owned(),
            );
        }
        if self.live_nodes.contains(&0) {
            return Err("logical node zero is invalid".to_owned());
        }
        let resources = self.resource_summary()?;
        if resources.process_count == 0
            || resources.process_count > u64::from(self.resource_bounds.max_processes)
            || resources.process_count > 20
        {
            return Err("behavior case must contain one to twenty bounded processes".to_owned());
        }
        if resources.action_count > u64::from(self.resource_bounds.max_actions) {
            return Err(format!(
                "case has {} actions above configured bound {}",
                resources.action_count, self.resource_bounds.max_actions
            ));
        }
        for (kind, bytes, configured, ceiling) in [
            (
                "payload",
                resources.payload_bytes,
                self.resource_bounds.max_payload_bytes,
                8 * 1024 * 1024,
            ),
            (
                "allocation",
                resources.allocation_bytes,
                self.resource_bounds.max_allocation_bytes,
                64 * 1024 * 1024,
            ),
        ] {
            let bound = configured.min(ceiling);
            if bytes > bound {
                return Err(format!(
                    "case has {bytes} {kind} bytes above configured bound {bound}"
                ));
            }
        }
        for path in &self.read_only_fixture_paths {
            validate_data_path(path)?;
        }
        for operation in self
            .processes
            .iter()
            .flat_map(|process| process.actions.iter().map(|action| &action.operation))
        {
            for path in operation.paths().into_iter().flatten() {
                validate_data_path(path)?;
            }
            for path in operation.mutating_paths().into_iter().flatten() {
                if self.read_only_fixture_paths.contains(path) {
                    return Err(format!(
                        "case mutates declared read-only fixture path {path:?}"
                    ));
                }
                if !path.starts_with("/cases/") && !path_is_within(path, "/runs/self") {
                    return Err(format!(
                        "writable path {path:?} has no attempt-local ownership"
                    ));
                }
            }
        }
        if self.resource_bounds.max_race_states == 0 {
            return Err("case race-state bound must be nonzero".to_owned());
        }

        let mut ids = BTreeSet::new();
        let mut execution_ids = BTreeSet::new();
        for process in &self.processes {
            validate_artifact_id(&process.id, "process")?;
            validate_artifact_id(&process.access.execution_id, "execution")?;
            if !execution_ids.insert(&process.access.execution_id) {
                return Err(
                    "contextual processes must have distinct execution identities".to_owned(),
                );
            }
            for prefix in process
                .access
                .read_prefixes
                .iter()
                .chain(&process.access.write_prefixes)
            {
                validate_data_path(prefix)?;
            }
            let execution_root = format!("/runs/{}", process.access.execution_id);
            for prefix in &process.access.write_prefixes {
                if !path_is_within(prefix, "/cases")
                    && prefix != "/runs"
                    && !path_is_within(prefix, "/runs/self")
                    && !path_is_within(prefix, &execution_root)
                {
                    return Err(format!(
                        "write grant {prefix:?} has no attempt-local ownership"
                    ));
                }
            }
            if process.id.is_empty() || !ids.insert(process.id.clone()) {
                return Err(format!(
                    "process ID {:?} is empty or duplicated",
                    process.id
                ));
            }
            if !self.live_nodes.contains(&process.logical_node_id) {
                return Err(format!(
                    "process {} targets node {} outside exact live set {:?}",
                    process.id, process.logical_node_id, self.live_nodes
                ));
            }
            if process.actions.is_empty() {
                return Err(format!("process {} has no actions", process.id));
            }
            for action in &process.actions {
                if matches!(
                    action.operation,
                    ActionOp::GatedStreamRead {
                        park_after_first_frame: true,
                        ..
                    }
                ) && !matches!(
                    &self.failure,
                    FailureInjection::StopProcess {
                        process: target,
                        phase: ProcessStopPhase::AfterStreamFirstFrame,
                        ..
                    } if target == &process.id
                ) {
                    return Err(
                        "a parked stream reader must be the first-frame stop target".to_owned()
                    );
                }
            }
        }
        for process in &self.processes {
            for dependency in &process.depends_on {
                if dependency == &process.id || !ids.contains(dependency) {
                    return Err(format!(
                        "process {} has invalid dependency {dependency:?}",
                        process.id
                    ));
                }
            }
        }
        let mut complete = BTreeSet::new();
        while complete.len() < self.processes.len() {
            let before = complete.len();
            for process in &self.processes {
                if process
                    .depends_on
                    .iter()
                    .all(|dependency| complete.contains(dependency))
                {
                    complete.insert(process.id.clone());
                }
            }
            if complete.len() == before {
                return Err("process dependency graph contains a cycle".to_owned());
            }
        }
        let mut race_groups = std::collections::BTreeMap::<u32, (u8, u32)>::new();
        for action in self.processes.iter().flat_map(|process| &process.actions) {
            let ExpectedOutcome::Linearized {
                group, successes, ..
            } = action.expected
            else {
                continue;
            };
            let entry = race_groups.entry(group).or_insert((successes, 0));
            if entry.0 != successes {
                return Err(format!(
                    "race group {group} declares conflicting legal success counts"
                ));
            }
            entry.1 = entry
                .1
                .checked_add(1)
                .ok_or_else(|| "race group member count overflowed".to_owned())?;
        }
        let mut legal_states = 1_u64;
        for (group, (successes, members)) in race_groups {
            if members < 2 {
                return Err(format!(
                    "race group {group} requires at least two operations"
                ));
            }
            if u32::from(successes) > members {
                return Err(format!(
                    "race group {group} requires {successes} successes from {members} operations"
                ));
            }
            let choose = bounded_binomial(members, u32::from(successes), legal_states)
                .ok_or_else(|| "race legal-state count overflowed".to_owned())?;
            legal_states = legal_states
                .checked_mul(choose)
                .ok_or_else(|| "race legal-state count overflowed".to_owned())?;
            if legal_states > u64::from(self.resource_bounds.max_race_states) {
                return Err(format!(
                    "case has {legal_states} legal race states above configured bound {}",
                    self.resource_bounds.max_race_states
                ));
            }
        }
        for action in self.processes.iter().flat_map(|process| &process.actions) {
            for key in action.evidence_hints.keys() {
                if !matches!(
                    key.as_str(),
                    EVIDENCE_HINT_TOKEN
                        | EVIDENCE_HINT_LAP
                        | EVIDENCE_HINT_EDGE_INDEX
                        | EVIDENCE_HINT_BARRIER
                ) {
                    return Err(format!(
                        "action evidence hint {key:?} is not a supported hint key"
                    ));
                }
            }
            for key in [EVIDENCE_HINT_LAP, EVIDENCE_HINT_EDGE_INDEX] {
                if let Some(value) = action.evidence_hints.get(key) {
                    if value.parse::<u32>().is_err() {
                        return Err(format!(
                            "action evidence hint {key:?} must be a decimal u32, observed {value:?}"
                        ));
                    }
                }
            }
            if let Some(barrier) = action.evidence_hints.get(EVIDENCE_HINT_BARRIER) {
                if !matches!(
                    barrier.as_str(),
                    BARRIER_TOKEN_RECEIVED | BARRIER_TOKEN_FORWARDED | BARRIER_LAP_COMPLETED
                ) {
                    return Err(format!(
                        "action evidence hint barrier {barrier:?} is not a supported barrier kind"
                    ));
                }
                if !action.evidence_hints.contains_key(EVIDENCE_HINT_TOKEN) {
                    return Err(format!(
                        "action evidence hint barrier {barrier:?} requires a {EVIDENCE_HINT_TOKEN:?} hint"
                    ));
                }
            }
        }
        let mut route_ids = BTreeSet::new();
        let mut route_paths = BTreeSet::new();
        for route in &self.routes {
            if route.id.is_empty() || !route_ids.insert(&route.id) || route.edges.is_empty() {
                return Err(
                    "data routes require a non-empty identity and at least one edge".to_owned(),
                );
            }
            for edge in &route.edges {
                if !route_paths.insert(edge.path.as_str()) {
                    return Err("data routes must not share writable edge paths".to_owned());
                }
                if edge.source == edge.destination
                    || edge.source_role == edge.destination_role
                    || !self.live_nodes.contains(&edge.source)
                    || !self.live_nodes.contains(&edge.destination)
                    || edge.path.is_empty()
                {
                    return Err(format!("route {} contains invalid edge {edge:?}", route.id));
                }
                for (node, role, source) in [
                    (edge.source, &edge.source_role, true),
                    (edge.destination, &edge.destination_role, false),
                ] {
                    let witnesses = self
                        .processes
                        .iter()
                        .flat_map(|process| {
                            process.actions.iter().map(move |action| (process, action))
                        })
                        .filter(|(_, action)| action.operation.path() == edge.path)
                        .filter(
                            |(_, action)| match (route.kind, source, &action.operation) {
                                (DataKind::Blob, true, ActionOp::PublishBlob { .. })
                                | (DataKind::Blob, false, ActionOp::ReadBlob { .. })
                                | (
                                    DataKind::Stream,
                                    true,
                                    ActionOp::StreamWrite { .. }
                                    | ActionOp::GatedStreamWrite { .. },
                                )
                                | (
                                    DataKind::Stream,
                                    false,
                                    ActionOp::StreamRead { .. }
                                    | ActionOp::StreamReadInto { .. }
                                    | ActionOp::StreamReadWithRetry { .. }
                                    | ActionOp::GatedStreamRead { .. },
                                ) => true,
                                _ => false,
                            },
                        )
                        .map(|(process, _)| process);
                    let mut witnesses = witnesses;
                    if !witnesses.next().is_some_and(|process| {
                        process.logical_node_id == node && &process.id == role
                    }) || witnesses.next().is_some()
                    {
                        return Err(format!(
                            "route {} requires exactly one endpoint owned by {role:?} on node {node} for {:?}",
                            route.id, edge.path
                        ));
                    }
                }
            }
            if self.topology == TopologyFamily::RandomDag {
                let vertices = route
                    .edges
                    .iter()
                    .flat_map(|edge| [edge.source_role.as_str(), edge.destination_role.as_str()])
                    .collect::<BTreeSet<_>>();
                let mut ready = BTreeSet::new();
                while ready.len() < vertices.len() {
                    let before = ready.len();
                    for vertex in &vertices {
                        if route
                            .edges
                            .iter()
                            .filter(|edge| edge.destination_role == *vertex)
                            .all(|edge| ready.contains(edge.source_role.as_str()))
                        {
                            ready.insert(*vertex);
                        }
                    }
                    if ready.len() == before {
                        return Err(format!("route {} contains a logical-role cycle", route.id));
                    }
                }
            }
            let ring = self.topology == TopologyFamily::RingWalk && route.edges.len() > 1;
            let (read_class, write_class) = match route.kind {
                DataKind::Blob => (ActionClass::ReadBlob, ActionClass::PublishBlob),
                DataKind::Stream => (ActionClass::StreamRead, ActionClass::StreamWrite),
            };
            for (edge_index, edge) in route.edges.iter().enumerate() {
                let (writer, step) = self
                    .processes
                    .iter()
                    .find_map(|program| {
                        (program.logical_node_id == edge.source && program.id == edge.source_role)
                            .then(|| {
                                program
                                    .actions
                                    .iter()
                                    .position(|action| {
                                        action.operation.path() == edge.path
                                            && action.operation.class() == write_class
                                    })
                                    .map(|step| (program, step))
                            })
                            .flatten()
                    })
                    .expect("unique route source checked above");
                if self
                    .scenarios
                    .contains(&CoverageScenario::ConcurrentStartup)
                    && !writer.depends_on.is_empty()
                {
                    return Err(
                        "concurrent route sources must start without completion dependencies"
                            .to_owned(),
                    );
                }
                for (input_index, input) in route.edges.iter().enumerate() {
                    let required = if ring {
                        edge_index > 0 && input_index == edge_index - 1
                    } else {
                        input.destination_role == edge.source_role
                    };
                    if required
                        && (input.destination_role != edge.source_role
                            || !writer.actions[..step].iter().any(|action| {
                                action.operation.path() == input.path
                                    && action.operation.class() == read_class
                            }))
                    {
                        return Err(format!(
                            "route {} forwards before consuming its required input {:?}",
                            route.id, input.path
                        ));
                    }
                }
            }
            for path in route
                .edges
                .iter()
                .map(|edge| edge.path.as_str())
                .chain(route.join_inputs.iter().map(String::as_str))
            {
                validate_data_path(path)?;
            }
            if route.join_inputs.iter().collect::<BTreeSet<_>>().len() != route.join_inputs.len()
                || route
                    .join_inputs
                    .iter()
                    .any(|path| !route.edges.iter().any(|edge| edge.path == *path))
            {
                return Err(format!(
                    "route {} has duplicate or absent join inputs",
                    route.id
                ));
            }
        }
        if self.topology != TopologyFamily::Fixed {
            if self.routes.is_empty() {
                return Err("case topology requires a structural route witness".to_owned());
            }
            if !self
                .routes
                .iter()
                .any(|route| crate::coverage::route_has_topology(route, self.topology))
            {
                return Err(format!(
                    "case lacks a structurally valid {:?} route",
                    self.topology
                ));
            }
        }
        match &self.failure {
            FailureInjection::None => {}
            FailureInjection::StopProcess { process, .. }
            | FailureInjection::LaunchFailure { process, .. }
            | FailureInjection::SlowProcess { process, .. }
                if ids.contains(process) => {}
            FailureInjection::StopProcess { process, .. }
            | FailureInjection::LaunchFailure { process, .. }
            | FailureInjection::SlowProcess { process, .. } => {
                return Err(format!(
                    "fault injection targets unknown process {process:?}"
                ));
            }
        }
        if let FailureInjection::SlowProcess {
            process,
            parked_path,
            release_path,
        } = &self.failure
        {
            self.validate_slow_process(process, parked_path, release_path)?;
        }
        Ok(())
    }

    fn validate_slow_process(
        &self,
        target: &str,
        parked_path: &str,
        release_path: &str,
    ) -> Result<(), String> {
        let invalid = || {
            "slow process requires owned park -> healthy completion -> release actions".to_owned()
        };
        if parked_path == release_path
            || !parked_path.starts_with("/cases/")
            || !release_path.starts_with("/cases/")
            || self.read_only_fixture_paths.contains(parked_path)
            || self.read_only_fixture_paths.contains(release_path)
        {
            return Err(invalid());
        }
        let awaits = |action: &Action, path: &str| {
            action.expected == ExpectedOutcome::Ok
                && matches!(&action.operation, ActionOp::AwaitEntry { path: observed, expected_kind }
                    if observed == path && expected_kind == "blob")
        };
        let publishes = |action: &Action, path: &str| {
            action.expected == ExpectedOutcome::Ok
                && matches!(&action.operation, ActionOp::PublishBlob { path: observed, bytes }
                    if observed == path && bytes.is_empty())
        };
        let slow = self
            .processes
            .iter()
            .find(|program| program.id == target)
            .ok_or_else(invalid)?;
        if !slow.depends_on.is_empty()
            || slow.actions.len() < 3
            || !publishes(&slow.actions[0], parked_path)
            || !awaits(&slow.actions[1], release_path)
        {
            return Err(invalid());
        }
        let releasers = self
            .processes
            .iter()
            .filter(|program| {
                program
                    .actions
                    .last()
                    .is_some_and(|action| publishes(action, release_path))
            })
            .collect::<Vec<_>>();
        let [releaser] = releasers.as_slice() else {
            return Err(invalid());
        };
        let healthy = self
            .processes
            .iter()
            .filter(|program| program.id != target && program.id != releaser.id)
            .collect::<Vec<_>>();
        if healthy.is_empty()
            || releaser.actions.len() != 2
            || !awaits(&releaser.actions[0], parked_path)
            || releaser.depends_on.iter().collect::<BTreeSet<_>>()
                != healthy
                    .iter()
                    .map(|program| &program.id)
                    .collect::<BTreeSet<_>>()
            || healthy.iter().any(|program| {
                program
                    .depends_on
                    .iter()
                    .any(|dependency| dependency == target || dependency == &releaser.id)
                    || (program.depends_on.is_empty()
                        && !program
                            .actions
                            .first()
                            .is_some_and(|action| awaits(action, parked_path)))
            })
        {
            return Err(invalid());
        }
        // Marker publishers are unique, and no mutation may spoof/retract a
        // marker between the observations establishing the causal chain.
        for program in &self.processes {
            for (step, action) in program.actions.iter().enumerate() {
                let allowed = (program.id == target && step == 0)
                    || (program.id == releaser.id && step == 1)
                    || matches!(action.operation, ActionOp::AwaitEntry { .. });
                if !allowed
                    && (action.operation.path() == parked_path
                        || action.operation.path() == release_path
                        || matches!(&action.operation,
                            ActionOp::Rename { destination, .. }
                                | ActionOp::GatedStreamRead { observed_path: destination, .. }
                            if destination == parked_path || destination == release_path))
                {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
}

fn bounded_binomial(n: u32, k: u32, current_product: u64) -> Option<u64> {
    let k = k.min(n - k);
    let mut result = 1_u64;
    for divisor in 1..=k {
        result = result.checked_mul(u64::from(n - k + divisor))?;
        result /= u64::from(divisor);
        if result.checked_mul(current_product).is_none() {
            return None;
        }
    }
    Some(result)
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum DescriptorTerminalResult {
    Ok,
    Error {
        errno: Option<i32>,
        error_type: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "direction", rename_all = "snake_case")]
pub enum DescriptorObservation {
    Write {
        method: DescriptorWriteMethod,
        finish: DescriptorFinish,
        terminal_results: Vec<DescriptorTerminalResult>,
        #[serde(default)]
        dropped: bool,
        #[serde(default)]
        reservation_released: bool,
    },
    Read {
        method: DescriptorReadMethod,
        finish: DescriptorFinish,
        terminal_results: Vec<DescriptorTerminalResult>,
    },
}
/// Evidence-hint keys understood by codegen. Values are opaque corpus
/// labels; `lap`/`edge_index` values must be decimal `u32` text.
pub const EVIDENCE_HINT_TOKEN: &str = "token";
pub const EVIDENCE_HINT_LAP: &str = "lap";
pub const EVIDENCE_HINT_EDGE_INDEX: &str = "edge_index";
pub const EVIDENCE_HINT_BARRIER: &str = "barrier";

/// Barrier kinds an action may request via the `barrier` evidence hint.
pub const BARRIER_TOKEN_RECEIVED: &str = "token_received";
pub const BARRIER_TOKEN_FORWARDED: &str = "token_forwarded";
pub const BARRIER_LAP_COMPLETED: &str = "lap_completed";

/// A causal milestone record emitted by a binding operation that actually
/// occurred. Barrier records reuse [`ActionObservation`] with
/// `outcome == "barrier"`, the owning action's `step`/`action`/`path`, and
/// the milestone payload in [`ActionObservation::barrier`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BarrierObservation {
    /// A `write_stream` context was entered or `read_stream` returned a
    /// reader. `incarnation` is the binding-assigned nonzero namespace
    /// revision identifying the attached stream, not a pre-open lookup.
    StreamOpened { incarnation: u64 },
    /// One completely sent or decoded logical frame. The observation action
    /// identifies stream_write versus stream_read, including round trips.
    StreamFrame {
        incarnation: u64,
        index: u64,
        length: u64,
        digest: String,
    },
    /// The first frame of a stream arrived (for gated reads this doubles as
    /// the release-observed milestone).
    StreamFirstFrame { incarnation: u64 },
    /// The stream reader observed end-of-stream.
    StreamEof { incarnation: u64 },
    /// The gated writer observed its release path appear in the namespace.
    ReleaseObserved { path: String },
    /// A labeled ring token arrived at this action.
    TokenReceived {
        token: String,
        lap: u32,
        edge_index: u32,
    },
    /// A labeled ring token was forwarded past this action.
    TokenForwarded {
        token: String,
        lap: u32,
        edge_index: u32,
    },
    /// A labeled ring lap completed at this action.
    LapCompleted { token: String, lap: u32 },
    /// Reserved for harness-side lifecycle evidence (not emitted by codegen).
    ProcessStopped { process: String },
    /// Reserved for harness-side lifecycle evidence (not emitted by codegen).
    ProcessSpawned { process: String },
    /// Reserved for harness-side lifecycle evidence (not emitted by codegen).
    FailureObserved { process: String },
    /// A namespace mutation committed: `to_revision` is the revision the
    /// binding returned; `from_revision` is the pre-mutation lookup revision
    /// when the entry already existed.
    MutationApplied {
        from_revision: Option<u64>,
        to_revision: Option<u64>,
    },
}

/// Bytes actually transferred before the terminal outcome. An absent value
/// means no transfer completed; incomplete values describe the observed prefix.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferObservation {
    pub length: usize,
    pub digest: String,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionObservation {
    pub process: String,
    pub step: usize,
    pub action: String,
    pub path: String,
    pub outcome: String,
    pub length: Option<usize>,
    pub digest: Option<String>,
    pub kind: Option<String>,
    pub revision: Option<u64>,
    pub active: Option<bool>,
    pub errno: Option<i32>,
    pub error_type: Option<String>,
    pub error: Option<String>,
    pub descriptor: Option<DescriptorObservation>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transfer: Option<TransferObservation>,
    /// Causal milestone payload when `outcome == "barrier"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub barrier: Option<BarrierObservation>,
    /// Binding-assigned nonzero identity of the attached stream incarnation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub incarnation: Option<u64>,
    /// Corpus-labeled ring token this action participated in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    /// Corpus-labeled ring lap this action participated in.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lap: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionObservation {
    pub process: String,
    pub request_id: String,
    pub logical_node_id: u64,
    pub lifecycle: Vec<String>,
    pub results: Vec<ActionObservation>,
    pub terminal: bool,
    pub exit_success: bool,
    #[serde(default)]
    pub exit_status: Option<String>,
    pub stdout: String,
    pub stderr: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaseObservation {
    pub case_id: String,
    pub executions: Vec<ExecutionObservation>,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn case(operation: ActionOp) -> BehaviorCase {
        BehaviorCase {
            schema_version: CASE_SCHEMA_VERSION,
            generator_version: GENERATOR_VERSION,
            id: "resource-case".to_owned(),
            seed: 1,
            live_nodes: BTreeSet::from([1, 2]),
            topology: TopologyFamily::Fixed,
            scenarios: BTreeSet::new(),
            routes: Vec::new(),
            read_only_fixture_paths: BTreeSet::new(),
            resource_bounds: CaseResourceBounds::default(),
            processes: vec![ProcessProgram {
                id: "worker".to_owned(),
                logical_node_id: 1,
                access: AccessSpec::unrestricted("worker"),
                depends_on: Vec::new(),
                actions: vec![Action::ok(operation)],
            }],
            failure: FailureInjection::None,
        }
    }

    #[test]
    fn configured_resource_bounds_cannot_raise_hard_case_ceilings() {
        let mut input = case(ActionOp::MappingExportClose {
            path: "/models/fixture".to_owned(),
            length: 64 * 1024 * 1024,
        });
        input.resource_bounds.max_allocation_bytes = u64::MAX;
        input.validate().unwrap();
        if let ActionOp::MappingExportClose { length, .. } =
            &mut input.processes[0].actions[0].operation
        {
            *length += 1;
        }
        assert!(input.validate().is_err());
        input.processes[0].actions[0].operation = ActionOp::PublishBlob {
            path: "/cases/bounds/blob".to_owned(),
            bytes: vec![0; 8 * 1024 * 1024 + 1],
        };
        input.resource_bounds.max_payload_bytes = u64::MAX;
        assert!(input.validate().is_err());
    }

    #[test]
    fn singleton_race_group_is_rejected_before_execution() {
        let mut input = case(ActionOp::Unlink {
            path: "/cases/race/missing".to_owned(),
        });
        input.processes[0].actions[0].expected = ExpectedOutcome::Linearized {
            group: 1,
            successes: 0,
            error: libc::ENOENT,
        };
        assert!(input.validate().is_err());
        let peer = input.processes[0].actions[0].clone();
        input.processes[0].actions.push(peer);
        input.validate().unwrap();
    }

    #[test]
    fn request_identities_distinguish_delimiters_and_attempts() {
        let mut template = case(ActionOp::PublishBlob {
            path: "/runs/self/output".to_owned(),
            bytes: vec![],
        });
        template.processes[0].id = "a-1-b".to_owned();
        template.processes[0].access = AccessSpec::unrestricted("c");
        let mut sibling = template.processes[0].clone();
        sibling.id = "a".to_owned();
        sibling.access = AccessSpec::unrestricted("b-1-c");
        template.processes.push(sibling);
        template.validate().unwrap();
        let first = template.for_attempt(7, true);
        let second = template.for_attempt(8, true);
        assert_ne!(
            first.execution_request_id(&first.processes[0]),
            first.execution_request_id(&first.processes[1]),
        );
        assert_ne!(
            first.execution_request_id(&first.processes[0]),
            second.execution_request_id(&second.processes[0]),
        );
    }

    #[test]
    fn attempt_scoping_preserves_restricted_grants_and_fixture_reads() {
        let allowed = "/cases/group/allowed/source";
        let denied = "/cases/group/denied/entry";
        let fixture = "/cases/fixture/blob";
        let mut template = case(ActionOp::PublishBlob {
            path: allowed.to_owned(),
            bytes: vec![1],
        });
        template.read_only_fixture_paths.insert(fixture.to_owned());
        template.processes[0].access.read_prefixes =
            vec!["/cases/group/allowed".to_owned(), fixture.to_owned()];
        template.processes[0].access.write_prefixes = vec!["/cases/group/allowed".to_owned()];
        template.processes[0].actions.extend([
            Action::error(
                ActionOp::Rename {
                    source: denied.to_owned(),
                    destination: allowed.to_owned(),
                    replace: false,
                },
                libc::EACCES,
            ),
            Action::error(
                ActionOp::Rename {
                    source: allowed.to_owned(),
                    destination: denied.to_owned(),
                    replace: false,
                },
                libc::EACCES,
            ),
            Action::ok(ActionOp::ReadBlob {
                path: fixture.to_owned(),
                expected: vec![2],
            }),
        ]);
        template.processes.push(ProcessProgram {
            id: "reader".to_owned(),
            logical_node_id: 2,
            access: AccessSpec::unrestricted("reader"),
            depends_on: Vec::new(),
            actions: vec![Action::ok(ActionOp::ReadBlob {
                path: allowed.to_owned(),
                expected: vec![1],
            })],
        });
        template.routes.push(DataRoute {
            id: "allowed-route".to_owned(),
            kind: DataKind::Blob,
            edges: vec![DataEdge {
                source: 1,
                destination: 2,
                source_role: "worker".to_owned(),
                destination_role: "reader".to_owned(),
                path: allowed.to_owned(),
            }],
            join_inputs: vec![allowed.to_owned()],
        });
        template.validate().unwrap();
        let first = template.for_attempt(3, true);
        let second = template.for_attempt(4, true);
        first.validate().unwrap();
        second.validate().unwrap();
        let granted = |prefixes: &[String], path: &str| {
            prefixes.iter().any(|prefix| path_is_within(path, prefix))
        };
        for (before, after) in template.processes[0]
            .actions
            .iter()
            .zip(&first.processes[0].actions)
        {
            assert_eq!(before.expected, after.expected);
            for (old_path, new_path) in before
                .operation
                .paths()
                .into_iter()
                .flatten()
                .zip(after.operation.paths().into_iter().flatten())
            {
                assert_eq!(
                    granted(&template.processes[0].access.read_prefixes, old_path),
                    granted(&first.processes[0].access.read_prefixes, new_path)
                );
                assert_eq!(
                    granted(&template.processes[0].access.write_prefixes, old_path),
                    granted(&first.processes[0].access.write_prefixes, new_path)
                );
            }
        }
        assert_eq!(
            first.routes[0].edges[0].path,
            first.processes[0].actions[0].operation.path()
        );
        assert_eq!(
            first.routes[0].join_inputs[0],
            first.routes[0].edges[0].path
        );
        assert_eq!(first.processes[0].actions[3].operation.path(), fixture);
        assert!(!first.owned_paths().contains(fixture));
        assert!(first.owned_paths().is_disjoint(&second.owned_paths()));
        for path in second.owned_paths() {
            assert!(!granted(&first.processes[0].access.write_prefixes, &path));
        }
    }

    #[test]
    fn attempts_resolve_self_grants_without_aliasing_and_reject_fixed_writes() {
        let template = case(ActionOp::PublishBlob {
            path: "/runs/self/output".to_owned(),
            bytes: vec![],
        });
        let first = template.for_attempt(1, true);
        let second = first.for_attempt(2, false);
        first.validate().unwrap();
        second.validate().unwrap();
        assert_eq!(
            first.processes[0].actions[0].operation.path(),
            "/runs/self/output"
        );
        assert!(first.owned_paths().is_disjoint(&second.owned_paths()));
        for path in first.owned_paths() {
            assert!(
                first.processes[0]
                    .access
                    .write_prefixes
                    .iter()
                    .any(|prefix| path_is_within(&path, prefix))
            );
            assert!(
                !second.processes[0]
                    .access
                    .write_prefixes
                    .iter()
                    .any(|prefix| path_is_within(&path, prefix))
            );
        }
        assert!(
            case(ActionOp::PublishBlob {
                path: "/runs/fixed/shared".to_owned(),
                bytes: vec![]
            })
            .validate()
            .is_err()
        );
        assert!(
            case(ActionOp::PublishBlob {
                path: "/runs/selfish/shared".to_owned(),
                bytes: vec![]
            })
            .validate()
            .is_err()
        );
    }

    #[test]
    fn artifact_components_reject_traversal_and_normalization_aliases() {
        let template = case(ActionOp::Lookup {
            path: "/cases/test/missing".to_owned(),
            expected_kind: "blob".to_owned(),
        });
        for id in [
            "",
            ".",
            "..",
            "../escape",
            "/absolute",
            "nested/file",
            "nested\\file",
            "trailing.",
            "space ",
            "a\0b",
            "café",
        ] {
            let mut invalid = template.clone();
            invalid.id = id.to_owned();
            assert!(invalid.validate().is_err(), "case {id:?}");
            invalid = template.clone();
            invalid.processes[0].id = id.to_owned();
            assert!(invalid.validate().is_err(), "process {id:?}");
            invalid = template.clone();
            invalid.processes[0].access.execution_id = id.to_owned();
            assert!(invalid.validate().is_err(), "execution {id:?}");
        }
        let decoded: BehaviorCase =
            serde_json::from_str(&serde_json::to_string(&template).unwrap()).unwrap();
        decoded.validate().unwrap();
        assert_eq!(decoded, template);
    }

    #[test]
    fn requested_allocations_and_read_buffers_are_bounded_before_generation() {
        let mut input = case(ActionOp::StreamReadInto {
            path: "/cases/test/stream".to_owned(),
            expected: vec![],
            buffer_sizes: vec![1],
        });
        input.resource_bounds.max_allocation_bytes = STREAM_ENDPOINT_CAPACITY + 17;
        input.validate().unwrap();
        input.resource_bounds.max_allocation_bytes -= 1;
        assert!(input.validate().is_err());
        input.resource_bounds.max_allocation_bytes += 1;
        for sizes in [vec![], vec![0], vec![1, 0], vec![1_usize << 40]] {
            if let ActionOp::StreamReadInto { buffer_sizes, .. } =
                &mut input.processes[0].actions[0].operation
            {
                *buffer_sizes = sizes;
            }
            assert!(input.validate().is_err());
        }
        input.processes[0].actions[0].operation = ActionOp::DescriptorWrite {
            path: "/cases/test/blob".to_owned(),
            flags: libc::O_WRONLY | libc::O_CREAT,
            length: Some(1 << 40),
            bytes: vec![],
            method: DescriptorWriteMethod::Write,
            finish: DescriptorFinish::Abort,
        };
        assert!(input.validate().is_err());
        input.processes[0].actions[0].operation = ActionOp::MappingExportClose {
            path: "/cases/test/blob".to_owned(),
            length: 1 << 40,
        };
        assert!(input.validate().is_err());
    }

    #[test]
    fn descriptor_copying_methods_require_their_additional_buffers() {
        let mut input = case(ActionOp::DescriptorRead {
            path: "/cases/test/blob".to_owned(),
            flags: libc::O_RDONLY,
            expected: vec![1; 8],
            method: DescriptorReadMethod::ReadInto,
            offset: 0,
            finish: DescriptorFinish::Close,
        });
        input.resource_bounds.max_allocation_bytes = 8;
        input.validate().unwrap();
        if let ActionOp::DescriptorRead { method, .. } =
            &mut input.processes[0].actions[0].operation
        {
            *method = DescriptorReadMethod::Read;
        }
        assert!(input.validate().is_err());
        input.resource_bounds.max_allocation_bytes = 16;
        input.validate().unwrap();

        input.processes[0].actions[0].operation = ActionOp::DescriptorWrite {
            path: "/cases/test/blob".to_owned(),
            flags: libc::O_WRONLY | libc::O_CREAT,
            length: Some(16),
            bytes: vec![1; 8],
            method: DescriptorWriteMethod::WriteFrom,
            finish: DescriptorFinish::Close,
        };
        input.resource_bounds.max_allocation_bytes = 24;
        input.validate().unwrap();
        if let ActionOp::DescriptorWrite { method, .. } =
            &mut input.processes[0].actions[0].operation
        {
            *method = DescriptorWriteMethod::Write;
        }
        assert!(input.validate().is_err());
        input.resource_bounds.max_allocation_bytes = 32;
        input.validate().unwrap();
    }

    #[test]
    fn resource_summary_counts_roundtrip_retry_and_checked_descriptor_ranges() {
        let mut input = case(ActionOp::StreamRoundTrip {
            path: "/cases/test/stream".to_owned(),
            chunks: vec![vec![], vec![1, 2], vec![3]],
        });
        input.processes[0]
            .actions
            .push(Action::ok(ActionOp::StreamReadWithRetry {
                path: "/cases/test/retry".to_owned(),
                expected: vec![4, 5],
            }));
        let resources = input.resource_summary().unwrap();
        assert_eq!(resources.payload_bytes, 5);
        assert_eq!(
            resources.allocation_bytes,
            61 + 7 * STREAM_ENDPOINT_CAPACITY
        );
        assert_eq!(resources.observation_count, 18);
        input.processes[0]
            .actions
            .push(Action::ok(ActionOp::DescriptorRead {
                path: "/cases/test/blob".to_owned(),
                flags: libc::O_RDONLY,
                expected: vec![1],
                method: DescriptorReadMethod::Mapping,
                offset: u64::MAX,
                finish: DescriptorFinish::Close,
            }));
        assert!(input.validate().is_err());
        assert!(
            CaseResources {
                payload_bytes: u64::MAX,
                ..Default::default()
            }
            .checked_add(resources)
            .is_err()
        );
    }
}
