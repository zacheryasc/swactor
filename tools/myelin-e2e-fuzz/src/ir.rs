//! Typed intermediate representation of generated behavioral programs.

use std::collections::BTreeSet;

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
    StreamRead {
        path: String,
        expected: Vec<u8>,
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
            Self::StreamWrite { .. } => ActionClass::StreamWrite,
            Self::StreamRead { .. } | Self::StreamReadInto { .. } => ActionClass::StreamRead,
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
            | Self::StreamRead { path, .. }
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
}

impl Action {
    pub fn ok(operation: ActionOp) -> Self {
        Self {
            operation,
            expected: ExpectedOutcome::Ok,
        }
    }

    pub fn error(operation: ActionOp, errno: i32) -> Self {
        Self {
            operation,
            expected: ExpectedOutcome::Error(errno),
        }
    }

    pub fn exception(operation: ActionOp, exception: PythonException) -> Self {
        Self {
            operation,
            expected: ExpectedOutcome::Exception(exception),
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
        }
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
    KillNode {
        logical_node_id: u64,
    },
    StopOrchestrator,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BehaviorCase {
    pub id: String,
    pub seed: u64,
    pub node_count: u8,
    pub processes: Vec<ProcessProgram>,
    #[serde(default)]
    pub failure: FailureInjection,
}

impl BehaviorCase {
    pub fn validate(&self) -> Result<(), String> {
        if self.node_count < 2 || self.node_count > 3 {
            return Err("behavior case topology must contain two or three nodes".to_owned());
        }
        if self.processes.is_empty() || self.processes.len() > 3 {
            return Err("behavior case must contain one to three processes".to_owned());
        }
        let mut ids = BTreeSet::new();
        for process in &self.processes {
            if process.id.is_empty() || !ids.insert(process.id.clone()) {
                return Err(format!(
                    "process ID {:?} is empty or duplicated",
                    process.id
                ));
            }
            if process.logical_node_id == 0 || process.logical_node_id > u64::from(self.node_count)
            {
                return Err(format!(
                    "process {} targets node {} outside topology 1..={}",
                    process.id, process.logical_node_id, self.node_count
                ));
            }
            if process.actions.is_empty() {
                return Err(format!("process {} has no actions", process.id));
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
        match &self.failure {
            FailureInjection::None | FailureInjection::StopOrchestrator => {}
            FailureInjection::StopProcess { process, .. }
            | FailureInjection::LaunchFailure { process, .. }
                if ids.contains(process) => {}
            FailureInjection::StopProcess { process, .. }
            | FailureInjection::LaunchFailure { process, .. } => {
                return Err(format!(
                    "stop injection targets unknown process {process:?}"
                ));
            }
            FailureInjection::KillNode { logical_node_id }
                if (1..=u64::from(self.node_count)).contains(logical_node_id) => {}
            FailureInjection::KillNode { logical_node_id } => {
                return Err(format!(
                    "node-loss injection targets node {logical_node_id} outside topology 1..={}",
                    self.node_count
                ));
            }
        }
        Ok(())
    }
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
