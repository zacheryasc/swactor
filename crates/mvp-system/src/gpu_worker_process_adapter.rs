#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ArenaFd(pub i32);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HelperAbiVersion(pub u32);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DeviceHandle(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdapterConfig {
    pub arena_fd: ArenaFd,
    pub arena_bytes: u64,
    pub helper_abi_version: HelperAbiVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCommand {
    InitializeWorker {
        helper_abi_version: HelperAbiVersion,
    },
    InstallRing {
        ring_id: RingId,
    },
    ExecuteStep {
        step_id: StepId,
    },
    ReleaseDeviceObject {
        handle: DeviceHandle,
    },
    ShutdownWorker,
    RingReadable {
        ring_id: RingId,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JsonLine(serde_json::Map<String, serde_json::Value>);

impl JsonLine {
    pub fn parse(line: &str) -> Result<Self, serde_json::Error> {
        let value: serde_json::Value = serde_json::from_str(line.trim_end())?;
        match value {
            serde_json::Value::Object(map) => Ok(Self(map)),
            other => Ok(Self(serde_json::Map::from_iter([("value".into(), other)]))),
        }
    }

    pub fn is_object(&self) -> bool {
        true
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.0.contains_key(key)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProcessFaultReason {
    InvalidJson,
    UnknownEventShape,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerFatalReason {
    UnsupportedHelperAbi,
    BackendUnavailable,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AdapterEvent {
    WorkerReady {
        generation: WorkerGeneration,
    },
    WorkerFatal {
        reason: WorkerFatalReason,
    },
    ProcessFault {
        reason: ProcessFaultReason,
        line: String,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerAction {
    ReadArenaEnvironment {
        arena_fd: ArenaFd,
        arena_bytes: u64,
    },
    MapArena {
        arena_fd: ArenaFd,
        arena_bytes: u64,
    },
    InitializeRingHelper {
        helper_abi_version: HelperAbiVersion,
    },
    InitializeBackend,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InitializationFailure {
    BackendUnavailable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitStatus {
    Code(i32),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandRejectionReason {
    PayloadBytesForbidden,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommandRejection {
    pub reason: CommandRejectionReason,
}

#[cfg(test)]
pub struct ProcessAdapterHarness {
    config: AdapterConfig,
    stdin_lines: Vec<String>,
    events: Vec<AdapterEvent>,
    worker_actions: Vec<WorkerAction>,
    command_rejections: Vec<CommandRejection>,
    exit_status: Option<ExitStatus>,
}

#[cfg(test)]
impl ProcessAdapterHarness {
    pub fn new(config: AdapterConfig) -> Self {
        Self {
            config,
            stdin_lines: Vec::new(),
            events: Vec::new(),
            worker_actions: Vec::new(),
            command_rejections: Vec::new(),
            exit_status: None,
        }
    }

    pub fn start_worker_process(&mut self) {
        self.worker_actions
            .push(WorkerAction::ReadArenaEnvironment {
                arena_fd: self.config.arena_fd,
                arena_bytes: self.config.arena_bytes,
            });
    }

    pub fn send_command(&mut self, command: WorkerCommand) {
        if let WorkerCommand::InitializeWorker { helper_abi_version } = command {
            if helper_abi_version != self.config.helper_abi_version {
                self.events.push(AdapterEvent::WorkerFatal {
                    reason: WorkerFatalReason::UnsupportedHelperAbi,
                });
                return;
            }
            self.worker_actions.push(WorkerAction::MapArena {
                arena_fd: self.config.arena_fd,
                arena_bytes: self.config.arena_bytes,
            });
            self.worker_actions
                .push(WorkerAction::InitializeRingHelper { helper_abi_version });
            self.worker_actions.push(WorkerAction::InitializeBackend);
        }
        self.stdin_lines.push(format_json_command(&command));
    }

    pub fn send_raw_json_command(&mut self, raw: &str) {
        match JsonLine::parse(raw) {
            Ok(parsed)
                if parsed.contains_key("payload")
                    || parsed.contains_key("bytes")
                    || parsed.contains_key("data") =>
            {
                self.command_rejections.push(CommandRejection {
                    reason: CommandRejectionReason::PayloadBytesForbidden,
                });
            }
            Ok(_) => self.stdin_lines.push(format!("{}\n", raw.trim_end())),
            Err(_) => self.command_rejections.push(CommandRejection {
                reason: CommandRejectionReason::PayloadBytesForbidden,
            }),
        }
    }

    pub fn receive_stdout_line(&mut self, line: &str) {
        let parsed = match JsonLine::parse(line) {
            Ok(parsed) => parsed,
            Err(_) => {
                self.events.push(AdapterEvent::ProcessFault {
                    reason: ProcessFaultReason::InvalidJson,
                    line: line.into(),
                });
                return;
            }
        };
        let Some(serde_json::Value::String(kind)) = parsed.0.get("type") else {
            self.events.push(AdapterEvent::ProcessFault {
                reason: ProcessFaultReason::UnknownEventShape,
                line: line.into(),
            });
            return;
        };
        match kind.as_str() {
            "WorkerReady" => {
                let generation = parsed
                    .0
                    .get("generation")
                    .and_then(|value| value.as_u64())
                    .unwrap_or(1);
                self.events.push(AdapterEvent::WorkerReady {
                    generation: WorkerGeneration(generation),
                });
            }
            _ => self.events.push(AdapterEvent::ProcessFault {
                reason: ProcessFaultReason::UnknownEventShape,
                line: line.into(),
            }),
        }
    }

    pub fn receive_stderr_line(&mut self, _line: &str) {}

    pub fn inject_initialization_failure(&mut self, failure: InitializationFailure) {
        match failure {
            InitializationFailure::BackendUnavailable => {
                self.events.push(AdapterEvent::WorkerFatal {
                    reason: WorkerFatalReason::BackendUnavailable,
                });
                self.exit_status = Some(ExitStatus::Code(1));
            }
        }
    }

    pub fn stdin_lines(&self) -> &[String] {
        &self.stdin_lines
    }

    pub fn events(&self) -> &[AdapterEvent] {
        &self.events
    }

    pub fn worker_actions(&self) -> &[WorkerAction] {
        &self.worker_actions
    }

    pub fn exit_status(&self) -> Option<ExitStatus> {
        self.exit_status
    }

    pub fn command_rejections(&self) -> &[CommandRejection] {
        &self.command_rejections
    }
}

#[cfg(test)]
fn format_json_command(command: &WorkerCommand) -> String {
    let value = match command {
        WorkerCommand::InitializeWorker { helper_abi_version } => serde_json::json!({
            "type": "InitializeWorker",
            "helper_abi_version": helper_abi_version.0,
        }),
        WorkerCommand::InstallRing { ring_id } => serde_json::json!({
            "type": "InstallRing",
            "ring_id": ring_id.0,
        }),
        WorkerCommand::ExecuteStep { step_id } => serde_json::json!({
            "type": "ExecuteStep",
            "step_id": step_id.0,
        }),
        WorkerCommand::ReleaseDeviceObject { handle } => serde_json::json!({
            "type": "ReleaseDeviceObject",
            "handle": handle.0,
        }),
        WorkerCommand::ShutdownWorker => serde_json::json!({
            "type": "ShutdownWorker",
        }),
        WorkerCommand::RingReadable { ring_id } => serde_json::json!({
            "type": "RingReadable",
            "ring_id": ring_id.0,
        }),
    };
    format!("{}\n", value)
}
