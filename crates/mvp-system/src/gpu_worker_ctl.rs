#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProcessId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WorkerGeneration(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RingId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StepId(pub u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ObjectId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceHandle {
    pub generation: WorkerGeneration,
    pub id: u64,
}

impl DeviceHandle {
    pub fn new(generation: WorkerGeneration, id: u64) -> Self {
        Self { generation, id }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArenaEnv {
    pub arena_fd: i32,
    pub arena_bytes: u64,
}

impl ArenaEnv {
    pub fn test_default() -> Self {
        Self {
            arena_fd: 3,
            arena_bytes: 4096,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerConfig {
    pub node_id: NodeId,
    pub arena_env: ArenaEnv,
    pub initialization_timeout_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ActorCommand {
    InstallRing {
        generation: WorkerGeneration,
        ring_id: RingId,
    },
    ExecuteStep {
        generation: WorkerGeneration,
        step_id: StepId,
        input: DeviceHandle,
    },
    ReleaseDeviceObject {
        generation: WorkerGeneration,
        handle: DeviceHandle,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCommand {
    InitializeWorker {
        generation: WorkerGeneration,
    },
    InstallRing {
        ring_id: RingId,
    },
    ExecuteStep {
        step_id: StepId,
        input: DeviceHandle,
    },
    ReleaseDeviceObject {
        handle: DeviceHandle,
    },
    ShutdownWorker,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerEvent {
    RingInstalled { ring_id: RingId },
    ObjectLoaded { object_id: ObjectId, sequence: u64 },
    ObjectProduced { object_id: ObjectId, sequence: u64 },
    StepCompleted { step_id: StepId },
    RingReadable { ring_id: RingId },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExitStatus {
    Code(i32),
    Signal(i32),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCtlEvent {
    StartWorker,
    ProcessStarted { pid: ProcessId },
    WorkerReady { generation: WorkerGeneration },
    ActorCommand(ActorCommand),
    StdoutEvent(WorkerEvent),
    ProcessExited { status: ExitStatus },
    RestartRequested,
    ShutdownRequested,
    WorkerStopped { generation: WorkerGeneration },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WorkerFailure {
    InitializationTimeout,
    ProcessExited,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommandRejection {
    NotRunning,
    OldGenerationHandle,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCtlOut {
    WorkerRunning {
        generation: WorkerGeneration,
    },
    WorkerFailed {
        generation: WorkerGeneration,
        reason: WorkerFailure,
    },
    CommandRejected {
        reason: CommandRejection,
    },
    RingFaulted {
        ring_id: RingId,
    },
    WorkerStopped {
        generation: WorkerGeneration,
    },
    RingQuiesced {
        ring_id: RingId,
    },
    TerminalStopped {
        generation: WorkerGeneration,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum WorkerCtlCommand {
    SpawnProcessActor { node_id: NodeId },
    StopDriverPump { ring_id: RingId },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RoutedEvent {
    ToEdgeEstablisher(WorkerEvent),
    ToRxOrRole(WorkerEvent),
    ToTxOrRole(WorkerEvent),
    ToStageController(WorkerEvent),
    ToDriverOrWorkerSide(WorkerEvent),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CtlState {
    Idle,
    Starting,
    Running,
    Crashed,
    ShuttingDown,
    Stopped,
}

pub struct GpuWorkerCtl {
    config: WorkerConfig,
    state: CtlState,
    current_generation: WorkerGeneration,
    now_ms: u64,
    start_time_ms: Option<u64>,
    commands: Vec<WorkerCtlCommand>,
    serialized: Vec<WorkerCommand>,
    events: Vec<WorkerCtlOut>,
    routed: Vec<RoutedEvent>,
    installed_rings: std::collections::BTreeSet<RingId>,
}

impl GpuWorkerCtl {
    pub fn new(config: WorkerConfig) -> Self {
        Self {
            config,
            state: CtlState::Idle,
            current_generation: WorkerGeneration(1),
            now_ms: 0,
            start_time_ms: None,
            commands: Vec::new(),
            serialized: Vec::new(),
            events: Vec::new(),
            routed: Vec::new(),
            installed_rings: std::collections::BTreeSet::new(),
        }
    }

    pub fn observe(&mut self, event: WorkerCtlEvent) {
        match event {
            WorkerCtlEvent::StartWorker => self.start(),
            WorkerCtlEvent::ProcessStarted { .. } => {
                self.state = CtlState::Starting;
                self.serialized.push(WorkerCommand::InitializeWorker {
                    generation: self.current_generation,
                });
            }
            WorkerCtlEvent::WorkerReady { generation } => {
                self.current_generation = generation;
                self.state = CtlState::Running;
                self.events.push(WorkerCtlOut::WorkerRunning { generation });
            }
            WorkerCtlEvent::ActorCommand(command) => self.actor_command(command),
            WorkerCtlEvent::StdoutEvent(event) => self.route(event),
            WorkerCtlEvent::ProcessExited { status } => self.process_exited(status),
            WorkerCtlEvent::RestartRequested => self.restart(),
            WorkerCtlEvent::ShutdownRequested => self.shutdown(),
            WorkerCtlEvent::WorkerStopped { generation } => {
                self.events.push(WorkerCtlOut::WorkerStopped { generation });
            }
        }
    }

    pub fn advance_time_ms(&mut self, delta: u64) {
        self.now_ms = self.now_ms.saturating_add(delta);
        if self.state == CtlState::Starting {
            if let Some(start_time) = self.start_time_ms {
                if self.now_ms.saturating_sub(start_time) > self.config.initialization_timeout_ms {
                    self.events.push(WorkerCtlOut::WorkerFailed {
                        generation: self.current_generation,
                        reason: WorkerFailure::InitializationTimeout,
                    });
                    self.state = CtlState::Crashed;
                }
            }
        }
    }

    pub fn commands(&self) -> &[WorkerCtlCommand] {
        &self.commands
    }

    pub fn serialized_worker_commands(&self) -> &[WorkerCommand] {
        &self.serialized
    }

    pub fn events(&self) -> &[WorkerCtlOut] {
        &self.events
    }

    pub fn routed(&self) -> &[RoutedEvent] {
        &self.routed
    }

    pub fn current_generation(&self) -> WorkerGeneration {
        self.current_generation
    }

    fn start(&mut self) {
        self.state = CtlState::Starting;
        self.start_time_ms = Some(self.now_ms);
        self.commands.push(WorkerCtlCommand::SpawnProcessActor {
            node_id: self.config.node_id,
        });
    }

    fn actor_command(&mut self, command: ActorCommand) {
        if self.state != CtlState::Running {
            self.events.push(WorkerCtlOut::CommandRejected {
                reason: CommandRejection::NotRunning,
            });
            return;
        }
        let command_generation = match command {
            ActorCommand::InstallRing { generation, .. }
            | ActorCommand::ExecuteStep { generation, .. }
            | ActorCommand::ReleaseDeviceObject { generation, .. } => generation,
        };
        if command_generation != self.current_generation {
            self.events.push(WorkerCtlOut::CommandRejected {
                reason: CommandRejection::OldGenerationHandle,
            });
            return;
        }
        match command {
            ActorCommand::InstallRing { ring_id, .. } => {
                self.installed_rings.insert(ring_id);
                self.serialized.push(WorkerCommand::InstallRing { ring_id });
            }
            ActorCommand::ExecuteStep { step_id, input, .. } => {
                if input.generation != self.current_generation {
                    self.events.push(WorkerCtlOut::CommandRejected {
                        reason: CommandRejection::OldGenerationHandle,
                    });
                } else {
                    self.serialized
                        .push(WorkerCommand::ExecuteStep { step_id, input });
                }
            }
            ActorCommand::ReleaseDeviceObject { handle, .. } => {
                if handle.generation != self.current_generation {
                    self.events.push(WorkerCtlOut::CommandRejected {
                        reason: CommandRejection::OldGenerationHandle,
                    });
                } else {
                    self.serialized
                        .push(WorkerCommand::ReleaseDeviceObject { handle });
                }
            }
        }
    }

    fn route(&mut self, event: WorkerEvent) {
        match event.clone() {
            WorkerEvent::RingInstalled { .. } => {
                self.routed.push(RoutedEvent::ToEdgeEstablisher(event))
            }
            WorkerEvent::ObjectLoaded { .. } => self.routed.push(RoutedEvent::ToRxOrRole(event)),
            WorkerEvent::ObjectProduced { .. } => self.routed.push(RoutedEvent::ToTxOrRole(event)),
            WorkerEvent::StepCompleted { .. } => {
                self.routed.push(RoutedEvent::ToStageController(event))
            }
            WorkerEvent::RingReadable { .. } => {
                self.routed.push(RoutedEvent::ToDriverOrWorkerSide(event))
            }
        }
    }

    fn process_exited(&mut self, status: ExitStatus) {
        match (self.state, status) {
            (CtlState::ShuttingDown, ExitStatus::Code(0)) => {
                for ring_id in &self.installed_rings {
                    self.events
                        .push(WorkerCtlOut::RingQuiesced { ring_id: *ring_id });
                }
                self.events.push(WorkerCtlOut::TerminalStopped {
                    generation: self.current_generation,
                });
                self.state = CtlState::Stopped;
            }
            (_, _) => {
                for ring_id in &self.installed_rings {
                    self.events
                        .push(WorkerCtlOut::RingFaulted { ring_id: *ring_id });
                    self.commands
                        .push(WorkerCtlCommand::StopDriverPump { ring_id: *ring_id });
                }
                self.events.push(WorkerCtlOut::WorkerFailed {
                    generation: self.current_generation,
                    reason: WorkerFailure::ProcessExited,
                });
                self.state = CtlState::Crashed;
            }
        }
    }

    fn restart(&mut self) {
        self.state = CtlState::Starting;
        self.current_generation = WorkerGeneration(self.current_generation.0 + 1);
        self.start_time_ms = Some(self.now_ms);
    }

    fn shutdown(&mut self) {
        if self.state == CtlState::Running {
            self.serialized.push(WorkerCommand::ShutdownWorker);
            self.state = CtlState::ShuttingDown;
        }
    }
}

pub type GpuWorkerCtlHarness = GpuWorkerCtl;
