use std::collections::{HashMap, VecDeque};

use iroh::EndpointAddr;
use myelin_control_contract::{
    ContextualControlReply, ContextualEventCursor, ContextualEventRecord,
    ContextualEventsAcknowledgement, ContextualEventsBatch, ContextualExecutionView,
    ContextualProcessEvent, ContextualProcessEventKind, ContextualProcessSpec, ControlRevision,
    MAX_CONTEXTUAL_EVENT_CURSORS,
};
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;
use swactor_transport::{CodecRegistry, NetworkMessage};

use crate::contextual_process::ContextualNodeCommand;
use crate::node_actor::NodeAgentMsg;
use crate::orchestration::manual_control::{ManualActorControl, ManualControlMsg};
use crate::run_fsm as core;

use swactor_transport::JsonCodec;
#[derive(Clone)]
pub(crate) struct ContextualArtifactCleanup {
    directory: ActorAddress,
}

impl ContextualArtifactCleanup {
    pub(crate) fn new(directory: ActorAddress) -> Self {
        Self { directory }
    }

    fn cleanup(&self, ctx: &Ctx<'_>, namespace_path: String) {
        let Ok(namespace_path) = data_plane::path::DataPath::parse(namespace_path) else {
            return;
        };
        let random = ActorAddress::new_random();
        let operation_id = data_plane::namespace::OperationId::from_u128(u128::from_le_bytes(
            random.0[..16]
                .try_into()
                .expect("actor address contains sixteen operation ID bytes"),
        ));
        let _ = ctx.spawn(ContextualArtifactCleanupOperation {
            directory: self.directory,
            namespace_path,
            operation_id,
        });
    }
}

struct ContextualArtifactCleanupOperation {
    directory: ActorAddress,
    namespace_path: data_plane::path::DataPath,
    operation_id: data_plane::namespace::OperationId,
}

impl ActorInterface for ContextualArtifactCleanupOperation {
    type Incoming = data_plane::namespace::DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let request_id = data_plane::namespace::DirectoryRequestId(1);
        if ctx
            .send(
                self.directory,
                data_plane::namespace::DataDirectoryIn::Unregister {
                    request_id,
                    path: self.namespace_path.clone(),
                    operation_id: self.operation_id,
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, _reply: Self::Incoming) {
        ctx.stop_self();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StageRefWire {
    pub stage_index: u32,
    pub node_id: u64,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum OrchestratorMsg {
    ObservePoolReady {
        nodes: Vec<u64>,
    },
    ObservePlanAvailable {
        run_id: u64,
        stages: Vec<StageRefWire>,
    },
    ObserveStageReady {
        run_id: u64,
        stage_index: u32,
    },
    ObserveNodeRuntimeReady {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        endpoint: EndpointAddr,
        node_actor: ActorAddress,
        readiness_id: u64,
        #[serde(default)]
        artifact_digest: Option<String>,
        #[serde(default)]
        deployment_generation: Option<String>,
    },
    ObserveNodeRuntimeReadyAck {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        readiness_id: u64,
    },
    ObserveWeightsReady {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
    },
    ObserveTokenInEndpointReady,
    ObserveTokenOutEndpointReady,
    ObserveTokenReceived {
        sequence: u64,
        token_id: u32,
        eos: bool,
    },
    ObserveStageFault {
        run_id: u64,
        stage_index: u32,
        reason: Option<String>,
    },
    ObserveEndpointFault {
        run_id: u64,
        endpoint: EndpointKindWire,
    },
    ObserveStageStopped {
        run_id: u64,
        stage_index: u32,
    },
    ObserveTokenEndpointsStopped,
    ObserveOperatorStop {
        run_id: u64,
    },
    ObserveMembershipLost {
        run_id: u64,
        node_id: u64,
    },
    Snapshot {
        reply_to: ActorAddress,
    },
    Manual(ManualControlMsg),
    ContextualSpawn {
        logical_node_id: u64,
        request_id: String,
        spec: ContextualProcessSpec,
        reply_to: ActorAddress,
    },
    ContextualStop {
        request_id: String,
        control_request_id: String,
        #[serde(default)]
        force: bool,
        reply_to: ActorAddress,
    },
    ContextualQuery {
        logical_node_id: u64,
        control_request_id: String,
        reply_to: ActorAddress,
    },
    ContextualEvents {
        request_id: String,
        after_sequence: u64,
        reply_to: ActorAddress,
    },
    ContextualEventsBatch {
        cursors: Vec<ContextualEventCursor>,
        wait_key: Option<String>,
        reply_to: ActorAddress,
    },
    ContextualEventsAck {
        request_id: String,
        execution_incarnation: String,
        through_sequence: u64,
        reply_to: ActorAddress,
    },
    ControlChanges {
        generation: Option<String>,
        after_revision: Option<u64>,
        wait_key: Option<String>,
        reply_to: ActorAddress,
    },
    ContextualEvent {
        observation: ContextualProcessEvent,
        delivery_owner: Option<ActorAddress>,
        sequence: u64,
    },
    /// The request owner disconnected; release only its exact subscription.
    ContextualControlCancel {
        control_request_id: String,
        reply_to: ActorAddress,
    },
}

impl NetworkMessage for OrchestratorMsg {
    fn type_tag() -> &'static str {
        "myelin::OrchestratorMsg"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ContextualReplyObserverMsg {
    Reply(ContextualControlReply),
}

fn send_contextual_reply(ctx: &Ctx<'_>, reply_to: ActorAddress, reply: ContextualControlReply) {
    let _ = ctx.send(reply_to, ContextualReplyObserverMsg::Reply(reply));
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum EndpointKindWire {
    TokenIn,
    TokenOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct SamplingDataWire {
    pub source_sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum TokenObjectPayloadWire {
    Prompt {
        tokens: Vec<u32>,
    },
    Decode {
        token_id: u32,
        sampling: SamplingDataWire,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum RunCommandWire {
    ProvisionStage {
        run_id: u64,
        stage_index: u32,
        node_id: u64,
    },
    CreateTokenInEndpoint {
        run_id: u64,
    },
    CreateTokenOutEndpoint {
        run_id: u64,
    },
    InjectTokenObject {
        run_id: u64,
        sequence: u64,
        payload: TokenObjectPayloadWire,
    },
    StopRun {
        run_id: u64,
        stage_index: u32,
    },
    TearDownTokenEndpoints {
        run_id: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum LifecycleEventWire {
    Faulted { run_id: u64 },
    Completed { run_id: u64 },
    OperatorStopped { run_id: u64 },
    TornDown { run_id: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum OrchestratorReport {
    Command(RunCommandWire),
    Lifecycle(LifecycleEventWire),
    NodeRuntimeReady {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        endpoint: EndpointAddr,
        node_actor: ActorAddress,
        readiness_id: u64,
        #[serde(default)]
        artifact_digest: Option<String>,
        #[serde(default)]
        deployment_generation: Option<String>,
    },
    NodeRuntimeReadyAck {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        readiness_id: u64,
    },
    WeightsReady {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
    },
    StageReady {
        run_id: u64,
        stage_index: u32,
    },
    StageFault {
        run_id: u64,
        stage_index: u32,
        reason: Option<String>,
    },
    Snapshot {
        commands: Vec<RunCommandWire>,
        events: Vec<LifecycleEventWire>,
        injected_sequences: Vec<u64>,
    },
}

impl NetworkMessage for OrchestratorReport {
    fn type_tag() -> &'static str {
        "myelin::OrchestratorReport"
    }
}
const MAX_CONTEXTUAL_EVENTS_PER_EXECUTION: usize = 4096;

struct ContextualExecutionState {
    logical_node_id: u64,
    execution_incarnation: String,
    process: Option<String>,
    terminal: bool,
    next_sequence: u64,
    events: VecDeque<ContextualEventRecord>,
    spawn_waiter: Option<ActorAddress>,
    staged_source: Option<String>,
    last_delivery: Option<(ActorAddress, u64)>,
}

impl ContextualExecutionState {
    fn record(&mut self, observation: ContextualProcessEvent) {
        if let ContextualProcessEventKind::Spawned { process, .. } = &observation.event {
            self.process = Some(process.clone());
        }
        self.terminal |= observation.event.is_terminal();
        self.events.push_back(ContextualEventRecord {
            sequence: self.next_sequence,
            observation,
        });
        self.next_sequence = self.next_sequence.saturating_add(1);
        if self.events.len() > MAX_CONTEXTUAL_EVENTS_PER_EXECUTION {
            self.events.pop_front();
        }
    }

    fn view(&self, request_id: String, after_sequence: u64) -> ContextualExecutionView {
        ContextualExecutionView {
            request_id,
            execution_incarnation: self.execution_incarnation.clone(),
            logical_node_id: self.logical_node_id,
            process: self.process.clone(),
            terminal: self.terminal,
            truncated_before: self
                .events
                .front()
                .map_or(self.next_sequence, |event| event.sequence),
            next_sequence: self.next_sequence,
            events: self
                .events
                .iter()
                .filter(|event| event.sequence >= after_sequence)
                .cloned()
                .collect(),
        }
    }
}

struct PendingContextualReply {
    reply_to: Option<ActorAddress>,
    target_request_id: Option<String>,
    logical_node_id: u64,
}

pub(crate) struct OrchestratorActor {
    core: core::OrchestratorRun,
    report_to: Option<ActorAddress>,
    command_cursor: usize,
    event_cursor: usize,
    manual: Option<ManualActorControl>,
    contextual_executions: HashMap<String, ContextualExecutionState>,
    contextual_acknowledgements: VecDeque<ContextualEventsAcknowledgement>,
    pending_contextual_replies: HashMap<String, PendingContextualReply>,
    contextual_event_waiters: HashMap<String, (ActorAddress, Vec<ContextualEventCursor>)>,
    control_revision: u64,
    control_change_waiters: HashMap<String, ActorAddress>,
    contextual_artifact_cleanup: Option<ContextualArtifactCleanup>,
}

impl OrchestratorActor {
    pub(crate) fn new(config: core::RunConfig, report_to: Option<ActorAddress>) -> Self {
        Self {
            core: core::OrchestratorRun::new(config),
            report_to,
            command_cursor: 0,
            event_cursor: 0,
            manual: None,
            contextual_executions: HashMap::new(),
            contextual_acknowledgements: VecDeque::new(),
            pending_contextual_replies: HashMap::new(),
            contextual_event_waiters: HashMap::new(),
            control_revision: 0,
            control_change_waiters: HashMap::new(),
            contextual_artifact_cleanup: None,
        }
    }

    pub(crate) fn with_manual_control(mut self, manual: ManualActorControl) -> Self {
        self.manual = Some(manual);
        self
    }
    pub(crate) fn with_contextual_artifact_cleanup(
        mut self,
        cleanup: ContextualArtifactCleanup,
    ) -> Self {
        self.contextual_artifact_cleanup = Some(cleanup);
        self
    }

    fn cleanup_contextual_artifact(
        &self,
        ctx: &Ctx<'_>,
        _request_id: &str,
        source: Option<String>,
    ) {
        if let (Some(cleanup), Some(source)) = (&self.contextual_artifact_cleanup, source) {
            cleanup.cleanup(ctx, source);
        }
    }

    fn contextual_rejection(
        &self,
        ctx: &Ctx<'_>,
        reply_to: ActorAddress,
        error: impl Into<String>,
    ) {
        send_contextual_reply(
            ctx,
            reply_to,
            ContextualControlReply::Rejected {
                error: error.into(),
            },
        );
    }

    fn contextual_node_actor(&self, logical_node_id: u64) -> Result<ActorAddress, String> {
        self.manual
            .as_ref()
            .ok_or_else(|| "manual node control is unavailable".to_owned())?
            .running_node_actor(logical_node_id)
    }

    fn route_contextual(
        &self,
        ctx: &Ctx<'_>,
        logical_node_id: u64,
        command: ContextualNodeCommand,
    ) -> Result<(), String> {
        let node_actor = self.contextual_node_actor(logical_node_id)?;
        ctx.send(node_actor, NodeAgentMsg::Contextual(command))
            .map_err(|error| format!("route contextual command to node {logical_node_id}: {error}"))
    }

    fn contextual_event(
        &mut self,
        ctx: &Ctx<'_>,
        observation: &ContextualProcessEvent,
        delivery_owner: Option<ActorAddress>,
        sequence: u64,
    ) {
        self.record_contextual_delivery(ctx, observation, delivery_owner, sequence);
        if let Some(delivery_owner) = delivery_owner {
            let _ = self.route_contextual(
                ctx,
                observation.logical_node_id,
                ContextualNodeCommand::AcknowledgeEvent {
                    delivery_owner,
                    reply_to: ctx.self_addr(),
                    sequence,
                },
            );
        }
    }

    fn record_contextual_delivery(
        &mut self,
        ctx: &Ctx<'_>,
        observation: &ContextualProcessEvent,
        delivery_owner: Option<ActorAddress>,
        sequence: u64,
    ) {
        let pending = self.pending_contextual_replies.get(&observation.request_id);
        if pending.is_some_and(|pending| pending.logical_node_id != observation.logical_node_id) {
            return;
        }
        let target = pending
            .and_then(|pending| pending.target_request_id.as_deref())
            .unwrap_or(&observation.request_id);
        if let Some(execution) = self.contextual_executions.get_mut(target) {
            if execution.logical_node_id != observation.logical_node_id {
                return;
            }
            if let Some(owner) = delivery_owner {
                if let Some((previous_owner, previous_sequence)) = execution.last_delivery
                    && (previous_owner != owner || sequence <= previous_sequence)
                {
                    return;
                }
                execution.last_delivery = Some((owner, sequence));
            }
        }
        if let Some(pending) = self
            .pending_contextual_replies
            .remove(&observation.request_id)
        {
            if let Some(target) = pending.target_request_id {
                let staged_source =
                    self.contextual_executions
                        .get_mut(&target)
                        .and_then(|execution| {
                            let mut target_observation = observation.clone();
                            target_observation.request_id = target.clone();
                            execution.record(target_observation);
                            observation
                                .event
                                .is_terminal()
                                .then(|| execution.staged_source.take())
                                .flatten()
                        });
                self.cleanup_contextual_artifact(ctx, &target, staged_source);
            }
            if let Some(reply_to) = pending.reply_to {
                send_contextual_reply(
                    ctx,
                    reply_to,
                    ContextualControlReply::Event {
                        observation: observation.clone(),
                    },
                );
            }
            return;
        }
        let Some(execution) = self.contextual_executions.get_mut(&observation.request_id) else {
            return;
        };
        if execution.terminal {
            return;
        }
        let resolves_spawn = matches!(
            observation.event,
            ContextualProcessEventKind::Spawned { .. }
        ) || observation.event.is_terminal();
        execution.record(observation.clone());
        let staged_source = observation
            .event
            .is_terminal()
            .then(|| execution.staged_source.take())
            .flatten();
        if resolves_spawn && let Some(waiter) = execution.spawn_waiter.take() {
            send_contextual_reply(
                ctx,
                waiter,
                ContextualControlReply::Event {
                    observation: observation.clone(),
                },
            );
        }
        self.cleanup_contextual_artifact(ctx, &observation.request_id, staged_source);
    }

    fn handle_contextual(&mut self, ctx: &Ctx<'_>, msg: &OrchestratorMsg) -> bool {
        match msg {
            OrchestratorMsg::ControlChanges {
                generation,
                after_revision,
                wait_key,
                reply_to,
            } => {
                let current_generation = ctx.self_addr().to_string();
                if generation.as_deref() == Some(current_generation.as_str())
                    && *after_revision == Some(self.control_revision)
                    && let Some(key) = wait_key
                {
                    if self.control_change_waiters.len() >= MAX_CONTEXTUAL_EVENT_CURSORS {
                        self.contextual_rejection(ctx, *reply_to, "too many control observers");
                    } else {
                        self.control_change_waiters.insert(key.clone(), *reply_to);
                    }
                } else {
                    send_contextual_reply(
                        ctx,
                        *reply_to,
                        ContextualControlReply::ControlRevision(ControlRevision {
                            generation: current_generation,
                            revision: self.control_revision,
                        }),
                    );
                }
                true
            }
            OrchestratorMsg::ContextualSpawn {
                logical_node_id,
                request_id,
                spec,
                reply_to,
            } => {
                if request_id.trim().is_empty() {
                    self.contextual_rejection(ctx, *reply_to, "request_id must not be empty");
                    return true;
                }
                if self.contextual_executions.contains_key(request_id) {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        format!("contextual request {request_id:?} already exists"),
                    );
                    return true;
                }
                if self.contextual_executions.len() >= MAX_CONTEXTUAL_EVENT_CURSORS {
                    self.cleanup_contextual_artifact(
                        ctx,
                        request_id,
                        spec.staged_program
                            .as_ref()
                            .map(|program| program.namespace_path.clone()),
                    );
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        "too many unacknowledged contextual executions",
                    );
                    return true;
                }
                self.contextual_executions.insert(
                    request_id.clone(),
                    ContextualExecutionState {
                        logical_node_id: *logical_node_id,
                        execution_incarnation: ActorAddress::new_random().to_string(),
                        process: None,
                        terminal: false,
                        next_sequence: 0,
                        events: VecDeque::new(),
                        spawn_waiter: Some(*reply_to),
                        staged_source: spec
                            .staged_program
                            .as_ref()
                            .map(|program| program.namespace_path.clone()),
                        last_delivery: None,
                    },
                );
                if let Err(error) = self.route_contextual(
                    ctx,
                    *logical_node_id,
                    ContextualNodeCommand::Spawn {
                        request_id: request_id.clone(),
                        spec: spec.clone(),
                        reply_to: ctx.self_addr(),
                    },
                ) {
                    let staged_source = self
                        .contextual_executions
                        .remove(request_id)
                        .and_then(|execution| execution.staged_source);
                    self.cleanup_contextual_artifact(ctx, request_id, staged_source);
                    self.contextual_rejection(ctx, *reply_to, error);
                }
                true
            }
            OrchestratorMsg::ContextualStop {
                request_id,
                control_request_id,
                force,
                reply_to,
            } => {
                if self
                    .pending_contextual_replies
                    .contains_key(control_request_id)
                {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        format!("contextual control request {control_request_id:?} already exists"),
                    );
                    return true;
                }
                let Some(execution) = self.contextual_executions.get(request_id) else {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        format!("contextual request {request_id:?} does not exist"),
                    );
                    return true;
                };
                if execution.process.is_none() {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        format!("contextual request {request_id:?} has not spawned"),
                    );
                    return true;
                }
                let logical_node_id = execution.logical_node_id;
                self.pending_contextual_replies.insert(
                    control_request_id.clone(),
                    PendingContextualReply {
                        reply_to: Some(*reply_to),
                        target_request_id: Some(request_id.clone()),
                        logical_node_id,
                    },
                );
                if let Err(error) = self.route_contextual(
                    ctx,
                    logical_node_id,
                    ContextualNodeCommand::Stop {
                        request_id: control_request_id.clone(),
                        target_request_id: request_id.clone(),
                        force: *force,
                        reply_to: ctx.self_addr(),
                    },
                ) {
                    self.pending_contextual_replies.remove(control_request_id);
                    self.contextual_rejection(ctx, *reply_to, error);
                }
                true
            }
            OrchestratorMsg::ContextualQuery {
                logical_node_id,
                control_request_id,
                reply_to,
            } => {
                if self
                    .pending_contextual_replies
                    .contains_key(control_request_id)
                {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        format!("contextual control request {control_request_id:?} already exists"),
                    );
                    return true;
                }
                self.pending_contextual_replies.insert(
                    control_request_id.clone(),
                    PendingContextualReply {
                        reply_to: Some(*reply_to),
                        target_request_id: None,
                        logical_node_id: *logical_node_id,
                    },
                );
                if let Err(error) = self.route_contextual(
                    ctx,
                    *logical_node_id,
                    ContextualNodeCommand::Query {
                        request_id: control_request_id.clone(),
                        reply_to: ctx.self_addr(),
                    },
                ) {
                    self.pending_contextual_replies.remove(control_request_id);
                    self.contextual_rejection(ctx, *reply_to, error);
                }
                true
            }
            OrchestratorMsg::ContextualEvents {
                request_id,
                after_sequence,
                reply_to,
            } => {
                let Some(execution) = self.contextual_executions.get(request_id) else {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        format!("contextual request {request_id:?} does not exist"),
                    );
                    return true;
                };
                let reply = ContextualControlReply::Events {
                    execution: execution.view(request_id.clone(), *after_sequence),
                };
                send_contextual_reply(ctx, *reply_to, reply);
                true
            }
            OrchestratorMsg::ContextualEventsBatch {
                cursors,
                wait_key,
                reply_to,
            } => {
                if cursors.len() > MAX_CONTEXTUAL_EVENT_CURSORS {
                    self.contextual_rejection(ctx, *reply_to, "too many execution cursors");
                    return true;
                }
                if cursors
                    .iter()
                    .map(|cursor| &cursor.request_id)
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != cursors.len()
                {
                    self.contextual_rejection(ctx, *reply_to, "duplicate execution cursor");
                    return true;
                }
                for cursor in cursors {
                    if cursor.after_sequence != 0 && cursor.execution_incarnation.is_none() {
                        self.contextual_rejection(
                            ctx,
                            *reply_to,
                            "nonzero cursor requires execution incarnation",
                        );
                        return true;
                    }
                    if let Some(execution) = self.contextual_executions.get(&cursor.request_id)
                        && cursor
                            .execution_incarnation
                            .as_ref()
                            .is_some_and(|incarnation| {
                                incarnation != &execution.execution_incarnation
                            })
                    {
                        self.contextual_rejection(
                            ctx,
                            *reply_to,
                            "execution cursor incarnation changed",
                        );
                        return true;
                    }
                }
                if let Some(key) = wait_key
                    && !self.contextual_cursors_ready(cursors)
                {
                    if self.contextual_event_waiters.len() >= MAX_CONTEXTUAL_EVENT_CURSORS {
                        self.contextual_rejection(ctx, *reply_to, "too many execution observers");
                    } else {
                        self.contextual_event_waiters
                            .insert(key.clone(), (*reply_to, cursors.clone()));
                    }
                    return true;
                }
                let mut executions = Vec::with_capacity(cursors.len());
                let mut missing = Vec::new();
                for cursor in cursors {
                    match self.contextual_executions.get(&cursor.request_id) {
                        Some(execution) => executions
                            .push(execution.view(cursor.request_id.clone(), cursor.after_sequence)),
                        None => missing.push(cursor.request_id.clone()),
                    }
                }
                send_contextual_reply(
                    ctx,
                    *reply_to,
                    ContextualControlReply::EventsBatch(ContextualEventsBatch {
                        executions,
                        missing,
                    }),
                );
                true
            }
            OrchestratorMsg::ContextualEventsAck {
                request_id,
                execution_incarnation,
                through_sequence,
                reply_to,
            } => {
                if self.pending_contextual_replies.values().any(|pending| {
                    pending.target_request_id.as_deref() == Some(request_id.as_str())
                }) {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        "acknowledgement must wait for outstanding stop outcomes",
                    );
                    return true;
                }
                if let Some(execution) = self.contextual_executions.get(request_id) {
                    if !execution.terminal
                        || execution.next_sequence != *through_sequence
                        || execution.execution_incarnation != *execution_incarnation
                    {
                        self.contextual_rejection(
                            ctx,
                            *reply_to,
                            "acknowledgement must cover the complete terminal execution",
                        );
                        return true;
                    }
                    self.contextual_executions.remove(request_id);
                    if self.contextual_acknowledgements.len() == MAX_CONTEXTUAL_EVENT_CURSORS {
                        self.contextual_acknowledgements.pop_front();
                    }
                    self.contextual_acknowledgements
                        .push_back(ContextualEventsAcknowledgement {
                            request_id: request_id.clone(),
                            execution_incarnation: execution_incarnation.clone(),
                            through_sequence: *through_sequence,
                        });
                } else if !self.contextual_acknowledgements.iter().any(|receipt| {
                    receipt.request_id == *request_id
                        && receipt.execution_incarnation == *execution_incarnation
                        && receipt.through_sequence == *through_sequence
                }) {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        "terminal acknowledgement has no retained incarnation receipt",
                    );
                    return true;
                }
                // Only the exact retained receipt reconciles a lost ACK reply.
                send_contextual_reply(
                    ctx,
                    *reply_to,
                    ContextualControlReply::Acknowledged(ContextualEventsAcknowledgement {
                        request_id: request_id.clone(),
                        execution_incarnation: execution_incarnation.clone(),
                        through_sequence: *through_sequence,
                    }),
                );
                true
            }
            OrchestratorMsg::ContextualEvent {
                observation,
                delivery_owner,
                sequence,
            } => {
                self.contextual_event(ctx, observation, *delivery_owner, *sequence);
                true
            }
            OrchestratorMsg::ContextualControlCancel {
                control_request_id,
                reply_to,
            } => {
                if let Some(pending) = self.pending_contextual_replies.get_mut(control_request_id)
                    && pending.reply_to == Some(*reply_to)
                {
                    if pending.target_request_id.is_some() {
                        // The accepted stop still owes an outcome to the execution log.
                        pending.reply_to = None;
                    } else {
                        let node = pending.logical_node_id;
                        self.pending_contextual_replies.remove(control_request_id);
                        let _ = self.route_contextual(
                            ctx,
                            node,
                            ContextualNodeCommand::Cancel {
                                request_id: control_request_id.clone(),
                                reply_to: ctx.self_addr(),
                            },
                        );
                    }
                }
                if self
                    .contextual_event_waiters
                    .get(control_request_id)
                    .is_some_and(|(owner, _)| owner == reply_to)
                {
                    self.contextual_event_waiters.remove(control_request_id);
                }
                if self.control_change_waiters.get(control_request_id) == Some(reply_to) {
                    self.control_change_waiters.remove(control_request_id);
                }
                if let Some(execution) = self.contextual_executions.get_mut(control_request_id)
                    && execution.spawn_waiter == Some(*reply_to)
                {
                    execution.spawn_waiter = None;
                }
                true
            }
            _ => false,
        }
    }
    fn contextual_cursors_ready(&self, cursors: &[ContextualEventCursor]) -> bool {
        cursors.is_empty()
            || cursors.iter().any(|cursor| {
                self.contextual_executions
                    .get(&cursor.request_id)
                    .is_none_or(|execution| {
                        execution.terminal || execution.next_sequence != cursor.after_sequence
                    })
            })
    }

    fn wake_contextual_event_waiters(&mut self, ctx: &Ctx<'_>) {
        if self.contextual_event_waiters.is_empty() {
            return;
        }
        // Check and subscribe are serialized by this actor; no event can fall
        // between the predicate check and registration.
        let ready = self
            .contextual_event_waiters
            .iter()
            .filter(|(_, (_, cursors))| self.contextual_cursors_ready(cursors))
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        for key in ready {
            if let Some((reply_to, cursors)) = self.contextual_event_waiters.remove(&key) {
                self.handle_contextual(
                    ctx,
                    &OrchestratorMsg::ContextualEventsBatch {
                        cursors,
                        wait_key: None,
                        reply_to,
                    },
                );
            }
        }
    }

    fn publish_control_change(&mut self, ctx: &Ctx<'_>) {
        self.control_revision = self
            .control_revision
            .checked_add(1)
            .expect("control revision exhausted");
        for (_, reply_to) in self.control_change_waiters.drain() {
            send_contextual_reply(
                ctx,
                reply_to,
                ContextualControlReply::ControlRevision(ControlRevision {
                    generation: ctx.self_addr().to_string(),
                    revision: self.control_revision,
                }),
            );
        }
    }

    fn fail_contextual_node(&mut self, ctx: &Ctx<'_>, logical_node_id: u64, reason: &str) {
        let mut waiters = Vec::new();
        let mut artifacts = Vec::new();
        for (request_id, execution) in &mut self.contextual_executions {
            if execution.logical_node_id != logical_node_id || execution.terminal {
                continue;
            }
            let observation = ContextualProcessEvent {
                request_id: request_id.clone(),
                logical_node_id,
                event: ContextualProcessEventKind::ProcessError {
                    error: reason.to_owned(),
                },
            };
            execution.record(observation.clone());
            if let Some(source) = execution.staged_source.take() {
                artifacts.push((request_id.clone(), source));
            }
            if let Some(waiter) = execution.spawn_waiter.take() {
                waiters.push((waiter, observation));
            }
        }
        for (request_id, source) in artifacts {
            self.cleanup_contextual_artifact(ctx, &request_id, Some(source));
        }
        for (waiter, observation) in waiters {
            send_contextual_reply(ctx, waiter, ContextualControlReply::Event { observation });
        }

        // Stop and query controls parked against the failed node will never
        // receive a reply; reject them now so HTTP callers fail boundedly and
        // the pending map cannot grow across node losses.
        let mut timed_out_callers = Vec::new();
        self.pending_contextual_replies.retain(|_, pending| {
            if pending.logical_node_id == logical_node_id {
                timed_out_callers.extend(pending.reply_to);
                false
            } else {
                true
            }
        });
        for reply_to in timed_out_callers {
            self.contextual_rejection(ctx, reply_to, reason.to_owned());
        }
    }

    fn observe(&mut self, msg: OrchestratorMsg) {
        match msg {
            OrchestratorMsg::ObservePoolReady { nodes } => {
                self.core.observe(core::RunEvent::PoolReady {
                    nodes: nodes.into_iter().map(core::NodeId).collect(),
                });
            }
            OrchestratorMsg::ObservePlanAvailable { run_id, stages } => {
                self.core
                    .observe(core::RunEvent::PlanAvailable(core::RunPlan {
                        run_id: core::RunId(run_id),
                        stages: stages
                            .into_iter()
                            .map(|stage| core::StageRef {
                                stage_index: stage.stage_index,
                                node_id: core::NodeId(stage.node_id),
                            })
                            .collect(),
                    }));
            }
            OrchestratorMsg::ObserveStageReady {
                run_id,
                stage_index,
            } => self.core.observe(core::RunEvent::StageReady {
                run_id: core::RunId(run_id),
                stage_index,
            }),
            OrchestratorMsg::ObserveNodeRuntimeReady { .. }
            | OrchestratorMsg::ObserveNodeRuntimeReadyAck { .. }
            | OrchestratorMsg::ObserveWeightsReady { .. }
            | OrchestratorMsg::Snapshot { .. }
            | OrchestratorMsg::Manual(_)
            | OrchestratorMsg::ContextualSpawn { .. }
            | OrchestratorMsg::ContextualStop { .. }
            | OrchestratorMsg::ContextualQuery { .. }
            | OrchestratorMsg::ContextualEvents { .. }
            | OrchestratorMsg::ContextualEventsBatch { .. }
            | OrchestratorMsg::ContextualEventsAck { .. }
            | OrchestratorMsg::ControlChanges { .. }
            | OrchestratorMsg::ContextualEvent { .. }
            | OrchestratorMsg::ContextualControlCancel { .. } => {}
            OrchestratorMsg::ObserveTokenInEndpointReady => {
                self.core.observe(core::RunEvent::TokenInEndpointReady)
            }
            OrchestratorMsg::ObserveTokenOutEndpointReady => {
                self.core.observe(core::RunEvent::TokenOutEndpointReady)
            }
            OrchestratorMsg::ObserveTokenReceived {
                sequence,
                token_id,
                eos,
            } => self.core.observe(core::RunEvent::TokenReceived {
                sequence,
                token_id,
                eos,
            }),
            OrchestratorMsg::ObserveStageFault {
                run_id,
                stage_index,
                reason: _,
            } => self.core.observe(core::RunEvent::StageFault {
                run_id: core::RunId(run_id),
                stage_index,
                reason: core::StageFaultReason::WorkerCrashed,
            }),
            OrchestratorMsg::ObserveEndpointFault { run_id, endpoint } => {
                self.core.observe(core::RunEvent::EndpointFault {
                    run_id: core::RunId(run_id),
                    endpoint: match endpoint {
                        EndpointKindWire::TokenIn => core::EndpointKind::TokenIn,
                        EndpointKindWire::TokenOut => core::EndpointKind::TokenOut,
                    },
                });
            }
            OrchestratorMsg::ObserveStageStopped {
                run_id,
                stage_index,
            } => self.core.observe(core::RunEvent::StageStopped {
                run_id: core::RunId(run_id),
                stage_index,
            }),
            OrchestratorMsg::ObserveTokenEndpointsStopped => {
                self.core.observe(core::RunEvent::TokenEndpointsStopped)
            }
            OrchestratorMsg::ObserveOperatorStop { run_id } => {
                self.core.observe(core::RunEvent::OperatorStop {
                    run_id: core::RunId(run_id),
                });
            }
            OrchestratorMsg::ObserveMembershipLost { run_id, node_id } => {
                self.core.observe(core::RunEvent::MembershipLost {
                    run_id: core::RunId(run_id),
                    node_id: core::NodeId(node_id),
                });
            }
        }
    }

    fn drain_outputs(&mut self, ctx: &Ctx) {
        let Some(report_to) = self.report_to else {
            self.command_cursor = self.core.commands().len();
            self.event_cursor = self.core.events().len();
            return;
        };
        for command in &self.core.commands()[self.command_cursor..] {
            let _ = ctx.send(report_to, OrchestratorReport::Command(command.into()));
        }
        self.command_cursor = self.core.commands().len();

        for event in &self.core.events()[self.event_cursor..] {
            let _ = ctx.send(report_to, OrchestratorReport::Lifecycle(event.into()));
        }
        self.event_cursor = self.core.events().len();
    }
}

impl ActorInterface for OrchestratorActor {
    type Incoming = OrchestratorMsg;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        if let Some(manual) = self.manual.as_mut() {
            manual.start(ctx.self_addr());
        }
    }
    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        if self.handle_contextual(ctx, &msg) {
            if matches!(
                msg,
                OrchestratorMsg::ContextualSpawn { .. }
                    | OrchestratorMsg::ContextualStop { .. }
                    | OrchestratorMsg::ContextualEventsAck { .. }
            ) || matches!(&msg, OrchestratorMsg::ContextualEvent { observation, .. }
                    if !matches!(observation.event, ContextualProcessEventKind::LiveExecutions { .. }
                        | ContextualProcessEventKind::ControlUnavailable { .. }))
            {
                self.publish_control_change(ctx);
            }
            self.wake_contextual_event_waiters(ctx);
            return;
        }
        if let OrchestratorMsg::Manual(manual_msg) = msg.clone() {
            let changes_state = !matches!(
                &manual_msg,
                ManualControlMsg::Query { .. }
                    | ManualControlMsg::QueryFleet { .. }
                    | ManualControlMsg::SearchOffers { .. }
                    | ManualControlMsg::Flush { .. }
                    | ManualControlMsg::Cancel { .. }
            );
            if let ManualControlMsg::Kill { request, .. } = &manual_msg {
                self.fail_contextual_node(
                    ctx,
                    request.logical_node_id,
                    "contextual process node termination was requested",
                );
                self.wake_contextual_event_waiters(ctx);
            }
            if let Some(manual) = self.manual.as_mut() {
                manual.handle(ctx, manual_msg);
            }
            if changes_state {
                self.publish_control_change(ctx);
            }
            return;
        }
        if !matches!(&msg, OrchestratorMsg::Snapshot { .. }) {
            self.publish_control_change(ctx);
        }
        if let OrchestratorMsg::ObserveMembershipLost { node_id, .. } = &msg {
            self.fail_contextual_node(
                ctx,
                *node_id,
                "contextual process node was lost from membership",
            );
            self.wake_contextual_event_waiters(ctx);
        }
        match msg.clone() {
            OrchestratorMsg::ObserveNodeRuntimeReady {
                run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
                readiness_id,
                artifact_digest,
                deployment_generation,
            } => {
                if let Some(report_to) = self.report_to {
                    let _ = ctx.send(
                        report_to,
                        OrchestratorReport::NodeRuntimeReady {
                            run_id,
                            node_id,
                            stage_index,
                            endpoint,
                            node_actor,
                            readiness_id,
                            artifact_digest: artifact_digest.clone(),
                            deployment_generation: deployment_generation.clone(),
                        },
                    );
                }
                return;
            }
            OrchestratorMsg::ObserveNodeRuntimeReadyAck {
                run_id,
                node_id,
                stage_index,
                readiness_id,
            } => {
                if let Some(manual) = self.manual.as_mut() {
                    manual.observe_node_ack(
                        ctx.self_addr(),
                        run_id,
                        stage_index,
                        node_id,
                        readiness_id,
                    );
                }
                if let Some(report_to) = self.report_to {
                    let _ = ctx.send(
                        report_to,
                        OrchestratorReport::NodeRuntimeReadyAck {
                            run_id,
                            node_id,
                            stage_index,
                            readiness_id,
                        },
                    );
                }
                return;
            }
            OrchestratorMsg::ObserveWeightsReady {
                run_id,
                node_id,
                stage_index,
            } => {
                if let Some(report_to) = self.report_to {
                    let _ = ctx.send(
                        report_to,
                        OrchestratorReport::WeightsReady {
                            run_id,
                            node_id,
                            stage_index,
                        },
                    );
                }
                return;
            }
            OrchestratorMsg::Snapshot { reply_to } => {
                let _ = ctx.send(
                    reply_to,
                    OrchestratorReport::Snapshot {
                        commands: self
                            .core
                            .commands()
                            .iter()
                            .map(RunCommandWire::from)
                            .collect(),
                        events: self
                            .core
                            .events()
                            .iter()
                            .map(LifecycleEventWire::from)
                            .collect(),
                        injected_sequences: self.core.injected_sequences(),
                    },
                );
                return;
            }
            _ => {}
        }

        let direct_report = match msg.clone() {
            OrchestratorMsg::ObserveStageReady {
                run_id,
                stage_index,
            } => Some(OrchestratorReport::StageReady {
                run_id,
                stage_index,
            }),
            OrchestratorMsg::ObserveStageFault {
                run_id,
                stage_index,
                reason,
            } => Some(OrchestratorReport::StageFault {
                run_id,
                stage_index,
                reason,
            }),
            _ => None,
        };

        self.observe(msg);
        if let (Some(report_to), Some(report)) = (self.report_to, direct_report) {
            let _ = ctx.send(report_to, report);
        }
        self.drain_outputs(ctx);
    }
}

impl From<&core::RunCommand> for RunCommandWire {
    fn from(command: &core::RunCommand) -> Self {
        match command {
            core::RunCommand::ProvisionStage { provision } => Self::ProvisionStage {
                run_id: provision.run_id.0,
                stage_index: provision.stage_index,
                node_id: provision.node_id.0,
            },
            core::RunCommand::CreateTokenInEndpoint { run_id } => {
                Self::CreateTokenInEndpoint { run_id: run_id.0 }
            }
            core::RunCommand::CreateTokenOutEndpoint { run_id } => {
                Self::CreateTokenOutEndpoint { run_id: run_id.0 }
            }
            core::RunCommand::InjectTokenObject { run_id, object } => Self::InjectTokenObject {
                run_id: run_id.0,
                sequence: object.sequence,
                payload: (&object.payload).into(),
            },
            core::RunCommand::StopRun {
                run_id,
                stage_index,
            } => Self::StopRun {
                run_id: run_id.0,
                stage_index: *stage_index,
            },
            core::RunCommand::TearDownTokenEndpoints { run_id } => {
                Self::TearDownTokenEndpoints { run_id: run_id.0 }
            }
        }
    }
}

impl From<&core::LifecycleEvent> for LifecycleEventWire {
    fn from(event: &core::LifecycleEvent) -> Self {
        match event {
            core::LifecycleEvent::Faulted { run_id, .. } => Self::Faulted { run_id: run_id.0 },
            core::LifecycleEvent::Completed { run_id } => Self::Completed { run_id: run_id.0 },
            core::LifecycleEvent::OperatorStopped { run_id } => {
                Self::OperatorStopped { run_id: run_id.0 }
            }
            core::LifecycleEvent::TornDown { run_id } => Self::TornDown { run_id: run_id.0 },
        }
    }
}

pub(crate) fn register_codecs(registry: &mut CodecRegistry) {
    registry
        .register::<OrchestratorMsg, _>(JsonCodec::<OrchestratorMsg>::default())
        .expect("unique codec registration");
    registry
        .register::<OrchestratorReport, _>(JsonCodec::<OrchestratorReport>::default())
        .expect("unique codec registration");
}

impl From<&core::TokenObjectPayload> for TokenObjectPayloadWire {
    fn from(payload: &core::TokenObjectPayload) -> Self {
        match payload {
            core::TokenObjectPayload::Prompt { tokens } => Self::Prompt {
                tokens: tokens.clone(),
            },
            core::TokenObjectPayload::Decode { token_id, sampling } => Self::Decode {
                token_id: *token_id,
                sampling: SamplingDataWire {
                    source_sequence: sampling.source_sequence,
                },
            },
        }
    }
}

#[cfg(test)]
mod event_observation_tests {
    use super::*;
    use crate::tests::fuzz_support::drive_steps;
    use parking_lot::Mutex;
    use std::sync::Arc;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::{Runtime, RuntimeParts};
    use swactor_engine::{Engine, SteppingBackend};

    struct Replies(Arc<Mutex<Vec<ContextualControlReply>>>);

    impl ActorInterface for Replies {
        type Incoming = ContextualReplyObserverMsg;
        type Response = ();

        fn handle(&mut self, _: &Ctx<'_>, reply: Self::Incoming) {
            let ContextualReplyObserverMsg::Reply(reply) = reply;
            self.0.lock().push(reply);
        }
    }

    fn pending_execution() -> ContextualExecutionState {
        ContextualExecutionState {
            logical_node_id: 1,
            execution_incarnation: "test-incarnation".to_owned(),
            process: None,
            terminal: false,
            next_sequence: 0,
            events: VecDeque::new(),
            spawn_waiter: None,
            staged_source: None,
            last_delivery: None,
        }
    }

    fn terminal() -> ContextualProcessEvent {
        ContextualProcessEvent {
            request_id: "attempt".to_owned(),
            logical_node_id: 1,
            event: ContextualProcessEventKind::ProcessError {
                error: "terminated".to_owned(),
            },
        }
    }

    fn send(
        runtime: &Runtime,
        backend: &SteppingBackend,
        actor: ActorAddress,
        message: OrchestratorMsg,
        replies: &Mutex<Vec<ContextualControlReply>>,
    ) -> Vec<ContextualControlReply> {
        runtime.send_to(actor, message).unwrap();
        drive_steps(backend, 64);
        std::mem::take(&mut *replies.lock())
    }

    #[test]
    fn stop_reply_and_execution_history_keep_distinct_request_identities() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
        let replies = Arc::new(Mutex::new(Vec::new()));
        let reply_to = runtime.spawn(Replies(replies.clone())).unwrap();
        let mut actor = OrchestratorActor::new(
            core::RunConfig {
                run_id: core::RunId(1),
                max_tokens: 1,
                prompt: vec![],
            },
            None,
        );
        actor
            .contextual_executions
            .insert("attempt".to_owned(), pending_execution());
        actor.pending_contextual_replies.insert(
            "stop-command".to_owned(),
            PendingContextualReply {
                reply_to: Some(reply_to),
                target_request_id: Some("attempt".to_owned()),
                logical_node_id: 1,
            },
        );
        let actor = runtime.spawn(actor).unwrap();
        let accepted = ContextualProcessEvent {
            request_id: "stop-command".to_owned(),
            logical_node_id: 1,
            event: ContextualProcessEventKind::StopAccepted {
                process: reply_to.to_full_hex(),
            },
        };
        let direct = send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEvent {
                observation: accepted.clone(),
                delivery_owner: None,
                sequence: 0,
            },
            &replies,
        );
        assert!(
            matches!(&direct[..], [ContextualControlReply::Event { observation }]
            if observation == &accepted)
        );
        send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEvent {
                observation: terminal(),
                delivery_owner: None,
                sequence: 0,
            },
            &replies,
        );
        let history = send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEvents {
                request_id: "attempt".to_owned(),
                after_sequence: 0,
                reply_to,
            },
            &replies,
        );
        let [ContextualControlReply::Events { execution }] = &history[..] else {
            panic!("missing execution history: {history:?}");
        };
        assert!(execution.terminal);
        let mut retained = accepted;
        retained.request_id = "attempt".to_owned();
        assert_eq!(
            execution
                .events
                .iter()
                .map(|event| &event.observation)
                .collect::<Vec<_>>(),
            vec![&retained, &terminal()],
        );
    }

    #[test]
    fn terminal_history_survives_lost_response_until_complete_acknowledgement() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
        let replies = Arc::new(Mutex::new(Vec::new()));
        let reply_to = runtime.spawn(Replies(replies.clone())).unwrap();
        let mut actor = OrchestratorActor::new(
            core::RunConfig {
                run_id: core::RunId(1),
                max_tokens: 1,
                prompt: vec![],
            },
            None,
        );
        let mut execution = pending_execution();
        execution.record(terminal());
        actor
            .contextual_executions
            .insert("attempt".to_owned(), execution);
        let actor = runtime.spawn(actor).unwrap();
        let query = || OrchestratorMsg::ContextualEvents {
            request_id: "attempt".to_owned(),
            after_sequence: 0,
            reply_to,
        };
        let first = send(&runtime, &backend, actor, query(), &replies);
        assert!(
            matches!(&first[..], [ContextualControlReply::Events { execution }]
            if execution.terminal && execution.events[0].observation == terminal())
        );
        assert_eq!(send(&runtime, &backend, actor, query(), &replies), first);
        let rejected = send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEventsAck {
                request_id: "attempt".to_owned(),
                through_sequence: 0,
                reply_to,
                execution_incarnation: "test-incarnation".to_owned(),
            },
            &replies,
        );
        assert!(matches!(
            &rejected[..],
            [ContextualControlReply::Rejected { .. }]
        ));
        assert_eq!(send(&runtime, &backend, actor, query(), &replies), first);
        let stale = send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEventsAck {
                request_id: "attempt".to_owned(),
                through_sequence: 1,
                reply_to,
                execution_incarnation: "previous-incarnation".to_owned(),
            },
            &replies,
        );
        assert!(matches!(
            &stale[..],
            [ContextualControlReply::Rejected { .. }]
        ));
        assert_eq!(send(&runtime, &backend, actor, query(), &replies), first);
        for _ in 0..2 {
            let ack = send(
                &runtime,
                &backend,
                actor,
                OrchestratorMsg::ContextualEventsAck {
                    request_id: "attempt".to_owned(),
                    through_sequence: 1,
                    reply_to,
                    execution_incarnation: "test-incarnation".to_owned(),
                },
                &replies,
            );
            assert!(matches!(
                &ack[..],
                [ContextualControlReply::Acknowledged(_)]
            ));
        }
        assert!(matches!(
            &send(&runtime, &backend, actor, query(), &replies)[..],
            [ContextualControlReply::Rejected { .. }]
        ));
        // A previous incarnation cannot fabricate a receipt after deletion.
        let old_incarnation = send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEventsAck {
                request_id: "attempt".to_owned(),
                through_sequence: 1,
                reply_to,
                execution_incarnation: "previous-incarnation".to_owned(),
            },
            &replies,
        );
        assert!(matches!(
            &old_incarnation[..],
            [ContextualControlReply::Rejected { .. }]
        ));
        // Restart loses this observer journal, not the obligation to prove the
        // original terminal result. An absent execution is never an ACK.
        let restarted = runtime
            .spawn(OrchestratorActor::new(
                core::RunConfig {
                    run_id: core::RunId(1),
                    max_tokens: 1,
                    prompt: vec![],
                },
                None,
            ))
            .unwrap();
        let lost_journal = send(
            &runtime,
            &backend,
            restarted,
            OrchestratorMsg::ContextualEventsAck {
                request_id: "attempt".to_owned(),
                through_sequence: 1,
                reply_to,
                execution_incarnation: "test-incarnation".to_owned(),
            },
            &replies,
        );
        assert!(matches!(
            &lost_journal[..],
            [ContextualControlReply::Rejected { .. }]
        ));
    }

    #[test]
    fn cursor_subscription_wakes_on_event_and_cancellation_removes_waiter() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
        let replies = Arc::new(Mutex::new(Vec::new()));
        let reply_to = runtime.spawn(Replies(replies.clone())).unwrap();
        let mut actor = OrchestratorActor::new(
            core::RunConfig {
                run_id: core::RunId(1),
                max_tokens: 1,
                prompt: vec![],
            },
            None,
        );
        actor
            .contextual_executions
            .insert("attempt".to_owned(), pending_execution());
        let actor = runtime.spawn(actor).unwrap();
        for key in ["cancelled", "live"] {
            assert!(
                send(
                    &runtime,
                    &backend,
                    actor,
                    OrchestratorMsg::ContextualEventsBatch {
                        cursors: vec![ContextualEventCursor {
                            request_id: "attempt".to_owned(),
                            after_sequence: 0,
                            execution_incarnation: None,
                        }],
                        wait_key: Some(key.to_owned()),
                        reply_to,
                    },
                    &replies
                )
                .is_empty()
            );
        }
        assert!(
            send(
                &runtime,
                &backend,
                actor,
                OrchestratorMsg::ContextualControlCancel {
                    control_request_id: "cancelled".to_owned(),
                    reply_to,
                },
                &replies
            )
            .is_empty()
        );
        // A dropped observer from an earlier request must not cancel its replacement.
        assert!(
            send(
                &runtime,
                &backend,
                actor,
                OrchestratorMsg::ContextualControlCancel {
                    control_request_id: "live".to_owned(),
                    reply_to: ActorAddress::new_random(),
                },
                &replies,
            )
            .is_empty()
        );
        let observed = send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEvent {
                observation: terminal(),
                delivery_owner: None,
                sequence: 0,
            },
            &replies,
        );
        assert!(
            matches!(&observed[..], [ContextualControlReply::EventsBatch(ContextualEventsBatch { executions, missing })]
            if missing.is_empty() && executions.len() == 1
                && executions[0].events[0].observation == terminal())
        );
    }

    #[test]
    fn delivery_duplicates_and_replaced_owner_cannot_repeat_a_local_terminal_result() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
        let replies = Arc::new(Mutex::new(Vec::new()));
        let reply_to = runtime.spawn(Replies(replies.clone())).unwrap();
        let mut actor = OrchestratorActor::new(
            core::RunConfig {
                run_id: core::RunId(1),
                max_tokens: 1,
                prompt: vec![],
            },
            None,
        );
        let mut execution = pending_execution();
        execution.spawn_waiter = Some(reply_to);
        actor
            .contextual_executions
            .insert("attempt".to_owned(), execution);
        let actor = runtime.spawn(actor).unwrap();
        let owner = ActorAddress([71; 32]);
        let event = OrchestratorMsg::ContextualEvent {
            observation: terminal(),
            delivery_owner: Some(owner),
            sequence: 7,
        };
        assert!(
            matches!(send(&runtime, &backend, actor, event.clone(), &replies).as_slice(), [ContextualControlReply::Event { observation }] if observation == &terminal())
        );
        assert!(send(&runtime, &backend, actor, event, &replies).is_empty());
        assert!(
            send(
                &runtime,
                &backend,
                actor,
                OrchestratorMsg::ContextualEvent {
                    observation: terminal(),
                    delivery_owner: Some(ActorAddress([72; 32])),
                    sequence: 1,
                },
                &replies
            )
            .is_empty()
        );
        let history = send(
            &runtime,
            &backend,
            actor,
            OrchestratorMsg::ContextualEvents {
                request_id: "attempt".to_owned(),
                after_sequence: 0,
                reply_to,
            },
            &replies,
        );
        assert!(
            matches!(history.as_slice(), [ContextualControlReply::Events { execution }] if execution.terminal && execution.events.len() == 1 && execution.next_sequence == 1)
        );
    }

    #[test]
    fn disconnecting_observers_does_not_invalidate_control_state() {
        let parts = RuntimeParts::new(RuntimeConfig::default());
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let _engine = Engine::new(parts, backend.clone()).unwrap();
        let replies = Arc::new(Mutex::new(Vec::new()));
        let reply_to = runtime.spawn(Replies(replies.clone())).unwrap();
        let actor = runtime
            .spawn(OrchestratorActor::new(
                core::RunConfig {
                    run_id: core::RunId(1),
                    max_tokens: 1,
                    prompt: vec![],
                },
                None,
            ))
            .unwrap();
        let query = || OrchestratorMsg::ControlChanges {
            generation: None,
            after_revision: None,
            wait_key: None,
            reply_to,
        };
        let initial = send(&runtime, &backend, actor, query(), &replies);
        for message in [
            OrchestratorMsg::ContextualControlCancel {
                control_request_id: "disconnected".to_owned(),
                reply_to,
            },
            OrchestratorMsg::Manual(ManualControlMsg::Cancel { reply_to }),
        ] {
            assert!(send(&runtime, &backend, actor, message, &replies).is_empty());
        }
        assert_eq!(send(&runtime, &backend, actor, query(), &replies), initial);
    }
}
