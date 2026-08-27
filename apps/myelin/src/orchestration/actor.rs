use std::collections::{HashMap, VecDeque};

use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;
use swactor_transport::{CodecRegistry, NetworkMessage};

use crate::contextual_process::{
    ContextualNodeCommand, ContextualProcessEventKindWire, ContextualProcessEventWire,
    ContextualProcessSpecWire,
};
use crate::node_actor::NodeAgentMsg;
use crate::orchestration::manual_control::{ManualActorControl, ManualControlMsg};
use crate::run_fsm as core;

use swactor_transport::JsonCodec;

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
    AdvanceTimeMs(u64),
    Snapshot {
        reply_to: ActorAddress,
    },
    Manual(ManualControlMsg),
    ContextualSpawn {
        logical_node_id: u64,
        request_id: String,
        spec: ContextualProcessSpecWire,
        reply_to: ActorAddress,
    },
    ContextualStop {
        request_id: String,
        control_request_id: String,
        kill_after_ms: Option<u64>,
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
    ContextualEvent(ContextualProcessEventWire),
    /// An HTTP control observer timed out waiting for its reply; drop the
    /// pending entry so a hung node cannot accumulate parked control requests.
    ContextualControlCancel {
        control_request_id: String,
    },
}

impl NetworkMessage for OrchestratorMsg {
    fn type_tag() -> &'static str {
        "myelin::OrchestratorMsg"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContextualEventRecord {
    pub sequence: u64,
    pub observation: ContextualProcessEventWire,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ContextualExecutionView {
    pub request_id: String,
    pub logical_node_id: u64,
    pub process: Option<ActorAddress>,
    pub terminal: bool,
    pub truncated_before: u64,
    pub next_sequence: u64,
    pub events: Vec<ContextualEventRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub(crate) enum ContextualControlReply {
    Event {
        observation: ContextualProcessEventWire,
    },
    Events {
        execution: ContextualExecutionView,
    },
    Rejected {
        error: String,
    },
    /// Locally injected by the HTTP reply observer when its deadline passes;
    /// never produced by the orchestrator.
    TimedOut,
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
const MAX_CONTEXTUAL_EXECUTIONS: usize = 256;
const MAX_CONTEXTUAL_EVENTS_PER_EXECUTION: usize = 4096;

struct ContextualExecutionState {
    logical_node_id: u64,
    process: Option<ActorAddress>,
    terminal: bool,
    next_sequence: u64,
    events: VecDeque<ContextualEventRecord>,
    spawn_waiter: Option<ActorAddress>,
}

impl ContextualExecutionState {
    fn record(&mut self, observation: ContextualProcessEventWire) {
        if let ContextualProcessEventKindWire::Spawned { process, .. } = &observation.event {
            self.process = Some(*process);
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
            logical_node_id: self.logical_node_id,
            process: self.process,
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
    reply_to: ActorAddress,
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
    pending_contextual_replies: HashMap<String, PendingContextualReply>,
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
            pending_contextual_replies: HashMap::new(),
        }
    }

    pub(crate) fn with_manual_control(mut self, manual: ManualActorControl) -> Self {
        self.manual = Some(manual);
        self
    }

    fn contextual_rejection(
        &self,
        ctx: &Ctx<'_>,
        reply_to: ActorAddress,
        error: impl Into<String>,
    ) {
        let _ = ctx.send(
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

    fn handle_contextual(&mut self, ctx: &Ctx<'_>, msg: &OrchestratorMsg) -> bool {
        match msg {
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
                if self.contextual_executions.len() >= MAX_CONTEXTUAL_EXECUTIONS {
                    self.contextual_executions
                        .retain(|_, execution| !execution.terminal);
                }
                if self.contextual_executions.len() >= MAX_CONTEXTUAL_EXECUTIONS {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        "too many live contextual executions",
                    );
                    return true;
                }
                self.contextual_executions.insert(
                    request_id.clone(),
                    ContextualExecutionState {
                        logical_node_id: *logical_node_id,
                        process: None,
                        terminal: false,
                        next_sequence: 0,
                        events: VecDeque::new(),
                        spawn_waiter: Some(*reply_to),
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
                    self.contextual_executions.remove(request_id);
                    self.contextual_rejection(ctx, *reply_to, error);
                }
                true
            }
            OrchestratorMsg::ContextualStop {
                request_id,
                control_request_id,
                kill_after_ms,
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
                let Some(process) = execution.process else {
                    self.contextual_rejection(
                        ctx,
                        *reply_to,
                        format!("contextual request {request_id:?} has not spawned"),
                    );
                    return true;
                };
                let logical_node_id = execution.logical_node_id;
                self.pending_contextual_replies.insert(
                    control_request_id.clone(),
                    PendingContextualReply {
                        reply_to: *reply_to,
                        target_request_id: Some(request_id.clone()),
                        logical_node_id,
                    },
                );
                if let Err(error) = self.route_contextual(
                    ctx,
                    logical_node_id,
                    ContextualNodeCommand::Stop {
                        request_id: control_request_id.clone(),
                        process,
                        kill_after_ms: *kill_after_ms,
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
                        reply_to: *reply_to,
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
                let terminal = execution.terminal;
                let reply = ContextualControlReply::Events {
                    execution: execution.view(request_id.clone(), *after_sequence),
                };
                let _ = ctx.send(*reply_to, reply);
                if terminal {
                    self.contextual_executions.remove(request_id);
                }
                true
            }
            OrchestratorMsg::ContextualEvent(observation) => {
                if let Some(pending) = self
                    .pending_contextual_replies
                    .remove(&observation.request_id)
                {
                    if pending.logical_node_id != observation.logical_node_id {
                        self.contextual_rejection(
                            ctx,
                            pending.reply_to,
                            format!(
                                "contextual reply came from node {}, expected {}",
                                observation.logical_node_id, pending.logical_node_id
                            ),
                        );
                        return true;
                    }
                    if let Some(target) = pending.target_request_id
                        && let Some(execution) = self.contextual_executions.get_mut(&target)
                    {
                        execution.record(observation.clone());
                    }
                    let _ = ctx.send(
                        pending.reply_to,
                        ContextualControlReply::Event {
                            observation: observation.clone(),
                        },
                    );
                    return true;
                }
                let Some(execution) = self.contextual_executions.get_mut(&observation.request_id)
                else {
                    return true;
                };
                if execution.logical_node_id != observation.logical_node_id {
                    let expected = execution.logical_node_id;
                    let waiter = execution.spawn_waiter.take();
                    execution.terminal = true;
                    if let Some(waiter) = waiter {
                        let _ = ctx.send(
                            waiter,
                            ContextualControlReply::Rejected {
                                error: format!(
                                    "contextual reply came from node {}, expected {}",
                                    observation.logical_node_id, expected
                                ),
                            },
                        );
                    }
                    return true;
                }
                let resolves_spawn = matches!(
                    &observation.event,
                    ContextualProcessEventKindWire::Spawned { .. }
                ) || observation.event.is_terminal();
                execution.record(observation.clone());
                if resolves_spawn && let Some(waiter) = execution.spawn_waiter.take() {
                    let _ = ctx.send(
                        waiter,
                        ContextualControlReply::Event {
                            observation: observation.clone(),
                        },
                    );
                }
                true
            }
            OrchestratorMsg::ContextualControlCancel { control_request_id } => {
                // The HTTP caller already observed a timeout; the pending
                // entry is stale and must not swallow a later reply.
                self.pending_contextual_replies.remove(control_request_id);
                true
            }
            _ => false,
        }
    }

    fn fail_contextual_node(&mut self, ctx: &Ctx<'_>, logical_node_id: u64, reason: &str) {
        let mut waiters = Vec::new();
        for (request_id, execution) in &mut self.contextual_executions {
            if execution.logical_node_id != logical_node_id || execution.terminal {
                continue;
            }
            let observation = ContextualProcessEventWire {
                request_id: request_id.clone(),
                logical_node_id,
                event: ContextualProcessEventKindWire::ProcessError {
                    error: reason.to_owned(),
                },
            };
            execution.record(observation.clone());
            if let Some(waiter) = execution.spawn_waiter.take() {
                waiters.push((waiter, observation));
            }
        }
        for (waiter, observation) in waiters {
            let _ = ctx.send(waiter, ContextualControlReply::Event { observation });
        }

        // Stop and query controls parked against the failed node will never
        // receive a reply; reject them now so HTTP callers fail boundedly and
        // the pending map cannot grow across node losses.
        let mut timed_out_callers = Vec::new();
        self.pending_contextual_replies.retain(|_, pending| {
            if pending.logical_node_id == logical_node_id {
                timed_out_callers.push(pending.reply_to);
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
            | OrchestratorMsg::ContextualEvent(_)
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
            OrchestratorMsg::AdvanceTimeMs(delta) => self.core.advance_time_ms(delta),
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
            return;
        }
        if let OrchestratorMsg::Manual(manual_msg) = msg.clone() {
            if let ManualControlMsg::Kill { request, .. } = &manual_msg {
                self.fail_contextual_node(
                    ctx,
                    request.logical_node_id,
                    "contextual process node termination was requested",
                );
            }
            if let Some(manual) = self.manual.as_mut() {
                manual.handle(ctx, manual_msg);
            }
            return;
        }
        if let OrchestratorMsg::ObserveMembershipLost { node_id, .. } = &msg {
            self.fail_contextual_node(
                ctx,
                *node_id,
                "contextual process node was lost from membership",
            );
        }
        match msg.clone() {
            OrchestratorMsg::ObserveNodeRuntimeReady {
                run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
                readiness_id,
            } => {
                if let Some(manual) = self.manual.as_mut() {
                    manual.observe_runtime_ready(
                        ctx.self_addr(),
                        node_id,
                        crate::orchestration::daemon::RuntimeFacts {
                            run_id,
                            attempt_id: manual
                                .read_model()
                                .nodes
                                .iter()
                                .find(|node| node.logical_node_id == node_id)
                                .and_then(|node| node.spec.as_ref())
                                .map_or(0, |spec| spec.attempt_id),
                            endpoint: serde_json::to_string(&endpoint).unwrap_or_default(),
                            node_actor,
                            swim_node_id: distribution::types::NodeId(*endpoint.id.as_bytes()),
                            stage_index,
                            readiness_id,
                        },
                    );
                }
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
                    manual.observe_node_ack(ctx.self_addr(), node_id, readiness_id);
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
