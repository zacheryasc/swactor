use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;
use swactor_transport::{CodecRegistry, NetworkMessage};

use crate::run_fsm as core;

use crate::transport::json_codec::JsonCodec;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StageRefWire {
    pub stage_index: u32,
    pub node_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
        datastream_publisher: ActorAddress,
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
    AdvanceTimeMs(u64),
    Snapshot {
        reply_to: ActorAddress,
    },
}

impl NetworkMessage for OrchestratorMsg {
    fn type_tag() -> &'static str {
        "mvp_system::OrchestratorMsg"
    }
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
    RunRejected { run_id: u64 },
    RunFaulted { run_id: u64 },
    RunCompleted { run_id: u64 },
    RunOperatorStopped { run_id: u64 },
    RunTornDown { run_id: u64 },
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
        datastream_publisher: ActorAddress,
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
        "mvp_system::OrchestratorReport"
    }
}

pub(crate) struct OrchestratorActor {
    core: core::OrchestratorRun,
    report_to: Option<ActorAddress>,
    command_cursor: usize,
    event_cursor: usize,
}

impl OrchestratorActor {
    pub(crate) fn new(config: core::RunConfig, report_to: Option<ActorAddress>) -> Self {
        Self {
            core: core::OrchestratorRun::new(config),
            report_to,
            command_cursor: 0,
            event_cursor: 0,
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
            OrchestratorMsg::ObserveNodeRuntimeReady { .. } => {}
            OrchestratorMsg::ObserveNodeRuntimeReadyAck { .. } => {}
            OrchestratorMsg::ObserveWeightsReady { .. } => {}
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
            OrchestratorMsg::AdvanceTimeMs(delta) => self.core.advance_time_ms(delta),
            OrchestratorMsg::Snapshot { .. } => {}
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

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg.clone() {
            OrchestratorMsg::ObserveNodeRuntimeReady {
                run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
                datastream_publisher,
                readiness_id,
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
                            datastream_publisher,
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
            core::LifecycleEvent::RunRejected { run_id, .. } => {
                Self::RunRejected { run_id: run_id.0 }
            }
            core::LifecycleEvent::RunFaulted { run_id, .. } => {
                Self::RunFaulted { run_id: run_id.0 }
            }
            core::LifecycleEvent::RunCompleted { run_id } => {
                Self::RunCompleted { run_id: run_id.0 }
            }
            core::LifecycleEvent::RunOperatorStopped { run_id } => {
                Self::RunOperatorStopped { run_id: run_id.0 }
            }
            core::LifecycleEvent::RunTornDown { run_id } => Self::RunTornDown { run_id: run_id.0 },
        }
    }
}

pub(crate) fn register_codecs(registry: &mut CodecRegistry) {
    registry.register::<OrchestratorMsg, _>(JsonCodec::<OrchestratorMsg>::default());
    registry.register::<OrchestratorReport, _>(JsonCodec::<OrchestratorReport>::default());
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
