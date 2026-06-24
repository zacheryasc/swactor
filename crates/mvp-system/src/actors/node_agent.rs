use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;
use swactor_transport::{CodecRegistry, NetworkMessage};

use crate::stage_controller as stage;

use super::codec::JsonCodec;
use super::orchestrator::OrchestratorMsg;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageProvisionWire {
    pub run_id: u64,
    pub authorized_orchestrator: u64,
    pub node_id: u64,
    pub stage_index: u32,
    pub stage_count: u32,
    pub layer_start: u32,
    pub layer_end_exclusive: u32,
    pub inbound_edge_id: u64,
    pub outbound_edge_id: u64,
    pub weight_artifact: String,
}

impl StageProvisionWire {
    fn to_core(&self) -> stage::ProvisionStage {
        stage::ProvisionStage {
            run_id: stage::RunId(self.run_id),
            authorized_orchestrator: stage::NodeId(self.authorized_orchestrator),
            node_id: stage::NodeId(self.node_id),
            stage_index: self.stage_index,
            stage_count: self.stage_count,
            layer_range: stage::LayerRange {
                start: self.layer_start,
                end_exclusive: self.layer_end_exclusive,
            },
            inbound: stage::EdgeProvision::inbound(stage::EdgeId(self.inbound_edge_id)),
            outbound: stage::EdgeProvision::outbound(stage::EdgeId(self.outbound_edge_id)),
            weight_source: stage::WeightSource::TestArtifact(self.weight_artifact.clone()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeAgentMsg {
    ProvisionStage(StageProvisionWire),
    MarkWorkerReady,
    MarkWeightsReady,
    MarkInboundEdgeReady {
        edge_id: u64,
    },
    MarkOutboundEdgeReady {
        edge_id: u64,
    },
    ObjectLoaded {
        edge_id: u64,
        object_id: u64,
        sequence: u64,
        handle_generation: u64,
        handle_id: u64,
    },
    StepCompleted {
        step_id: u64,
    },
    WorkerCrashed,
    StopRun {
        run_id: u64,
    },
    Snapshot {
        reply_to: ActorAddress,
    },
}

impl NetworkMessage for NodeAgentMsg {
    fn type_tag() -> &'static str {
        "mvp_system::NodeAgentMsg"
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageCommandWire {
    EstablishInboundEdge {
        edge_id: u64,
    },
    EstablishOutboundEdge {
        edge_id: u64,
    },
    ConfigureWorkerRole {
        run_id: u64,
        stage_index: u32,
        layer_start: u32,
        layer_end_exclusive: u32,
    },
    LoadWeights {
        artifact: String,
        layer_start: u32,
        layer_end_exclusive: u32,
    },
    RewireEdge {
        edge_id: u64,
    },
    ExecuteStep {
        step_id: u64,
        input_edge_id: u64,
        object_id: u64,
        sequence: u64,
        output_edge_ids: Vec<u64>,
    },
    ReleaseInputHandle {
        object_id: u64,
        handle_generation: u64,
        handle_id: u64,
    },
    StopLocalEdges {
        run_id: u64,
    },
    ReleaseRunDeviceObjects {
        run_id: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageLifecycleWire {
    StageReady {
        run_id: u64,
        stage_index: u32,
    },
    StepAccepted {
        run_id: u64,
        stage_index: u32,
        sequence: u64,
    },
    StageFault {
        run_id: u64,
        stage_index: u32,
    },
    StageStopped {
        run_id: u64,
        stage_index: u32,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeAgentReport {
    Command(StageCommandWire),
    Lifecycle(StageLifecycleWire),
    Snapshot {
        commands: Vec<StageCommandWire>,
        events: Vec<StageLifecycleWire>,
    },
}

impl NetworkMessage for NodeAgentReport {
    fn type_tag() -> &'static str {
        "mvp_system::NodeAgentReport"
    }
}

pub struct NodeAgentActor {
    core: stage::StageController,
    orchestrator: ActorAddress,
    report_to: Option<ActorAddress>,
    command_cursor: usize,
    event_cursor: usize,
}

impl NodeAgentActor {
    pub fn new(
        local_node_id: stage::NodeId,
        orchestrator: ActorAddress,
        report_to: Option<ActorAddress>,
    ) -> Self {
        Self {
            core: stage::StageController::new(local_node_id),
            orchestrator,
            report_to,
            command_cursor: 0,
            event_cursor: 0,
        }
    }

    fn observe(&mut self, ctx: &Ctx, msg: NodeAgentMsg) {
        match msg {
            NodeAgentMsg::ProvisionStage(provision) => {
                self.core.observe(stage::StageEvent::ProvisionStage {
                    from: stage::NodeId(provision.authorized_orchestrator),
                    provision: provision.to_core(),
                })
            }
            NodeAgentMsg::MarkWorkerReady => self.core.observe(stage::StageEvent::WorkerReady),
            NodeAgentMsg::MarkWeightsReady => self.core.observe(stage::StageEvent::WeightsReady),
            NodeAgentMsg::MarkInboundEdgeReady { edge_id } => {
                self.core.observe(stage::StageEvent::InboundEdgeReady {
                    edge_id: stage::EdgeId(edge_id),
                })
            }
            NodeAgentMsg::MarkOutboundEdgeReady { edge_id } => {
                self.core.observe(stage::StageEvent::OutboundEdgeReady {
                    edge_id: stage::EdgeId(edge_id),
                })
            }
            NodeAgentMsg::ObjectLoaded {
                edge_id,
                object_id,
                sequence,
                handle_generation,
                handle_id,
            } => self.core.observe(stage::StageEvent::ObjectLoaded {
                edge_id: stage::EdgeId(edge_id),
                object_id: stage::ObjectId(object_id),
                sequence,
                handle: stage::DeviceHandle {
                    generation: handle_generation,
                    id: handle_id,
                },
            }),
            NodeAgentMsg::StepCompleted { step_id } => {
                self.core.observe(stage::StageEvent::StepCompleted {
                    step_id: stage::StepId(step_id),
                })
            }
            NodeAgentMsg::WorkerCrashed => self.core.observe(stage::StageEvent::WorkerCrashed),
            NodeAgentMsg::StopRun { run_id } => self.core.observe(stage::StageEvent::StopRun {
                run_id: stage::RunId(run_id),
            }),
            NodeAgentMsg::Snapshot { reply_to } => {
                let _ = ctx.send(
                    reply_to,
                    NodeAgentReport::Snapshot {
                        commands: self
                            .core
                            .commands()
                            .iter()
                            .map(StageCommandWire::from)
                            .collect(),
                        events: self
                            .core
                            .events()
                            .iter()
                            .map(StageLifecycleWire::from)
                            .collect(),
                    },
                );
                return;
            }
        }
        self.drain_outputs(ctx);
    }

    fn drain_outputs(&mut self, ctx: &Ctx) {
        for command in &self.core.commands()[self.command_cursor..] {
            if let Some(report_to) = self.report_to {
                let _ = ctx.send(report_to, NodeAgentReport::Command(command.into()));
            }
        }
        self.command_cursor = self.core.commands().len();

        for event in &self.core.events()[self.event_cursor..] {
            match event {
                stage::StageLifecycleEvent::StageReady {
                    run_id,
                    stage_index,
                } => {
                    let _ = ctx.send(
                        self.orchestrator,
                        OrchestratorMsg::ObserveStageReady {
                            run_id: run_id.0,
                            stage_index: *stage_index,
                        },
                    );
                }
                stage::StageLifecycleEvent::StageFault {
                    run_id,
                    stage_index,
                    ..
                } => {
                    let _ = ctx.send(
                        self.orchestrator,
                        OrchestratorMsg::ObserveStageFault {
                            run_id: run_id.0,
                            stage_index: *stage_index,
                        },
                    );
                }
                stage::StageLifecycleEvent::StageStopped {
                    run_id,
                    stage_index,
                } => {
                    let _ = ctx.send(
                        self.orchestrator,
                        OrchestratorMsg::ObserveStageStopped {
                            run_id: run_id.0,
                            stage_index: *stage_index,
                        },
                    );
                }
                stage::StageLifecycleEvent::StepAccepted { .. } => {}
            }
            if let Some(report_to) = self.report_to {
                let _ = ctx.send(report_to, NodeAgentReport::Lifecycle(event.into()));
            }
        }
        self.event_cursor = self.core.events().len();
    }
}

impl ActorInterface for NodeAgentActor {
    type Incoming = NodeAgentMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        self.observe(ctx, msg);
    }
}

impl From<&stage::StageCommand> for StageCommandWire {
    fn from(command: &stage::StageCommand) -> Self {
        match command {
            stage::StageCommand::EstablishInboundEdge { edge_id } => {
                Self::EstablishInboundEdge { edge_id: edge_id.0 }
            }
            stage::StageCommand::EstablishOutboundEdge { edge_id } => {
                Self::EstablishOutboundEdge { edge_id: edge_id.0 }
            }
            stage::StageCommand::ConfigureWorkerRole {
                run_id,
                stage_index,
                layer_range,
            } => Self::ConfigureWorkerRole {
                run_id: run_id.0,
                stage_index: *stage_index,
                layer_start: layer_range.start,
                layer_end_exclusive: layer_range.end_exclusive,
            },
            stage::StageCommand::LoadWeights { source, range } => Self::LoadWeights {
                artifact: match source {
                    stage::WeightSource::TestArtifact(value) => value.clone(),
                },
                layer_start: range.start,
                layer_end_exclusive: range.end_exclusive,
            },
            stage::StageCommand::RewireEdge { edge_id } => Self::RewireEdge { edge_id: edge_id.0 },
            stage::StageCommand::ExecuteStep(step) => Self::ExecuteStep {
                step_id: step.step_id.0,
                input_edge_id: step.input.edge_id.0,
                object_id: step.input.object_id.0,
                sequence: step.input.sequence,
                output_edge_ids: step.outputs.iter().map(|output| output.edge_id.0).collect(),
            },
            stage::StageCommand::ReleaseInputHandle { object_id, handle } => {
                Self::ReleaseInputHandle {
                    object_id: object_id.0,
                    handle_generation: handle.generation,
                    handle_id: handle.id,
                }
            }
            stage::StageCommand::StopLocalEdges { run_id } => {
                Self::StopLocalEdges { run_id: run_id.0 }
            }
            stage::StageCommand::ReleaseRunDeviceObjects { run_id } => {
                Self::ReleaseRunDeviceObjects { run_id: run_id.0 }
            }
        }
    }
}

impl From<&stage::StageLifecycleEvent> for StageLifecycleWire {
    fn from(event: &stage::StageLifecycleEvent) -> Self {
        match event {
            stage::StageLifecycleEvent::StageReady {
                run_id,
                stage_index,
            } => Self::StageReady {
                run_id: run_id.0,
                stage_index: *stage_index,
            },
            stage::StageLifecycleEvent::StepAccepted {
                run_id,
                stage_index,
                sequence,
            } => Self::StepAccepted {
                run_id: run_id.0,
                stage_index: *stage_index,
                sequence: *sequence,
            },
            stage::StageLifecycleEvent::StageFault {
                run_id,
                stage_index,
                ..
            } => Self::StageFault {
                run_id: run_id.0,
                stage_index: *stage_index,
            },
            stage::StageLifecycleEvent::StageStopped {
                run_id,
                stage_index,
            } => Self::StageStopped {
                run_id: run_id.0,
                stage_index: *stage_index,
            },
        }
    }
}

pub fn register_codecs(registry: &mut CodecRegistry) {
    registry.register::<NodeAgentMsg, _>(JsonCodec::<NodeAgentMsg>::default());
    registry.register::<NodeAgentReport, _>(JsonCodec::<NodeAgentReport>::default());
}
