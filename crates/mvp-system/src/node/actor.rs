use iroh::EndpointAddr;
use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;
use swactor_transport::{CodecRegistry, NetworkMessage};

use crate::orchestration::run_plan;
use crate::staging::{self as stage, gguf_shard::StageShardPlan};

use crate::orchestration::actor::OrchestratorMsg;
use crate::transport::json_codec::JsonCodec;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StageEdgeKindWire {
    TokenIn,
    Activation,
    TokenOut,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageObjectSpecWire {
    pub max_extent: u64,
    pub alignment: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageRingSpecWire {
    pub data_capacity: u64,
    pub alignment: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageInboundEdgeWire {
    pub edge_id: u64,
    pub kind: StageEdgeKindWire,
    pub object_spec: StageObjectSpecWire,
    pub ring_spec: StageRingSpecWire,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StageOutboundEdgeWire {
    pub edge_id: u64,
    pub kind: StageEdgeKindWire,
    pub consumer_node_id: u64,
    pub consumer_endpoint: Option<EndpointAddr>,
    pub object_spec: StageObjectSpecWire,
    pub ring_spec: StageRingSpecWire,
}

impl StageInboundEdgeWire {
    fn fallback(edge_id: u64) -> Self {
        Self {
            edge_id,
            kind: StageEdgeKindWire::Activation,
            object_spec: StageObjectSpecWire {
                max_extent: 4096,
                alignment: 4,
            },
            ring_spec: StageRingSpecWire {
                data_capacity: 4096 + run_plan::MO01_HEADER_BYTES,
                alignment: 64,
            },
        }
    }
}

impl StageOutboundEdgeWire {
    fn fallback(edge_id: u64) -> Self {
        Self {
            edge_id,
            kind: StageEdgeKindWire::Activation,
            consumer_node_id: 0,
            consumer_endpoint: None,
            object_spec: StageObjectSpecWire {
                max_extent: 4096,
                alignment: 4,
            },
            ring_spec: StageRingSpecWire {
                data_capacity: 4096 + run_plan::MO01_HEADER_BYTES,
                alignment: 64,
            },
        }
    }
}

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
    pub inbound_edge: Option<StageInboundEdgeWire>,
    pub outbound_edge: Option<StageOutboundEdgeWire>,
    pub model_id: String,
    pub gguf_source: run_plan::GgufSource,
    pub tokenizer: run_plan::TokenizerSource,
    pub stage_shard_plan: Option<StageShardPlan>,
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
            weight_source: stage::WeightSource::new(
                self.model_id.clone(),
                self.gguf_source.clone(),
                self.tokenizer.clone(),
            ),
            shard_plan: self.stage_shard_plan.clone(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeAgentMsg {
    ProvisionStage(StageProvisionWire),
    MarkWorkerReady,
    RuntimeLoaded {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        endpoint: EndpointAddr,
        node_actor: ActorAddress,
        datastream_publisher: ActorAddress,
        readiness_id: u64,
    },
    RuntimeReadyAck {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        readiness_id: u64,
    },
    MarkWeightsReady {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
    },
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
    LocalEdgesStopped {
        run_id: u64,
    },
    WorkerRingsQuiesced {
        run_id: u64,
    },
    DeviceObjectsReleased {
        run_id: u64,
    },
    WorkerRoleReset {
        run_id: u64,
    },
    Snapshot {
        reply_to: ActorAddress,
    },
    InferPrompt {
        request_id: u64,
        prompt: String,
        max_tokens: u32,
        reply_to: ActorAddress,
    },
    EncodePrompt {
        request_id: u64,
        prompt: String,
        reply_to: ActorAddress,
    },
    DecodeTokens {
        request_id: u64,
        tokens: Vec<u32>,
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
        edge: StageInboundEdgeWire,
    },
    EstablishOutboundEdge {
        edge_id: u64,
        edge: StageOutboundEdgeWire,
    },
    ConfigureWorkerRole {
        run_id: u64,
        stage_index: u32,
        layer_start: u32,
        layer_end_exclusive: u32,
    },
    LoadWeights {
        model_id: String,
        gguf_source: run_plan::GgufSource,
        tokenizer: run_plan::TokenizerSource,
        layer_start: u32,
        layer_end_exclusive: u32,
        stage_shard_plan: Option<StageShardPlan>,
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
    PromptRequested {
        request_id: u64,
        prompt: String,
        max_tokens: u32,
        reply_to: ActorAddress,
    },
    EncodePromptRequested {
        request_id: u64,
        prompt: String,
        reply_to: ActorAddress,
    },
    DecodeTokensRequested {
        request_id: u64,
        tokens: Vec<u32>,
        reply_to: ActorAddress,
    },
    RuntimeReadyAck {
        run_id: u64,
        node_id: u64,
        stage_index: u32,
        readiness_id: u64,
    },
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
    inbound_edge: Option<StageInboundEdgeWire>,
    outbound_edge: Option<StageOutboundEdgeWire>,
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
            inbound_edge: None,
            outbound_edge: None,
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
                });
                self.inbound_edge = provision.inbound_edge;
                self.outbound_edge = provision.outbound_edge;
            }
            NodeAgentMsg::MarkWorkerReady => self.core.observe(stage::StageEvent::WorkerReady),
            NodeAgentMsg::RuntimeLoaded {
                run_id,
                node_id,
                stage_index,
                endpoint,
                node_actor,
                datastream_publisher,
                readiness_id,
            } => {
                self.core.observe(stage::StageEvent::WorkerReady);
                let _ = ctx.send(
                    self.orchestrator,
                    OrchestratorMsg::ObserveNodeRuntimeReady {
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
            NodeAgentMsg::RuntimeReadyAck {
                run_id,
                node_id,
                stage_index,
                readiness_id,
            } => {
                let _ = ctx.send(
                    self.orchestrator,
                    OrchestratorMsg::ObserveNodeRuntimeReadyAck {
                        run_id,
                        node_id,
                        stage_index,
                        readiness_id,
                    },
                );
                if let Some(report_to) = self.report_to {
                    let _ = ctx.send(
                        report_to,
                        NodeAgentReport::RuntimeReadyAck {
                            run_id,
                            node_id,
                            stage_index,
                            readiness_id,
                        },
                    );
                }
            }
            NodeAgentMsg::MarkWeightsReady {
                run_id,
                node_id,
                stage_index,
            } => {
                let _ = ctx.send(
                    self.orchestrator,
                    OrchestratorMsg::ObserveWeightsReady {
                        run_id,
                        node_id,
                        stage_index,
                    },
                );
                self.core.observe(stage::StageEvent::WeightsReady)
            }
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
            NodeAgentMsg::LocalEdgesStopped { run_id } => {
                self.core.observe(stage::StageEvent::LocalEdgesStopped {
                    run_id: stage::RunId(run_id),
                })
            }
            NodeAgentMsg::WorkerRingsQuiesced { run_id } => {
                self.core.observe(stage::StageEvent::WorkerRingsQuiesced {
                    run_id: stage::RunId(run_id),
                })
            }
            NodeAgentMsg::DeviceObjectsReleased { run_id } => {
                self.core.observe(stage::StageEvent::DeviceObjectsReleased {
                    run_id: stage::RunId(run_id),
                })
            }
            NodeAgentMsg::WorkerRoleReset { run_id } => {
                self.core.observe(stage::StageEvent::WorkerRoleReset {
                    run_id: stage::RunId(run_id),
                })
            }
            NodeAgentMsg::InferPrompt {
                request_id,
                prompt,
                max_tokens,
                reply_to,
            } => {
                if let Some(report_to) = self.report_to {
                    let _ = ctx.send(
                        report_to,
                        NodeAgentReport::PromptRequested {
                            request_id,
                            prompt,
                            max_tokens,
                            reply_to,
                        },
                    );
                }
                return;
            }
            NodeAgentMsg::EncodePrompt {
                request_id,
                prompt,
                reply_to,
            } => {
                if let Some(report_to) = self.report_to {
                    let _ = ctx.send(
                        report_to,
                        NodeAgentReport::EncodePromptRequested {
                            request_id,
                            prompt,
                            reply_to,
                        },
                    );
                }
                return;
            }
            NodeAgentMsg::DecodeTokens {
                request_id,
                tokens,
                reply_to,
            } => {
                if let Some(report_to) = self.report_to {
                    let _ = ctx.send(
                        report_to,
                        NodeAgentReport::DecodeTokensRequested {
                            request_id,
                            tokens,
                            reply_to,
                        },
                    );
                }
                return;
            }
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
                let _ = ctx.send(
                    report_to,
                    NodeAgentReport::Command(self.command_wire(command)),
                );
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

    fn command_wire(&self, command: &stage::StageCommand) -> StageCommandWire {
        match command {
            stage::StageCommand::EstablishInboundEdge { edge_id } => {
                let edge = self
                    .inbound_edge
                    .clone()
                    .filter(|edge| edge.edge_id == edge_id.0)
                    .unwrap_or_else(|| StageInboundEdgeWire::fallback(edge_id.0));
                StageCommandWire::EstablishInboundEdge {
                    edge_id: edge_id.0,
                    edge,
                }
            }
            stage::StageCommand::EstablishOutboundEdge { edge_id } => {
                let edge = self
                    .outbound_edge
                    .clone()
                    .filter(|edge| edge.edge_id == edge_id.0)
                    .unwrap_or_else(|| StageOutboundEdgeWire::fallback(edge_id.0));
                StageCommandWire::EstablishOutboundEdge {
                    edge_id: edge_id.0,
                    edge,
                }
            }
            _ => command.into(),
        }
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
            stage::StageCommand::EstablishInboundEdge { edge_id } => Self::EstablishInboundEdge {
                edge_id: edge_id.0,
                edge: StageInboundEdgeWire::fallback(edge_id.0),
            },
            stage::StageCommand::EstablishOutboundEdge { edge_id } => Self::EstablishOutboundEdge {
                edge_id: edge_id.0,
                edge: StageOutboundEdgeWire::fallback(edge_id.0),
            },
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
            stage::StageCommand::LoadWeights {
                source,
                range,
                shard_plan,
            } => Self::LoadWeights {
                model_id: source.model_id.clone(),
                gguf_source: source.gguf_source.clone(),
                tokenizer: source.tokenizer.clone(),
                layer_start: range.start,
                layer_end_exclusive: range.end_exclusive,
                stage_shard_plan: shard_plan.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use iroh::SecretKey;
    use swactor::config::RuntimeConfig;
    use swactor::runtime::Runtime;

    #[test]
    fn node_agent_runtime_loaded_reports_orchestrator() {
        let runtime = Runtime::new(RuntimeConfig::default());
        let orchestrator_inbox = runtime
            .new_inbox::<OrchestratorMsg>()
            .expect("orchestrator inbox");
        let orchestrator = *orchestrator_inbox.addr();
        let node_actor = ActorAddress::new_random();
        let datastream_publisher = ActorAddress::new_random();
        let endpoint = EndpointAddr::new(SecretKey::from_bytes(&[9; 32]).public());
        let actor = runtime
            .spawn(NodeAgentActor::new(stage::NodeId(11), orchestrator, None))
            .expect("spawn node agent");

        runtime
            .send_to(
                actor,
                NodeAgentMsg::RuntimeLoaded {
                    run_id: 7,
                    node_id: 11,
                    stage_index: 3,
                    endpoint: endpoint.clone(),
                    node_actor,
                    datastream_publisher,
                    readiness_id: 99,
                },
            )
            .expect("send runtime loaded");
        runtime.tick();

        assert_eq!(
            orchestrator_inbox.try_recv(),
            Some(OrchestratorMsg::ObserveNodeRuntimeReady {
                run_id: 7,
                node_id: 11,
                stage_index: 3,
                endpoint,
                node_actor,
                datastream_publisher,
                readiness_id: 99,
            })
        );
    }

    #[test]
    fn node_agent_runtime_ready_ack_reports_worker_loop() {
        let runtime = Runtime::new(RuntimeConfig::default());
        let orchestrator_inbox = runtime
            .new_inbox::<OrchestratorMsg>()
            .expect("orchestrator inbox");
        let reports = runtime
            .new_inbox::<NodeAgentReport>()
            .expect("node report inbox");
        let actor = runtime
            .spawn(NodeAgentActor::new(
                stage::NodeId(11),
                *orchestrator_inbox.addr(),
                Some(*reports.addr()),
            ))
            .expect("spawn node agent");

        runtime
            .send_to(
                actor,
                NodeAgentMsg::RuntimeReadyAck {
                    run_id: 7,
                    node_id: 11,
                    stage_index: 3,
                    readiness_id: 99,
                },
            )
            .expect("send runtime ready ack");
        runtime.tick();

        assert_eq!(
            reports.try_recv(),
            Some(NodeAgentReport::RuntimeReadyAck {
                run_id: 7,
                node_id: 11,
                stage_index: 3,
                readiness_id: 99,
            })
        );
        assert_eq!(reports.try_recv(), None);
    }
}
