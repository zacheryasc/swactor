//! MVP adapter for coarse reusable data-plane actor reports.

use data_plane::actor as dp;
use swactor::actor::{ActorAddress, ActorInterface};
use swactor::runtime::Ctx;

use crate::node::actor::NodeAgentMsg;

pub struct MvpDataPlaneReportSinkActor {
    node_agent: ActorAddress,
}

impl MvpDataPlaneReportSinkActor {
    pub fn new(node_agent: ActorAddress) -> Self {
        Self { node_agent }
    }
}

impl ActorInterface for MvpDataPlaneReportSinkActor {
    type Incoming = dp::DataPlaneReportMsg;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx, msg: Self::Incoming) {
        match msg {
            dp::DataPlaneReportMsg::InboundEdgeReady { edge_id } => {
                let _ = ctx.send(
                    self.node_agent,
                    NodeAgentMsg::MarkInboundEdgeReady { edge_id: edge_id.0 },
                );
            }
            dp::DataPlaneReportMsg::OutboundEdgeReady { edge_id } => {
                let _ = ctx.send(
                    self.node_agent,
                    NodeAgentMsg::MarkOutboundEdgeReady { edge_id: edge_id.0 },
                );
            }
            dp::DataPlaneReportMsg::ObjectLoaded {
                edge_id,
                object_id,
                sequence,
                handle,
                ..
            } => {
                let _ = ctx.send(
                    self.node_agent,
                    NodeAgentMsg::ObjectLoaded {
                        edge_id: edge_id.0,
                        object_id: object_id.0,
                        sequence,
                        handle_generation: handle.generation.0,
                        handle_id: handle.id,
                    },
                );
            }
            dp::DataPlaneReportMsg::ObjectProduced { .. } => {}
            dp::DataPlaneReportMsg::EdgeFaulted { .. }
            | dp::DataPlaneReportMsg::WorkerDataPlaneFaulted { .. } => {
                let _ = ctx.send(
                    self.node_agent,
                    NodeAgentMsg::WorkerCrashed {
                        reason: Some("data plane faulted".to_owned()),
                    },
                );
            }
            dp::DataPlaneReportMsg::EdgeStopped { .. } => {}
            dp::DataPlaneReportMsg::LocalEdgesStopped { run_id } => {
                let _ = ctx.send(
                    self.node_agent,
                    NodeAgentMsg::LocalEdgesStopped { run_id: run_id.0 },
                );
            }
        }
    }
}
