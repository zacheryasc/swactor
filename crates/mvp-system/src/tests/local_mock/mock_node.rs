use mvp_system::run_plan as plan;
use mvp_system::stage_controller as stage;
use mvp_system::tx_rx_edge_actor as edge_actor;

use super::mock_transport::MockObject;
use super::mock_worker::MockWorker;

pub struct MockExecution {
    pub step_id: u64,
    pub produced: MockObject,
}

pub struct MockNode {
    pub stage_index: u32,
    stage_count: u32,
    inbound_edge: Option<plan::EdgeId>,
    outbound_edge: Option<plan::EdgeId>,
    outbound_object_allocator: Option<edge_actor::ObjectIdAllocator>,
    controller: stage::StageControllerHarness,
    worker: MockWorker,
    event_cursor: usize,
}

impl MockNode {
    pub fn new(
        stage_index: u32,
        node_id: plan::NodeId,
        stage_count: u32,
        eos_after_sequence: u64,
    ) -> Self {
        Self {
            stage_index,
            stage_count,
            inbound_edge: None,
            outbound_edge: None,
            outbound_object_allocator: None,
            controller: stage::StageControllerHarness::new(stage::NodeId(node_id.0)),
            worker: MockWorker::new(stage_index, eos_after_sequence),
            event_cursor: 0,
        }
    }

    pub fn provision(&mut self, from: stage::NodeId, provision: stage::ProvisionStage) {
        self.inbound_edge = Some(plan::EdgeId(provision.inbound.edge_id.0));
        self.outbound_edge = Some(plan::EdgeId(provision.outbound.edge_id.0));
        self.outbound_object_allocator = Some(edge_actor::ObjectIdAllocator::new(
            edge_actor::EdgeId(provision.outbound.edge_id.0),
        ));
        self.controller
            .observe(stage::StageEvent::ProvisionStage { from, provision });
    }

    pub fn provision_from_wrong_orchestrator(&mut self, provision: stage::ProvisionStage) {
        self.inbound_edge = Some(plan::EdgeId(provision.inbound.edge_id.0));
        self.outbound_edge = Some(plan::EdgeId(provision.outbound.edge_id.0));
        self.outbound_object_allocator = Some(edge_actor::ObjectIdAllocator::new(
            edge_actor::EdgeId(provision.outbound.edge_id.0),
        ));
        self.controller.observe(stage::StageEvent::ProvisionStage {
            from: stage::NodeId(provision.authorized_orchestrator.0 + 1),
            provision,
        });
    }

    pub fn crash_worker(&mut self) {
        self.controller.observe(stage::StageEvent::WorkerCrashed);
    }

    pub fn mark_ready(&mut self) {
        let inbound_edge = self.inbound_edge.expect("stage must be provisioned first");
        let outbound_edge = self.outbound_edge.expect("stage must be provisioned first");
        self.controller.observe(stage::StageEvent::WorkerReady);
        self.controller.observe(stage::StageEvent::WeightsReady);
        self.controller
            .observe(stage::StageEvent::InboundEdgeReady {
                edge_id: stage::EdgeId(inbound_edge.0),
            });
        self.controller
            .observe(stage::StageEvent::OutboundEdgeReady {
                edge_id: stage::EdgeId(outbound_edge.0),
            });
    }

    pub fn execute_loaded_object(&mut self, object: MockObject) -> Option<MockExecution> {
        let outbound_edge = self.outbound_edge?;
        let command_start = self.controller.commands().len();
        self.controller.observe(stage::StageEvent::ObjectLoaded {
            edge_id: stage::EdgeId(object.edge_id.0),
            object_id: stage::ObjectId(object.object_id),
            sequence: object.sequence,
            handle: stage::DeviceHandle::new_current(object.object_id),
        });
        let step = self.controller.commands()[command_start..]
            .iter()
            .find_map(|command| match command {
                stage::StageCommand::ExecuteStep(step) => Some(step.clone()),
                _ => None,
            })?;
        let output_object_id = self.outbound_object_allocator.as_mut()?.alloc().object_id.0;
        let produced = self.worker.execute(
            &step,
            outbound_edge,
            output_object_id,
            self.stage_index + 1 == self.stage_count,
        );
        self.controller.observe(stage::StageEvent::StepCompleted {
            step_id: step.step_id,
        });
        Some(MockExecution {
            step_id: step.step_id.0,
            produced,
        })
    }

    pub fn stop(&mut self, run_id: plan::RunId) {
        self.controller.observe(stage::StageEvent::StopRun {
            run_id: stage::RunId(run_id.0),
        });
    }

    pub fn drain_lifecycle_events(&mut self) -> Vec<stage::StageLifecycleEvent> {
        let events = self.controller.events()[self.event_cursor..].to_vec();
        self.event_cursor = self.controller.events().len();
        events
    }
}
