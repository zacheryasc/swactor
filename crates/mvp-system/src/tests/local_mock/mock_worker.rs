use mvp_system::run_plan as plan;
use mvp_system::stage_controller as stage;

use super::mock_transport::{MockObject, MockObjectKind};

pub struct MockWorker {
    stage_index: u32,
    eos_after_sequence: u64,
}

impl MockWorker {
    pub fn new(stage_index: u32, eos_after_sequence: u64) -> Self {
        Self {
            stage_index,
            eos_after_sequence,
        }
    }

    pub fn execute(
        &self,
        step: &stage::ExecuteStep,
        outbound_edge: plan::EdgeId,
        is_last_stage: bool,
    ) -> MockObject {
        let sequence = step.input.sequence;
        let object_id = 10_000 + u64::from(self.stage_index) * 1_000 + sequence;
        if is_last_stage {
            MockObject {
                edge_id: outbound_edge,
                object_id,
                sequence,
                kind: MockObjectKind::Token,
                token_id: Some(if sequence >= self.eos_after_sequence {
                    99
                } else {
                    42
                }),
                eos: sequence >= self.eos_after_sequence,
            }
        } else {
            MockObject {
                edge_id: outbound_edge,
                object_id,
                sequence,
                kind: MockObjectKind::Activation,
                token_id: None,
                eos: false,
            }
        }
    }
}
