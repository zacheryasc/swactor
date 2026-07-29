use crate::run_plan as plan;
use mvp_system::staging as stage;

use super::mock_transport::{MockObject, MockObjectKind};

pub struct MockWorker {
    eos_after_sequence: u64,
}

impl MockWorker {
    pub fn new(_stage_index: u32, eos_after_sequence: u64) -> Self {
        Self { eos_after_sequence }
    }

    pub fn execute(
        &self,
        step: &stage::ExecuteStep,
        outbound_edge: plan::EdgeId,
        object_id: u64,
        is_last_stage: bool,
    ) -> MockObject {
        let sequence = step.input.sequence;
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
