//! Test-only constructors and harness type aliases for the `tests` tree.
//!
//! These helpers previously lived in the production modules
//! (`orchestration/run_fsm`, `staging::control`) but serve only test code.
//! Defining them here keeps them compiled under `#[cfg(test)]` only, so
//! production builds stay free of the dead-code warnings they would otherwise
//! trigger. Inherent methods resolve through their type, so existing call sites
//! such as `fsm::RunPlan::test_linear(..)` and
//! `stage::WeightSource::embedded_gguf(..)` work unchanged.

use crate::run_fsm::{NodeId as FsmNodeId, OrchestratorRun, RunId as FsmRunId, RunPlan, StageRef};
use crate::run_plan::{GgufSource, TokenizerSource};
use crate::staging::{DeviceHandle, StageController, WeightSource};

impl RunPlan {
    /// Build a linear pipeline plan (stage 0 -> stage 1 -> ... -> last stage).
    /// Test-only convenience over the public `RunPlan` fields.
    pub(crate) fn test_linear(run_id: FsmRunId, stages: Vec<StageRef>) -> Self {
        Self { run_id, stages }
    }

    /// Node ids in declared stage order.
    pub(crate) fn stage_nodes(&self) -> Vec<FsmNodeId> {
        self.stages.iter().map(|stage| stage.node_id).collect()
    }
}

impl DeviceHandle {
    /// A handle on the worker's current (first) generation.
    pub(crate) fn new_current(id: u64) -> Self {
        Self { generation: 1, id }
    }
}

impl WeightSource {
    /// Weights carried by an embedded GGUF file with an embedded tokenizer.
    pub(crate) fn embedded_gguf(model_id: impl Into<String>, path: impl Into<String>) -> Self {
        Self::new(
            model_id,
            GgufSource::LocalPath(path.into()),
            TokenizerSource::EmbeddedGguf,
        )
    }
}

/// Test alias for the orchestrator run core, retained for readable test prose.
pub(crate) type OrchestratorHarness = OrchestratorRun;

/// Test alias for the stage controller core, retained for readable test prose.
pub(crate) type StageControllerHarness = StageController;
