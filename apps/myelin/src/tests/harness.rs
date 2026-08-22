//! Test-only constructors and harness type aliases for the `tests` tree.
//!
//! These helpers previously lived in the production modules
//! (`orchestration/run_fsm`, `staging::control`) but serve only test code.
//! Defining them here keeps them compiled under `#[cfg(test)]` only, so
//! production builds stay free of the dead-code warnings they would otherwise
//! trigger. Inherent methods resolve through their type, so existing call sites
//! such as `fsm::RunPlan::test_linear(..)` and
//! `stage::WeightSource::embedded_gguf(..)` work unchanged.

use std::time::Duration;

use distribution::node::DistributedNodeConfig;
use iroh::RelayMode;
use iroh_driver::{EDGE_ALPN, IrohDriver, IrohDriverConfig};
use swactor_engine::{Engine, TokioBackend, TokioConfig};

use crate::orchestration::distribution_stack::DistributionRuntimeStack;
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

pub(crate) fn build_iroh_composition(
    poll: Duration,
) -> (Engine, IrohDriver, DistributionRuntimeStack) {
    let (parts, runtime, codec, transport_router) =
        DistributionRuntimeStack::build_runtime(crate::codecs::register_myelin_actor_codecs, None);
    let engine = Engine::new(
        parts,
        TokioBackend::new(TokioConfig::default()).expect("tokio backend"),
    )
    .expect("engine");
    let mut driver = IrohDriver::with_engine(
        engine.handle(),
        IrohDriverConfig {
            secret_key: None,
            relay_mode: RelayMode::Disabled,
            node: DistributedNodeConfig::default(),
            peer_auth: None,
            additional_alpns: vec![EDGE_ALPN.to_vec()],
        },
    )
    .expect("iroh driver");
    let stack = DistributionRuntimeStack::new_from_runtime(
        runtime,
        codec,
        transport_router,
        driver.node_id(),
        DistributedNodeConfig::default(),
        engine.handle(),
    );
    driver.enable_actor_bridge(iroh_driver::ActorBridgeConfig {
        runtime: stack.runtime.clone(),
        codec: stack.codec.clone(),
        routes: stack.actor_bridge_routes(),
        swim: stack.actors.swim,
        relay_mirror: stack.relay_mirror.clone(),
        route_view: stack.route_view.clone(),
        outbox: stack.outbox.clone(),
    });
    stack.spawn_protocol_ticker(poll);
    driver.install_actor_bridge_pump(poll);
    (engine, driver, stack)
}

/// Test alias for the stage controller core, retained for readable test prose.
pub(crate) type StageControllerHarness = StageController;
