//! swactor job runner — drives one command job end to end **through swactor**.
//!
//! Implements the Synaptic Job Runner Specification (spec id 4) as a plugin on
//! swactor's substrate: orchestrator↔node control and bulk transfer (workspace
//! push / output pull) travel as actor messages over the runtime/iroh plane; the
//! node runs `setup`/`run` as supervised processes via `swactor-process`; the
//! exit code is authoritative.

pub mod fsm;
pub mod model;
pub mod node;
pub mod orchestrator;
pub mod wire;

pub use fsm::{JobCommand, JobEvent, JobState, TransitionCtx, transition};
pub use model::{ClusterConfig, Job, ProviderConfig, Workspace};
pub use node::{
    JobDataPlanePort, JobPhase, JobRouteRegistrar, NodeJobActor, ProcessExitRelay, extract_tar,
};
pub use orchestrator::{JobDone, OrchestratorJobActor, OrchestratorJobMsg, pack_workspace};
pub use wire::{
    CHUNK_SIZE, EDGE_RECORD_SIZE, INFERENCE_RESULTS_EDGE_ID, JobEdgeSink, MODEL_WEIGHTS_EDGE_ID,
    NodeJobCommand, NodeJobEvent, OUTPUTS_EDGE_ID, OutputChunk, WORKSPACE_EDGE_ID,
    register_job_codecs,
};
