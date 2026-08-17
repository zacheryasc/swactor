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

pub use fsm::{transition, JobCommand, JobEvent, JobState, TransitionCtx};
pub use model::{ClusterConfig, Job, ProviderConfig, Workspace};
pub use node::{extract_tar, JobPhase, NodeJobActor, ProcessExitRelay};
pub use orchestrator::{pack_workspace, JobDone, OrchestratorJobActor, OrchestratorJobMsg};
pub use wire::{
    register_job_codecs, EDGE_RECORD_SIZE, JobEdgeSink, NodeJobCommand, NodeJobEvent, OutputChunk,
    CHUNK_SIZE, OUTPUTS_EDGE_ID, WORKSPACE_EDGE_ID,
};
