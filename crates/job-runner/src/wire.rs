//! Orchestrator↔node wire protocol — spec §6 command/event tables, plus the
//! bulk-transfer frames that carry workspace/output bytes over the same actor
//! plane (the shared iroh endpoint's actor ALPN).
//!
//! These are `NetworkMessage`s: they cross runtime boundaries (local mailbox or
use crate::orchestrator::OrchestratorJobMsg;

use std::collections::BTreeMap;
use swactor::actor::ActorAddress;

use serde::{Deserialize, Serialize};
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

/// Maximum payload bytes per chunk (workspace/output bytes are streamed).
pub const CHUNK_SIZE: usize = 64 * 1024;

/// Maximum bytes per EDGE_ALPN byte record when shipping bulk tars. Keeping
/// records modest bounds the per-write time over a relay so the edge send pump's
/// write watchdog never trips on a large workspace/output transfer.
pub const EDGE_RECORD_SIZE: usize = 1024 * 1024;

/// Logical edge-stream id for the orchestrator→worker workspace tar, carried on
/// the EDGE_ALPN byte transport instead of the actor plane. Public so the
/// substrate-agnostic job-runner crate and the iroh integration layer agree.
pub const WORKSPACE_EDGE_ID: u64 = 1;

/// Logical edge-stream id for the worker→orchestrator outputs tar.
pub const OUTPUTS_EDGE_ID: u64 = 2;

/// Logical edge-stream id for model weights sent into a running job.
pub const MODEL_WEIGHTS_EDGE_ID: u64 = 3;

/// Logical edge-stream id for inference results emitted by a running job.
pub const INFERENCE_RESULTS_EDGE_ID: u64 = 4;

/// Substrate-agnostic bulk byte sink for the EDGE_ALPN path. The integration
/// layer (e.g. `job_deploy`) implements this around the real iroh edge handle;
/// the job-runner crate stays free of any iroh dependency. When a node actor is
/// built without a sink (`None`), it falls back to streaming bytes as actor
/// `OutputChunk` messages — the path the in-process tests exercise.
pub trait JobEdgeSink: Send + Sync + 'static {
    /// Enqueue one byte record. Send all records, then drop the sink so the
    /// underlying transport finishes the stream and the receiver observes
    /// end-of-stream.
    fn send_bytes(&self, bytes: Vec<u8>) -> Result<(), String>;
}

/// Orchestrator → node commands. Spec §6 command table + workspace chunks.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeJobCommand {
    /// Pin the controller's reverse route before lifecycle reports are sent.
    RegisterController {
        controller: ActorAddress,
        node: [u8; 32],
    },
    /// Bind an embedded node job actor to one orchestrator and configure its
    /// application-owned data-plane bridge before lifecycle commands arrive.
    Assign {
        job_id: u64,
        orchestrator: ActorAddress,
        result_peer: Option<String>,
    },
    /// Application-owned data-plane setup completion delivered back to the node
    /// actor from its engine blocking-I/O owner.
    #[doc(hidden)]
    DataPlaneConfigured {
        job_id: u64,
        env: BTreeMap<String, String>,
        error: Option<String>,
    },
    /// Engine-owned retry that keeps announcing assignment until the
    /// orchestrator proves the reverse actor route by sending lifecycle work.
    #[doc(hidden)]
    CheckAssignment {
        job_id: u64,
    },
    MaterializeWorkspace {
        job_id: u64,
    },
    /// One chunk of the workspace tar stream. `eof` marks the final chunk; the
    /// node extracts the accumulated tar on the eof chunk.
    WorkspaceChunk {
        job_id: u64,
        seq: u64,
        data: Vec<u8>,
        eof: bool,
    },
    RunSetup {
        job_id: u64,
        command: String,
        env: BTreeMap<String, String>,
    },
    RunJob {
        job_id: u64,
        command: String,
        env: BTreeMap<String, String>,
    },
    CollectOutputs {
        job_id: u64,
        outputs: Vec<String>,
    },
    /// Engine-owned timer observation used while an edge workspace transfer is
    /// pending. `job_id` rejects stale timer delivery.
    #[doc(hidden)]
    CheckWorkspaceReady {
        job_id: u64,
    },
    /// Engine-owned timer observation used while an edge output sink is
    /// pending. `job_id` rejects stale timer delivery.
    #[doc(hidden)]
    CheckOutputSink {
        job_id: u64,
    },
}

impl NetworkMessage for NodeJobCommand {
    fn type_tag() -> &'static str {
        "job_runner::NodeJobCommand"
    }
}

/// Node → orchestrator events. Spec §6 event table.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeJobEvent {
    Assigned { job_id: u64 },
    WorkspaceMaterialized { job_id: u64 },
    SetupCompleted { job_id: u64 },
    JobExited { job_id: u64, code: i32 },
    OutputsCollected { job_id: u64 },
    NodeFault { job_id: u64, reason: String },
}

impl NetworkMessage for NodeJobEvent {
    fn type_tag() -> &'static str {
        "job_runner::NodeJobEvent"
    }
}

/// Node → orchestrator output-byte chunk. Separate from lifecycle events so the
/// orchestrator can buffer output bytes independently of the FSM.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputChunk {
    pub job_id: u64,
    pub name: String,
    pub seq: u64,
    pub data: Vec<u8>,
    pub eof: bool,
}

impl NetworkMessage for OutputChunk {
    fn type_tag() -> &'static str {
        "job_runner::OutputChunk"
    }
}

/// Register the job wire messages with a codec registry (JSON).
pub fn register_job_codecs(registry: &mut CodecRegistry) {
    registry.register::<NodeJobCommand, JsonCodec<NodeJobCommand>>(JsonCodec::default());
    registry.register::<NodeJobEvent, JsonCodec<NodeJobEvent>>(JsonCodec::default());
    registry.register::<OrchestratorJobMsg, JsonCodec<OrchestratorJobMsg>>(JsonCodec::default());
    registry.register::<OutputChunk, JsonCodec<OutputChunk>>(JsonCodec::default());
}
