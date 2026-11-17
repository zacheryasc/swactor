//! Shared, typed protocol transition evidence carried by the telemetry byte pipe.
//!
//! Actors keep their own focused lifecycle enums. They mutate authoritative state
//! first, then emit an observational record: neither queue availability nor a
//! subscriber determines protocol progress. This crate is not a state machine.
//!
//! The versioned channel identifies the wire contract. Ordering and retention
//! belong to telemetry; records deliberately contain no parallel clock/sequence.
//! Failure artifact consumers use [`consumer`] to resolve channels and render
//! full correlation identities without adding protocol semantics to telemetry.

use serde::{Deserialize, Serialize};
use swactor::actor::ActorAddress;
use telemetry::{ChannelId, Record, TelemetryProducer};

pub mod consumer;

/// Wire version, unambiguously encoded in [`TransitionRecord::CHANNEL`].
pub const VERSION: u16 = 1;

/// Concrete producing actor (or actor-owned protocol component).
///
/// Component records carry the real owning actor's address, never a fabricated
/// address used as a substitute for actor identity.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActorType {
    ChildDataPlaneSessionActor,
    StreamOpenOperationActor,
    StreamConsumerActor,
    DescriptorOpenOperationActor,
    HostStreamBindingActor,
    HostDataPlaneSessionActor,
    HostBlobBindingActor,
    DestinationBlobTransferActor,
    ArenaAllocatorActor,
    NamespaceControlActor,
    NamespaceOpenLookupActor,
    NamespaceBlobReserveActor,
    NamespaceResolveActor,
    NamespacePublishActor,
    NamespaceBlobReservationReleaseActor,
    NamespaceUnpublishActor,
    NamespaceStreamOpenActor,
    NamespaceStreamCloseActor,
    DataDirectoryActor,
    NamespaceClientActor,
    FileBlobSourceActor,
    ContextualProcessActor,
    BootstrapActor,
    ContextualProcessController,
    ProgramFileTransfer,
    ProgramNamespaceResolver,
    ContextualEventDelivery,
    EdgeEstablisher,
    ClusterDriver,
    IdempotentEffectExecutor,
    BootstrapOutputActor,
    ProcessExitActor,
    DockerWaitActor,
    SshBootstrapActor,
    ArtifactBootstrapActor,
    VastAiProviderMonitorActor,
    /// Orchestrator-owned manual command, durable node, persistence and effect protocols.
    ManualActorControl,
}

/// Protocol identity; independent of the concrete actor implementing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Protocol {
    HostSession,
    HostStream,
    HostBlob,
    BlobTransfer,
    NamespaceOperation,
    ChildSession,
    StreamOpenOperation,
    StreamConsumer,
    DescriptorOperation,
    NamespaceDirectory,
    NamespaceClient,
    BlobSource,
    ContextualProcess,
    ContextualEventDelivery,
    Provisioning,
    Bootstrap,
    ProvisioningReconciler,
    ProvisioningNode,
    ProvisioningBootstrap,
    ProvisioningExecutor,
    Edge,
    /// Full managed resource lifetime; logical node id/zero LE u64 operation,
    /// provision attempt generation, control incarnation. Running and retryable
    /// cleanup failure are nonterminal; successful resource release ends it.
    ManualNode,
    /// Domain-separated 128-bit command-id digest, no generation, control incarnation.
    ManualCommand,
    /// FIFO persistence generation/zero LE u64 operation, no generation,
    /// control incarnation; queued writes cancelled on persistence failure finish once.
    ManualPersistence,
    /// Logical node id/effect id LE u64 operation, provision attempt generation,
    /// control incarnation; recovery and each provider operation finish independently.
    ManualEffect,
}

/// Shared observation vocabulary, not an actor's authoritative lifecycle state.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Phase {
    Created,
    Idle,
    Pending,
    Queued,
    Resolving,
    Recovering,
    Attaching,
    AwaitingAttachment,
    AwaitingPeer,
    AwaitingTransport,
    AwaitingReadiness,
    Awaiting,
    AwaitingReply,
    Allocating,
    Opening,
    Forwarding,
    Open,
    Running,
    Filling,
    Transferring,
    Sealing,
    Publishing,
    Ready,
    Closing,
    Stopping,
    Releasing,
    Aborting,
    Closed,
    Finished,
    Completed,
    Failed,
    Revoked,
    Backoff,
    Redeploying,
}

/// Static trigger codes: emission never formats a per-transition reason string.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    Started,
    Requested,
    Discovered,
    NamespaceResolved,
    PeerMatched,
    PeerOffered,
    PeerAcknowledged,
    TransportInstalled,
    TransportReady,
    OpenRequested,
    Opened,
    GrantReceived,
    Forwarded,
    Attached,
    AttachmentFailed,
    Allocated,
    Ready,
    CloseRequested,
    HostClosed,
    ReleaseRequested,
    Released,
    Completed,
    Cancelled,
    Failed,
    RouteLost,
    RouteRestored,
    PeerTerminated,
    ParentStopped,
    Displaced,
    Stopped,
    Shutdown,
    Retired,
    Retry,
    Timeout,
    Duplicate,
    StaleIdentity,
    Restarted,
}

/// Terminal result of the observed operation or lifecycle.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Failed,
    Cancelled,
    TimedOut,
    Stale,
    PeerLost,
    Replaced,
}

/// Bounded typed evidence correlated with an actual actor and optional operation.
///
/// `generation` identifies a namespace/session/resource revision when relevant;
/// `incarnation` distinguishes restarted stream/transport lifetimes. The meaning
/// of these identities is owned by the named protocol, not the telemetry pipe.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransitionRecord {
    pub actor_type: ActorType,
    pub actor: ActorAddress,
    pub protocol: Protocol,
    pub operation: Option<[u8; 16]>,
    pub generation: Option<u64>,
    pub incarnation: Option<u64>,
    pub previous: Phase,
    pub next: Phase,
    pub reason: Reason,
    pub outcome: Option<Outcome>,
}

impl Record for TransitionRecord {
    const CHANNEL: &'static str = "protocol.transitions.v1";
}

/// Cloneable, best-effort observer; contains no task capability or protocol state.
#[derive(Clone)]
pub struct TransitionEmitter {
    producer: TelemetryProducer,
    channel: ChannelId,
}

impl TransitionEmitter {
    /// Register the shared record channel on a node's existing producer endpoint.
    /// Repeated registrations on the same endpoint reuse the channel id.
    pub fn register(producer: &TelemetryProducer) -> Self {
        Self {
            producer: producer.clone(),
            channel: producer.register_record::<TransitionRecord>(),
        }
    }

    /// Submit observational evidence after changing the actor's own phase.
    ///
    /// There is intentionally no delivery result to use as a correctness gate.
    /// A full mux drops the record without affecting the caller. The fixed record
    /// schema can always serialize; allocation/order/retention use the byte pipe.
    pub fn emit(&self, record: TransitionRecord) {
        let _ = self.producer.submit_record(self.channel, &record);
    }
}

impl std::fmt::Debug for TransitionEmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TransitionEmitter")
            .field("stream", self.producer.stream_id())
            .field("channel", &self.channel)
            .finish_non_exhaustive()
    }
}
