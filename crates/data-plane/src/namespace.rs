//! Authoritative virtual blob namespace actor and restart-tolerant client proxy.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor_engine::{ActorTimer, EngineHandle, EngineInstant};
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

use crate::blob_transfer::{BlobTransferEvent, BlobTransferId};
use crate::host::{HostRouteRegistrar, HostRouteWatch};
use crate::namespace_store::{
    MutationReceipt, MutationRejection, MutationRequest, NamespaceStore, NamespaceStoreError,
    PersistedBinding, PersistedMutationResult, PersistedOperation,
};
use crate::path::DataPath;
use crate::protocol::HostStreamIn;
use crate::source::BlobSourceIn;

const RETIREMENT_RETRY_BATCH: usize = 32;

pub use crate::namespace_store::{OperationId, SourceRecovery};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DirectoryRequestId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobBinding {
    pub source: ActorAddress,
    pub source_node: [u8; 32],
    pub owner: Option<ActorAddress>,
    pub length: u64,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    Blob,
    Stream,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceNode {
    pub kind: EntryKind,
    pub revision: u64,
    pub active: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamRole {
    Source,
    Sink,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamIncarnation {
    pub authority_epoch: u64,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMatch {
    pub incarnation: StreamIncarnation,
    pub source: ActorAddress,
    pub source_descriptor: Vec<u8>,
    pub sink_descriptor: Vec<u8>,
    pub sink: ActorAddress,
    pub revision: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NamespaceError {
    PathNotFound(DataPath),
    PathExists(DataPath),
    WrongEntryType {
        path: DataPath,
        expected: EntryKind,
        found: EntryKind,
    },
    PathReplaced(DataPath),
    DuplicateStreamRole {
        path: DataPath,
        role: StreamRole,
    },
    StaleIncarnation {
        path: DataPath,
        incarnation: StreamIncarnation,
    },
    OperationConflict(OperationId),
    StreamActive(DataPath),
    Storage(String),
    SourceRecovery(String),
    DirectoryUnavailable(String),
    Protocol(String),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathNotFound(path) => write!(f, "data path not found: {path}"),
            Self::PathExists(path) => write!(f, "data path already exists: {path}"),
            Self::WrongEntryType {
                path,
                expected,
                found,
            } => write!(
                f,
                "data path {path} has entry kind {found:?}, expected {expected:?}"
            ),
            Self::PathReplaced(path) => {
                write!(f, "pending data path was replaced: {path}")
            }
            Self::DuplicateStreamRole { path, role } => {
                write!(f, "stream path {path} already has a {role:?}")
            }
            Self::StaleIncarnation { path, incarnation } => write!(
                f,
                "stream path {path} no longer names incarnation {}:{}",
                incarnation.authority_epoch, incarnation.revision
            ),
            Self::OperationConflict(operation) => write!(
                f,
                "namespace operation ID {:02x?} was reused for a different request",
                operation.bytes()
            ),
            Self::StreamActive(path) => {
                write!(f, "stream path {path} has active or pending endpoints")
            }
            Self::Storage(reason) => write!(f, "namespace persistence failed: {reason}"),
            Self::SourceRecovery(reason) => write!(f, "namespace source recovery failed: {reason}"),
            Self::DirectoryUnavailable(reason) => {
                write!(f, "namespace directory is unavailable: {reason}")
            }
            Self::Protocol(reason) => write!(f, "namespace protocol failed: {reason}"),
        }
    }
}

impl std::error::Error for NamespaceError {}

impl From<NamespaceStoreError> for NamespaceError {
    fn from(error: NamespaceStoreError) -> Self {
        Self::Storage(error.to_string())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataDirectoryIn {
    Register {
        request_id: DirectoryRequestId,
        path: DataPath,
        source: ActorAddress,
        source_node: [u8; 32],
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
        reservation: Option<OperationId>,
        reply_to: ActorAddress,
    },
    Resolve {
        request_id: DirectoryRequestId,
        path: DataPath,
        reply_to: ActorAddress,
    },
    Lookup {
        request_id: DirectoryRequestId,
        path: DataPath,
        reply_to: ActorAddress,
    },
    ReserveBlob {
        request_id: DirectoryRequestId,
        path: DataPath,
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    ReleaseBlobReservation {
        request_id: DirectoryRequestId,
        path: DataPath,
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    Unregister {
        request_id: DirectoryRequestId,
        path: DataPath,
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    Rename {
        request_id: DirectoryRequestId,
        source: DataPath,
        destination: DataPath,
        replace: bool,
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    OpenStream {
        request_id: DirectoryRequestId,
        path: DataPath,
        role: StreamRole,
        descriptor: Vec<u8>,
        endpoint: ActorAddress,
        replace: bool,
        ensure: bool,
        expected_revision: Option<u64>,
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    CancelStream {
        request_id: DirectoryRequestId,
        path: DataPath,
        operation_id: OperationId,
        reply_to: Option<ActorAddress>,
    },
    CloseStream {
        request_id: DirectoryRequestId,
        path: DataPath,
        incarnation: StreamIncarnation,
        reply_to: ActorAddress,
    },
    SourceRetired {
        source: ActorAddress,
    },
    StreamDisplaced {
        endpoint: ActorAddress,
        incarnation: StreamIncarnation,
    },
    /// Periodic self-tick that re-drives pending retirements. Retire is a
    /// lifecycle one-shot over an at-most-once transport; without traffic
    /// that re-runs [`DataDirectoryActor`]'s retry pass, a single lost frame
    /// strands the retired source and its published binding forever.
    RetryRetirements,
    /// Local observation: re-drive only this still-pending obligation.
    RetirementRouteChanged {
        target: ActorAddress,
    },
}

impl NetworkMessage for DataDirectoryIn {
    fn type_tag() -> &'static str {
        "data-plane.data-directory.in.v1"
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DataDirectoryOut {
    Registered {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<MutationReceipt, NamespaceError>,
    },
    Resolved {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<BlobBinding, NamespaceError>,
    },
    LookedUp {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<NamespaceNode, NamespaceError>,
    },
    BlobReserved {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<(), NamespaceError>,
    },
    BlobReservationReleased {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<(), NamespaceError>,
    },
    Unregistered {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<MutationReceipt, NamespaceError>,
    },
    Renamed {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<MutationReceipt, NamespaceError>,
    },
    StreamOpened {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<StreamMatch, NamespaceError>,
    },
    StreamClosed {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<(), NamespaceError>,
    },
    StreamCancelled {
        request_id: DirectoryRequestId,
        authority_epoch: u64,
        result: Result<(), NamespaceError>,
    },
}
impl DataDirectoryOut {
    pub fn request_id(&self) -> DirectoryRequestId {
        match self {
            Self::Registered { request_id, .. }
            | Self::Resolved { request_id, .. }
            | Self::LookedUp { request_id, .. }
            | Self::BlobReserved { request_id, .. }
            | Self::BlobReservationReleased { request_id, .. }
            | Self::Unregistered { request_id, .. }
            | Self::Renamed { request_id, .. }
            | Self::StreamOpened { request_id, .. }
            | Self::StreamClosed { request_id, .. }
            | Self::StreamCancelled { request_id, .. } => *request_id,
        }
    }

    fn authority_epoch(&self) -> u64 {
        match self {
            Self::Registered {
                authority_epoch, ..
            }
            | Self::Resolved {
                authority_epoch, ..
            }
            | Self::LookedUp {
                authority_epoch, ..
            }
            | Self::BlobReserved {
                authority_epoch, ..
            }
            | Self::BlobReservationReleased {
                authority_epoch, ..
            }
            | Self::Unregistered {
                authority_epoch, ..
            }
            | Self::Renamed {
                authority_epoch, ..
            }
            | Self::StreamOpened {
                authority_epoch, ..
            }
            | Self::StreamClosed {
                authority_epoch, ..
            }
            | Self::StreamCancelled {
                authority_epoch, ..
            } => *authority_epoch,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NamespaceRequest {
    Register {
        path: DataPath,
        source: ActorAddress,
        source_node: [u8; 32],
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
        reservation: Option<OperationId>,
    },
    Resolve {
        path: DataPath,
    },
    Lookup {
        path: DataPath,
    },
    ReserveBlob {
        path: DataPath,
        operation_id: OperationId,
    },
    ReleaseBlobReservation {
        path: DataPath,
        operation_id: OperationId,
    },
    Unregister {
        path: DataPath,
        operation_id: OperationId,
    },
    Rename {
        source: DataPath,
        destination: DataPath,
        replace: bool,
        operation_id: OperationId,
    },
    OpenStream {
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        descriptor: Vec<u8>,
        replace: bool,
        ensure: bool,
        expected_revision: Option<u64>,
        operation_id: OperationId,
    },
    CancelStream {
        path: DataPath,
        operation_id: OperationId,
    },
    CloseStream {
        path: DataPath,
        incarnation: StreamIncarnation,
    },
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NamespaceClientIn {
    Request {
        request: NamespaceRequest,
        reply_to: ActorAddress,
    },
    Cancel {
        reply_to: ActorAddress,
    },
    CancelBlobReservation {
        path: DataPath,
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    DirectoryReply(DataDirectoryOut),
    TransferFailed {
        destination: ActorAddress,
        transfer_id: BlobTransferId,
        reason: String,
    },
    Retry,
}

impl NetworkMessage for NamespaceClientIn {
    fn type_tag() -> &'static str {
        "data-plane.namespace-client.in.v1"
    }
}

#[derive(Clone, Debug)]
enum RuntimeSource {
    Available {
        actor: ActorAddress,
        node: [u8; 32],
        owner: Option<ActorAddress>,
    },
    Unavailable(String),
}

struct PendingStream {
    role: StreamRole,
    endpoint: ActorAddress,
    operation_id: OperationId,
    request_id: DirectoryRequestId,
    descriptor: Vec<u8>,
    reply_to: ActorAddress,
    revision: u64,
    opened_from_revision: Option<u64>,
}

struct StreamOpenRequest {
    request_id: DirectoryRequestId,
    path: DataPath,
    role: StreamRole,
    endpoint: ActorAddress,
    replace: bool,
    ensure: bool,
    expected_revision: Option<u64>,
    descriptor: Vec<u8>,
    operation_id: OperationId,
    reply_to: ActorAddress,
}

struct StreamReplacementFence {
    incarnation: StreamIncarnation,
    endpoints: HashSet<ActorAddress>,
    old: Option<ActiveStream>,
    requests: Vec<StreamOpenRequest>,
}
struct BlobRegistration {
    path: DataPath,
    source: ActorAddress,
    source_node: [u8; 32],
    length: u64,
    recovery: SourceRecovery,
    operation_id: OperationId,
    reservation: Option<OperationId>,
    retired: Option<ActorAddress>,
}

struct ActiveStream {
    binding: StreamMatch,
    source_operation: OperationId,
    sink_operation: OperationId,
}

enum RuntimeStream {
    Pending(PendingStream),
    Active(ActiveStream),
}

struct PendingStreamDisplacement {
    incarnation: StreamIncarnation,
    // Once the endpoint acknowledges it may stop, so only durability remains
    // retryable. Keep the acknowledgement until that commit succeeds.
    acknowledged: bool,
}

impl PendingStreamDisplacement {
    /// Returns true only when the acknowledged obligation is durably complete.
    fn retry(&self, ctx: &Ctx<'_>, endpoint: ActorAddress, store: &mut NamespaceStore) -> bool {
        if !self.acknowledged {
            let _ = ctx.send(
                endpoint,
                HostStreamIn::Displaced {
                    incarnation: self.incarnation,
                    reply_to: Some(ctx.self_addr()),
                },
            );
            return false;
        }
        let mut next = store.snapshot().clone();
        next.stream_retirements
            .retain(|(known, _)| *known != endpoint);
        if let Err(error) = store.commit(next) {
            eprintln!("data-directory: retiring stream displacement failed: {error}");
            return false;
        }
        true
    }
}

pub struct DataDirectoryActor {
    store: NamespaceStore,
    sources: BTreeMap<DataPath, RuntimeSource>,
    streams: BTreeMap<DataPath, RuntimeStream>,
    blob_reservations: BTreeMap<DataPath, OperationId>,
    pending_retirements: HashSet<ActorAddress>,
    pending_stream_displacements: HashMap<ActorAddress, PendingStreamDisplacement>,
    stream_replacement_fences: BTreeMap<DataPath, StreamReplacementFence>,
    authority_epoch: u64,
    retire_retry: Option<RetirementRetry>,
    retirement_watches: HashMap<ActorAddress, HostRouteWatch>,
    retirement_retry_queue: VecDeque<ActorAddress>,
    retirement_timer: Option<ActorTimer>,
    #[cfg(feature = "directory-trace")]
    trace: DirectoryTrace,
}

/// Load-diagnostic counters for the directory actor (compiled in only with
/// the `directory-trace` feature). Production builds carry no cost.
#[cfg(feature = "directory-trace")]
pub struct DirectoryTrace {
    messages: u64,
    retire_fans: u64,
    last_report: std::time::Instant,
    last_commits: u64,
}

#[cfg(feature = "directory-trace")]
impl DirectoryTrace {
    fn emit(line: String) {
        use std::io::Write;
        eprintln!("{line}");
        if let Ok(mut file) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open("target/dir-trace.log")
        {
            let _ = writeln!(file, "{line}");
        }
    }
    fn new() -> Self {
        Self {
            messages: 0,
            retire_fans: 0,
            last_report: std::time::Instant::now(),
            last_commits: crate::namespace_store::trace_commit_count(),
        }
    }

    /// Aggregate one message-handling step; print a summary every 2 seconds.
    fn observed_step(&mut self, pending_retirements: usize, store: &NamespaceStore) {
        self.messages += 1;
        if self.last_report.elapsed() >= Duration::from_secs(2) {
            let snapshot = store.snapshot();
            let commits = crate::namespace_store::trace_commit_count();
            let elapsed = self.last_report.elapsed().as_secs_f64().max(0.001);
            Self::emit(format!(
                "[dir-trace] msgs={} ({:.0}/s) retire_fans={} ({:.0}/s) pending_retire={} \
                 persisted_retire={} ops={} bindings={} stream_nodes={} commits={} ({:.0}/s) \
                 avg_commit={:.1}ms worst_commit={:.1}ms",
                self.messages,
                self.messages as f64 / elapsed,
                self.retire_fans,
                self.retire_fans as f64 / elapsed,
                pending_retirements,
                snapshot.retirements.len(),
                snapshot.operations.len(),
                snapshot.bindings.len(),
                snapshot.stream_nodes.len(),
                commits,
                (commits - self.last_commits) as f64 / elapsed,
                crate::namespace_store::trace_commit_avg_ms(),
                crate::namespace_store::trace_commit_worst_ms(),
            ));
            self.last_commits = commits;
            self.last_report = std::time::Instant::now();
        }
    }
}

/// Engine-hosted periodic tick that re-sends pending `Retire` messages.
///
/// Retire frames are fire-and-forget: a write onto a connection that dies
/// mid-flight is dropped without feedback. The tick is the only periodic
/// re-drive of pending retirements: re-fanning on every inbound message
/// would couple the outbound Retire rate to the cluster's request rate, and
/// a loaded directory multiplied each unacknowledged retirement into a
/// self-sustaining frame storm. The tick keeps the retry pass running at a
/// fixed cadence until each retirement is acknowledged; a fresh retirement
/// is also sent immediately at enqueue time.
pub struct RetirementRetry {
    engine: EngineHandle,
    sender: ExternalSender,
    period: Duration,
    routes: Option<Arc<dyn HostRouteRegistrar>>,
}

impl RetirementRetry {
    pub fn new(
        engine: EngineHandle,
        sender: ExternalSender,
        period: Duration,
        routes: Option<Arc<dyn HostRouteRegistrar>>,
    ) -> Self {
        Self {
            engine,
            sender,
            period,
            routes,
        }
    }
}

impl DataDirectoryActor {
    pub fn recover(
        store_path: impl AsRef<Path>,
        retire_retry: Option<RetirementRetry>,
        mut recover_source: impl FnMut(
            &SourceRecovery,
            u64,
        ) -> Result<(ActorAddress, [u8; 32]), NamespaceError>,
    ) -> Result<Self, NamespaceError> {
        let mut store = NamespaceStore::open(store_path)?;
        let bindings = store.snapshot().bindings.clone();
        let mut sources = BTreeMap::new();
        for (path, binding) in bindings {
            let owner = match &binding.recovery {
                SourceRecovery::Actor { owner, .. } => *owner,
                SourceRecovery::File { .. } => None,
            };
            let source = match recover_source(&binding.recovery, binding.length) {
                Ok((actor, node)) => RuntimeSource::Available { actor, node, owner },
                Err(error) => RuntimeSource::Unavailable(error.to_string()),
            };
            sources.insert(path, source);
        }
        let pending_retirements: HashSet<_> =
            store.snapshot().retirements.iter().copied().collect();
        let retirement_retry_queue = pending_retirements.iter().copied().collect();
        let pending_stream_displacements = store
            .snapshot()
            .stream_retirements
            .iter()
            .map(|(endpoint, incarnation)| {
                (
                    *endpoint,
                    PendingStreamDisplacement {
                        incarnation: *incarnation,
                        acknowledged: false,
                    },
                )
            })
            .collect();
        let mut stream_replacement_fences = BTreeMap::new();
        for (endpoint, incarnation) in &store.snapshot().stream_retirements {
            let Some((path, _)) = store
                .snapshot()
                .stream_nodes
                .iter()
                .find(|(_, revision)| **revision == incarnation.revision)
            else {
                continue;
            };
            let fence = stream_replacement_fences
                .entry(path.clone())
                .or_insert_with(|| StreamReplacementFence {
                    incarnation: *incarnation,
                    endpoints: HashSet::new(),
                    old: None,
                    requests: Vec::new(),
                });
            if fence.incarnation == *incarnation {
                fence.endpoints.insert(*endpoint);
            }
        }
        let authority_epoch = store.advance_authority_epoch()?;
        Ok(Self {
            store,
            sources,
            streams: BTreeMap::new(),
            blob_reservations: BTreeMap::new(),
            pending_retirements,
            pending_stream_displacements,
            stream_replacement_fences,
            authority_epoch,
            retire_retry,
            retirement_watches: HashMap::new(),
            retirement_retry_queue,
            retirement_timer: None,
            #[cfg(feature = "directory-trace")]
            trace: DirectoryTrace::new(),
        })
    }

    fn watch_retirement(&mut self, ctx: &Ctx<'_>, target: ActorAddress) {
        if self.retirement_watches.contains_key(&target) {
            return;
        }
        let Some(retry) = &self.retire_retry else {
            return;
        };
        let Some(routes) = &retry.routes else {
            return;
        };
        let sender = retry.sender.clone();
        let directory = ctx.self_addr();
        if let Some(watch) = routes.watch_route(
            target,
            Arc::new(move || {
                let _ = sender.send_to(
                    directory,
                    DataDirectoryIn::RetirementRouteChanged { target },
                );
            }),
        ) {
            self.retirement_watches.insert(target, watch);
        }
    }

    fn retry_retirement(&mut self, ctx: &Ctx<'_>, target: ActorAddress) {
        if self
            .pending_stream_displacements
            .get(&target)
            .is_some_and(|pending| pending.retry(ctx, target, &mut self.store))
        {
            self.pending_stream_displacements.remove(&target);
            self.retirement_watches.remove(&target);
        }
        self.resume_stream_replacement_fences(ctx);
        let routable = self
            .retire_retry
            .as_ref()
            .and_then(|retry| retry.routes.as_deref())
            .is_none_or(|routes| routes.is_routable(target));
        if routable && self.pending_retirements.contains(&target) {
            let _ = ctx.send(
                target,
                BlobSourceIn::Retire {
                    reply_to: Some(ctx.self_addr()),
                },
            );
        }
    }

    fn queue_retirement(&mut self, ctx: &Ctx<'_>, source: ActorAddress) {
        if self.pending_retirements.insert(source) {
            self.retirement_retry_queue.push_back(source);
        }
        self.watch_retirement(ctx, source);
        let _ = ctx.send(
            source,
            BlobSourceIn::Retire {
                reply_to: Some(ctx.self_addr()),
            },
        );
    }

    fn retry_retirements(&mut self, ctx: &Ctx<'_>) {
        let store = &mut self.store;
        let watches = &mut self.retirement_watches;
        self.pending_stream_displacements
            .retain(|endpoint, pending| {
                if !pending.retry(ctx, *endpoint, store) {
                    return true;
                }
                watches.remove(endpoint);
                false
            });
        let routes = self
            .retire_retry
            .as_ref()
            .and_then(|retry| retry.routes.clone());
        let mut examined = 0;
        let mut sent = 0_u64;
        let pending = self.retirement_retry_queue.len();
        while examined < pending && (sent as usize) < RETIREMENT_RETRY_BATCH {
            let source = self
                .retirement_retry_queue
                .pop_front()
                .expect("pending retry count matches queue");
            examined += 1;
            if !self.pending_retirements.contains(&source) {
                continue;
            }
            self.retirement_retry_queue.push_back(source);
            // Preserve the durable retirement tombstone while a route is
            // absent, but do not flood the actor runtime with frames that
            // cannot be delivered. The route watch re-drives this source as
            // soon as it becomes routable again.
            if routes
                .as_deref()
                .is_some_and(|routes| !routes.is_routable(source))
            {
                continue;
            }
            let _ = ctx.send(
                source,
                BlobSourceIn::Retire {
                    reply_to: Some(ctx.self_addr()),
                },
            );
            sent += 1;
        }
        #[cfg(feature = "directory-trace")]
        {
            self.trace.retire_fans += sent;
        }
        self.resume_stream_replacement_fences(ctx);
    }

    fn confirm_retirement(&mut self, source: ActorAddress) -> Result<(), NamespaceError> {
        if !self.pending_retirements.contains(&source) {
            return Ok(());
        }
        let mut next = self.store.snapshot().clone();
        next.retirements.retain(|retired| *retired != source);
        self.store.commit(next)?;
        self.pending_retirements.remove(&source);
        self.retirement_watches.remove(&source);
        Ok(())
    }
    pub fn authority_epoch(&self) -> u64 {
        self.authority_epoch
    }

    fn replay(
        &self,
        operation_id: OperationId,
        request: &MutationRequest,
    ) -> Option<Result<MutationReceipt, NamespaceError>> {
        self.store
            .snapshot()
            .operations
            .get(&operation_id)
            .map(|operation| {
                if &operation.request != request {
                    return Err(NamespaceError::OperationConflict(operation_id));
                }
                match &operation.result {
                    PersistedMutationResult::Committed(receipt) => Ok(*receipt),
                    PersistedMutationResult::Rejected(MutationRejection::PathNotFound(path)) => {
                        Err(NamespaceError::PathNotFound(path.clone()))
                    }
                    PersistedMutationResult::Rejected(MutationRejection::PathExists(path)) => {
                        Err(NamespaceError::PathExists(path.clone()))
                    }
                    PersistedMutationResult::Rejected(MutationRejection::StreamActive(path)) => {
                        Err(NamespaceError::StreamActive(path.clone()))
                    }
                    PersistedMutationResult::Rejected(MutationRejection::PathReplaced(path)) => {
                        Err(NamespaceError::PathReplaced(path.clone()))
                    }
                }
            })
    }

    /// Persist a rejected mutation under its operation ID so later retries of
    /// the same operation replay the rejection instead of re-evaluating
    /// against state that has since changed. Returns the matching error.
    fn reject(
        &mut self,
        operation_id: OperationId,
        request: MutationRequest,
        rejection: MutationRejection,
    ) -> NamespaceError {
        let mut next = self.store.snapshot().clone();
        next.operations.insert(
            operation_id,
            PersistedOperation {
                request,
                result: PersistedMutationResult::Rejected(rejection.clone()),
            },
        );
        if let Err(error) = self.store.commit(next) {
            return error.into();
        }
        match rejection {
            MutationRejection::PathNotFound(path) => NamespaceError::PathNotFound(path),
            MutationRejection::PathExists(path) => NamespaceError::PathExists(path),
            MutationRejection::StreamActive(path) => NamespaceError::StreamActive(path),
            MutationRejection::PathReplaced(path) => NamespaceError::PathReplaced(path),
        }
    }

    fn record_stream_participant(
        &mut self,
        path: &DataPath,
        operation_id: OperationId,
        revision: u64,
    ) -> Result<(), NamespaceError> {
        let request = MutationRequest::BindStream { path: path.clone() };
        if let Some(existing) = self.store.snapshot().operations.get(&operation_id) {
            return if existing.request == request {
                Ok(())
            } else {
                Err(NamespaceError::OperationConflict(operation_id))
            };
        }
        let mut next = self.store.snapshot().clone();
        next.operations.insert(
            operation_id,
            PersistedOperation {
                request,
                result: PersistedMutationResult::Committed(MutationReceipt { revision }),
            },
        );
        self.store.commit(next)?;
        Ok(())
    }
    fn bind_stream(
        &mut self,
        path: DataPath,
        operation_id: OperationId,
        retired: Option<ActorAddress>,
    ) -> Result<MutationReceipt, NamespaceError> {
        let request = MutationRequest::BindStream { path: path.clone() };
        if let Some(replayed) = self.replay(operation_id, &request) {
            return replayed;
        }
        if self.blob_reservations.contains_key(&path) {
            return Err(self.reject(operation_id, request, MutationRejection::PathExists(path)));
        }
        let revision = self.store.snapshot().next_revision;
        let next_revision = revision
            .checked_add(1)
            .filter(|revision| *revision != 0)
            .ok_or(NamespaceStoreError::RevisionExhausted)?;
        let receipt = MutationReceipt { revision };
        let mut next = self.store.snapshot().clone();
        next.next_revision = next_revision;
        next.bindings.remove(&path);
        next.stream_nodes.insert(path.clone(), revision);
        if let Some(retired) = retired
            && !next.retirements.contains(&retired)
        {
            next.retirements.push(retired);
        }
        next.operations.insert(
            operation_id,
            PersistedOperation {
                request,
                result: PersistedMutationResult::Committed(receipt),
            },
        );
        self.store.commit(next)?;
        self.sources.remove(&path);
        Ok(receipt)
    }

    fn send_stream_result(
        &self,
        ctx: &Ctx<'_>,
        request_id: DirectoryRequestId,
        reply_to: ActorAddress,
        result: Result<StreamMatch, NamespaceError>,
    ) {
        let _ = ctx.send(
            reply_to,
            NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamOpened {
                request_id,
                authority_epoch: self.authority_epoch,
                result,
            }),
        );
    }

    fn displace_stream(&mut self, ctx: &Ctx<'_>, path: &DataPath) {
        match self.streams.remove(path) {
            Some(RuntimeStream::Pending(pending)) => {
                self.send_stream_result(
                    ctx,
                    pending.request_id,
                    pending.reply_to,
                    Err(NamespaceError::PathReplaced(path.clone())),
                );
            }
            Some(RuntimeStream::Active(active)) => {
                // A live generation must be fenced: its endpoints keep a
                // working transport, so removing the runtime entry alone
                // would let displaced writers and readers continue. Fan a
                // displacement notice to both endpoints and keep re-fanning
                // until each acknowledges.
                let incarnation = active.binding.incarnation;
                for endpoint in [active.binding.source, active.binding.sink] {
                    if let Err(error) = self.queue_stream_displacement(ctx, endpoint, incarnation) {
                        eprintln!("data-directory: persisting stream displacement failed: {error}");
                    }
                }
            }
            None => {}
        }
    }

    fn queue_stream_displacement(
        &mut self,
        ctx: &Ctx<'_>,
        endpoint: ActorAddress,
        incarnation: StreamIncarnation,
    ) -> Result<(), NamespaceError> {
        if self
            .pending_stream_displacements
            .get(&endpoint)
            .is_some_and(|known| known.incarnation == incarnation)
        {
            return Ok(());
        }
        let mut next = self.store.snapshot().clone();
        next.stream_retirements
            .retain(|(known, _)| *known != endpoint);
        next.stream_retirements.push((endpoint, incarnation));
        self.store.commit(next)?;
        self.pending_stream_displacements.insert(
            endpoint,
            PendingStreamDisplacement {
                incarnation,
                acknowledged: false,
            },
        );
        self.watch_retirement(ctx, endpoint);
        let _ = ctx.send(
            endpoint,
            HostStreamIn::Displaced {
                incarnation,
                reply_to: Some(ctx.self_addr()),
            },
        );
        Ok(())
    }

    fn begin_stream_replacement(
        &mut self,
        ctx: &Ctx<'_>,
        request: StreamOpenRequest,
        active: ActiveStream,
    ) {
        let path = request.path.clone();
        let incarnation = active.binding.incarnation;
        let endpoints = HashSet::from([active.binding.source, active.binding.sink]);
        let mut next = self.store.snapshot().clone();
        for endpoint in &endpoints {
            next.stream_retirements
                .retain(|(known, _)| known != endpoint);
            next.stream_retirements.push((*endpoint, incarnation));
        }
        if let Err(error) = self.store.commit(next) {
            self.streams.insert(path, RuntimeStream::Active(active));
            self.send_stream_result(ctx, request.request_id, request.reply_to, Err(error.into()));
            return;
        }
        for endpoint in &endpoints {
            self.pending_stream_displacements.insert(
                *endpoint,
                PendingStreamDisplacement {
                    incarnation,
                    acknowledged: false,
                },
            );
            self.watch_retirement(ctx, *endpoint);
        }
        self.stream_replacement_fences.insert(
            path,
            StreamReplacementFence {
                incarnation,
                endpoints: endpoints.clone(),
                old: Some(active),
                requests: vec![request],
            },
        );
        for endpoint in endpoints {
            let _ = ctx.send(
                endpoint,
                HostStreamIn::Displaced {
                    incarnation,
                    reply_to: Some(ctx.self_addr()),
                },
            );
        }
    }

    fn resume_stream_replacement_fences(&mut self, ctx: &Ctx<'_>) {
        let ready: Vec<_> = self
            .stream_replacement_fences
            .iter()
            .filter(|(_, fence)| {
                fence.endpoints.iter().all(|endpoint| {
                    self.pending_stream_displacements
                        .get(endpoint)
                        .is_none_or(|pending| pending.incarnation != fence.incarnation)
                })
            })
            .map(|(path, _)| path.clone())
            .collect();
        for path in ready {
            if let Some(fence) = self.stream_replacement_fences.remove(&path) {
                for request in fence.requests {
                    self.open_stream(ctx, request);
                }
            }
        }
    }

    fn hold_stream_open_for_fence(&mut self, ctx: &Ctx<'_>, request: StreamOpenRequest) {
        let old_reply = self
            .stream_replacement_fences
            .get(&request.path)
            .and_then(|fence| fence.old.as_ref())
            .and_then(|old| {
                let belongs_to_old = request.operation_id == old.source_operation
                    || request.operation_id == old.sink_operation;
                belongs_to_old.then(|| {
                    let expected = match request.role {
                        StreamRole::Source => (old.source_operation, old.binding.source),
                        StreamRole::Sink => (old.sink_operation, old.binding.sink),
                    };
                    if expected == (request.operation_id, request.endpoint) {
                        Ok(old.binding.clone())
                    } else {
                        Err(NamespaceError::OperationConflict(request.operation_id))
                    }
                })
            });
        if let Some(result) = old_reply {
            self.send_stream_result(ctx, request.request_id, request.reply_to, result);
            return;
        }
        let fence = self
            .stream_replacement_fences
            .get_mut(&request.path)
            .expect("replacement fence checked");
        if let Some(known) = fence.requests.iter_mut().find(|known| {
            known.operation_id == request.operation_id
                && known.role == request.role
                && known.endpoint == request.endpoint
        }) {
            *known = request;
        } else {
            fence.requests.push(request);
        }
    }

    fn confirm_stream_displacement(
        &mut self,
        ctx: &Ctx<'_>,
        endpoint: ActorAddress,
        incarnation: StreamIncarnation,
    ) {
        let Some(pending) = self.pending_stream_displacements.get_mut(&endpoint) else {
            return;
        };
        if pending.incarnation != incarnation {
            return;
        }
        pending.acknowledged = true;
        self.retry_retirement(ctx, endpoint);
    }

    fn open_stream(&mut self, ctx: &Ctx<'_>, request: StreamOpenRequest) {
        if self.stream_replacement_fences.contains_key(&request.path) {
            self.hold_stream_open_for_fence(ctx, request);
            return;
        }
        let StreamOpenRequest {
            request_id,
            path,
            role,
            endpoint,
            replace,
            ensure,
            expected_revision,
            descriptor,
            operation_id,
            reply_to,
        } = request;
        if let Some(replayed) = self.replay(
            operation_id,
            &MutationRequest::BindStream { path: path.clone() },
        ) {
            let receipt = match replayed {
                // A cancelled open replays its persisted rejection: the
                // cancel tombstone must win over a reordered late retry.
                Err(error) => {
                    self.send_stream_result(ctx, request_id, reply_to, Err(error));
                    return;
                }
                Ok(receipt) => receipt,
            };
            let is_current = matches!(
                self.streams.get(&path),
                Some(RuntimeStream::Pending(pending))
                    if pending.operation_id == operation_id
            ) || matches!(
                self.streams.get(&path),
                Some(RuntimeStream::Active(active))
                    if active.source_operation == operation_id
                        || active.sink_operation == operation_id
            );
            if !is_current {
                self.send_stream_result(
                    ctx,
                    request_id,
                    reply_to,
                    Err(NamespaceError::StaleIncarnation {
                        path,
                        incarnation: StreamIncarnation {
                            authority_epoch: self.authority_epoch,
                            revision: receipt.revision,
                        },
                    }),
                );
                return;
            }
        }
        if !ensure {
            let snapshot = self.store.snapshot();
            let Some(current_revision) = snapshot.stream_nodes.get(&path).copied() else {
                let error = if snapshot.bindings.contains_key(&path) {
                    NamespaceError::WrongEntryType {
                        path,
                        expected: EntryKind::Stream,
                        found: EntryKind::Blob,
                    }
                } else {
                    NamespaceError::PathNotFound(path)
                };
                self.send_stream_result(ctx, request_id, reply_to, Err(error));
                return;
            };
            let pending_from_expected = matches!(
                self.streams.get(&path),
                Some(RuntimeStream::Pending(pending))
                    if pending.opened_from_revision == expected_revision
            );
            if expected_revision != Some(current_revision) && !pending_from_expected {
                self.send_stream_result(
                    ctx,
                    request_id,
                    reply_to,
                    Err(NamespaceError::PathReplaced(path)),
                );
                return;
            }
        }
        if let Some(RuntimeStream::Pending(pending)) = self.streams.get_mut(&path)
            && pending.operation_id == operation_id
        {
            if pending.role != role || pending.endpoint != endpoint {
                self.send_stream_result(
                    ctx,
                    request_id,
                    reply_to,
                    Err(NamespaceError::OperationConflict(operation_id)),
                );
            } else {
                pending.request_id = request_id;
                pending.reply_to = reply_to;
            }
            return;
        }
        if let Some(RuntimeStream::Active(active)) = self.streams.get(&path)
            && (active.source_operation == operation_id || active.sink_operation == operation_id)
        {
            self.send_stream_result(ctx, request_id, reply_to, Ok(active.binding.clone()));
            return;
        }

        if replace && matches!(self.streams.get(&path), Some(RuntimeStream::Active(_))) {
            let Some(RuntimeStream::Active(active)) = self.streams.remove(&path) else {
                unreachable!("active stream checked");
            };
            self.begin_stream_replacement(
                ctx,
                StreamOpenRequest {
                    request_id,
                    path,
                    role,
                    endpoint,
                    replace,
                    ensure,
                    expected_revision,
                    descriptor,
                    operation_id,
                    reply_to,
                },
                active,
            );
            return;
        }
        let compatible_pending = matches!(
            self.streams.get(&path),
            Some(RuntimeStream::Pending(pending)) if pending.role != role
        );
        if replace && self.streams.contains_key(&path) && !compatible_pending {
            self.displace_stream(ctx, &path);
        }

        if matches!(self.streams.get(&path), Some(RuntimeStream::Pending(_))) {
            let Some(RuntimeStream::Pending(pending)) = self.streams.remove(&path) else {
                unreachable!("pending stream checked");
            };
            if pending.role == role {
                self.streams
                    .insert(path.clone(), RuntimeStream::Pending(pending));
                self.send_stream_result(
                    ctx,
                    request_id,
                    reply_to,
                    Err(NamespaceError::DuplicateStreamRole { path, role }),
                );
                return;
            }
            if let Err(error) =
                self.record_stream_participant(&path, operation_id, pending.revision)
            {
                self.streams
                    .insert(path.clone(), RuntimeStream::Pending(pending));
                self.send_stream_result(ctx, request_id, reply_to, Err(error));
                return;
            }
            let (
                source,
                sink,
                source_descriptor,
                sink_descriptor,
                source_operation,
                sink_operation,
            ) = match role {
                StreamRole::Source => (
                    endpoint,
                    pending.endpoint,
                    descriptor,
                    pending.descriptor,
                    operation_id,
                    pending.operation_id,
                ),
                StreamRole::Sink => (
                    pending.endpoint,
                    endpoint,
                    pending.descriptor,
                    descriptor,
                    pending.operation_id,
                    operation_id,
                ),
            };
            let binding = StreamMatch {
                incarnation: StreamIncarnation {
                    authority_epoch: self.authority_epoch,
                    revision: pending.revision,
                },
                source,
                source_descriptor,
                sink_descriptor,
                sink,
                revision: pending.revision,
            };
            self.send_stream_result(
                ctx,
                pending.request_id,
                pending.reply_to,
                Ok(binding.clone()),
            );
            self.send_stream_result(ctx, request_id, reply_to, Ok(binding.clone()));
            self.streams.insert(
                path,
                RuntimeStream::Active(ActiveStream {
                    binding,
                    source_operation,
                    sink_operation,
                }),
            );
            return;
        }

        if self.streams.contains_key(&path) {
            self.send_stream_result(
                ctx,
                request_id,
                reply_to,
                Err(NamespaceError::DuplicateStreamRole { path, role }),
            );
            return;
        }

        if self.store.snapshot().bindings.contains_key(&path) && !replace {
            self.send_stream_result(
                ctx,
                request_id,
                reply_to,
                Err(NamespaceError::WrongEntryType {
                    path,
                    expected: EntryKind::Stream,
                    found: EntryKind::Blob,
                }),
            );
            return;
        }
        let retired = self.sources.get(&path).and_then(|source| match source {
            RuntimeSource::Available { actor, .. } => Some(*actor),
            RuntimeSource::Unavailable(_) => None,
        });
        match self.bind_stream(path.clone(), operation_id, retired) {
            Ok(receipt) => {
                if let Some(retired) = retired {
                    self.queue_retirement(ctx, retired);
                }
                self.streams.insert(
                    path,
                    RuntimeStream::Pending(PendingStream {
                        role,
                        descriptor,
                        endpoint,
                        operation_id,
                        request_id,
                        reply_to,
                        revision: receipt.revision,
                        opened_from_revision: (!ensure).then_some(expected_revision).flatten(),
                    }),
                );
            }
            Err(error) => self.send_stream_result(ctx, request_id, reply_to, Err(error)),
        }
    }

    /// Retract a waiting stream open. Cancels ride the same at-most-once
    /// transport as every other directory frame, so callers re-send them
    /// until acknowledged, and the cancel must also survive reordering
    /// against the open itself: when the open frame was lost and lands only
    /// after this cancel, the persisted rejection below turns its replay
    /// into a typed failure instead of resurrecting a pending endpoint that
    /// no live actor will ever close.
    fn cancel_stream(
        &mut self,
        path: &DataPath,
        operation_id: OperationId,
    ) -> Result<(), NamespaceError> {
        if !self.store.snapshot().operations.contains_key(&operation_id) {
            let request = MutationRequest::BindStream { path: path.clone() };
            let mut next = self.store.snapshot().clone();
            next.operations.insert(
                operation_id,
                PersistedOperation {
                    request,
                    result: PersistedMutationResult::Rejected(MutationRejection::PathReplaced(
                        path.clone(),
                    )),
                },
            );
            self.store.commit(next)?;
        }
        if matches!(
            self.streams.get(path),
            Some(RuntimeStream::Pending(pending)) if pending.operation_id == operation_id
        ) {
            self.streams.remove(path);
        }
        if let Some(fence) = self.stream_replacement_fences.get_mut(path) {
            fence
                .requests
                .retain(|request| request.operation_id != operation_id);
        }
        Ok(())
    }

    fn close_stream(
        &mut self,
        path: &DataPath,
        incarnation: StreamIncarnation,
    ) -> Result<(), NamespaceError> {
        let active_matches = matches!(
            self.streams.get(path),
            Some(RuntimeStream::Active(active)) if active.binding.incarnation == incarnation
        );
        if active_matches {
            self.streams.remove(path);
            return Ok(());
        }
        let already_closed = !self.streams.contains_key(path)
            && incarnation.authority_epoch == self.authority_epoch
            && self.store.snapshot().stream_nodes.get(path).copied() == Some(incarnation.revision);
        if already_closed {
            return Ok(());
        }
        Err(NamespaceError::StaleIncarnation {
            path: path.clone(),
            incarnation,
        })
    }

    fn reserve_blob(
        &mut self,
        path: DataPath,
        operation_id: OperationId,
    ) -> Result<(), NamespaceError> {
        if self.store.snapshot().bindings.contains_key(&path)
            || self.store.snapshot().stream_nodes.contains_key(&path)
        {
            return Err(NamespaceError::PathExists(path));
        }
        match self.blob_reservations.get(&path) {
            Some(existing) if *existing == operation_id => Ok(()),
            Some(_) => Err(NamespaceError::PathExists(path)),
            None => {
                self.blob_reservations.insert(path, operation_id);
                Ok(())
            }
        }
    }

    fn release_blob_reservation(
        &mut self,
        path: &DataPath,
        operation_id: OperationId,
    ) -> Result<(), NamespaceError> {
        match self.blob_reservations.get(path) {
            Some(existing) if *existing == operation_id => {
                self.blob_reservations.remove(path);
                Ok(())
            }
            Some(_) => Err(NamespaceError::OperationConflict(operation_id)),
            None => Ok(()),
        }
    }

    fn register(
        &mut self,
        registration: BlobRegistration,
    ) -> Result<MutationReceipt, NamespaceError> {
        let BlobRegistration {
            path,
            source,
            source_node,
            length,
            recovery,
            operation_id,
            reservation,
            retired,
        } = registration;
        let owner = match &recovery {
            SourceRecovery::Actor { owner, .. } => *owner,
            SourceRecovery::File { .. } => None,
        };
        let request = MutationRequest::Register {
            path: path.clone(),
            length,
            recovery: recovery.clone(),
        };
        if let Some(replayed) = self.replay(operation_id, &request) {
            return replayed;
        }
        match self.blob_reservations.get(&path) {
            Some(existing) if Some(*existing) == reservation => {}
            Some(_) => {
                return Err(self.reject(
                    operation_id,
                    request,
                    MutationRejection::PathExists(path),
                ));
            }
            None if reservation.is_some() => {
                return Err(self.reject(
                    operation_id,
                    request,
                    MutationRejection::PathReplaced(path),
                ));
            }
            None => {}
        }
        let revision = self.store.snapshot().next_revision;
        let next_revision = revision
            .checked_add(1)
            .filter(|revision| *revision != 0)
            .ok_or(NamespaceStoreError::RevisionExhausted)?;
        let receipt = MutationReceipt { revision };
        let mut next = self.store.snapshot().clone();
        next.next_revision = next_revision;
        next.stream_nodes.remove(&path);
        next.bindings.insert(
            path.clone(),
            PersistedBinding {
                length,
                revision,
                recovery,
            },
        );
        if let Some(retired) = retired
            && !next.retirements.contains(&retired)
        {
            next.retirements.push(retired);
        }
        next.operations.insert(
            operation_id,
            PersistedOperation {
                request,
                result: PersistedMutationResult::Committed(receipt),
            },
        );
        self.store.commit(next)?;
        if reservation.is_some() {
            self.blob_reservations.remove(&path);
        }
        self.sources.insert(
            path,
            RuntimeSource::Available {
                actor: source,
                node: source_node,
                owner,
            },
        );
        Ok(receipt)
    }

    fn resolve(&self, path: &DataPath) -> Result<BlobBinding, NamespaceError> {
        if self.store.snapshot().stream_nodes.contains_key(path) {
            return Err(NamespaceError::WrongEntryType {
                path: path.clone(),
                expected: EntryKind::Blob,
                found: EntryKind::Stream,
            });
        }
        let persisted = self
            .store
            .snapshot()
            .bindings
            .get(path)
            .ok_or_else(|| NamespaceError::PathNotFound(path.clone()))?;
        match self.sources.get(path) {
            Some(RuntimeSource::Available { actor, node, owner }) => Ok(BlobBinding {
                source: *actor,
                source_node: *node,
                owner: *owner,
                length: persisted.length,
                revision: persisted.revision,
            }),
            Some(RuntimeSource::Unavailable(reason)) => {
                Err(NamespaceError::SourceRecovery(reason.clone()))
            }
            None => Err(NamespaceError::SourceRecovery(format!(
                "binding {path} has no recovered runtime source"
            ))),
        }
    }

    fn lookup(&self, path: &DataPath) -> Result<NamespaceNode, NamespaceError> {
        if self.blob_reservations.contains_key(path) {
            return Err(NamespaceError::PathExists(path.clone()));
        }
        let snapshot = self.store.snapshot();
        match (snapshot.bindings.get(path), snapshot.stream_nodes.get(path)) {
            (Some(binding), None) => Ok(NamespaceNode {
                kind: EntryKind::Blob,
                revision: binding.revision,
                active: false,
            }),
            (None, Some(revision)) => Ok(NamespaceNode {
                kind: EntryKind::Stream,
                revision: *revision,
                active: self.streams.contains_key(path),
            }),
            (None, None) => Err(NamespaceError::PathNotFound(path.clone())),
            (Some(_), Some(_)) => Err(NamespaceError::Storage(format!(
                "data path {path} is bound as both blob and stream"
            ))),
        }
    }

    fn unregister(
        &mut self,
        path: DataPath,
        operation_id: OperationId,
        retired: Option<ActorAddress>,
    ) -> Result<MutationReceipt, NamespaceError> {
        let request = MutationRequest::Unregister { path: path.clone() };
        if let Some(replayed) = self.replay(operation_id, &request) {
            return replayed;
        }
        if self.blob_reservations.contains_key(&path) {
            return Err(self.reject(operation_id, request, MutationRejection::PathExists(path)));
        }
        if !self.store.snapshot().bindings.contains_key(&path)
            && !self.store.snapshot().stream_nodes.contains_key(&path)
        {
            return Err(self.reject(operation_id, request, MutationRejection::PathNotFound(path)));
        }
        let revision = self.store.snapshot().next_revision;
        let next_revision = revision
            .checked_add(1)
            .filter(|revision| *revision != 0)
            .ok_or(NamespaceStoreError::RevisionExhausted)?;
        let receipt = MutationReceipt { revision };
        let mut next = self.store.snapshot().clone();
        next.next_revision = next_revision;
        next.bindings.remove(&path);
        next.stream_nodes.remove(&path);
        if let Some(retired) = retired
            && !next.retirements.contains(&retired)
        {
            next.retirements.push(retired);
        }
        next.operations.insert(
            operation_id,
            PersistedOperation {
                request,
                result: PersistedMutationResult::Committed(receipt),
            },
        );
        self.store.commit(next)?;
        self.sources.remove(&path);
        Ok(receipt)
    }

    fn rename(
        &mut self,
        source: DataPath,
        destination: DataPath,
        replace: bool,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        let request = MutationRequest::Rename {
            source: source.clone(),
            destination: destination.clone(),
            replace,
        };
        if let Some(replayed) = self.replay(operation_id, &request) {
            return replayed;
        }
        let snapshot = self.store.snapshot();
        let source_exists =
            snapshot.bindings.contains_key(&source) || snapshot.stream_nodes.contains_key(&source);
        if !source_exists {
            return Err(self.reject(
                operation_id,
                request,
                MutationRejection::PathNotFound(source),
            ));
        }
        if self.blob_reservations.contains_key(&source)
            || self.blob_reservations.contains_key(&destination)
        {
            let reserved = if self.blob_reservations.contains_key(&source) {
                source
            } else {
                destination
            };
            return Err(self.reject(
                operation_id,
                request,
                MutationRejection::PathExists(reserved),
            ));
        }
        let active_path = self
            .streams
            .contains_key(&source)
            .then_some(source.clone())
            .or_else(|| {
                self.streams
                    .contains_key(&destination)
                    .then_some(destination.clone())
            });
        if let Some(path) = active_path {
            return Err(self.reject(operation_id, request, MutationRejection::StreamActive(path)));
        }

        let destination_exists = snapshot.bindings.contains_key(&destination)
            || snapshot.stream_nodes.contains_key(&destination);
        if source != destination && destination_exists && !replace {
            return Err(self.reject(
                operation_id,
                request,
                MutationRejection::PathExists(destination),
            ));
        }

        let revision = snapshot.next_revision;
        let next_revision = revision
            .checked_add(1)
            .filter(|revision| *revision != 0)
            .ok_or(NamespaceStoreError::RevisionExhausted)?;
        let receipt = MutationReceipt { revision };
        let destination_retired = (source != destination)
            .then(|| self.sources.get(&destination))
            .flatten()
            .and_then(|source| match source {
                RuntimeSource::Available { actor, .. } => Some(*actor),
                RuntimeSource::Unavailable(_) => None,
            });
        let mut next = snapshot.clone();
        next.next_revision = next_revision;
        let source_binding = next.bindings.remove(&source);
        let source_stream = next.stream_nodes.remove(&source);
        next.bindings.remove(&destination);
        next.stream_nodes.remove(&destination);
        if let Some(mut binding) = source_binding {
            binding.revision = revision;
            next.bindings.insert(destination.clone(), binding);
        } else if source_stream.is_some() {
            next.stream_nodes.insert(destination.clone(), revision);
        }
        if let Some(retired) = destination_retired
            && !next.retirements.contains(&retired)
        {
            next.retirements.push(retired);
        }
        next.operations.insert(
            operation_id,
            PersistedOperation {
                request,
                result: PersistedMutationResult::Committed(receipt),
            },
        );
        self.store.commit(next)?;

        let source_runtime = self.sources.remove(&source);
        if source != destination {
            self.sources.remove(&destination);
        }
        if let Some(source_runtime) = source_runtime {
            self.sources.insert(destination, source_runtime);
        }
        Ok(receipt)
    }
}

impl ActorInterface for DataDirectoryActor {
    type Incoming = DataDirectoryIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if let Some(retire_retry) = &self.retire_retry {
            self.retirement_timer = Some(retire_retry.engine.send_every(
                retire_retry.period,
                retire_retry.sender.clone(),
                ctx.self_addr(),
                DataDirectoryIn::RetryRetirements,
            ));
        }
        let targets: Vec<_> = self
            .pending_retirements
            .iter()
            .chain(self.pending_stream_displacements.keys())
            .copied()
            .collect();
        for target in targets {
            self.watch_retirement(ctx, target);
        }
        self.retry_retirements(ctx);
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryIn) {
        #[cfg(feature = "directory-trace")]
        self.trace
            .observed_step(self.pending_retirements.len(), &self.store);
        match message {
            DataDirectoryIn::Register {
                request_id,
                path,
                source,
                source_node,
                length,
                recovery,
                operation_id,
                reservation,
                reply_to,
            } => {
                let logical = path.clone();
                let replayed = self.store.snapshot().operations.contains_key(&operation_id);
                let retired = (!replayed)
                    .then(|| self.sources.get(&path))
                    .flatten()
                    .and_then(|runtime_source| match runtime_source {
                        RuntimeSource::Available { actor, .. } if *actor != source => Some(*actor),
                        RuntimeSource::Available { .. } | RuntimeSource::Unavailable(_) => None,
                    });
                let result = self.register(BlobRegistration {
                    path,
                    source,
                    source_node,
                    length,
                    recovery,
                    operation_id,
                    reservation,
                    retired,
                });
                if result.is_ok()
                    && let Some(retired) = retired
                {
                    self.queue_retirement(ctx, retired);
                }
                if result.is_ok() {
                    self.displace_stream(ctx, &logical);
                }
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::Registered {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::Resolve {
                request_id,
                path,
                reply_to,
            } => {
                let result = self.resolve(&path);
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::Resolved {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::Lookup {
                request_id,
                path,
                reply_to,
            } => {
                let result = self.lookup(&path);
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::LookedUp {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::ReserveBlob {
                request_id,
                path,
                operation_id,
                reply_to,
            } => {
                let result = self.reserve_blob(path, operation_id);
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::BlobReserved {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::ReleaseBlobReservation {
                request_id,
                path,
                operation_id,
                reply_to,
            } => {
                let result = self.release_blob_reservation(&path, operation_id);
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::BlobReservationReleased {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::Unregister {
                request_id,
                path,
                operation_id,
                reply_to,
            } => {
                if self.streams.contains_key(&path) {
                    let _ = ctx.send(
                        reply_to,
                        NamespaceClientIn::DirectoryReply(DataDirectoryOut::Unregistered {
                            request_id,
                            authority_epoch: self.authority_epoch,
                            result: Err(NamespaceError::WrongEntryType {
                                path,
                                expected: EntryKind::Blob,
                                found: EntryKind::Stream,
                            }),
                        }),
                    );
                    return;
                }
                let replayed = self.store.snapshot().operations.contains_key(&operation_id);
                let retired = (!replayed)
                    .then(|| self.sources.get(&path))
                    .flatten()
                    .and_then(|source| match source {
                        RuntimeSource::Available { actor, .. } => Some(*actor),
                        RuntimeSource::Unavailable(_) => None,
                    });
                let result = self.unregister(path, operation_id, retired);
                if result.is_ok()
                    && let Some(retired) = retired
                {
                    self.queue_retirement(ctx, retired);
                }
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::Unregistered {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::Rename {
                request_id,
                source,
                destination,
                replace,
                operation_id,
                reply_to,
            } => {
                let replayed = self.store.snapshot().operations.contains_key(&operation_id);
                let retired = (!replayed && source != destination)
                    .then(|| self.sources.get(&destination))
                    .flatten()
                    .and_then(|source| match source {
                        RuntimeSource::Available { actor, .. } => Some(*actor),
                        RuntimeSource::Unavailable(_) => None,
                    });
                let result = self.rename(source, destination, replace, operation_id);
                if result.is_ok()
                    && let Some(retired) = retired
                {
                    self.queue_retirement(ctx, retired);
                }
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::Renamed {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::OpenStream {
                request_id,
                path,
                role,
                endpoint,
                descriptor,
                replace,
                ensure,
                expected_revision,
                operation_id,
                reply_to,
            } => self.open_stream(
                ctx,
                StreamOpenRequest {
                    request_id,
                    path,
                    role,
                    endpoint,
                    replace,
                    ensure,
                    expected_revision,
                    descriptor,
                    operation_id,
                    reply_to,
                },
            ),
            DataDirectoryIn::CancelStream {
                request_id,
                path,
                operation_id,
                reply_to,
            } => {
                let result = self.cancel_stream(&path, operation_id);
                if let Some(reply_to) = reply_to {
                    let _ = ctx.send(
                        reply_to,
                        NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamCancelled {
                            request_id,
                            authority_epoch: self.authority_epoch,
                            result,
                        }),
                    );
                }
            }
            DataDirectoryIn::CloseStream {
                request_id,
                path,
                incarnation,
                reply_to,
            } => {
                let result = self.close_stream(&path, incarnation);
                let _ = ctx.send(
                    reply_to,
                    NamespaceClientIn::DirectoryReply(DataDirectoryOut::StreamClosed {
                        request_id,
                        authority_epoch: self.authority_epoch,
                        result,
                    }),
                );
            }
            DataDirectoryIn::SourceRetired { source } => {
                let _ = self.confirm_retirement(source);
            }
            // The retry tick is the only periodic re-drive of pending
            // retirements: re-fanning on *every* inbound message couples the
            // outbound Retire rate to the cluster's request rate, so a loaded
            // directory (many pending namespace requests) multiplied each
            // unacknowledged retirement into a self-sustaining frame storm
            // that starved namespace replies behind bulk traffic.
            DataDirectoryIn::RetryRetirements => self.retry_retirements(ctx),
            DataDirectoryIn::RetirementRouteChanged { target } => {
                self.retry_retirement(ctx, target);
            }
            DataDirectoryIn::StreamDisplaced {
                endpoint,
                incarnation,
            } => {
                self.confirm_stream_displacement(ctx, endpoint, incarnation);
            }
        }
    }
}

impl Drop for DataDirectoryActor {
    fn drop(&mut self) {
        if let Some(timer) = self.retirement_timer.take() {
            timer.cancel();
        }
    }
}

pub trait NamespaceDiscovery: Send + Sync + 'static {
    fn current_directory(&self) -> Option<ActorAddress>;
    /// Discoveries carrying a durable authority epoch must validate it exactly
    /// and reject replies while the current authority is undiscovered. A
    /// transport wake alone never authenticates an old authority.
    fn accepts_authority_epoch(&self, _epoch: u64) -> bool {
        true
    }
}

struct PendingRequest {
    request: NamespaceRequest,
    reply_to: ActorAddress,
    enqueued: EngineInstant,
}

/// How long a namespace request may stay unanswered while no directory is
/// discoverable. Authority loss must surface as a bounded typed failure to
/// callers (e.g. a contextual process blocked on `lookup` after the
/// orchestrator died), never as a hang.
const NAMESPACE_REQUEST_DEADLINE: Duration = Duration::from_secs(80);

pub struct NamespaceClientActor {
    engine: EngineHandle,
    sender: ExternalSender,
    discovery: Arc<dyn NamespaceDiscovery>,
    retry_period: Duration,
    request_deadline: Duration,
    next_request_id: u64,
    pending: HashMap<DirectoryRequestId, PendingRequest>,
}

impl NamespaceClientActor {
    pub fn new(
        engine: EngineHandle,
        sender: ExternalSender,
        discovery: Arc<dyn NamespaceDiscovery>,
        retry_period: Duration,
    ) -> Self {
        Self::new_with_deadline(
            engine,
            sender,
            discovery,
            retry_period,
            NAMESPACE_REQUEST_DEADLINE,
        )
    }

    pub fn new_with_deadline(
        engine: EngineHandle,
        sender: ExternalSender,
        discovery: Arc<dyn NamespaceDiscovery>,
        retry_period: Duration,
        request_deadline: Duration,
    ) -> Self {
        Self {
            engine,
            sender,
            discovery,
            retry_period,
            request_deadline,
            next_request_id: 1,
            pending: HashMap::new(),
        }
    }

    fn dispatch(&self, ctx: &Ctx<'_>, request_id: DirectoryRequestId, request: &NamespaceRequest) {
        let Some(directory) = self.discovery.current_directory() else {
            return;
        };
        let message = match request {
            NamespaceRequest::Register {
                path,
                source,
                source_node,
                length,
                recovery,
                operation_id,
                reservation,
            } => DataDirectoryIn::Register {
                request_id,
                path: path.clone(),
                source: *source,
                source_node: *source_node,
                length: *length,
                recovery: recovery.clone(),
                operation_id: *operation_id,
                reservation: *reservation,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::Resolve { path } => DataDirectoryIn::Resolve {
                request_id,
                path: path.clone(),
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::Lookup { path } => DataDirectoryIn::Lookup {
                request_id,
                path: path.clone(),
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::ReserveBlob { path, operation_id } => DataDirectoryIn::ReserveBlob {
                request_id,
                path: path.clone(),
                operation_id: *operation_id,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::ReleaseBlobReservation { path, operation_id } => {
                DataDirectoryIn::ReleaseBlobReservation {
                    request_id,
                    path: path.clone(),
                    operation_id: *operation_id,
                    reply_to: ctx.self_addr(),
                }
            }
            NamespaceRequest::Unregister { path, operation_id } => DataDirectoryIn::Unregister {
                request_id,
                path: path.clone(),
                operation_id: *operation_id,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::Rename {
                source,
                destination,
                replace,
                operation_id,
            } => DataDirectoryIn::Rename {
                request_id,
                source: source.clone(),
                destination: destination.clone(),
                replace: *replace,
                operation_id: *operation_id,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::OpenStream {
                path,
                role,
                endpoint,
                descriptor,
                replace,
                ensure,
                expected_revision,
                operation_id,
            } => DataDirectoryIn::OpenStream {
                request_id,
                path: path.clone(),
                role: *role,
                endpoint: *endpoint,
                descriptor: descriptor.clone(),
                replace: *replace,
                ensure: *ensure,
                expected_revision: *expected_revision,
                operation_id: *operation_id,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::CancelStream { path, operation_id } => {
                DataDirectoryIn::CancelStream {
                    request_id,
                    path: path.clone(),
                    operation_id: *operation_id,
                    reply_to: Some(ctx.self_addr()),
                }
            }
            NamespaceRequest::CloseStream { path, incarnation } => DataDirectoryIn::CloseStream {
                request_id,
                path: path.clone(),
                incarnation: *incarnation,
                reply_to: ctx.self_addr(),
            },
        };
        let _ = ctx.send(directory, message);
    }

    /// Enqueue a tracked CancelStream retraction for an abandoned stream
    /// open. The retraction is a pending request the retry tick re-drives
    /// until the directory acknowledges it: a dropped frame would otherwise
    /// strand the parked endpoint in the directory forever, and unlink of
    /// its path would then fail with ENXIO indefinitely.
    fn retract_stream_open(&mut self, ctx: &Ctx<'_>, path: DataPath, operation_id: OperationId) {
        let Some(next) = self
            .next_request_id
            .checked_add(1)
            .filter(|next| *next != 0)
        else {
            return;
        };
        let request_id = DirectoryRequestId(self.next_request_id);
        self.next_request_id = next;
        let request = NamespaceRequest::CancelStream { path, operation_id };
        self.dispatch(ctx, request_id, &request);
        self.pending.insert(
            request_id,
            PendingRequest {
                request,
                reply_to: ctx.self_addr(),
                enqueued: self.engine.now(),
            },
        );
    }
}

impl ActorInterface for NamespaceClientActor {
    type Incoming = NamespaceClientIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.engine.send_every(
            self.retry_period,
            self.sender.clone(),
            ctx.self_addr(),
            NamespaceClientIn::Retry,
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: NamespaceClientIn) {
        match message {
            NamespaceClientIn::Request { request, reply_to } => {
                let request_id = DirectoryRequestId(self.next_request_id);
                let Some(next) = self
                    .next_request_id
                    .checked_add(1)
                    .filter(|next| *next != 0)
                else {
                    let _ = ctx.send(
                        reply_to,
                        DataDirectoryOut::Resolved {
                            request_id,
                            authority_epoch: 0,
                            result: Err(NamespaceError::Protocol(
                                "namespace client request IDs exhausted".to_owned(),
                            )),
                        },
                    );
                    return;
                };
                self.next_request_id = next;
                self.dispatch(ctx, request_id, &request);
                self.pending.insert(
                    request_id,
                    PendingRequest {
                        request,
                        reply_to,
                        enqueued: self.engine.now(),
                    },
                );
            }
            NamespaceClientIn::Cancel { reply_to } => {
                let cancelled: Vec<(DataPath, OperationId)> = self
                    .pending
                    .values()
                    .filter(|pending| pending.reply_to == reply_to)
                    .filter_map(|pending| match &pending.request {
                        NamespaceRequest::OpenStream {
                            path, operation_id, ..
                        } => Some((path.clone(), *operation_id)),
                        _ => None,
                    })
                    .collect();
                self.pending.retain(|_, pending| {
                    if pending.reply_to != reply_to {
                        return true;
                    }
                    if matches!(
                        pending.request,
                        NamespaceRequest::CloseStream { .. }
                            | NamespaceRequest::CancelStream { .. }
                    ) {
                        // Caller cancellation transfers cleanup ownership to
                        // the proxy; it cannot revoke an unacknowledged retract.
                        pending.reply_to = ctx.self_addr();
                        true
                    } else {
                        false
                    }
                });
                // A dropped cancel frame would strand the pending endpoint
                // in the directory forever (nothing else retracts it), so
                // the retraction is tracked as a pending request the retry
                // tick re-drives until the directory acknowledges it.
                for (path, operation_id) in cancelled {
                    self.retract_stream_open(ctx, path, operation_id);
                }
            }
            NamespaceClientIn::CancelBlobReservation {
                path,
                operation_id,
                reply_to,
            } => {
                self.pending
                    .retain(|_, pending| pending.reply_to != reply_to);
                if let Some(directory) = self.discovery.current_directory() {
                    let _ = ctx.send(
                        directory,
                        DataDirectoryIn::ReleaseBlobReservation {
                            request_id: DirectoryRequestId(0),
                            path,
                            operation_id,
                            reply_to: ctx.self_addr(),
                        },
                    );
                }
            }
            NamespaceClientIn::DirectoryReply(reply) => {
                if !self
                    .discovery
                    .accepts_authority_epoch(reply.authority_epoch())
                {
                    return;
                }
                if let Some(pending) = self.pending.remove(&reply.request_id()) {
                    let _ = ctx.send(pending.reply_to, reply);
                }
            }
            NamespaceClientIn::TransferFailed {
                destination,
                transfer_id,
                reason,
            } => {
                let _ = ctx.send(
                    destination,
                    BlobTransferEvent::Failed {
                        transfer_id,
                        reason,
                    },
                );
            }
            NamespaceClientIn::Retry => {
                // A request no directory has answered within the bound fails
                // with a typed error instead of hanging the caller forever
                // (e.g. authority loss — the registry may keep serving the
                // dead directory's address, so age is the only sound bound).
                // Stream retractions are exempt: a CloseStream or
                // CancelStream that expires while the directory is
                // unreachable would strand the endpoint in the directory
                // forever — unlink of that path then fails with ENXIO
                // indefinitely — while every later request flows again.
                // Retractions have no caller left to disappoint; they retry
                // until the directory acknowledges them.
                let now = self.engine.now();
                let expired: Vec<DirectoryRequestId> = self
                    .pending
                    .iter()
                    .filter(|(_, pending)| {
                        !matches!(
                            pending.request,
                            NamespaceRequest::CloseStream { .. }
                                | NamespaceRequest::CancelStream { .. }
                        ) && now
                            .to_instant()
                            .duration_since(pending.enqueued.to_instant())
                            > self.request_deadline
                    })
                    .map(|(request_id, _)| *request_id)
                    .collect();
                for request_id in expired {
                    if let Some(pending) = self.pending.remove(&request_id) {
                        // An expired stream open may already be registered
                        // in the directory: first-role opens park without a
                        // reply until their peer arrives, so the deadline
                        // fires while the endpoint is committed. Its caller
                        // is gone with the failure reply — nothing else
                        // retracts the open — so the expiry itself must
                        // enqueue the same deadline-exempt retraction the
                        // cancel path uses. Without it the parked endpoint
                        // survives forever and unlink of the path fails
                        // with ENXIO indefinitely.
                        if let NamespaceRequest::OpenStream {
                            path, operation_id, ..
                        } = &pending.request
                        {
                            self.retract_stream_open(ctx, path.clone(), *operation_id);
                        }
                        let _ = ctx.send(
                            pending.reply_to,
                            expired_reply(request_id, &pending.request),
                        );
                    }
                }
                for (request_id, pending) in &self.pending {
                    self.dispatch(ctx, *request_id, &pending.request);
                }
            }
        }
    }
}

/// Build the typed failure a caller receives when its namespace request
fn expired_reply(request_id: DirectoryRequestId, request: &NamespaceRequest) -> DataDirectoryOut {
    fn failure<T>() -> Result<T, NamespaceError> {
        Err(NamespaceError::DirectoryUnavailable(
            "no namespace authority was discoverable within the request deadline".to_owned(),
        ))
    }
    match request {
        NamespaceRequest::Register { .. } => DataDirectoryOut::Registered {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::Resolve { .. } => DataDirectoryOut::Resolved {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::Lookup { .. } => DataDirectoryOut::LookedUp {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::ReserveBlob { .. } => DataDirectoryOut::BlobReserved {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::ReleaseBlobReservation { .. } => {
            DataDirectoryOut::BlobReservationReleased {
                request_id,
                authority_epoch: 0,
                result: failure(),
            }
        }
        NamespaceRequest::Unregister { .. } => DataDirectoryOut::Unregistered {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::Rename { .. } => DataDirectoryOut::Renamed {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::OpenStream { .. } => DataDirectoryOut::StreamOpened {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::CancelStream { .. } => DataDirectoryOut::StreamCancelled {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
        NamespaceRequest::CloseStream { .. } => DataDirectoryOut::StreamClosed {
            request_id,
            authority_epoch: 0,
            result: failure(),
        },
    }
}

struct NamespaceRequestCancellation {
    runtime: Runtime,
    proxy: ActorAddress,
    reply_to: ActorAddress,
    armed: bool,
}

impl Drop for NamespaceRequestCancellation {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.runtime.send_to(
                self.proxy,
                NamespaceClientIn::Cancel {
                    reply_to: self.reply_to,
                },
            );
        }
    }
}

#[derive(Clone)]
pub struct NamespaceClient {
    runtime: Runtime,
    proxy: ActorAddress,
}

impl NamespaceClient {
    pub fn new(runtime: Runtime, proxy: ActorAddress) -> Self {
        Self { runtime, proxy }
    }

    pub fn proxy(&self) -> ActorAddress {
        self.proxy
    }

    async fn request(&self, request: NamespaceRequest) -> Result<DataDirectoryOut, NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<DataDirectoryOut>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        let reply_to = *inbox.addr();
        self.runtime
            .send_to(self.proxy, NamespaceClientIn::Request { request, reply_to })
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        let mut cancellation = NamespaceRequestCancellation {
            runtime: self.runtime.clone(),
            proxy: self.proxy,
            reply_to,
            armed: true,
        };
        let reply = inbox.recv().await;
        cancellation.armed = false;
        Ok(reply)
    }

    pub async fn register(
        &self,
        path: DataPath,
        source: ActorAddress,
        source_node: [u8; 32],
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        match self
            .request(NamespaceRequest::Register {
                path,
                source,
                source_node,
                length,
                recovery,
                operation_id,
                reservation: None,
            })
            .await?
        {
            DataDirectoryOut::Registered { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected register reply, received {other:?}"
            ))),
        }
    }

    pub async fn resolve(&self, path: DataPath) -> Result<BlobBinding, NamespaceError> {
        match self.request(NamespaceRequest::Resolve { path }).await? {
            DataDirectoryOut::Resolved { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected resolve reply, received {other:?}"
            ))),
        }
    }

    pub async fn lookup(&self, path: DataPath) -> Result<NamespaceNode, NamespaceError> {
        match self.request(NamespaceRequest::Lookup { path }).await? {
            DataDirectoryOut::LookedUp { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected lookup reply, received {other:?}"
            ))),
        }
    }

    pub async fn unregister(
        &self,
        path: DataPath,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        match self
            .request(NamespaceRequest::Unregister { path, operation_id })
            .await?
        {
            DataDirectoryOut::Unregistered { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected unregister reply, received {other:?}"
            ))),
        }
    }

    pub async fn rename(
        &self,
        source: DataPath,
        destination: DataPath,
        replace: bool,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        match self
            .request(NamespaceRequest::Rename {
                source,
                destination,
                replace,
                operation_id,
            })
            .await?
        {
            DataDirectoryOut::Renamed { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected rename reply, received {other:?}"
            ))),
        }
    }

    async fn open_stream_inner(
        &self,
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        replace: bool,
        operation_id: OperationId,
    ) -> Result<StreamMatch, NamespaceError> {
        match self
            .request(NamespaceRequest::OpenStream {
                path,
                role,
                endpoint,
                replace,
                ensure: true,
                expected_revision: None,
                descriptor: Vec::new(),
                operation_id,
            })
            .await?
        {
            DataDirectoryOut::StreamOpened { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected stream-open reply, received {other:?}"
            ))),
        }
    }

    pub async fn open_stream(
        &self,
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        operation_id: OperationId,
    ) -> Result<StreamMatch, NamespaceError> {
        self.open_stream_inner(path, role, endpoint, false, operation_id)
            .await
    }

    pub async fn replace_with_stream(
        &self,
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        operation_id: OperationId,
    ) -> Result<StreamMatch, NamespaceError> {
        self.open_stream_inner(path, role, endpoint, true, operation_id)
            .await
    }

    pub async fn close_stream(
        &self,
        path: DataPath,
        incarnation: StreamIncarnation,
    ) -> Result<(), NamespaceError> {
        match self
            .request(NamespaceRequest::CloseStream { path, incarnation })
            .await?
        {
            DataDirectoryOut::StreamClosed { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected stream-close reply, received {other:?}"
            ))),
        }
    }
}

struct DirectoryStreamCancellation {
    runtime: Runtime,
    directory: ActorAddress,
    path: DataPath,
    operation_id: OperationId,
    armed: bool,
}

impl Drop for DirectoryStreamCancellation {
    fn drop(&mut self) {
        if self.armed {
            // Fire-and-forget: this path is unused in production (stream
            // opens route through the retrying namespace proxy); the
            // durable cancel tombstone still makes a lost frame safe
            // against a later replay of the same open.
            let _ = self.runtime.send_to(
                self.directory,
                DataDirectoryIn::CancelStream {
                    request_id: DirectoryRequestId(0),
                    path: self.path.clone(),
                    operation_id: self.operation_id,
                    reply_to: None,
                },
            );
        }
    }
}

#[derive(Clone)]
pub struct DirectoryClient {
    runtime: Runtime,
    directory: ActorAddress,
    next_request_id: Arc<AtomicU64>,
}

impl DirectoryClient {
    pub fn new(runtime: Runtime, directory: ActorAddress) -> Self {
        Self {
            runtime,
            directory,
            next_request_id: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn directory(&self) -> ActorAddress {
        self.directory
    }

    fn request_id(&self) -> DirectoryRequestId {
        DirectoryRequestId(self.next_request_id.fetch_add(1, Ordering::Relaxed))
    }

    async fn receive(
        &self,
        inbox: &swactor::runtime::Inbox<NamespaceClientIn>,
    ) -> Result<DataDirectoryOut, NamespaceError> {
        match inbox.recv().await {
            NamespaceClientIn::DirectoryReply(reply) => Ok(reply),
            other => Err(NamespaceError::Protocol(format!(
                "expected directory reply, received {other:?}"
            ))),
        }
    }

    pub async fn register(
        &self,
        path: DataPath,
        source: ActorAddress,
        source_node: [u8; 32],
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::Register {
                    request_id: self.request_id(),
                    path,
                    source,
                    source_node,
                    length,
                    recovery,
                    operation_id,
                    reservation: None,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::Registered { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected register reply, received {other:?}"
            ))),
        }
    }

    pub async fn resolve(&self, path: DataPath) -> Result<BlobBinding, NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::Resolve {
                    request_id: self.request_id(),
                    path,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::Resolved { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected resolve reply, received {other:?}"
            ))),
        }
    }

    pub async fn lookup(&self, path: DataPath) -> Result<NamespaceNode, NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::Lookup {
                    request_id: self.request_id(),
                    path,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::LookedUp { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected lookup reply, received {other:?}"
            ))),
        }
    }

    pub async fn unregister(
        &self,
        path: DataPath,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::Unregister {
                    request_id: self.request_id(),
                    path,
                    operation_id,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::Unregistered { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected unregister reply, received {other:?}"
            ))),
        }
    }

    pub async fn rename(
        &self,
        source: DataPath,
        destination: DataPath,
        replace: bool,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::Rename {
                    request_id: self.request_id(),
                    source,
                    destination,
                    replace,
                    operation_id,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::Renamed { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected rename reply, received {other:?}"
            ))),
        }
    }

    pub async fn reserve_blob(
        &self,
        path: DataPath,
        operation_id: OperationId,
    ) -> Result<(), NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::ReserveBlob {
                    request_id: self.request_id(),
                    path,
                    operation_id,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::BlobReserved { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected reserve reply, received {other:?}"
            ))),
        }
    }

    pub async fn release_blob_reservation(
        &self,
        path: DataPath,
        operation_id: OperationId,
    ) -> Result<(), NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::ReleaseBlobReservation {
                    request_id: self.request_id(),
                    path,
                    operation_id,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::BlobReservationReleased { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected release reply, received {other:?}"
            ))),
        }
    }

    async fn open_stream_inner(
        &self,
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        replace: bool,
        operation_id: OperationId,
    ) -> Result<StreamMatch, NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        let mut cancellation = DirectoryStreamCancellation {
            runtime: self.runtime.clone(),
            directory: self.directory,
            path: path.clone(),
            operation_id,
            armed: true,
        };
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::OpenStream {
                    request_id: self.request_id(),
                    path,
                    role,
                    endpoint,
                    replace,
                    ensure: true,
                    expected_revision: None,
                    operation_id,
                    descriptor: Vec::new(),
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        let result = match self.receive(&inbox).await? {
            DataDirectoryOut::StreamOpened { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected stream-open reply, received {other:?}"
            ))),
        };
        cancellation.armed = false;
        result
    }

    pub async fn open_stream(
        &self,
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        operation_id: OperationId,
    ) -> Result<StreamMatch, NamespaceError> {
        self.open_stream_inner(path, role, endpoint, false, operation_id)
            .await
    }

    pub async fn replace_with_stream(
        &self,
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        operation_id: OperationId,
    ) -> Result<StreamMatch, NamespaceError> {
        self.open_stream_inner(path, role, endpoint, true, operation_id)
            .await
    }

    pub async fn close_stream(
        &self,
        path: DataPath,
        incarnation: StreamIncarnation,
    ) -> Result<(), NamespaceError> {
        let inbox = self
            .runtime
            .new_inbox::<NamespaceClientIn>()
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        self.runtime
            .send_to(
                self.directory,
                DataDirectoryIn::CloseStream {
                    request_id: self.request_id(),
                    path,
                    incarnation,
                    reply_to: *inbox.addr(),
                },
            )
            .map_err(|error| NamespaceError::DirectoryUnavailable(error.to_string()))?;
        match self.receive(&inbox).await? {
            DataDirectoryOut::StreamClosed { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected stream-close reply, received {other:?}"
            ))),
        }
    }
}

pub fn register_namespace_codecs(registry: &mut CodecRegistry) {
    registry
        .register::<DataDirectoryIn, _>(JsonCodec::default())
        .expect("unique codec registration");
    registry
        .register::<NamespaceClientIn, _>(JsonCodec::default())
        .expect("unique codec registration");
}
