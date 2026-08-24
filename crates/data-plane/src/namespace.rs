//! Authoritative virtual blob namespace actor and restart-tolerant client proxy.

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor_engine::EngineHandle;
use swactor_transport::{CodecRegistry, JsonCodec, NetworkMessage};

use crate::blob_transfer::{BlobTransferEvent, BlobTransferId};
use crate::namespace_store::{
    MutationReceipt, MutationRejection, MutationRequest, NamespaceStore, NamespaceStoreError,
    PersistedBinding, PersistedMutationResult, PersistedOperation,
};
use crate::path::DataPath;
use crate::source::BlobSourceIn;

pub use crate::namespace_store::{OperationId, SourceRecovery};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DirectoryRequestId(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobBinding {
    pub source: ActorAddress,
    pub length: u64,
    pub revision: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EntryKind {
    Blob,
    Stream,
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
    Storage(String),
    SourceRecovery(String),
    DirectoryUnavailable(String),
    Protocol(String),
}

impl fmt::Display for NamespaceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PathNotFound(path) => write!(f, "data path not found: {path}"),
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
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    Resolve {
        request_id: DirectoryRequestId,
        path: DataPath,
        reply_to: ActorAddress,
    },
    Unregister {
        request_id: DirectoryRequestId,
        path: DataPath,
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
        operation_id: OperationId,
        reply_to: ActorAddress,
    },
    CancelStream {
        path: DataPath,
        operation_id: OperationId,
    },
    CloseStream {
        request_id: DirectoryRequestId,
        path: DataPath,
        incarnation: StreamIncarnation,
        reply_to: ActorAddress,
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
    Unregistered {
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
}
impl DataDirectoryOut {
    pub fn request_id(&self) -> DirectoryRequestId {
        match self {
            Self::Registered { request_id, .. }
            | Self::Resolved { request_id, .. }
            | Self::Unregistered { request_id, .. }
            | Self::StreamOpened { request_id, .. }
            | Self::StreamClosed { request_id, .. } => *request_id,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NamespaceRequest {
    Register {
        path: DataPath,
        source: ActorAddress,
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
    },
    Resolve {
        path: DataPath,
    },
    Unregister {
        path: DataPath,
        operation_id: OperationId,
    },
    OpenStream {
        path: DataPath,
        role: StreamRole,
        endpoint: ActorAddress,
        descriptor: Vec<u8>,
        replace: bool,
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
    Available(ActorAddress),
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
}

struct StreamOpenRequest {
    request_id: DirectoryRequestId,
    path: DataPath,
    role: StreamRole,
    endpoint: ActorAddress,
    replace: bool,
    descriptor: Vec<u8>,
    operation_id: OperationId,
    reply_to: ActorAddress,
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

pub struct DataDirectoryActor {
    store: NamespaceStore,
    sources: BTreeMap<DataPath, RuntimeSource>,
    streams: BTreeMap<DataPath, RuntimeStream>,
    authority_epoch: u64,
}

impl DataDirectoryActor {
    pub fn recover(
        store_path: impl AsRef<Path>,
        mut recover_source: impl FnMut(&SourceRecovery, u64) -> Result<ActorAddress, NamespaceError>,
    ) -> Result<Self, NamespaceError> {
        let mut store = NamespaceStore::open(store_path)?;
        let bindings = store.snapshot().bindings.clone();
        let mut sources = BTreeMap::new();
        for (path, binding) in bindings {
            let source = match recover_source(&binding.recovery, binding.length) {
                Ok(actor) => RuntimeSource::Available(actor),
                Err(error) => RuntimeSource::Unavailable(error.to_string()),
            };
            sources.insert(path, source);
        }
        let authority_epoch = store.advance_authority_epoch()?;
        Ok(Self {
            store,
            sources,
            streams: BTreeMap::new(),
            authority_epoch,
        })
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
                }
            })
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
        let revision = self.store.snapshot().next_revision;
        let next_revision = revision
            .checked_add(1)
            .filter(|revision| *revision != 0)
            .ok_or(NamespaceStoreError::RevisionExhausted)?;
        let receipt = MutationReceipt { revision };
        let mut next = self.store.snapshot().clone();
        next.next_revision = next_revision;
        next.bindings.remove(&path);
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
        if let Some(RuntimeStream::Pending(pending)) = self.streams.remove(path) {
            self.send_stream_result(
                ctx,
                pending.request_id,
                pending.reply_to,
                Err(NamespaceError::PathReplaced(path.clone())),
            );
        }
    }

    fn open_stream(&mut self, ctx: &Ctx<'_>, request: StreamOpenRequest) {
        let StreamOpenRequest {
            request_id,
            path,
            role,
            endpoint,
            replace,
            descriptor,
            operation_id,
            reply_to,
        } = request;
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

        let compatible_pending = matches!(
            self.streams.get(&path),
            Some(RuntimeStream::Pending(pending)) if pending.role != role
        );
        if replace && self.streams.contains_key(&path) && !compatible_pending {
            self.displace_stream(ctx, &path);
        }

        if let Some(RuntimeStream::Pending(pending)) = self.streams.remove(&path) {
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
            RuntimeSource::Available(actor) => Some(*actor),
            RuntimeSource::Unavailable(_) => None,
        });
        match self.bind_stream(path.clone(), operation_id, retired) {
            Ok(receipt) => {
                if let Some(retired) = retired {
                    let _ = ctx.send(retired, BlobSourceIn::Retire);
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
                    }),
                );
            }
            Err(error) => self.send_stream_result(ctx, request_id, reply_to, Err(error)),
        }
    }

    fn cancel_stream(&mut self, path: &DataPath, operation_id: OperationId) {
        let should_remove = matches!(
            self.streams.get(path),
            Some(RuntimeStream::Pending(pending)) if pending.operation_id == operation_id
        );
        if should_remove {
            self.streams.remove(path);
        }
    }

    fn close_stream(
        &mut self,
        path: &DataPath,
        incarnation: StreamIncarnation,
    ) -> Result<(), NamespaceError> {
        let matches = matches!(
            self.streams.get(path),
            Some(RuntimeStream::Active(active)) if active.binding.incarnation == incarnation
        );
        if matches {
            self.streams.remove(path);
            Ok(())
        } else {
            Err(NamespaceError::StaleIncarnation {
                path: path.clone(),
                incarnation,
            })
        }
    }

    fn register(
        &mut self,
        path: DataPath,
        source: ActorAddress,
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
        retired: Option<ActorAddress>,
    ) -> Result<MutationReceipt, NamespaceError> {
        let request = MutationRequest::Register {
            path: path.clone(),
            length,
            recovery: recovery.clone(),
        };
        if let Some(replayed) = self.replay(operation_id, &request) {
            return replayed;
        }
        let revision = self.store.snapshot().next_revision;
        let next_revision = revision
            .checked_add(1)
            .filter(|revision| *revision != 0)
            .ok_or(NamespaceStoreError::RevisionExhausted)?;
        let receipt = MutationReceipt { revision };
        let mut next = self.store.snapshot().clone();
        next.next_revision = next_revision;
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
        self.sources.insert(path, RuntimeSource::Available(source));
        Ok(receipt)
    }

    fn resolve(&self, path: &DataPath) -> Result<BlobBinding, NamespaceError> {
        if self.streams.contains_key(path) {
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
            Some(RuntimeSource::Available(source)) => Ok(BlobBinding {
                source: *source,
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
        if !self.store.snapshot().bindings.contains_key(&path) {
            let mut next = self.store.snapshot().clone();
            next.operations.insert(
                operation_id,
                PersistedOperation {
                    request,
                    result: PersistedMutationResult::Rejected(MutationRejection::PathNotFound(
                        path.clone(),
                    )),
                },
            );
            self.store.commit(next)?;
            return Err(NamespaceError::PathNotFound(path));
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
}

impl ActorInterface for DataDirectoryActor {
    type Incoming = DataDirectoryIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        for source in self.store.snapshot().retirements.iter().copied() {
            let _ = ctx.send(source, BlobSourceIn::Retire);
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryIn) {
        match message {
            DataDirectoryIn::Register {
                request_id,
                path,
                source,
                length,
                recovery,
                operation_id,
                reply_to,
            } => {
                let logical = path.clone();
                let replayed = self.store.snapshot().operations.contains_key(&operation_id);
                let retired = (!replayed)
                    .then(|| self.sources.get(&path))
                    .flatten()
                    .and_then(|runtime_source| match runtime_source {
                        RuntimeSource::Available(actor) if *actor != source => Some(*actor),
                        RuntimeSource::Available(_) | RuntimeSource::Unavailable(_) => None,
                    });
                let result = self.register(path, source, length, recovery, operation_id, retired);
                if result.is_ok()
                    && let Some(retired) = retired
                {
                    let _ = ctx.send(retired, BlobSourceIn::Retire);
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
                        RuntimeSource::Available(actor) => Some(*actor),
                        RuntimeSource::Unavailable(_) => None,
                    });
                let result = self.unregister(path, operation_id, retired);
                if result.is_ok()
                    && let Some(retired) = retired
                {
                    let _ = ctx.send(retired, BlobSourceIn::Retire);
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
            DataDirectoryIn::OpenStream {
                request_id,
                path,
                role,
                endpoint,
                descriptor,
                replace,
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
                    descriptor,
                    operation_id,
                    reply_to,
                },
            ),
            DataDirectoryIn::CancelStream { path, operation_id } => {
                self.cancel_stream(&path, operation_id);
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
        }
    }
}

pub trait NamespaceDiscovery: Send + Sync + 'static {
    fn current_directory(&self) -> Option<ActorAddress>;
}

struct PendingRequest {
    request: NamespaceRequest,
    reply_to: ActorAddress,
}

pub struct NamespaceClientActor {
    engine: EngineHandle,
    sender: ExternalSender,
    discovery: Arc<dyn NamespaceDiscovery>,
    retry_period: Duration,
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
        Self {
            engine,
            sender,
            discovery,
            retry_period,
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
                length,
                recovery,
                operation_id,
            } => DataDirectoryIn::Register {
                request_id,
                path: path.clone(),
                source: *source,
                length: *length,
                recovery: recovery.clone(),
                operation_id: *operation_id,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::Resolve { path } => DataDirectoryIn::Resolve {
                request_id,
                path: path.clone(),
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::Unregister { path, operation_id } => DataDirectoryIn::Unregister {
                request_id,
                path: path.clone(),
                operation_id: *operation_id,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::OpenStream {
                path,
                role,
                endpoint,
                descriptor,
                replace,
                operation_id,
            } => DataDirectoryIn::OpenStream {
                request_id,
                path: path.clone(),
                role: *role,
                endpoint: *endpoint,
                descriptor: descriptor.clone(),
                replace: *replace,
                operation_id: *operation_id,
                reply_to: ctx.self_addr(),
            },
            NamespaceRequest::CloseStream { path, incarnation } => DataDirectoryIn::CloseStream {
                request_id,
                path: path.clone(),
                incarnation: *incarnation,
                reply_to: ctx.self_addr(),
            },
        };
        let _ = ctx.send(directory, message);
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
                self.pending
                    .insert(request_id, PendingRequest { request, reply_to });
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
                if let Some(directory) = self.discovery.current_directory() {
                    for (path, operation_id) in cancelled {
                        let _ = ctx.send(
                            directory,
                            DataDirectoryIn::CancelStream { path, operation_id },
                        );
                    }
                }
                self.pending
                    .retain(|_, pending| pending.reply_to != reply_to);
            }
            NamespaceClientIn::DirectoryReply(reply) => {
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
                for (request_id, pending) in &self.pending {
                    self.dispatch(ctx, *request_id, &pending.request);
                }
            }
        }
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
        length: u64,
        recovery: SourceRecovery,
        operation_id: OperationId,
    ) -> Result<MutationReceipt, NamespaceError> {
        match self
            .request(NamespaceRequest::Register {
                path,
                source,
                length,
                recovery,
                operation_id,
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
            let _ = self.runtime.send_to(
                self.directory,
                DataDirectoryIn::CancelStream {
                    path: self.path.clone(),
                    operation_id: self.operation_id,
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
                    length,
                    recovery,
                    operation_id,
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
