//! Host-side session, binding, and arena-allocation actors.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};

use crate::arena::{
    ArenaEvent, ArenaManager, ArenaRequest, LeaseRequestId, LeaseRing, QuiescenceProof, RingId,
    RingSpec,
};
use crate::blob::{
    BLOB_HEADER_LEN, BlobLease, BlobMetadata, ContentDigest, abort_host_blob,
    install_filling_read_blob, install_read_blob, install_writable_blob, mark_host_released,
    seal_host_read_blob, validate_host_sealed, write_host_blob_chunk,
};
use crate::bootstrap::JobHandoff;
use crate::path::{DataPath, JobContext};
use crate::protocol::{
    AttachmentFailure, ChildSessionIn, DataOperation, DataPlaneError, HostSessionIn, JobCapability,
    PublishedBlobInfo,
};

const BLOB_ALIGNMENT: u64 = 64;
const FIRST_BLOB_REQUEST_ID: u64 = 2;

#[derive(Clone)]
pub struct BlobSource {
    bytes: Arc<[u8]>,
    digest: Option<ContentDigest>,
}

impl BlobSource {
    pub fn new(bytes: impl Into<Arc<[u8]>>) -> Self {
        Self {
            bytes: bytes.into(),
            digest: None,
        }
    }

    pub fn with_sha256(bytes: impl Into<Arc<[u8]>>) -> Self {
        let bytes = bytes.into();
        let digest = Some(ContentDigest::sha256(&bytes));
        Self { bytes, digest }
    }

    pub fn length(&self) -> u64 {
        self.bytes.len() as u64
    }

    pub fn digest(&self) -> Option<ContentDigest> {
        self.digest
    }
}

pub trait HostRouteRegistrar: Send + Sync + 'static {
    fn register_child(
        &self,
        child_session: ActorAddress,
        child_node: [u8; 32],
    ) -> Result<(), String>;
}

pub struct HostDataPlaneConfig {
    pub arena: ArenaManager,
    pub arena_generation: u64,
    pub session_generation: u64,
    pub capability: JobCapability,
    pub job_context: JobContext,
    pub blobs: BTreeMap<DataPath, BlobSource>,
    pub route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostSessionState {
    AwaitingAttachment,
    Running,
    Closing,
    Closed,
}

#[derive(Clone)]
struct PublishedBlob {
    binding: ActorAddress,
    lease: BlobLease,
    metadata: BlobMetadata,
}

#[derive(Clone)]
enum BlobEntry {
    Complete(BlobSource),
    Filling {
        source: ActorAddress,
        waiters: HashSet<ActorAddress>,
    },
    Arena {
        source: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
}

pub struct HostDataPlaneSessionActor {
    arena: Option<ArenaManager>,
    arena_generation: u64,
    session_generation: u64,
    capability: JobCapability,
    job_context: JobContext,
    blobs: BTreeMap<DataPath, BlobEntry>,
    route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    child_session: Option<ActorAddress>,
    allocator: Option<ActorAddress>,
    active_bindings: HashSet<ActorAddress>,
    published: BTreeMap<DataPath, PublishedBlob>,
    state: HostSessionState,
}

impl HostDataPlaneSessionActor {
    pub fn new(config: HostDataPlaneConfig) -> Result<Self, DataPlaneError> {
        if config.arena_generation == 0 || config.session_generation == 0 {
            return Err(DataPlaneError::SessionFailed(
                "session and arena generations must be nonzero".to_owned(),
            ));
        }
        config
            .job_context
            .validate()
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        Ok(Self {
            arena: Some(config.arena),
            arena_generation: config.arena_generation,
            session_generation: config.session_generation,
            capability: config.capability,
            job_context: config.job_context,
            blobs: config
                .blobs
                .into_iter()
                .map(|(path, source)| (path, BlobEntry::Complete(source)))
                .collect(),
            route_registrar: config.route_registrar,
            child_session: None,
            allocator: None,
            active_bindings: HashSet::new(),
            published: BTreeMap::new(),
            state: HostSessionState::AwaitingAttachment,
        })
    }

    pub fn state(&self) -> HostSessionState {
        self.state
    }

    fn send_open_failure(
        &self,
        ctx: &Ctx<'_>,
        child_session: ActorAddress,
        operation: ActorAddress,
        error: DataPlaneError,
    ) {
        let _ = ctx.send(
            child_session,
            ChildSessionIn::OperationFailed { operation, error },
        );
    }

    fn validate_open(
        &self,
        child_session: ActorAddress,
        logical: &DataPath,
        operation: DataOperation,
    ) -> Result<DataPath, DataPlaneError> {
        if self.state != HostSessionState::Running || self.child_session != Some(child_session) {
            return Err(DataPlaneError::SessionNotRunning);
        }
        let resolved = self
            .job_context
            .resolve(logical)
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        let authorized = match operation {
            DataOperation::ReadBlob | DataOperation::ReadStream => {
                self.job_context.can_read(&resolved)
            }
            DataOperation::WriteBlob | DataOperation::WriteStream => {
                self.job_context.can_write(&resolved)
            }
        };
        if !authorized {
            return Err(DataPlaneError::Unauthorized {
                path: resolved,
                operation,
            });
        }
        Ok(resolved)
    }

    fn maybe_finish_close(&mut self) {
        if self.state == HostSessionState::Closing && self.active_bindings.is_empty() {
            self.state = HostSessionState::Closed;
        }
    }
}

impl ActorInterface for HostDataPlaneSessionActor {
    type Incoming = HostSessionIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let arena = self.arena.take().expect("host arena is installed once");
        let allocator = ctx
            .spawn(ArenaAllocatorActor::new(arena, self.arena_generation))
            .expect("spawn arena allocator actor");
        self.allocator = Some(allocator);
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostSessionIn) {
        match message {
            HostSessionIn::Attach {
                child_session,
                arena_generation,
                job_capability,
                child_node,
            } => {
                let route_failure = if let (Some(registrar), Some(child_node)) =
                    (&self.route_registrar, child_node)
                {
                    registrar
                        .register_child(child_session, child_node)
                        .err()
                        .map(AttachmentFailure::RouteRejected)
                } else {
                    None
                };
                let failure = if matches!(
                    self.state,
                    HostSessionState::Closing | HostSessionState::Closed
                ) {
                    Some(AttachmentFailure::SessionClosed)
                } else if self.child_session.is_some() {
                    Some(AttachmentFailure::DuplicateAttachment)
                } else if let Some(reason) = route_failure {
                    Some(reason)
                } else if arena_generation != self.arena_generation {
                    Some(AttachmentFailure::ArenaGenerationMismatch {
                        expected: self.arena_generation,
                        found: arena_generation,
                    })
                } else if job_capability != self.capability {
                    Some(AttachmentFailure::CapabilityRejected)
                } else {
                    None
                };

                if let Some(reason) = failure {
                    let _ = ctx.send(
                        child_session,
                        ChildSessionIn::AttachmentFailed {
                            error: DataPlaneError::Attachment(reason),
                        },
                    );
                } else {
                    self.child_session = Some(child_session);
                    self.state = HostSessionState::Running;
                    let _ = ctx.send(
                        child_session,
                        ChildSessionIn::Attached {
                            session_generation: self.session_generation,
                        },
                    );
                }
            }
            HostSessionIn::OpenReadBlob {
                path,
                child_session,
                operation,
            } => {
                let resolved =
                    match self.validate_open(child_session, &path, DataOperation::ReadBlob) {
                        Ok(path) => path,
                        Err(error) => {
                            self.send_open_failure(ctx, child_session, operation, error);
                            return;
                        }
                    };
                let Some(source) = self.blobs.get(&resolved).cloned() else {
                    self.send_open_failure(
                        ctx,
                        child_session,
                        operation,
                        DataPlaneError::PathNotFound(resolved),
                    );
                    return;
                };
                let binding = match source {
                    BlobEntry::Complete(source) => HostBlobBindingActor::read(
                        ctx.self_addr(),
                        self.allocator.expect("allocator started"),
                        child_session,
                        operation,
                        resolved.clone(),
                        source,
                    ),
                    BlobEntry::Filling { source, .. } => HostBlobBindingActor::waiting(
                        ctx.self_addr(),
                        self.allocator.expect("allocator started"),
                        child_session,
                        operation,
                        resolved.clone(),
                        source,
                    ),
                    BlobEntry::Arena {
                        source,
                        lease,
                        metadata,
                    } => HostBlobBindingActor::presealed(
                        ctx.self_addr(),
                        self.allocator.expect("allocator started"),
                        child_session,
                        operation,
                        resolved.clone(),
                        source,
                        lease,
                        metadata,
                    ),
                };
                match ctx.spawn(binding) {
                    Ok(binding) => {
                        self.active_bindings.insert(binding);
                        if let Some(BlobEntry::Filling { waiters, .. }) =
                            self.blobs.get_mut(&resolved)
                        {
                            waiters.insert(binding);
                        }
                    }
                    Err(error) => self.send_open_failure(
                        ctx,
                        child_session,
                        operation,
                        DataPlaneError::SessionFailed(error.to_string()),
                    ),
                }
            }
            HostSessionIn::OpenWriteBlob {
                path,
                length,
                child_session,
                operation,
            } => {
                let resolved =
                    match self.validate_open(child_session, &path, DataOperation::WriteBlob) {
                        Ok(path) => path,
                        Err(error) => {
                            self.send_open_failure(ctx, child_session, operation, error);
                            return;
                        }
                    };
                if self.published.contains_key(&resolved) {
                    self.send_open_failure(
                        ctx,
                        child_session,
                        operation,
                        DataPlaneError::PathAlreadyExists(resolved),
                    );
                    return;
                }
                let binding = HostBlobBindingActor::write(
                    ctx.self_addr(),
                    self.allocator.expect("allocator started"),
                    child_session,
                    operation,
                    resolved,
                    length,
                );
                match ctx.spawn(binding) {
                    Ok(binding) => {
                        self.active_bindings.insert(binding);
                    }
                    Err(error) => self.send_open_failure(
                        ctx,
                        child_session,
                        operation,
                        DataPlaneError::SessionFailed(error.to_string()),
                    ),
                }
            }
            HostSessionIn::ReleaseBlob {
                binding,
                lease_id,
                generation,
            } => {
                if self.active_bindings.contains(&binding) {
                    let _ = ctx.send(
                        binding,
                        HostBindingIn::Release {
                            lease_id,
                            generation,
                        },
                    );
                }
            }
            HostSessionIn::SealWriteBlob {
                binding,
                operation,
                lease,
                metadata,
            } => {
                if self.active_bindings.contains(&binding) {
                    let _ = ctx.send(
                        binding,
                        HostBindingIn::Seal {
                            operation,
                            lease,
                            metadata,
                        },
                    );
                }
            }
            HostSessionIn::AbortWriteBlob {
                binding,
                operation,
                lease_id,
                generation,
            } => {
                if self.active_bindings.contains(&binding) {
                    let _ = ctx.send(
                        binding,
                        HostBindingIn::Abort {
                            operation,
                            lease_id,
                            generation,
                        },
                    );
                }
            }
            HostSessionIn::BindingPublished {
                binding,
                operation,
                path,
                lease,
                metadata,
            } => {
                if !self.active_bindings.contains(&binding) {
                    return;
                }
                if self.published.contains_key(&path) {
                    let _ = ctx.send(
                        binding,
                        HostBindingIn::PublicationRejected(DataPlaneError::PathAlreadyExists(path)),
                    );
                    return;
                }
                self.published.insert(
                    path.clone(),
                    PublishedBlob {
                        binding,
                        lease,
                        metadata,
                    },
                );
                let _ = ctx.send(binding, HostBindingIn::PublicationAccepted { operation });
            }
            HostSessionIn::BindingFaulted {
                binding,
                operation,
                error,
            } => {
                if self.active_bindings.contains(&binding) {
                    if let Some(child) = self.child_session {
                        let _ =
                            ctx.send(child, ChildSessionIn::OperationFailed { operation, error });
                    }
                }
            }
            HostSessionIn::BindingDone { binding } => {
                self.active_bindings.remove(&binding);
                self.published.retain(|_, blob| blob.binding != binding);
                self.maybe_finish_close();
            }
            HostSessionIn::InspectPublished { path, reply_to } => {
                let resolved = self.job_context.resolve(&path).ok();
                let published = resolved.and_then(|path| {
                    self.published.get(&path).map(|blob| PublishedBlobInfo {
                        path,
                        binding: blob.binding,
                        lease: blob.lease,
                        metadata: blob.metadata.clone(),
                    })
                });
                let _ = ctx.send(reply_to, published);
            }
            HostSessionIn::ReleasePublished { path } => {
                if let Ok(path) = self.job_context.resolve(&path) {
                    if let Some(blob) = self.published.remove(&path) {
                        let _ = ctx.send(blob.binding, HostBindingIn::ReleasePublished);
                    }
                }
            }
            HostSessionIn::BeginBlobSource {
                path,
                metadata,
                reply_to,
            } => {
                let resolved = self
                    .job_context
                    .resolve(&path)
                    .map_err(|error| DataPlaneError::InvalidPath(error.to_string()));
                let resolved = match resolved {
                    Ok(path) if self.job_context.can_read(&path) => path,
                    Ok(path) => {
                        let _ = ctx.send(
                            reply_to,
                            Err::<(), _>(DataPlaneError::Unauthorized {
                                path,
                                operation: DataOperation::ReadBlob,
                            }),
                        );
                        return;
                    }
                    Err(error) => {
                        let _ = ctx.send(reply_to, Err::<(), _>(error));
                        return;
                    }
                };
                if self.blobs.contains_key(&resolved) {
                    let _ = ctx.send(
                        reply_to,
                        Err::<(), _>(DataPlaneError::PathAlreadyExists(resolved)),
                    );
                    return;
                }
                let source_actor = IncomingBlobSourceActor::new(
                    ctx.self_addr(),
                    self.allocator.expect("allocator started"),
                    resolved.clone(),
                    metadata,
                    reply_to,
                );
                match ctx.spawn(source_actor) {
                    Ok(source) => {
                        self.blobs.insert(
                            resolved,
                            BlobEntry::Filling {
                                source,
                                waiters: HashSet::new(),
                            },
                        );
                    }
                    Err(error) => {
                        let _ = ctx.send(
                            reply_to,
                            Err::<(), _>(DataPlaneError::SessionFailed(error.to_string())),
                        );
                    }
                }
            }
            HostSessionIn::BlobSourceChunk { path, bytes } => {
                if let Ok(path) = self.job_context.resolve(&path) {
                    if let Some(BlobEntry::Filling { source, .. }) = self.blobs.get(&path) {
                        let _ = ctx.send(*source, IncomingSourceIn::Chunk(bytes));
                    }
                }
            }
            HostSessionIn::FinishBlobSource { path } => {
                if let Ok(path) = self.job_context.resolve(&path) {
                    if let Some(BlobEntry::Filling { source, .. }) = self.blobs.get(&path) {
                        let _ = ctx.send(*source, IncomingSourceIn::Finish);
                    }
                }
            }
            HostSessionIn::FailBlobSource { path, reason } => {
                if let Ok(path) = self.job_context.resolve(&path) {
                    if let Some(BlobEntry::Filling { source, .. }) = self.blobs.get(&path) {
                        let _ = ctx.send(*source, IncomingSourceIn::Fail(reason));
                    }
                }
            }
            HostSessionIn::SourceReady {
                path,
                source,
                lease,
                metadata,
            } => {
                let waiters = match self.blobs.remove(&path) {
                    Some(BlobEntry::Filling {
                        source: expected,
                        waiters,
                    }) if expected == source => waiters,
                    Some(entry) => {
                        self.blobs.insert(path, entry);
                        return;
                    }
                    None => return,
                };
                self.blobs.insert(
                    path,
                    BlobEntry::Arena {
                        source,
                        lease,
                        metadata: metadata.clone(),
                    },
                );
                for binding in waiters {
                    let _ = ctx.send(source, IncomingSourceIn::Acquire { binding });
                }
            }
            HostSessionIn::SourceFaulted {
                path,
                source,
                error,
            } => {
                let waiters = match self.blobs.remove(&path) {
                    Some(BlobEntry::Filling {
                        source: expected,
                        waiters,
                    }) if expected == source => waiters,
                    Some(entry) => {
                        self.blobs.insert(path, entry);
                        return;
                    }
                    None => return,
                };
                for binding in waiters {
                    let _ = ctx.send(binding, HostBindingIn::Allocated(Err(error.clone())));
                }
            }
            HostSessionIn::ConfigureRun { run_id, reply_to } => {
                let result = if self.state != HostSessionState::AwaitingAttachment
                    || self.child_session.is_some()
                {
                    Err(DataPlaneError::SessionNotRunning)
                } else {
                    let mut context = self.job_context.clone();
                    context.run_id = run_id;
                    match context.validate() {
                        Ok(()) => {
                            self.job_context = context;
                            Ok(())
                        }
                        Err(error) => Err(DataPlaneError::InvalidPath(error.to_string())),
                    }
                };
                let _ = ctx.send(reply_to, result);
            }
            HostSessionIn::Close => {
                if matches!(
                    self.state,
                    HostSessionState::Closing | HostSessionState::Closed
                ) {
                    return;
                }
                self.state = HostSessionState::Closing;
                for binding in self.active_bindings.iter().copied() {
                    let _ = ctx.send(binding, HostBindingIn::SessionClosed);
                }
                let mut sources = HashSet::new();
                for entry in self.blobs.values() {
                    match entry {
                        BlobEntry::Filling { source, .. } | BlobEntry::Arena { source, .. } => {
                            sources.insert(*source);
                        }
                        BlobEntry::Complete(_) => {}
                    }
                }
                for source in sources {
                    let _ = ctx.send(source, IncomingSourceIn::SessionClosed);
                }
                self.maybe_finish_close();
            }
        }
    }
}

pub fn install_session_env(
    handoff: &mut JobHandoff,
    host_session: ActorAddress,
    capability: JobCapability,
) {
    handoff.env.insert(
        crate::bootstrap::ENV_DATA_PLANE_ACTOR.to_owned(),
        host_session.to_full_hex(),
    );
    handoff.env.insert(
        crate::bootstrap::ENV_JOB_CAPABILITY.to_owned(),
        capability.to_hex(),
    );
}

#[derive(Clone)]
enum AllocationKind {
    Read {
        bytes: Arc<[u8]>,
        digest: Option<ContentDigest>,
    },
    FillingRead {
        length: u64,
        digest: Option<ContentDigest>,
    },
    Write {
        length: u64,
        digest: Option<ContentDigest>,
    },
}

#[derive(Clone)]
enum ArenaAllocatorIn {
    Allocate {
        binding: ActorAddress,
        kind: AllocationKind,
    },
    AllocateSource {
        source: ActorAddress,
        kind: AllocationKind,
    },
    ValidateSealed {
        binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    WriteSource {
        source: ActorAddress,
        lease: BlobLease,
        offset: u64,
        bytes: Vec<u8>,
    },
    SealSource {
        source: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
        written: u64,
    },
    AbortSource {
        source: ActorAddress,
        lease: BlobLease,
    },
    ReleaseSource {
        source: ActorAddress,
        lease: BlobLease,
    },
    Release {
        binding: ActorAddress,
        lease: BlobLease,
    },
}

struct ArenaAllocatorActor {
    arena: ArenaManager,
    next_request_id: u64,
    next_generation: u64,
}

impl ArenaAllocatorActor {
    fn new(arena: ArenaManager, arena_generation: u64) -> Self {
        Self {
            arena,
            next_request_id: FIRST_BLOB_REQUEST_ID,
            next_generation: arena_generation,
        }
    }

    fn allocate(
        &mut self,
        kind: AllocationKind,
    ) -> Result<(BlobLease, BlobMetadata), DataPlaneError> {
        let length = match &kind {
            AllocationKind::Read { bytes, .. } => bytes.len() as u64,
            AllocationKind::Write { length, .. } | AllocationKind::FillingRead { length, .. } => {
                *length
            }
        };
        let request_id = self.next_request_id;
        self.next_request_id = self
            .next_request_id
            .checked_add(1)
            .ok_or_else(|| DataPlaneError::SessionFailed("blob request id exhausted".to_owned()))?;
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or_else(|| DataPlaneError::SessionFailed("blob generation exhausted".to_owned()))?;
        let mut events = self.arena.request(ArenaRequest::LeaseRing(LeaseRing {
            request_id: LeaseRequestId(request_id),
            ring_spec: RingSpec {
                header_bytes: BLOB_HEADER_LEN,
                data_bytes: length,
                alignment: BLOB_ALIGNMENT,
            },
        }));
        let allocation = match (events.len(), events.pop()) {
            (1, Some(ArenaEvent::RingLeased { lease })) => lease,
            (1, Some(ArenaEvent::RingLeaseQueued { request_id })) => {
                self.arena.request(ArenaRequest::CancelLease { request_id });
                return Err(DataPlaneError::ArenaExhausted);
            }
            (1, Some(ArenaEvent::RingLeaseRejected { .. })) => {
                return Err(DataPlaneError::ArenaExhausted);
            }
            _ => {
                return Err(DataPlaneError::SessionFailed(
                    "unexpected blob arena allocation outcome".to_owned(),
                ));
            }
        };

        let installed = match kind {
            AllocationKind::Read { bytes, digest } => {
                install_read_blob(&self.arena, &allocation, generation, &bytes, digest)
            }
            AllocationKind::Write { length, digest } => {
                install_writable_blob(&self.arena, &allocation, generation, length, digest)
            }
            AllocationKind::FillingRead { length, digest } => {
                install_filling_read_blob(&self.arena, &allocation, generation, length, digest)
            }
        };
        match installed {
            Ok(grant) => Ok(grant),
            Err(error) => {
                let _ = self.arena.request(ArenaRequest::ReleaseRing {
                    ring_id: allocation.ring_id,
                    proof: QuiescenceProof::verified(),
                });
                Err(error.into())
            }
        }
    }

    fn release(&mut self, lease: BlobLease) -> Result<(), DataPlaneError> {
        let ring_id = RingId(lease.lease_id.0);
        let Some(allocation) = self.arena.lookup_lease(ring_id) else {
            return Err(DataPlaneError::Blob(
                crate::protocol::BlobFailure::InvalidLease,
            ));
        };
        if allocation.layout.start_offset != lease.offset
            || allocation.layout.data_bytes != lease.length
        {
            return Err(DataPlaneError::Blob(
                crate::protocol::BlobFailure::InvalidLease,
            ));
        }
        mark_host_released(&self.arena, lease)?;
        let events = self.arena.request(ArenaRequest::ReleaseRing {
            ring_id,
            proof: QuiescenceProof::verified(),
        });
        if matches!(events.as_slice(), [ArenaEvent::RingReleased { .. }]) {
            Ok(())
        } else {
            Err(DataPlaneError::SessionFailed(
                "arena rejected blob release".to_owned(),
            ))
        }
    }
}

impl ActorInterface for ArenaAllocatorActor {
    type Incoming = ArenaAllocatorIn;
    type Response = ();

    fn handle(&mut self, ctx: &Ctx<'_>, message: ArenaAllocatorIn) {
        match message {
            ArenaAllocatorIn::Allocate { binding, kind } => {
                let result = self.allocate(kind);
                let _ = ctx.send(binding, HostBindingIn::Allocated(result));
            }
            ArenaAllocatorIn::AllocateSource { source, kind } => {
                let result = self.allocate(kind);
                let _ = ctx.send(source, IncomingSourceIn::Allocated(result));
            }
            ArenaAllocatorIn::ValidateSealed {
                binding,
                lease,
                metadata,
            } => {
                let result =
                    validate_host_sealed(&self.arena, lease, &metadata).map_err(Into::into);
                let _ = ctx.send(binding, HostBindingIn::SealValidated(result));
            }
            ArenaAllocatorIn::WriteSource {
                source,
                lease,
                offset,
                bytes,
            } => {
                if let Err(error) = write_host_blob_chunk(&self.arena, lease, offset, &bytes) {
                    let _ = ctx.send(source, IncomingSourceIn::AllocatorFault(error.into()));
                }
            }
            ArenaAllocatorIn::SealSource {
                source,
                lease,
                metadata,
                written,
            } => {
                let result =
                    seal_host_read_blob(&self.arena, lease, &metadata, written).map_err(Into::into);
                let _ = ctx.send(source, IncomingSourceIn::Sealed(result));
            }
            ArenaAllocatorIn::AbortSource { source, lease } => {
                let result = abort_host_blob(&self.arena, lease)
                    .map_err(DataPlaneError::from)
                    .and_then(|()| self.release(lease));
                let _ = ctx.send(source, IncomingSourceIn::Released(result));
            }
            ArenaAllocatorIn::ReleaseSource { source, lease } => {
                let result = self.release(lease);
                let _ = ctx.send(source, IncomingSourceIn::Released(result));
            }
            ArenaAllocatorIn::Release { binding, lease } => {
                let result = self.release(lease);
                let _ = ctx.send(binding, HostBindingIn::Released(result));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum IncomingSourceState {
    Allocating,
    Filling,
    Sealing,
    Sealed,
    Releasing,
    Faulted,
}

#[derive(Clone)]
enum IncomingSourceIn {
    Allocated(Result<(BlobLease, BlobMetadata), DataPlaneError>),
    Chunk(Vec<u8>),
    Finish,
    Fail(String),
    Sealed(Result<(), DataPlaneError>),
    AllocatorFault(DataPlaneError),
    Acquire { binding: ActorAddress },
    ReleaseGrant { binding: ActorAddress },
    Released(Result<(), DataPlaneError>),
    SessionClosed,
}

struct IncomingBlobSourceActor {
    host_session: ActorAddress,
    allocator: ActorAddress,
    path: DataPath,
    requested: BlobMetadata,
    ready_reply: Option<ActorAddress>,
    lease: Option<BlobLease>,
    metadata: Option<BlobMetadata>,
    written: u64,
    grants: HashSet<ActorAddress>,
    state: IncomingSourceState,
}

impl IncomingBlobSourceActor {
    fn new(
        host_session: ActorAddress,
        allocator: ActorAddress,
        path: DataPath,
        requested: BlobMetadata,
        ready_reply: ActorAddress,
    ) -> Self {
        Self {
            host_session,
            allocator,
            path,
            requested,
            ready_reply: Some(ready_reply),
            lease: None,
            metadata: None,
            written: 0,
            grants: HashSet::new(),
            state: IncomingSourceState::Allocating,
        }
    }

    fn fault(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        self.state = IncomingSourceState::Faulted;
        if let Some(reply_to) = self.ready_reply.take() {
            let _ = ctx.send(reply_to, Err::<(), _>(error.clone()));
        }
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::SourceFaulted {
                path: self.path.clone(),
                source: ctx.self_addr(),
                error,
            },
        );
        if let Some(lease) = self.lease {
            self.state = IncomingSourceState::Releasing;
            let _ = ctx.send(
                self.allocator,
                ArenaAllocatorIn::AbortSource {
                    source: ctx.self_addr(),
                    lease,
                },
            );
        } else {
            ctx.stop_self();
        }
    }
}

impl ActorInterface for IncomingBlobSourceActor {
    type Incoming = IncomingSourceIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.allocator,
            ArenaAllocatorIn::AllocateSource {
                source: ctx.self_addr(),
                kind: AllocationKind::FillingRead {
                    length: self.requested.length,
                    digest: self.requested.digest,
                },
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: IncomingSourceIn) {
        match message {
            IncomingSourceIn::Allocated(Ok((lease, metadata)))
                if self.state == IncomingSourceState::Allocating =>
            {
                self.lease = Some(lease);
                self.metadata = Some(metadata);
                self.state = IncomingSourceState::Filling;
                if let Some(reply_to) = self.ready_reply.take() {
                    let _ = ctx.send(reply_to, Ok::<_, DataPlaneError>(()));
                }
            }
            IncomingSourceIn::Allocated(Err(error))
                if self.state == IncomingSourceState::Allocating =>
            {
                self.fault(ctx, error);
            }
            IncomingSourceIn::Chunk(bytes) if self.state == IncomingSourceState::Filling => {
                let count = bytes.len() as u64;
                let Some(next_written) = self
                    .written
                    .checked_add(count)
                    .filter(|written| *written <= self.requested.length)
                else {
                    self.fault(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::Length {
                            expected: self.requested.length,
                            found: self.written.saturating_add(count),
                        }),
                    );
                    return;
                };
                let lease = self.lease.expect("filling source lease");
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::WriteSource {
                        source: ctx.self_addr(),
                        lease,
                        offset: self.written,
                        bytes,
                    },
                );
                self.written = next_written;
            }
            IncomingSourceIn::Finish if self.state == IncomingSourceState::Filling => {
                self.state = IncomingSourceState::Sealing;
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::SealSource {
                        source: ctx.self_addr(),
                        lease: self.lease.expect("sealing source lease"),
                        metadata: self.metadata.clone().expect("sealing source metadata"),
                        written: self.written,
                    },
                );
            }
            IncomingSourceIn::Fail(reason)
                if matches!(
                    self.state,
                    IncomingSourceState::Allocating | IncomingSourceState::Filling
                ) =>
            {
                self.fault(ctx, DataPlaneError::SourceFailure(reason));
            }
            IncomingSourceIn::Sealed(Ok(())) if self.state == IncomingSourceState::Sealing => {
                self.state = IncomingSourceState::Sealed;
                let _ = ctx.send(
                    self.host_session,
                    HostSessionIn::SourceReady {
                        path: self.path.clone(),
                        source: ctx.self_addr(),
                        lease: self.lease.expect("sealed source lease"),
                        metadata: self.metadata.clone().expect("sealed source metadata"),
                    },
                );
            }
            IncomingSourceIn::Sealed(Err(error)) if self.state == IncomingSourceState::Sealing => {
                self.fault(ctx, error);
            }
            IncomingSourceIn::AllocatorFault(error) => self.fault(ctx, error),
            IncomingSourceIn::Acquire { binding } if self.state == IncomingSourceState::Sealed => {
                self.grants.insert(binding);
                let _ = ctx.send(
                    binding,
                    HostBindingIn::Presealed {
                        source: ctx.self_addr(),
                        lease: self.lease.expect("sealed source lease"),
                        metadata: self.metadata.clone().expect("sealed source metadata"),
                    },
                );
            }
            IncomingSourceIn::ReleaseGrant { binding }
                if self.state == IncomingSourceState::Sealed =>
            {
                self.grants.remove(&binding);
                let _ = ctx.send(binding, HostBindingIn::SourceReleased);
            }
            IncomingSourceIn::SessionClosed
                if !matches!(
                    self.state,
                    IncomingSourceState::Releasing | IncomingSourceState::Faulted
                ) =>
            {
                if let Some(lease) = self.lease {
                    self.state = IncomingSourceState::Releasing;
                    let _ = ctx.send(
                        self.allocator,
                        ArenaAllocatorIn::ReleaseSource {
                            source: ctx.self_addr(),
                            lease,
                        },
                    );
                } else {
                    ctx.stop_self();
                }
            }
            IncomingSourceIn::Released(result) if self.state == IncomingSourceState::Releasing => {
                if let Err(error) = result {
                    let _ = ctx.send(
                        self.host_session,
                        HostSessionIn::SourceFaulted {
                            path: self.path.clone(),
                            source: ctx.self_addr(),
                            error,
                        },
                    );
                }
                ctx.stop_self();
            }
            _ => {}
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HostBindingState {
    Allocated,
    Filling,
    Granted,
    Sealing,
    Publishing,
    Published,
    Releasing,
    Released,
    Faulted,
}

#[derive(Clone)]
enum BindingMode {
    Read { source: BlobSource },
    ArenaSource { source: ActorAddress },
    Write { length: u64 },
}

#[derive(Clone)]
enum ReleaseOutcome {
    ReadReleased,
    WriteAborted { operation: ActorAddress },
    PublishedReleased,
    SessionClosed,
    Faulted,
}

#[derive(Clone)]
enum HostBindingIn {
    Allocated(Result<(BlobLease, BlobMetadata), DataPlaneError>),
    Presealed {
        source: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    SourceReleased,
    Release {
        lease_id: crate::ids::BlobLeaseId,
        generation: u64,
    },
    Seal {
        operation: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    SealValidated(Result<(), DataPlaneError>),
    Abort {
        operation: ActorAddress,
        lease_id: crate::ids::BlobLeaseId,
        generation: u64,
    },
    PublicationAccepted {
        operation: ActorAddress,
    },
    PublicationRejected(DataPlaneError),
    ReleasePublished,
    SessionClosed,
    Released(Result<(), DataPlaneError>),
}

struct HostBlobBindingActor {
    host_session: ActorAddress,
    allocator: ActorAddress,
    child_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    mode: BindingMode,
    state: HostBindingState,
    lease: Option<BlobLease>,
    metadata: Option<BlobMetadata>,
    release_outcome: Option<ReleaseOutcome>,
}

impl HostBlobBindingActor {
    fn read(
        host_session: ActorAddress,
        allocator: ActorAddress,
        child_session: ActorAddress,
        operation: ActorAddress,
        path: DataPath,
        source: BlobSource,
    ) -> Self {
        Self {
            host_session,
            allocator,
            child_session,
            operation,
            path,
            mode: BindingMode::Read { source },
            state: HostBindingState::Allocated,
            lease: None,
            metadata: None,
            release_outcome: None,
        }
    }

    fn waiting(
        host_session: ActorAddress,
        allocator: ActorAddress,
        child_session: ActorAddress,
        operation: ActorAddress,
        path: DataPath,
        source: ActorAddress,
    ) -> Self {
        Self {
            host_session,
            allocator,
            child_session,
            operation,
            path,
            mode: BindingMode::ArenaSource { source },
            state: HostBindingState::Allocated,
            lease: None,
            metadata: None,
            release_outcome: None,
        }
    }

    fn presealed(
        host_session: ActorAddress,
        allocator: ActorAddress,
        child_session: ActorAddress,
        operation: ActorAddress,
        path: DataPath,
        source: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) -> Self {
        Self {
            host_session,
            allocator,
            child_session,
            operation,
            path,
            mode: BindingMode::ArenaSource { source },
            state: HostBindingState::Allocated,
            lease: Some(lease),
            metadata: Some(metadata),
            release_outcome: None,
        }
    }

    fn write(
        host_session: ActorAddress,
        allocator: ActorAddress,
        child_session: ActorAddress,
        operation: ActorAddress,
        path: DataPath,
        length: u64,
    ) -> Self {
        Self {
            host_session,
            allocator,
            child_session,
            operation,
            path,
            mode: BindingMode::Write { length },
            state: HostBindingState::Allocated,
            lease: None,
            metadata: None,
            release_outcome: None,
        }
    }

    fn begin_release(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        let Some(lease) = self.lease else {
            self.finish_without_lease(ctx, outcome);
            return;
        };
        self.state = HostBindingState::Releasing;
        self.release_outcome = Some(outcome);
        match self.mode {
            BindingMode::ArenaSource { source } => {
                let _ = ctx.send(
                    source,
                    IncomingSourceIn::ReleaseGrant {
                        binding: ctx.self_addr(),
                    },
                );
            }
            BindingMode::Read { .. } | BindingMode::Write { .. } => {
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::Release {
                        binding: ctx.self_addr(),
                        lease,
                    },
                );
            }
        }
    }

    fn finish_without_lease(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        self.state = HostBindingState::Released;
        if let ReleaseOutcome::WriteAborted { operation } = outcome {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::WriteAborted { operation },
            );
        }
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::BindingDone {
                binding: ctx.self_addr(),
            },
        );
        ctx.stop_self();
    }

    fn fail(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        self.state = HostBindingState::Faulted;
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::BindingFaulted {
                binding: ctx.self_addr(),
                operation: self.operation,
                error,
            },
        );
        if self.lease.is_some() {
            self.begin_release(ctx, ReleaseOutcome::Faulted);
        } else {
            self.finish_without_lease(ctx, ReleaseOutcome::Faulted);
        }
    }

    fn lease_matches(&self, lease_id: crate::ids::BlobLeaseId, generation: u64) -> bool {
        self.lease
            .is_some_and(|lease| lease.lease_id == lease_id && lease.generation == generation)
    }
}

impl ActorInterface for HostBlobBindingActor {
    type Incoming = HostBindingIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.state = HostBindingState::Filling;
        let kind = match &self.mode {
            BindingMode::Read { source } => Some(AllocationKind::Read {
                bytes: source.bytes.clone(),
                digest: source.digest,
            }),
            BindingMode::Write { length } => Some(AllocationKind::Write {
                length: *length,
                digest: None,
            }),
            BindingMode::ArenaSource { source } => {
                let _ = ctx.send(
                    *source,
                    IncomingSourceIn::Acquire {
                        binding: ctx.self_addr(),
                    },
                );
                None
            }
        };
        if let Some(kind) = kind {
            let _ = ctx.send(
                self.allocator,
                ArenaAllocatorIn::Allocate {
                    binding: ctx.self_addr(),
                    kind,
                },
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostBindingIn) {
        match message {
            HostBindingIn::Allocated(Ok((lease, metadata)))
                if self.state == HostBindingState::Filling =>
            {
                self.lease = Some(lease);
                self.metadata = Some(metadata.clone());
                self.state = HostBindingState::Granted;
                let response = match self.mode {
                    BindingMode::Read { .. } => ChildSessionIn::BlobOpened {
                        operation: self.operation,
                        host_binding: ctx.self_addr(),
                        lease,
                        metadata,
                    },
                    BindingMode::Write { .. } => ChildSessionIn::WriteBlobOpened {
                        operation: self.operation,
                        host_binding: ctx.self_addr(),
                        lease,
                        metadata,
                    },
                    BindingMode::ArenaSource { .. } => ChildSessionIn::BlobOpened {
                        operation: self.operation,
                        host_binding: ctx.self_addr(),
                        lease,
                        metadata,
                    },
                };
                let _ = ctx.send(self.child_session, response);
            }
            HostBindingIn::Presealed {
                source,
                lease,
                metadata,
            } if self.state == HostBindingState::Filling
                && matches!(
                    self.mode,
                    BindingMode::ArenaSource {
                        source: expected
                    } if expected == source
                ) =>
            {
                if self.lease.is_some_and(|expected| expected != lease)
                    || self
                        .metadata
                        .as_ref()
                        .is_some_and(|expected| expected != &metadata)
                {
                    self.fail(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
                    );
                    return;
                }
                self.lease = Some(lease);
                self.metadata = Some(metadata.clone());
                self.state = HostBindingState::Granted;
                let _ = ctx.send(
                    self.child_session,
                    ChildSessionIn::BlobOpened {
                        operation: self.operation,
                        host_binding: ctx.self_addr(),
                        lease,
                        metadata,
                    },
                );
            }
            HostBindingIn::Allocated(Err(error)) if self.state == HostBindingState::Filling => {
                self.fail(ctx, error);
            }
            HostBindingIn::Release {
                lease_id,
                generation,
            } if self.state == HostBindingState::Granted
                && matches!(
                    self.mode,
                    BindingMode::Read { .. } | BindingMode::ArenaSource { .. }
                ) =>
            {
                if self.lease_matches(lease_id, generation) {
                    self.begin_release(ctx, ReleaseOutcome::ReadReleased);
                }
            }
            HostBindingIn::Seal {
                operation,
                lease,
                metadata,
            } if self.state == HostBindingState::Granted
                && matches!(self.mode, BindingMode::Write { .. }) =>
            {
                if operation != self.operation
                    || self.lease != Some(lease)
                    || self.metadata.as_ref() != Some(&metadata)
                {
                    self.fail(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::InvalidLease),
                    );
                    return;
                }
                self.state = HostBindingState::Sealing;
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::ValidateSealed {
                        binding: ctx.self_addr(),
                        lease,
                        metadata,
                    },
                );
            }
            HostBindingIn::SealValidated(Ok(())) if self.state == HostBindingState::Sealing => {
                let lease = self.lease.expect("validated write lease");
                let metadata = self.metadata.clone().expect("validated write metadata");
                self.state = HostBindingState::Publishing;
                let _ = ctx.send(
                    self.host_session,
                    HostSessionIn::BindingPublished {
                        binding: ctx.self_addr(),
                        operation: self.operation,
                        path: self.path.clone(),
                        lease,
                        metadata,
                    },
                );
            }
            HostBindingIn::SealValidated(Err(error)) if self.state == HostBindingState::Sealing => {
                self.fail(ctx, error);
            }
            HostBindingIn::Abort {
                operation,
                lease_id,
                generation,
            } if self.state == HostBindingState::Granted
                && matches!(self.mode, BindingMode::Write { .. }) =>
            {
                if operation == self.operation && self.lease_matches(lease_id, generation) {
                    self.begin_release(ctx, ReleaseOutcome::WriteAborted { operation });
                }
            }
            HostBindingIn::PublicationAccepted { operation }
                if self.state == HostBindingState::Publishing && operation == self.operation =>
            {
                self.state = HostBindingState::Published;
                let _ = ctx.send(
                    self.child_session,
                    ChildSessionIn::WritePublished { operation },
                );
            }
            HostBindingIn::PublicationRejected(error)
                if self.state == HostBindingState::Publishing =>
            {
                self.fail(ctx, error);
            }
            HostBindingIn::ReleasePublished if self.state == HostBindingState::Published => {
                self.begin_release(ctx, ReleaseOutcome::PublishedReleased);
            }
            HostBindingIn::SessionClosed
                if !matches!(
                    self.state,
                    HostBindingState::Released | HostBindingState::Releasing
                ) =>
            {
                self.begin_release(ctx, ReleaseOutcome::SessionClosed);
            }
            HostBindingIn::Released(result) if self.state == HostBindingState::Releasing => {
                if let Err(error) = result {
                    let _ = ctx.send(
                        self.host_session,
                        HostSessionIn::BindingFaulted {
                            binding: ctx.self_addr(),
                            operation: self.operation,
                            error,
                        },
                    );
                }
                let outcome = self
                    .release_outcome
                    .take()
                    .unwrap_or(ReleaseOutcome::Faulted);
                self.finish_without_lease(ctx, outcome);
            }
            HostBindingIn::SourceReleased if self.state == HostBindingState::Releasing => {
                let outcome = self
                    .release_outcome
                    .take()
                    .unwrap_or(ReleaseOutcome::ReadReleased);
                self.finish_without_lease(ctx, outcome);
            }
            _ => {}
        }
    }
}
