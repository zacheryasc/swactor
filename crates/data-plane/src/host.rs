//! Host-side session, binding, and arena-allocation actors.

use std::collections::HashSet;
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::Runtime;

use crate::arena::{
    ArenaEvent, ArenaManager, ArenaRequest, LeaseRequestId, LeaseRing, QuiescenceProof, RingId,
    RingSpec,
};
use crate::blob::{
    BLOB_HEADER_LEN, BlobLease, BlobMetadata, ContentDigest, abort_host_blob,
    install_filling_read_blob, install_writable_blob, mark_host_released, seal_host_read_blob,
    validate_host_sealed, write_host_blob_chunk,
};
use crate::blob_transfer::{
    BlobTransferEvent, BlobTransferId, BlobTransferOffer, BlobTransferReceiver, BlobTransferSender,
};
use crate::bootstrap::JobHandoff;
use crate::namespace::{
    BlobBinding as NamespaceBlobBinding, DataDirectoryOut, NamespaceClient, NamespaceClientIn,
    NamespaceError, NamespaceRequest, OperationId, SourceRecovery,
};
use crate::path::{DataPath, JobContext};
use crate::protocol::{
    AttachmentFailure, ChildSessionIn, DataOperation, DataPlaneError, HostSessionIn, JobCapability,
};
use crate::source::{BlobSourceIn, BlobSourcePublisher, BlobSourceRetirement, FileBlobSourceActor};

const BLOB_ALIGNMENT: u64 = 64;
const FIRST_BLOB_REQUEST_ID: u64 = 2;

pub trait HostRouteRegistrar: Send + Sync + 'static {
    fn register_child(
        &self,
        child_session: ActorAddress,
        child_node: [u8; 32],
    ) -> Result<(), String>;
}

pub struct HostDataPlaneConfig {
    pub runtime: Runtime,
    pub arena: ArenaManager,
    pub arena_generation: u64,
    pub session_generation: u64,
    pub capability: JobCapability,
    pub job_context: JobContext,
    pub namespace: Option<NamespaceClient>,
    pub transfer_receiver: Option<Arc<dyn BlobTransferReceiver>>,
    pub source_sender: Option<Arc<dyn BlobTransferSender>>,
    pub source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    pub route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostSessionState {
    AwaitingAttachment,
    Running,
    Closing,
    Closed,
}

pub struct HostDataPlaneSessionActor {
    arena: Option<ArenaManager>,
    arena_generation: u64,
    session_generation: u64,
    capability: JobCapability,
    job_context: JobContext,
    namespace: Option<NamespaceClient>,
    transfer_receiver: Option<Arc<dyn BlobTransferReceiver>>,
    runtime: Runtime,
    source_sender: Option<Arc<dyn BlobTransferSender>>,
    source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    child_session: Option<ActorAddress>,
    allocator: Option<ActorAddress>,
    active_bindings: HashSet<ActorAddress>,
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
            namespace: config.namespace,
            transfer_receiver: config.transfer_receiver,
            runtime: config.runtime,
            source_sender: config.source_sender,
            source_publisher: config.source_publisher,
            route_registrar: config.route_registrar,
            child_session: None,
            allocator: None,
            active_bindings: HashSet::new(),
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
            .spawn(ArenaAllocatorActor::new(
                arena,
                self.arena_generation,
                self.runtime.clone(),
                self.source_sender.clone(),
            ))
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
                let (Some(namespace), Some(receiver)) = (&self.namespace, &self.transfer_receiver)
                else {
                    self.send_open_failure(
                        ctx,
                        child_session,
                        operation,
                        DataPlaneError::SessionFailed(
                            "data namespace service is unavailable".to_owned(),
                        ),
                    );
                    return;
                };
                let binding = HostBlobBindingActor::namespace_read(
                    HostBindingAddresses {
                        host_session: ctx.self_addr(),
                        allocator: self.allocator.expect("allocator started"),
                        child_session,
                        operation,
                    },
                    resolved,
                    namespace.clone(),
                    Arc::clone(receiver),
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
            HostSessionIn::CancelReadBlob { operation } => {
                for binding in self.active_bindings.iter().copied() {
                    let _ = ctx.send(binding, HostBindingIn::CancelRead { operation });
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
                let binding = HostBlobBindingActor::write(
                    HostBindingAddresses {
                        host_session: ctx.self_addr(),
                        allocator: self.allocator.expect("allocator started"),
                        child_session,
                        operation,
                    },
                    resolved,
                    length,
                    self.namespace.clone(),
                    self.source_publisher.clone(),
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
            HostSessionIn::BindingFaulted {
                binding,
                operation,
                error,
            } => {
                if self.active_bindings.contains(&binding)
                    && let Some(child) = self.child_session
                {
                    let _ = ctx.send(child, ChildSessionIn::OperationFailed { operation, error });
                }
            }
            HostSessionIn::BindingDone { binding } | HostSessionIn::BindingDetached { binding } => {
                self.active_bindings.remove(&binding);
                self.maybe_finish_close();
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
    AllocateTransfer {
        transfer: ActorAddress,
        kind: AllocationKind,
    },
    ValidateSealed {
        binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    WriteTransfer {
        transfer: ActorAddress,
        lease: BlobLease,
        offset: u64,
        bytes: Vec<u8>,
    },
    SealTransfer {
        transfer: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
        written: u64,
    },
    AbortTransfer {
        transfer: ActorAddress,
        lease: BlobLease,
    },
    CreatePublishedSource {
        binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    Release {
        binding: ActorAddress,
        lease: BlobLease,
    },
}

struct ArenaSourceRetirement {
    runtime: Runtime,
    binding: ActorAddress,
}

impl BlobSourceRetirement for ArenaSourceRetirement {
    fn retired(&self) {
        let _ = self
            .runtime
            .send_to(self.binding, HostBindingIn::ReleasePublished);
    }
}

struct ArenaAllocatorActor {
    arena: ArenaManager,
    next_request_id: u64,
    next_generation: u64,
    runtime: Runtime,
    source_sender: Option<Arc<dyn BlobTransferSender>>,
}

impl ArenaAllocatorActor {
    fn new(
        arena: ArenaManager,
        arena_generation: u64,
        runtime: Runtime,
        source_sender: Option<Arc<dyn BlobTransferSender>>,
    ) -> Self {
        Self {
            arena,
            next_request_id: FIRST_BLOB_REQUEST_ID,
            next_generation: arena_generation,
            runtime,
            source_sender,
        }
    }

    fn allocate(
        &mut self,
        kind: AllocationKind,
    ) -> Result<(BlobLease, BlobMetadata), DataPlaneError> {
        let length = match &kind {
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

    fn create_published_source(
        &self,
        ctx: &Ctx<'_>,
        binding: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) -> Result<ActorAddress, DataPlaneError> {
        let sender = self.source_sender.as_ref().ok_or_else(|| {
            DataPlaneError::SessionFailed("blob source transfer service is unavailable".to_owned())
        })?;
        let fd = unsafe { libc::dup(self.arena.arena_fd()) };
        if fd < 0 {
            return Err(DataPlaneError::SessionFailed(format!(
                "duplicate arena backing: {}",
                std::io::Error::last_os_error()
            )));
        }
        let owned = unsafe { OwnedFd::from_raw_fd(fd) };
        let file = std::fs::File::from(owned);
        let offset = lease.offset.checked_add(BLOB_HEADER_LEN).ok_or_else(|| {
            DataPlaneError::SessionFailed("blob payload offset overflow".to_owned())
        })?;
        let retirement: Arc<dyn BlobSourceRetirement> = Arc::new(ArenaSourceRetirement {
            runtime: self.runtime.clone(),
            binding,
        });
        let source = FileBlobSourceActor::from_file_region(
            self.runtime.clone(),
            Arc::clone(sender),
            file,
            offset,
            metadata.length,
            Some(retirement),
        )
        .map_err(namespace_error)?;
        ctx.spawn(source)
            .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))
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
            ArenaAllocatorIn::AllocateTransfer { transfer, kind } => {
                let result = self.allocate(kind);
                let _ = ctx.send(transfer, BlobTransferEvent::Allocated(result));
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
            ArenaAllocatorIn::WriteTransfer {
                transfer,
                lease,
                offset,
                bytes,
            } => {
                if let Err(error) = write_host_blob_chunk(&self.arena, lease, offset, &bytes) {
                    let _ = ctx.send(transfer, BlobTransferEvent::AllocatorFailed(error.into()));
                }
            }
            ArenaAllocatorIn::SealTransfer {
                transfer,
                lease,
                metadata,
                written,
            } => {
                let result =
                    seal_host_read_blob(&self.arena, lease, &metadata, written).map_err(Into::into);
                let _ = ctx.send(transfer, BlobTransferEvent::Sealed(result));
            }
            ArenaAllocatorIn::AbortTransfer { transfer, lease } => {
                let result = abort_host_blob(&self.arena, lease)
                    .map_err(DataPlaneError::from)
                    .and_then(|()| self.release(lease));
                let _ = ctx.send(transfer, BlobTransferEvent::Released(result));
            }
            ArenaAllocatorIn::CreatePublishedSource {
                binding,
                lease,
                metadata,
            } => {
                let result = self.create_published_source(ctx, binding, lease, metadata);
                let _ = ctx.send(binding, HostBindingIn::PublishedSource(result));
            }
            ArenaAllocatorIn::Release { binding, lease } => {
                let result = self.release(lease);
                let _ = ctx.send(binding, HostBindingIn::Released(result));
            }
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DestinationTransferState {
    Allocating,
    Filling,
    Sealing,
    Releasing,
    Finished,
}

struct DestinationBlobTransferActor {
    allocator: ActorAddress,
    binding: ActorAddress,
    receiver: Arc<dyn BlobTransferReceiver>,
    failure_proxy: ActorAddress,
    source: ActorAddress,
    length: u64,
    transfer_id: BlobTransferId,
    lease: Option<BlobLease>,
    metadata: Option<BlobMetadata>,
    offer: Option<BlobTransferOffer>,
    written: u64,
    pending_error: Option<DataPlaneError>,
    state: DestinationTransferState,
}

impl DestinationBlobTransferActor {
    fn new(
        allocator: ActorAddress,
        binding: ActorAddress,
        receiver: Arc<dyn BlobTransferReceiver>,
        failure_proxy: ActorAddress,
        source: ActorAddress,
        length: u64,
        transfer_id: BlobTransferId,
    ) -> Self {
        Self {
            allocator,
            binding,
            receiver,
            failure_proxy,
            source,
            length,
            transfer_id,
            lease: None,
            metadata: None,
            offer: None,
            written: 0,
            pending_error: None,
            state: DestinationTransferState::Allocating,
        }
    }

    fn finish_failure(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        self.state = DestinationTransferState::Finished;
        let _ = ctx.send(self.binding, HostBindingIn::TransferFailed(error));
        ctx.stop_self();
    }

    fn fault(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if matches!(
            self.state,
            DestinationTransferState::Releasing | DestinationTransferState::Finished
        ) {
            return;
        }
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
        if let Some(lease) = self.lease {
            self.pending_error = Some(error);
            self.state = DestinationTransferState::Releasing;
            let _ = ctx.send(
                self.allocator,
                ArenaAllocatorIn::AbortTransfer {
                    transfer: ctx.self_addr(),
                    lease,
                },
            );
        } else {
            self.finish_failure(ctx, error);
        }
    }
}

impl ActorInterface for DestinationBlobTransferActor {
    type Incoming = BlobTransferEvent;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.allocator,
            ArenaAllocatorIn::AllocateTransfer {
                transfer: ctx.self_addr(),
                kind: AllocationKind::FillingRead {
                    length: self.length,
                    digest: None,
                },
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: BlobTransferEvent) {
        match message {
            BlobTransferEvent::Allocated(Ok((lease, metadata)))
                if self.state == DestinationTransferState::Allocating =>
            {
                self.lease = Some(lease);
                self.metadata = Some(metadata);
                match self.receiver.open(ctx.self_addr(), self.transfer_id) {
                    Ok(mut offer) => {
                        offer.failure_proxy = Some(self.failure_proxy);
                        self.offer = Some(offer.clone());
                        self.state = DestinationTransferState::Filling;
                        if ctx
                            .send(self.source, BlobSourceIn::BeginTransfer { offer })
                            .is_err()
                        {
                            self.fault(
                                ctx,
                                DataPlaneError::SourceFailure(
                                    "route to selected blob source is unavailable".to_owned(),
                                ),
                            );
                        }
                    }
                    Err(error) => self.fault(ctx, DataPlaneError::SourceFailure(error)),
                }
            }
            BlobTransferEvent::Allocated(Err(error))
                if self.state == DestinationTransferState::Allocating =>
            {
                self.finish_failure(ctx, error);
            }
            BlobTransferEvent::Chunk { transfer_id, bytes }
                if self.state == DestinationTransferState::Filling
                    && transfer_id == self.transfer_id =>
            {
                let found = self.written.saturating_add(bytes.len() as u64);
                let Some(next) = self
                    .written
                    .checked_add(bytes.len() as u64)
                    .filter(|written| *written <= self.length)
                else {
                    self.fault(
                        ctx,
                        DataPlaneError::Blob(crate::protocol::BlobFailure::Length {
                            expected: self.length,
                            found,
                        }),
                    );
                    return;
                };
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::WriteTransfer {
                        transfer: ctx.self_addr(),
                        lease: self.lease.expect("allocated destination lease"),
                        offset: self.written,
                        bytes,
                    },
                );
                self.written = next;
            }
            BlobTransferEvent::Finished { transfer_id }
                if self.state == DestinationTransferState::Filling
                    && transfer_id == self.transfer_id =>
            {
                self.offer = None;
                self.state = DestinationTransferState::Sealing;
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::SealTransfer {
                        transfer: ctx.self_addr(),
                        lease: self.lease.expect("allocated destination lease"),
                        metadata: self.metadata.clone().expect("destination metadata"),
                        written: self.written,
                    },
                );
            }
            BlobTransferEvent::Failed {
                transfer_id,
                reason,
            } if transfer_id == self.transfer_id => {
                self.fault(ctx, DataPlaneError::SourceFailure(reason));
            }
            BlobTransferEvent::AllocatorFailed(error) => self.fault(ctx, error),
            BlobTransferEvent::Sealed(Ok(()))
                if self.state == DestinationTransferState::Sealing =>
            {
                self.state = DestinationTransferState::Finished;
                let _ = ctx.send(
                    self.binding,
                    HostBindingIn::TransferReady {
                        lease: self.lease.expect("sealed destination lease"),
                        metadata: self.metadata.clone().expect("sealed destination metadata"),
                    },
                );
                ctx.stop_self();
            }
            BlobTransferEvent::Sealed(Err(error))
                if self.state == DestinationTransferState::Sealing =>
            {
                self.fault(ctx, error);
            }
            BlobTransferEvent::Released(result)
                if self.state == DestinationTransferState::Releasing =>
            {
                let error = self.pending_error.take().unwrap_or_else(|| {
                    DataPlaneError::SessionFailed(
                        "destination transfer released without a failure".to_owned(),
                    )
                });
                if let Err(release_error) = result {
                    self.finish_failure(ctx, release_error);
                } else {
                    self.finish_failure(ctx, error);
                }
            }
            BlobTransferEvent::Cancel => self.fault(ctx, DataPlaneError::OperationCancelled),
            _ => {}
        }
    }
}

struct NamespaceResolveActor {
    namespace_proxy: ActorAddress,
    path: DataPath,
    binding: ActorAddress,
}

impl ActorInterface for NamespaceResolveActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::Resolve {
                        path: self.path.clone(),
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            let _ = ctx.send(
                self.binding,
                HostBindingIn::Resolved(Err(DataPlaneError::SessionFailed(
                    "namespace client is unavailable".to_owned(),
                ))),
            );
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if let DataDirectoryOut::Resolved { result, .. } = message {
            let result = result.map_err(namespace_error);
            let _ = ctx.send(self.binding, HostBindingIn::Resolved(result));
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.namespace_proxy,
            NamespaceClientIn::Cancel {
                reply_to: ctx.self_addr(),
            },
        );
    }
}

struct NamespacePublishActor {
    namespace_proxy: ActorAddress,
    path: DataPath,
    source: ActorAddress,
    length: u64,
    operation_id: OperationId,
    operation: ActorAddress,
    binding: ActorAddress,
}

impl ActorInterface for NamespacePublishActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::Register {
                        path: self.path.clone(),
                        source: self.source,
                        length: self.length,
                        recovery: SourceRecovery::Actor { actor: self.source },
                        operation_id: self.operation_id,
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            let _ = ctx.send(
                self.binding,
                HostBindingIn::PublicationRejected(DataPlaneError::SessionFailed(
                    "namespace client is unavailable".to_owned(),
                )),
            );
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if let DataDirectoryOut::Registered { result, .. } = message {
            let response = match result {
                Ok(_) => HostBindingIn::PublicationAccepted {
                    operation: self.operation,
                },
                Err(error) => HostBindingIn::PublicationRejected(namespace_error(error)),
            };
            let _ = ctx.send(self.binding, response);
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.namespace_proxy,
            NamespaceClientIn::Cancel {
                reply_to: ctx.self_addr(),
            },
        );
    }
}

struct NamespaceUnpublishActor {
    namespace_proxy: ActorAddress,
    path: DataPath,
    operation_id: OperationId,
    source: ActorAddress,
}

impl ActorInterface for NamespaceUnpublishActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::Unregister {
                        path: self.path.clone(),
                        operation_id: self.operation_id,
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if let DataDirectoryOut::Unregistered { result, .. } = message {
            if result.is_ok() || matches!(result, Err(NamespaceError::PathNotFound(_))) {
                let _ = ctx.send(self.source, BlobSourceIn::Retire);
            }
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.namespace_proxy,
            NamespaceClientIn::Cancel {
                reply_to: ctx.self_addr(),
            },
        );
    }
}

fn namespace_error(error: NamespaceError) -> DataPlaneError {
    match error {
        NamespaceError::PathNotFound(path) => DataPlaneError::PathNotFound(path),
        NamespaceError::SourceRecovery(reason) => DataPlaneError::SourceFailure(reason),
        NamespaceError::DirectoryUnavailable(reason)
        | NamespaceError::Storage(reason)
        | NamespaceError::Protocol(reason) => DataPlaneError::SessionFailed(reason),
        NamespaceError::OperationConflict(operation) => DataPlaneError::SessionFailed(format!(
            "namespace operation conflict: {:02x?}",
            operation.bytes()
        )),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HostBindingState {
    Resolving,
    Allocated,
    Filling,
    Granted,
    Sealing,
    Publishing,
    Published,
    Unpublishing,
    Releasing,
    Released,
    Faulted,
}

#[derive(Clone)]
enum BindingMode {
    NamespaceRead {
        namespace: NamespaceClient,
        receiver: Arc<dyn BlobTransferReceiver>,
    },
    Write {
        length: u64,
        namespace: Option<NamespaceClient>,
        source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    },
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
    Resolved(Result<NamespaceBlobBinding, DataPlaneError>),
    TransferReady {
        lease: BlobLease,
        metadata: BlobMetadata,
    },
    TransferFailed(DataPlaneError),
    Allocated(Result<(BlobLease, BlobMetadata), DataPlaneError>),
    CancelRead {
        operation: ActorAddress,
    },
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
    PublishedSource(Result<ActorAddress, DataPlaneError>),
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

struct HostBindingAddresses {
    host_session: ActorAddress,
    allocator: ActorAddress,
    child_session: ActorAddress,
    operation: ActorAddress,
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
    auxiliary: Option<ActorAddress>,
    published_source: Option<ActorAddress>,
}

impl HostBlobBindingActor {
    fn namespace_read(
        addresses: HostBindingAddresses,
        path: DataPath,
        namespace: NamespaceClient,
        receiver: Arc<dyn BlobTransferReceiver>,
    ) -> Self {
        let HostBindingAddresses {
            host_session,
            allocator,
            child_session,
            operation,
        } = addresses;
        Self {
            host_session,
            allocator,
            child_session,
            operation,
            path,
            mode: BindingMode::NamespaceRead {
                namespace,
                receiver,
            },
            state: HostBindingState::Resolving,
            lease: None,
            metadata: None,
            release_outcome: None,
            auxiliary: None,
            published_source: None,
        }
    }

    fn write(
        addresses: HostBindingAddresses,
        path: DataPath,
        length: u64,
        namespace: Option<NamespaceClient>,
        source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    ) -> Self {
        let HostBindingAddresses {
            host_session,
            allocator,
            child_session,
            operation,
        } = addresses;
        Self {
            host_session,
            allocator,
            child_session,
            operation,
            path,
            mode: BindingMode::Write {
                length,
                namespace,
                source_publisher,
            },
            state: HostBindingState::Allocated,
            lease: None,
            metadata: None,
            release_outcome: None,
            auxiliary: None,
            published_source: None,
        }
    }

    fn begin_unpublish(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        let (namespace, source) = match (&self.mode, self.published_source) {
            (
                BindingMode::Write {
                    namespace: Some(namespace),
                    ..
                },
                Some(source),
            ) => (namespace.clone(), source),
            _ => {
                self.begin_release(ctx, outcome);
                return;
            }
        };
        let mut id_bytes = [0_u8; 16];
        id_bytes.copy_from_slice(&ctx.self_addr().0[..16]);
        let operation_id = OperationId::from_u128(u128::from_be_bytes(id_bytes).wrapping_add(1));
        match ctx.spawn(NamespaceUnpublishActor {
            namespace_proxy: namespace.proxy(),
            path: self.path.clone(),
            operation_id,
            source,
        }) {
            Ok(unpublisher) => {
                self.auxiliary = Some(unpublisher);
                self.release_outcome = Some(outcome);
                self.state = HostBindingState::Unpublishing;
            }
            Err(_) => {
                self.state = HostBindingState::Published;
            }
        }
    }

    fn begin_release(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        let Some(lease) = self.lease else {
            self.finish_without_lease(ctx, outcome);
            return;
        };
        self.state = HostBindingState::Releasing;
        self.release_outcome = Some(outcome);
        let _ = ctx.send(
            self.allocator,
            ArenaAllocatorIn::Release {
                binding: ctx.self_addr(),
                lease,
            },
        );
    }

    fn finish_without_lease(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        self.state = HostBindingState::Released;
        if let Some(auxiliary) = self.auxiliary.take() {
            let _ = ctx.stop_actor(auxiliary);
        }
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
        if let Some(source) = self.published_source.take() {
            let _ = ctx.send(source, BlobSourceIn::Retire);
        }
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
        if let BindingMode::NamespaceRead { namespace, .. } = &self.mode {
            self.state = HostBindingState::Resolving;
            match ctx.spawn(NamespaceResolveActor {
                namespace_proxy: namespace.proxy(),
                path: self.path.clone(),
                binding: ctx.self_addr(),
            }) {
                Ok(resolver) => {
                    self.auxiliary = Some(resolver);
                }
                Err(error) => {
                    self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
                }
            }
            return;
        }

        self.state = HostBindingState::Filling;
        let length = match &self.mode {
            BindingMode::Write { length, .. } => *length,
            BindingMode::NamespaceRead { .. } => unreachable!("handled above"),
        };
        let _ = ctx.send(
            self.allocator,
            ArenaAllocatorIn::Allocate {
                binding: ctx.self_addr(),
                kind: AllocationKind::Write {
                    length,
                    digest: None,
                },
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostBindingIn) {
        match message {
            HostBindingIn::Resolved(Ok(binding))
                if self.state == HostBindingState::Resolving
                    && matches!(self.mode, BindingMode::NamespaceRead { .. }) =>
            {
                self.auxiliary = None;
                let (receiver, failure_proxy) = match &self.mode {
                    BindingMode::NamespaceRead {
                        namespace,
                        receiver,
                    } => (Arc::clone(receiver), namespace.proxy()),
                    _ => unreachable!("namespace resolve on non-namespace binding"),
                };
                let mut id_bytes = [0_u8; 8];
                id_bytes.copy_from_slice(&ctx.self_addr().0[..8]);
                let transfer_id = BlobTransferId(u64::from_le_bytes(id_bytes).max(1));
                match ctx.spawn(DestinationBlobTransferActor::new(
                    self.allocator,
                    ctx.self_addr(),
                    receiver,
                    failure_proxy,
                    binding.source,
                    binding.length,
                    transfer_id,
                )) {
                    Ok(transfer) => {
                        self.auxiliary = Some(transfer);
                        self.state = HostBindingState::Filling;
                    }
                    Err(error) => {
                        self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
                    }
                }
            }
            HostBindingIn::Resolved(Err(error)) if self.state == HostBindingState::Resolving => {
                self.auxiliary = None;
                self.fail(ctx, error);
            }
            HostBindingIn::TransferReady { lease, metadata }
                if self.state == HostBindingState::Filling
                    && matches!(self.mode, BindingMode::NamespaceRead { .. }) =>
            {
                self.auxiliary = None;
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
            HostBindingIn::CancelRead { operation }
                if operation == self.operation
                    && matches!(self.mode, BindingMode::NamespaceRead { .. }) =>
            {
                match self.state {
                    HostBindingState::Resolving => {
                        self.finish_without_lease(ctx, ReleaseOutcome::Faulted);
                    }
                    HostBindingState::Filling => {
                        if let Some(transfer) = self.auxiliary {
                            let _ = ctx.send(transfer, BlobTransferEvent::Cancel);
                        } else {
                            self.finish_without_lease(ctx, ReleaseOutcome::Faulted);
                        }
                    }
                    HostBindingState::Granted => {
                        self.begin_release(ctx, ReleaseOutcome::ReadReleased);
                    }
                    _ => {}
                }
            }
            HostBindingIn::TransferFailed(error)
                if matches!(
                    self.state,
                    HostBindingState::Resolving | HostBindingState::Filling
                ) =>
            {
                self.auxiliary = None;
                self.fail(ctx, error);
            }
            HostBindingIn::Allocated(Ok((lease, metadata)))
                if self.state == HostBindingState::Filling =>
            {
                self.lease = Some(lease);
                self.metadata = Some(metadata.clone());
                self.state = HostBindingState::Granted;
                let response = match self.mode {
                    BindingMode::Write { .. } => ChildSessionIn::WriteBlobOpened {
                        operation: self.operation,
                        host_binding: ctx.self_addr(),
                        lease,
                        metadata,
                    },
                    BindingMode::NamespaceRead { .. } => {
                        unreachable!("namespace read uses TransferReady")
                    }
                };
                let _ = ctx.send(self.child_session, response);
            }
            HostBindingIn::Allocated(Err(error)) if self.state == HostBindingState::Filling => {
                self.fail(ctx, error);
            }
            HostBindingIn::Release {
                lease_id,
                generation,
            } if self.state == HostBindingState::Granted
                && matches!(self.mode, BindingMode::NamespaceRead { .. }) =>
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
                if matches!(
                    self.mode,
                    BindingMode::Write {
                        namespace: Some(_),
                        ..
                    }
                ) {
                    let _ = ctx.send(
                        self.allocator,
                        ArenaAllocatorIn::CreatePublishedSource {
                            binding: ctx.self_addr(),
                            lease,
                            metadata,
                        },
                    );
                } else {
                    self.fail(
                        ctx,
                        DataPlaneError::SessionFailed(
                            "data namespace publication service is unavailable".to_owned(),
                        ),
                    );
                }
            }
            HostBindingIn::PublishedSource(Ok(source))
                if self.state == HostBindingState::Publishing =>
            {
                let (namespace, publisher) = match &self.mode {
                    BindingMode::Write {
                        namespace: Some(namespace),
                        source_publisher: Some(publisher),
                        ..
                    } => (namespace.clone(), Arc::clone(publisher)),
                    _ => {
                        let _ = ctx.send(source, BlobSourceIn::Retire);
                        self.fail(
                            ctx,
                            DataPlaneError::SessionFailed(
                                "namespace source publisher is unavailable".to_owned(),
                            ),
                        );
                        return;
                    }
                };
                if let Err(error) = publisher.publish_source(source) {
                    let _ = ctx.send(source, BlobSourceIn::Retire);
                    self.fail(ctx, DataPlaneError::SessionFailed(error));
                    return;
                }
                self.published_source = Some(source);
                let mut id_bytes = [0_u8; 16];
                id_bytes.copy_from_slice(&ctx.self_addr().0[..16]);
                let operation_id = OperationId::from_u128(u128::from_be_bytes(id_bytes));
                match ctx.spawn(NamespacePublishActor {
                    namespace_proxy: namespace.proxy(),
                    path: self.path.clone(),
                    source,
                    length: self.metadata.as_ref().expect("write metadata").length,
                    operation_id,
                    operation: self.operation,
                    binding: ctx.self_addr(),
                }) {
                    Ok(publisher) => {
                        self.auxiliary = Some(publisher);
                    }
                    Err(error) => {
                        self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
                    }
                }
            }
            HostBindingIn::PublishedSource(Err(error))
                if self.state == HostBindingState::Publishing =>
            {
                self.fail(ctx, error);
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
            HostBindingIn::Abort {
                operation,
                lease_id,
                generation,
            } if matches!(
                self.state,
                HostBindingState::Sealing
                    | HostBindingState::Publishing
                    | HostBindingState::Published
            ) && matches!(self.mode, BindingMode::Write { .. }) =>
            {
                if operation != self.operation || !self.lease_matches(lease_id, generation) {
                    return;
                }
                let outcome = ReleaseOutcome::WriteAborted { operation };
                match self.state {
                    HostBindingState::Sealing => self.begin_release(ctx, outcome),
                    HostBindingState::Publishing => {
                        self.release_outcome = Some(outcome);
                    }
                    HostBindingState::Published => self.begin_unpublish(ctx, outcome),
                    _ => unreachable!("guarded abort state"),
                }
            }
            HostBindingIn::PublicationAccepted { operation }
                if self.state == HostBindingState::Publishing && operation == self.operation =>
            {
                self.auxiliary = None;
                self.state = HostBindingState::Published;
                if let Some(outcome) = self.release_outcome.take() {
                    self.begin_unpublish(ctx, outcome);
                } else {
                    let _ = ctx.send(
                        self.host_session,
                        HostSessionIn::BindingDetached {
                            binding: ctx.self_addr(),
                        },
                    );
                    let _ = ctx.send(
                        self.child_session,
                        ChildSessionIn::WritePublished { operation },
                    );
                }
            }
            HostBindingIn::PublicationRejected(error)
                if self.state == HostBindingState::Publishing =>
            {
                self.auxiliary = None;
                self.fail(ctx, error);
            }
            HostBindingIn::ReleasePublished
                if matches!(
                    self.state,
                    HostBindingState::Published | HostBindingState::Unpublishing
                ) =>
            {
                let outcome = self
                    .release_outcome
                    .take()
                    .unwrap_or(ReleaseOutcome::PublishedReleased);
                self.begin_release(ctx, outcome);
            }
            HostBindingIn::SessionClosed
                if self.state == HostBindingState::Filling
                    && matches!(self.mode, BindingMode::NamespaceRead { .. }) =>
            {
                if let Some(transfer) = self.auxiliary {
                    let _ = ctx.send(transfer, BlobTransferEvent::Cancel);
                } else {
                    self.finish_without_lease(ctx, ReleaseOutcome::SessionClosed);
                }
            }
            HostBindingIn::SessionClosed
                if matches!(
                    self.state,
                    HostBindingState::Publishing
                        | HostBindingState::Published
                        | HostBindingState::Unpublishing
                ) => {}
            HostBindingIn::SessionClosed
                if !matches!(
                    self.state,
                    HostBindingState::Released
                        | HostBindingState::Releasing
                        | HostBindingState::Publishing
                        | HostBindingState::Published
                        | HostBindingState::Unpublishing
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
            _ => {}
        }
    }
}
