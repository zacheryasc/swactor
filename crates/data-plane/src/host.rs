//! Host-side session, binding, and arena-allocation actors.

use std::collections::{HashMap, HashSet};
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use swactor::actor::{ActorAddress, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor_engine::EngineHandle;

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
use crate::byte_ring::{self, ByteRingSpec, RingHandle, Role};
use crate::mapped_arena::MappedArena;
use crate::namespace::{
    BlobBinding as NamespaceBlobBinding, DataDirectoryOut, EntryKind, NamespaceClient,
    NamespaceClientIn, NamespaceError, NamespaceNode, NamespaceRequest, OperationId,
    SourceRecovery, StreamIncarnation, StreamMatch, StreamRole,
};
use crate::path::{DataPath, SessionAccess};
use crate::protocol::{
    AccessMode, AttachmentFailure, ChildSessionIn, DataPlaneError, HostSessionIn, HostStreamIn,
    NamespaceOperation, NamespaceOperationResult, OpenOptions, OpenPolicy, SessionCapability,
};
use crate::source::{BlobSourceIn, BlobSourcePublisher, BlobSourceRetirement, FileBlobSourceActor};
use crate::stream_transport::{
    StreamPeerDescriptor, StreamSinkRequest, StreamSourceRequest, StreamTransport,
    StreamTransportEvent, StreamTransportNotifier,
};

const BLOB_ALIGNMENT: u64 = 64;
const FIRST_BLOB_REQUEST_ID: u64 = 2;
const STREAM_RING_CAPACITY: u64 = 256 * 1024;
const BLOB_ROUTE_RETRY: Duration = Duration::from_millis(100);
const BLOB_ROUTE_RETRY_LIMIT: u16 = 300;
const STREAM_PEER_OFFER_RETRY: Duration = Duration::from_millis(100);

fn namespace_operation_id(address: ActorAddress) -> OperationId {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&address.0[..16]);
    OperationId::from_u128(u128::from_be_bytes(bytes))
}

pub trait HostRouteRegistrar: Send + Sync + 'static {
    fn register_child(
        &self,
        child_session: ActorAddress,
        child_node: [u8; 32],
    ) -> Result<(), String>;
    fn revoke_child(&self, child_session: ActorAddress) -> Result<(), String>;

    fn is_routable(&self, _actor: ActorAddress) -> bool {
        true
    }
}

pub struct HostDataPlaneConfig {
    pub runtime: Runtime,
    pub engine: EngineHandle,
    pub arena: ArenaManager,
    pub arena_generation: u64,
    pub session_generation: u64,
    pub capability: SessionCapability,
    pub session_access: SessionAccess,
    pub namespace: Option<NamespaceClient>,
    pub transfer_receiver: Option<Arc<dyn BlobTransferReceiver>>,
    pub source_sender: Option<Arc<dyn BlobTransferSender>>,
    pub source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    pub route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    pub stream_transport: Option<Arc<dyn StreamTransport>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostSessionState {
    AwaitingAttachment,
    Running,
    Revoked,
    Closing,
    Closed,
}

#[derive(Clone)]
struct PendingOpen {
    child_session: ActorAddress,
    path: DataPath,
    options: OpenOptions,
}
struct BlobWriteSpawn {
    child_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    length: u64,
    digest: Option<ContentDigest>,
    reservation: Option<OperationId>,
}

struct StreamSpawn {
    child_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    role: StreamRole,
    replace: bool,
    ensure: bool,
    expected_revision: Option<u64>,
}

pub struct HostDataPlaneSessionActor {
    arena: Option<ArenaManager>,
    arena_generation: u64,
    session_generation: u64,
    capability: SessionCapability,
    session_access: SessionAccess,
    namespace: Option<NamespaceClient>,
    transfer_receiver: Option<Arc<dyn BlobTransferReceiver>>,
    runtime: Runtime,
    engine: EngineHandle,
    source_sender: Option<Arc<dyn BlobTransferSender>>,
    source_publisher: Option<Arc<dyn BlobSourcePublisher>>,
    route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    stream_arena: Option<Arc<MappedArena>>,
    stream_transport: Option<Arc<dyn StreamTransport>>,
    child_session: Option<ActorAddress>,
    allocator: Option<ActorAddress>,
    active_bindings: HashSet<ActorAddress>,
    stream_bindings: HashMap<ActorAddress, ActorAddress>,
    pending_opens: HashMap<ActorAddress, PendingOpen>,
    open_lookups: HashMap<ActorAddress, ActorAddress>,
    namespace_operations: HashMap<ActorAddress, ActorAddress>,
    state: HostSessionState,
    close_replies: Vec<ActorAddress>,
}

impl HostDataPlaneSessionActor {
    pub fn new(config: HostDataPlaneConfig) -> Result<Self, DataPlaneError> {
        if config.arena_generation == 0 || config.session_generation == 0 {
            return Err(DataPlaneError::SessionFailed(
                "session and arena generations must be nonzero".to_owned(),
            ));
        }
        config
            .session_access
            .validate()
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        let stream_arena = if config.stream_transport.is_some() {
            let fd = unsafe { libc::dup(config.arena.arena_fd()) };
            if fd < 0 {
                return Err(DataPlaneError::SessionFailed(format!(
                    "duplicate arena backing for streams: {}",
                    std::io::Error::last_os_error()
                )));
            }
            let owned = unsafe { OwnedFd::from_raw_fd(fd) };
            let (mapped, _) = MappedArena::map(owned)
                .map_err(|error| DataPlaneError::SessionFailed(error.to_string()))?;
            Some(Arc::new(mapped))
        } else {
            None
        };
        Ok(Self {
            arena: Some(config.arena),
            arena_generation: config.arena_generation,
            session_generation: config.session_generation,
            capability: config.capability,
            session_access: config.session_access,
            namespace: config.namespace,
            transfer_receiver: config.transfer_receiver,
            runtime: config.runtime,
            engine: config.engine,
            source_sender: config.source_sender,
            source_publisher: config.source_publisher,
            route_registrar: config.route_registrar,
            stream_arena,
            stream_transport: config.stream_transport,
            child_session: None,
            allocator: None,
            active_bindings: HashSet::new(),
            stream_bindings: HashMap::new(),
            pending_opens: HashMap::new(),
            open_lookups: HashMap::new(),
            namespace_operations: HashMap::new(),
            state: HostSessionState::AwaitingAttachment,
            close_replies: Vec::new(),
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
        access: AccessMode,
    ) -> Result<DataPath, DataPlaneError> {
        if self.state != HostSessionState::Running || self.child_session != Some(child_session) {
            return Err(DataPlaneError::SessionNotRunning);
        }
        let resolved = self
            .session_access
            .resolve(logical)
            .map_err(|error| DataPlaneError::InvalidPath(error.to_string()))?;
        let authorized = (!access.can_read() || self.session_access.can_read(&resolved))
            && (!access.can_write() || self.session_access.can_write(&resolved));
        if !authorized {
            return Err(DataPlaneError::Unauthorized {
                path: resolved,
                access,
            });
        }
        Ok(resolved)
    }

    fn validate_namespace_operation(
        &self,
        child_session: ActorAddress,
        request: NamespaceOperation,
    ) -> Result<NamespaceOperation, DataPlaneError> {
        match request {
            NamespaceOperation::Lookup { path } => Ok(NamespaceOperation::Lookup {
                path: self.validate_open(child_session, &path, AccessMode::ReadOnly)?,
            }),
            NamespaceOperation::Unlink { path } => Ok(NamespaceOperation::Unlink {
                path: self.validate_open(child_session, &path, AccessMode::WriteOnly)?,
            }),
            NamespaceOperation::Rename {
                source,
                destination,
                replace,
            } => Ok(NamespaceOperation::Rename {
                source: self.validate_open(child_session, &source, AccessMode::WriteOnly)?,
                destination: self.validate_open(
                    child_session,
                    &destination,
                    AccessMode::WriteOnly,
                )?,
                replace,
            }),
        }
    }

    fn send_namespace_result(
        &self,
        ctx: &Ctx<'_>,
        child_session: ActorAddress,
        operation: ActorAddress,
        result: Result<NamespaceOperationResult, DataPlaneError>,
    ) {
        let _ = ctx.send(
            child_session,
            ChildSessionIn::NamespaceResolved {
                reply_to: operation,
                result,
            },
        );
    }

    fn spawn_blob_read(
        &mut self,
        ctx: &Ctx<'_>,
        child_session: ActorAddress,
        operation: ActorAddress,
        path: DataPath,
        expected_revision: Option<u64>,
    ) {
        let (Some(namespace), Some(receiver)) = (&self.namespace, &self.transfer_receiver) else {
            self.send_open_failure(
                ctx,
                child_session,
                operation,
                DataPlaneError::SessionFailed("data namespace service is unavailable".to_owned()),
            );
            return;
        };
        let binding = HostBlobBindingActor::namespace_read(NamespaceReadParams {
            addresses: HostBindingAddresses {
                host_session: ctx.self_addr(),
                allocator: self.allocator.expect("allocator started"),
                child_session,
                operation,
            },
            path,
            expected_revision,
            namespace: namespace.clone(),
            receiver: Arc::clone(receiver),
            engine: self.engine.clone(),
            sender: self.runtime.create_sender(),
            route_registrar: self.route_registrar.clone(),
        });
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

    fn spawn_blob_write(&mut self, ctx: &Ctx<'_>, request: BlobWriteSpawn) {
        let BlobWriteSpawn {
            child_session,
            operation,
            path,
            length,
            digest,
            reservation,
        } = request;
        let binding = HostBlobBindingActor::write(
            HostBindingAddresses {
                host_session: ctx.self_addr(),
                allocator: self.allocator.expect("allocator started"),
                child_session,
                operation,
            },
            path.clone(),
            length,
            digest,
            reservation,
            self.namespace.clone(),
            self.source_publisher.clone(),
        );
        match ctx.spawn(binding) {
            Ok(binding) => {
                self.active_bindings.insert(binding);
            }
            Err(error) => {
                if let (Some(namespace), Some(reservation)) = (&self.namespace, reservation) {
                    let _ = ctx.send(
                        namespace.proxy(),
                        NamespaceClientIn::CancelBlobReservation {
                            path,
                            operation_id: reservation,
                            reply_to: ctx.self_addr(),
                        },
                    );
                }
                self.send_open_failure(
                    ctx,
                    child_session,
                    operation,
                    DataPlaneError::SessionFailed(error.to_string()),
                );
            }
        }
    }

    fn spawn_stream(&mut self, ctx: &Ctx<'_>, request: StreamSpawn) {
        let StreamSpawn {
            child_session,
            operation,
            path,
            role,
            replace,
            ensure,
            expected_revision,
        } = request;
        let (Some(namespace), Some(arena), Some(transport)) = (
            self.namespace.clone(),
            self.stream_arena.clone(),
            self.stream_transport.clone(),
        ) else {
            self.send_open_failure(
                ctx,
                child_session,
                operation,
                DataPlaneError::SessionFailed(
                    "stream namespace or transport service is unavailable".to_owned(),
                ),
            );
            return;
        };
        let local_descriptor = match transport.descriptor() {
            Ok(descriptor) => descriptor,
            Err(reason) => {
                self.send_open_failure(
                    ctx,
                    child_session,
                    operation,
                    DataPlaneError::StreamFault(reason),
                );
                return;
            }
        };
        let binding = HostStreamBindingActor {
            runtime: self.runtime.clone(),
            engine: self.engine.clone(),
            sender: self.runtime.create_sender(),
            route_registrar: self.route_registrar.clone(),
            host_session: ctx.self_addr(),
            allocator: self.allocator.expect("allocator started"),
            child_session,
            operation,
            path,
            replace,
            ensure,
            expected_revision,
            role,
            namespace,
            arena,
            transport,
            local_descriptor,
            ring: None,
            matched: None,
            peer_descriptor: None,
            peer_offer_acknowledged: false,
            transport_installed: false,
            transport_ready: false,
            transport_quiesced: false,
            opened: false,
            terminal: None,
            data_waiters: Vec::new(),
            capacity_waiters: Vec::new(),
            close_waiters: Vec::new(),
            release_started: false,
            namespace_open: None,
            peer_ack_pending: false,
        };
        match ctx.spawn(binding) {
            Ok(binding) => {
                if let Some(publisher) = &self.source_publisher
                    && let Err(error) = publisher.publish_source(binding)
                {
                    let _ = ctx.stop_actor(binding);
                    self.send_open_failure(
                        ctx,
                        child_session,
                        operation,
                        DataPlaneError::SessionFailed(format!("publish stream endpoint: {error}")),
                    );
                    return;
                }
                self.stream_bindings.insert(operation, binding);
            }
            Err(error) => self.send_open_failure(
                ctx,
                child_session,
                operation,
                DataPlaneError::SessionFailed(error.to_string()),
            ),
        }
    }

    fn revoke_session(&mut self, ctx: &Ctx<'_>) {
        if matches!(
            self.state,
            HostSessionState::Revoked | HostSessionState::Closing | HostSessionState::Closed
        ) {
            return;
        }
        self.state = HostSessionState::Revoked;
        if let (Some(registrar), Some(child_session)) = (&self.route_registrar, self.child_session)
        {
            let _ = registrar.revoke_child(child_session);
        }
        for lookup in self.open_lookups.drain().map(|(_, lookup)| lookup) {
            let _ = ctx.stop_actor(lookup);
        }
        for operation in self
            .namespace_operations
            .drain()
            .map(|(_, operation)| operation)
        {
            let _ = ctx.stop_actor(operation);
        }
        for (operation, pending) in self.pending_opens.drain() {
            let _ = ctx.send(
                pending.child_session,
                ChildSessionIn::OperationFailed {
                    operation,
                    error: DataPlaneError::SessionNotRunning,
                },
            );
        }
        for binding in self.active_bindings.iter().copied() {
            let _ = ctx.send(binding, HostBindingIn::SessionClosed);
        }
        for binding in self.stream_bindings.values().copied() {
            let _ = ctx.send(
                binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: None,
                },
            );
        }
    }

    fn begin_close(&mut self, ctx: &Ctx<'_>, reply_to: Option<ActorAddress>) {
        if self.state == HostSessionState::Closed {
            if let Some(reply_to) = reply_to {
                let _ = ctx.send(reply_to, Ok::<(), DataPlaneError>(()));
            }
            return;
        }
        if let Some(reply_to) = reply_to {
            self.close_replies.push(reply_to);
        }
        self.revoke_session(ctx);
        self.state = HostSessionState::Closing;
        self.maybe_finish_close(ctx);
    }

    fn maybe_finish_close(&mut self, ctx: &Ctx<'_>) {
        if self.state == HostSessionState::Closing
            && self.active_bindings.is_empty()
            && self.stream_bindings.is_empty()
            && self.pending_opens.is_empty()
            && self.open_lookups.is_empty()
            && self.namespace_operations.is_empty()
        {
            self.state = HostSessionState::Closed;
            if let Some(allocator) = self.allocator.take() {
                let _ = ctx.stop_actor(allocator);
            }
            for reply_to in self.close_replies.drain(..) {
                let _ = ctx.send(reply_to, Ok::<(), DataPlaneError>(()));
            }
            ctx.stop_self();
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
                session_capability,
                child_node,
            } => {
                let mut failure = if matches!(
                    self.state,
                    HostSessionState::Revoked
                        | HostSessionState::Closing
                        | HostSessionState::Closed
                ) {
                    Some(AttachmentFailure::SessionClosed)
                } else if self.child_session.is_some() {
                    Some(AttachmentFailure::DuplicateAttachment)
                } else {
                    None
                };
                let mut route_installed = false;
                if failure.is_none()
                    && let (Some(registrar), Some(child_node)) = (&self.route_registrar, child_node)
                {
                    match registrar.register_child(child_session, child_node) {
                        Ok(()) => route_installed = true,
                        Err(reason) => {
                            failure = Some(AttachmentFailure::RouteRejected(reason));
                        }
                    }
                }
                if failure.is_none() && arena_generation != self.arena_generation {
                    failure = Some(AttachmentFailure::ArenaGenerationMismatch {
                        expected: self.arena_generation,
                        found: arena_generation,
                    });
                }
                if failure.is_none() && session_capability != self.capability {
                    failure = Some(AttachmentFailure::CapabilityRejected);
                }

                if let Some(reason) = failure {
                    let _ = ctx.send(
                        child_session,
                        ChildSessionIn::AttachmentFailed {
                            error: DataPlaneError::Attachment(reason),
                        },
                    );
                    if route_installed && let Some(registrar) = &self.route_registrar {
                        let _ = registrar.revoke_child(child_session);
                    }
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
            HostSessionIn::Namespace {
                operation,
                request,
                child_session,
            } => {
                let request = match self.validate_namespace_operation(child_session, request) {
                    Ok(request) => request,
                    Err(error) => {
                        self.send_namespace_result(ctx, child_session, operation, Err(error));
                        return;
                    }
                };
                let Some(namespace) = &self.namespace else {
                    self.send_namespace_result(
                        ctx,
                        child_session,
                        operation,
                        Err(DataPlaneError::SessionFailed(
                            "data namespace service is unavailable".to_owned(),
                        )),
                    );
                    return;
                };
                match ctx.spawn(NamespaceControlActor {
                    namespace_proxy: namespace.proxy(),
                    host_session: ctx.self_addr(),
                    child_session,
                    operation,
                    request,
                    completed: false,
                }) {
                    Ok(actor) => {
                        self.namespace_operations.insert(operation, actor);
                    }
                    Err(error) => self.send_namespace_result(
                        ctx,
                        child_session,
                        operation,
                        Err(DataPlaneError::SessionFailed(error.to_string())),
                    ),
                }
            }
            HostSessionIn::NamespaceResolved {
                operation,
                child_session,
                result,
            } => {
                if self.namespace_operations.remove(&operation).is_some() {
                    self.send_namespace_result(ctx, child_session, operation, result);
                }
                self.maybe_finish_close(ctx);
            }
            HostSessionIn::CancelNamespace { operation } => {
                if let Some(actor) = self.namespace_operations.remove(&operation) {
                    let _ = ctx.stop_actor(actor);
                }
                self.maybe_finish_close(ctx);
            }
            HostSessionIn::Open {
                path,
                options,
                policy,
                child_session,
                operation,
            } => {
                if let Err(error) = options.validate() {
                    self.send_open_failure(ctx, child_session, operation, error);
                    return;
                }
                let resolved = match self.validate_open(child_session, &path, options.access) {
                    Ok(path) => path,
                    Err(error) => {
                        self.send_open_failure(ctx, child_session, operation, error);
                        return;
                    }
                };
                if let OpenPolicy::EnsureStream { replace } = policy {
                    if options.create
                        || options.exclusive
                        || options.truncate
                        || options.allocation.is_some()
                    {
                        self.send_open_failure(
                            ctx,
                            child_session,
                            operation,
                            DataPlaneError::InvalidArgument(
                                "stream ensure policy does not accept blob creation flags"
                                    .to_owned(),
                            ),
                        );
                        return;
                    }
                    let role = match options.access {
                        AccessMode::ReadOnly => StreamRole::Sink,
                        AccessMode::WriteOnly => StreamRole::Source,
                        AccessMode::ReadWrite => {
                            self.send_open_failure(
                                ctx,
                                child_session,
                                operation,
                                DataPlaneError::Unsupported(
                                    "read-write stream descriptors are not supported".to_owned(),
                                ),
                            );
                            return;
                        }
                    };
                    self.spawn_stream(
                        ctx,
                        StreamSpawn {
                            child_session,
                            operation,
                            path: resolved,
                            role,
                            replace,
                            ensure: true,
                            expected_revision: None,
                        },
                    );
                    return;
                }
                let Some(namespace) = &self.namespace else {
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
                self.pending_opens.insert(
                    operation,
                    PendingOpen {
                        child_session,
                        path: resolved.clone(),
                        options,
                    },
                );
                match ctx.spawn(NamespaceOpenLookupActor {
                    namespace_proxy: namespace.proxy(),
                    host_session: ctx.self_addr(),
                    operation,
                    path: resolved,
                    completed: false,
                }) {
                    Ok(lookup) => {
                        self.open_lookups.insert(operation, lookup);
                    }
                    Err(error) => {
                        self.pending_opens.remove(&operation);
                        self.send_open_failure(
                            ctx,
                            child_session,
                            operation,
                            DataPlaneError::SessionFailed(error.to_string()),
                        );
                    }
                }
            }
            HostSessionIn::OpenResolved { operation, result } => {
                self.open_lookups.remove(&operation);
                let Some(pending) = self.pending_opens.remove(&operation) else {
                    return;
                };
                let PendingOpen {
                    child_session,
                    path,
                    options,
                } = pending;
                match result {
                    Err(NamespaceError::PathNotFound(_)) if options.create => {
                        let Some(allocation) = options.allocation.clone() else {
                            self.send_open_failure(
                                ctx,
                                child_session,
                                operation,
                                DataPlaneError::Unsupported(
                                    "fixed-length blob creation requires allocation metadata"
                                        .to_owned(),
                                ),
                            );
                            return;
                        };
                        if options.exclusive {
                            let reservation = namespace_operation_id(operation);
                            let namespace = self
                                .namespace
                                .as_ref()
                                .expect("lookup requires namespace service");
                            self.pending_opens.insert(
                                operation,
                                PendingOpen {
                                    child_session,
                                    path: path.clone(),
                                    options,
                                },
                            );
                            match ctx.spawn(NamespaceBlobReserveActor {
                                namespace_proxy: namespace.proxy(),
                                host_session: ctx.self_addr(),
                                operation,
                                path,
                                reservation,
                                completed: false,
                            }) {
                                Ok(resolver) => {
                                    self.open_lookups.insert(operation, resolver);
                                }
                                Err(error) => {
                                    self.pending_opens.remove(&operation);
                                    self.send_open_failure(
                                        ctx,
                                        child_session,
                                        operation,
                                        DataPlaneError::SessionFailed(error.to_string()),
                                    );
                                }
                            }
                        } else {
                            self.spawn_blob_write(
                                ctx,
                                BlobWriteSpawn {
                                    child_session,
                                    operation,
                                    path,
                                    length: allocation.length,
                                    digest: allocation.digest,
                                    reservation: None,
                                },
                            );
                        }
                    }
                    Err(error) => {
                        self.send_open_failure(
                            ctx,
                            child_session,
                            operation,
                            namespace_error(error),
                        );
                    }
                    Ok(NamespaceNode {
                        kind: EntryKind::Blob,
                        revision,
                        ..
                    }) => {
                        if options.create && options.exclusive {
                            self.send_open_failure(
                                ctx,
                                child_session,
                                operation,
                                DataPlaneError::PathExists(path),
                            );
                        } else if options.access == AccessMode::ReadOnly
                            && !options.create
                            && !options.truncate
                            && options.allocation.is_none()
                        {
                            self.spawn_blob_read(
                                ctx,
                                child_session,
                                operation,
                                path,
                                Some(revision),
                            );
                        } else if options.access.can_write() && options.truncate {
                            let Some(allocation) = options.allocation else {
                                self.send_open_failure(
                                    ctx,
                                    child_session,
                                    operation,
                                    DataPlaneError::Unsupported(
                                        "fixed-length blob replacement requires allocation metadata"
                                            .to_owned(),
                                    ),
                                );
                                return;
                            };
                            self.spawn_blob_write(
                                ctx,
                                BlobWriteSpawn {
                                    child_session,
                                    operation,
                                    path,
                                    length: allocation.length,
                                    digest: allocation.digest,
                                    reservation: None,
                                },
                            );
                        } else {
                            self.send_open_failure(
                                ctx,
                                child_session,
                                operation,
                                DataPlaneError::Unsupported(
                                    "non-truncating writable blob opens are not supported"
                                        .to_owned(),
                                ),
                            );
                        }
                    }
                    Ok(NamespaceNode {
                        kind: EntryKind::Stream,
                        revision,
                        ..
                    }) => {
                        if options.create
                            || options.exclusive
                            || options.truncate
                            || options.allocation.is_some()
                        {
                            self.send_open_failure(
                                ctx,
                                child_session,
                                operation,
                                DataPlaneError::WrongEntryType {
                                    path,
                                    expected: EntryKind::Blob,
                                    found: EntryKind::Stream,
                                },
                            );
                            return;
                        }
                        let role = match options.access {
                            AccessMode::ReadOnly => StreamRole::Sink,
                            AccessMode::WriteOnly => StreamRole::Source,
                            AccessMode::ReadWrite => {
                                self.send_open_failure(
                                    ctx,
                                    child_session,
                                    operation,
                                    DataPlaneError::Unsupported(
                                        "read-write stream descriptors are not supported"
                                            .to_owned(),
                                    ),
                                );
                                return;
                            }
                        };
                        self.spawn_stream(
                            ctx,
                            StreamSpawn {
                                child_session,
                                operation,
                                path,
                                role,
                                replace: false,
                                ensure: false,
                                expected_revision: Some(revision),
                            },
                        );
                    }
                }
            }
            HostSessionIn::BlobReserved {
                operation,
                path: reserved_path,
                reservation,
                result,
            } => {
                self.open_lookups.remove(&operation);
                let Some(PendingOpen {
                    child_session,
                    path,
                    options,
                }) = self.pending_opens.remove(&operation)
                else {
                    if result.is_ok()
                        && let Some(namespace) = &self.namespace
                    {
                        let _ = ctx.send(
                            namespace.proxy(),
                            NamespaceClientIn::CancelBlobReservation {
                                path: reserved_path,
                                operation_id: reservation,
                                reply_to: ctx.self_addr(),
                            },
                        );
                    }
                    return;
                };
                match result {
                    Ok(()) => {
                        let allocation = options
                            .allocation
                            .expect("exclusive fixed blob reservation has allocation");
                        self.spawn_blob_write(
                            ctx,
                            BlobWriteSpawn {
                                child_session,
                                operation,
                                path,
                                length: allocation.length,
                                digest: allocation.digest,
                                reservation: Some(reservation),
                            },
                        );
                    }
                    Err(error) => self.send_open_failure(
                        ctx,
                        child_session,
                        operation,
                        namespace_error(error),
                    ),
                }
            }
            HostSessionIn::CancelOpen { operation } => {
                self.pending_opens.remove(&operation);
                if let Some(lookup) = self.open_lookups.remove(&operation) {
                    let _ = ctx.stop_actor(lookup);
                }
                for binding in self.active_bindings.iter().copied() {
                    let _ = ctx.send(binding, HostBindingIn::CancelOpen { operation });
                }
                if let Some(binding) = self.stream_bindings.get(&operation).copied() {
                    let _ = ctx.send(
                        binding,
                        HostStreamIn::Close {
                            clean: false,
                            reply_to: None,
                        },
                    );
                }
            }
            HostSessionIn::StreamControl { binding, message } => {
                if self
                    .stream_bindings
                    .values()
                    .any(|stream_binding| *stream_binding == binding)
                {
                    let _ = ctx.send(binding, message);
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
                self.stream_bindings
                    .retain(|_, stream_binding| *stream_binding != binding);
                self.maybe_finish_close(ctx);
            }
            HostSessionIn::ConfigureExecution {
                execution_id,
                reply_to,
            } => {
                let result = if self.state != HostSessionState::AwaitingAttachment
                    || self.child_session.is_some()
                {
                    Err(DataPlaneError::SessionNotRunning)
                } else {
                    let mut access = self.session_access.clone();
                    access.execution_id = execution_id;
                    match access.validate() {
                        Ok(()) => {
                            self.session_access = access;
                            Ok(())
                        }
                        Err(error) => Err(DataPlaneError::InvalidPath(error.to_string())),
                    }
                };
                let _ = ctx.send(reply_to, result);
            }
            HostSessionIn::Revoke => self.revoke_session(ctx),
            HostSessionIn::Close { reply_to } => self.begin_close(ctx, reply_to),
        }
    }
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
    AllocateStream {
        binding: ActorAddress,
        capacity: u64,
    },
    ReleaseStream {
        binding: ActorAddress,
        ring: RingHandle,
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

    fn allocate_stream(&mut self, capacity: u64) -> Result<RingHandle, DataPlaneError> {
        let request_id = self.next_request_id;
        self.next_request_id = self.next_request_id.checked_add(1).ok_or_else(|| {
            DataPlaneError::SessionFailed("stream request id exhausted".to_owned())
        })?;
        let generation = self.next_generation;
        self.next_generation = self
            .next_generation
            .checked_add(1)
            .filter(|next| *next != 0)
            .ok_or_else(|| {
                DataPlaneError::SessionFailed("stream generation exhausted".to_owned())
            })?;
        byte_ring::install(
            &mut self.arena,
            ByteRingSpec {
                capacity,
                generation,
                alignment: BLOB_ALIGNMENT,
                request_id,
            },
        )
        .map_err(|error| match error {
            byte_ring::InstallError::LeaseRejected(_) | byte_ring::InstallError::LeaseQueued => {
                DataPlaneError::ArenaExhausted
            }
            other => {
                DataPlaneError::SessionFailed(format!("stream ring installation failed: {other:?}"))
            }
        })
    }

    fn release_stream(&mut self, ring: RingHandle) -> Result<(), DataPlaneError> {
        let ring_id = RingId(ring.lease_id);
        let Some(allocation) = self.arena.lookup_lease(ring_id) else {
            return Err(DataPlaneError::StreamFault(
                "stream arena lease is no longer live".to_owned(),
            ));
        };
        if allocation.layout.start_offset != ring.offset
            || allocation.layout.data_bytes != ring.capacity
        {
            return Err(DataPlaneError::StreamFault(
                "stream arena lease does not match ring handle".to_owned(),
            ));
        }
        let events = self.arena.request(ArenaRequest::ReleaseRing {
            ring_id,
            proof: QuiescenceProof::verified(),
        });
        if matches!(events.as_slice(), [ArenaEvent::RingReleased { .. }]) {
            Ok(())
        } else {
            Err(DataPlaneError::SessionFailed(
                "arena rejected stream release".to_owned(),
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
            ArenaAllocatorIn::AllocateStream { binding, capacity } => {
                let result = self.allocate_stream(capacity);
                let _ = ctx.send(binding, HostStreamIn::Allocated(result));
            }
            ArenaAllocatorIn::ReleaseStream { binding, ring } => {
                let result = self.release_stream(ring);
                let _ = ctx.send(binding, HostStreamIn::ReleaseComplete(result));
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
    engine: EngineHandle,
    sender: ExternalSender,
    route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    source_confirmed: bool,
    route_attempts: u16,
    lease: Option<BlobLease>,
    metadata: Option<BlobMetadata>,
    offer: Option<BlobTransferOffer>,
    written: u64,
    pending_error: Option<DataPlaneError>,
    state: DestinationTransferState,
}

/// Wiring for a [`DestinationBlobTransferActor`], bundled to keep the
/// constructor within arity limits.
struct DestinationTransferParams {
    allocator: ActorAddress,
    binding: ActorAddress,
    receiver: Arc<dyn BlobTransferReceiver>,
    failure_proxy: ActorAddress,
    source: ActorAddress,
    length: u64,
    transfer_id: BlobTransferId,
    engine: EngineHandle,
    sender: ExternalSender,
    route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
}

impl DestinationBlobTransferActor {
    fn new(params: DestinationTransferParams) -> Self {
        let DestinationTransferParams {
            allocator,
            binding,
            receiver,
            failure_proxy,
            source,
            length,
            transfer_id,
            engine,
            sender,
            route_registrar,
        } = params;
        Self {
            allocator,
            binding,
            receiver,
            failure_proxy,
            source,
            length,
            transfer_id,
            engine,
            sender,
            route_registrar,
            source_confirmed: false,
            route_attempts: 0,
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

    fn try_start_transfer(&mut self, ctx: &Ctx<'_>) {
        if self.route_attempts >= BLOB_ROUTE_RETRY_LIMIT {
            self.fault(
                ctx,
                DataPlaneError::SourceFailure(
                    "selected blob source did not respond before the transfer deadline".to_owned(),
                ),
            );
            return;
        }
        self.route_attempts += 1;
        let routable = self
            .route_registrar
            .as_ref()
            .is_none_or(|routes| routes.is_routable(self.source));
        if routable {
            let offer = self
                .offer
                .as_ref()
                .expect("filling transfer retains its offer")
                .clone();
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
                return;
            }
            if !self.source_confirmed {
                self.engine.send_after(
                    BLOB_ROUTE_RETRY,
                    self.sender.clone(),
                    ctx.self_addr(),
                    BlobTransferEvent::RouteRetry,
                );
            }
            return;
        }
        self.engine.send_after(
            BLOB_ROUTE_RETRY,
            self.sender.clone(),
            ctx.self_addr(),
            BlobTransferEvent::RouteRetry,
        );
    }

    fn seal(&mut self, ctx: &Ctx<'_>) {
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
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
                if self.length == 0 {
                    self.seal(ctx);
                } else {
                    match self.receiver.open(ctx.self_addr(), self.transfer_id) {
                        Ok(mut offer) => {
                            offer.failure_proxy = Some(self.failure_proxy);
                            self.offer = Some(offer);
                            self.state = DestinationTransferState::Filling;
                            self.try_start_transfer(ctx);
                        }
                        Err(error) => self.fault(ctx, DataPlaneError::SourceFailure(error)),
                    }
                }
            }
            BlobTransferEvent::RouteRetry
                if self.state == DestinationTransferState::Filling && !self.source_confirmed =>
            {
                self.try_start_transfer(ctx);
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
                self.source_confirmed = true;
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
                self.source_confirmed = true;
                self.seal(ctx);
            }
            BlobTransferEvent::Failed {
                transfer_id,
                reason,
            } if transfer_id == self.transfer_id => {
                self.source_confirmed = true;
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

struct NamespaceControlActor {
    namespace_proxy: ActorAddress,
    host_session: ActorAddress,
    child_session: ActorAddress,
    operation: ActorAddress,
    request: NamespaceOperation,
    completed: bool,
}

impl NamespaceControlActor {
    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<NamespaceOperationResult, DataPlaneError>) {
        self.completed = true;
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::NamespaceResolved {
                operation: self.operation,
                child_session: self.child_session,
                result,
            },
        );
        ctx.stop_self();
    }
}

impl ActorInterface for NamespaceControlActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let operation_id = namespace_operation_id(self.operation);
        let request = match &self.request {
            NamespaceOperation::Lookup { path } => NamespaceRequest::Lookup { path: path.clone() },
            NamespaceOperation::Unlink { path } => NamespaceRequest::Unregister {
                path: path.clone(),
                operation_id,
            },
            NamespaceOperation::Rename {
                source,
                destination,
                replace,
            } => NamespaceRequest::Rename {
                source: source.clone(),
                destination: destination.clone(),
                replace: *replace,
                operation_id,
            },
        };
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request,
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            self.finish(
                ctx,
                Err(DataPlaneError::SessionFailed(
                    "namespace client is unavailable".to_owned(),
                )),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        let result = match (&self.request, message) {
            (NamespaceOperation::Lookup { .. }, DataDirectoryOut::LookedUp { result, .. }) => {
                result
                    .map(NamespaceOperationResult::Node)
                    .map_err(namespace_error)
            }
            (NamespaceOperation::Unlink { .. }, DataDirectoryOut::Unregistered { result, .. })
            | (NamespaceOperation::Rename { .. }, DataDirectoryOut::Renamed { result, .. }) => {
                result
                    .map(|receipt| NamespaceOperationResult::Mutation {
                        revision: receipt.revision,
                    })
                    .map_err(namespace_error)
            }
            (_, other) => Err(DataPlaneError::SessionFailed(format!(
                "unexpected namespace control reply: {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.completed {
            let _ = ctx.send(
                self.namespace_proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct NamespaceOpenLookupActor {
    namespace_proxy: ActorAddress,
    host_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    completed: bool,
}

impl ActorInterface for NamespaceOpenLookupActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::Lookup {
                        path: self.path.clone(),
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            self.completed = true;
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::OpenResolved {
                    operation: self.operation,
                    result: Err(NamespaceError::DirectoryUnavailable(
                        "namespace client is unavailable".to_owned(),
                    )),
                },
            );
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        let result = match message {
            DataDirectoryOut::LookedUp { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected lookup reply, received {other:?}"
            ))),
        };
        self.completed = true;
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::OpenResolved {
                operation: self.operation,
                result,
            },
        );
        ctx.stop_self();
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.completed {
            let _ = ctx.send(
                self.namespace_proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct NamespaceBlobReserveActor {
    namespace_proxy: ActorAddress,
    host_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    reservation: OperationId,
    completed: bool,
}

impl ActorInterface for NamespaceBlobReserveActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::ReserveBlob {
                        path: self.path.clone(),
                        operation_id: self.reservation,
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            self.completed = true;
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::BlobReserved {
                    operation: self.operation,
                    path: self.path.clone(),
                    reservation: self.reservation,
                    result: Err(NamespaceError::DirectoryUnavailable(
                        "namespace client is unavailable".to_owned(),
                    )),
                },
            );
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        let result = match message {
            DataDirectoryOut::BlobReserved { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected blob reservation reply, received {other:?}"
            ))),
        };
        self.completed = true;
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::BlobReserved {
                operation: self.operation,
                path: self.path.clone(),
                reservation: self.reservation,
                result,
            },
        );
        ctx.stop_self();
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.completed {
            let _ = ctx.send(
                self.namespace_proxy,
                NamespaceClientIn::CancelBlobReservation {
                    path: self.path.clone(),
                    operation_id: self.reservation,
                    reply_to: ctx.self_addr(),
                },
            );
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
    reservation: Option<OperationId>,
    operation: ActorAddress,
    binding: ActorAddress,
    completed: bool,
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
                        reservation: self.reservation,
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
            self.completed = true;
            let _ = ctx.send(self.binding, response);
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.completed {
            let _ = ctx.send(
                self.namespace_proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct NamespaceBlobReservationReleaseActor {
    namespace_proxy: ActorAddress,
    binding: ActorAddress,
    path: DataPath,
    reservation: OperationId,
}

impl ActorInterface for NamespaceBlobReservationReleaseActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::ReleaseBlobReservation {
                        path: self.path.clone(),
                        operation_id: self.reservation,
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            let _ = ctx.send(
                self.binding,
                HostBindingIn::ReservationReleased(Err(DataPlaneError::SessionFailed(
                    "namespace client is unavailable".to_owned(),
                ))),
            );
            ctx.stop_self();
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        let result = match message {
            DataDirectoryOut::BlobReservationReleased { result, .. } => {
                result.map_err(namespace_error)
            }
            other => Err(DataPlaneError::SessionFailed(format!(
                "expected blob reservation release, received {other:?}"
            ))),
        };
        let _ = ctx.send(self.binding, HostBindingIn::ReservationReleased(result));
        ctx.stop_self();
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
            if matches!(result, Err(NamespaceError::PathNotFound(_))) {
                let _ = ctx.send(self.source, BlobSourceIn::Retire { reply_to: None });
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

struct NamespaceStreamOpenActor {
    proxy: ActorAddress,
    parent: ActorAddress,
    path: DataPath,
    role: StreamRole,
    replace: bool,
    ensure: bool,
    expected_revision: Option<u64>,
    descriptor: Vec<u8>,
    operation_id: OperationId,
    completed: bool,
}

impl ActorInterface for NamespaceStreamOpenActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.proxy,
            NamespaceClientIn::Request {
                request: NamespaceRequest::OpenStream {
                    path: self.path.clone(),
                    role: self.role,
                    endpoint: self.parent,
                    descriptor: self.descriptor.clone(),
                    replace: self.replace,
                    ensure: self.ensure,
                    expected_revision: self.expected_revision,
                    operation_id: self.operation_id,
                },
                reply_to: ctx.self_addr(),
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        let result = match message {
            DataDirectoryOut::StreamOpened { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected stream-open reply, received {other:?}"
            ))),
        };
        self.completed = true;
        let _ = ctx.send(self.parent, HostStreamIn::NamespaceMatched(result));
        ctx.stop_self();
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.completed {
            let _ = ctx.send(
                self.proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct NamespaceStreamCloseActor {
    proxy: ActorAddress,
    path: DataPath,
    incarnation: StreamIncarnation,
}

impl ActorInterface for NamespaceStreamCloseActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.proxy,
            NamespaceClientIn::Request {
                request: NamespaceRequest::CloseStream {
                    path: self.path.clone(),
                    incarnation: self.incarnation,
                },
                reply_to: ctx.self_addr(),
            },
        );
    }

    fn handle(&mut self, ctx: &Ctx<'_>, _message: DataDirectoryOut) {
        ctx.stop_self();
    }
}

struct RuntimeStreamNotifier {
    runtime: Runtime,
    target: ActorAddress,
}

impl StreamTransportNotifier for RuntimeStreamNotifier {
    fn notify(&self, event: StreamTransportEvent) {
        let _ = self
            .runtime
            .send_to(self.target, HostStreamIn::Transport(event));
    }
}

struct HostStreamBindingActor {
    runtime: Runtime,
    engine: EngineHandle,
    sender: ExternalSender,
    route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    host_session: ActorAddress,
    allocator: ActorAddress,
    child_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    replace: bool,
    ensure: bool,
    expected_revision: Option<u64>,
    role: StreamRole,
    namespace: NamespaceClient,
    arena: Arc<MappedArena>,
    transport: Arc<dyn StreamTransport>,
    local_descriptor: StreamPeerDescriptor,
    ring: Option<RingHandle>,
    matched: Option<StreamMatch>,
    peer_descriptor: Option<StreamPeerDescriptor>,
    peer_offer_acknowledged: bool,
    transport_installed: bool,
    transport_ready: bool,
    opened: bool,
    transport_quiesced: bool,
    terminal: Option<DataPlaneError>,
    data_waiters: Vec<ActorAddress>,
    capacity_waiters: Vec<ActorAddress>,
    close_waiters: Vec<ActorAddress>,
    release_started: bool,
    namespace_open: Option<ActorAddress>,
    peer_ack_pending: bool,
}

impl HostStreamBindingActor {
    fn operation_id(address: ActorAddress) -> OperationId {
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&address.0[..16]);
        OperationId::from_u128(u128::from_le_bytes(bytes))
    }

    fn fail_open(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if !self.opened {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::OperationFailed {
                    operation: self.operation,
                    error: error.clone(),
                },
            );
        }
        self.begin_terminal(ctx, error, true, true);
    }

    fn send_peer_offer(&mut self, ctx: &Ctx<'_>) {
        let Some(matched) = self.matched.as_ref() else {
            return;
        };
        if self.role != StreamRole::Sink
            || !matched.sink_descriptor.is_empty()
            || self.peer_offer_acknowledged
            || self.terminal.is_some()
        {
            return;
        }
        let routable = self
            .route_registrar
            .as_ref()
            .is_none_or(|routes| routes.is_routable(matched.source));
        if routable {
            let _ = ctx.send(
                matched.source,
                HostStreamIn::PeerOffer {
                    incarnation: matched.incarnation,
                    descriptor: self.local_descriptor.clone(),
                },
            );
        }
        self.engine.send_after(
            STREAM_PEER_OFFER_RETRY,
            self.sender.clone(),
            ctx.self_addr(),
            HostStreamIn::PeerOfferRetry {
                incarnation: matched.incarnation,
            },
        );
    }

    fn send_peer_termination(&mut self, ctx: &Ctx<'_>) {
        if !self.peer_ack_pending {
            return;
        }
        let Some(matched) = self.matched.as_ref() else {
            return;
        };
        let peer = match self.role {
            StreamRole::Source => matched.sink,
            StreamRole::Sink => matched.source,
        };
        let incarnation = matched.incarnation;
        let error = if matches!(self.terminal.as_ref(), Some(DataPlaneError::StreamClosed)) {
            DataPlaneError::StreamClosed
        } else {
            DataPlaneError::PeerLost
        };
        let routable = self
            .route_registrar
            .as_ref()
            .is_none_or(|routes| routes.is_routable(peer));
        if routable {
            let _ = ctx.send(
                peer,
                HostStreamIn::PeerTerminated {
                    incarnation,
                    error,
                    reply_to: Some(ctx.self_addr()),
                },
            );
        }
        self.engine.send_after(
            STREAM_PEER_OFFER_RETRY,
            self.sender.clone(),
            ctx.self_addr(),
            HostStreamIn::PeerTerminationRetry { incarnation },
        );
    }

    fn try_install_transport(&mut self, ctx: &Ctx<'_>) {
        if self.transport_installed || self.terminal.is_some() {
            return;
        }
        let (Some(ring), Some(matched)) = (self.ring, self.matched.clone()) else {
            return;
        };
        let endpoint_role = match self.role {
            StreamRole::Source => Role::Consumer,
            StreamRole::Sink => Role::Producer,
        };
        let endpoint = match byte_ring::attach_mapped(&self.arena, ring, endpoint_role) {
            Ok(endpoint) => endpoint,
            Err(error) => {
                self.fail_open(
                    ctx,
                    DataPlaneError::StreamFault(format!("attach host stream ring: {error:?}")),
                );
                return;
            }
        };
        let notifier: Arc<dyn StreamTransportNotifier> = Arc::new(RuntimeStreamNotifier {
            runtime: self.runtime.clone(),
            target: ctx.self_addr(),
        });
        let install = match self.role {
            StreamRole::Source => {
                let Some(peer) = self.peer_descriptor.clone() else {
                    return;
                };
                self.transport.install_source(StreamSourceRequest {
                    incarnation: matched.incarnation,
                    peer,
                    endpoint,
                    notifier,
                })
            }
            StreamRole::Sink => self.transport.install_sink(StreamSinkRequest {
                incarnation: matched.incarnation,
                endpoint,
                notifier,
            }),
        };
        match install {
            Ok(()) => {
                self.transport_installed = true;
                if self.role == StreamRole::Sink && matched.sink_descriptor.is_empty() {
                    self.send_peer_offer(ctx);
                }
            }
            Err(reason) => self.fail_open(ctx, DataPlaneError::StreamFault(reason)),
        }
    }

    fn open_if_ready(&mut self, ctx: &Ctx<'_>) {
        if self.opened || !self.transport_ready || self.terminal.is_some() {
            return;
        }
        let Some(ring) = self.ring else {
            return;
        };
        self.opened = true;
        let _ = ctx.send(
            self.child_session,
            ChildSessionIn::StreamOpened {
                operation: self.operation,
                host_binding: ctx.self_addr(),
                ring,
                role: match self.role {
                    StreamRole::Source => Role::Producer,
                    StreamRole::Sink => Role::Consumer,
                },
            },
        );
    }

    fn wake_waiters(
        ctx: &Ctx<'_>,
        child_session: ActorAddress,
        waiters: &mut Vec<ActorAddress>,
        result: Result<(), DataPlaneError>,
    ) {
        for reply_to in waiters.drain(..) {
            let _ = ctx.send(
                child_session,
                ChildSessionIn::StreamWake {
                    reply_to,
                    result: result.clone(),
                },
            );
        }
    }

    fn begin_terminal(
        &mut self,
        ctx: &Ctx<'_>,
        error: DataPlaneError,
        notify_peer: bool,
        terminate_transport: bool,
    ) {
        if self.terminal.is_some() {
            return;
        }
        if let Some(namespace_open) = self.namespace_open.take() {
            let _ = ctx.stop_actor(namespace_open);
        }
        if let Some(ring) = self.ring {
            let _ = byte_ring::mark_peer_terminated_mapped(&self.arena, ring);
        }
        self.terminal = Some(error.clone());
        Self::wake_waiters(
            ctx,
            self.child_session,
            &mut self.data_waiters,
            Err(error.clone()),
        );
        Self::wake_waiters(
            ctx,
            self.child_session,
            &mut self.capacity_waiters,
            Err(error.clone()),
        );
        if notify_peer && self.matched.is_some() {
            self.peer_ack_pending = true;
            self.send_peer_termination(ctx);
        }
        if let Some(matched) = &self.matched {
            let _ = ctx.spawn(NamespaceStreamCloseActor {
                proxy: self.namespace.proxy(),
                path: self.path.clone(),
                incarnation: matched.incarnation,
            });
            if self.transport_installed {
                if terminate_transport {
                    self.transport.terminate(matched.incarnation);
                }
                if !self.transport_quiesced {
                    return;
                }
            }
            if self.peer_ack_pending {
                return;
            }
        }
        self.release_ring(ctx);
    }

    fn complete_release(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        for reply_to in self.close_waiters.drain(..) {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::StreamWake {
                    reply_to,
                    result: result.clone(),
                },
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

    fn release_ring(&mut self, ctx: &Ctx<'_>) {
        if self.release_started {
            return;
        }
        self.release_started = true;
        if let Some(ring) = self.ring {
            let _ = ctx.send(
                self.allocator,
                ArenaAllocatorIn::ReleaseStream {
                    binding: ctx.self_addr(),
                    ring,
                },
            );
        } else {
            self.complete_release(ctx, Ok(()));
        }
    }
}

impl ActorInterface for HostStreamBindingActor {
    type Incoming = HostStreamIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        let _ = ctx.send(
            self.allocator,
            ArenaAllocatorIn::AllocateStream {
                binding: ctx.self_addr(),
                capacity: STREAM_RING_CAPACITY,
            },
        );
        match ctx.spawn(NamespaceStreamOpenActor {
            proxy: self.namespace.proxy(),
            parent: ctx.self_addr(),
            path: self.path.clone(),
            role: self.role,
            descriptor: self.local_descriptor.0.clone(),
            replace: self.replace,
            ensure: self.ensure,
            expected_revision: self.expected_revision,
            operation_id: Self::operation_id(ctx.self_addr()),
            completed: false,
        }) {
            Ok(namespace_open) => {
                self.namespace_open = Some(namespace_open);
            }
            Err(error) => {
                self.fail_open(ctx, DataPlaneError::SessionFailed(error.to_string()));
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostStreamIn) {
        match message {
            HostStreamIn::NamespaceMatched(result) => {
                self.namespace_open = None;
                match result {
                    Ok(matched) => {
                        let expected = match self.role {
                            StreamRole::Source => matched.source,
                            StreamRole::Sink => matched.sink,
                        };
                        if expected != ctx.self_addr() {
                            self.fail_open(
                                ctx,
                                DataPlaneError::StreamFault(
                                    "namespace matched the wrong stream endpoint".to_owned(),
                                ),
                            );
                            return;
                        }
                        if self.role == StreamRole::Source && !matched.sink_descriptor.is_empty() {
                            self.peer_descriptor =
                                Some(StreamPeerDescriptor(matched.sink_descriptor.clone()));
                        }
                        self.matched = Some(matched);
                        self.try_install_transport(ctx);
                    }
                    Err(error) => self.fail_open(ctx, namespace_error(error)),
                }
            }
            HostStreamIn::Allocated(result) => match result {
                Ok(ring) => {
                    self.ring = Some(ring);
                    self.try_install_transport(ctx);
                }
                Err(error) => self.fail_open(ctx, error),
            },
            HostStreamIn::PeerOffer {
                incarnation,
                descriptor,
            } => {
                if self.role != StreamRole::Source {
                    return;
                }
                let sink = self
                    .matched
                    .as_ref()
                    .filter(|matched| matched.incarnation == incarnation)
                    .map(|matched| matched.sink);
                if let Some(sink) = sink {
                    self.peer_descriptor = Some(descriptor);
                    let _ = ctx.send(sink, HostStreamIn::PeerOfferAck { incarnation });
                    self.try_install_transport(ctx);
                }
            }
            HostStreamIn::PeerOfferAck { incarnation } => {
                if self.role == StreamRole::Sink
                    && self
                        .matched
                        .as_ref()
                        .is_some_and(|matched| matched.incarnation == incarnation)
                {
                    self.peer_offer_acknowledged = true;
                }
            }
            HostStreamIn::PeerOfferRetry { incarnation } => {
                if self
                    .matched
                    .as_ref()
                    .is_some_and(|matched| matched.incarnation == incarnation)
                {
                    self.send_peer_offer(ctx);
                }
            }
            HostStreamIn::Transport(StreamTransportEvent::Ready) => {
                self.transport_ready = true;
                self.open_if_ready(ctx);
            }
            HostStreamIn::Transport(StreamTransportEvent::DataAvailable) => {
                Self::wake_waiters(ctx, self.child_session, &mut self.data_waiters, Ok(()));
            }
            HostStreamIn::Transport(StreamTransportEvent::CapacityAvailable) => {
                Self::wake_waiters(ctx, self.child_session, &mut self.capacity_waiters, Ok(()));
            }
            HostStreamIn::Transport(StreamTransportEvent::Fault(reason)) => {
                self.fail_open(ctx, DataPlaneError::StreamFault(reason));
            }
            HostStreamIn::Transport(StreamTransportEvent::Quiesced) => {
                self.transport_quiesced = true;
                if self.terminal.is_some() && !self.peer_ack_pending {
                    self.release_ring(ctx);
                }
            }
            HostStreamIn::DataAvailable => {
                if let Some(matched) = &self.matched {
                    self.transport.source_progress(matched.incarnation);
                }
            }
            HostStreamIn::CapacityAvailable => {
                if let Some(matched) = &self.matched {
                    self.transport.sink_progress(matched.incarnation);
                }
            }
            HostStreamIn::WaitData { reply_to } => {
                let result = self
                    .terminal
                    .as_ref()
                    .map(|error| Err(error.clone()))
                    .or_else(|| {
                        self.matched
                            .as_ref()
                            .filter(|matched| self.transport.sink_has_data(matched.incarnation))
                            .map(|_| Ok(()))
                    });
                if let Some(result) = result {
                    let _ = ctx.send(
                        self.child_session,
                        ChildSessionIn::StreamWake { reply_to, result },
                    );
                } else {
                    self.data_waiters.push(reply_to);
                }
            }
            HostStreamIn::WaitCapacity { reply_to } => {
                let result = self
                    .terminal
                    .as_ref()
                    .map(|error| Err(error.clone()))
                    .or_else(|| {
                        self.matched
                            .as_ref()
                            .filter(|matched| {
                                self.transport.source_has_capacity(matched.incarnation)
                            })
                            .map(|_| Ok(()))
                    });
                if let Some(result) = result {
                    let _ = ctx.send(
                        self.child_session,
                        ChildSessionIn::StreamWake { reply_to, result },
                    );
                } else {
                    self.capacity_waiters.push(reply_to);
                }
            }
            HostStreamIn::Close { clean, reply_to } => {
                if let Some(reply_to) = reply_to {
                    self.close_waiters.push(reply_to);
                }
                if clean {
                    self.begin_terminal(ctx, DataPlaneError::StreamClosed, false, false);
                } else {
                    self.fail_open(ctx, DataPlaneError::OperationCancelled);
                }
            }
            HostStreamIn::PeerTerminated {
                incarnation,
                error,
                reply_to,
            } => {
                if self
                    .matched
                    .as_ref()
                    .is_some_and(|matched| matched.incarnation == incarnation)
                {
                    if !self.opened {
                        let _ = ctx.send(
                            self.child_session,
                            ChildSessionIn::OperationFailed {
                                operation: self.operation,
                                error: error.clone(),
                            },
                        );
                    }
                    self.begin_terminal(ctx, error, false, true);
                    if let Some(reply_to) = reply_to {
                        let _ =
                            ctx.send(reply_to, HostStreamIn::PeerTerminationAck { incarnation });
                    }
                }
            }
            HostStreamIn::PeerTerminationAck { incarnation } => {
                if self
                    .matched
                    .as_ref()
                    .is_some_and(|matched| matched.incarnation == incarnation)
                {
                    self.peer_ack_pending = false;
                    if self.terminal.is_some()
                        && (!self.transport_installed || self.transport_quiesced)
                    {
                        self.release_ring(ctx);
                    }
                }
            }
            HostStreamIn::PeerTerminationRetry { incarnation } => {
                if self.peer_ack_pending
                    && self
                        .matched
                        .as_ref()
                        .is_some_and(|matched| matched.incarnation == incarnation)
                {
                    self.send_peer_termination(ctx);
                }
            }
            HostStreamIn::ReleaseComplete(result) => {
                if let Err(error) = &result
                    && !self.opened
                {
                    let _ = ctx.send(
                        self.child_session,
                        ChildSessionIn::OperationFailed {
                            operation: self.operation,
                            error: error.clone(),
                        },
                    );
                }
                self.complete_release(ctx, result);
            }
        }
    }

    fn on_stop(&mut self, _ctx: &Ctx<'_>) {
        if let Some(matched) = &self.matched {
            self.transport.terminate(matched.incarnation);
        }
    }
}

fn namespace_error(error: NamespaceError) -> DataPlaneError {
    match error {
        NamespaceError::PathExists(path) => DataPlaneError::PathExists(path),
        NamespaceError::PathNotFound(path) => DataPlaneError::PathNotFound(path),
        NamespaceError::WrongEntryType {
            path,
            expected,
            found,
        } => DataPlaneError::WrongEntryType {
            path,
            expected,
            found,
        },
        NamespaceError::PathReplaced(path) => DataPlaneError::PathReplaced(path),
        NamespaceError::DuplicateStreamRole { path, role } => {
            DataPlaneError::SessionFailed(format!("stream path {path} already has a {role:?}"))
        }
        NamespaceError::StaleIncarnation { path, incarnation } => {
            DataPlaneError::SessionFailed(format!(
                "stream path {path} no longer names incarnation {}:{}",
                incarnation.authority_epoch, incarnation.revision
            ))
        }
        NamespaceError::StreamActive(path) => DataPlaneError::Busy(format!(
            "stream path {path} has active or pending endpoints"
        )),
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
        expected_revision: Option<u64>,
        receiver: Arc<dyn BlobTransferReceiver>,
        engine: EngineHandle,
        sender: ExternalSender,
        route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
    },
    Write {
        length: u64,
        digest: Option<ContentDigest>,
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
    CancelOpen {
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
    ReservationReleased(Result<(), DataPlaneError>),
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
    cancelled: bool,
    reservation: Option<OperationId>,
}

/// Wiring for a namespace-read [`HostBlobBindingActor`], bundled to keep
/// the constructor within arity limits.
struct NamespaceReadParams {
    addresses: HostBindingAddresses,
    path: DataPath,
    expected_revision: Option<u64>,
    namespace: NamespaceClient,
    receiver: Arc<dyn BlobTransferReceiver>,
    engine: EngineHandle,
    sender: ExternalSender,
    route_registrar: Option<Arc<dyn HostRouteRegistrar>>,
}

impl HostBlobBindingActor {
    fn namespace_read(params: NamespaceReadParams) -> Self {
        let NamespaceReadParams {
            addresses:
                HostBindingAddresses {
                    host_session,
                    allocator,
                    child_session,
                    operation,
                },
            path,
            expected_revision,
            namespace,
            receiver,
            engine,
            sender,
            route_registrar,
        } = params;
        Self {
            host_session,
            allocator,
            child_session,
            operation,
            path,
            mode: BindingMode::NamespaceRead {
                namespace,
                expected_revision,
                receiver,
                engine,
                sender,
                route_registrar,
            },
            state: HostBindingState::Resolving,
            lease: None,
            metadata: None,
            release_outcome: None,
            auxiliary: None,
            published_source: None,
            reservation: None,
            cancelled: false,
        }
    }

    fn write(
        addresses: HostBindingAddresses,
        path: DataPath,
        length: u64,
        digest: Option<ContentDigest>,
        reservation: Option<OperationId>,
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
                digest,
                namespace,
                source_publisher,
            },
            state: HostBindingState::Allocated,
            lease: None,
            metadata: None,
            release_outcome: None,
            auxiliary: None,
            published_source: None,
            cancelled: false,
            reservation,
        }
    }

    fn begin_reservation_release(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) -> bool {
        let Some(reservation) = self.reservation.take() else {
            return false;
        };
        let BindingMode::Write {
            namespace: Some(namespace),
            ..
        } = &self.mode
        else {
            return false;
        };
        match ctx.spawn(NamespaceBlobReservationReleaseActor {
            namespace_proxy: namespace.proxy(),
            binding: ctx.self_addr(),
            path: self.path.clone(),
            reservation,
        }) {
            Ok(releaser) => {
                self.auxiliary = Some(releaser);
                self.release_outcome = Some(outcome);
                self.state = HostBindingState::Releasing;
                true
            }
            Err(_) => {
                let _ = ctx.send(
                    namespace.proxy(),
                    NamespaceClientIn::CancelBlobReservation {
                        path: self.path.clone(),
                        operation_id: reservation,
                        reply_to: ctx.self_addr(),
                    },
                );
                false
            }
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

    fn release_published(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        if let Some(lease) = self.lease.take() {
            let _ = ctx.send(
                self.allocator,
                ArenaAllocatorIn::Release {
                    binding: ctx.self_addr(),
                    lease,
                },
            );
        }
        // Published bindings are detached from their host session. The
        // allocator may already have stopped with that session, so waiting for
        // its acknowledgement would strand this binding forever. A live
        // allocator still receives the release; its own shutdown reclaims all
        // outstanding leases otherwise.
        self.finish_without_lease(ctx, outcome);
    }

    fn finish_without_lease(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        if self.begin_reservation_release(ctx, outcome.clone()) {
            return;
        }
        self.state = HostBindingState::Released;
        let read_released = matches!(outcome, ReleaseOutcome::ReadReleased);
        if let Some(auxiliary) = self.auxiliary.take() {
            let _ = ctx.stop_actor(auxiliary);
        }
        if let ReleaseOutcome::WriteAborted { operation } = outcome {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::WriteAborted { operation },
            );
        }
        if read_released {
            let _ = ctx.send(self.child_session, ChildSessionIn::BlobReleased);
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
            let _ = ctx.send(source, BlobSourceIn::Retire { reply_to: None });
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
        let (length, digest) = match &self.mode {
            BindingMode::Write { length, digest, .. } => (*length, *digest),
            BindingMode::NamespaceRead { .. } => unreachable!("handled above"),
        };
        let _ = ctx.send(
            self.allocator,
            ArenaAllocatorIn::Allocate {
                binding: ctx.self_addr(),
                kind: AllocationKind::Write { length, digest },
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
                let (receiver, failure_proxy, expected_revision, engine, sender, route_registrar) =
                    match &self.mode {
                        BindingMode::NamespaceRead {
                            namespace,
                            receiver,
                            expected_revision,
                            engine,
                            sender,
                            route_registrar,
                        } => (
                            Arc::clone(receiver),
                            namespace.proxy(),
                            *expected_revision,
                            engine.clone(),
                            sender.clone(),
                            route_registrar.clone(),
                        ),
                        _ => unreachable!("namespace resolve on non-namespace binding"),
                    };
                if expected_revision.is_some_and(|revision| revision != binding.revision) {
                    self.fail(ctx, DataPlaneError::PathReplaced(self.path.clone()));
                    return;
                }
                let mut id_bytes = [0_u8; 8];
                id_bytes.copy_from_slice(&ctx.self_addr().0[..8]);
                let transfer_id = BlobTransferId(u64::from_le_bytes(id_bytes).max(1));
                match ctx.spawn(DestinationBlobTransferActor::new(
                    DestinationTransferParams {
                        allocator: self.allocator,
                        binding: ctx.self_addr(),
                        receiver,
                        failure_proxy,
                        source: binding.source,
                        length: binding.length,
                        transfer_id,
                        engine,
                        sender,
                        route_registrar,
                    },
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
                if self.cancelled {
                    self.begin_release(ctx, ReleaseOutcome::Faulted);
                } else {
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
            }
            HostBindingIn::CancelOpen { operation } if operation == self.operation => {
                self.cancelled = true;
                match self.state {
                    HostBindingState::Resolving => {
                        self.finish_without_lease(ctx, ReleaseOutcome::Faulted);
                    }
                    HostBindingState::Filling => {
                        if matches!(self.mode, BindingMode::NamespaceRead { .. })
                            && let Some(transfer) = self.auxiliary
                        {
                            let _ = ctx.send(transfer, BlobTransferEvent::Cancel);
                        }
                    }
                    HostBindingState::Granted => {
                        self.begin_release(ctx, ReleaseOutcome::Faulted);
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
                if self.cancelled {
                    self.begin_release(ctx, ReleaseOutcome::Faulted);
                } else {
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
                        let _ = ctx.send(source, BlobSourceIn::Retire { reply_to: None });
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
                    let _ = ctx.send(source, BlobSourceIn::Retire { reply_to: None });
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
                    reservation: self.reservation,
                    operation_id,
                    operation: self.operation,
                    binding: ctx.self_addr(),
                    completed: false,
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
                self.reservation = None;
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
                self.release_published(ctx, outcome);
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
            HostBindingIn::ReservationReleased(result) => {
                self.auxiliary = None;
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
