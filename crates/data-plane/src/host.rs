//! Host-side session, binding, and arena-allocation actors.

use std::collections::{HashMap, HashSet};
use std::os::fd::{FromRawFd, OwnedFd};
use std::sync::Arc;
use std::time::Duration;

use protocol_telemetry::{
    ActorType, Outcome, Phase, Protocol, Reason, TransitionEmitter, TransitionRecord,
};
use swactor::actor::{ActorAddress, ActorExited, ActorInterface, Ctx};
use swactor::runtime::{ExternalSender, Runtime};
use swactor::std::CtxWatching;
use swactor_engine::{ActorTimer, EngineHandle};

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
use crate::byte_ring::{self, ByteRingSpec, PeerTermination, RingHandle, RingProbe, Role};
use crate::mapped_arena::MappedArena;
use crate::namespace::{
    BlobBinding as NamespaceBlobBinding, DataDirectoryIn, DataDirectoryOut, EntryKind,
    NamespaceClient, NamespaceClientIn, NamespaceError, NamespaceNode, NamespaceRequest,
    OperationId, SourceRecovery, StreamIncarnation, StreamMatch, StreamRole,
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
const STREAM_PEER_ROUTE_LOSS_GRACE: Duration = Duration::from_secs(2);
const STREAM_PEER_TERMINATION_ROUTE_RETRY_LIMIT: u8 = 20;

fn namespace_operation_id(address: ActorAddress) -> OperationId {
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&address.0[..16]);
    OperationId::from_u128(u128::from_be_bytes(bytes))
}

/// Owns a route subscription; dropping it releases the adapter's callbacks.
#[must_use = "retain the watch while the operation needs route observations"]
pub struct HostRouteWatch {
    _subscription: Box<dyn Send>,
}

impl HostRouteWatch {
    pub fn new(subscription: impl Send + 'static) -> Self {
        Self {
            _subscription: Box::new(subscription),
        }
    }
}

pub trait HostRouteRegistrar: Send + Sync + 'static {
    fn register_child(
        &self,
        child_session: ActorAddress,
        child_node: [u8; 32],
    ) -> Result<(), String>;
    fn revoke_child(&self, child_session: ActorAddress) -> Result<(), String>;

    /// Pin a remotely-hosted blob source for the lifetime of one namespace
    /// read binding. Directory gossip is eventual; a resolved transfer must
    /// not spend its bounded retry budget waiting for the source's claim.
    fn retain_source(&self, _source: ActorAddress, _source_node: [u8; 32]) -> Result<(), String> {
        Ok(())
    }

    /// Drop one namespace read binding's claim on a remotely-hosted source.
    fn release_source(&self, _source: ActorAddress) {}

    /// Whether this actor still has a registered route. Temporary transport
    /// disconnection must remain routable here; false means the actor route
    /// was revoked and no future actor acknowledgement can arrive.
    fn is_routable(&self, _actor: ActorAddress) -> bool {
        true
    }

    /// Observe route and connection readiness for one actor. Register before
    /// checking readiness; notifications are observations, not retry attempts.
    /// Static/local registrars need no subscription.
    fn watch_route(
        &self,
        _actor: ActorAddress,
        _changed: Arc<dyn Fn() + Send + Sync>,
    ) -> Option<HostRouteWatch> {
        None
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
    pub transitions: Option<TransitionEmitter>,
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
    transitions: Option<TransitionEmitter>,
    child_session: Option<ActorAddress>,
    allocator: Option<ActorAddress>,
    active_bindings: HashMap<ActorAddress, ActorAddress>,
    stream_bindings: HashMap<ActorAddress, ActorAddress>,
    pending_opens: HashMap<ActorAddress, PendingOpen>,
    open_lookups: HashMap<ActorAddress, ActorAddress>,
    namespace_operations: HashMap<ActorAddress, ActorAddress>,
    state: HostSessionState,
    close_replies: Vec<ActorAddress>,
    child_close_reply: Option<ActorAddress>,
}

impl HostDataPlaneSessionActor {
    fn enter_phase(&mut self, ctx: &Ctx<'_>, next: HostSessionState, reason: Reason) {
        let previous = self.state;
        self.state = next;
        let phase = |state| match state {
            HostSessionState::AwaitingAttachment => Phase::AwaitingAttachment,
            HostSessionState::Running => Phase::Running,
            HostSessionState::Revoked => Phase::Revoked,
            HostSessionState::Closing => Phase::Closing,
            HostSessionState::Closed => Phase::Closed,
        };
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::HostDataPlaneSessionActor,
                protocol: Protocol::HostSession,
                operation: None,
                generation: Some(self.session_generation),
                incarnation: None,
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome: (next == HostSessionState::Closed).then_some(Outcome::Success),
            });
        }
    }

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
            transitions: config.transitions,
            child_session: None,
            allocator: None,
            active_bindings: HashMap::new(),
            stream_bindings: HashMap::new(),
            pending_opens: HashMap::new(),
            open_lookups: HashMap::new(),
            namespace_operations: HashMap::new(),
            state: HostSessionState::AwaitingAttachment,
            close_replies: Vec::new(),
            child_close_reply: None,
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
                transitions: self.transitions.clone(),
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
                self.active_bindings.insert(binding, operation);
                ctx.watch(binding);
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
                transitions: self.transitions.clone(),
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
                self.active_bindings.insert(binding, operation);
                ctx.watch(binding);
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
            transitions: self.transitions.clone(),
            session_generation: self.session_generation,
            phase: StreamPhase::Resolving,
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
            ring_probe: None,
            ring_allocation_pending: true,
            matched: None,
            pending_displacements: HashMap::new(),
            fenced_displacements: HashSet::new(),
            pending_peer_terminations: HashMap::new(),
            peer_descriptor: None,
            peer_was_local: false,
            peer_state: StreamPeerState::Negotiating,
            transport_state: StreamTransportState::Absent,
            route_state: StreamRouteState::Unknown,
            open_completion: StreamOpenCompletion::Owned,
            terminal: None,
            data_waiters: Vec::new(),
            capacity_waiters: Vec::new(),
            progress_waiters: Vec::new(),
            close_waiters: Vec::new(),
            release: StreamRelease::NotStarted,
            namespace_close_result: None,
            namespace_open: None,
            namespace_close: None,
            route_watch: None,
            offer_retry: None,
            termination_retry: None,
            termination_route_retries: 0,
            route_loss_timer: None,
            route_loss_generation: 0,
            pending_peer_offers: HashMap::new(),
            detach_pending: false,
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
                ctx.watch(binding);
            }
            Err(error) => self.send_open_failure(
                ctx,
                child_session,
                operation,
                DataPlaneError::SessionFailed(error.to_string()),
            ),
        }
    }

    fn revoke_child_route(&self) {
        if let (Some(registrar), Some(child_session)) = (&self.route_registrar, self.child_session)
        {
            let _ = registrar.revoke_child(child_session);
        }
    }

    fn revoke_session(&mut self, ctx: &Ctx<'_>, revoke_child_route: bool) {
        if matches!(
            self.state,
            HostSessionState::Revoked | HostSessionState::Closing | HostSessionState::Closed
        ) {
            return;
        }
        self.enter_phase(ctx, HostSessionState::Revoked, Reason::Cancelled);
        if revoke_child_route {
            self.revoke_child_route();
        }
        for lookup in self.open_lookups.drain().map(|(_, lookup)| lookup) {
            let _ = ctx.stop_actor(lookup);
        }
        for (operation, actor) in self.namespace_operations.drain() {
            let _ = ctx.stop_actor(actor);
            if let Some(child) = self.child_session {
                let _ = ctx.send(
                    child,
                    ChildSessionIn::NamespaceResolved {
                        reply_to: operation,
                        result: Err(DataPlaneError::SessionNotRunning),
                    },
                );
            }
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
        for binding in self.active_bindings.keys().copied() {
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
        self.revoke_session(ctx, false);
        self.enter_phase(ctx, HostSessionState::Closing, Reason::CloseRequested);
        self.maybe_finish_close(ctx);
    }

    fn begin_child_close(&mut self, ctx: &Ctx<'_>, child_session: ActorAddress) {
        if self.child_session != Some(child_session) {
            let _ = ctx.send(
                child_session,
                ChildSessionIn::CloseCompleted {
                    result: Err(DataPlaneError::SessionNotRunning),
                },
            );
            return;
        }
        if self.state == HostSessionState::Closed {
            let _ = ctx.send(
                child_session,
                ChildSessionIn::CloseCompleted { result: Ok(()) },
            );
            return;
        }
        self.child_close_reply = Some(child_session);
        self.begin_close(ctx, None);
    }

    fn maybe_finish_close(&mut self, ctx: &Ctx<'_>) {
        if self.state == HostSessionState::Closing
            && self.active_bindings.is_empty()
            && self.stream_bindings.is_empty()
            && self.pending_opens.is_empty()
            && self.open_lookups.is_empty()
            && self.namespace_operations.is_empty()
        {
            self.enter_phase(ctx, HostSessionState::Closed, Reason::Completed);
            if let Some(allocator) = self.allocator.take() {
                let _ = ctx.stop_actor(allocator);
            }
            for reply_to in self.close_replies.drain(..) {
                let _ = ctx.send(reply_to, Ok::<(), DataPlaneError>(()));
            }
            if let Some(child_session) = self.child_close_reply.take() {
                let _ = ctx.send(
                    child_session,
                    ChildSessionIn::CloseCompleted { result: Ok(()) },
                );
            }
            self.revoke_child_route();
            ctx.stop_self();
        }
    }
    fn attach(
        &mut self,
        ctx: &Ctx<'_>,
        child_session: ActorAddress,
        arena_generation: u64,
        session_capability: SessionCapability,
        child_node: Option<[u8; 32]>,
    ) {
        let mut failure = if matches!(
            self.state,
            HostSessionState::Revoked | HostSessionState::Closing | HostSessionState::Closed
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
            if self.runtime.is_local_actor(child_session) {
                ctx.watch(child_session);
            }
            self.enter_phase(ctx, HostSessionState::Running, Reason::Attached);
            let _ = ctx.send(
                child_session,
                ChildSessionIn::Attached {
                    session_generation: self.session_generation,
                },
            );
        }
    }

    fn namespace_requested(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        request: NamespaceOperation,
        child_session: ActorAddress,
    ) {
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
            phase: NamespaceOperationPhase::Created,
            transitions: self.transitions.clone(),
            generation: Some(self.session_generation),
        }) {
            Ok(actor) => {
                self.namespace_operations.insert(operation, actor);
                ctx.watch(actor);
            }
            Err(error) => self.send_namespace_result(
                ctx,
                child_session,
                operation,
                Err(DataPlaneError::SessionFailed(error.to_string())),
            ),
        }
    }

    fn namespace_resolved(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        child_session: ActorAddress,
        result: Result<NamespaceOperationResult, DataPlaneError>,
    ) {
        if self.namespace_operations.remove(&operation).is_some() {
            self.send_namespace_result(ctx, child_session, operation, result);
        }
        self.maybe_finish_close(ctx);
    }

    fn namespace_cancelled(&mut self, ctx: &Ctx<'_>, operation: ActorAddress) {
        if let Some(actor) = self.namespace_operations.remove(&operation) {
            let _ = ctx.stop_actor(actor);
            if let Some(child) = self.child_session {
                self.send_namespace_result(
                    ctx,
                    child,
                    operation,
                    Err(DataPlaneError::OperationCancelled),
                );
            }
        }
        self.maybe_finish_close(ctx);
    }

    fn open_requested(
        &mut self,
        ctx: &Ctx<'_>,
        path: DataPath,
        options: OpenOptions,
        policy: OpenPolicy,
        child_session: ActorAddress,
        operation: ActorAddress,
    ) {
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
                        "stream ensure policy does not accept blob creation flags".to_owned(),
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
                DataPlaneError::SessionFailed("data namespace service is unavailable".to_owned()),
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
            phase: NamespaceOperationPhase::Created,
            transitions: self.transitions.clone(),
            generation: Some(self.session_generation),
        }) {
            Ok(lookup) => {
                self.open_lookups.insert(operation, lookup);
                ctx.watch(lookup);
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

    fn open_resolved(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        result: Result<NamespaceNode, NamespaceError>,
    ) {
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
                            "fixed-length blob creation requires allocation metadata".to_owned(),
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
                        phase: NamespaceOperationPhase::Created,
                        transitions: self.transitions.clone(),
                        generation: Some(self.session_generation),
                    }) {
                        Ok(resolver) => {
                            self.open_lookups.insert(operation, resolver);
                            ctx.watch(resolver);
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
                self.send_open_failure(ctx, child_session, operation, namespace_error(error));
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
                    self.spawn_blob_read(ctx, child_session, operation, path, Some(revision));
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
                            "non-truncating writable blob opens are not supported".to_owned(),
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

    fn blob_reserved(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        reserved_path: DataPath,
        reservation: OperationId,
        result: Result<(), NamespaceError>,
    ) {
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
            Err(error) => {
                self.send_open_failure(ctx, child_session, operation, namespace_error(error))
            }
        }
    }

    fn open_cancelled(&mut self, ctx: &Ctx<'_>, operation: ActorAddress) {
        if let Some(pending) = self.pending_opens.remove(&operation) {
            self.send_open_failure(
                ctx,
                pending.child_session,
                operation,
                DataPlaneError::OperationCancelled,
            );
        }
        if let Some(lookup) = self.open_lookups.remove(&operation) {
            let _ = ctx.stop_actor(lookup);
        }
        for binding in self.active_bindings.keys().copied() {
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

    fn stream_open_forwarded(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        host_binding: ActorAddress,
        incarnation: StreamIncarnation,
    ) {
        if self.stream_bindings.get(&operation) == Some(&host_binding) {
            let _ = ctx.send(host_binding, HostStreamIn::OpenForwarded { incarnation });
        }
    }

    fn stream_control(&mut self, ctx: &Ctx<'_>, binding: ActorAddress, message: HostStreamIn) {
        let reply_to = message.child_reply_to();
        let completed_close = matches!(
            &message,
            HostStreamIn::Close {
                reply_to: Some(_),
                ..
            }
        );
        let active = self
            .stream_bindings
            .values()
            .any(|stream_binding| *stream_binding == binding);
        let delivered = active && ctx.send(binding, message).is_ok();
        if !delivered && let (Some(child_session), Some(reply_to)) = (self.child_session, reply_to)
        {
            let result = if completed_close {
                Ok(())
            } else {
                Err(DataPlaneError::StreamClosed)
            };
            let _ = ctx.send(
                child_session,
                ChildSessionIn::StreamWake { reply_to, result },
            );
        }
    }

    fn release_blob(
        &mut self,
        ctx: &Ctx<'_>,
        binding: ActorAddress,
        lease_id: crate::ids::BlobLeaseId,
        generation: u64,
    ) {
        let delivered = self.active_bindings.contains_key(&binding)
            && ctx
                .send(
                    binding,
                    HostBindingIn::Release {
                        lease_id,
                        generation,
                    },
                )
                .is_ok();
        if !delivered && let Some(child) = self.child_session {
            let _ = ctx.send(
                child,
                ChildSessionIn::BlobReleased {
                    binding,
                    lease_id,
                    generation,
                },
            );
        }
    }

    fn seal_write_blob(
        &mut self,
        ctx: &Ctx<'_>,
        binding: ActorAddress,
        operation: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) {
        let delivered = self.active_bindings.get(&binding) == Some(&operation)
            && ctx
                .send(
                    binding,
                    HostBindingIn::Seal {
                        operation,
                        lease,
                        metadata,
                    },
                )
                .is_ok();
        if !delivered && let Some(child) = self.child_session {
            self.send_open_failure(ctx, child, operation, DataPlaneError::SessionNotRunning);
        }
    }

    fn abort_write_blob(
        &mut self,
        ctx: &Ctx<'_>,
        binding: ActorAddress,
        operation: ActorAddress,
        lease_id: crate::ids::BlobLeaseId,
        generation: u64,
    ) {
        let delivered = self.active_bindings.get(&binding) == Some(&operation)
            && ctx
                .send(
                    binding,
                    HostBindingIn::Abort {
                        operation,
                        lease_id,
                        generation,
                    },
                )
                .is_ok();
        if !delivered && let Some(child) = self.child_session {
            self.send_open_failure(ctx, child, operation, DataPlaneError::SessionNotRunning);
        }
    }

    fn binding_faulted(
        &mut self,
        ctx: &Ctx<'_>,
        binding: ActorAddress,
        operation: ActorAddress,
        error: DataPlaneError,
    ) {
        if self.active_bindings.contains_key(&binding)
            && let Some(child) = self.child_session
        {
            let _ = ctx.send(child, ChildSessionIn::OperationFailed { operation, error });
        }
    }

    fn binding_done(&mut self, ctx: &Ctx<'_>, binding: ActorAddress) {
        self.active_bindings.remove(&binding);
        let was_stream = self
            .stream_bindings
            .values()
            .any(|stream_binding| *stream_binding == binding);
        self.stream_bindings
            .retain(|_, stream_binding| *stream_binding != binding);
        if was_stream {
            let _ = ctx.send(binding, HostStreamIn::SessionDetached);
        }
        self.maybe_finish_close(ctx);
    }

    fn binding_detached(&mut self, ctx: &Ctx<'_>, binding: ActorAddress) {
        self.active_bindings.remove(&binding);
        self.stream_bindings
            .retain(|_, stream_binding| *stream_binding != binding);
        self.maybe_finish_close(ctx);
    }

    fn configure_execution(&mut self, ctx: &Ctx<'_>, execution_id: String, reply_to: ActorAddress) {
        let result =
            if self.state != HostSessionState::AwaitingAttachment || self.child_session.is_some() {
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

    fn revoke(&mut self, ctx: &Ctx<'_>) {
        self.revoke_session(ctx, true);
    }

    fn close(&mut self, ctx: &Ctx<'_>, reply_to: Option<ActorAddress>) {
        self.begin_close(ctx, reply_to);
    }

    fn close_child(&mut self, ctx: &Ctx<'_>, child_session: ActorAddress) {
        self.begin_child_close(ctx, child_session)
    }
}

impl ActorInterface for HostDataPlaneSessionActor {
    type Incoming = HostSessionIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, self.state, Reason::Started);
        let arena = self.arena.take().expect("host arena is installed once");
        let allocator = ctx
            .spawn(ArenaAllocatorActor::new(
                self.transitions.clone(),
                arena,
                self.arena_generation,
                self.runtime.clone(),
                self.source_sender.clone(),
            ))
            .expect("spawn arena allocator actor");
        self.allocator = Some(allocator);
        ctx.watch(allocator);
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostSessionIn) {
        match message {
            HostSessionIn::Attach {
                child_session,
                arena_generation,
                session_capability,
                child_node,
            } => self.attach(
                ctx,
                child_session,
                arena_generation,
                session_capability,
                child_node,
            ),
            HostSessionIn::Namespace {
                operation,
                request,
                child_session,
            } => self.namespace_requested(ctx, operation, request, child_session),
            HostSessionIn::NamespaceResolved {
                operation,
                child_session,
                result,
            } => self.namespace_resolved(ctx, operation, child_session, result),
            HostSessionIn::CancelNamespace { operation } => {
                self.namespace_cancelled(ctx, operation)
            }
            HostSessionIn::Open {
                path,
                options,
                policy,
                child_session,
                operation,
            } => self.open_requested(ctx, path, options, policy, child_session, operation),
            HostSessionIn::OpenResolved { operation, result } => {
                self.open_resolved(ctx, operation, result)
            }
            HostSessionIn::BlobReserved {
                operation,
                path: reserved_path,
                reservation,
                result,
            } => self.blob_reserved(ctx, operation, reserved_path, reservation, result),
            HostSessionIn::CancelOpen { operation } => self.open_cancelled(ctx, operation),
            HostSessionIn::StreamOpenForwarded {
                operation,
                host_binding,
                incarnation,
            } => self.stream_open_forwarded(ctx, operation, host_binding, incarnation),
            HostSessionIn::StreamControl { binding, message } => {
                self.stream_control(ctx, binding, message)
            }
            HostSessionIn::ReleaseBlob {
                binding,
                lease_id,
                generation,
            } => self.release_blob(ctx, binding, lease_id, generation),
            HostSessionIn::SealWriteBlob {
                binding,
                operation,
                lease,
                metadata,
            } => self.seal_write_blob(ctx, binding, operation, lease, metadata),
            HostSessionIn::AbortWriteBlob {
                binding,
                operation,
                lease_id,
                generation,
            } => self.abort_write_blob(ctx, binding, operation, lease_id, generation),
            HostSessionIn::BindingFaulted {
                binding,
                operation,
                error,
            } => self.binding_faulted(ctx, binding, operation, error),
            HostSessionIn::BindingDone { binding } => self.binding_done(ctx, binding),
            HostSessionIn::BindingDetached { binding } => self.binding_detached(ctx, binding),
            HostSessionIn::ConfigureExecution {
                execution_id,
                reply_to,
            } => self.configure_execution(ctx, execution_id, reply_to),
            HostSessionIn::Revoke => self.revoke(ctx),
            HostSessionIn::Close { reply_to } => self.close(ctx, reply_to),
            HostSessionIn::CloseChild { child_session } => self.close_child(ctx, child_session),
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if Some(exited.addr) == self.child_session {
            self.begin_close(ctx, None);
            return;
        }
        if Some(exited.addr) == self.allocator {
            self.allocator = None;
            self.begin_close(ctx, None);
            return;
        }
        if let Some(operation) = self.active_bindings.remove(&exited.addr)
            && let Some(child) = self.child_session
        {
            self.send_open_failure(ctx, child, operation, DataPlaneError::SessionNotRunning);
        }
        if let Some(operation) = self
            .stream_bindings
            .iter()
            .find_map(|(operation, binding)| (*binding == exited.addr).then_some(*operation))
        {
            self.stream_bindings.remove(&operation);
            if let Some(child) = self.child_session {
                self.send_open_failure(ctx, child, operation, DataPlaneError::SessionNotRunning);
            }
        }
        if let Some(operation) = self
            .open_lookups
            .iter()
            .find_map(|(operation, lookup)| (*lookup == exited.addr).then_some(*operation))
        {
            self.open_lookups.remove(&operation);
            if let Some(pending) = self.pending_opens.remove(&operation) {
                self.send_open_failure(
                    ctx,
                    pending.child_session,
                    operation,
                    DataPlaneError::SessionNotRunning,
                );
            }
        }
        if let Some(operation) = self
            .namespace_operations
            .iter()
            .find_map(|(operation, actor)| (*actor == exited.addr).then_some(*operation))
        {
            self.namespace_operations.remove(&operation);
            if let Some(child) = self.child_session {
                self.send_namespace_result(
                    ctx,
                    child,
                    operation,
                    Err(DataPlaneError::SessionNotRunning),
                );
            }
        }
        self.maybe_finish_close(ctx);
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.state != HostSessionState::Closed {
            self.revoke_session(ctx, false);
            for reply_to in self.close_replies.drain(..) {
                let _ = ctx.send(reply_to, Err::<(), _>(DataPlaneError::SessionNotRunning));
            }
            if let Some(child) = self.child_close_reply.take() {
                let _ = ctx.send(
                    child,
                    ChildSessionIn::CloseCompleted {
                        result: Err(DataPlaneError::SessionNotRunning),
                    },
                );
            }
        }
        self.revoke_child_route();
        if let Some(allocator) = self.allocator.take() {
            let _ = ctx.stop_actor(allocator);
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
    allocator: ActorAddress,
    lease: BlobLease,
    binding: ActorAddress,
}

impl BlobSourceRetirement for ArenaSourceRetirement {
    fn binding(&self) -> ActorAddress {
        self.binding
    }

    fn retired(&self) {
        if self.runtime.is_local_actor(self.allocator)
            && self
                .runtime
                .send_to(
                    self.allocator,
                    ArenaAllocatorIn::Release {
                        binding: self.binding,
                        lease: self.lease,
                    },
                )
                .is_ok()
        {
            return;
        }
        // A stopped allocator already dropped the arena's lease ownership.
        let _ = self
            .runtime
            .send_to(self.binding, HostBindingIn::ReleasePublished);
    }
}

struct ArenaAllocatorActor {
    transitions: Option<TransitionEmitter>,
    arena: ArenaManager,
    next_request_id: u64,
    next_generation: u64,
    runtime: Runtime,
    source_sender: Option<Arc<dyn BlobTransferSender>>,
}

impl ArenaAllocatorActor {
    fn new(
        transitions: Option<TransitionEmitter>,
        arena: ArenaManager,
        arena_generation: u64,
        runtime: Runtime,
        source_sender: Option<Arc<dyn BlobTransferSender>>,
    ) -> Self {
        Self {
            transitions,
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
            allocator: ctx.self_addr(),
            lease,
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
        .map_err(namespace_error)?
        .with_transitions(self.transitions.clone());
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
    CancellingAllocation,
    AwaitingSource,
    Filling,
    Sealing,
    Releasing,
    Finished,
}

struct DestinationBlobTransferActor {
    transitions: Option<TransitionEmitter>,
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
    route_attempts: u16,
    route_watch: Option<HostRouteWatch>,
    route_retry: Option<ActorTimer>,
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
    transitions: Option<TransitionEmitter>,
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
    fn enter_phase(&mut self, ctx: &Ctx<'_>, next: DestinationTransferState, reason: Reason) {
        let previous = self.state;
        self.state = next;
        let phase = |state| match state {
            DestinationTransferState::Allocating => Phase::Allocating,
            DestinationTransferState::CancellingAllocation => Phase::Aborting,
            DestinationTransferState::AwaitingSource => Phase::AwaitingPeer,
            DestinationTransferState::Filling => Phase::Filling,
            DestinationTransferState::Sealing => Phase::Sealing,
            DestinationTransferState::Releasing => Phase::Releasing,
            DestinationTransferState::Finished => Phase::Finished,
        };
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::DestinationBlobTransferActor,
                protocol: Protocol::BlobTransfer,
                operation: Some(self.binding.0[..16].try_into().expect("binding identity")),
                generation: self.lease.map(|lease| lease.generation),
                incarnation: Some(self.transfer_id.0),
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome: (next == DestinationTransferState::Finished).then_some(
                    if reason == Reason::Failed {
                        Outcome::Failed
                    } else {
                        Outcome::Success
                    },
                ),
            });
        }
    }

    fn new(params: DestinationTransferParams) -> Self {
        let DestinationTransferParams {
            transitions,
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
            transitions,
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
            route_attempts: 0,
            route_watch: None,
            route_retry: None,
            lease: None,
            metadata: None,
            offer: None,
            written: 0,
            pending_error: None,
            state: DestinationTransferState::Allocating,
        }
    }

    fn stop_route_wait(&mut self) {
        self.route_watch = None;
        if let Some(timer) = self.route_retry.take() {
            timer.cancel();
        }
    }

    fn finish_failure(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        self.stop_route_wait();
        self.enter_phase(ctx, DestinationTransferState::Finished, Reason::Failed);
        let _ = ctx.send(self.binding, HostBindingIn::TransferFailed(error));
        ctx.stop_self();
    }

    fn try_start_transfer(&mut self, ctx: &Ctx<'_>, retry: bool) {
        if retry && self.route_attempts >= BLOB_ROUTE_RETRY_LIMIT {
            self.fault(
                ctx,
                DataPlaneError::SourceFailure(
                    "selected blob source did not respond before the transfer deadline".to_owned(),
                ),
            );
            return;
        }
        if retry {
            self.route_attempts += 1;
        }
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
        }
        if self.route_retry.is_none() {
            self.route_retry = Some(self.engine.send_after(
                BLOB_ROUTE_RETRY,
                self.sender.clone(),
                ctx.self_addr(),
                BlobTransferEvent::RouteRetry,
            ));
        }
    }

    fn seal(&mut self, ctx: &Ctx<'_>) {
        self.stop_route_wait();
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
        self.enter_phase(ctx, DestinationTransferState::Sealing, Reason::Completed);
        if ctx
            .send(
                self.allocator,
                ArenaAllocatorIn::SealTransfer {
                    transfer: ctx.self_addr(),
                    lease: self.lease.expect("allocated destination lease"),
                    metadata: self.metadata.clone().expect("destination metadata"),
                    written: self.written,
                },
            )
            .is_err()
        {
            self.finish_failure(ctx, DataPlaneError::SessionNotRunning);
        }
    }

    fn fault(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if matches!(
            self.state,
            DestinationTransferState::Releasing | DestinationTransferState::Finished
        ) {
            return;
        }
        self.stop_route_wait();
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
        if let Some(lease) = self.lease {
            self.pending_error = Some(error);
            self.enter_phase(
                ctx,
                DestinationTransferState::Releasing,
                Reason::ReleaseRequested,
            );
            if ctx
                .send(
                    self.allocator,
                    ArenaAllocatorIn::AbortTransfer {
                        transfer: ctx.self_addr(),
                        lease,
                    },
                )
                .is_err()
            {
                self.finish_failure(ctx, DataPlaneError::SessionNotRunning);
            }
        } else if matches!(
            self.state,
            DestinationTransferState::Allocating | DestinationTransferState::CancellingAllocation
        ) {
            self.pending_error = Some(error);
            self.enter_phase(
                ctx,
                DestinationTransferState::CancellingAllocation,
                Reason::Cancelled,
            );
        } else {
            self.finish_failure(ctx, error);
        }
    }
    fn allocated(
        &mut self,
        ctx: &Ctx<'_>,
        result: Result<(BlobLease, BlobMetadata), DataPlaneError>,
    ) {
        match result {
            Ok((lease, metadata))
                if matches!(
                    self.state,
                    DestinationTransferState::Allocating
                        | DestinationTransferState::CancellingAllocation
                ) =>
            {
                self.lease = Some(lease);
                self.metadata = Some(metadata);
                if let Some(error) = self.pending_error.take() {
                    self.fault(ctx, error);
                    return;
                }
                if self.length == 0 {
                    self.seal(ctx);
                } else {
                    match self.receiver.open(ctx.self_addr(), self.transfer_id) {
                        Ok(mut offer) => {
                            offer.failure_proxy = Some(self.failure_proxy);
                            self.offer = Some(offer);
                            self.enter_phase(
                                ctx,
                                DestinationTransferState::AwaitingSource,
                                Reason::Allocated,
                            );
                            if let Some(routes) = &self.route_registrar {
                                let sender = self.sender.clone();
                                let target = ctx.self_addr();
                                self.route_watch = routes.watch_route(
                                    self.source,
                                    Arc::new(move || {
                                        let _ =
                                            sender.send_to(target, BlobTransferEvent::RouteChanged);
                                    }),
                                );
                            }
                            self.try_start_transfer(ctx, true);
                        }
                        Err(error) => self.fault(ctx, DataPlaneError::SourceFailure(error)),
                    }
                }
            }
            Err(error)
                if matches!(
                    self.state,
                    DestinationTransferState::Allocating
                        | DestinationTransferState::CancellingAllocation
                ) =>
            {
                self.finish_failure(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn allocator_failed(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        self.fault(ctx, error);
    }

    fn route_retry(&mut self, ctx: &Ctx<'_>) {
        match () {
            () if self.state == DestinationTransferState::AwaitingSource => {
                self.route_retry = None;
                self.try_start_transfer(ctx, true);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn route_changed(&mut self, ctx: &Ctx<'_>) {
        match () {
            () if self.state == DestinationTransferState::AwaitingSource => {
                self.try_start_transfer(ctx, false);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn chunk(&mut self, ctx: &Ctx<'_>, transfer_id: BlobTransferId, bytes: Vec<u8>) {
        match (transfer_id, bytes) {
            (transfer_id, bytes)
                if matches!(
                    self.state,
                    DestinationTransferState::AwaitingSource | DestinationTransferState::Filling
                ) && transfer_id == self.transfer_id =>
            {
                self.enter_phase(ctx, DestinationTransferState::Filling, Reason::Ready);
                self.stop_route_wait();
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
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn finished(&mut self, ctx: &Ctx<'_>, transfer_id: BlobTransferId) {
        match (transfer_id,) {
            (transfer_id,)
                if matches!(
                    self.state,
                    DestinationTransferState::AwaitingSource | DestinationTransferState::Filling
                ) && transfer_id == self.transfer_id =>
            {
                self.enter_phase(ctx, DestinationTransferState::Filling, Reason::Ready);
                self.seal(ctx);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn failed(&mut self, ctx: &Ctx<'_>, transfer_id: BlobTransferId, reason: String) {
        match (transfer_id, reason) {
            (transfer_id, reason)
                if transfer_id == self.transfer_id
                    && matches!(
                        self.state,
                        DestinationTransferState::AwaitingSource
                            | DestinationTransferState::Filling
                    ) =>
            {
                self.enter_phase(ctx, DestinationTransferState::Filling, Reason::Ready);
                self.fault(ctx, DataPlaneError::SourceFailure(reason));
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn sealed(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        match result {
            Ok(()) if self.state == DestinationTransferState::Sealing => {
                if ctx
                    .send(
                        self.binding,
                        HostBindingIn::TransferReady {
                            lease: self.lease.expect("sealed destination lease"),
                            metadata: self.metadata.clone().expect("sealed destination metadata"),
                        },
                    )
                    .is_err()
                {
                    self.fault(ctx, DataPlaneError::OperationCancelled);
                } else {
                    self.lease = None;
                    self.enter_phase(ctx, DestinationTransferState::Finished, Reason::Completed);
                    ctx.stop_self();
                }
            }
            Err(error) if self.state == DestinationTransferState::Sealing => {
                self.fault(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn released(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        match result {
            result if self.state == DestinationTransferState::Releasing => {
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
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn cancel(&mut self, ctx: &Ctx<'_>) {
        self.fault(ctx, DataPlaneError::OperationCancelled);
    }
}

impl ActorInterface for DestinationBlobTransferActor {
    type Incoming = BlobTransferEvent;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, self.state, Reason::Started);
        ctx.watch(self.allocator);
        ctx.watch(self.binding);
        ctx.watch(self.source);
        if ctx
            .send(
                self.allocator,
                ArenaAllocatorIn::AllocateTransfer {
                    transfer: ctx.self_addr(),
                    kind: AllocationKind::FillingRead {
                        length: self.length,
                        digest: None,
                    },
                },
            )
            .is_err()
        {
            self.finish_failure(ctx, DataPlaneError::SessionNotRunning);
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: BlobTransferEvent) {
        match message {
            BlobTransferEvent::Allocated(result) => self.allocated(ctx, result),
            BlobTransferEvent::AllocatorFailed(error) => self.allocator_failed(ctx, error),
            BlobTransferEvent::RouteRetry => self.route_retry(ctx),
            BlobTransferEvent::RouteChanged => self.route_changed(ctx),
            BlobTransferEvent::Chunk { transfer_id, bytes } => self.chunk(ctx, transfer_id, bytes),
            BlobTransferEvent::Finished { transfer_id } => self.finished(ctx, transfer_id),
            BlobTransferEvent::Failed {
                transfer_id,
                reason,
            } => self.failed(ctx, transfer_id, reason),
            BlobTransferEvent::Sealed(result) => self.sealed(ctx, result),
            BlobTransferEvent::Released(result) => self.released(ctx, result),
            BlobTransferEvent::Cancel => self.cancel(ctx),
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if self.state == DestinationTransferState::Finished {
            return;
        }
        if exited.addr == self.allocator {
            self.lease = None;
            self.finish_failure(ctx, DataPlaneError::SessionNotRunning);
        } else if exited.addr == self.binding {
            self.fault(ctx, DataPlaneError::OperationCancelled);
        } else if exited.addr == self.source {
            self.fault(ctx, DataPlaneError::PeerLost);
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if let Some(offer) = self.offer.take() {
            self.receiver.cancel(&offer);
        }
        if self.state != DestinationTransferState::Finished {
            if let Some(lease) = self.lease.take() {
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::AbortTransfer {
                        transfer: ctx.self_addr(),
                        lease,
                    },
                );
            }
            let _ = ctx.send(
                self.binding,
                HostBindingIn::TransferFailed(DataPlaneError::OperationCancelled),
            );
        }
    }
}

impl Drop for DestinationBlobTransferActor {
    fn drop(&mut self) {
        self.stop_route_wait();
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NamespaceOperationPhase {
    Created,
    AwaitingReply,
    Finished(Outcome),
}

impl NamespaceOperationPhase {
    fn telemetry(self) -> Phase {
        match self {
            Self::Created => Phase::Created,
            Self::AwaitingReply => Phase::AwaitingReply,
            Self::Finished(_) => Phase::Finished,
        }
    }

    fn outcome(self) -> Option<Outcome> {
        match self {
            Self::Finished(outcome) => Some(outcome),
            _ => None,
        }
    }
}

struct NamespaceControlActor {
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    generation: Option<u64>,
    namespace_proxy: ActorAddress,
    host_session: ActorAddress,
    child_session: ActorAddress,
    operation: ActorAddress,
    request: NamespaceOperation,
}

impl NamespaceControlActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceControlActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.generation,
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<NamespaceOperationResult, DataPlaneError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
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
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.namespace_proxy);
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
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
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

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.namespace_proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
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
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    generation: Option<u64>,
    namespace_proxy: ActorAddress,
    host_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
}

impl NamespaceOpenLookupActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceOpenLookupActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.generation,
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<NamespaceNode, NamespaceError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::OpenResolved {
                operation: self.operation,
                result,
            },
        );
        ctx.stop_self();
    }
}

impl ActorInterface for NamespaceOpenLookupActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.namespace_proxy);
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
            self.finish(
                ctx,
                Err(NamespaceError::DirectoryUnavailable(
                    "namespace client is unavailable".to_owned(),
                )),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::LookedUp { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected lookup reply, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.namespace_proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
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
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    generation: Option<u64>,
    namespace_proxy: ActorAddress,
    host_session: ActorAddress,
    operation: ActorAddress,
    path: DataPath,
    reservation: OperationId,
}

impl NamespaceBlobReserveActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceBlobReserveActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.generation,
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<(), NamespaceError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
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
}

impl ActorInterface for NamespaceBlobReserveActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.namespace_proxy);
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
            self.finish(
                ctx,
                Err(NamespaceError::DirectoryUnavailable(
                    "namespace client is unavailable".to_owned(),
                )),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::BlobReserved { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected blob reservation reply, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.namespace_proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
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
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    generation: Option<u64>,
    operation: ActorAddress,
    namespace_proxy: ActorAddress,
    path: DataPath,
    binding: ActorAddress,
}

impl NamespaceResolveActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceResolveActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.generation,
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<NamespaceBlobBinding, DataPlaneError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        if let Ok(binding) = &result {
            self.generation = Some(binding.revision);
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
        let _ = ctx.send(self.binding, HostBindingIn::Resolved(result));
        ctx.stop_self();
    }
}

impl ActorInterface for NamespaceResolveActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.namespace_proxy);
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
            self.finish(
                ctx,
                Err(DataPlaneError::SessionFailed(
                    "namespace client is unavailable".to_owned(),
                )),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::Resolved { result, .. } => result.map_err(namespace_error),
            other => Err(DataPlaneError::SessionFailed(format!(
                "expected resolve reply, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.namespace_proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
            let _ = ctx.send(
                self.namespace_proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct NamespacePublishActor {
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    generation: Option<u64>,
    namespace_proxy: ActorAddress,
    path: DataPath,
    source: ActorAddress,
    source_node: [u8; 32],
    length: u64,
    operation_id: OperationId,
    reservation: Option<OperationId>,
    operation: ActorAddress,
    binding: ActorAddress,
}

impl NamespacePublishActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespacePublishActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.generation,
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
        let response = match result {
            Ok(()) => HostBindingIn::PublicationAccepted {
                operation: self.operation,
            },
            Err(error) => HostBindingIn::PublicationRejected(error),
        };
        let _ = ctx.send(self.binding, response);
        ctx.stop_self();
    }
}

impl ActorInterface for NamespacePublishActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.namespace_proxy);
        if ctx
            .send(
                self.namespace_proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::Register {
                        path: self.path.clone(),
                        source: self.source,
                        source_node: self.source_node,
                        length: self.length,
                        recovery: SourceRecovery::Actor {
                            actor: self.source,
                            node: self.source_node,
                            owner: Some(self.binding),
                        },
                        operation_id: self.operation_id,
                        reservation: self.reservation,
                    },
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
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::Registered { result, .. } => {
                result.map(|_| ()).map_err(namespace_error)
            }
            other => Err(DataPlaneError::SessionFailed(format!(
                "expected publish reply, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.namespace_proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
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
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    generation: Option<u64>,
    operation: ActorAddress,
    namespace_proxy: ActorAddress,
    binding: ActorAddress,
    path: DataPath,
    reservation: OperationId,
}

impl NamespaceBlobReservationReleaseActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceBlobReservationReleaseActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.generation,
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
        let _ = ctx.send(self.binding, HostBindingIn::ReservationReleased(result));
        ctx.stop_self();
    }
}

impl ActorInterface for NamespaceBlobReservationReleaseActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.namespace_proxy);
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
            self.finish(
                ctx,
                Err(DataPlaneError::SessionFailed(
                    "namespace client is unavailable".to_owned(),
                )),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::BlobReservationReleased { result, .. } => {
                result.map_err(namespace_error)
            }
            other => Err(DataPlaneError::SessionFailed(format!(
                "expected blob reservation release, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.namespace_proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
            let _ = ctx.send(
                self.namespace_proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct NamespaceUnpublishActor {
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    generation: Option<u64>,
    operation: ActorAddress,
    binding: ActorAddress,
    namespace_proxy: ActorAddress,
    path: DataPath,
    operation_id: OperationId,
    source: ActorAddress,
}

impl NamespaceUnpublishActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceUnpublishActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.generation,
                incarnation: None,
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
        let _ = ctx.send(self.binding, HostBindingIn::Unpublished(result));
        ctx.stop_self();
    }
}

impl ActorInterface for NamespaceUnpublishActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.namespace_proxy);
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
            self.finish(ctx, Err(DataPlaneError::SessionNotRunning));
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::Unregistered { result: Ok(_), .. } => Ok(()),
            DataDirectoryOut::Unregistered {
                result: Err(NamespaceError::PathNotFound(_)),
                ..
            } => {
                let _ = ctx.send(self.source, BlobSourceIn::Retire { reply_to: None });
                Ok(())
            }
            DataDirectoryOut::Unregistered {
                result: Err(error), ..
            } => Err(namespace_error(error)),
            other => Err(DataPlaneError::SessionFailed(format!(
                "expected unpublish reply, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.namespace_proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            let _ = ctx.send(
                self.binding,
                HostBindingIn::Unpublished(Err(DataPlaneError::SessionNotRunning)),
            );
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
            let _ = ctx.send(
                self.binding,
                HostBindingIn::Unpublished(Err(DataPlaneError::SessionNotRunning)),
            );
            let _ = ctx.send(
                self.namespace_proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct NamespaceStreamOpenActor {
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    session_generation: u64,
    incarnation: Option<StreamIncarnation>,
    operation: ActorAddress,
    proxy: ActorAddress,
    parent: ActorAddress,
    path: DataPath,
    role: StreamRole,
    replace: bool,
    ensure: bool,
    expected_revision: Option<u64>,
    descriptor: Vec<u8>,
    operation_id: OperationId,
}

impl NamespaceStreamOpenActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceStreamOpenActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: Some(
                    self.incarnation
                        .map_or(self.session_generation, |incarnation| {
                            incarnation.authority_epoch
                        }),
                ),
                incarnation: self.incarnation.map(|incarnation| incarnation.revision),
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<StreamMatch, NamespaceError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        if let Ok(matched) = &result {
            self.incarnation = Some(matched.incarnation);
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
        let _ = ctx.send(self.parent, HostStreamIn::NamespaceMatched(result));
        ctx.stop_self();
    }
}

impl ActorInterface for NamespaceStreamOpenActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.proxy);
        if ctx
            .send(
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
            )
            .is_err()
        {
            self.finish(
                ctx,
                Err(NamespaceError::DirectoryUnavailable(
                    "namespace client is unavailable".to_owned(),
                )),
            );
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::StreamOpened { result, .. } => result,
            other => Err(NamespaceError::Protocol(format!(
                "expected stream-open reply, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
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
    phase: NamespaceOperationPhase,
    transitions: Option<TransitionEmitter>,
    operation: ActorAddress,
    proxy: ActorAddress,
    parent: ActorAddress,
    path: DataPath,
    incarnation: StreamIncarnation,
}

impl NamespaceStreamCloseActor {
    fn enter_phase(
        &mut self,
        ctx: &Ctx<'_>,
        next: NamespaceOperationPhase,
        reason: Reason,
    ) -> bool {
        if self.phase.outcome().is_some() {
            return false;
        }
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::NamespaceStreamCloseActor,
                protocol: Protocol::NamespaceOperation,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: Some(self.incarnation.authority_epoch),
                incarnation: Some(self.incarnation.revision),
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome: next.outcome(),
            });
        }
        true
    }

    fn finish(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let (outcome, reason) = if result.is_ok() {
            (Outcome::Success, Reason::Completed)
        } else {
            (Outcome::Failed, Reason::Failed)
        };
        self.enter_phase(ctx, NamespaceOperationPhase::Finished(outcome), reason);
        let _ = ctx.send(self.parent, HostStreamIn::NamespaceClosed(result));
        ctx.stop_self();
    }
}

impl ActorInterface for NamespaceStreamCloseActor {
    type Incoming = DataDirectoryOut;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, NamespaceOperationPhase::AwaitingReply, Reason::Started);
        ctx.watch(self.proxy);
        if ctx
            .send(
                self.proxy,
                NamespaceClientIn::Request {
                    request: NamespaceRequest::CloseStream {
                        path: self.path.clone(),
                        incarnation: self.incarnation,
                    },
                    reply_to: ctx.self_addr(),
                },
            )
            .is_err()
        {
            self.finish(ctx, Err(DataPlaneError::SessionNotRunning));
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: DataDirectoryOut) {
        if self.phase != NamespaceOperationPhase::AwaitingReply {
            return;
        }
        let result = match message {
            DataDirectoryOut::StreamClosed { result, .. } => result.map_err(namespace_error),
            other => Err(DataPlaneError::SessionFailed(format!(
                "expected stream-close reply, received {other:?}"
            ))),
        };
        self.finish(ctx, result);
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if exited.addr == self.proxy
            && self.enter_phase(
                ctx,
                NamespaceOperationPhase::Finished(Outcome::PeerLost),
                Reason::PeerTerminated,
            )
        {
            let _ = ctx.send(
                self.parent,
                HostStreamIn::NamespaceClosed(Err(DataPlaneError::SessionNotRunning)),
            );
            ctx.stop_self();
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if self.enter_phase(
            ctx,
            NamespaceOperationPhase::Finished(Outcome::Cancelled),
            Reason::ParentStopped,
        ) {
            let _ = ctx.send(
                self.parent,
                HostStreamIn::NamespaceClosed(Err(DataPlaneError::SessionNotRunning)),
            );
            let _ = ctx.send(
                self.proxy,
                NamespaceClientIn::Cancel {
                    reply_to: ctx.self_addr(),
                },
            );
        }
    }
}

struct RuntimeStreamNotifier {
    runtime: Runtime,
    target: ActorAddress,
    incarnation: StreamIncarnation,
}

impl StreamTransportNotifier for RuntimeStreamNotifier {
    fn notify(&self, event: StreamTransportEvent) {
        let _ = self.runtime.send_to(
            self.target,
            HostStreamIn::Transport {
                incarnation: self.incarnation,
                event,
            },
        );
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StreamPhase {
    Resolving,
    AwaitingPeer,
    AwaitingTransport,
    Opening,
    Open,
    Closing,
    Releasing,
    Closed,
}

impl StreamPhase {
    fn terminal(self) -> bool {
        matches!(self, Self::Closing | Self::Releasing | Self::Closed)
    }

    fn telemetry(self) -> Phase {
        match self {
            Self::Resolving => Phase::Resolving,
            Self::AwaitingPeer => Phase::AwaitingPeer,
            Self::AwaitingTransport => Phase::AwaitingTransport,
            Self::Opening => Phase::Opening,
            Self::Open => Phase::Open,
            Self::Closing => Phase::Closing,
            Self::Releasing => Phase::Releasing,
            Self::Closed => Phase::Closed,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamTransportState {
    Absent,
    Installed,
    Ready,
    Quiescing,
    Quiesced,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamPeerState {
    Negotiating,
    Acknowledged,
    AwaitingTerminationAck,
    Terminated,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamRouteState {
    Unknown,
    Routable,
    Lost,
    Unavailable,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StreamOpenCompletion {
    Owned,
    Forwarding,
    Delivered,
    Failed,
}

enum StreamRelease {
    NotStarted,
    Pending,
    Complete(Result<(), DataPlaneError>),
}

struct HostStreamBindingActor {
    transitions: Option<TransitionEmitter>,
    session_generation: u64,
    phase: StreamPhase,
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
    ring_probe: Option<RingProbe>,
    ring_allocation_pending: bool,
    matched: Option<StreamMatch>,
    pending_displacements: HashMap<StreamIncarnation, HashSet<ActorAddress>>,
    fenced_displacements: HashSet<StreamIncarnation>,
    pending_peer_terminations: HashMap<StreamIncarnation, (DataPlaneError, Option<ActorAddress>)>,
    peer_descriptor: Option<StreamPeerDescriptor>,
    peer_state: StreamPeerState,
    transport_state: StreamTransportState,
    open_completion: StreamOpenCompletion,
    route_state: StreamRouteState,
    terminal: Option<DataPlaneError>,
    data_waiters: Vec<ActorAddress>,
    capacity_waiters: Vec<ActorAddress>,
    progress_waiters: Vec<ActorAddress>,
    close_waiters: Vec<ActorAddress>,
    release: StreamRelease,
    namespace_close_result: Option<Result<(), DataPlaneError>>,
    namespace_open: Option<ActorAddress>,
    namespace_close: Option<ActorAddress>,
    route_watch: Option<HostRouteWatch>,
    offer_retry: Option<ActorTimer>,
    termination_retry: Option<ActorTimer>,
    termination_route_retries: u8,
    route_loss_timer: Option<ActorTimer>,
    route_loss_generation: u64,
    peer_was_local: bool,
    pending_peer_offers: HashMap<StreamIncarnation, StreamPeerDescriptor>,
    detach_pending: bool,
}

impl HostStreamBindingActor {
    fn operation_id(address: ActorAddress) -> OperationId {
        let mut bytes = [0_u8; 16];
        bytes.copy_from_slice(&address.0[..16]);
        OperationId::from_u128(u128::from_le_bytes(bytes))
    }

    fn ring_peer_termination(error: &DataPlaneError) -> PeerTermination {
        if matches!(error, DataPlaneError::PathReplaced(_)) {
            PeerTermination::PathReplaced
        } else {
            PeerTermination::Closed
        }
    }

    fn peer_is_routable(&self, peer: ActorAddress) -> bool {
        (!self.peer_was_local || self.runtime.is_local_actor(peer))
            && self
                .route_registrar
                .as_ref()
                .is_none_or(|routes| routes.is_routable(peer))
    }

    fn fail_open(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        self.begin_terminal(ctx, error, true, true);
    }

    fn displace(&mut self, ctx: &Ctx<'_>) {
        self.begin_terminal(
            ctx,
            DataPlaneError::PathReplaced(self.path.clone()),
            false,
            true,
        );
    }

    fn acknowledge_displacement(
        &self,
        ctx: &Ctx<'_>,
        reply_to: ActorAddress,
        incarnation: StreamIncarnation,
    ) {
        let _ = ctx.send(
            reply_to,
            DataDirectoryIn::StreamDisplaced {
                endpoint: ctx.self_addr(),
                incarnation,
            },
        );
    }

    fn fence_displacement(
        &mut self,
        ctx: &Ctx<'_>,
        incarnation: StreamIncarnation,
        reply_to: Option<ActorAddress>,
    ) {
        self.displace(ctx);
        self.fenced_displacements.insert(incarnation);
        let mut replies = self
            .pending_displacements
            .remove(&incarnation)
            .unwrap_or_default();
        if let Some(reply_to) = reply_to {
            replies.insert(reply_to);
        }
        for reply_to in replies {
            self.acknowledge_displacement(ctx, reply_to, incarnation);
        }
    }

    fn apply_peer_termination(
        &mut self,
        ctx: &Ctx<'_>,
        incarnation: StreamIncarnation,
        error: DataPlaneError,
        reply_to: Option<ActorAddress>,
    ) {
        self.begin_terminal(ctx, error, false, true);
        if let Some(reply_to) = reply_to {
            let _ = ctx.send(reply_to, HostStreamIn::PeerTerminationAck { incarnation });
        }
    }

    fn send_peer_offer(&mut self, ctx: &Ctx<'_>) {
        let Some(matched) = self.matched.as_ref() else {
            return;
        };
        if self.role != StreamRole::Sink
            || !matched.sink_descriptor.is_empty()
            || self.peer_state == StreamPeerState::Acknowledged
            || self.phase.terminal()
            || self.transport_state == StreamTransportState::Absent
        {
            return;
        }
        let source = matched.source;
        let incarnation = matched.incarnation;
        self.enter_phase(ctx, self.phase, Reason::PeerOffered);
        if self.peer_is_routable(source) {
            let _ = ctx.send(
                source,
                HostStreamIn::PeerOffer {
                    incarnation,
                    descriptor: self.local_descriptor.clone(),
                },
            );
        }
        if self.offer_retry.is_none() {
            self.offer_retry = Some(self.engine.send_after(
                STREAM_PEER_OFFER_RETRY,
                self.sender.clone(),
                ctx.self_addr(),
                HostStreamIn::PeerOfferRetry { incarnation },
            ));
        }
    }

    fn send_peer_termination(&mut self, ctx: &Ctx<'_>) {
        if self.peer_state != StreamPeerState::AwaitingTerminationAck {
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
        let routable = self.peer_is_routable(peer);
        let sent = routable
            && ctx
                .send(
                    peer,
                    HostStreamIn::PeerTerminated {
                        incarnation,
                        error,
                        reply_to: Some(ctx.self_addr()),
                    },
                )
                .is_ok();
        let local_peer_is_gone = self.peer_was_local && !self.runtime.is_local_actor(peer);
        if local_peer_is_gone || !sent {
            self.route_state = StreamRouteState::Unavailable;
        }
        if local_peer_is_gone
            || self.termination_route_retries >= STREAM_PEER_TERMINATION_ROUTE_RETRY_LIMIT
        {
            // A peer whose exact local actor is gone, or whose remote route
            // never converges within the bounded grace, cannot acknowledge.
            // Namespace closure and transport termination still fence the
            // incarnation; retaining this actor would only leak the session.
            self.peer_state = StreamPeerState::Terminated;
            if matches!(
                self.transport_state,
                StreamTransportState::Absent | StreamTransportState::Quiesced
            ) {
                self.release_ring(ctx);
            }
            return;
        }
        self.termination_route_retries += 1;
        if self.termination_retry.is_none() {
            self.termination_retry = Some(self.engine.send_after(
                STREAM_PEER_OFFER_RETRY,
                self.sender.clone(),
                ctx.self_addr(),
                HostStreamIn::PeerTerminationRetry { incarnation },
            ));
        }
    }
    fn peer_route(&self) -> Option<(StreamIncarnation, ActorAddress)> {
        self.matched.as_ref().map(|matched| {
            let peer = match self.role {
                StreamRole::Source => matched.sink,
                StreamRole::Sink => matched.source,
            };
            (matched.incarnation, peer)
        })
    }

    fn peer_loss_blocks_progress(&self) -> bool {
        if matches!(
            self.open_completion,
            StreamOpenCompletion::Owned | StreamOpenCompletion::Forwarding
        ) {
            return true;
        }
        match self.role {
            StreamRole::Source => {
                !self.capacity_waiters.is_empty() || !self.progress_waiters.is_empty()
            }
            StreamRole::Sink => !self.data_waiters.is_empty(),
        }
    }

    fn cancel_peer_route_loss(&mut self) {
        if let Some(timer) = self.route_loss_timer.take() {
            self.route_loss_generation = self.route_loss_generation.wrapping_add(1);
            timer.cancel();
        }
    }

    fn update_peer_route_loss(&mut self, ctx: &Ctx<'_>) {
        let Some((incarnation, peer)) = self.peer_route() else {
            self.cancel_peer_route_loss();
            return;
        };
        let routable = self.peer_is_routable(peer);
        let next_route = if routable {
            StreamRouteState::Routable
        } else {
            StreamRouteState::Lost
        };
        if self.route_state != next_route {
            self.route_state = next_route;
            self.enter_phase(
                ctx,
                self.phase,
                if routable {
                    Reason::RouteRestored
                } else {
                    Reason::RouteLost
                },
            );
        }
        if routable || self.phase.terminal() || !self.peer_loss_blocks_progress() {
            self.cancel_peer_route_loss();
            return;
        }
        if self.route_loss_timer.is_none() {
            self.route_loss_generation = self.route_loss_generation.wrapping_add(1);
            let generation = self.route_loss_generation;
            self.route_loss_timer = Some(self.engine.send_after(
                STREAM_PEER_ROUTE_LOSS_GRACE,
                self.sender.clone(),
                ctx.self_addr(),
                HostStreamIn::PeerRouteLossCheck {
                    incarnation,
                    generation,
                },
            ));
        }
    }

    fn try_install_transport(&mut self, ctx: &Ctx<'_>) {
        if self.transport_state != StreamTransportState::Absent || self.phase.terminal() {
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
        let ring_probe = Some(endpoint.probe());
        let notifier: Arc<dyn StreamTransportNotifier> = Arc::new(RuntimeStreamNotifier {
            runtime: self.runtime.clone(),
            target: ctx.self_addr(),
            incarnation: matched.incarnation,
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
                self.transport_state = StreamTransportState::Installed;
                self.enter_phase(
                    ctx,
                    StreamPhase::AwaitingTransport,
                    Reason::TransportInstalled,
                );
                self.ring_probe = ring_probe;
                if self.role == StreamRole::Sink && matched.sink_descriptor.is_empty() {
                    self.send_peer_offer(ctx);
                }
            }
            Err(reason) => self.fail_open(ctx, DataPlaneError::StreamFault(reason)),
        }
    }

    fn open_if_ready(&mut self, ctx: &Ctx<'_>) {
        if self.open_completion != StreamOpenCompletion::Owned
            || self.transport_state != StreamTransportState::Ready
            || self.phase.terminal()
        {
            return;
        }
        let (Some(ring), Some(matched)) = (self.ring, self.matched.as_ref()) else {
            return;
        };
        let incarnation = matched.incarnation;
        self.open_completion = StreamOpenCompletion::Forwarding;
        self.enter_phase(ctx, StreamPhase::Opening, Reason::Opened);
        if ctx
            .send(
                self.child_session,
                ChildSessionIn::StreamOpened {
                    operation: self.operation,
                    host_binding: ctx.self_addr(),
                    incarnation,
                    ring,
                    role: match self.role {
                        StreamRole::Source => Role::Producer,
                        StreamRole::Sink => Role::Consumer,
                    },
                },
            )
            .is_err()
        {
            self.fail_open(ctx, DataPlaneError::SessionNotRunning);
        }
    }

    fn open_forwarded(&mut self, ctx: &Ctx<'_>, incarnation: StreamIncarnation) {
        if self.matches_incarnation(incarnation)
            && self.open_completion == StreamOpenCompletion::Forwarding
            && !self.phase.terminal()
        {
            self.open_completion = StreamOpenCompletion::Delivered;
            self.enter_phase(ctx, StreamPhase::Open, Reason::Completed);
            self.update_peer_route_loss(ctx);
        }
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

    fn enter_phase(&mut self, ctx: &Ctx<'_>, next: StreamPhase, reason: Reason) {
        let previous = self.phase;
        self.phase = next;
        if let Some(transitions) = &self.transitions {
            let outcome = next.terminal().then_some(match self.terminal.as_ref() {
                Some(DataPlaneError::OperationCancelled) => Outcome::Cancelled,
                Some(DataPlaneError::PeerLost) => Outcome::PeerLost,
                Some(DataPlaneError::PathReplaced(_)) => Outcome::Replaced,
                Some(DataPlaneError::StreamClosed) | None => Outcome::Success,
                Some(_) => Outcome::Failed,
            });
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::HostStreamBindingActor,
                protocol: Protocol::HostStream,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: Some(
                    self.matched
                        .as_ref()
                        .map_or(self.session_generation, |matched| {
                            matched.incarnation.authority_epoch
                        }),
                ),
                incarnation: self
                    .matched
                    .as_ref()
                    .map(|matched| matched.incarnation.revision),
                previous: previous.telemetry(),
                next: next.telemetry(),
                reason,
                outcome,
            });
        }
    }

    fn matches_incarnation(&self, incarnation: StreamIncarnation) -> bool {
        self.matched
            .as_ref()
            .is_some_and(|matched| matched.incarnation == incarnation)
    }

    fn begin_terminal(
        &mut self,
        ctx: &Ctx<'_>,
        error: DataPlaneError,
        notify_peer: bool,
        terminate_transport: bool,
    ) {
        if self.phase.terminal() {
            return;
        }
        let opening = matches!(
            self.open_completion,
            StreamOpenCompletion::Owned | StreamOpenCompletion::Forwarding
        );
        if opening {
            self.open_completion = StreamOpenCompletion::Failed;
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::OperationFailed {
                    operation: self.operation,
                    error: error.clone(),
                },
            );
        }
        let reason = match &error {
            DataPlaneError::OperationCancelled => Reason::Cancelled,
            DataPlaneError::PeerLost => Reason::PeerTerminated,
            DataPlaneError::PathReplaced(_) => Reason::Displaced,
            DataPlaneError::StreamClosed => Reason::CloseRequested,
            _ => Reason::Failed,
        };
        self.terminal = Some(error.clone());
        self.enter_phase(ctx, StreamPhase::Closing, reason);
        if let Some(namespace_open) = self.namespace_open {
            let _ = ctx.stop_actor(namespace_open);
        }
        if let Some(ring) = self.ring {
            let _ = byte_ring::mark_peer_terminated_mapped(
                &self.arena,
                ring,
                Self::ring_peer_termination(&error),
            );
        }
        if let Some(timer) = self.offer_retry.take() {
            timer.cancel();
        }
        self.cancel_peer_route_loss();
        self.pending_peer_offers.clear();
        self.pending_peer_terminations.clear();
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
        Self::wake_waiters(
            ctx,
            self.child_session,
            &mut self.progress_waiters,
            Err(error),
        );
        self.start_namespace_close(ctx);
        if let Some(incarnation) = self.matched.as_ref().map(|matched| matched.incarnation) {
            if !matches!(
                self.transport_state,
                StreamTransportState::Absent | StreamTransportState::Quiesced
            ) {
                self.transport_state = StreamTransportState::Quiescing;
                if terminate_transport || opening {
                    self.transport.terminate(incarnation);
                }
            }
            if notify_peer || (opening && !terminate_transport) {
                self.peer_state = StreamPeerState::AwaitingTerminationAck;
                self.send_peer_termination(ctx);
            }
        }
        self.release_ring(ctx);
    }

    fn start_namespace_close(&mut self, ctx: &Ctx<'_>) {
        if self.namespace_close.is_some() || self.namespace_close_result.is_some() {
            return;
        }
        let Some(matched) = self.matched.as_ref() else {
            return;
        };
        match ctx.spawn(NamespaceStreamCloseActor {
            proxy: self.namespace.proxy(),
            parent: ctx.self_addr(),
            path: self.path.clone(),
            incarnation: matched.incarnation,
            phase: NamespaceOperationPhase::Created,
            transitions: self.transitions.clone(),
            operation: self.operation,
        }) {
            Ok(namespace_close) => {
                self.namespace_close = Some(namespace_close);
                ctx.watch(namespace_close);
            }
            Err(error) => {
                self.namespace_close_result =
                    Some(Err(DataPlaneError::SessionFailed(error.to_string())))
            }
        }
    }

    fn namespace_closed(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.namespace_close.take().is_none() || self.namespace_close_result.is_some() {
            return;
        }
        self.namespace_close_result = Some(result);
        self.finish_release(ctx);
    }

    fn complete_release(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if !matches!(self.release, StreamRelease::Pending) {
            return;
        }
        self.ring = None;
        self.ring_probe = None;
        self.release = StreamRelease::Complete(result);
        self.finish_release(ctx);
    }

    fn finish_release(&mut self, ctx: &Ctx<'_>) {
        if self.phase == StreamPhase::Closed
            || self.namespace_open.is_some()
            || self.namespace_close.is_some()
        {
            return;
        }
        let StreamRelease::Complete(result) = &self.release else {
            return;
        };
        let result = result
            .clone()
            .and_then(|()| self.namespace_close_result.clone().unwrap_or(Ok(())));
        self.release = StreamRelease::Complete(result.clone());
        self.pending_displacements.clear();
        self.pending_peer_terminations.clear();
        self.route_watch = None;
        self.enter_phase(ctx, StreamPhase::Closed, Reason::Released);
        Self::wake_waiters(ctx, self.child_session, &mut self.close_waiters, result);
        if ctx
            .send(
                self.host_session,
                HostSessionIn::BindingDone {
                    binding: ctx.self_addr(),
                },
            )
            .is_err()
        {
            ctx.stop_self();
        }
    }

    fn release_ring(&mut self, ctx: &Ctx<'_>) {
        if !self.phase.terminal()
            || !matches!(self.release, StreamRelease::NotStarted)
            || self.ring_allocation_pending
            || self.peer_state == StreamPeerState::AwaitingTerminationAck
            || !matches!(
                self.transport_state,
                StreamTransportState::Absent | StreamTransportState::Quiesced
            )
        {
            return;
        }
        self.release = StreamRelease::Pending;
        self.enter_phase(ctx, StreamPhase::Releasing, Reason::Released);
        if let Some(ring) = self.ring {
            if ctx
                .send(
                    self.allocator,
                    ArenaAllocatorIn::ReleaseStream {
                        binding: ctx.self_addr(),
                        ring,
                    },
                )
                .is_err()
            {
                self.complete_release(ctx, Err(DataPlaneError::SessionNotRunning));
            }
        } else {
            self.complete_release(ctx, Ok(()));
        }
    }

    fn namespace_matched(&mut self, ctx: &Ctx<'_>, result: Result<StreamMatch, NamespaceError>) {
        self.namespace_open = None;
        if self.matched.is_some() {
            return;
        }
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
                let incarnation = matched.incarnation;
                if self.phase.terminal() {
                    self.matched = Some(matched);
                    self.enter_phase(ctx, self.phase, Reason::NamespaceResolved);
                    self.start_namespace_close(ctx);
                    self.finish_release(ctx);
                    return;
                }
                let pending_peer_termination = self.pending_peer_terminations.remove(&incarnation);
                if self.pending_displacements.contains_key(&incarnation) {
                    self.matched = Some(matched);
                    self.enter_phase(ctx, self.phase, Reason::NamespaceResolved);
                    self.fence_displacement(ctx, incarnation, None);
                    if let Some((error, reply_to)) = pending_peer_termination {
                        self.apply_peer_termination(ctx, incarnation, error, reply_to);
                    }
                    return;
                }
                let peer = match self.role {
                    StreamRole::Source => matched.sink,
                    StreamRole::Sink => matched.source,
                };
                self.peer_was_local = self.runtime.is_local_actor(peer);
                if self.peer_was_local {
                    ctx.watch(peer);
                }
                if self.role == StreamRole::Source && !matched.sink_descriptor.is_empty() {
                    self.peer_descriptor =
                        Some(StreamPeerDescriptor(matched.sink_descriptor.clone()));
                }
                self.matched = Some(matched);
                self.enter_phase(ctx, StreamPhase::AwaitingPeer, Reason::NamespaceResolved);
                self.enter_phase(ctx, StreamPhase::AwaitingPeer, Reason::PeerMatched);
                if let Some((error, reply_to)) = pending_peer_termination {
                    self.apply_peer_termination(ctx, incarnation, error, reply_to);
                    return;
                }
                if let Some(routes) = &self.route_registrar {
                    let matched = self.matched.as_ref().expect("installed match");
                    let peer = match self.role {
                        StreamRole::Source => matched.sink,
                        StreamRole::Sink => matched.source,
                    };
                    let incarnation = matched.incarnation;
                    let sender = self.sender.clone();
                    let target = ctx.self_addr();
                    self.route_watch = routes.watch_route(
                        peer,
                        Arc::new(move || {
                            let _ =
                                sender.send_to(target, HostStreamIn::RouteChanged { incarnation });
                        }),
                    );
                }
                if let Some(descriptor) = self.pending_peer_offers.remove(&incarnation)
                    && let Some(matched) = self.matched.as_ref()
                {
                    self.peer_descriptor = Some(descriptor);
                    let _ = ctx.send(matched.sink, HostStreamIn::PeerOfferAck { incarnation });
                }
                self.pending_peer_offers.clear();
                self.pending_peer_terminations.clear();
                self.pending_displacements.clear();
                self.try_install_transport(ctx);
                self.update_peer_route_loss(ctx);
            }
            Err(error) => {
                if self.phase.terminal() {
                    self.finish_release(ctx);
                } else {
                    self.fail_open(ctx, namespace_error(error));
                }
            }
        }
    }

    fn allocated(&mut self, ctx: &Ctx<'_>, result: Result<RingHandle, DataPlaneError>) {
        if !self.ring_allocation_pending {
            return;
        }
        self.ring_allocation_pending = false;
        match result {
            Ok(ring) => {
                self.ring = Some(ring);
                if let Some(error) = self.terminal.as_ref() {
                    let _ = byte_ring::mark_peer_terminated_mapped(
                        &self.arena,
                        ring,
                        Self::ring_peer_termination(error),
                    );
                }
            }
            Err(error) => self.fail_open(ctx, error),
        }
        if self.phase.terminal() {
            self.release_ring(ctx);
        } else {
            self.try_install_transport(ctx);
        }
    }

    fn peer_offer(
        &mut self,
        ctx: &Ctx<'_>,
        incarnation: StreamIncarnation,
        descriptor: StreamPeerDescriptor,
    ) {
        if self.role != StreamRole::Source || self.phase.terminal() {
            return;
        }
        if self.matched.is_none() {
            // Only the namespace match authenticates an offer's incarnation.
            self.pending_peer_offers
                .entry(incarnation)
                .or_insert(descriptor);
            return;
        }
        let sink = self
            .matched
            .as_ref()
            .filter(|matched| matched.incarnation == incarnation)
            .map(|matched| matched.sink);
        if let Some(sink) = sink {
            if self.peer_descriptor.is_none() {
                self.peer_descriptor = Some(descriptor);
            }
            self.enter_phase(ctx, self.phase, Reason::PeerOffered);
            let _ = ctx.send(sink, HostStreamIn::PeerOfferAck { incarnation });
            self.try_install_transport(ctx);
        }
    }

    fn peer_offer_ack(&mut self, ctx: &Ctx<'_>, incarnation: StreamIncarnation) {
        if self.phase.terminal() {
            return;
        }
        if self.role == StreamRole::Sink
            && self
                .matched
                .as_ref()
                .is_some_and(|matched| matched.incarnation == incarnation)
        {
            self.peer_state = StreamPeerState::Acknowledged;
            self.enter_phase(ctx, self.phase, Reason::PeerAcknowledged);
            if let Some(timer) = self.offer_retry.take() {
                timer.cancel();
            }
        }
    }

    fn peer_offer_retry(&mut self, ctx: &Ctx<'_>, incarnation: StreamIncarnation) {
        if self
            .matched
            .as_ref()
            .is_some_and(|matched| matched.incarnation == incarnation)
        {
            self.offer_retry = None;
            self.send_peer_offer(ctx);
        }
    }

    fn route_changed(&mut self, ctx: &Ctx<'_>, incarnation: StreamIncarnation) {
        if self
            .matched
            .as_ref()
            .is_some_and(|matched| matched.incarnation == incarnation)
        {
            self.update_peer_route_loss(ctx);
            self.send_peer_offer(ctx);
            self.send_peer_termination(ctx);
            if self.role == StreamRole::Source
                && self.peer_descriptor.is_some()
                && !self.phase.terminal()
                && let Some(matched) = &self.matched
                && matched.sink_descriptor.is_empty()
            {
                let _ = ctx.send(matched.sink, HostStreamIn::PeerOfferAck { incarnation });
            }
        }
    }

    fn peer_route_loss_check(
        &mut self,
        ctx: &Ctx<'_>,
        incarnation: StreamIncarnation,
        generation: u64,
    ) {
        if generation != self.route_loss_generation {
            return;
        }
        if !self.matches_incarnation(incarnation) {
            return;
        }
        self.route_loss_timer = None;
        let Some((matched_incarnation, peer)) = self.peer_route() else {
            return;
        };
        let routable = self.peer_is_routable(peer);
        if incarnation == matched_incarnation
            && !routable
            && !self.phase.terminal()
            && self.peer_loss_blocks_progress()
        {
            self.apply_peer_termination(ctx, incarnation, DataPlaneError::PeerLost, None);
        }
    }

    fn transport_ready(&mut self, ctx: &Ctx<'_>) {
        if self.transport_state != StreamTransportState::Installed || self.phase.terminal() {
            return;
        }
        self.transport_state = StreamTransportState::Ready;
        self.enter_phase(ctx, StreamPhase::AwaitingTransport, Reason::TransportReady);
        self.open_if_ready(ctx);
    }

    fn transport_data_available(&mut self, ctx: &Ctx<'_>) {
        if self.phase.terminal() {
            return;
        }
        self.cancel_peer_route_loss();
        Self::wake_waiters(ctx, self.child_session, &mut self.data_waiters, Ok(()));
    }

    fn transport_capacity_available(&mut self, ctx: &Ctx<'_>) {
        if self.phase.terminal() {
            return;
        }
        self.cancel_peer_route_loss();
        Self::wake_waiters(ctx, self.child_session, &mut self.capacity_waiters, Ok(()));
        Self::wake_waiters(ctx, self.child_session, &mut self.progress_waiters, Ok(()));
    }

    fn transport_fault(&mut self, ctx: &Ctx<'_>, reason: String) {
        self.fail_open(ctx, DataPlaneError::StreamFault(reason));
    }

    fn transport_quiesced(&mut self, ctx: &Ctx<'_>) {
        if matches!(
            self.transport_state,
            StreamTransportState::Absent | StreamTransportState::Quiesced
        ) {
            return;
        }
        self.transport_state = StreamTransportState::Quiesced;
        if self.open_completion == StreamOpenCompletion::Owned && !self.phase.terminal() {
            self.fail_open(ctx, DataPlaneError::PeerLost);
        }
        self.release_ring(ctx);
    }

    fn data_available(&mut self) {
        if self.phase.terminal() {
            return;
        }
        if let Some(matched) = &self.matched {
            self.transport.source_progress(matched.incarnation);
        }
    }

    fn capacity_available(&mut self) {
        if self.phase.terminal() {
            return;
        }
        if let Some(matched) = &self.matched {
            self.transport.sink_progress(matched.incarnation);
        }
    }

    fn wait_data(&mut self, ctx: &Ctx<'_>, reply_to: ActorAddress) {
        let result = self
            .terminal
            .as_ref()
            .map(|error| Err(error.clone()))
            .or_else(|| {
                self.ring_probe
                    .as_ref()
                    .filter(|probe| probe.has_data())
                    .map(|_| Ok(()))
            });
        if let Some(result) = result {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::StreamWake { reply_to, result },
            );
        } else {
            self.data_waiters.push(reply_to);
            self.update_peer_route_loss(ctx);
        }
    }

    fn wait_capacity(&mut self, ctx: &Ctx<'_>, reply_to: ActorAddress) {
        let result = self
            .terminal
            .as_ref()
            .map(|error| Err(error.clone()))
            .or_else(|| {
                self.ring_probe
                    .as_ref()
                    .filter(|probe| probe.has_capacity())
                    .map(|_| Ok(()))
            });
        if let Some(result) = result {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::StreamWake { reply_to, result },
            );
        } else {
            self.capacity_waiters.push(reply_to);
            self.update_peer_route_loss(ctx);
        }
    }

    fn wait_progress(&mut self, ctx: &Ctx<'_>, reply_to: ActorAddress, target: u64) {
        let result = self
            .terminal
            .as_ref()
            .map(|error| Err(error.clone()))
            .or_else(|| {
                self.ring_probe.as_ref().and_then(|probe| {
                    probe
                        .positions()
                        .map(|(_, consumed)| (consumed >= target).then_some(Ok(())))
                        .unwrap_or_else(|error| {
                            Some(Err(DataPlaneError::StreamFault(format!(
                                "observe stream flush progress: {error:?}"
                            ))))
                        })
                })
            });
        if let Some(result) = result {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::StreamWake { reply_to, result },
            );
        } else {
            self.progress_waiters.push(reply_to);
            self.update_peer_route_loss(ctx);
        }
    }

    fn close(&mut self, ctx: &Ctx<'_>, clean: bool, reply_to: Option<ActorAddress>) {
        if let Some(reply_to) = reply_to {
            if self.phase == StreamPhase::Closed {
                if let StreamRelease::Complete(result) = &self.release {
                    let _ = ctx.send(
                        self.child_session,
                        ChildSessionIn::StreamWake {
                            reply_to,
                            result: result.clone(),
                        },
                    );
                }
                return;
            }
            self.close_waiters.push(reply_to);
        }
        if clean {
            self.begin_terminal(ctx, DataPlaneError::StreamClosed, false, false);
        } else {
            self.fail_open(ctx, DataPlaneError::OperationCancelled);
        }
    }

    fn peer_terminated(
        &mut self,
        ctx: &Ctx<'_>,
        incarnation: StreamIncarnation,
        error: DataPlaneError,
        reply_to: Option<ActorAddress>,
    ) {
        let matched_incarnation = self.matched.as_ref().map(|matched| matched.incarnation);
        if matched_incarnation == Some(incarnation) {
            self.apply_peer_termination(ctx, incarnation, error, reply_to);
        } else if matched_incarnation.is_none() && !self.phase.terminal() {
            // Peer termination can overtake this binding's namespace
            // reply. Retain it by incarnation so only the authenticated
            // match applies the terminal state and receives an ACK.
            self.pending_peer_terminations
                .insert(incarnation, (error, reply_to));
        }
    }

    fn displaced(
        &mut self,
        ctx: &Ctx<'_>,
        incarnation: StreamIncarnation,
        reply_to: Option<ActorAddress>,
    ) {
        let matched_incarnation = self.matched.as_ref().map(|matched| matched.incarnation);
        if matched_incarnation == Some(incarnation) {
            self.fence_displacement(ctx, incarnation, reply_to);
        } else if self.fenced_displacements.contains(&incarnation) {
            if let Some(reply_to) = reply_to {
                self.acknowledge_displacement(ctx, reply_to, incarnation);
            }
        } else if matched_incarnation.is_none() && !self.phase.terminal() {
            // The directory notice can overtake the namespace reply.
            // Retain its acknowledger until the exact incarnation is
            // matched and synchronously fenced.
            let replies = self.pending_displacements.entry(incarnation).or_default();
            if let Some(reply_to) = reply_to {
                replies.insert(reply_to);
            }
        }
    }

    fn peer_termination_ack(&mut self, ctx: &Ctx<'_>, incarnation: StreamIncarnation) {
        if self
            .matched
            .as_ref()
            .is_some_and(|matched| matched.incarnation == incarnation)
        {
            self.peer_state = StreamPeerState::Terminated;
            if let Some(timer) = self.termination_retry.take() {
                timer.cancel();
            }
            if self.phase.terminal()
                && (matches!(
                    self.transport_state,
                    StreamTransportState::Absent | StreamTransportState::Quiesced
                ))
            {
                self.release_ring(ctx);
            }
        }
    }

    fn peer_termination_retry(&mut self, ctx: &Ctx<'_>, incarnation: StreamIncarnation) {
        if self.peer_state == StreamPeerState::AwaitingTerminationAck
            && self
                .matched
                .as_ref()
                .is_some_and(|matched| matched.incarnation == incarnation)
        {
            self.termination_retry = None;
            self.send_peer_termination(ctx);
        }
    }

    fn release_complete(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        self.complete_release(ctx, result);
    }

    fn session_detached(&mut self, ctx: &Ctx<'_>) {
        if self.phase != StreamPhase::Closed {
            return;
        }
        if self.detach_pending {
            ctx.stop_self();
        } else {
            // BindingDone can race controls already forwarded from the
            // host session. Requeue the detach marker behind the current
            // mailbox so those controls receive the terminal result.
            self.detach_pending = true;
            let _ = ctx.send(ctx.self_addr(), HostStreamIn::SessionDetached);
        }
    }

    fn transport_event(
        &mut self,
        ctx: &Ctx<'_>,
        incarnation: StreamIncarnation,
        event: StreamTransportEvent,
    ) {
        if !self.matches_incarnation(incarnation) {
            return;
        }
        match event {
            StreamTransportEvent::Ready => self.transport_ready(ctx),
            StreamTransportEvent::DataAvailable => self.transport_data_available(ctx),
            StreamTransportEvent::CapacityAvailable => self.transport_capacity_available(ctx),
            StreamTransportEvent::Fault(reason) => self.transport_fault(ctx, reason),
            StreamTransportEvent::Quiesced => self.transport_quiesced(ctx),
        }
    }
}

impl ActorInterface for HostStreamBindingActor {
    type Incoming = HostStreamIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, StreamPhase::Resolving, Reason::Started);
        ctx.watch(self.host_session);
        ctx.watch(self.allocator);
        if self.runtime.is_local_actor(self.child_session) {
            ctx.watch(self.child_session);
        }
        if let Err(error) = ctx.send(
            self.allocator,
            ArenaAllocatorIn::AllocateStream {
                binding: ctx.self_addr(),
                capacity: STREAM_RING_CAPACITY,
            },
        ) {
            self.ring_allocation_pending = false;
            self.fail_open(ctx, DataPlaneError::SessionFailed(error.to_string()));
            return;
        }
        match ctx.spawn(NamespaceStreamOpenActor {
            proxy: self.namespace.proxy(),
            parent: ctx.self_addr(),
            path: self.path.clone(),
            role: self.role,
            descriptor: self.local_descriptor.0.clone(),
            replace: self.replace,
            ensure: self.ensure,
            expected_revision: self.expected_revision,
            operation_id: Self::operation_id(self.operation),
            phase: NamespaceOperationPhase::Created,
            transitions: self.transitions.clone(),
            operation: self.operation,
            session_generation: self.session_generation,
            incarnation: None,
        }) {
            Ok(namespace_open) => {
                self.namespace_open = Some(namespace_open);
                ctx.watch(namespace_open);
            }
            Err(error) => {
                self.fail_open(ctx, DataPlaneError::SessionFailed(error.to_string()));
            }
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostStreamIn) {
        match message {
            HostStreamIn::NamespaceMatched(result) => self.namespace_matched(ctx, result),
            HostStreamIn::Allocated(result) => self.allocated(ctx, result),
            HostStreamIn::PeerOffer {
                incarnation,
                descriptor,
            } => self.peer_offer(ctx, incarnation, descriptor),
            HostStreamIn::PeerOfferAck { incarnation } => self.peer_offer_ack(ctx, incarnation),
            HostStreamIn::PeerOfferRetry { incarnation } => self.peer_offer_retry(ctx, incarnation),
            HostStreamIn::RouteChanged { incarnation } => self.route_changed(ctx, incarnation),
            HostStreamIn::PeerRouteLossCheck {
                incarnation,
                generation,
            } => self.peer_route_loss_check(ctx, incarnation, generation),
            HostStreamIn::DataAvailable => self.data_available(),
            HostStreamIn::CapacityAvailable => self.capacity_available(),
            HostStreamIn::WaitData { reply_to } => self.wait_data(ctx, reply_to),
            HostStreamIn::WaitCapacity { reply_to } => self.wait_capacity(ctx, reply_to),
            HostStreamIn::WaitProgress { reply_to, target } => {
                self.wait_progress(ctx, reply_to, target)
            }
            HostStreamIn::Close { clean, reply_to } => self.close(ctx, clean, reply_to),
            HostStreamIn::PeerTerminated {
                incarnation,
                error,
                reply_to,
            } => self.peer_terminated(ctx, incarnation, error, reply_to),
            HostStreamIn::Displaced {
                incarnation,
                reply_to,
            } => self.displaced(ctx, incarnation, reply_to),
            HostStreamIn::PeerTerminationAck { incarnation } => {
                self.peer_termination_ack(ctx, incarnation)
            }
            HostStreamIn::PeerTerminationRetry { incarnation } => {
                self.peer_termination_retry(ctx, incarnation)
            }
            HostStreamIn::ReleaseComplete(result) => self.release_complete(ctx, result),
            HostStreamIn::SessionDetached => self.session_detached(ctx),
            HostStreamIn::NamespaceClosed(result) => self.namespace_closed(ctx, result),
            HostStreamIn::OpenForwarded { incarnation } => self.open_forwarded(ctx, incarnation),
            HostStreamIn::Transport { incarnation, event } => {
                self.transport_event(ctx, incarnation, event)
            }
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if self.namespace_open == Some(exited.addr) {
            self.namespace_open = None;
            if self.phase.terminal() {
                self.finish_release(ctx);
            } else {
                self.fail_open(ctx, DataPlaneError::SessionNotRunning);
            }
        } else if self.namespace_close == Some(exited.addr) {
            self.namespace_closed(ctx, Err(DataPlaneError::SessionNotRunning));
        } else if exited.addr == self.allocator {
            self.ring_allocation_pending = false;
            self.fail_open(ctx, DataPlaneError::SessionNotRunning);
            if matches!(self.release, StreamRelease::Pending) {
                self.complete_release(ctx, Err(DataPlaneError::SessionNotRunning));
            }
        } else if exited.addr == self.child_session || exited.addr == self.host_session {
            self.fail_open(ctx, DataPlaneError::SessionNotRunning);
            if exited.addr == self.host_session && self.phase == StreamPhase::Closed {
                ctx.stop_self();
            }
        } else if let Some((incarnation, peer)) = self.peer_route()
            && peer == exited.addr
        {
            self.route_state = StreamRouteState::Unavailable;
            self.apply_peer_termination(ctx, incarnation, DataPlaneError::PeerLost, None);
            self.peer_state = StreamPeerState::Terminated;
            if let Some(timer) = self.termination_retry.take() {
                timer.cancel();
            }
            self.release_ring(ctx);
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        if !self.phase.terminal() {
            self.begin_terminal(ctx, DataPlaneError::SessionNotRunning, true, true);
        }
        if let Some(namespace_open) = self.namespace_open.take() {
            let _ = ctx.stop_actor(namespace_open);
        }
        if let Some(matched) = &self.matched {
            self.transport.terminate(matched.incarnation);
        }
        // Namespace cleanup owns its request independently until it receives a
        // terminal reply; stopping the binding must not cancel that cleanup.
        if self.phase != StreamPhase::Closed {
            Self::wake_waiters(
                ctx,
                self.child_session,
                &mut self.close_waiters,
                Err(DataPlaneError::SessionNotRunning),
            );
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::BindingDone {
                    binding: ctx.self_addr(),
                },
            );
        }
    }
}

impl Drop for HostStreamBindingActor {
    fn drop(&mut self) {
        if let Some(timer) = self.offer_retry.take() {
            timer.cancel();
        }
        if let Some(timer) = self.termination_retry.take() {
            timer.cancel();
        }
        if let Some(timer) = self.route_loss_timer.take() {
            timer.cancel();
        }
    }
}

#[cfg(all(test, target_os = "linux"))]
mod stream_displacement_tests {
    use super::*;
    use crate::arena::{ArenaConfig, NodeId};
    use crate::bootstrap::{BootstrapSpec, prepare_arena};
    use crate::byte_ring::{Endpoint, RecordKind};
    use crate::namespace::DirectoryRequestId;
    use crate::stream_transport::LocalStreamTransport;
    use futures_lite::future;
    use std::any::Any;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Mutex, Weak};
    use swactor::actor::Message;
    use swactor::runtime::{Inbox, RemoteSink, RuntimeConfig, RuntimeParts};
    use swactor::std::StdExtension;
    use swactor_engine::{Engine, SteppingBackend};

    fn settle(backend: &SteppingBackend) {
        for _ in 0..64 {
            backend.step();
        }
    }

    fn receive<M: Message>(backend: &SteppingBackend, inbox: &Inbox<M>) -> M {
        for _ in 0..256 {
            backend.step();
            if let Some(message) = inbox.try_recv() {
                return message;
            }
        }
        panic!("stream actor did not respond after deterministic scheduling");
    }

    struct TestRoutes {
        routable: AtomicBool,
        changed: Mutex<Option<Weak<dyn Fn() + Send + Sync>>>,
    }

    impl TestRoutes {
        fn new() -> Self {
            Self {
                routable: AtomicBool::new(true),
                changed: Mutex::new(None),
            }
        }

        fn revoke(&self) {
            self.routable.store(false, Ordering::Release);
            let changed = self.changed.lock().expect("route callback lock").clone();
            if let Some(changed) = changed.and_then(|changed| changed.upgrade()) {
                changed();
            }
        }

        fn restore(&self) {
            self.routable.store(true, Ordering::Release);
            let changed = self.changed.lock().expect("route callback lock").clone();
            if let Some(changed) = changed.and_then(|changed| changed.upgrade()) {
                changed();
            }
        }
    }

    impl HostRouteRegistrar for TestRoutes {
        fn register_child(
            &self,
            _child_session: ActorAddress,
            _child_node: [u8; 32],
        ) -> Result<(), String> {
            Ok(())
        }

        fn revoke_child(&self, _child_session: ActorAddress) -> Result<(), String> {
            Ok(())
        }

        fn is_routable(&self, _actor: ActorAddress) -> bool {
            self.routable.load(Ordering::Acquire)
        }

        fn watch_route(
            &self,
            _actor: ActorAddress,
            changed: Arc<dyn Fn() + Send + Sync>,
        ) -> Option<HostRouteWatch> {
            *self.changed.lock().expect("route callback lock") = Some(Arc::downgrade(&changed));
            Some(HostRouteWatch::new(changed))
        }
    }

    struct PassivePeer;

    impl ActorInterface for PassivePeer {
        type Incoming = HostStreamIn;
        type Response = ();

        fn handle(&mut self, _ctx: &Ctx<'_>, _message: HostStreamIn) {}
    }

    struct AcceptingRemoteSink;

    impl RemoteSink for AcceptingRemoteSink {
        fn send(
            &self,
            _addr: ActorAddress,
            _message: Box<dyn Any + Send>,
        ) -> Result<(), swactor::Error> {
            Ok(())
        }
    }

    #[derive(Default)]
    struct RecordingRemoteSink {
        messages: Mutex<Vec<(ActorAddress, HostStreamIn)>>,
    }

    impl RecordingRemoteSink {
        fn receive(&self, backend: &SteppingBackend) -> (ActorAddress, HostStreamIn) {
            for _ in 0..256 {
                backend.step();
                if let Some(message) = self.messages.lock().expect("remote messages").pop() {
                    return message;
                }
            }
            panic!("remote stream notice was not sent");
        }
    }

    impl RemoteSink for RecordingRemoteSink {
        fn send(
            &self,
            addr: ActorAddress,
            message: Box<dyn Any + Send>,
        ) -> Result<(), swactor::Error> {
            let message = message
                .downcast::<HostStreamIn>()
                .expect("unexpected remote message type");
            self.messages
                .lock()
                .expect("remote messages")
                .push((addr, *message));
            Ok(())
        }
    }

    // Hold namespace and allocator replies independently while using the real
    // binding actor, arena allocator, and local stream transport.
    struct NamespaceProxy {
        requests: ActorAddress,
    }

    impl ActorInterface for NamespaceProxy {
        type Incoming = NamespaceClientIn;
        type Response = ();

        fn handle(&mut self, ctx: &Ctx<'_>, message: Self::Incoming) {
            ctx.send(self.requests, message).unwrap();
        }
    }

    struct Harness {
        _engine: Engine,
        backend: SteppingBackend,
        runtime: Runtime,
        telemetry: telemetry::TelemetryEndpoint,
        allocator: ArenaAllocatorActor,
        allocation_requests: Inbox<ArenaAllocatorIn>,
        namespace_requests: Inbox<NamespaceClientIn>,
        namespace_proxy: ActorAddress,
        namespace_reply: ActorAddress,
        allocation_capacity: u64,
        child: Inbox<ChildSessionIn>,
        host: Inbox<HostSessionIn>,
        peer: Inbox<HostStreamIn>,
        directory: Inbox<DataDirectoryIn>,
        binding: ActorAddress,
        path: DataPath,
        role: StreamRole,
        arena: Arc<MappedArena>,
        transport: Arc<LocalStreamTransport>,
        routes: Arc<TestRoutes>,
    }

    impl Harness {
        fn new() -> Self {
            Self::with_role(StreamRole::Sink)
        }

        fn with_role(role: StreamRole) -> Self {
            let parts = RuntimeParts::new(RuntimeConfig {
                worker_count: 1,
                ..RuntimeConfig::default()
            })
            .with_extension(Arc::new(StdExtension::new()));
            let runtime = parts.runtime().clone();
            let backend = SteppingBackend::new();
            let engine = Engine::new(parts, backend.clone()).expect("stream engine");
            let mut allocator_arena = ArenaManager::boot(ArenaConfig {
                node_id: NodeId(1),
                reservation_ceiling: 2 << 20,
                base_alignment: 64,
            })
            .expect("stream arena");
            let prepared = prepare_arena(
                &mut allocator_arena,
                BootstrapSpec {
                    arena_generation: 1,
                    alignment: 64,
                },
            )
            .expect("prepare stream arena");
            let (arena, _) = MappedArena::map(prepared.arena_fd).expect("map stream arena");
            let arena = Arc::new(arena);
            let allocation_requests = runtime.new_inbox().expect("allocator inbox");
            let namespace_requests = runtime.new_inbox().expect("namespace inbox");
            let namespace_proxy = runtime
                .spawn(NamespaceProxy {
                    requests: *namespace_requests.addr(),
                })
                .expect("namespace proxy actor");
            let child = runtime.new_inbox().expect("child inbox");
            let host = runtime.new_inbox().expect("host inbox");
            let peer = runtime.new_inbox().expect("peer inbox");
            let directory = runtime.new_inbox().expect("directory inbox");
            let path = DataPath::parse("/streams/displacement").expect("stream path");
            let transport = Arc::new(LocalStreamTransport::new());
            let routes = Arc::new(TestRoutes::new());
            let telemetry = telemetry::TelemetryEndpoint::new(telemetry::StreamId::new(
                telemetry::NodeId::new("namespace-wrappers"),
                telemetry::Lifetime(1),
            ))
            .with_retention(1024, 1 << 20);
            let transitions = TransitionEmitter::register(&telemetry.producer());
            let binding = runtime
                .spawn(HostStreamBindingActor {
                    transitions: Some(transitions),
                    session_generation: 1,
                    phase: StreamPhase::Resolving,
                    runtime: runtime.clone(),
                    engine: engine.handle(),
                    sender: runtime.create_sender(),
                    route_registrar: Some(routes.clone()),
                    host_session: *host.addr(),
                    allocator: *allocation_requests.addr(),
                    child_session: *child.addr(),
                    operation: *child.addr(),
                    path: path.clone(),
                    replace: false,
                    ensure: false,
                    expected_revision: None,
                    role,
                    namespace: NamespaceClient::new(runtime.clone(), namespace_proxy),
                    arena: arena.clone(),
                    transport: transport.clone(),
                    local_descriptor: transport.descriptor().expect("local descriptor"),
                    ring: None,
                    ring_probe: None,
                    ring_allocation_pending: true,
                    matched: None,
                    pending_displacements: HashMap::new(),
                    fenced_displacements: HashSet::new(),
                    pending_peer_terminations: HashMap::new(),
                    peer_descriptor: None,
                    peer_state: StreamPeerState::Negotiating,
                    transport_state: StreamTransportState::Absent,
                    open_completion: StreamOpenCompletion::Owned,
                    route_state: StreamRouteState::Unknown,
                    terminal: None,
                    data_waiters: Vec::new(),
                    capacity_waiters: Vec::new(),
                    progress_waiters: Vec::new(),
                    close_waiters: Vec::new(),
                    release: StreamRelease::NotStarted,
                    peer_was_local: false,
                    namespace_close_result: None,
                    namespace_open: None,
                    namespace_close: None,
                    route_watch: None,
                    offer_retry: None,
                    termination_retry: None,
                    termination_route_retries: 0,
                    route_loss_timer: None,
                    route_loss_generation: 0,
                    pending_peer_offers: HashMap::new(),
                    detach_pending: false,
                })
                .expect("stream binding");
            let ArenaAllocatorIn::AllocateStream { capacity, .. } =
                receive(&backend, &allocation_requests)
            else {
                panic!("expected stream allocation");
            };
            let NamespaceClientIn::Request {
                request: NamespaceRequest::OpenStream { .. },
                reply_to: namespace_reply,
            } = receive(&backend, &namespace_requests)
            else {
                panic!("expected stream namespace open");
            };
            Self {
                _engine: engine,
                backend,
                allocator: ArenaAllocatorActor::new(
                    None,
                    allocator_arena,
                    1,
                    runtime.clone(),
                    None,
                ),
                runtime,
                telemetry,
                allocation_requests,
                namespace_requests,
                namespace_proxy,
                namespace_reply,
                allocation_capacity: capacity,
                child,
                host,
                peer,
                directory,
                binding,
                path,
                role,
                arena,
                transport,
                routes,
            }
        }

        fn namespace_transitions(&self, actor: ActorAddress) -> Vec<TransitionRecord> {
            self.telemetry.tick();
            let retained = self.telemetry.subscribe_retained(
                "namespace-wrapper-lifecycle",
                telemetry::SubscriptionRequest::all(),
            );
            let mut decoder =
                protocol_telemetry::consumer::TransitionDecoder::from_snapshot(retained.snapshot());
            retained
                .drain_available()
                .iter()
                .filter_map(|event| {
                    decoder
                        .observe(event)
                        .expect("decode namespace transition")
                        .map(|observed| observed.record)
                })
                .filter(|record| {
                    record.actor == actor && record.protocol == Protocol::NamespaceOperation
                })
                .collect()
        }

        fn receive_child(&self) -> ChildSessionIn {
            let message = receive(&self.backend, &self.child);
            if let ChildSessionIn::StreamOpened { incarnation, .. } = &message {
                self.runtime
                    .send_to(
                        self.binding,
                        HostStreamIn::OpenForwarded {
                            incarnation: *incarnation,
                        },
                    )
                    .expect("acknowledge caller grant");
                settle(&self.backend);
            }
            message
        }

        fn allocate(&mut self) -> RingHandle {
            let ring = self
                .allocator
                .allocate_stream(self.allocation_capacity)
                .expect("allocate stream");
            self.runtime
                .send_to(self.binding, HostStreamIn::Allocated(Ok(ring)))
                .expect("deliver allocation");
            ring
        }

        fn install_peer(&mut self, incarnation: StreamIncarnation) -> Endpoint {
            let ring = self.allocator.allocate_stream(64).expect("peer ring");
            let notifier: Arc<dyn StreamTransportNotifier> = Arc::new(RuntimeStreamNotifier {
                runtime: self.runtime.clone(),
                target: *self.peer.addr(),
                incarnation,
            });
            match self.role {
                StreamRole::Source => {
                    self.transport
                        .install_sink(StreamSinkRequest {
                            incarnation,
                            endpoint: byte_ring::attach_mapped(&self.arena, ring, Role::Producer)
                                .expect("peer transport endpoint"),
                            notifier,
                        })
                        .expect("install peer sink");
                    byte_ring::attach_mapped(&self.arena, ring, Role::Consumer)
                        .expect("peer reader")
                }
                StreamRole::Sink => {
                    self.transport
                        .install_source(StreamSourceRequest {
                            incarnation,
                            peer: self.transport.descriptor().expect("peer descriptor"),
                            endpoint: byte_ring::attach_mapped(&self.arena, ring, Role::Consumer)
                                .expect("peer transport endpoint"),
                            notifier,
                        })
                        .expect("install peer source");
                    byte_ring::attach_mapped(&self.arena, ring, Role::Producer)
                        .expect("peer writer")
                }
            }
        }

        fn match_namespace(&self, incarnation: StreamIncarnation) {
            self.match_namespace_with_peer(incarnation, *self.peer.addr());
        }

        fn match_namespace_with_peer(&self, incarnation: StreamIncarnation, peer: ActorAddress) {
            let descriptor = self.transport.descriptor().expect("peer descriptor").0;
            let (source, sink) = match self.role {
                StreamRole::Source => (self.binding, peer),
                StreamRole::Sink => (peer, self.binding),
            };
            self.runtime
                .send_to(
                    self.namespace_reply,
                    DataDirectoryOut::StreamOpened {
                        request_id: DirectoryRequestId(1),
                        authority_epoch: incarnation.authority_epoch,
                        result: Ok(StreamMatch {
                            incarnation,
                            source,
                            source_descriptor: descriptor.clone(),
                            sink_descriptor: descriptor,
                            sink,
                            revision: incarnation.revision,
                        }),
                    },
                )
                .expect("deliver delayed namespace result");
        }

        fn displace(&self, incarnation: StreamIncarnation) {
            self.runtime
                .send_to(
                    self.binding,
                    HostStreamIn::Displaced {
                        incarnation,
                        reply_to: Some(*self.directory.addr()),
                    },
                )
                .expect("deliver displacement");
        }

        fn expect_displacement_ack(&self) {
            assert!(matches!(
                receive(&self.backend, &self.directory),
                DataDirectoryIn::StreamDisplaced { endpoint, .. } if endpoint == self.binding
            ));
        }
        fn expect_replaced(&self) {
            assert!(matches!(
                receive(&self.backend, &self.child),
                ChildSessionIn::OperationFailed {
                    error: DataPlaneError::PathReplaced(path),
                    ..
                } if path == self.path
            ));
        }

        fn finish_release_before_detach(&mut self) {
            let NamespaceClientIn::Request {
                request: NamespaceRequest::CloseStream { incarnation, .. },
                reply_to,
            } = receive(&self.backend, &self.namespace_requests)
            else {
                panic!("expected namespace stream close");
            };
            self.runtime
                .send_to(
                    reply_to,
                    DataDirectoryOut::StreamClosed {
                        request_id: DirectoryRequestId(2),
                        authority_epoch: incarnation.authority_epoch,
                        result: Ok(()),
                    },
                )
                .expect("complete namespace close");
            for _ in 0..=STREAM_PEER_TERMINATION_ROUTE_RETRY_LIMIT {
                self.backend.advance_time(STREAM_PEER_OFFER_RETRY);
                settle(&self.backend);
            }
            let ArenaAllocatorIn::ReleaseStream { binding, ring } =
                receive(&self.backend, &self.allocation_requests)
            else {
                panic!("expected stream release");
            };
            let result = self.allocator.release_stream(ring);
            assert_eq!(result, Ok(()));
            assert!(
                self.allocator
                    .arena
                    .lookup_lease(RingId(ring.lease_id))
                    .is_none()
            );
            self.runtime
                .send_to(binding, HostStreamIn::ReleaseComplete(result))
                .expect("complete allocation release");
            assert!(matches!(
                receive(&self.backend, &self.host),
                HostSessionIn::BindingDone { binding } if binding == self.binding
            ));
            assert!(self.child.try_recv().is_none(), "retired stream reopened");
        }

        fn detach_binding(&self) {
            self.runtime
                .send_to(self.binding, HostStreamIn::SessionDetached)
                .expect("detach retired stream binding");
            settle(&self.backend);
            assert!(!self.runtime.is_local_actor(self.binding));
        }

        fn finish_release(&mut self) {
            self.finish_release_before_detach();
            self.detach_binding();
        }

        fn finish_unmatched_release(&mut self) {
            let ArenaAllocatorIn::ReleaseStream { binding, ring } =
                receive(&self.backend, &self.allocation_requests)
            else {
                panic!("expected unmatched stream release");
            };
            let result = self.allocator.release_stream(ring);
            assert_eq!(result, Ok(()));
            self.runtime
                .send_to(binding, HostStreamIn::ReleaseComplete(result))
                .expect("complete unmatched allocation release");
            assert!(matches!(
                receive(&self.backend, &self.host),
                HostSessionIn::BindingDone { binding } if binding == self.binding
            ));
            self.detach_binding();
            assert!(self.child.try_recv().is_none(), "open completed twice");
            assert!(self.host.try_recv().is_none(), "binding completed twice");
        }

        fn disconnect_peer(&mut self) {
            let replacement = self.runtime.new_inbox().expect("replacement peer inbox");
            drop(std::mem::replace(&mut self.peer, replacement));
        }
    }

    const STALE: StreamIncarnation = StreamIncarnation {
        authority_epoch: 7,
        revision: 11,
    };
    const CURRENT: StreamIncarnation = StreamIncarnation {
        authority_epoch: 7,
        revision: 12,
    };
    const UNRELATED: StreamIncarnation = StreamIncarnation {
        authority_epoch: 8,
        revision: 1,
    };

    #[test]
    fn displacement_before_namespace_match_cannot_reopen_stale_stream() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(STALE);
        harness.displace(STALE);
        // An unrelated notice must not overwrite the pending exact fence.
        harness.displace(UNRELATED);
        harness.match_namespace(STALE);
        harness.expect_replaced();
        harness.expect_displacement_ack();
        // The terminal binding still acknowledges the directory's re-fan.
        harness.displace(STALE);
        harness.expect_displacement_ack();
        harness.finish_release();
    }

    #[test]
    fn displaced_pending_stream_releases_a_late_ring_allocation() {
        let mut harness = Harness::new();
        harness.displace(STALE);
        harness.match_namespace(STALE);
        harness.expect_replaced();
        harness.expect_displacement_ack();
        harness.allocate();
        harness.finish_release();
    }

    #[test]
    fn unrelated_displacements_preserve_pending_and_open_stream_progress() {
        let mut harness = Harness::new();
        harness.displace(STALE);
        harness.displace(UNRELATED);
        harness.allocate();
        let mut writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        let ChildSessionIn::StreamOpened { ring, .. } = harness.receive_child() else {
            panic!("unrelated displacement rejected current stream");
        };
        let mut reader = byte_ring::attach_mapped(&harness.arena, ring, Role::Consumer)
            .expect("current stream reader");
        harness.displace(STALE);
        harness.displace(UNRELATED);
        writer
            .send_record(RecordKind::Data, b"current")
            .expect("write current data");
        harness.transport.source_progress(CURRENT);
        assert_eq!(
            reader.recv_record().expect("read current data"),
            Some((RecordKind::Data, b"current".to_vec()))
        );
        harness.displace(CURRENT);
        harness.expect_displacement_ack();
        assert_eq!(
            reader.peer_termination().expect("read displacement fence"),
            Some(PeerTermination::PathReplaced)
        );
        harness.finish_release();
    }

    #[test]
    fn terminal_stream_releases_when_its_peer_route_is_gone() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));
        harness.disconnect_peer();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: None,
                },
            )
            .expect("close stream after peer exit");
        harness.finish_release();
    }

    #[test]
    fn terminal_stream_retries_peer_notice_while_remote_route_converges() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        let peer = ActorAddress::new_random();
        let remote = Arc::new(RecordingRemoteSink::default());
        harness.runtime.set_remote_sink(remote.clone());
        harness.match_namespace_with_peer(CURRENT, peer);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));

        harness.routes.revoke();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: None,
                },
            )
            .expect("close stream while peer route is converging");
        let NamespaceClientIn::Request {
            request: NamespaceRequest::CloseStream { incarnation, .. },
            reply_to: namespace_reply,
        } = receive(&harness.backend, &harness.namespace_requests)
        else {
            panic!("expected namespace stream close");
        };
        assert!(
            remote.messages.lock().expect("remote messages").is_empty(),
            "terminal notice bypassed the missing route"
        );

        harness.routes.restore();
        let (target, message) = remote.receive(&harness.backend);
        let HostStreamIn::PeerTerminated {
            incarnation: noticed,
            error: DataPlaneError::PeerLost,
            reply_to: Some(acknowledger),
        } = message
        else {
            panic!("expected peer termination notice");
        };
        assert_eq!(target, peer);
        assert_eq!(noticed, CURRENT);
        harness
            .runtime
            .send_to(
                acknowledger,
                HostStreamIn::PeerTerminationAck {
                    incarnation: noticed,
                },
            )
            .expect("acknowledge terminal notice");
        harness
            .runtime
            .send_to(
                namespace_reply,
                DataDirectoryOut::StreamClosed {
                    request_id: DirectoryRequestId(2),
                    authority_epoch: incarnation.authority_epoch,
                    result: Ok(()),
                },
            )
            .expect("complete namespace close");

        let ArenaAllocatorIn::ReleaseStream { binding, ring } =
            receive(&harness.backend, &harness.allocation_requests)
        else {
            panic!("expected stream release");
        };
        let result = harness.allocator.release_stream(ring);
        assert_eq!(result, Ok(()));
        harness
            .runtime
            .send_to(binding, HostStreamIn::ReleaseComplete(result))
            .expect("complete allocation release");
        assert!(matches!(
            receive(&harness.backend, &harness.host),
            HostSessionIn::BindingDone { binding } if binding == harness.binding
        ));
        harness.detach_binding();
    }

    #[test]
    fn terminal_stream_releases_when_local_peer_disappears_behind_remote_sink() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        let peer = harness
            .runtime
            .spawn(PassivePeer)
            .expect("spawn local peer");
        harness.match_namespace_with_peer(CURRENT, peer);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));

        harness
            .runtime
            .stop_actor(peer)
            .expect("stop local stream peer");
        settle(&harness.backend);
        assert!(!harness.runtime.is_local_actor(peer));
        harness
            .runtime
            .set_remote_sink(Arc::new(AcceptingRemoteSink));
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: None,
                },
            )
            .expect("close stream after local peer exit");
        harness.finish_release();
    }

    #[test]
    fn released_stream_binding_drains_reply_controls_before_session_detach() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));
        harness.disconnect_peer();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: None,
                },
            )
            .expect("terminate stream");
        harness.finish_release_before_detach();

        let reply_to = *harness.directory.addr();
        harness
            .runtime
            .send_to(harness.binding, HostStreamIn::WaitCapacity { reply_to })
            .expect("forward late capacity wait");
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamWake {
                reply_to: observed,
                result: Err(DataPlaneError::OperationCancelled | DataPlaneError::PeerLost),
            } if observed == reply_to
        ));
        harness.detach_binding();
    }

    #[test]
    fn session_detach_drains_already_queued_stream_controls() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));
        harness.disconnect_peer();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: None,
                },
            )
            .expect("terminate stream");
        harness.finish_release_before_detach();

        let suspended = harness
            .runtime
            .admin()
            .suspend_actor(harness.binding)
            .expect("suspend binding");
        settle(&harness.backend);
        future::block_on(suspended.recv()).expect("binding suspended");
        let reply_to = *harness.directory.addr();
        harness
            .runtime
            .send_to(harness.binding, HostStreamIn::SessionDetached)
            .expect("queue session detach");
        harness
            .runtime
            .send_to(harness.binding, HostStreamIn::WaitCapacity { reply_to })
            .expect("queue final capacity wait");
        let resumed = harness
            .runtime
            .admin()
            .resume_actor(harness.binding)
            .expect("resume binding");
        settle(&harness.backend);
        future::block_on(resumed.recv()).expect("binding resumed");

        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamWake {
                reply_to: observed,
                result: Err(DataPlaneError::OperationCancelled | DataPlaneError::PeerLost),
            } if observed == reply_to
        ));
    }

    #[test]
    fn host_session_wakes_reply_controls_after_stream_binding_retirement() {
        let parts = RuntimeParts::new(RuntimeConfig {
            worker_count: 1,
            ..RuntimeConfig::default()
        })
        .with_extension(Arc::new(StdExtension::new()));
        let runtime = parts.runtime().clone();
        let backend = SteppingBackend::new();
        let engine = Engine::new(parts, backend.clone()).expect("host-session engine");
        let child = runtime.new_inbox::<ChildSessionIn>().expect("child inbox");
        let binding = runtime
            .new_inbox::<HostStreamIn>()
            .expect("stream binding inbox");
        let mut session = HostDataPlaneSessionActor::new(HostDataPlaneConfig {
            runtime: runtime.clone(),
            engine: engine.handle(),
            arena: ArenaManager::boot(ArenaConfig {
                node_id: NodeId(2),
                reservation_ceiling: 2 << 20,
                base_alignment: 64,
            })
            .expect("host-session arena"),
            arena_generation: 1,
            session_generation: 1,
            capability: SessionCapability::new([3; 32]),
            session_access: SessionAccess {
                execution_id: "retired-stream-control".to_owned(),
                read_prefixes: Vec::new(),
                write_prefixes: Vec::new(),
            },
            namespace: None,
            transfer_receiver: None,
            source_sender: None,
            source_publisher: None,
            route_registrar: None,
            stream_transport: None,
            transitions: None,
        })
        .expect("host session");
        session.state = HostSessionState::Running;
        session.child_session = Some(*child.addr());
        session
            .stream_bindings
            .insert(*binding.addr(), *binding.addr());
        let host_session = runtime.spawn(session).expect("spawn host session");

        runtime
            .send_to(
                host_session,
                HostSessionIn::BindingDone {
                    binding: *binding.addr(),
                },
            )
            .expect("retire binding");
        assert!(matches!(
            receive(&backend, &binding),
            HostStreamIn::SessionDetached
        ));

        let reply_to = *binding.addr();
        runtime
            .send_to(
                host_session,
                HostSessionIn::StreamControl {
                    binding: *binding.addr(),
                    message: HostStreamIn::WaitCapacity { reply_to },
                },
            )
            .expect("send late stream control");
        assert!(matches!(
            receive(&backend, &child),
            ChildSessionIn::StreamWake {
                reply_to: observed,
                result: Err(DataPlaneError::StreamClosed),
            } if observed == reply_to
        ));
    }

    #[test]
    fn active_stream_terminates_when_peer_route_is_revoked_without_peer_notice() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        let ChildSessionIn::StreamOpened { ring, .. } = harness.receive_child() else {
            panic!("expected open stream");
        };
        let reader =
            byte_ring::attach_mapped(&harness.arena, ring, Role::Consumer).expect("stream reader");
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::WaitData {
                    reply_to: *harness.directory.addr(),
                },
            )
            .expect("park stream read");

        harness.routes.revoke();
        settle(&harness.backend);

        assert!(
            harness.child.try_recv().is_none(),
            "transient route loss terminated the stream before its grace period"
        );

        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::PeerRouteLossCheck {
                    incarnation: CURRENT,
                    generation: 1,
                },
            )
            .expect("expire route-loss grace");
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamWake {
                result: Err(DataPlaneError::PeerLost),
                ..
            }
        ));
        assert_eq!(
            reader.peer_termination().expect("read peer loss"),
            Some(PeerTermination::Closed)
        );
        harness.finish_release();
    }

    #[test]
    fn active_stream_survives_transient_route_loss_with_transport_progress() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        let ChildSessionIn::StreamOpened { ring, .. } = harness.receive_child() else {
            panic!("expected open stream");
        };
        let reader =
            byte_ring::attach_mapped(&harness.arena, ring, Role::Consumer).expect("stream reader");
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::WaitData {
                    reply_to: *harness.directory.addr(),
                },
            )
            .expect("park stream read");
        harness.routes.revoke();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Transport {
                    incarnation: CURRENT,
                    event: StreamTransportEvent::DataAvailable,
                },
            )
            .expect("report transport progress");
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamWake { result: Ok(()), .. }
        ));
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::PeerRouteLossCheck {
                    incarnation: CURRENT,
                    generation: 1,
                },
            )
            .expect("deliver stale route-loss timer");
        settle(&harness.backend);

        assert!(
            harness.child.try_recv().is_none(),
            "stale route-loss timer terminated a progressing stream"
        );

        assert_eq!(reader.peer_termination().expect("live peer"), None);
        harness.displace(CURRENT);
        harness.expect_displacement_ack();
        harness.finish_release();
    }

    #[test]
    fn flush_progress_waits_for_transport_consumption() {
        let mut harness = Harness::new();
        harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::WaitProgress {
                    reply_to: *harness.directory.addr(),
                    target: 1,
                },
            )
            .expect("park flush progress");
        settle(&harness.backend);

        assert!(
            harness.child.try_recv().is_none(),
            "flush progress woke before transport consumption"
        );

        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Transport {
                    incarnation: CURRENT,
                    event: StreamTransportEvent::CapacityAvailable,
                },
            )
            .expect("report transport consumption");
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamWake { result: Ok(()), .. }
        ));

        harness.displace(CURRENT);
        harness.expect_displacement_ack();
        harness.finish_release();
    }

    #[test]
    fn flush_progress_observes_consumption_before_wait_registration() {
        let mut harness = Harness::with_role(StreamRole::Source);
        let ring = harness.allocate();
        let _reader = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));
        let mut writer =
            byte_ring::attach_mapped(&harness.arena, ring, Role::Producer).expect("child writer");
        writer
            .send_record(RecordKind::Data, b"already-consumed")
            .expect("commit source data");
        let target = writer.positions().expect("source positions").0;
        harness
            .runtime
            .send_to(harness.binding, HostStreamIn::DataAvailable)
            .expect("drive source transport");
        settle(&harness.backend);
        assert!(writer.positions().expect("source progress").1 >= target);

        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::WaitCapacity {
                    reply_to: *harness.directory.addr(),
                },
            )
            .expect("synchronize completed capacity notification");
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamWake { result: Ok(()), .. }
        ));
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::WaitProgress {
                    reply_to: *harness.directory.addr(),
                    target,
                },
            )
            .expect("register completed flush progress");
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamWake { result: Ok(()), .. }
        ));

        harness.displace(CURRENT);
        harness.expect_displacement_ack();
        harness.finish_release();
    }

    #[test]
    fn displacement_marker_does_not_require_a_stable_cursor_snapshot() {
        let mut harness = Harness::new();
        let ring = harness.allocate();
        harness
            .allocator
            .arena
            .write_arena(ring.offset + byte_ring::OFF_CONSUME, &1_u64.to_le_bytes())
            .expect("create transient cursor snapshot");
        byte_ring::mark_peer_terminated_mapped(&harness.arena, ring, PeerTermination::PathReplaced)
            .expect("mark active ring without cursor validation");
        let endpoint = byte_ring::attach_mapped(&harness.arena, ring, Role::Consumer);
        assert!(matches!(
            endpoint,
            Err(byte_ring::AttachError::Header(
                byte_ring::HeaderError::CommitBelowConsume { .. }
            ))
        ));
        let marker = harness
            .allocator
            .arena
            .read_arena(ring.offset + byte_ring::OFF_TERMINAL, 8)
            .expect("read terminal marker");
        assert_eq!(u64::from_le_bytes(marker.try_into().unwrap()), 2);
    }

    #[test]
    fn clean_close_before_readiness_completes_open_and_reclaims_ring() {
        let mut harness = Harness::new();
        harness.allocate();
        harness.match_namespace(CURRENT);
        settle(&harness.backend);
        assert!(harness.child.try_recv().is_none());
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: true,
                    reply_to: None,
                },
            )
            .unwrap();
        assert!(matches!(
            receive(&harness.backend, &harness.child),
            ChildSessionIn::OperationFailed {
                error: DataPlaneError::StreamClosed,
                ..
            }
        ));
        // A transport callback already queued when close ran cannot resurrect it.
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Transport {
                    incarnation: CURRENT,
                    event: StreamTransportEvent::Ready,
                },
            )
            .unwrap();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::PeerTerminationAck {
                    incarnation: CURRENT,
                },
            )
            .unwrap();
        harness.finish_release();
    }

    #[test]
    fn close_reply_waits_for_namespace_retirement_after_ring_release() {
        let mut harness = Harness::new();
        let ring = harness.allocate();
        let _writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));
        harness.displace(CURRENT);
        harness.expect_displacement_ack();
        let reply_to = *harness.directory.addr();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: false,
                    reply_to: Some(reply_to),
                },
            )
            .unwrap();
        let NamespaceClientIn::Request {
            request: NamespaceRequest::CloseStream { .. },
            reply_to: namespace_close,
        } = receive(&harness.backend, &harness.namespace_requests)
        else {
            panic!("namespace close missing");
        };
        let ArenaAllocatorIn::ReleaseStream {
            binding,
            ring: released_ring,
        } = receive(&harness.backend, &harness.allocation_requests)
        else {
            panic!("ring release missing");
        };
        assert_eq!(ring, released_ring);
        let result = harness.allocator.release_stream(ring);
        assert_eq!(result, Ok(()));
        harness
            .runtime
            .send_to(binding, HostStreamIn::ReleaseComplete(result.clone()))
            .unwrap();
        harness
            .runtime
            .send_to(binding, HostStreamIn::ReleaseComplete(result))
            .unwrap();
        settle(&harness.backend);
        assert!(
            harness.host.try_recv().is_none(),
            "host forgot its namespace cleanup owner"
        );
        assert!(
            harness.child.try_recv().is_none(),
            "close acknowledged before namespace retirement"
        );
        assert!(harness.runtime.is_local_actor(namespace_close));
        assert!(
            harness
                .allocator
                .arena
                .lookup_lease(RingId(ring.lease_id))
                .is_none()
        );
        for _ in 0..2 {
            harness
                .runtime
                .send_to(
                    namespace_close,
                    DataDirectoryOut::StreamClosed {
                        request_id: DirectoryRequestId(2),
                        authority_epoch: CURRENT.authority_epoch,
                        result: Ok(()),
                    },
                )
                .unwrap();
        }
        assert!(
            matches!(receive(&harness.backend, &harness.child), ChildSessionIn::StreamWake { reply_to: observed, result: Ok(()) } if observed == reply_to)
        );
        assert!(
            matches!(receive(&harness.backend, &harness.host), HostSessionIn::BindingDone { binding } if binding == harness.binding)
        );
        settle(&harness.backend);
        assert!(harness.child.try_recv().is_none());
        assert!(!harness.runtime.is_local_actor(namespace_close));
        harness.detach_binding();
        for (actor, actor_type) in [
            (harness.namespace_reply, ActorType::NamespaceStreamOpenActor),
            (namespace_close, ActorType::NamespaceStreamCloseActor),
        ] {
            let records = harness.namespace_transitions(actor);
            assert_eq!(
                records
                    .iter()
                    .map(|record| (record.previous, record.next, record.reason, record.outcome))
                    .collect::<Vec<_>>(),
                [
                    (Phase::Created, Phase::AwaitingReply, Reason::Started, None),
                    (
                        Phase::AwaitingReply,
                        Phase::Finished,
                        Reason::Completed,
                        Some(Outcome::Success),
                    ),
                ],
                "a duplicate reply or on_stop must not complete the wrapper twice"
            );
            let terminal = records.last().unwrap();
            assert_eq!(terminal.actor_type, actor_type);
            assert_eq!(
                terminal.operation,
                Some(harness.child.addr().0[..16].try_into().unwrap())
            );
            assert_eq!(terminal.generation, Some(CURRENT.authority_epoch));
            assert_eq!(terminal.incarnation, Some(CURRENT.revision));
        }
    }

    #[test]
    fn namespace_open_cancel_retains_one_terminal_and_resolves_caller() {
        let mut harness = Harness::new();
        harness.allocate();
        for _ in 0..2 {
            harness
                .runtime
                .send_to(
                    harness.binding,
                    HostStreamIn::Close {
                        clean: false,
                        reply_to: None,
                    },
                )
                .unwrap();
        }
        assert!(matches!(
            receive(&harness.backend, &harness.child),
            ChildSessionIn::OperationFailed {
                error: DataPlaneError::OperationCancelled,
                ..
            }
        ));
        assert!(matches!(
            receive(&harness.backend, &harness.namespace_requests),
            NamespaceClientIn::Cancel { reply_to } if reply_to == harness.namespace_reply
        ));
        harness.finish_unmatched_release();
        let records = harness.namespace_transitions(harness.namespace_reply);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].next, Phase::AwaitingReply);
        assert_eq!(records[0].outcome, None);
        assert_eq!(records[1].previous, Phase::AwaitingReply);
        assert_eq!(records[1].next, Phase::Finished);
        assert_eq!(records[1].reason, Reason::ParentStopped);
        assert_eq!(records[1].outcome, Some(Outcome::Cancelled));
        assert_eq!(records[1].actor_type, ActorType::NamespaceStreamOpenActor);
        assert_eq!(
            records[1].operation,
            Some(harness.child.addr().0[..16].try_into().unwrap())
        );
        assert_eq!(records[1].generation, Some(1));
        assert_eq!(records[1].incarnation, None);
        assert!(harness.namespace_requests.try_recv().is_none());
    }

    #[test]
    fn namespace_proxy_exit_retains_peer_loss_and_parent_completes_open_once() {
        let mut harness = Harness::new();
        harness.allocate();
        harness.runtime.stop_actor(harness.namespace_proxy).unwrap();
        assert!(matches!(
            receive(&harness.backend, &harness.child),
            ChildSessionIn::OperationFailed {
                error: DataPlaneError::SessionNotRunning,
                ..
            }
        ));
        harness.finish_unmatched_release();
        let records = harness.namespace_transitions(harness.namespace_reply);
        assert_eq!(
            records
                .iter()
                .filter_map(|record| record.outcome.map(|outcome| (record.reason, outcome)))
                .collect::<Vec<_>>(),
            [(Reason::PeerTerminated, Outcome::PeerLost)],
            "the wrapper's subsequent stop must not overwrite or duplicate peer loss"
        );
        assert_eq!(records[1].actor_type, ActorType::NamespaceStreamOpenActor);
        assert_eq!(
            records[1].operation,
            Some(harness.child.addr().0[..16].try_into().unwrap())
        );
        assert_eq!(records[1].generation, Some(1));
        assert_eq!(records[1].incarnation, None);
    }

    #[test]
    fn namespace_reply_failure_fences_queued_success_and_retains_one_completion() {
        let mut harness = Harness::new();
        harness.allocate();
        harness
            .runtime
            .send_to(
                harness.namespace_reply,
                DataDirectoryOut::StreamOpened {
                    request_id: DirectoryRequestId(1),
                    authority_epoch: CURRENT.authority_epoch,
                    result: Err(NamespaceError::PathNotFound(harness.path.clone())),
                },
            )
            .unwrap();
        harness.match_namespace(CURRENT);
        assert!(matches!(
            receive(&harness.backend, &harness.child),
            ChildSessionIn::OperationFailed {
                error: DataPlaneError::PathNotFound(path),
                ..
            } if path == harness.path
        ));
        harness.finish_unmatched_release();
        let records = harness.namespace_transitions(harness.namespace_reply);
        assert_eq!(
            records
                .iter()
                .filter_map(|record| record.outcome.map(|outcome| (record.reason, outcome)))
                .collect::<Vec<_>>(),
            [(Reason::Failed, Outcome::Failed)]
        );
        assert_eq!(records[1].actor_type, ActorType::NamespaceStreamOpenActor);
        assert_eq!(records[1].incarnation, None);
        assert!(harness.namespace_requests.try_recv().is_none());
    }

    #[test]
    fn route_loss_before_readiness_resolves_pending_reader() {
        let mut harness = Harness::new();
        harness.allocate();
        harness.match_namespace(CURRENT);
        settle(&harness.backend);
        harness.routes.revoke();
        settle(&harness.backend);
        assert!(harness.child.try_recv().is_none());
        harness.backend.advance_time(STREAM_PEER_ROUTE_LOSS_GRACE);
        assert!(matches!(
            receive(&harness.backend, &harness.child),
            ChildSessionIn::OperationFailed {
                error: DataPlaneError::PeerLost,
                ..
            }
        ));
        harness.finish_release();
    }

    #[test]
    fn stale_namespace_and_transport_events_cannot_rebind_open_incarnation() {
        let mut harness = Harness::new();
        let ring = harness.allocate();
        let mut writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened {
                incarnation: CURRENT,
                ..
            }
        ));
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::NamespaceMatched(Ok(StreamMatch {
                    incarnation: UNRELATED,
                    source: *harness.peer.addr(),
                    sink: harness.binding,
                    source_descriptor: Vec::new(),
                    sink_descriptor: Vec::new(),
                    revision: UNRELATED.revision,
                })),
            )
            .unwrap();
        for event in [
            StreamTransportEvent::Ready,
            StreamTransportEvent::Quiesced,
            StreamTransportEvent::Fault("old transport".to_owned()),
        ] {
            harness
                .runtime
                .send_to(
                    harness.binding,
                    HostStreamIn::Transport {
                        incarnation: UNRELATED,
                        event,
                    },
                )
                .unwrap();
        }
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::PeerTerminated {
                    incarnation: UNRELATED,
                    error: DataPlaneError::PeerLost,
                    reply_to: None,
                },
            )
            .unwrap();
        settle(&harness.backend);
        assert!(harness.child.try_recv().is_none());
        let mut reader = byte_ring::attach_mapped(&harness.arena, ring, Role::Consumer).unwrap();
        writer
            .send_record(RecordKind::Data, b"current incarnation")
            .unwrap();
        harness.transport.source_progress(CURRENT);
        assert_eq!(
            reader.recv_record().unwrap(),
            Some((RecordKind::Data, b"current incarnation".to_vec()))
        );
        assert_eq!(reader.peer_termination().unwrap(), None);
        harness.displace(CURRENT);
        harness.expect_displacement_ack();
        harness.finish_release();
    }

    #[test]
    fn peer_termination_before_match_fails_once_and_acknowledges_duplicates() {
        let mut harness = Harness::new();
        harness.allocate();
        let notice = HostStreamIn::PeerTerminated {
            incarnation: CURRENT,
            error: DataPlaneError::PeerLost,
            reply_to: Some(*harness.peer.addr()),
        };
        harness
            .runtime
            .send_to(harness.binding, notice.clone())
            .unwrap();
        harness
            .runtime
            .send_to(harness.binding, notice.clone())
            .unwrap();
        harness.match_namespace(CURRENT);
        assert!(matches!(
            receive(&harness.backend, &harness.child),
            ChildSessionIn::OperationFailed {
                error: DataPlaneError::PeerLost,
                ..
            }
        ));
        assert!(matches!(
            receive(&harness.backend, &harness.peer),
            HostStreamIn::PeerTerminationAck {
                incarnation: CURRENT
            }
        ));
        harness.runtime.send_to(harness.binding, notice).unwrap();
        assert!(matches!(
            receive(&harness.backend, &harness.peer),
            HostStreamIn::PeerTerminationAck {
                incarnation: CURRENT
            }
        ));
        harness.finish_release();
    }

    #[test]
    fn local_peer_exit_before_readiness_releases_open_completion() {
        let mut harness = Harness::new();
        harness.allocate();
        let peer = harness.runtime.spawn(PassivePeer).unwrap();
        harness.match_namespace_with_peer(CURRENT, peer);
        settle(&harness.backend);
        assert!(harness.child.try_recv().is_none());
        harness.runtime.stop_actor(peer).unwrap();
        assert!(matches!(
            receive(&harness.backend, &harness.child),
            ChildSessionIn::OperationFailed {
                error: DataPlaneError::PeerLost,
                ..
            }
        ));
        harness.finish_release();
        assert!(!harness.runtime.is_local_actor(peer));
    }

    #[test]
    fn unmatched_offer_from_newer_epoch_does_not_replace_authenticated_offer() {
        let mut harness = Harness::with_role(StreamRole::Source);
        harness.allocate();
        let _reader = harness.install_peer(CURRENT);
        let descriptor = harness.transport.descriptor().unwrap();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::PeerOffer {
                    incarnation: CURRENT,
                    descriptor: descriptor.clone(),
                },
            )
            .unwrap();
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::PeerOffer {
                    incarnation: UNRELATED,
                    descriptor: StreamPeerDescriptor(vec![0xff]),
                },
            )
            .unwrap();
        harness
            .runtime
            .send_to(
                harness.namespace_reply,
                DataDirectoryOut::StreamOpened {
                    request_id: DirectoryRequestId(1),
                    authority_epoch: CURRENT.authority_epoch,
                    result: Ok(StreamMatch {
                        incarnation: CURRENT,
                        source: harness.binding,
                        sink: *harness.peer.addr(),
                        source_descriptor: descriptor.0,
                        sink_descriptor: Vec::new(),
                        revision: CURRENT.revision,
                    }),
                },
            )
            .unwrap();
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened {
                incarnation: CURRENT,
                ..
            }
        ));
        harness.displace(CURRENT);
        harness.expect_displacement_ack();
        harness.finish_release();
    }

    #[test]
    fn unread_eof_wakes_reader_after_transport_has_quiesced() {
        let mut harness = Harness::new();
        let ring = harness.allocate();
        let mut writer = harness.install_peer(CURRENT);
        harness.match_namespace(CURRENT);
        assert!(matches!(
            harness.receive_child(),
            ChildSessionIn::StreamOpened { .. }
        ));
        writer
            .send_record(RecordKind::Data, b"writer finished")
            .unwrap();
        writer.send_record(RecordKind::Eof, b"").unwrap();
        harness.transport.source_progress(CURRENT);
        settle(&harness.backend);
        assert!(
            !harness.transport.sink_has_data(CURRENT),
            "transport must have retired its endpoint"
        );
        let reply_to = *harness.directory.addr();
        harness
            .runtime
            .send_to(harness.binding, HostStreamIn::WaitData { reply_to })
            .unwrap();
        assert!(matches!(receive(&harness.backend, &harness.child),
            ChildSessionIn::StreamWake { reply_to: observed, result: Ok(()) } if observed == reply_to));
        let mut reader = byte_ring::attach_mapped(&harness.arena, ring, Role::Consumer).unwrap();
        assert_eq!(
            reader.recv_record().unwrap(),
            Some((RecordKind::Data, b"writer finished".to_vec()))
        );
        assert_eq!(
            reader.recv_record().unwrap(),
            Some((RecordKind::Eof, Vec::new()))
        );
        harness
            .runtime
            .send_to(
                harness.binding,
                HostStreamIn::Close {
                    clean: true,
                    reply_to: None,
                },
            )
            .unwrap();
        harness.finish_release();
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
    Cancelling,
    Granted,
    Sealing,
    Publishing,
    Published,
    Unpublishing,
    Retiring,
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
    Unpublished(Result<(), DataPlaneError>),
    ReservationReleased(Result<(), DataPlaneError>),
    ReleasePublished,
    SessionClosed,
    Released(Result<(), DataPlaneError>),
}

struct HostBindingAddresses {
    transitions: Option<TransitionEmitter>,
    host_session: ActorAddress,
    allocator: ActorAddress,
    child_session: ActorAddress,
    operation: ActorAddress,
}

struct HostBlobBindingActor {
    transitions: Option<TransitionEmitter>,
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
    retained_source: Option<ActorAddress>,
    published_source: Option<ActorAddress>,
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
    fn enter_phase(&mut self, ctx: &Ctx<'_>, next: HostBindingState, reason: Reason) {
        let previous = self.state;
        self.state = next;
        let phase = |state| match state {
            HostBindingState::Resolving => Phase::Resolving,
            HostBindingState::Allocated => Phase::Allocating,
            HostBindingState::Filling => Phase::Filling,
            HostBindingState::Cancelling => Phase::Aborting,
            HostBindingState::Granted => Phase::Open,
            HostBindingState::Sealing => Phase::Sealing,
            HostBindingState::Publishing => Phase::Publishing,
            HostBindingState::Published => Phase::Ready,
            HostBindingState::Unpublishing => Phase::Closing,
            HostBindingState::Retiring => Phase::Releasing,
            HostBindingState::Releasing => Phase::Releasing,
            HostBindingState::Released => Phase::Closed,
            HostBindingState::Faulted => Phase::Failed,
        };
        if let Some(transitions) = &self.transitions {
            transitions.emit(TransitionRecord {
                actor: ctx.self_addr(),
                actor_type: ActorType::HostBlobBindingActor,
                protocol: Protocol::HostBlob,
                operation: Some(
                    self.operation.0[..16]
                        .try_into()
                        .expect("operation identity"),
                ),
                generation: self.lease.map(|lease| lease.generation),
                incarnation: None,
                previous: phase(previous),
                next: phase(next),
                reason,
                outcome: match next {
                    HostBindingState::Released => Some(Outcome::Success),
                    HostBindingState::Faulted => Some(Outcome::Failed),
                    _ => None,
                },
            });
        }
    }

    fn namespace_read(params: NamespaceReadParams) -> Self {
        let NamespaceReadParams {
            addresses:
                HostBindingAddresses {
                    transitions,
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
            transitions,
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
            retained_source: None,
            release_outcome: None,
            auxiliary: None,
            published_source: None,
            reservation: None,
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
            transitions,
            host_session,
            allocator,
            child_session,
            operation,
        } = addresses;
        Self {
            transitions,
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
            retained_source: None,
            published_source: None,
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
            phase: NamespaceOperationPhase::Created,
            transitions: self.transitions.clone(),
            operation: self.operation,
            generation: self.lease.map(|lease| lease.generation),
        }) {
            Ok(releaser) => {
                self.auxiliary = Some(releaser);
                ctx.watch(releaser);
                self.release_outcome = Some(outcome);
                self.enter_phase(ctx, HostBindingState::Releasing, Reason::ReleaseRequested);
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
            binding: ctx.self_addr(),
            phase: NamespaceOperationPhase::Created,
            transitions: self.transitions.clone(),
            operation: self.operation,
            generation: self.lease.map(|lease| lease.generation),
            namespace_proxy: namespace.proxy(),
            path: self.path.clone(),
            operation_id,
            source,
        }) {
            Ok(unpublisher) => {
                self.auxiliary = Some(unpublisher);
                ctx.watch(unpublisher);
                self.release_outcome = Some(outcome);
                self.enter_phase(ctx, HostBindingState::Unpublishing, Reason::CloseRequested);
            }
            Err(error) => self.fail(ctx, DataPlaneError::SessionFailed(error.to_string())),
        }
    }

    fn begin_release(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        let Some(lease) = self.lease else {
            self.finish_without_lease(ctx, outcome);
            return;
        };
        self.release_retained_source();
        self.enter_phase(ctx, HostBindingState::Releasing, Reason::ReleaseRequested);
        self.release_outcome = Some(outcome);
        if ctx
            .send(
                self.allocator,
                ArenaAllocatorIn::Release {
                    binding: ctx.self_addr(),
                    lease,
                },
            )
            .is_err()
        {
            let outcome = self
                .release_outcome
                .take()
                .unwrap_or(ReleaseOutcome::Faulted);
            self.finish_without_lease(ctx, outcome);
        }
    }

    fn release_retained_source(&mut self) {
        let Some(source) = self.retained_source.take() else {
            return;
        };
        if let BindingMode::NamespaceRead {
            route_registrar: Some(registrar),
            ..
        } = &self.mode
        {
            registrar.release_source(source);
        }
    }
    fn finish_without_lease(&mut self, ctx: &Ctx<'_>, outcome: ReleaseOutcome) {
        if self.begin_reservation_release(ctx, outcome.clone()) {
            return;
        }
        self.release_retained_source();
        self.enter_phase(ctx, HostBindingState::Released, Reason::Released);
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
        if read_released && let Some(lease) = self.lease {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::BlobReleased {
                    binding: ctx.self_addr(),
                    lease_id: lease.lease_id,
                    generation: lease.generation,
                },
            );
        }
        self.lease = None;
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::BindingDone {
                binding: ctx.self_addr(),
            },
        );
        ctx.stop_self();
    }

    fn fail(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        if matches!(
            self.state,
            HostBindingState::Released | HostBindingState::Retiring
        ) {
            return;
        }
        self.enter_phase(ctx, HostBindingState::Faulted, Reason::Failed);
        let _ = ctx.send(
            self.host_session,
            HostSessionIn::BindingFaulted {
                binding: ctx.self_addr(),
                operation: self.operation,
                error,
            },
        );
        if let Some(source) = self.published_source.take() {
            self.release_outcome = Some(ReleaseOutcome::Faulted);
            self.enter_phase(ctx, HostBindingState::Retiring, Reason::ReleaseRequested);
            let _ = ctx.send(source, BlobSourceIn::Retire { reply_to: None });
            // Source retirement owns allocator release until every physical
            // adapter callback drops its backing-region owner.
        } else if self.lease.is_some() {
            self.begin_release(ctx, ReleaseOutcome::Faulted);
        } else {
            self.finish_without_lease(ctx, ReleaseOutcome::Faulted);
        }
    }

    fn lease_matches(&self, lease_id: crate::ids::BlobLeaseId, generation: u64) -> bool {
        self.lease
            .is_some_and(|lease| lease.lease_id == lease_id && lease.generation == generation)
    }
    fn resolved(&mut self, ctx: &Ctx<'_>, result: Result<NamespaceBlobBinding, DataPlaneError>) {
        match result {
            Ok(binding)
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
                if let Some(registrar) = route_registrar.as_ref()
                    && let Err(error) = registrar.retain_source(binding.source, binding.source_node)
                {
                    self.fail(
                        ctx,
                        DataPlaneError::SessionFailed(format!(
                            "retain resolved blob source route: {error}"
                        )),
                    );
                    return;
                }
                self.retained_source = Some(binding.source);
                let mut id_bytes = [0_u8; 8];
                id_bytes.copy_from_slice(&ctx.self_addr().0[..8]);
                let transfer_id = BlobTransferId(u64::from_le_bytes(id_bytes).max(1));
                match ctx.spawn(DestinationBlobTransferActor::new(
                    DestinationTransferParams {
                        transitions: self.transitions.clone(),
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
                        ctx.watch(transfer);
                        self.enter_phase(ctx, HostBindingState::Filling, Reason::Allocated);
                    }
                    Err(error) => {
                        self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
                    }
                }
            }
            Err(error) if self.state == HostBindingState::Resolving => {
                self.auxiliary = None;
                self.fail(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn transfer_ready(&mut self, ctx: &Ctx<'_>, lease: BlobLease, metadata: BlobMetadata) {
        match (lease, metadata) {
            (lease, metadata)
                if matches!(
                    self.state,
                    HostBindingState::Filling | HostBindingState::Cancelling
                ) && matches!(self.mode, BindingMode::NamespaceRead { .. }) =>
            {
                self.auxiliary = None;
                self.lease = Some(lease);
                self.metadata = Some(metadata.clone());
                if self.state == HostBindingState::Cancelling {
                    self.begin_release(ctx, ReleaseOutcome::Faulted);
                } else {
                    self.enter_phase(ctx, HostBindingState::Granted, Reason::Opened);
                    if ctx
                        .send(
                            self.child_session,
                            ChildSessionIn::BlobOpened {
                                operation: self.operation,
                                host_binding: ctx.self_addr(),
                                lease,
                                metadata,
                            },
                        )
                        .is_err()
                    {
                        self.fail(ctx, DataPlaneError::SessionNotRunning);
                    }
                }
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn transfer_failed(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        match error {
            error
                if matches!(
                    self.state,
                    HostBindingState::Resolving
                        | HostBindingState::Filling
                        | HostBindingState::Cancelling
                ) =>
            {
                self.auxiliary = None;
                self.fail(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn allocated(
        &mut self,
        ctx: &Ctx<'_>,
        result: Result<(BlobLease, BlobMetadata), DataPlaneError>,
    ) {
        match result {
            Ok((lease, metadata))
                if matches!(
                    self.state,
                    HostBindingState::Filling | HostBindingState::Cancelling
                ) =>
            {
                self.lease = Some(lease);
                self.metadata = Some(metadata.clone());
                if self.state == HostBindingState::Cancelling {
                    self.begin_release(ctx, ReleaseOutcome::Faulted);
                } else {
                    self.enter_phase(ctx, HostBindingState::Granted, Reason::Opened);
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
                    if ctx.send(self.child_session, response).is_err() {
                        self.fail(ctx, DataPlaneError::SessionNotRunning);
                    }
                }
            }
            Err(error)
                if matches!(
                    self.state,
                    HostBindingState::Filling | HostBindingState::Cancelling
                ) =>
            {
                self.fail(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn cancel_open(&mut self, ctx: &Ctx<'_>, operation: ActorAddress) {
        if operation == self.operation
            && matches!(
                self.state,
                HostBindingState::Resolving | HostBindingState::Filling | HostBindingState::Granted
            )
        {
            let _ = ctx.send(
                self.child_session,
                ChildSessionIn::OperationFailed {
                    operation,
                    error: DataPlaneError::OperationCancelled,
                },
            );
        }
        match (operation,) {
            (operation,) if operation == self.operation => match self.state {
                HostBindingState::Resolving => {
                    self.finish_without_lease(ctx, ReleaseOutcome::Faulted);
                }
                HostBindingState::Filling => {
                    self.enter_phase(ctx, HostBindingState::Cancelling, Reason::Cancelled);
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
            },
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn release(&mut self, ctx: &Ctx<'_>, lease_id: crate::ids::BlobLeaseId, generation: u64) {
        match (lease_id, generation) {
            (lease_id, generation)
                if self.state == HostBindingState::Granted
                    && matches!(self.mode, BindingMode::NamespaceRead { .. }) =>
            {
                if self.lease_matches(lease_id, generation) {
                    self.begin_release(ctx, ReleaseOutcome::ReadReleased);
                }
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn seal(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        lease: BlobLease,
        metadata: BlobMetadata,
    ) {
        match (operation, lease, metadata) {
            (operation, lease, metadata)
                if self.state == HostBindingState::Granted
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
                self.enter_phase(ctx, HostBindingState::Sealing, Reason::Requested);
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::ValidateSealed {
                        binding: ctx.self_addr(),
                        lease,
                        metadata,
                    },
                );
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn seal_validated(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        match result {
            Ok(()) if self.state == HostBindingState::Sealing => {
                let lease = self.lease.expect("validated write lease");
                let metadata = self.metadata.clone().expect("validated write metadata");
                self.enter_phase(ctx, HostBindingState::Publishing, Reason::Requested);
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
            Err(error) if self.state == HostBindingState::Sealing => {
                self.fail(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn published_source(&mut self, ctx: &Ctx<'_>, result: Result<ActorAddress, DataPlaneError>) {
        match result {
            Ok(source) if self.state == HostBindingState::Publishing => {
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
                let source_node = match publisher.publish_source(source) {
                    Ok(node) => node,
                    Err(error) => {
                        let _ = ctx.send(source, BlobSourceIn::Retire { reply_to: None });
                        self.fail(ctx, DataPlaneError::SessionFailed(error));
                        return;
                    }
                };
                self.published_source = Some(source);
                let mut id_bytes = [0_u8; 16];
                id_bytes.copy_from_slice(&ctx.self_addr().0[..16]);
                let operation_id = OperationId::from_u128(u128::from_be_bytes(id_bytes));
                match ctx.spawn(NamespacePublishActor {
                    namespace_proxy: namespace.proxy(),
                    path: self.path.clone(),
                    source,
                    source_node,
                    length: self.metadata.as_ref().expect("write metadata").length,
                    reservation: self.reservation,
                    operation_id,
                    operation: self.operation,
                    binding: ctx.self_addr(),
                    phase: NamespaceOperationPhase::Created,
                    transitions: self.transitions.clone(),
                    generation: self.lease.map(|lease| lease.generation),
                }) {
                    Ok(publisher) => {
                        self.auxiliary = Some(publisher);
                        ctx.watch(publisher);
                    }
                    Err(error) => {
                        self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
                    }
                }
            }
            Err(error) if self.state == HostBindingState::Publishing => {
                self.fail(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn abort(
        &mut self,
        ctx: &Ctx<'_>,
        operation: ActorAddress,
        lease_id: crate::ids::BlobLeaseId,
        generation: u64,
    ) {
        match (operation, lease_id, generation) {
            (operation, lease_id, generation)
                if self.state == HostBindingState::Granted
                    && matches!(self.mode, BindingMode::Write { .. }) =>
            {
                if operation == self.operation && self.lease_matches(lease_id, generation) {
                    self.begin_release(ctx, ReleaseOutcome::WriteAborted { operation });
                }
            }
            (operation, lease_id, generation)
                if matches!(
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
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn publication_accepted(&mut self, ctx: &Ctx<'_>, operation: ActorAddress) {
        match (operation,) {
            (operation,)
                if self.state == HostBindingState::Publishing && operation == self.operation =>
            {
                self.auxiliary = None;
                self.reservation = None;
                self.enter_phase(ctx, HostBindingState::Published, Reason::Completed);
                if self.lease.is_none() {
                    self.fail(
                        ctx,
                        DataPlaneError::SourceFailure(
                            "published source retired before publication acknowledgement"
                                .to_owned(),
                        ),
                    );
                    return;
                }
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
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn publication_rejected(&mut self, ctx: &Ctx<'_>, error: DataPlaneError) {
        match error {
            error if self.state == HostBindingState::Publishing => {
                self.auxiliary = None;
                self.fail(ctx, error);
            }
            _ => {} // Stale or duplicate event: this phase no longer owns it.
        }
    }

    fn unpublished(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.state != HostBindingState::Unpublishing {
            return;
        }
        self.auxiliary = None;
        if let Err(error) = result {
            self.fail(ctx, error);
        } else if self.lease.is_none() {
            let outcome = self
                .release_outcome
                .take()
                .unwrap_or(ReleaseOutcome::PublishedReleased);
            self.finish_without_lease(ctx, outcome);
        } else {
            self.enter_phase(ctx, HostBindingState::Retiring, Reason::ReleaseRequested);
        }
    }

    fn reservation_released(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if self.state != HostBindingState::Releasing || self.auxiliary.is_none() {
            return;
        }
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

    fn release_published_event(&mut self, ctx: &Ctx<'_>) {
        // Only the source-retirement callback uses this fallback when the
        // allocator itself has already stopped and reclaimed its arena.
        self.released(ctx, Ok(()));
    }

    fn session_closed(&mut self, ctx: &Ctx<'_>) {
        match self.state {
            HostBindingState::Filling | HostBindingState::Cancelling => {
                self.enter_phase(ctx, HostBindingState::Cancelling, Reason::Cancelled);
                if let Some(transfer) = self.auxiliary
                    && matches!(self.mode, BindingMode::NamespaceRead { .. })
                {
                    let _ = ctx.send(transfer, BlobTransferEvent::Cancel);
                }
            }
            HostBindingState::Publishing => {
                self.release_outcome = Some(ReleaseOutcome::SessionClosed);
            }
            HostBindingState::Published
            | HostBindingState::Unpublishing
            | HostBindingState::Retiring
            | HostBindingState::Releasing
            | HostBindingState::Released => {}
            _ => self.begin_release(ctx, ReleaseOutcome::SessionClosed),
        }
    }

    fn released(&mut self, ctx: &Ctx<'_>, result: Result<(), DataPlaneError>) {
        if !matches!(
            self.state,
            HostBindingState::Releasing
                | HostBindingState::Publishing
                | HostBindingState::Published
                | HostBindingState::Unpublishing
                | HostBindingState::Retiring
        ) {
            return;
        }
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
        if matches!(
            self.state,
            HostBindingState::Publishing | HostBindingState::Unpublishing
        ) && self.auxiliary.is_some()
        {
            self.lease = None;
            self.published_source = None;
            return;
        }
        let outcome = self.release_outcome.take().unwrap_or_else(|| {
            if self.state == HostBindingState::Releasing {
                ReleaseOutcome::Faulted
            } else {
                ReleaseOutcome::PublishedReleased
            }
        });
        self.finish_without_lease(ctx, outcome);
    }
}

impl ActorInterface for HostBlobBindingActor {
    type Incoming = HostBindingIn;
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx<'_>) {
        self.enter_phase(ctx, self.state, Reason::Started);
        ctx.watch(self.allocator);
        ctx.watch(self.host_session);
        if let BindingMode::NamespaceRead {
            namespace,
            expected_revision,
            ..
        } = &self.mode
        {
            let namespace_proxy = namespace.proxy();
            match ctx.spawn(NamespaceResolveActor {
                namespace_proxy,
                path: self.path.clone(),
                binding: ctx.self_addr(),
                phase: NamespaceOperationPhase::Created,
                transitions: self.transitions.clone(),
                operation: self.operation,
                generation: *expected_revision,
            }) {
                Ok(resolver) => {
                    self.auxiliary = Some(resolver);
                    ctx.watch(resolver);
                }
                Err(error) => {
                    self.fail(ctx, DataPlaneError::SessionFailed(error.to_string()));
                }
            }
            return;
        }

        self.enter_phase(ctx, HostBindingState::Filling, Reason::Allocated);
        let (length, digest) = match &self.mode {
            BindingMode::Write { length, digest, .. } => (*length, *digest),
            BindingMode::NamespaceRead { .. } => unreachable!("handled above"),
        };
        if ctx
            .send(
                self.allocator,
                ArenaAllocatorIn::Allocate {
                    binding: ctx.self_addr(),
                    kind: AllocationKind::Write { length, digest },
                },
            )
            .is_err()
        {
            self.fail(ctx, DataPlaneError::SessionNotRunning);
        }
    }

    fn handle(&mut self, ctx: &Ctx<'_>, message: HostBindingIn) {
        match message {
            HostBindingIn::Resolved(result) => self.resolved(ctx, result),
            HostBindingIn::TransferReady { lease, metadata } => {
                self.transfer_ready(ctx, lease, metadata)
            }
            HostBindingIn::TransferFailed(error) => self.transfer_failed(ctx, error),
            HostBindingIn::Allocated(result) => self.allocated(ctx, result),
            HostBindingIn::CancelOpen { operation } => self.cancel_open(ctx, operation),
            HostBindingIn::Release {
                lease_id,
                generation,
            } => self.release(ctx, lease_id, generation),
            HostBindingIn::Seal {
                operation,
                lease,
                metadata,
            } => self.seal(ctx, operation, lease, metadata),
            HostBindingIn::SealValidated(result) => self.seal_validated(ctx, result),
            HostBindingIn::PublishedSource(result) => self.published_source(ctx, result),
            HostBindingIn::Abort {
                operation,
                lease_id,
                generation,
            } => self.abort(ctx, operation, lease_id, generation),
            HostBindingIn::PublicationAccepted { operation } => {
                self.publication_accepted(ctx, operation)
            }
            HostBindingIn::PublicationRejected(error) => self.publication_rejected(ctx, error),
            HostBindingIn::Unpublished(result) => self.unpublished(ctx, result),
            HostBindingIn::ReservationReleased(result) => self.reservation_released(ctx, result),
            HostBindingIn::ReleasePublished => self.release_published_event(ctx),
            HostBindingIn::SessionClosed => self.session_closed(ctx),
            HostBindingIn::Released(result) => self.released(ctx, result),
        }
    }

    fn on_actor_exit(&mut self, ctx: &Ctx<'_>, exited: ActorExited) {
        if self.state == HostBindingState::Released {
            return;
        }
        if self.auxiliary == Some(exited.addr) {
            self.auxiliary = None;
            self.fail(ctx, DataPlaneError::SessionNotRunning);
        } else if exited.addr == self.allocator {
            if matches!(
                self.state,
                HostBindingState::Published | HostBindingState::Unpublishing
            ) {
                return;
            }
            self.lease = None;
            self.fail(ctx, DataPlaneError::SessionNotRunning);
        } else if exited.addr == self.host_session {
            self.session_closed(ctx);
        }
    }

    fn on_stop(&mut self, ctx: &Ctx<'_>) {
        self.release_retained_source();
        if self.state != HostBindingState::Released {
            if let Some(auxiliary) = self.auxiliary.take() {
                let _ = ctx.stop_actor(auxiliary);
            }
            let source_owns_release =
                self.published_source.is_some() || self.state == HostBindingState::Retiring;
            if let Some(source) = self.published_source.take() {
                let _ = ctx.send(source, BlobSourceIn::Retire { reply_to: None });
            }
            if !source_owns_release && let Some(lease) = self.lease.take() {
                let _ = ctx.send(
                    self.allocator,
                    ArenaAllocatorIn::Release {
                        binding: ctx.self_addr(),
                        lease,
                    },
                );
            }
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::BindingFaulted {
                    binding: ctx.self_addr(),
                    operation: self.operation,
                    error: DataPlaneError::SessionNotRunning,
                },
            );
            let _ = ctx.send(
                self.host_session,
                HostSessionIn::BindingDone {
                    binding: ctx.self_addr(),
                },
            );
        }
    }
}
